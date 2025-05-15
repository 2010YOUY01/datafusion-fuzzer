use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Int64Type, SchemaRef};
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::error::Result;
use datafusion_test_utils::array_gen::PrimitiveArrayGenerator;
use rand::rngs::StdRng;
use rand::{Rng, RngCore};

use crate::common::{LogicalTable, LogicalTableType};
use crate::{fuzz_context::GlobalContext, rng::rng_from_seed};

pub struct DatasetGenerator {
    rng: StdRng,
    ctx: Arc<GlobalContext>,

    buffered_datasets: Option<RecordBatch>,
}

impl DatasetGenerator {
    pub fn new(seed: u64, context: Arc<GlobalContext>) -> Self {
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
            buffered_datasets: None,
        }
    }

    pub fn generate_dataset(&mut self, schema: SchemaRef) -> Result<()> {
        let cfg_max_row_count = self.ctx.runner_config.max_row_count;
        let actual_row_count = self.rng.gen_range(0..cfg_max_row_count);

        let cols: Result<Vec<_>> = schema
            .fields()
            .iter()
            .map(|field| self.generate_array_of_type(field.data_type(), actual_row_count))
            .collect();

        assert!(self.buffered_datasets.is_none());
        let batch = RecordBatch::try_new(schema, cols?)?;
        self.buffered_datasets = Some(batch.clone());
        Ok(())
    }

    fn generate_array_of_type(&mut self, field_type: &DataType, len: u64) -> Result<ArrayRef> {
        match field_type {
            DataType::Int64 => {
                let mut arr_gen = PrimitiveArrayGenerator {
                    num_primitives: len as usize,
                    num_distinct_primitives: len as usize,
                    null_pct: 0.0,
                    rng: rng_from_seed(self.rng.next_u64()),
                };

                Ok(arr_gen.gen_data::<Int64Type>())
            }
            _ => return internal_err!("Unsupported data type"),
        }
    }
}

/// Methods related to registering the generated dataset into datafusion context
impl DatasetGenerator {
    /// Register the buffered dataset into datafusion context
    pub fn register_table(&mut self) -> Result<LogicalTable> {
        // Construct mem table
        let table_name = self.ctx.runtime_context.next_table_name(); // t1, t2, ...
        let buffered_dataset = self
            .buffered_datasets
            .take()
            .ok_or_else(|| internal_datafusion_err!("No dataset to register"))?;
        let dataset_schema = buffered_dataset.schema();
        let mem_table =
            MemTable::try_new(Arc::clone(&dataset_schema), vec![vec![buffered_dataset]])?;

        // Register memtable into datafusion context
        self.ctx
            .runtime_context
            .df_ctx
            .register_table(&table_name, Arc::new(mem_table))?;

        // Register table into fuzzer runtime context
        let logical_table = LogicalTable::new(
            table_name.clone(),
            Arc::clone(&dataset_schema),
            LogicalTableType::Table,
        );
        self.ctx
            .runtime_context
            .registered_tables
            .write()
            .unwrap()
            .insert(table_name.clone(), Arc::new(logical_table.clone()));

        Ok(logical_table)
    }
}
