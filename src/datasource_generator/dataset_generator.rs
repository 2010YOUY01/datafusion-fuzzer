use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type};
use datafusion::catalog::MemTable;
use datafusion::common::internal_err;
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

    // TODO: pick random column type
    pub fn generate_dataset(&mut self) -> Result<LogicalTable> {
        // ==== Generate schema ====
        // Generated schema for table 't1':
        // col_t1_1, col_t1_2, ...
        let table_name = self.ctx.runtime_context.next_table_name(); // t1, t2, ...
        let cfg_max_col_count = self.ctx.runner_config.max_column_count;

        let num_columns = self.rng.gen_range(1..=cfg_max_col_count);
        let mut columns = Vec::new();
        // Hack: using unparser to display the expr will lowercase the column name
        // here we all use lowercase column name to avoid the issue.
        for i in 0..num_columns {
            let column_type = DataType::Int64;
            let column_name = format!(
                "col_{table_name}_{}_{}",
                i + 1,
                format!("{:?}", column_type).to_lowercase()
            );
            columns.push(Field::new(column_name, column_type, false));
        }
        let schema = Schema::new(columns);

        // ==== Generate dataset ====
        let cfg_max_row_count = self.ctx.runner_config.max_row_count;
        let actual_row_count = self.rng.gen_range(0..cfg_max_row_count);

        let cols: Result<Vec<_>> = schema
            .fields()
            .iter()
            .map(|field| self.generate_array_of_type(field.data_type(), actual_row_count))
            .collect();

        assert!(self.buffered_datasets.is_none());
        let batch = RecordBatch::try_new(Arc::new(schema), cols?)?;
        self.buffered_datasets = Some(batch.clone());

        // ==== Register table ====
        let registered_table = self.register_table(&table_name, &batch)?;

        // ==== Celanup ====
        self.buffered_datasets = None;

        Ok(registered_table)
    }

    /// Register the buffered dataset both datafusion context and fuzzer context
    fn register_table(&mut self, table_name: &str, dataset: &RecordBatch) -> Result<LogicalTable> {
        // Construct mem table
        let dataset_schema = dataset.schema();
        let mem_table =
            MemTable::try_new(Arc::clone(&dataset_schema), vec![vec![dataset.clone()]])?;

        // Register memtable into datafusion context
        self.ctx
            .runtime_context
            .df_ctx
            .register_table(table_name, Arc::new(mem_table))?;

        // Register table into fuzzer runtime context
        let logical_table = LogicalTable::new(
            table_name.to_string(),
            Arc::clone(&dataset_schema),
            LogicalTableType::Table,
        );
        self.ctx
            .runtime_context
            .registered_tables
            .write()
            .unwrap()
            .insert(table_name.to_string(), Arc::new(logical_table.clone()));

        Ok(logical_table)
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
