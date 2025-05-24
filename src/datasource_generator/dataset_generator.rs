use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::MemTable;
use datafusion::error::Result;
use rand::Rng;
use rand::rngs::StdRng;

use crate::common::{FuzzerDataType, LogicalTable, LogicalTableType, get_available_data_types};
use crate::{common::rng::rng_from_seed, fuzz_context::GlobalContext};

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

    pub fn generate_dataset(&mut self) -> Result<LogicalTable> {
        // ==== Generate schema ====
        // Generated schema for table 't1':
        // col_t1_1, col_t1_2, ...
        let table_name = self.ctx.runtime_context.next_table_name(); // t1, t2, ...
        let cfg_max_col_count = self.ctx.runner_config.max_column_count;

        let num_columns = self.rng.random_range(1..=cfg_max_col_count);
        let mut columns = Vec::new();
        let mut column_fuzzer_types = Vec::new(); // Store fuzzer types for array generation
        let available_types = get_available_data_types();

        // Hack: using unparser to display the expr will lowercase the column name
        // here we all use lowercase column name to avoid the issue.
        for i in 0..num_columns {
            // Pick a random column type from available types
            let fuzzer_column_type =
                &available_types[self.rng.random_range(0..available_types.len())];
            let datafusion_column_type = fuzzer_column_type.to_datafusion_type();
            let column_name = format!(
                "col_{table_name}_{}_{}",
                i + 1,
                fuzzer_column_type.display_name()
            );
            columns.push(Field::new(column_name, datafusion_column_type, false));
            column_fuzzer_types.push(fuzzer_column_type.clone());
        }
        let schema = Schema::new(columns);

        // ==== Generate dataset ====
        let cfg_max_row_count = self.ctx.runner_config.max_row_count;
        let actual_row_count = self.rng.random_range(0..cfg_max_row_count);

        let cols: Result<Vec<_>> = column_fuzzer_types
            .iter()
            .map(|fuzzer_type| self.generate_array_of_type(fuzzer_type, actual_row_count))
            .collect();

        assert!(self.buffered_datasets.is_none());
        let batch = RecordBatch::try_new(Arc::new(schema), cols?)?;
        self.buffered_datasets = Some(batch.clone());

        // ==== Register table ====
        let registered_table = self.register_table(&table_name, &batch)?;

        // ==== Cleanup ====
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

    // TODO: now numbers generated are all simple values (e.g. int32 - [-100, 100])
    // edge cases and large numbers are not covered.
    fn generate_array_of_type(
        &mut self,
        fuzzer_type: &FuzzerDataType,
        len: u64,
    ) -> Result<ArrayRef> {
        match fuzzer_type {
            FuzzerDataType::Int32 => {
                use datafusion::arrow::array::Int32Builder;

                let mut builder = Int32Builder::new();
                for _ in 0..len {
                    // Use naive range -100 to 100 to avoid edge cases
                    let value = self.rng.random_range(-100..=100);
                    builder.append_value(value);
                }

                Ok(Arc::new(builder.finish()))
            }
            FuzzerDataType::Int64 => {
                use datafusion::arrow::array::Int64Builder;

                let mut builder = Int64Builder::new();
                for _ in 0..len {
                    // Use naive range -100 to 100 to avoid edge cases
                    let value = self.rng.random_range(-100..=100);
                    builder.append_value(value);
                }

                Ok(Arc::new(builder.finish()))
            }
            FuzzerDataType::UInt32 => {
                use datafusion::arrow::array::UInt32Builder;

                let mut builder = UInt32Builder::new();
                for _ in 0..len {
                    // Use naive range 0 to 100 to avoid edge cases
                    let value = self.rng.random_range(0..=100);
                    builder.append_value(value);
                }

                Ok(Arc::new(builder.finish()))
            }
            FuzzerDataType::UInt64 => {
                use datafusion::arrow::array::UInt64Builder;

                let mut builder = UInt64Builder::new();
                for _ in 0..len {
                    // Use naive range 0 to 100 to avoid edge cases
                    let value = self.rng.random_range(0..=100);
                    builder.append_value(value);
                }

                Ok(Arc::new(builder.finish()))
            }
            FuzzerDataType::Float32 => {
                use datafusion::arrow::array::Float32Builder;

                let mut builder = Float32Builder::new();
                for _ in 0..len {
                    // Use naive range -100.0 to 100.0 to avoid edge cases
                    let value = self.rng.random_range(-100.0..=100.0);
                    builder.append_value(value);
                }

                Ok(Arc::new(builder.finish()))
            }
            FuzzerDataType::Float64 => {
                use datafusion::arrow::array::Float64Builder;

                let mut builder = Float64Builder::new();
                for _ in 0..len {
                    // Use naive range -100.0 to 100.0 to avoid edge cases
                    let value = self.rng.random_range(-100.0..=100.0);
                    builder.append_value(value);
                }

                Ok(Arc::new(builder.finish()))
            }
            FuzzerDataType::Boolean => {
                use datafusion::arrow::array::BooleanBuilder;

                let mut builder = BooleanBuilder::new();
                for _ in 0..len {
                    let value = self.rng.random_bool(0.5); // 50% chance of true/false
                    builder.append_value(value);
                }

                Ok(Arc::new(builder.finish()))
            }
        }
    }
}
