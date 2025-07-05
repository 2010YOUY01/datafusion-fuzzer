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

    /// Safely calculate 10^scale, preventing overflow
    fn safe_power_of_10(scale: i8) -> i128 {
        // The maximum power of 10 that fits in i128 is approximately 10^38
        // For safety, we limit to 10^30 to avoid overflow in calculations
        let safe_scale = std::cmp::min(scale as u32, 30);
        match safe_scale {
            0 => 1,
            1..=30 => 10_i128.pow(safe_scale),
            _ => 10_i128.pow(30), // Fallback to 10^30 for any edge cases
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
        let df_ctx = self.ctx.runtime_context.get_session_context();
        df_ctx.register_table(table_name, Arc::new(mem_table))?;

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

    // TODO(coverage): now numbers generated are all simple values (e.g. int32 - [-100, 100])
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
            FuzzerDataType::Decimal { precision, scale } => {
                // Use appropriate Builder based on precision
                // DataFusion automatically chooses Decimal128 vs Decimal256 based on precision
                if *precision <= 38 {
                    use datafusion::arrow::array::Decimal128Builder;

                    let mut builder =
                        Decimal128Builder::new().with_precision_and_scale(*precision, *scale)?;
                    for _ in 0..len {
                        // Generate very simple, safe decimal values to avoid casting issues
                        // Use a much more conservative approach

                        // For casting compatibility, use very small values
                        // Generate a simple integer value between -100 and 100
                        let simple_value = self.rng.random_range(-100..=100);

                        // Apply scale factor to create a proper decimal value
                        let scale_factor = Self::safe_power_of_10(*scale);
                        let decimal_value = simple_value * scale_factor;

                        builder.append_value(decimal_value);
                    }

                    Ok(Arc::new(builder.finish()))
                } else {
                    use datafusion::arrow::array::Decimal256Builder;
                    use datafusion::arrow::datatypes::i256;

                    let mut builder =
                        Decimal256Builder::new().with_precision_and_scale(*precision, *scale)?;
                    for _ in 0..len {
                        // Generate a decimal value that fits within the precision and scale
                        // Be very conservative to avoid casting issues

                        // Calculate the maximum value that fits in the precision
                        // For precision P and scale S, max value is 10^(P-S) - 1
                        let max_integral_digits = *precision as i32 - *scale as i32;

                        let decimal_value = if max_integral_digits <= 0 {
                            // Edge case: scale >= precision, only fractional part
                            let fractional_part = if *scale > 0 {
                                self.rng.random_range(0..Self::safe_power_of_10(*scale))
                            } else {
                                0
                            };
                            fractional_part
                        } else {
                            // Normal case: both integral and fractional parts
                            let max_integral_value =
                                Self::safe_power_of_10(max_integral_digits as i8) - 1;

                            // Use even more conservative ranges to avoid casting issues
                            let safe_integral_limit = std::cmp::min(max_integral_value, 1000);
                            let integral_part = self
                                .rng
                                .random_range(-safe_integral_limit..=safe_integral_limit);

                            let fractional_part = if *scale > 0 {
                                self.rng.random_range(0..Self::safe_power_of_10(*scale))
                            } else {
                                0
                            };

                            // Combine integral and fractional parts
                            let scale_factor = Self::safe_power_of_10(*scale);
                            let decimal_value = integral_part * scale_factor + fractional_part;

                            // Ensure the absolute value doesn't exceed the precision limit
                            let max_total_value = Self::safe_power_of_10(*precision as i8) - 1;
                            if decimal_value.abs() > max_total_value {
                                // Clamp to safe range
                                if decimal_value >= 0 {
                                    std::cmp::min(decimal_value, max_total_value)
                                } else {
                                    std::cmp::max(decimal_value, -max_total_value)
                                }
                            } else {
                                decimal_value
                            }
                        };

                        // Convert i128 to i256 for Decimal256
                        let decimal_value_256 = i256::from_i128(decimal_value);
                        builder.append_value(decimal_value_256);
                    }

                    Ok(Arc::new(builder.finish()))
                }
            }
        }
    }
}
