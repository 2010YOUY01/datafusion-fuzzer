use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Int64Type, SchemaRef};
use datafusion::arrow::{array::RecordBatch, datatypes::Schema};
use datafusion::common::internal_err;
use datafusion::error::Result;
use datafusion_test_utils::array_gen::PrimitiveArrayGenerator;
use rand::rngs::StdRng;
use rand::{Rng, RngCore};

use crate::{fuzz_context::GlobalContext, rng::rng_from_seed};

pub struct DatasetGenerator {
    rng: StdRng,
    ctx: Arc<GlobalContext>,
}

impl DatasetGenerator {
    pub fn new(seed: u64, context: Arc<GlobalContext>) -> Self {
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
        }
    }

    pub fn generate_dataset(&mut self, schema: SchemaRef) -> Result<RecordBatch> {
        let cfg_max_row_count = self.ctx.runner_config.max_row_count;
        let actual_row_count = self.rng.gen_range(0..cfg_max_row_count);

        let cols: Result<Vec<_>> = schema
            .fields()
            .iter()
            .map(|field| self.generate_array_of_type(field.data_type(), actual_row_count))
            .collect();

        Ok(RecordBatch::try_new(schema, cols?)?)
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
