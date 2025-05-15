use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use rand::{Rng, rngs::StdRng};

use crate::{fuzz_context::GlobalContext, rng::rng_from_seed};

pub struct SchemaGenerator {
    rng: StdRng,
    ctx: Arc<GlobalContext>,
}

impl SchemaGenerator {
    pub fn new(seed: u64, context: Arc<GlobalContext>) -> Self {
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
        }
    }

    pub fn generate_schema(&mut self) -> Schema {
        let cfg_max_col_count = self.ctx.runner_config.max_column_count;

        let num_columns = self.rng.gen_range(1..=cfg_max_col_count);
        let mut columns = Vec::new();
        for i in 0..num_columns {
            let column_name = format!("col_{}", i + 1);
            let column_type = DataType::Int64;
            columns.push(Field::new(column_name, column_type, false));
        }
        Schema::new(columns)
    }
}
