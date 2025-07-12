use datafusion::scalar::ScalarValue;
use rand::rngs::StdRng;
use std::sync::Arc;

use crate::common::FuzzerDataType;
use crate::common::value_generator::generate_value;
use crate::fuzz_context::GlobalContext;

// TODO(coverage): now only small numbers are geenrated to avoid overflows. Change to
// large edge cases (e.g. max/min values) in the future.
pub fn generate_scalar_literal(
    ctx: &Arc<GlobalContext>,
    rng: &mut StdRng,
    target_type: &FuzzerDataType,
) -> ScalarValue {
    let value = generate_value(
        rng,
        target_type,
        &ctx.runtime_context.value_generation_config,
    );
    value.to_scalar_value()
}
