use datafusion::scalar::ScalarValue;
use rand::{Rng, rngs::StdRng};

use crate::common::FuzzerDataType;

// TODO(coverage): now only small numbers are geenrated to avoid overflows. Change to
// large edge cases (e.g. max/min values) in the future.
pub fn generate_scalar_literal(
    rng: &mut StdRng,
    target_type: &FuzzerDataType,
    nullable: bool,
) -> ScalarValue {
    if nullable && rng.random_bool(0.1) {
        return ScalarValue::Null;
    }

    match target_type {
        FuzzerDataType::Boolean => ScalarValue::Boolean(Some(rng.random_bool(0.5))),
        FuzzerDataType::Int32 => {
            let value = rng.random_range(-100..=100);
            ScalarValue::Int32(Some(value))
        }
        FuzzerDataType::Int64 => {
            let value = rng.random_range(-100..=100);
            ScalarValue::Int64(Some(value))
        }
        FuzzerDataType::UInt32 => {
            let value = rng.random_range(0..=200);
            ScalarValue::UInt32(Some(value))
        }
        FuzzerDataType::UInt64 => {
            let value = rng.random_range(0..=200);
            ScalarValue::UInt64(Some(value))
        }
        FuzzerDataType::Float32 => {
            let value = rng.random_range(-100.0..=100.0);
            ScalarValue::Float32(Some(value))
        }
        FuzzerDataType::Float64 => {
            let value = rng.random_range(-100.0..=100.0);
            ScalarValue::Float64(Some(value))
        }
        FuzzerDataType::Decimal128 { precision, scale } => {
            // Generate a decimal value that fits within the precision and scale
            // Use smaller ranges to prevent overflow issues
            let scale_factor = 10_i128.pow(*scale as u32);

            // Use a much smaller range to avoid overflow and keep values manageable
            // Instead of using full precision, limit to smaller safe ranges
            let safe_range = match *precision {
                1..=10 => 1000,    // For small precision, use range -1000 to 1000
                11..=20 => 10000,  // For medium precision, use range -10000 to 10000
                21..=30 => 100000, // For larger precision, use range -100000 to 100000
                _ => 1000000,      // For max precision, use range -1000000 to 1000000
            };

            let max_value = safe_range;
            let min_value = -max_value;

            let integral_part = rng.random_range(min_value..=max_value);
            let fractional_part = if *scale > 0 {
                rng.random_range(0..scale_factor)
            } else {
                0
            };

            let decimal_value = integral_part * scale_factor + fractional_part;
            ScalarValue::Decimal128(Some(decimal_value), *precision, *scale)
        }
    }
}
