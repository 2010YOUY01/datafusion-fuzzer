use datafusion::scalar::ScalarValue;
use rand::{Rng, rngs::StdRng};

use crate::common::FuzzerDataType;

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
        FuzzerDataType::Decimal { precision, scale } => {
            // Generate very simple, safe decimal values to avoid casting issues
            // Use a much more conservative approach

            // For casting compatibility, use very small values
            // Generate a simple integer value between -100 and 100
            let simple_value = rng.random_range(-100..=100);

            // Apply scale factor to create a proper decimal value
            let scale_factor = safe_power_of_10(*scale);
            let decimal_value = simple_value * scale_factor;

            // Use appropriate ScalarValue variant based on precision
            if *precision <= 38 {
                ScalarValue::Decimal128(Some(decimal_value), *precision, *scale)
            } else {
                use datafusion::arrow::datatypes::i256;
                let decimal_value_256 = i256::from_i128(decimal_value);
                ScalarValue::Decimal256(Some(decimal_value_256), *precision, *scale)
            }
        }
    }
}
