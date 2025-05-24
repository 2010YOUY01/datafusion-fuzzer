use datafusion::scalar::ScalarValue;
use rand::{Rng, rngs::StdRng};

use crate::common::FuzzerDataType;

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
    }
}
