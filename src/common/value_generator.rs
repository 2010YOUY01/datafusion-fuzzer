use crate::common::FuzzerDataType;
use rand::Rng;
use rand::rngs::StdRng;

/// Raw value representation for generated data
#[derive(Debug, Clone)]
pub enum GeneratedValue {
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Decimal {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Null,
}

/// Configuration for value generation
#[derive(Debug, Clone, PartialEq)]
pub struct ValueGenerationConfig {
    pub nullable: bool,
    pub null_probability: f64,
    pub int_range: (i32, i32),
    pub uint_range: (u32, u32),
    pub float_range: (f64, f64),
}

impl Default for ValueGenerationConfig {
    fn default() -> Self {
        Self {
            nullable: false,
            null_probability: 0.1,
            int_range: (-100, 100),
            uint_range: (0, 200),
            float_range: (-100.0, 100.0),
        }
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

/// Core value generation logic shared by both functions
pub fn generate_value(
    rng: &mut StdRng,
    fuzzer_type: &FuzzerDataType,
    config: &ValueGenerationConfig,
) -> GeneratedValue {
    // Handle null generation
    if config.nullable && rng.random_bool(config.null_probability) {
        return GeneratedValue::Null;
    }

    match fuzzer_type {
        FuzzerDataType::Int32 => {
            let value = rng.random_range(config.int_range.0..=config.int_range.1);
            GeneratedValue::Int32(value)
        }
        FuzzerDataType::Int64 => {
            let value = rng.random_range(config.int_range.0 as i64..=config.int_range.1 as i64);
            GeneratedValue::Int64(value)
        }
        FuzzerDataType::UInt32 => {
            let value = rng.random_range(config.uint_range.0..=config.uint_range.1);
            GeneratedValue::UInt32(value)
        }
        FuzzerDataType::UInt64 => {
            let value = rng.random_range(config.uint_range.0 as u64..=config.uint_range.1 as u64);
            GeneratedValue::UInt64(value)
        }
        FuzzerDataType::Float32 => {
            let value = rng.random_range(config.float_range.0 as f32..=config.float_range.1 as f32);
            GeneratedValue::Float32(value)
        }
        FuzzerDataType::Float64 => {
            let value = rng.random_range(config.float_range.0..=config.float_range.1);
            GeneratedValue::Float64(value)
        }
        FuzzerDataType::Boolean => {
            let value = rng.random_bool(0.5);
            GeneratedValue::Boolean(value)
        }
        FuzzerDataType::Decimal { precision, scale } => {
            // Use the existing safe decimal generation logic
            let simple_value = rng.random_range(-99999..=99999);
            let scale_factor = safe_power_of_10(*scale);
            let decimal_value = simple_value * scale_factor;

            GeneratedValue::Decimal {
                value: decimal_value,
                precision: *precision,
                scale: *scale,
            }
        }
    }
}

impl GeneratedValue {
    /// Convert to SQL string representation
    pub fn to_sql_string(&self) -> String {
        match self {
            GeneratedValue::Int32(v) => v.to_string(),
            GeneratedValue::Int64(v) => v.to_string(),
            GeneratedValue::UInt32(v) => v.to_string(),
            GeneratedValue::UInt64(v) => v.to_string(),
            GeneratedValue::Float32(v) => v.to_string(),
            GeneratedValue::Float64(v) => v.to_string(),
            GeneratedValue::Boolean(v) => if *v { "TRUE" } else { "FALSE" }.to_string(),
            GeneratedValue::Decimal {
                value,
                precision: _,
                scale,
            } => {
                // Format decimal with proper scale
                if *scale > 0 {
                    let scale_factor = 10_i128.pow(*scale as u32);
                    let integer_part = value / scale_factor;
                    let fractional_part = (value % scale_factor).abs();
                    format!(
                        "{}.{:0width$}",
                        integer_part,
                        fractional_part,
                        width = *scale as usize
                    )
                } else {
                    value.to_string()
                }
            }
            GeneratedValue::Null => "NULL".to_string(),
        }
    }

    /// Convert to DataFusion ScalarValue
    pub fn to_scalar_value(&self) -> datafusion::scalar::ScalarValue {
        use datafusion::scalar::ScalarValue;

        match self {
            GeneratedValue::Int32(v) => ScalarValue::Int32(Some(*v)),
            GeneratedValue::Int64(v) => ScalarValue::Int64(Some(*v)),
            GeneratedValue::UInt32(v) => ScalarValue::UInt32(Some(*v)),
            GeneratedValue::UInt64(v) => ScalarValue::UInt64(Some(*v)),
            GeneratedValue::Float32(v) => ScalarValue::Float32(Some(*v)),
            GeneratedValue::Float64(v) => ScalarValue::Float64(Some(*v)),
            GeneratedValue::Boolean(v) => ScalarValue::Boolean(Some(*v)),
            GeneratedValue::Decimal {
                value,
                precision,
                scale,
            } => {
                if *precision <= 38 {
                    ScalarValue::Decimal128(Some(*value), *precision, *scale)
                } else {
                    use datafusion::arrow::datatypes::i256;
                    let decimal_value_256 = i256::from_i128(*value);
                    ScalarValue::Decimal256(Some(decimal_value_256), *precision, *scale)
                }
            }
            GeneratedValue::Null => ScalarValue::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rng::rng_from_seed;

    #[test]
    fn test_cached_config_in_runtime_context() {
        // Test that RuntimeContext has the expected cached config
        use crate::fuzz_context::RuntimeContext;

        let runtime_ctx = RuntimeContext::default();
        let expected_config = ValueGenerationConfig::default(); // Non-nullable by default

        assert_eq!(runtime_ctx.value_generation_config, expected_config);
        assert!(!runtime_ctx.value_generation_config.nullable); // Should be non-nullable
    }

    #[test]
    fn test_generate_value_with_cached_config() {
        // Test that generate_value works with cached configs from RuntimeContext
        use crate::fuzz_context::RuntimeContext;

        let mut rng = rng_from_seed(42);
        let fuzzer_type = FuzzerDataType::Int32;
        let runtime_ctx = RuntimeContext::default();

        let value = generate_value(&mut rng, &fuzzer_type, &runtime_ctx.value_generation_config);

        // Should generate a non-null Int32 value (since config is non-nullable by default)
        match value {
            GeneratedValue::Int32(v) => {
                assert!(v >= -100 && v <= 100, "Value should be in expected range");
            }
            _ => panic!("Expected Int32 value, got: {:?}", value),
        }
    }
}
