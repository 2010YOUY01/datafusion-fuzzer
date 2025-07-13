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
    Date32(i32),              // Days since Unix epoch (1970-01-01)
    Time64Nanosecond(i64),    // Nanoseconds since midnight
    TimestampNanosecond(i64), // Nanoseconds since Unix epoch (1970-01-01 00:00:00 UTC)
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
            nullable: true,
            null_probability: 0.1,
            int_range: (-100, 100),
            uint_range: (0, 200),
            float_range: (-100.0, 100.0),
        }
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
        FuzzerDataType::Date32 => {
            // Generate a reasonable range of dates:
            // - Days 0-36500 covers approximately 100 years from 1970-01-01
            // - This gives us dates from 1970-01-01 to roughly 2070
            let days_since_epoch = rng.random_range(0..=36500);
            GeneratedValue::Date32(days_since_epoch)
        }
        FuzzerDataType::Time64Nanosecond => {
            // Generate a reasonable range of times in nanoseconds since midnight:
            // - 0 to 86,399,999,999,999 nanoseconds (24 hours in nanoseconds)
            // - This covers the full range of a day from 00:00:00 to 23:59:59.999999999
            let nanoseconds_per_day = 24 * 60 * 60 * 1_000_000_000i64; // 24 hours in nanoseconds
            let nanoseconds_since_midnight = rng.random_range(0..nanoseconds_per_day);
            GeneratedValue::Time64Nanosecond(nanoseconds_since_midnight)
        }
        FuzzerDataType::TimestampNanosecond => {
            // Generate a reasonable range of timestamps in nanoseconds since Unix epoch:
            // - Start: 0 (1970-01-01 00:00:00 UTC)
            // - End: approximately 100 years of nanoseconds from epoch
            // - This gives us timestamps from 1970-01-01 to roughly 2070
            let nanoseconds_per_day = 24 * 60 * 60 * 1_000_000_000i64;
            let days_in_100_years = 36500i64; // Approximate
            let max_nanoseconds = nanoseconds_per_day * days_in_100_years;
            let nanoseconds_since_epoch = rng.random_range(0..=max_nanoseconds);
            GeneratedValue::TimestampNanosecond(nanoseconds_since_epoch)
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
            GeneratedValue::Date32(days_since_epoch) => {
                // Convert days since Unix epoch to SQL date format
                // Unix epoch is 1970-01-01
                days_to_date_string(*days_since_epoch)
            }
            GeneratedValue::Time64Nanosecond(nanoseconds) => {
                // Convert nanoseconds since midnight to SQL time format (HH:MM:SS.nnnnnnnnn)
                let ns = *nanoseconds;

                // Calculate hours, minutes, seconds, and nanoseconds
                let hours = ns / (60 * 60 * 1_000_000_000);
                let remaining_ns = ns % (60 * 60 * 1_000_000_000);
                let minutes = remaining_ns / (60 * 1_000_000_000);
                let remaining_ns = remaining_ns % (60 * 1_000_000_000);
                let seconds = remaining_ns / 1_000_000_000;
                let nanoseconds = remaining_ns % 1_000_000_000;

                // Format as SQL time literal with nanosecond precision
                format!(
                    "'{:02}:{:02}:{:02}.{:09}'",
                    hours, minutes, seconds, nanoseconds
                )
            }
            GeneratedValue::TimestampNanosecond(nanoseconds_since_epoch) => {
                // Convert nanoseconds since Unix epoch to SQL timestamp format
                nanoseconds_to_timestamp_string(*nanoseconds_since_epoch)
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
            GeneratedValue::Date32(v) => ScalarValue::Date32(Some(*v)),
            GeneratedValue::Time64Nanosecond(v) => ScalarValue::Time64Nanosecond(Some(*v)),
            GeneratedValue::TimestampNanosecond(v) => {
                ScalarValue::TimestampNanosecond(Some(*v), None)
            }
            GeneratedValue::Null => ScalarValue::Null,
        }
    }
}

// =================
// Utility functions
// =================

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

/// Convert days since Unix epoch (1970-01-01) to a proper date string
/// This function properly handles leap years and varying month lengths
fn days_to_date_string(days_since_epoch: i32) -> String {
    // Days per month (non-leap year)
    const DAYS_IN_MONTH: [i32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let mut days = days_since_epoch;
    let mut year = 1970;

    // Calculate year
    while days >= 365 {
        // Check if current year is a leap year
        let is_leap = (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
        let days_in_year = if is_leap { 366 } else { 365 };

        if days >= days_in_year {
            days -= days_in_year;
            year += 1;
        } else {
            break;
        }
    }

    // Calculate month and day
    let is_leap = (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
    let mut month = 1;
    let mut day = days + 1; // +1 because days are 0-indexed

    for (i, &days_in_month) in DAYS_IN_MONTH.iter().enumerate() {
        let adjusted_days = if i == 1 && is_leap {
            days_in_month + 1
        } else {
            days_in_month
        };

        if day <= adjusted_days {
            break;
        }
        day -= adjusted_days;
        month += 1;
    }

    format!("'{:04}-{:02}-{:02}'", year, month, day)
}

/// Convert nanoseconds since Unix epoch to a proper timestamp string
/// This function properly handles leap years and varying month lengths
fn nanoseconds_to_timestamp_string(nanoseconds_since_epoch: i64) -> String {
    // Days per month (non-leap year)
    const DAYS_IN_MONTH: [i32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let nanoseconds_per_day = 24 * 60 * 60 * 1_000_000_000i64;
    let days_since_epoch = nanoseconds_since_epoch / nanoseconds_per_day;
    let remaining_ns = nanoseconds_since_epoch % nanoseconds_per_day;

    let mut days = days_since_epoch as i32;
    let mut year = 1970;

    // Calculate year
    while days >= 365 {
        // Check if current year is a leap year
        let is_leap = (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
        let days_in_year = if is_leap { 366 } else { 365 };

        if days >= days_in_year {
            days -= days_in_year;
            year += 1;
        } else {
            break;
        }
    }

    // Calculate month and day
    let is_leap = (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
    let mut month = 1;
    let mut day = days + 1; // +1 because days are 0-indexed

    for (i, &days_in_month) in DAYS_IN_MONTH.iter().enumerate() {
        let adjusted_days = if i == 1 && is_leap {
            days_in_month + 1
        } else {
            days_in_month
        };

        if day <= adjusted_days {
            break;
        }
        day -= adjusted_days;
        month += 1;
    }

    // Calculate time components
    let hours = remaining_ns / (60 * 60 * 1_000_000_000);
    let remaining_ns = remaining_ns % (60 * 60 * 1_000_000_000);
    let minutes = remaining_ns / (60 * 1_000_000_000);
    let remaining_ns = remaining_ns % (60 * 1_000_000_000);
    let seconds = remaining_ns / 1_000_000_000;
    let nanoseconds = remaining_ns % 1_000_000_000;

    format!(
        "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}'",
        year, month, day, hours, minutes, seconds, nanoseconds
    )
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
        let expected_config = ValueGenerationConfig::default(); // Nullable by default

        assert_eq!(runtime_ctx.value_generation_config, expected_config);
        assert!(runtime_ctx.value_generation_config.nullable); // Should be nullable
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

    #[test]
    fn test_timestamp_nanosecond_generation() {
        // Test that TimestampNanosecond generation works correctly
        use crate::fuzz_context::RuntimeContext;

        let mut rng = rng_from_seed(42);
        let fuzzer_type = FuzzerDataType::TimestampNanosecond;
        let runtime_ctx = RuntimeContext::default();

        let value = generate_value(&mut rng, &fuzzer_type, &runtime_ctx.value_generation_config);

        // Should generate a TimestampNanosecond value
        match value {
            GeneratedValue::TimestampNanosecond(v) => {
                assert!(v >= 0, "Timestamp should be non-negative");
                // Check that it's a reasonable timestamp (not too far in the future)
                let max_ns = 24 * 60 * 60 * 1_000_000_000i64 * 36500; // ~100 years
                assert!(v <= max_ns, "Timestamp should be within reasonable range");
            }
            _ => panic!("Expected TimestampNanosecond value, got: {:?}", value),
        }

        // Test SQL string generation
        let sql_string = value.to_sql_string();
        assert!(
            sql_string.starts_with("'"),
            "SQL string should start with quote"
        );
        assert!(
            sql_string.ends_with("'"),
            "SQL string should end with quote"
        );
        assert!(
            sql_string.contains("-"),
            "SQL string should contain date separators"
        );
        assert!(
            sql_string.contains(":"),
            "SQL string should contain time separators"
        );

        // Test DataFusion ScalarValue conversion
        let scalar_value = value.to_scalar_value();
        assert!(
            matches!(
                scalar_value,
                datafusion::scalar::ScalarValue::TimestampNanosecond(Some(_), None)
            ),
            "Should convert to TimestampNanosecond ScalarValue"
        );
    }

    #[test]
    fn test_timestamp_type_conversions() {
        // Test that TimestampNanosecond type conversions work correctly
        let fuzzer_type = FuzzerDataType::TimestampNanosecond;

        // Test conversion to DataFusion type
        let df_type = fuzzer_type.to_datafusion_type();
        assert!(
            matches!(
                df_type,
                datafusion::arrow::datatypes::DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                    None
                )
            ),
            "Should convert to Timestamp(Nanosecond, None)"
        );

        // Test round-trip conversion
        let back_to_fuzzer = FuzzerDataType::from_datafusion_type(&df_type);
        assert_eq!(
            back_to_fuzzer,
            Some(fuzzer_type.clone()),
            "Round-trip conversion should work"
        );

        // Test properties
        assert_eq!(fuzzer_type.display_name(), "timestamp_nanosecond");
        assert_eq!(fuzzer_type.to_sql_type(), "TIMESTAMP");
        assert!(fuzzer_type.is_time());
        assert!(!fuzzer_type.is_numeric());
    }

    #[test]
    fn test_date_generation_validity() {
        // Test that date generation produces valid dates
        use crate::fuzz_context::RuntimeContext;

        let mut rng = rng_from_seed(42);
        let fuzzer_type = FuzzerDataType::Date32;

        // Use non-nullable configuration for testing
        let config = ValueGenerationConfig {
            nullable: false,
            null_probability: 0.0,
            int_range: (-100, 100),
            uint_range: (0, 200),
            float_range: (-100.0, 100.0),
        };

        // Generate multiple dates and verify they are valid
        for _ in 0..100 {
            let value = generate_value(&mut rng, &fuzzer_type, &config);

            match value {
                GeneratedValue::Date32(_) => {
                    let sql_string = value.to_sql_string();
                    // Verify the format is correct: 'YYYY-MM-DD'
                    assert!(sql_string.starts_with("'"), "Date should start with quote");
                    assert!(sql_string.ends_with("'"), "Date should end with quote");

                    // Extract the date part (remove quotes)
                    let date_part = &sql_string[1..sql_string.len() - 1];
                    let parts: Vec<&str> = date_part.split('-').collect();
                    assert_eq!(parts.len(), 3, "Date should have 3 parts: year-month-day");

                    let year: i32 = parts[0].parse().expect("Year should be parseable");
                    let month: i32 = parts[1].parse().expect("Month should be parseable");
                    let day: i32 = parts[2].parse().expect("Day should be parseable");

                    // Verify valid ranges
                    assert!(year >= 1970, "Year should be >= 1970");
                    assert!(year <= 2070, "Year should be <= 2070");
                    assert!(month >= 1, "Month should be >= 1");
                    assert!(month <= 12, "Month should be <= 12");
                    assert!(day >= 1, "Day should be >= 1");
                    assert!(day <= 31, "Day should be <= 31");

                    // Verify month-specific day limits
                    let max_days = match month {
                        2 => 29, // Allow leap year February
                        4 | 6 | 9 | 11 => 30,
                        _ => 31,
                    };
                    assert!(
                        day <= max_days,
                        "Day {} is invalid for month {}",
                        day,
                        month
                    );
                }
                _ => panic!("Expected Date32 value, got: {:?}", value),
            }
        }
    }

    #[test]
    fn test_timestamp_generation_validity() {
        // Test that timestamp generation produces valid timestamps
        use crate::fuzz_context::RuntimeContext;

        let mut rng = rng_from_seed(42);
        let fuzzer_type = FuzzerDataType::TimestampNanosecond;

        // Use non-nullable configuration for testing
        let config = ValueGenerationConfig {
            nullable: false,
            null_probability: 0.0,
            int_range: (-100, 100),
            uint_range: (0, 200),
            float_range: (-100.0, 100.0),
        };

        // Generate multiple timestamps and verify they are valid
        for _ in 0..100 {
            let value = generate_value(&mut rng, &fuzzer_type, &config);

            match value {
                GeneratedValue::TimestampNanosecond(_) => {
                    let sql_string = value.to_sql_string();
                    // Verify the format is correct: 'YYYY-MM-DD HH:MM:SS.nnnnnnnnn'
                    assert!(
                        sql_string.starts_with("'"),
                        "Timestamp should start with quote"
                    );
                    assert!(sql_string.ends_with("'"), "Timestamp should end with quote");

                    // Extract the date-time part (remove quotes)
                    let datetime_part = &sql_string[1..sql_string.len() - 1];
                    let space_parts: Vec<&str> = datetime_part.split(' ').collect();
                    assert_eq!(
                        space_parts.len(),
                        2,
                        "Timestamp should have date and time parts"
                    );

                    let date_part = space_parts[0];
                    let time_part = space_parts[1];

                    // Parse date part
                    let date_parts: Vec<&str> = date_part.split('-').collect();
                    assert_eq!(
                        date_parts.len(),
                        3,
                        "Date should have 3 parts: year-month-day"
                    );

                    let year: i32 = date_parts[0].parse().expect("Year should be parseable");
                    let month: i32 = date_parts[1].parse().expect("Month should be parseable");
                    let day: i32 = date_parts[2].parse().expect("Day should be parseable");

                    // Verify valid date ranges
                    assert!(year >= 1970, "Year should be >= 1970");
                    assert!(year <= 2070, "Year should be <= 2070");
                    assert!(month >= 1, "Month should be >= 1");
                    assert!(month <= 12, "Month should be <= 12");
                    assert!(day >= 1, "Day should be >= 1");
                    assert!(day <= 31, "Day should be <= 31");

                    // Verify month-specific day limits
                    let max_days = match month {
                        2 => 29, // Allow leap year February
                        4 | 6 | 9 | 11 => 30,
                        _ => 31,
                    };
                    assert!(
                        day <= max_days,
                        "Day {} is invalid for month {}",
                        day,
                        month
                    );

                    // Parse time part
                    let time_parts: Vec<&str> = time_part.split(':').collect();
                    assert_eq!(
                        time_parts.len(),
                        3,
                        "Time should have 3 parts: hour:minute:second.nanosecond"
                    );

                    let hour: i32 = time_parts[0].parse().expect("Hour should be parseable");
                    let minute: i32 = time_parts[1].parse().expect("Minute should be parseable");
                    let second_part = time_parts[2];

                    // Verify valid time ranges
                    assert!(hour >= 0, "Hour should be >= 0");
                    assert!(hour <= 23, "Hour should be <= 23");
                    assert!(minute >= 0, "Minute should be >= 0");
                    assert!(minute <= 59, "Minute should be <= 59");

                    // Parse seconds and nanoseconds
                    let second_parts: Vec<&str> = second_part.split('.').collect();
                    assert_eq!(
                        second_parts.len(),
                        2,
                        "Second part should have seconds and nanoseconds"
                    );

                    let second: i32 = second_parts[0].parse().expect("Second should be parseable");
                    let nanosecond: i32 = second_parts[1]
                        .parse()
                        .expect("Nanosecond should be parseable");

                    assert!(second >= 0, "Second should be >= 0");
                    assert!(second <= 59, "Second should be <= 59");
                    assert!(nanosecond >= 0, "Nanosecond should be >= 0");
                    assert!(
                        nanosecond <= 999_999_999,
                        "Nanosecond should be <= 999999999"
                    );
                }
                _ => panic!("Expected TimestampNanosecond value, got: {:?}", value),
            }
        }
    }
}
