use core::fmt;
use std::io;
use std::sync::OnceLock;

use datafusion::{arrow::datatypes::DataType, error::DataFusionError};

pub mod rng;
pub mod value_generator;

// How to add a new data type:
// 1. Add enum variant and update all match statements
//    --> src/common/mod.rs
// 2. Add SQL value generation
//    --> src/datasource_generator/dataset_generator.rs
// 3. Add scalar literal generation
//    --> src/query_generator/expr_literal_gen.rs
// 4. Add new operators (if needed)
//    --> src/query_generator/expr_impl.rs
// 5. Add operator enum variants (if needed)
//    --> src/query_generator/expr_def.rs
// 6. Add expression building (if needed)
//    --> src/query_generator/expr_gen.rs
// 7. Add error patterns (if needed)
//    --> src/cli/error_whitelist.rs

/// Make it easier to manage supported DataFusion data types.
/// I can't remember why I added this indrection...
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FuzzerDataType {
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    // When precision is [1, 38], the physical type in DF is Decimal128.
    // When precision is [39, 76], the physical type in DF is Decimal256.
    Decimal { precision: u8, scale: i8 },
    Date32,
    // Time64 with nanosecond precision, following DataFusion specification
    Time64Nanosecond,
    // Timestamp with nanosecond precision and no timezone
    TimestampNanosecond,
}

impl FuzzerDataType {
    /// Convert fuzzer data type to DataFusion data type
    pub fn to_datafusion_type(&self) -> DataType {
        match self {
            FuzzerDataType::Int32 => DataType::Int32,
            FuzzerDataType::Int64 => DataType::Int64,
            FuzzerDataType::UInt32 => DataType::UInt32,
            FuzzerDataType::UInt64 => DataType::UInt64,
            FuzzerDataType::Float32 => DataType::Float32,
            FuzzerDataType::Float64 => DataType::Float64,
            FuzzerDataType::Boolean => DataType::Boolean,
            FuzzerDataType::Decimal { precision, scale } => {
                // DataFusion automatically chooses the best internal representation:
                // - Decimal128 for precision 1-38
                // - Decimal256 for precision 39-76
                if *precision <= 38 {
                    DataType::Decimal128(*precision, *scale)
                } else {
                    DataType::Decimal256(*precision, *scale)
                }
            }
            FuzzerDataType::Date32 => DataType::Date32,
            FuzzerDataType::Time64Nanosecond => {
                DataType::Time64(datafusion::arrow::datatypes::TimeUnit::Nanosecond)
            }
            FuzzerDataType::TimestampNanosecond => {
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
            }
        }
    }

    /// Convert DataFusion data type to fuzzer data type (if supported)
    pub fn from_datafusion_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Int32 => Some(FuzzerDataType::Int32),
            DataType::Int64 => Some(FuzzerDataType::Int64),
            DataType::UInt32 => Some(FuzzerDataType::UInt32),
            DataType::UInt64 => Some(FuzzerDataType::UInt64),
            DataType::Float32 => Some(FuzzerDataType::Float32),
            DataType::Float64 => Some(FuzzerDataType::Float64),
            DataType::Boolean => Some(FuzzerDataType::Boolean),
            // Handle both Decimal128 and Decimal256 as the same fuzzer type
            // DataFusion automatically chooses the appropriate internal representation
            DataType::Decimal128(precision, scale) => Some(FuzzerDataType::Decimal {
                precision: *precision,
                scale: *scale,
            }),
            DataType::Decimal256(precision, scale) => Some(FuzzerDataType::Decimal {
                precision: *precision,
                scale: *scale,
            }),
            DataType::Date32 => Some(FuzzerDataType::Date32),
            DataType::Time64(datafusion::arrow::datatypes::TimeUnit::Nanosecond) => {
                Some(FuzzerDataType::Time64Nanosecond)
            }
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None) => {
                Some(FuzzerDataType::TimestampNanosecond)
            }
            _ => None,
        }
    }

    /// Get the display name for column naming
    pub fn display_name(&self) -> &'static str {
        match self {
            FuzzerDataType::Int32 => "int32",
            FuzzerDataType::Int64 => "int64",
            FuzzerDataType::UInt32 => "uint32",
            FuzzerDataType::UInt64 => "uint64",
            FuzzerDataType::Float32 => "float32",
            FuzzerDataType::Float64 => "float64",
            FuzzerDataType::Boolean => "boolean",
            FuzzerDataType::Decimal { .. } => "decimal128",
            FuzzerDataType::Date32 => "date32",
            FuzzerDataType::Time64Nanosecond => "time64_nanosecond",
            FuzzerDataType::TimestampNanosecond => "timestamp_nanosecond",
        }
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            FuzzerDataType::Int32
            | FuzzerDataType::Int64
            | FuzzerDataType::UInt32
            | FuzzerDataType::UInt64
            | FuzzerDataType::Float32
            | FuzzerDataType::Float64
            | FuzzerDataType::Decimal { .. } => true,
            FuzzerDataType::Boolean => false,
            FuzzerDataType::Date32 => false,
            FuzzerDataType::Time64Nanosecond => false,
            FuzzerDataType::TimestampNanosecond => false,
        }
    }

    pub fn is_time(&self) -> bool {
        match self {
            FuzzerDataType::Date32 => true,
            FuzzerDataType::Time64Nanosecond => true,
            FuzzerDataType::TimestampNanosecond => true,
            FuzzerDataType::Int32
            | FuzzerDataType::Int64
            | FuzzerDataType::UInt32
            | FuzzerDataType::UInt64
            | FuzzerDataType::Float32
            | FuzzerDataType::Float64
            | FuzzerDataType::Boolean
            | FuzzerDataType::Decimal { .. } => false,
        }
    }

    /// Create a random Decimal128 type with valid precision and scale
    pub fn random_decimal128<R: rand::Rng>(rng: &mut R) -> Self {
        // Use reasonable precision and scale values for testing
        // Precision: 1-76 (maximum for Decimal256, DataFusion auto-chooses implementation)
        let precision = rng.random_range(1..=76);
        // Scale: 0 to precision (can't exceed precision)
        let scale = rng.random_range(0..=precision as i8);

        FuzzerDataType::Decimal { precision, scale }
    }

    /// Convert to SQL type string for CREATE TABLE statements
    pub fn to_sql_type(&self) -> &'static str {
        match self {
            FuzzerDataType::Int32 => "INT",
            FuzzerDataType::Int64 => "BIGINT",
            FuzzerDataType::UInt32 => "INT UNSIGNED",
            FuzzerDataType::UInt64 => "BIGINT UNSIGNED",
            FuzzerDataType::Float32 => "FLOAT",
            FuzzerDataType::Float64 => "DOUBLE",
            FuzzerDataType::Boolean => "BOOLEAN",
            FuzzerDataType::Decimal {
                precision: _,
                scale: _,
            } => {
                // Note: This is a simplified approach. In a real implementation,
                // you might want to cache these strings or use a more sophisticated approach
                // For now, we'll use a default DECIMAL type
                "DECIMAL"
            }
            FuzzerDataType::Date32 => "DATE",
            FuzzerDataType::Time64Nanosecond => "TIME",
            FuzzerDataType::TimestampNanosecond => "TIMESTAMP",
        }
    }
}

/// All available data types for the fuzzer
static AVAILABLE_DATA_TYPES: OnceLock<Vec<FuzzerDataType>> = OnceLock::new();

/// Initialize the available data types (called once)
// TODO(known-bug): Generate Decimal 256 after the upstream issue addressed
// https://github.com/apache/datafusion/issues/16689
pub fn init_available_data_types() {
    AVAILABLE_DATA_TYPES.get_or_init(|| {
        vec![
            FuzzerDataType::Int32,
            FuzzerDataType::Int64,
            FuzzerDataType::UInt32,
            FuzzerDataType::UInt64,
            FuzzerDataType::Float32,
            FuzzerDataType::Float64,
            FuzzerDataType::Boolean,
            // Add decimal types with various precisions for testing
            // These will automatically use Decimal128 or Decimal256 internally
            FuzzerDataType::Decimal {
                precision: 10,
                scale: 2,
            }, // Common currency format (Decimal128)
            FuzzerDataType::Decimal {
                precision: 18,
                scale: 4,
            }, // Higher precision (Decimal128)
            FuzzerDataType::Decimal {
                precision: 38,
                scale: 10,
            }, // Max Decimal128 precision
            // Note: Decimal256 types (precision > 38) currently cause casting issues in DataFusion
            // They will be re-enabled once the upstream casting bugs are fixed
            FuzzerDataType::Date32,
            FuzzerDataType::Time64Nanosecond,
            FuzzerDataType::TimestampNanosecond,
        ]
    });
}

/// Get all available data types
pub fn get_available_data_types() -> &'static Vec<FuzzerDataType> {
    AVAILABLE_DATA_TYPES
        .get()
        .expect("Available data types not initialized. Call init_available_data_types() first.")
}

/// Get all numeric data types (excludes Boolean)
pub fn get_numeric_data_types() -> Vec<FuzzerDataType> {
    get_available_data_types()
        .iter()
        .filter(|data_type| data_type.is_numeric())
        .cloned()
        .collect()
}

/// Get all time data types (Date32, and future time types)
pub fn get_time_data_types() -> Vec<FuzzerDataType> {
    get_available_data_types()
        .iter()
        .filter(|data_type| data_type.is_time())
        .cloned()
        .collect()
}

#[derive(Debug, Clone)]
pub struct LogicalTable {
    pub name: String,
    pub columns: Vec<LogicalColumn>,
}

#[derive(Debug, Clone)]
pub struct LogicalColumn {
    pub name: String,
    pub data_type: FuzzerDataType,
}

#[derive(Debug, Clone)]
pub enum LogicalTableType {
    Table,
    View,
    Subquery(String),
}

impl LogicalTable {
    pub fn new(name: String) -> Self {
        Self {
            name,
            columns: Vec::new(),
        }
    }

    pub fn with_columns(name: String, columns: Vec<LogicalColumn>) -> Self {
        Self { name, columns }
    }
}

pub type Result<T = ()> = std::result::Result<T, FuzzerError>;

// ====
// Fuzzer Errors
// ====
#[derive(Debug)]
pub enum FuzzerError {
    FuzzerError(String),
    DataFusionError(DataFusionError),
    IoError(io::Error),
    // Add other error types as needed
}

impl std::error::Error for FuzzerError {}

impl fmt::Display for FuzzerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FuzzerError::FuzzerError(msg) => write!(f, "{}", msg),
            FuzzerError::DataFusionError(e) => write!(f, "DataFusion error: {}", e),
            FuzzerError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

// From conversion to allow ? operator with DataFusionError
impl From<DataFusionError> for FuzzerError {
    fn from(error: DataFusionError) -> Self {
        FuzzerError::DataFusionError(error)
    }
}

// From conversion to allow ? operator with io::Error
impl From<io::Error> for FuzzerError {
    fn from(error: io::Error) -> Self {
        FuzzerError::IoError(error)
    }
}

// Helper functions to create FuzzerError easily
pub fn fuzzer_err(msg: &str) -> FuzzerError {
    FuzzerError::FuzzerError(msg.to_string())
}
