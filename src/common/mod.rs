use core::fmt;
use std::io;
use std::sync::OnceLock;

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    error::DataFusionError,
};

pub mod rng;

// How to add a new data type:
// 1. Update current file
// 2. Update dataset_generator
// 3. Maybe update the `expr_impl.rs` if the new data type is supported in existing
// expressions.

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
    Decimal128 { precision: u8, scale: i8 },
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
            FuzzerDataType::Decimal128 { precision, scale } => {
                DataType::Decimal128(*precision, *scale)
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
            DataType::Decimal128(precision, scale) => Some(FuzzerDataType::Decimal128 {
                precision: *precision,
                scale: *scale,
            }),
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
            FuzzerDataType::Decimal128 { .. } => "decimal128",
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
            | FuzzerDataType::Decimal128 { .. } => true,
            FuzzerDataType::Boolean => false,
        }
    }

    /// Create a random Decimal128 type with valid precision and scale
    pub fn random_decimal128<R: rand::Rng>(rng: &mut R) -> Self {
        // Use reasonable precision and scale values for testing
        // Precision: 1-38 (maximum for Decimal128)
        let precision = rng.random_range(1..=38);
        // Scale: 0 to precision (can't exceed precision)
        let scale = rng.random_range(0..=precision as i8);

        FuzzerDataType::Decimal128 { precision, scale }
    }
}

/// All available data types for the fuzzer
static AVAILABLE_DATA_TYPES: OnceLock<Vec<FuzzerDataType>> = OnceLock::new();

/// Initialize the available data types (called once)
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
            // Add some common decimal types for testing
            FuzzerDataType::Decimal128 {
                precision: 10,
                scale: 2,
            }, // Common currency format
            FuzzerDataType::Decimal128 {
                precision: 18,
                scale: 4,
            }, // Higher precision
            FuzzerDataType::Decimal128 {
                precision: 38,
                scale: 10,
            }, // Max precision with scale
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

#[derive(Debug, Clone)]
pub struct LogicalTable {
    pub name: String,
    pub schema: SchemaRef,
    pub table_type: LogicalTableType,
}

#[derive(Debug, Clone)]
pub enum LogicalTableType {
    Table,
    View,
    Subquery(String),
}

impl LogicalTable {
    pub fn new(name: String, schema: SchemaRef, table_type: LogicalTableType) -> Self {
        Self {
            name,
            schema,
            table_type,
        }
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
