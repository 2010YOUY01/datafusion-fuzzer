// Oracle module - provides testing oracles for query consistency and correctness

pub mod oracle_impl_nested_queries;
pub mod oracle_impl_no_crash;
pub mod oracle_trait;

// Re-export main types and traits
pub use oracle_impl_nested_queries::NestedQueriesOracle;
pub use oracle_impl_no_crash::NoCrashOracle;
pub use oracle_trait::{Oracle, QueryContext, QueryExecutionResult};
