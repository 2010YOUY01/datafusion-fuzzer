// Oracle module - provides testing oracles for query consistency and correctness

pub(crate) mod oracle_common;
pub mod oracle_impl_nested_queries;
pub mod oracle_impl_no_crash;
pub mod oracle_impl_tlp_having;
pub mod oracle_impl_tlp_where;
pub mod oracle_trait;
pub(crate) mod tlp_shared;

// Re-export main types and traits
pub use oracle_impl_nested_queries::NestedQueriesOracle;
pub use oracle_impl_no_crash::NoCrashOracle;
pub use oracle_impl_tlp_having::TlpHavingOracle;
pub use oracle_impl_tlp_where::TlpWhereOracle;
pub use oracle_trait::{Oracle, QueryContext, QueryExecutionResult};
