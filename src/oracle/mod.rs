pub mod oracle_impl_no_crash;
pub mod oracle_trait;

pub use oracle_impl_no_crash::NoCrashOracle;
pub use oracle_trait::{Oracle, OracleContext, QueryContext};
