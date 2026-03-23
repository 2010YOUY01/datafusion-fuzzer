// Oracle module - provides testing oracles for query consistency and correctness

pub mod oracle_impl_nested_queries;
pub mod oracle_impl_no_crash;
pub mod oracle_trait;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::fuzz_context::GlobalContext;

// Re-export main types and traits
pub use oracle_impl_nested_queries::NestedQueriesOracle;
pub use oracle_impl_no_crash::NoCrashOracle;
pub use oracle_trait::{Oracle, QueryContext, QueryExecutionResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfiguredOracle {
    #[serde(rename = "NoCrash", alias = "NoCrashOracle")]
    NoCrash,
    #[serde(rename = "NestedQueries", alias = "NestedQueriesOracle")]
    NestedQueries,
}

impl ConfiguredOracle {
    pub fn build(self, seed: u64, ctx: Arc<GlobalContext>) -> Box<dyn Oracle + Send> {
        match self {
            Self::NoCrash => Box::new(NoCrashOracle::new(seed, ctx)),
            Self::NestedQueries => Box::new(NestedQueriesOracle::new(seed, ctx)),
        }
    }
}
