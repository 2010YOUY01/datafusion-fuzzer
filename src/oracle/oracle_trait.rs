use crate::common::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// A trait for test oracles
/// Example of an oracle:
///
/// ```text
/// # NoREC consistency check oracle
///     Randomly generated query(Q1):
///         select * from t1 where v1 > 0;
///     Mutated query(Q2):
///         select v1 > 0 from t1;
///     Consistency check:
///         result size of Q1 should be equal to the number of `True` in Q2's output
/// ```
///
/// # Extended Design
/// The oracle now supports query-context pairs to enable testing the same query
/// under different DataFusion configurations. For example:
///
/// ```text
/// # Configuration consistency check oracle
///     Query with Default Config:
///         (select sum(v1) from t1, default_session_context)
///     Query with Different Config:
///         (select sum(v1) from t1, optimized_session_context)
///     Consistency check:
///         Both queries should return the same result
/// ```
#[async_trait::async_trait]
pub trait Oracle {
    fn oracle_context(&self) -> OracleContext;

    /// Return the name of this oracle for display purposes
    fn name(&self) -> &'static str;

    /// Generate a group of equivalent query-context pairs to compare against each other
    fn generate_query_group(&mut self) -> Result<Vec<QueryContext>>;

    /// Validate the consistency of a group of query-context pairs
    /// # Returns
    /// * `Ok(())` - Query-context pairs are consistent
    /// * `Err(e)` - Query-context pairs are inconsistent, and `e` is the error message
    async fn validate_consistency(&self, query_group: &[QueryContext]) -> Result<()>;

    /// After one test run failed in `validate_consistency`, this function will be called
    /// to create a detailed error report.
    fn create_error_report(&self, query_group: &[QueryContext]) -> Result<String>;
}

impl std::fmt::Display for dyn Oracle + Send {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// A query-context pair that represents a SQL query along with its execution context
#[derive(Clone)]
pub struct QueryContext {
    /// The SQL query string
    pub query: String,
    /// The DataFusion session context with specific configuration
    pub context: Arc<SessionContext>,
    /// Optional description of the context configuration for debugging
    pub context_description: Option<String>,
}

impl QueryContext {
    /// Create a new QueryContext with a query and session context
    pub fn new(query: String, context: Arc<SessionContext>) -> Self {
        Self {
            query,
            context,
            context_description: None,
        }
    }

    /// Create a new QueryContext with a description of the context
    pub fn with_description(
        query: String,
        context: Arc<SessionContext>,
        description: String,
    ) -> Self {
        Self {
            query,
            context,
            context_description: Some(description),
        }
    }

    /// Get a display-friendly description of this query-context pair
    pub fn display_description(&self) -> String {
        match &self.context_description {
            Some(desc) => format!("Query with {}: {}", desc, self.query),
            None => format!("Query: {}", self.query),
        }
    }
}

/// Helper functions for working with Vec<QueryContext>
impl QueryContext {
    /// Create a Vec<QueryContext> from a list of queries using the same context
    /// This provides backward compatibility with the old Vec<String> API
    pub fn from_queries(queries: Vec<String>, context: Arc<SessionContext>) -> Vec<QueryContext> {
        queries
            .into_iter()
            .map(|query| QueryContext::new(query, Arc::clone(&context)))
            .collect()
    }

    /// Create a Vec<QueryContext> from a single query tested with multiple contexts
    /// This is useful for configuration consistency testing
    pub fn from_single_query_multiple_contexts(
        query: String,
        contexts: Vec<(Arc<SessionContext>, Option<String>)>,
    ) -> Vec<QueryContext> {
        contexts
            .into_iter()
            .map(|(context, description)| match description {
                Some(desc) => QueryContext::with_description(query.clone(), context, desc),
                None => QueryContext::new(query.clone(), context),
            })
            .collect()
    }

    /// Get all queries from a Vec<QueryContext> (for backward compatibility)
    pub fn get_queries(query_contexts: &[QueryContext]) -> Vec<String> {
        query_contexts
            .iter()
            .map(|entry| entry.query.clone())
            .collect()
    }
}

/// Context for each oracle check. Used to generate reproducer/bug report
pub struct OracleContext {}
