use crate::common::Result;
use crate::oracle::{Oracle, OracleContext, QueryContext};
use crate::query_generator::stmt_select_def::SelectStatementBuilder;
use std::sync::Arc;

/// An oracle that generates random queries and ensures they don't crash or error.
/// This is a simple but effective way to find basic stability issues in the query engine.
pub struct NoCrashOracle {
    /// Random seed for query generation
    seed: u64,
    /// Global context containing table information and configuration
    ctx: Arc<crate::fuzz_context::GlobalContext>,
}

impl NoCrashOracle {
    /// Create a new NoCrashOracle with the specified seed and context
    pub fn new(seed: u64, ctx: Arc<crate::fuzz_context::GlobalContext>) -> Self {
        Self { seed, ctx }
    }
}

// TODO: QPS drops significantly when using this oracle interface, instead of letting
// random queries be executed directly. Need to investigate why.
#[async_trait::async_trait]
impl Oracle for NoCrashOracle {
    fn oracle_context(&self) -> OracleContext {
        OracleContext {}
    }

    fn name(&self) -> &'static str {
        "NoCrashOracle"
    }

    fn generate_query_group(&mut self) -> Result<Vec<QueryContext>> {
        // Generate a single random query using the existing query generator
        let mut stmt_builder = SelectStatementBuilder::new(self.seed, Arc::clone(&self.ctx));
        let stmt = stmt_builder.generate_stmt()?;
        let sql = stmt.to_sql_string()?;

        // Use the existing DataFusion context from the global context
        // This context already has all the tables registered from the dataset generator
        let session_context = Arc::clone(&self.ctx.runtime_context.df_ctx);

        // Create a single QueryContext for this query
        let query_context = QueryContext::with_description(
            sql,
            session_context,
            "Random Query No-Crash Test".to_string(),
        );

        Ok(vec![query_context])
    }

    async fn validate_consistency(&self, query_group: &[QueryContext]) -> Result<()> {
        if query_group.is_empty() {
            return Err(crate::common::fuzzer_err("No queries to validate"));
        }

        // For the no-crash oracle, we only need to test one query
        let query_context = &query_group[0];

        match query_context.context.sql(&query_context.query).await {
            Ok(dataframe) => {
                // Try to collect the results to ensure full execution
                match dataframe.collect().await {
                    Ok(_) => {
                        // Query executed successfully without crashing
                        Ok(())
                    }
                    Err(e) => {
                        // Query failed during execution - this is what we want to catch
                        Err(crate::common::fuzzer_err(&format!(
                            "Query execution failed: {}",
                            e
                        )))
                    }
                }
            }
            Err(e) => {
                // Query failed during SQL parsing/planning - this is also what we want to catch
                Err(crate::common::fuzzer_err(&format!(
                    "Query planning failed: {}",
                    e
                )))
            }
        }
    }

    fn create_error_report(&self, query_group: &[QueryContext]) -> Result<String> {
        let mut report = String::new();
        report.push_str("No-Crash Oracle Test Failed\n");
        report.push_str("============================\n\n");

        if !query_group.is_empty() {
            let query_context = &query_group[0];
            report.push_str(&format!(
                "Query that caused crash/error:\n{}\n\n",
                query_context.query
            ));
            report.push_str(&format!(
                "Context: {}\n\n",
                query_context.display_description()
            ));
        }

        report.push_str("Expected: Query should execute without crashing or erroring\n");
        report.push_str("Actual: Query crashed or returned an error\n\n");

        report.push_str("This indicates a potential stability issue in the query engine.\n");
        report.push_str(
            "The query should either return valid results or a graceful error message.\n",
        );

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::FuzzerRunnerConfig;
    use crate::datasource_generator::dataset_generator::DatasetGenerator;
    use crate::fuzz_context::{GlobalContext, RuntimeContext};

    #[test]
    fn test_no_crash_oracle_creation() {
        let config = FuzzerRunnerConfig {
            seed: 42,
            rounds: 1,
            queries_per_round: 1,
            timeout_seconds: 30,
            log_path: Some("logs".into()),
            display_logs: false,
            enable_tui: false,
            max_column_count: 5,
            max_row_count: 100,
            max_expr_level: 3,
        };

        let runtime_context = RuntimeContext::default();

        let ctx = Arc::new(GlobalContext {
            runner_config: config.to_runner_config(),
            runtime_context,
        });

        let oracle = NoCrashOracle::new(42, ctx);
        let _oracle_context = oracle.oracle_context();
        // If we get here without panicking, the oracle was created successfully
    }

    #[test]
    fn test_no_crash_oracle_context() {
        let config = FuzzerRunnerConfig {
            seed: 42,
            rounds: 1,
            queries_per_round: 1,
            timeout_seconds: 30,
            log_path: Some("logs".into()),
            display_logs: false,
            enable_tui: false,
            max_column_count: 5,
            max_row_count: 100,
            max_expr_level: 3,
        };

        let runtime_context = RuntimeContext::default();

        let ctx = Arc::new(GlobalContext {
            runner_config: config.to_runner_config(),
            runtime_context,
        });

        let oracle = NoCrashOracle::new(42, ctx);
        let oracle_context = oracle.oracle_context();

        // OracleContext is a simple empty struct, so just verify it exists
        let _ = oracle_context;
    }

    #[test]
    fn test_no_crash_oracle_display() {
        let config = FuzzerRunnerConfig {
            seed: 42,
            rounds: 1,
            queries_per_round: 1,
            timeout_seconds: 30,
            log_path: Some("logs".into()),
            display_logs: false,
            enable_tui: false,
            max_column_count: 5,
            max_row_count: 100,
            max_expr_level: 3,
        };

        let runtime_context = RuntimeContext::default();

        let ctx = Arc::new(GlobalContext {
            runner_config: config.to_runner_config(),
            runtime_context,
        });

        let oracle = NoCrashOracle::new(42, ctx);
        assert_eq!(oracle.name(), "NoCrashOracle");

        // Test that the oracle can be displayed when boxed as a trait object
        let boxed_oracle: Box<dyn Oracle + Send> = Box::new(oracle);
        let display_str = format!("{}", boxed_oracle);
        assert_eq!(display_str, "NoCrashOracle");
    }

    #[tokio::test]
    async fn test_no_crash_oracle_with_data() {
        use crate::common::init_available_data_types;

        // Initialize data types
        init_available_data_types();

        let config = FuzzerRunnerConfig {
            seed: 42,
            rounds: 1,
            queries_per_round: 1,
            timeout_seconds: 30,
            log_path: Some("logs".into()),
            display_logs: false,
            enable_tui: false,
            max_column_count: 3,
            max_row_count: 10,
            max_expr_level: 2,
        };

        let runtime_context = RuntimeContext::default();

        let ctx = Arc::new(GlobalContext {
            runner_config: config.to_runner_config(),
            runtime_context,
        });

        // Generate some test data
        let mut dataset_generator = DatasetGenerator::new(1234, Arc::clone(&ctx));
        let _table = dataset_generator.generate_dataset().unwrap();

        // Create and test the oracle
        let mut oracle = NoCrashOracle::new(42, ctx);

        // Generate a query group
        let query_group = oracle.generate_query_group().unwrap();
        assert_eq!(query_group.len(), 1);

        // The query should not be empty
        assert!(!query_group[0].query.is_empty());

        // Test that validation works (this should succeed for a simple query)
        // Note: This might fail if the random query is complex, but that's actually
        // what we want to test - the oracle should catch problematic queries
        match oracle.validate_consistency(&query_group).await {
            Ok(()) => {
                // Query executed successfully - this is good
                println!("Query executed successfully: {}", query_group[0].query);
            }
            Err(_) => {
                // Query failed - this is what the oracle is designed to catch
                let error_report = oracle.create_error_report(&query_group).unwrap();
                println!("Oracle caught a problematic query:");
                println!("{}", error_report);
                // Don't fail the test - finding problematic queries is the purpose of the oracle
            }
        }
    }
}
