use crate::common::Result;
use crate::oracle::{Oracle, QueryContext, QueryExecutionResult};
use crate::query_generator::stmt_select_def::SelectStatementBuilder;
use std::sync::Arc;

/// An oracle that generates random queries and ensures they don't crash or produce non-whitelisted errors.
/// This oracle works in conjunction with an error message whitelist system - whitelisted errors
/// (such as "divide by zero") are considered acceptable, while non-whitelisted errors indicate
/// potential stability issues in the query engine.
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
    fn name(&self) -> &'static str {
        "NoCrashOracle"
    }

    fn generate_query_group(&mut self) -> Result<Vec<QueryContext>> {
        // Generate a single random query using the existing query generator
        let mut stmt_builder = SelectStatementBuilder::new(self.seed, Arc::clone(&self.ctx))
            // Views/subqueries are tested by other oracles
            .with_allow_derived_tables(false);
        let stmt = stmt_builder.generate_stmt()?;
        let sql = stmt.to_sql_string()?;

        // Use the existing DataFusion context from the global context
        // This context already has all the tables registered from the dataset generator
        let session_context = self.ctx.runtime_context.get_session_context();

        // Create a single QueryContext for this query
        let query_context = QueryContext::with_description(
            sql,
            session_context,
            "Random Query No-Crash Test".to_string(),
        );

        Ok(vec![query_context])
    }

    async fn validate_consistency(&self, results: &[QueryExecutionResult]) -> Result<()> {
        if results.is_empty() {
            return Err(crate::common::fuzzer_err("No query results to validate"));
        }

        // For the no-crash oracle, since error message whitelist check is now done
        // outside in the runner, we always pass validation. The oracle only fails
        // for truly unexpected crashes, not for whitelisted errors.
        // The actual error checking (whitelist validation) is handled in execute_single_query.
        Ok(())
    }

    fn create_error_report(&self, results: &[QueryExecutionResult]) -> Result<String> {
        let mut report = String::new();
        report.push_str("No-Crash Oracle Test Failed\n");
        report.push_str("============================\n\n");

        if !results.is_empty() {
            let query_result = &results[0];
            let query_context = &*query_result.query_context;

            report.push_str(&format!(
                "Query that caused non-whitelisted error/crash:\n{}\n\n",
                query_context.query
            ));
            report.push_str(&format!(
                "Context: {}\n\n",
                query_context.display_description()
            ));

            // Include the specific error if available
            if let Err(e) = &query_result.result {
                report.push_str(&format!("Error details: {}\n\n", e));
            }
        }

        report.push_str(
            "Expected: Query should execute without crashing or return a whitelisted error\n",
        );
        report.push_str("Actual: Query crashed or returned a non-whitelisted error\n\n");

        report.push_str("This indicates a potential stability issue in the query engine.\n");
        report.push_str(
            "The query should either return valid results or a graceful whitelisted error message.\n",
        );
        report.push_str("Whitelisted errors are acceptable and expected (e.g., divide by zero).\n");

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
            sample_interval_secs: 5,
            max_column_count: 5,
            max_row_count: 100,
            max_expr_level: 3,
            max_table_count: 3,
        };

        let runtime_context = RuntimeContext::default();

        let ctx = Arc::new(GlobalContext {
            runner_config: config.to_runner_config(),
            runtime_context,
        });

        let _oracle = NoCrashOracle::new(42, ctx);
        // If we get here without panicking, the oracle was created successfully
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
            sample_interval_secs: 5,
            max_column_count: 5,
            max_row_count: 100,
            max_expr_level: 3,
            max_table_count: 3,
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
            sample_interval_secs: 5,
            max_column_count: 3,
            max_row_count: 10,
            max_expr_level: 2,
            max_table_count: 3,
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

        // Simulate query execution for testing
        let query_context = &query_group[0];
        let execution_result = match query_context.context.sql(&query_context.query).await {
            Ok(dataframe) => {
                match dataframe.collect().await {
                    Ok(batches) => {
                        // Return all batches or an empty vector
                        Ok(batches)
                    }
                    Err(e) => Err(crate::common::fuzzer_err(&format!(
                        "Query execution failed: {}",
                        e
                    ))),
                }
            }
            Err(e) => Err(crate::common::fuzzer_err(&format!(
                "Query planning failed: {}",
                e
            ))),
        };

        let query_execution_result = QueryExecutionResult {
            query_context: Arc::new(query_context.clone()),
            result: execution_result,
        };

        match oracle.validate_consistency(&[query_execution_result]).await {
            Ok(()) => {
                // Query executed successfully - this is good
                println!("Query executed successfully: {}", query_group[0].query);
            }
            Err(_) => {
                // Query failed - this is what the oracle is designed to catch
                let failed_result = QueryExecutionResult {
                    query_context: Arc::new(query_context.clone()),
                    result: Err(crate::common::fuzzer_err("Simulated failure")),
                };
                let error_report = oracle.create_error_report(&[failed_result]).unwrap();
                println!("Oracle caught a problematic query:");
                println!("{}", error_report);
                // Don't fail the test - finding problematic queries is the purpose of the oracle
            }
        }
    }
}
