use datafusion_fuzzer::{
    cli::FuzzerRunnerConfig,
    common::{Result, init_available_data_types},
    datasource_generator::dataset_generator::DatasetGenerator,
    fuzz_context::{GlobalContext, RuntimeContext},
    oracle::{NoCrashOracle, Oracle, QueryExecutionResult},
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize data types
    init_available_data_types();

    println!("No-Crash Oracle Example");
    println!("=======================");

    // Create configuration
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
        max_table_count: 3,
    };

    // Create global context
    let runtime_context = RuntimeContext::default();
    let ctx = Arc::new(GlobalContext {
        runner_config: config.to_runner_config(),
        runtime_context,
    });

    // Generate some test data
    println!("Generating test data...");
    let mut dataset_generator = DatasetGenerator::new(1234, Arc::clone(&ctx));
    let table = dataset_generator.generate_dataset()?;
    println!("Generated table: {}", table.name);

    // Create the oracle
    let mut oracle = NoCrashOracle::new(42, ctx);

    // Generate a query group
    println!("\nGenerating random query...");
    let query_group = oracle.generate_query_group()?;
    let query = &query_group[0].query;
    println!("Generated query: {}", query);

    // Test the query
    println!("\nTesting query execution...");

    // Execute the query and create QueryExecutionResult
    let query_context = &query_group[0];
    let execution_result = match query_context.context.sql(&query_context.query).await {
        Ok(dataframe) => {
            match dataframe.collect().await {
                Ok(batches) => {
                    // Return all batches
                    Ok(batches)
                }
                Err(e) => Err(datafusion_fuzzer::common::fuzzer_err(&format!(
                    "Query execution failed: {}",
                    e
                ))),
            }
        }
        Err(e) => Err(datafusion_fuzzer::common::fuzzer_err(&format!(
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
            println!("✅ Query executed successfully without crashing!");
            println!("The query is stable and doesn't cause any errors.");
        }
        Err(e) => {
            println!("❌ Oracle caught a problematic query!");
            println!("Error: {}", e);

            // Generate error report with failed result
            let failed_result = QueryExecutionResult {
                query_context: Arc::new(query_context.clone()),
                result: Err(datafusion_fuzzer::common::fuzzer_err("Simulated failure")),
            };
            let error_report = oracle.create_error_report(&[failed_result])?;
            println!("\nDetailed Error Report:");
            println!("{}", error_report);
        }
    }

    Ok(())
}
