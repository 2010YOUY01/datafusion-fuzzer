use datafuzzer::{
    cli::FuzzerRunnerConfig,
    common::{Result, init_available_data_types},
    datasource_generator::dataset_generator::DatasetGenerator,
    fuzz_context::{GlobalContext, RuntimeContext},
    oracle::{NoCrashOracle, Oracle},
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
    match oracle.validate_consistency(&query_group).await {
        Ok(()) => {
            println!("✅ Query executed successfully without crashing!");
            println!("The query is stable and doesn't cause any errors.");
        }
        Err(e) => {
            println!("❌ Oracle caught a problematic query!");
            println!("Error: {}", e);

            // Generate error report
            let error_report = oracle.create_error_report(&query_group)?;
            println!("\nDetailed Error Report:");
            println!("{}", error_report);
        }
    }

    Ok(())
}
