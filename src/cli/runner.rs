use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{error, info, warn};

use crate::common::Result;
use crate::datasource_generator::dataset_generator::DatasetGenerator;
use crate::fuzz_context::{GlobalContext, ctx_observability::display_all_tables};
use crate::query_generator::stmt_select_def::SelectStatementBuilder;

use super::FuzzerRunnerConfig;

pub async fn run_fuzzer(config: FuzzerRunnerConfig) -> Result<()> {
    info!("Starting fuzzer with seed: {}", config.seed);

    // Create runner config from our CLI config
    let runner_config = config.to_runner_config();

    // Create the global context
    let ctx = Arc::new(GlobalContext::new(
        runner_config,
        crate::fuzz_context::RuntimeContext::default(),
    ));

    // Run the fuzzing process for the specified number of rounds
    for round in 0..config.rounds {
        info!("Starting round {}/{}", round + 1, config.rounds);

        // Create a new dataset generator for each round
        let tables_per_round = 6; // This value is hardcoded in the main.rs example
        let mut dataset_generator = DatasetGenerator::new(tables_per_round, Arc::clone(&ctx));

        // Generate random tables for this round
        for i in 0..tables_per_round {
            info!("Generating table {}/{}", i + 1, tables_per_round);
            match dataset_generator.generate_dataset() {
                Ok(table) => info!("Generated table: {}", table.name),
                Err(e) => error!("Failed to generate table: {}", e),
            }
        }

        // Display generated tables
        if let Err(e) = display_all_tables(Arc::clone(&ctx)).await {
            error!("Failed to display tables: {}", e);
        }

        // Generate and execute queries for this round
        let timeout_duration = Duration::from_secs(config.timeout_seconds);

        for i in 0..config.queries_per_round {
            info!("Generating query {}/{}", i + 1, config.queries_per_round);

            // Generate a SQL statement with a seed derived from the global seed
            let query_seed = config.seed.wrapping_add((round as u64) * 1000 + i as u64);
            let stmt_result = SelectStatementBuilder::new(query_seed, Arc::clone(&ctx)).build();

            match stmt_result {
                Ok(stmt) => {
                    match stmt.to_sql_string() {
                        Ok(sql) => {
                            info!("Generated SQL: {}", sql);

                            // Execute the query with a timeout
                            let start = Instant::now();
                            let query_future = ctx.runtime_context.df_ctx.sql(&sql);

                            match timeout(timeout_duration, query_future).await {
                                Ok(result) => match result {
                                    Ok(_) => {
                                        let duration = start.elapsed();
                                        info!("Query executed successfully in {:?}", duration);
                                    }
                                    Err(e) => {
                                        warn!("Query execution error: {}", e);
                                    }
                                },
                                Err(_) => {
                                    error!(
                                        "Query execution timed out after {:?}",
                                        timeout_duration
                                    );
                                }
                            }
                        }
                        Err(e) => error!("Failed to convert statement to SQL: {}", e),
                    }
                }
                Err(e) => error!("Failed to build statement: {}", e),
            }
        }

        info!("Completed round {}/{}", round + 1, config.rounds);
    }

    info!("Fuzzing completed successfully");
    Ok(())
}
