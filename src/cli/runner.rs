use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{error, info, warn};

use crate::common::Result;
use crate::datasource_generator::dataset_generator::DatasetGenerator;
use crate::fuzz_context::{GlobalContext, ctx_observability::display_all_tables};
use crate::fuzz_runner::FuzzerRunner;
use crate::oracle::{NoCrashOracle, Oracle};

use super::FuzzerRunnerConfig;

pub async fn run_fuzzer(config: FuzzerRunnerConfig, fuzzer: Arc<FuzzerRunner>) -> Result<()> {
    info!("Starting fuzzer with seed: {}", config.seed);

    // Propagate the configs from FuzzerRunnerConfig to RunnerConfig
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

        // Create RNG for oracle selection
        let mut rng = StdRng::seed_from_u64(config.seed.wrapping_add(round as u64));

        // Run oracle tests for this round
        for i in 0..config.queries_per_round {
            info!("Running oracle test {}/{}", i + 1, config.queries_per_round);

            // Create a seed for this specific oracle test
            let oracle_seed = config.seed.wrapping_add((round as u64) * 1000 + i as u64);

            // Create vector of available oracles
            // Currently only contains NoCrashOracle, but can be extended in the future
            let available_oracles: Vec<Box<dyn Oracle + Send>> =
                vec![Box::new(NoCrashOracle::new(oracle_seed, Arc::clone(&ctx)))];

            // Randomly select an oracle (currently only one option, but ready for expansion)
            let oracle_index = rng.random_range(0..available_oracles.len());
            let mut selected_oracle = available_oracles.into_iter().nth(oracle_index).unwrap();

            info!("Selected oracle: {}", selected_oracle);

            let start = Instant::now();

            // Generate query group using the selected oracle
            match selected_oracle.generate_query_group() {
                Ok(query_group) => {
                    if !query_group.is_empty() {
                        let query = &query_group[0].query;
                        info!("Generated query: {}", query);

                        // Run the oracle validation with timeout
                        let timeout_duration = Duration::from_secs(config.timeout_seconds);
                        let success = match timeout(
                            timeout_duration,
                            selected_oracle.validate_consistency(&query_group),
                        )
                        .await
                        {
                            Ok(result) => match result {
                                Ok(_) => {
                                    let duration = start.elapsed();
                                    info!("Oracle test passed in {:?}", duration);
                                    true
                                }
                                Err(e) => {
                                    error!("Oracle test failed: {}", e);

                                    // Generate and log error report
                                    if let Ok(error_report) =
                                        selected_oracle.create_error_report(&query_group)
                                    {
                                        error!("Error Report:\n{}", error_report);
                                    }
                                    false
                                }
                            },
                            Err(_) => {
                                error!("Oracle test timed out after {:?}", timeout_duration);
                                false
                            }
                        };

                        // Record the oracle test in our stats
                        fuzzer.record_query(query, success);
                    } else {
                        warn!("Oracle generated empty query group");
                        fuzzer.record_query("", false);
                    }
                }
                Err(e) => {
                    error!("Failed to generate query group: {}", e);
                    fuzzer.record_query("", false);
                }
            }
        }

        fuzzer.complete_round();
    }

    Ok(())
}
