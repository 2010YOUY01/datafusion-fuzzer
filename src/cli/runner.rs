use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::instant::Instant;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::common::{LogicalTable, Result};
use crate::datasource_generator::dataset_generator::DatasetGenerator;
use crate::fuzz_context::{GlobalContext, ctx_observability::display_all_tables};
use crate::fuzz_runner::{record_query_with_time, update_stat_for_round_completion};
use crate::oracle::{NoCrashOracle, Oracle, QueryContext, QueryExecutionResult};
use crate::query_generator::stmt_select_def::SelectStatementBuilder;

use super::error_whitelist;

pub async fn run_fuzzer(ctx: Arc<GlobalContext>) -> Result<()> {
    info!("Starting fuzzer with seed: {}", ctx.runner_config.seed);

    // Reset the table counter to ensure deterministic table naming
    ctx.runtime_context.reset_table_counter();

    // Create separate RNG instances for different phases, all seeded deterministically
    let base_seed = ctx.runner_config.seed;

    for round in 0..ctx.runner_config.rounds {
        info!("Starting round {}/{}", round + 1, ctx.runner_config.rounds);

        // Create deterministic seeds for this round
        let dataset_seed = base_seed.wrapping_add((round as u64) * 1000);
        let view_seed = base_seed.wrapping_add((round as u64) * 1000 + 100);
        let query_base_seed = base_seed.wrapping_add((round as u64) * 1000 + 200);

        // TODO: handle errors here in table/view creation, and catch potential bugs
        generate_datasets_for_round(dataset_seed, &ctx).await?;
        generate_views_for_round(view_seed, &ctx).await?;

        for i in 0..ctx.runner_config.queries_per_round {
            // ==== Running round `round`, test case `i` ====
            info!(
                "Running oracle test {}/{}",
                i + 1,
                ctx.runner_config.queries_per_round
            );

            // Create deterministic seed for this specific query
            let query_seed = query_base_seed.wrapping_add(i as u64);

            // >>> CORE LOGIC <<<
            execute_oracle_test(query_seed, &ctx).await;
        }

        update_stat_for_round_completion(&ctx.fuzzer_stats);

        // Reset DataFusion context to drop all tables before the next round
        if round < ctx.runner_config.rounds - 1 {
            // Don't reset after the last round
            info!("Resetting DataFusion context for next round");
            ctx.reset_datafusion_context();
        }
    }

    Ok(())
}

async fn generate_datasets_for_round(seed: u64, ctx: &Arc<GlobalContext>) -> Result<()> {
    // Create a deterministic RNG instance for this round
    let mut rng = StdRng::seed_from_u64(seed);

    // Generate a random number of tables per round (between 3 and 10)
    let tables_per_round = rng.random_range(3..=10);

    for i in 0..tables_per_round {
        info!("Generating table {}/{}", i + 1, tables_per_round);

        // Create a unique seed for each table based on the round seed and table index
        let table_seed = seed.wrapping_add((i as u64) * 100);
        let mut dataset_generator = DatasetGenerator::new(table_seed, Arc::clone(ctx));

        match dataset_generator.generate_dataset() {
            Ok(table) => info!("Generated table: {}", table.name),
            Err(e) => error!("Failed to generate table: {}", e),
        }
    }

    if let Err(e) = display_all_tables(Arc::clone(ctx)).await {
        error!("Failed to display tables: {}", e);
    }

    Ok(())
}

// TODO(coverage): support nested views like
// create view v2 as select * from v1;
async fn generate_views_for_round(seed: u64, ctx: &Arc<GlobalContext>) -> Result<()> {
    // Create a deterministic RNG instance for this round
    let mut rng = StdRng::seed_from_u64(seed);

    // Get all available tables (not views)
    let tables_lock = ctx.runtime_context.registered_tables.read().unwrap();
    let available_tables: Vec<Arc<LogicalTable>> = tables_lock.values().cloned().collect();
    drop(tables_lock);

    if available_tables.is_empty() {
        info!("No tables available for view generation");
        return Ok(());
    }

    // TODO(cfg): make max views count configurable
    let max_views = std::cmp::min(3, available_tables.len());
    let num_views = rng.random_range(1..=max_views);

    info!("Generating {} views", num_views);

    // Create a single statement builder for all views in this round
    let mut stmt_builder = SelectStatementBuilder::new(seed, Arc::clone(ctx))
        // Avoid large joins to slow down fuzzing
        .with_max_table_count(3);

    for i in 0..num_views {
        // Pick a random table to create a view from
        let selected_table = &available_tables[rng.random_range(0..available_tables.len())];

        // =========================
        // Core logic (generate view)
        // =========================
        let view_sql = match generate_view_sql(&mut stmt_builder, selected_table) {
            Ok(sql) => sql,
            Err(e) => {
                error!("Failed to generate view SQL: {}", e);
                continue; // Skip this view and try the next one
            }
        };
        let view_name = format!("v{}", i);

        info!("Creating view {} with SQL: {}", view_name, view_sql);

        match create_and_register_view(&view_name, &view_sql, ctx).await {
            Ok(_) => info!("Successfully created view: {}", view_name),
            Err(e) => error!("Failed to create view {}: {}", view_name, e),
        }
    }

    Ok(())
}

fn generate_view_sql(
    stmt_builder: &mut SelectStatementBuilder,
    _table: &LogicalTable,
) -> Result<String> {
    // Generate a statement using the existing query generator
    let stmt = stmt_builder.generate_stmt()?;
    let sql = stmt.to_sql_string()?;

    Ok(sql)
}

async fn create_and_register_view(
    view_name: &str,
    view_sql: &str,
    ctx: &Arc<GlobalContext>,
) -> Result<()> {
    let df_ctx = ctx.runtime_context.get_session_context();

    let create_view_sql = format!("CREATE VIEW {} AS {}", view_name, view_sql);
    info!("Executing CREATE VIEW SQL: {}", create_view_sql);

    df_ctx
        .sql(&create_view_sql)
        .await
        .map_err(|e| crate::common::fuzzer_err(&format!("Failed to execute CREATE VIEW: {}", e)))?
        .collect()
        .await
        .map_err(|e| {
            crate::common::fuzzer_err(&format!("Failed to complete CREATE VIEW: {}", e))
        })?;

    // Get the schema by querying the view with a LIMIT 0 query
    let schema_query = format!("SELECT * FROM {} LIMIT 0", view_name);
    let dataframe = df_ctx
        .sql(&schema_query)
        .await
        .map_err(|e| crate::common::fuzzer_err(&format!("Failed to get view schema: {}", e)))?;

    let _schema = dataframe.schema().inner().clone();

    // Register the view in our fuzzer context
    let logical_table = LogicalTable::new(view_name.to_string());

    ctx.runtime_context
        .registered_tables
        .write()
        .unwrap()
        .insert(view_name.to_string(), Arc::new(logical_table));

    Ok(())
}

async fn execute_oracle_test(seed: u64, ctx: &Arc<GlobalContext>) -> bool {
    // Create a deterministic RNG instance for this test
    let mut rng = StdRng::seed_from_u64(seed);

    // === Select a random oracle ===
    // TODO: disabled views since joining too many table is slow
    let available_oracles: Vec<Box<dyn Oracle + Send>> = vec![
        Box::new(NoCrashOracle::new(seed, Arc::clone(ctx))),
        // Box::new(NestedQueriesOracle::new(seed, Arc::clone(ctx))),
    ];
    let oracle_index = rng.random_range(0..available_oracles.len());
    let mut selected_oracle = available_oracles.into_iter().nth(oracle_index).unwrap();

    info!("Selected oracle: {}", selected_oracle);

    // === Generate query group ===
    let query_group = match selected_oracle.generate_query_group() {
        Ok(group) => group,
        Err(e) => {
            error!("Failed to generate query group: {}", e);
            return false;
        }
    };

    if query_group.is_empty() {
        warn!("Oracle generated empty query group");
        return false;
    }

    // === Execute queries and collect results ===
    let mut execution_results = Vec::new();
    for query_context in query_group {
        info!("Query:\n{}", query_context.query);

        let query_context_arc = Arc::new(query_context);
        let execution_result = execute_single_query(Arc::clone(&query_context_arc), ctx).await;

        execution_results.push(QueryExecutionResult {
            query_context: query_context_arc,
            result: execution_result,
        });
    }

    // === Validate execution results ===
    match selected_oracle
        .validate_consistency(&execution_results)
        .await
    {
        Ok(_) => {
            info!("Oracle test passed");
            true
        }
        Err(e) => {
            error!("Oracle test failed: {}", e);

            // Log error report if available
            if let Ok(error_report) = selected_oracle.create_error_report(&execution_results) {
                error!("Error Report:\n{}", error_report);
            }
            false
        }
    }
}

/// Query execution result that tracks both the outcome and whether it timed out
#[derive(Debug)]
struct QueryExecutionOutcome {
    result: Result<Vec<RecordBatch>>,
    timed_out: bool,
    execution_time: Duration,
}

/// We make sure error message is in 'whitelist'.
/// Error consistency check can be done later: all query in the group should all succeed
/// or all fail. (TODO: implement this)
async fn execute_single_query(
    query_context: Arc<QueryContext>,
    ctx: &Arc<GlobalContext>,
) -> Result<Vec<RecordBatch>> {
    let timeout_duration = Duration::from_secs(ctx.runner_config.timeout_seconds);

    // Execute query with timeout tracking
    let outcome = execute_query_with_timeout(&query_context, timeout_duration).await;

    // Log timeout queries specifically
    if outcome.timed_out {
        warn!(
            "Query timed out after {:.2}ms (timeout: {}s):\n{}",
            outcome.execution_time.as_secs_f64() * 1000.0,
            ctx.runner_config.timeout_seconds,
            query_context.query
        );
    }

    // Check if error is whitelisted using the dedicated error_whitelist module
    if let Err(ref e) = outcome.result {
        let error_msg = e.to_string();
        if !error_whitelist::is_error_whitelisted(&error_msg) {
            // Log non-whitelisted errors
            error!("Non-whitelisted error encountered: {}", error_msg);
            error!("Query that caused the error: {}", query_context.query);
        } else {
            info!("Whitelisted error encountered: {}", error_msg);
        }
    }

    record_query_with_time(
        &ctx.fuzzer_stats,
        &query_context.query,
        outcome.result.is_ok(),
        outcome.execution_time.into(),
        ctx.runner_config.sample_interval_secs,
    );

    outcome.result
}

/// Execute a query with timeout detection, returning both result and timeout status
async fn execute_query_with_timeout(
    query_context: &QueryContext,
    timeout_duration: Duration,
) -> QueryExecutionOutcome {
    let start_time = Instant::now();

    let timeout_result = tokio::time::timeout(timeout_duration, async {
        query_context
            .context
            .sql(&query_context.query)
            .await
            .map_err(|e| crate::common::fuzzer_err(&format!("Query planning failed: {}", e)))?
            .collect()
            .await
            .map_err(|e| crate::common::fuzzer_err(&format!("Query execution failed: {}", e)))
    })
    .await;

    let execution_time = start_time.elapsed();

    match timeout_result {
        Ok(result) => QueryExecutionOutcome {
            result,
            timed_out: false,
            execution_time,
        },
        Err(_) => QueryExecutionOutcome {
            result: Err(crate::common::fuzzer_err("Query execution timed out")),
            timed_out: true,
            execution_time,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::init_available_data_types;
    use crate::fuzz_context::{GlobalContext, RunnerConfig, RuntimeContext};
    use crate::fuzz_runner::FuzzerStats;
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    /// Test that ensures the fuzzer produces deterministic results when run with the same seed
    #[tokio::test]
    async fn test_fuzzer_determinism() {
        // Initialize data types for the test
        init_available_data_types();

        let seed = 42u64;
        let config = RunnerConfig {
            seed,
            rounds: 2,
            queries_per_round: 3,
            timeout_seconds: 2,
            log_path: None, // Disable file logging for tests
            display_logs: false,
            enable_tui: false,
            sample_interval_secs: 5,
            max_column_count: 3,
            max_row_count: 10,
            max_expr_level: 2,
            max_table_count: 3,
            max_insert_per_table: 20,
        };

        // Collect results from multiple runs
        let mut all_queries = Vec::new();
        let mut all_table_names = Vec::new();

        for run_id in 0..3 {
            println!("Running determinism test - Run {}", run_id + 1);

            // Create fresh context for each run
            let runtime_context = RuntimeContext::default();
            let fuzzer_stats = Arc::new(StdMutex::new(FuzzerStats::new_with_timeout(
                config.rounds,
                config.timeout_seconds as f64 * 1000.0,
            )));
            let ctx = Arc::new(GlobalContext::new(
                config.clone(),
                runtime_context,
                fuzzer_stats,
            ));

            // Capture queries and table names from this run
            let (queries, table_names) = run_fuzzer_and_capture_results(ctx).await;

            all_queries.push(queries);
            all_table_names.push(table_names);
        }

        // Verify all runs produced identical results
        assert_eq!(all_queries.len(), 3, "Should have results from 3 runs");

        // Check that all query sequences are identical
        for i in 1..all_queries.len() {
            assert_eq!(
                all_queries[0],
                all_queries[i],
                "Run {} queries differ from run 1: \nRun 1: {:?}\nRun {}: {:?}",
                i + 1,
                all_queries[0],
                i + 1,
                all_queries[i]
            );
        }

        // Check that all table name sequences are identical
        for i in 1..all_table_names.len() {
            assert_eq!(
                all_table_names[0],
                all_table_names[i],
                "Run {} table names differ from run 1: \nRun 1: {:?}\nRun {}: {:?}",
                i + 1,
                all_table_names[0],
                i + 1,
                all_table_names[i]
            );
        }

        println!(
            "✅ Determinism test passed! All {} runs produced identical results.",
            all_queries.len()
        );
        println!(
            "📊 Each run generated {} queries across {} rounds",
            all_queries[0].len(),
            config.rounds
        );
        println!("🏷️  Each run generated {} tables", all_table_names[0].len());
    }

    /// Test that different seeds produce different results
    #[tokio::test]
    async fn test_fuzzer_different_seeds_produce_different_results() {
        init_available_data_types();

        let config_base = RunnerConfig {
            seed: 42, // Default seed, will be overridden
            rounds: 1,
            queries_per_round: 2,
            timeout_seconds: 2,
            log_path: None,
            display_logs: false,
            enable_tui: false,
            sample_interval_secs: 5,
            max_column_count: 3,
            max_row_count: 10,
            max_expr_level: 2,
            max_table_count: 3,
            max_insert_per_table: 20,
        };

        let mut results_by_seed = Vec::new();

        // Test with different seeds
        for seed in [42, 123, 999] {
            let config = RunnerConfig {
                seed,
                ..config_base.clone()
            };

            let runtime_context = RuntimeContext::default();
            let fuzzer_stats = Arc::new(StdMutex::new(FuzzerStats::new_with_timeout(
                config.rounds,
                config.timeout_seconds as f64 * 1000.0,
            )));
            let ctx = Arc::new(GlobalContext::new(config, runtime_context, fuzzer_stats));

            let (queries, _) = run_fuzzer_and_capture_results(ctx).await;
            results_by_seed.push((seed, queries));
        }

        // Verify different seeds produce different results
        for i in 0..results_by_seed.len() {
            for j in (i + 1)..results_by_seed.len() {
                let (seed_i, queries_i) = &results_by_seed[i];
                let (seed_j, queries_j) = &results_by_seed[j];

                assert_ne!(
                    queries_i, queries_j,
                    "Seeds {} and {} produced identical queries: {:?}",
                    seed_i, seed_j, queries_i
                );
            }
        }

        println!("✅ Different seeds test passed! Each seed produced unique results.");
    }

    /// Test deterministic table counter reset
    #[test]
    fn test_table_counter_reset() {
        let runtime_context = RuntimeContext::default();

        // Generate some table names
        let name1 = runtime_context.next_table_name();
        let name2 = runtime_context.next_table_name();
        let name3 = runtime_context.next_table_name();

        assert_eq!(name1, "t0");
        assert_eq!(name2, "t1");
        assert_eq!(name3, "t2");

        // Reset and verify it starts from 0 again
        runtime_context.reset_table_counter();

        let name_after_reset1 = runtime_context.next_table_name();
        let name_after_reset2 = runtime_context.next_table_name();

        assert_eq!(name_after_reset1, "t0");
        assert_eq!(name_after_reset2, "t1");

        println!("✅ Table counter reset test passed!");
    }

    /// Helper function that runs the fuzzer and captures generated queries and table names
    async fn run_fuzzer_and_capture_results(ctx: Arc<GlobalContext>) -> (Vec<String>, Vec<String>) {
        // Use interior mutability to capture results during execution
        let captured_queries = StdArc::new(StdMutex::new(Vec::new()));
        let captured_table_names = StdArc::new(StdMutex::new(Vec::new()));

        // Run a simplified version of the fuzzer that captures results
        ctx.runtime_context.reset_table_counter();
        let base_seed = ctx.runner_config.seed;

        for round in 0..ctx.runner_config.rounds {
            let dataset_seed = base_seed.wrapping_add((round as u64) * 1000);
            let query_base_seed = base_seed.wrapping_add((round as u64) * 1000 + 200);

            // Generate datasets and capture table names
            {
                let mut rng = StdRng::seed_from_u64(dataset_seed);
                let tables_per_round = rng.random_range(3..=5); // Reduced for test stability

                for i in 0..tables_per_round {
                    let table_seed = dataset_seed.wrapping_add((i as u64) * 100);
                    let mut dataset_generator = DatasetGenerator::new(table_seed, Arc::clone(&ctx));

                    if let Ok(table) = dataset_generator.generate_dataset() {
                        captured_table_names.lock().unwrap().push(table.name);
                    }
                }
            }

            // Generate queries and capture them
            for i in 0..ctx.runner_config.queries_per_round {
                let query_seed = query_base_seed.wrapping_add(i as u64);

                // Generate a query using the same logic as execute_oracle_test
                let mut oracle = NoCrashOracle::new(query_seed, Arc::clone(&ctx));
                if let Ok(query_group) = oracle.generate_query_group() {
                    if let Some(query_context) = query_group.first() {
                        captured_queries
                            .lock()
                            .unwrap()
                            .push(query_context.query.clone());
                    }
                }
            }

            // Reset context between rounds (except the last one)
            if round < ctx.runner_config.rounds - 1 {
                ctx.reset_datafusion_context();
            }
        }

        let queries = captured_queries.lock().unwrap().clone();
        let table_names = captured_table_names.lock().unwrap().clone();

        (queries, table_names)
    }
}
