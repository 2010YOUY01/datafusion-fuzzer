use datafusion::arrow::record_batch::RecordBatch;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::common::{LogicalTable, LogicalTableType, Result};
use crate::datasource_generator::dataset_generator::DatasetGenerator;
use crate::fuzz_context::{GlobalContext, ctx_observability::display_all_tables};
use crate::fuzz_runner::{record_query, update_stat_for_round_completion};
use crate::oracle::{
    NestedQueriesOracle, NoCrashOracle, Oracle, QueryContext, QueryExecutionResult,
};
use crate::query_generator::stmt_select_def::SelectStatementBuilder;

use super::error_whitelist;

pub async fn run_fuzzer(ctx: Arc<GlobalContext>) -> Result<()> {
    info!("Starting fuzzer with seed: {}", ctx.runner_config.seed);

    // Create a single RNG instance for the entire fuzzer run
    let mut rng = StdRng::seed_from_u64(ctx.runner_config.seed);

    for round in 0..ctx.runner_config.rounds {
        info!("Starting round {}/{}", round + 1, ctx.runner_config.rounds);

        // TODO: handle errors here in table/view creation, and catch potential bugs
        generate_datasets_for_round(&mut rng, &ctx).await?;
        generate_views_for_round(&mut rng, &ctx).await?;

        for i in 0..ctx.runner_config.queries_per_round {
            // ==== Running round `round`, test case `i` ====
            info!(
                "Running oracle test {}/{}",
                i + 1,
                ctx.runner_config.queries_per_round
            );

            // >>> CORE LOGIC <<<
            execute_oracle_test(&mut rng, &ctx).await;
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

async fn generate_datasets_for_round(rng: &mut StdRng, ctx: &Arc<GlobalContext>) -> Result<()> {
    // Generate a random number of tables per round (between 3 and 10)
    let tables_per_round = rng.random_range(3..=10);
    let mut dataset_generator = DatasetGenerator::new(tables_per_round, Arc::clone(ctx));

    for i in 0..tables_per_round {
        info!("Generating table {}/{}", i + 1, tables_per_round);
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

async fn generate_views_for_round(rng: &mut StdRng, ctx: &Arc<GlobalContext>) -> Result<()> {
    // Get all available tables (not views)
    let tables_lock = ctx.runtime_context.registered_tables.read().unwrap();
    let available_tables: Vec<Arc<LogicalTable>> = tables_lock
        .values()
        .filter(|table| matches!(table.table_type, LogicalTableType::Table))
        .cloned()
        .collect();
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
    let query_seed = rng.random::<u64>();
    let mut stmt_builder = SelectStatementBuilder::new(query_seed, Arc::clone(ctx))
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

    let schema = dataframe.schema().inner().clone();

    // Register the view in our fuzzer context
    let logical_table = LogicalTable::new(view_name.to_string(), schema, LogicalTableType::View);

    ctx.runtime_context
        .registered_tables
        .write()
        .unwrap()
        .insert(view_name.to_string(), Arc::new(logical_table));

    Ok(())
}

async fn execute_oracle_test(rng: &mut StdRng, ctx: &Arc<GlobalContext>) -> bool {
    // Generate oracle seed from the main RNG to maintain deterministic behavior
    let oracle_seed = rng.random::<u64>();

    // === Select a random oracle ===
    let available_oracles: Vec<Box<dyn Oracle + Send>> = vec![
        Box::new(NoCrashOracle::new(oracle_seed, Arc::clone(ctx))),
        Box::new(NestedQueriesOracle::new(oracle_seed, Arc::clone(ctx))),
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

/// We make sure error message is in 'whitelist'.
/// Error consistency check can be done later: all query in the group should all succeed
/// or all fail. (TODO: implement this)
async fn execute_single_query(
    query_context: Arc<QueryContext>,
    ctx: &Arc<GlobalContext>,
) -> Result<Vec<RecordBatch>> {
    let result: Result<Vec<RecordBatch>> = async {
        query_context
            .context
            .sql(&query_context.query)
            .await
            .map_err(|e| crate::common::fuzzer_err(&format!("Query planning failed: {}", e)))?
            .collect()
            .await
            .map_err(|e| crate::common::fuzzer_err(&format!("Query execution failed: {}", e)))
    }
    .await;

    // Check if error is whitelisted using the dedicated error_whitelist module
    if let Err(ref e) = result {
        let error_msg = e.to_string();
        if !error_whitelist::is_error_whitelisted(&error_msg) {
            // Log non-whitelisted errors
            error!("Non-whitelisted error encountered: {}", error_msg);
            error!("Query that caused the error: {}", query_context.query);
        } else {
            info!("Whitelisted error encountered: {}", error_msg);
        }
    }

    record_query(
        &ctx.fuzzer_stats,
        &query_context.query,
        result.is_ok(),
        ctx.runner_config.sample_interval_secs,
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_whitelist_functionality() {
        // Test whitelisted error using the new module
        assert!(error_whitelist::is_error_whitelisted(
            "Arrow error: Divide by zero error"
        ));
        assert!(error_whitelist::is_error_whitelisted(
            "Some other message with Arrow error: Divide by zero error in it"
        ));

        // Test non-whitelisted error
        assert!(!error_whitelist::is_error_whitelisted(
            "Some random error message"
        ));
        assert!(!error_whitelist::is_error_whitelisted(
            "Different divide by zero"
        ));
        assert!(!error_whitelist::is_error_whitelisted(""));

        // Test case sensitivity (should be case sensitive)
        assert!(!error_whitelist::is_error_whitelisted(
            "arrow error: divide by zero error"
        ));
    }
}
