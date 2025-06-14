use datafusion::config::ConfigOptions;
use datafusion::prelude::SessionContext;
use datafusion_fuzzer::oracle::QueryContext;
/// Example demonstrating how to use the extended Oracle API with query-context pairs
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion_fuzzer::common::Result<()> {
    // Example 1: Configuration consistency testing
    // Test the same query across different DataFusion configurations
    let query = "SELECT COUNT(*) FROM test_table WHERE value > 100".to_string();

    // Create different contexts with various configurations
    let default_ctx = Arc::new(SessionContext::new());

    let mut batch_config = ConfigOptions::new();
    batch_config.execution.batch_size = 1024;
    let batch_ctx = Arc::new(SessionContext::new_with_config(batch_config.into()));

    let mut partition_config = ConfigOptions::new();
    partition_config.execution.target_partitions = 4;
    let partition_ctx = Arc::new(SessionContext::new_with_config(partition_config.into()));

    // Method 1: Using the convenience method
    let contexts = vec![
        (
            default_ctx.clone(),
            Some("Default Configuration".to_string()),
        ),
        (batch_ctx.clone(), Some("Batch Size 1024".to_string())),
        (
            partition_ctx.clone(),
            Some("4 Target Partitions".to_string()),
        ),
    ];

    let group1 = QueryContext::from_single_query_multiple_contexts(query.clone(), contexts);

    println!("=== Configuration Consistency Test ===");
    println!("Testing query: {}", query);
    println!("Number of configurations: {}", group1.len());
    for (i, entry) in group1.iter().enumerate() {
        println!("  {}. {}", i + 1, entry.display_description());
    }

    // Method 2: Using manual construction
    let mut group2 = Vec::new();

    group2.push(QueryContext::with_description(
        query.clone(),
        default_ctx,
        "Default Config".to_string(),
    ));

    group2.push(QueryContext::with_description(
        query.clone(),
        batch_ctx,
        "Optimized Batch Size".to_string(),
    ));

    println!("\n=== Manual Configuration Test ===");
    for entry in &group2 {
        println!("  - {}", entry.display_description());
    }

    // Example 2: Backward compatibility
    // Convert from old Vec<String> API to new Vec<QueryContext>
    let equivalent_queries = vec![
        "SELECT COUNT(*) FROM table1".to_string(),
        "SELECT COUNT(1) FROM table1".to_string(),
        "SELECT COUNT(table1.id) FROM table1".to_string(),
    ];

    let context = Arc::new(SessionContext::new());
    let backward_compatible_group = QueryContext::from_queries(equivalent_queries, context);

    println!("\n=== Backward Compatibility Example ===");
    println!("Testing equivalent queries with same context:");
    for query in QueryContext::get_queries(&backward_compatible_group) {
        println!("  - {}", query);
    }

    // Example 3: Demonstrating query-context pairs
    println!("\n=== Query-Context Pairs Example ===");
    let multi_config_group = QueryContext::from_single_query_multiple_contexts(
        "SELECT SUM(amount) FROM transactions WHERE amount > 1000".to_string(),
        vec![
            (
                Arc::new(SessionContext::new()),
                Some("Default Config".to_string()),
            ),
            (partition_ctx, Some("Partitioned Config".to_string())),
        ],
    );

    println!(
        "Generated {} query-context pairs:",
        multi_config_group.len()
    );
    for (i, entry) in multi_config_group.iter().enumerate() {
        println!("  {}. {}", i + 1, entry.display_description());
    }

    println!("\n✓ All examples demonstrate the Oracle API extensions!");
    println!("Note: To actually validate consistency, implement an Oracle trait");

    Ok(())
}
