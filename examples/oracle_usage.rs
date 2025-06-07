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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_context_creation() {
        let context = Arc::new(SessionContext::new());
        let query_context = QueryContext::new("SELECT 1".to_string(), context);

        assert_eq!(query_context.query, "SELECT 1");
        assert!(query_context.context_description.is_none());
    }

    #[test]
    fn test_query_context_with_description() {
        let context = Arc::new(SessionContext::new());
        let query_context = QueryContext::with_description(
            "SELECT 2".to_string(),
            context,
            "Test Context".to_string(),
        );

        assert_eq!(query_context.query, "SELECT 2");
        assert_eq!(
            query_context.context_description,
            Some("Test Context".to_string())
        );

        let description = query_context.display_description();
        assert!(description.contains("Test Context"));
        assert!(description.contains("SELECT 2"));
    }

    #[test]
    fn test_query_context_helper_functions() {
        let context = Arc::new(SessionContext::new());

        // Test from_queries
        let queries = vec!["SELECT 1".to_string(), "SELECT 2".to_string()];
        let query_contexts = QueryContext::from_queries(queries, Arc::clone(&context));
        assert_eq!(query_contexts.len(), 2);
        assert_eq!(query_contexts[0].query, "SELECT 1");
        assert_eq!(query_contexts[1].query, "SELECT 2");

        // Test get_queries
        let extracted_queries = QueryContext::get_queries(&query_contexts);
        assert_eq!(extracted_queries, vec!["SELECT 1", "SELECT 2"]);

        // Test from_single_query_multiple_contexts
        let contexts = vec![
            (Arc::clone(&context), Some("Context 1".to_string())),
            (Arc::clone(&context), Some("Context 2".to_string())),
        ];
        let multi_context_group = QueryContext::from_single_query_multiple_contexts(
            "SELECT COUNT(*)".to_string(),
            contexts,
        );
        assert_eq!(multi_context_group.len(), 2);
        assert_eq!(multi_context_group[0].query, "SELECT COUNT(*)");
        assert_eq!(multi_context_group[1].query, "SELECT COUNT(*)");
        assert!(multi_context_group[0].context_description.is_some());
        assert!(multi_context_group[1].context_description.is_some());
    }
}
