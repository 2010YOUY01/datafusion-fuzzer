use std::sync::Arc;

use datafusion::{arrow::util::pretty::pretty_format_batches, error::Result};

use super::GlobalContext;

/// Display the contents of all registered tables, showing up to 3 rows each
pub async fn display_all_tables(ctx: Arc<GlobalContext>) -> Result<()> {
    let tables = ctx.runtime_context.registered_tables.read().unwrap();

    for (table_name, _) in tables.iter() {
        let sql = format!("SELECT * FROM {} LIMIT 3", table_name);
        let df_ctx = ctx.runtime_context.df_ctx.clone();

        println!("=== Table: {} ===", table_name);

        match df_ctx.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => match pretty_format_batches(&batches) {
                    Ok(formatted) => println!("{}", formatted),
                    Err(e) => println!("Error formatting results: {}", e),
                },
                Err(e) => println!("Error collecting results: {}", e),
            },
            Err(e) => println!("Error executing query: {}", e),
        }

        println!();
    }

    Ok(())
}
