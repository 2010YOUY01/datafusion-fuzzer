use std::sync::Arc;

use datafuzzer::{
    common::Result,
    datasource_generator::dataset_generator::DatasetGenerator,
    fuzz_context::{GlobalContext, ctx_observability::display_all_tables},
    query_generator::stmt_select_def::SelectStatementBuilder,
};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = Arc::new(GlobalContext::default());

    let mut dataset_generator = DatasetGenerator::new(6, Arc::clone(&ctx));

    for _ in 0..10 {
        let table = dataset_generator.generate_dataset()?;
    }

    display_all_tables(Arc::clone(&ctx)).await?;

    for i in 0..10 {
        let stmt = SelectStatementBuilder::new(i as u64, Arc::clone(&ctx)).build()?;
        println!("Generated SQL:\n{}\n\n", stmt.to_sql_string()?);
    }

    Ok(())
}
