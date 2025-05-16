use std::sync::Arc;

use datafusion::{
    arrow::{datatypes::DataType, util::pretty::pretty_format_batches},
    error::DataFusionError,
    sql::unparser::expr_to_sql,
};
use datafuzzer::{
    datasource_generator::dataset_generator::DatasetGenerator, fuzz_context::GlobalContext,
    query_generator::expr_gen::ExprGenerator,
};

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let ctx = Arc::new(GlobalContext::default());

    let mut dataset_generator = DatasetGenerator::new(6, Arc::clone(&ctx));

    let table = dataset_generator.generate_dataset()?;

    let sql = format!("SELECT * FROM {}", table.name);
    let df_ctx = ctx.runtime_context.df_ctx.clone();
    let df = df_ctx.sql(&sql).await.unwrap();
    let result = df.collect().await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());

    // ==== Testing Expr Generator ====
    let mut expr_generator = ExprGenerator::new(9, Arc::clone(&ctx));
    let expr = expr_generator.generate_random_expr(DataType::Int64, 0);
    let sql_expr = expr_to_sql(&expr)?;
    println!("{}", sql_expr);

    Ok(())
}
