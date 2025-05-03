use std::sync::Arc;

use datafusion::{arrow::util::pretty::pretty_format_batches, prelude::*};
use datafuzzer::{
    datasource_generator::{dataset_generator::DatasetGenerator, schema::SchemaGenerator},
    fuzz_context::GlobalContext,
    rng::rng_from_seed,
};

#[tokio::main]
async fn main() {
    let mut rnd = rng_from_seed(2);
    let ctx = Arc::new(GlobalContext::default());

    let mut schema_generator = SchemaGenerator::new(3, Arc::clone(&ctx));
    let schema = schema_generator.generate_schema();
    let schema_ref = Arc::new(schema);

    let mut dataset_generator = DatasetGenerator::new(4, Arc::clone(&ctx));
    let _ = dataset_generator.generate_dataset(schema_ref).unwrap();

    // Register t1 into context
    let table = dataset_generator.register_table().unwrap();

    let sql = format!("SELECT * FROM {}", table.name);
    let df_ctx = ctx.runtime_context.df_ctx.clone();
    let df = df_ctx.sql(&sql).await.unwrap();
    let result = df.collect().await.unwrap();
    println!("{}", pretty_format_batches(&result).unwrap());
}
