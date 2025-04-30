use std::sync::Arc;

use datafusion::{arrow::util::pretty::pretty_format_batches, prelude::*};
use datafuzzer::{
    fuzz_context::GlobalContext,
    rng::rng_from_seed,
    table_generator::{dataset_generator::DatasetGenerator, schema::SchemaGenerator},
};

#[tokio::main]
async fn main() {
    let mut rnd = rng_from_seed(2);
    let ctx = Arc::new(GlobalContext::default());

    let mut schema_generator = SchemaGenerator::new(3, Arc::clone(&ctx));
    let schema = schema_generator.generate_schema();
    let schema_ref = Arc::new(schema);

    let mut dataset_generator = DatasetGenerator::new(4, Arc::clone(&ctx));
    let dataset = dataset_generator.generate_dataset(schema_ref).unwrap();

    println!("{}", pretty_format_batches(&[dataset]).unwrap());
}
