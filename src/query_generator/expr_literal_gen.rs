use datafusion::{arrow::datatypes::DataType, scalar::ScalarValue};
use rand::{Rng, rngs::StdRng};

pub fn generate_scalar_literal(
    rng: &mut StdRng,
    target_type: DataType,
    nullable: bool,
) -> ScalarValue {
    if nullable && rng.gen_bool(0.1) {
        return ScalarValue::Null;
    }

    match target_type {
        DataType::Boolean => ScalarValue::Boolean(Some(rng.gen_bool(0.5))),
        DataType::Int64 => ScalarValue::Int64(Some(rng.gen_range(-100..=100))),
        _ => unimplemented!(),
    }
}
