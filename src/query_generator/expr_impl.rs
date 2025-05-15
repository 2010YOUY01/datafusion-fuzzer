use datafusion::arrow::datatypes::DataType;

use super::expr_def::{BaseExpr, BaseExprWithInfo, ExprWrapper, TypeGroup};

pub struct AddExpr;
impl BaseExprWithInfo for AddExpr {
    fn describe(&self) -> ExprWrapper {
        ExprWrapper {
            expr: BaseExpr::Add,
            return_type: vec![DataType::Int64],
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}

pub struct SubExpr;
impl BaseExprWithInfo for SubExpr {
    fn describe(&self) -> ExprWrapper {
        ExprWrapper {
            expr: BaseExpr::Sub,
            return_type: vec![DataType::Int64],
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}
