use datafusion::arrow::datatypes::DataType;

use super::expr_def::{BaseExpr, BaseExprWithInfo, ExprWrapper, TypeGroup};
use crate::common::get_numeric_data_types;

/// To add new expressions: Add a new variant to [`BaseExpr`] and then follow the pattern along.
/// - [ ] Numeric Operators: +, -, *, /, %
/// - [ ] Comparison Operators: =, !=, <, <=, >, >=, <=>, IS DISTINCT FROM, IS NOT DISTINCT FROM, ~, ~*, !~, !~*, ~~ (LIKE), ~~* (ILIKE), !~~ (NOT LIKE), !~~* (NOT ILIKE)
/// - [ ] Logical Operators: AND, OR
/// - [ ] Bitwise Operators: &, |, #, >>, <<
/// - [ ] Other Operators: || (concat), @> (contains), <@ (contained by)

pub struct AddExpr;
impl BaseExprWithInfo for AddExpr {
    fn describe(&self) -> ExprWrapper {
        let possible_return_types = get_numeric_data_types();
        let return_types: Vec<DataType> = possible_return_types
            .iter()
            .map(|ft| ft.to_datafusion_type())
            .collect();

        ExprWrapper {
            expr: BaseExpr::Add,
            return_type: return_types,
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}

pub struct SubExpr;
impl BaseExprWithInfo for SubExpr {
    fn describe(&self) -> ExprWrapper {
        let possible_return_types = get_numeric_data_types();
        let return_types: Vec<DataType> = possible_return_types
            .iter()
            .map(|ft| ft.to_datafusion_type())
            .collect();

        ExprWrapper {
            expr: BaseExpr::Sub,
            return_type: return_types,
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}
