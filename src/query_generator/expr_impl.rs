use datafusion::arrow::datatypes::DataType;

use super::expr_def::{BaseExpr, BaseExprWithInfo, ExprWrapper, TypeGroup};
use crate::common::{FuzzerDataType, get_numeric_data_types};

/// To add new expressions: Add a new variant to [`BaseExpr`] and then follow the pattern along.
/// - [x] Numeric Operators: +, -, *, /, %
/// - [ ] Comparison Operators: =, !=, <, <=, >, >=, <=>, IS DISTINCT FROM, IS NOT DISTINCT FROM, ~, ~*, !~, !~*, ~~ (LIKE), ~~* (ILIKE), !~~ (NOT LIKE), !~~* (NOT ILIKE)
/// - [x] Logical Operators: AND, OR
/// - [ ] Bitwise Operators: &, |, #, >>, <<
/// - [ ] Other Operators: || (concat), @> (contains), <@ (contained by)

// ========================
// Numeric Operators
// ========================
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

pub struct MulExpr;
impl BaseExprWithInfo for MulExpr {
    fn describe(&self) -> ExprWrapper {
        let possible_return_types = get_numeric_data_types();
        let return_types: Vec<DataType> = possible_return_types
            .iter()
            .map(|ft| ft.to_datafusion_type())
            .collect();

        ExprWrapper {
            expr: BaseExpr::Mul,
            return_type: return_types,
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}

pub struct DivExpr;
impl BaseExprWithInfo for DivExpr {
    fn describe(&self) -> ExprWrapper {
        let possible_return_types = get_numeric_data_types();
        let return_types: Vec<DataType> = possible_return_types
            .iter()
            .map(|ft| ft.to_datafusion_type())
            .collect();

        ExprWrapper {
            expr: BaseExpr::Div,
            return_type: return_types,
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}

pub struct ModExpr;
impl BaseExprWithInfo for ModExpr {
    fn describe(&self) -> ExprWrapper {
        let possible_return_types = get_numeric_data_types();
        let return_types: Vec<DataType> = possible_return_types
            .iter()
            .map(|ft| ft.to_datafusion_type())
            .collect();

        ExprWrapper {
            expr: BaseExpr::Mod,
            return_type: return_types,
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}

// ========================
// Logical Operators
// ========================
pub struct AndExpr;
impl BaseExprWithInfo for AndExpr {
    fn describe(&self) -> ExprWrapper {
        let return_types = vec![FuzzerDataType::Boolean.to_datafusion_type()];

        ExprWrapper {
            expr: BaseExpr::And,
            return_type: return_types,
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}

pub struct OrExpr;
impl BaseExprWithInfo for OrExpr {
    fn describe(&self) -> ExprWrapper {
        let return_types = vec![FuzzerDataType::Boolean.to_datafusion_type()];

        ExprWrapper {
            expr: BaseExpr::Or,
            return_type: return_types,
            inferred_child_signature: vec![vec![TypeGroup::SameAsOutput, TypeGroup::SameAsOutput]],
        }
    }
}
