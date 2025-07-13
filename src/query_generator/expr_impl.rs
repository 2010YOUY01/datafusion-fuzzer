use datafusion::arrow::datatypes::DataType;

use super::expr_def::{BaseExpr, BaseExprWithInfo, ExprWrapper, TypeGroup};
use crate::common::{FuzzerDataType, get_numeric_data_types, get_time_data_types};

/// To add new expressions: Add a new variant to [`BaseExpr`] and then follow the pattern along.
/// - [x] Numeric Operators: +, -, *, /, %
/// - [ ] Comparison Operators: =, !=, <, <=, >, >=, <=>, IS DISTINCT FROM, IS NOT DISTINCT FROM, ~, ~*, !~, !~*, ~~ (LIKE), ~~* (ILIKE), !~~ (NOT LIKE), !~~* (NOT ILIKE)
/// - [x] Logical Operators: AND, OR
/// - [ ] Bitwise Operators: &, |, #, >>, <<
/// - [ ] Other Operators: || (concat), @> (contains), <@ (contained by)

// The following implementation includes several simplifications:
// The generation strategy aims to produce valid expressions with best effort;
// however, allowing the generation of invalid expressions can be beneficial if
// it simplifies the implementation.
//
// For date types, not all combinations in arithmetic expressions are valid:
// - Date32 + Date32 is invalid
// - Date32 + Interval is valid
// Nevertheless, we allow generating all combinations for simplicity.

// ========================
// Numeric Operators
// ========================
pub struct AddExpr;
impl BaseExprWithInfo for AddExpr {
    fn describe(&self) -> ExprWrapper {
        // Support both numeric and time types for addition
        let mut possible_return_types = get_numeric_data_types();
        possible_return_types.extend(get_time_data_types());

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
        // Support both numeric and time types for subtraction
        let mut possible_return_types = get_numeric_data_types();
        possible_return_types.extend(get_time_data_types());

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
        // Support both numeric and time types for multiplication
        let mut possible_return_types = get_numeric_data_types();
        possible_return_types.extend(get_time_data_types());

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
        // Support both numeric and time types for division
        // TODO(coverage): investigate if time types actually support division
        let mut possible_return_types = get_numeric_data_types();
        possible_return_types.extend(get_time_data_types());

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
        // Support both numeric and time types for modulo
        // TODO(coverage): investigate if time types actually support division
        let mut possible_return_types = get_numeric_data_types();
        possible_return_types.extend(get_time_data_types());

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
