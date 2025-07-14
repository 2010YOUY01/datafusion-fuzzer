use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

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

    fn build_expr(&self, child_exprs: &[Expr]) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(child_exprs[0].clone()),
            Operator::Plus,
            Box::new(child_exprs[1].clone()),
        ))
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

    fn build_expr(&self, child_exprs: &[Expr]) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(child_exprs[0].clone()),
            Operator::Minus,
            Box::new(child_exprs[1].clone()),
        ))
    }
}

// TODO(confirm): I think *, /, % are not available for time types?
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

    fn build_expr(&self, child_exprs: &[Expr]) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(child_exprs[0].clone()),
            Operator::Multiply,
            Box::new(child_exprs[1].clone()),
        ))
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

    fn build_expr(&self, child_exprs: &[Expr]) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(child_exprs[0].clone()),
            Operator::Divide,
            Box::new(child_exprs[1].clone()),
        ))
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

    fn build_expr(&self, child_exprs: &[Expr]) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(child_exprs[0].clone()),
            Operator::Modulo,
            Box::new(child_exprs[1].clone()),
        ))
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

    fn build_expr(&self, child_exprs: &[Expr]) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(child_exprs[0].clone()),
            Operator::And,
            Box::new(child_exprs[1].clone()),
        ))
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

    fn build_expr(&self, child_exprs: &[Expr]) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(child_exprs[0].clone()),
            Operator::Or,
            Box::new(child_exprs[1].clone()),
        ))
    }
}
