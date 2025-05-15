mod expr_def;
mod expr_impl;
mod scalar_literal_gen;

use datafusion::{
    arrow::datatypes::DataType,
    error::Result,
    logical_expr::{BinaryExpr, Operator},
    prelude::Expr,
};
use expr_def::{BaseExpr, ExprWrapper, all_available_exprs};
use scalar_literal_gen::generate_scalar_literal;

use crate::{common::LogicalTable, fuzz_context::GlobalContext, rng::rng_from_seed};
use rand::{Rng, rngs::StdRng};
use std::sync::Arc;

pub struct QueryGenerator {
    rng: StdRng,
    ctx: Arc<GlobalContext>,
}

impl QueryGenerator {
    pub fn new(seed: u64, context: Arc<GlobalContext>) -> Self {
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
        }
    }

    pub fn generate_query(&mut self) -> Result<String> {
        todo!()
    }
}

// ==== Select Statement ====
struct SelectStatement {
    // ---- Intermediate states ----
    src_tables: Vec<LogicalTable>,

    // ---- Final 'select' compoments ----
    select_exprs: Vec<ExprWrapper>,
    from_clause: FromClause,
}

pub struct ExprGenerator {
    rng: StdRng,
    ctx: Arc<GlobalContext>,
    max_level: u32,
}

impl ExprGenerator {
    pub fn new(seed: u64, context: Arc<GlobalContext>) -> Self {
        let max_level = context.runner_config.max_expr_level;
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
            max_level,
        }
    }

    fn pick_random_expr_with_return_type(&mut self, target_type: DataType) -> Arc<ExprWrapper> {
        let exprs = all_available_exprs();
        let expr_with_return_type: Vec<Arc<ExprWrapper>> = exprs
            .iter()
            .filter(|expr| expr.return_type.contains(&target_type))
            .map(|expr| Arc::clone(expr))
            .collect();

        let expr =
            expr_with_return_type[self.rng.gen_range(0..expr_with_return_type.len())].clone();

        expr
    }

    pub fn generate_random_expr(&mut self, target_type: DataType, cur_level: u32) -> Expr {
        let half_chance = self.rng.gen_bool(0.5);
        if cur_level == self.max_level || half_chance {
            // Generate a leaf expression
            return self.generate_leaf_expr(target_type);
        }

        let random_expr = self.pick_random_expr_with_return_type(target_type.clone());
        let child_signature = random_expr.pick_child_signature(target_type, &mut self.rng);

        let child_exprs: Vec<Expr> = child_signature
            .iter()
            .map(|dt| self.generate_random_expr(dt.clone(), cur_level + 1))
            .collect();

        self.build_with_childs(random_expr.expr.clone(), &child_exprs)
    }

    // Generate either a constant value or a column reference
    fn generate_leaf_expr(&mut self, target_type: DataType) -> Expr {
        // TODO: generate a column ref of random type
        let scalar_value = generate_scalar_literal(&mut self.rng, target_type, true);

        Expr::Literal(scalar_value)
    }

    /// If the number of childs is not correct, it will try to fix automatically.
    /// Note this function does not guarantee to return a valid expression on purpose
    /// , because invalid expression (like `true + 1`) can provide more test coverage.
    fn build_with_childs(&self, base_expr: BaseExpr, child_exprs: &[Expr]) -> Expr {
        // TODO: validate the number of `child_exprs`
        match base_expr {
            BaseExpr::Add => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(child_exprs[0].clone()),
                Operator::Plus,
                Box::new(child_exprs[1].clone()),
            )),
            BaseExpr::Sub => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(child_exprs[0].clone()),
                Operator::Minus,
                Box::new(child_exprs[1].clone()),
            )),
        }
    }
}

struct FromClause {
    // vector of (table, alias)
    from_list: Vec<(LogicalTable, Option<String>)>,
}
