use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    common::Column,
    logical_expr::{BinaryExpr, Operator},
    prelude::Expr,
    sql::TableReference,
};
use rand::{Rng, rngs::StdRng};

use crate::{
    common::{FuzzerDataType, LogicalTable, get_available_data_types, rng::rng_from_seed},
    fuzz_context::GlobalContext,
};

use super::{
    expr_def::{BaseExpr, ExprWrapper, all_available_exprs},
    expr_literal_gen::generate_scalar_literal,
};

pub struct ExprGenerator {
    rng: StdRng,
    ctx: Arc<GlobalContext>,
    max_level: u32,

    /// All possible column references that can be used in the generated expressions.
    src_columns: Arc<Vec<Column>>,
}

impl ExprGenerator {
    pub fn new(seed: u64, context: Arc<GlobalContext>) -> Self {
        let max_level = context.runner_config.max_expr_level;
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
            max_level,
            src_columns: Arc::new(Vec::new()),
        }
    }

    pub fn with_src_columns(mut self, src_columns: Arc<Vec<Column>>) -> Self {
        self.src_columns = src_columns;
        self
    }

    /// Helper function to convert a vector of LogicalTable to a vector of Column references
    pub fn tables_to_columns(
        tables: &[Arc<LogicalTable>],
        _ctx: &Arc<GlobalContext>,
    ) -> Vec<Column> {
        let mut columns = Vec::new();

        for table in tables {
            let table_ref = TableReference::bare(table.name.clone());
            let table = table.as_ref();

            // Use the actual column information from the table
            for logical_column in &table.columns {
                columns.push(Column::new(
                    Some(table_ref.clone()),
                    logical_column.name.clone(),
                ));
            }
        }
        columns
    }

    fn pick_random_expr_with_return_type(
        &mut self,
        target_type: DataType,
    ) -> Option<Arc<ExprWrapper>> {
        let exprs = all_available_exprs();
        let expr_with_return_type: Vec<Arc<ExprWrapper>> = exprs
            .iter()
            .filter(|expr| expr.return_type.contains(&target_type))
            .map(|expr| Arc::clone(expr))
            .collect();

        // If no expressions match the target type, return None
        if expr_with_return_type.is_empty() {
            return None;
        }

        let expr =
            expr_with_return_type[self.rng.random_range(0..expr_with_return_type.len())].clone();

        Some(expr)
    }

    pub fn generate_random_expr(&mut self, target_type: DataType, cur_level: u32) -> Expr {
        let half_chance = self.rng.random_bool(0.5);
        if cur_level == self.max_level || half_chance {
            // Generate a leaf expression
            return self.generate_leaf_expr(target_type);
        }

        // Try to pick a random expression with the target return type
        if let Some(random_expr) = self.pick_random_expr_with_return_type(target_type.clone()) {
            let child_signature = random_expr.pick_child_signature(target_type, &mut self.rng);

            let child_exprs: Vec<Expr> = child_signature
                .iter()
                .map(|dt| self.generate_random_expr(dt.clone(), cur_level + 1))
                .collect();

            self.build_with_childs(random_expr.expr.clone(), &child_exprs)
        } else {
            // No expressions available for this type, fallback to leaf expression
            self.generate_leaf_expr(target_type)
        }
    }

    /// Generate a boolean expression that stays within simple, well-typed patterns.
    /// This is used by TLP oracles to avoid false positives from intentionally invalid expressions.
    pub fn generate_valid_boolean_expr(&mut self, cur_level: u32) -> Expr {
        if cur_level >= self.max_level || self.rng.random_bool(0.5) {
            return self.generate_valid_boolean_leaf();
        }

        match self.rng.random_range(0..3) {
            0 => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(self.generate_valid_boolean_expr(cur_level + 1)),
                Operator::And,
                Box::new(self.generate_valid_boolean_expr(cur_level + 1)),
            )),
            1 => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(self.generate_valid_boolean_expr(cur_level + 1)),
                Operator::Or,
                Box::new(self.generate_valid_boolean_expr(cur_level + 1)),
            )),
            _ => self.generate_valid_boolean_leaf(),
        }
    }

    // Generate either a constant value or a column reference
    fn generate_leaf_expr(&mut self, target_type: DataType) -> Expr {
        // For certain chance: try to generate a column reference if available
        let columns = self.get_all_columns_of_type(target_type.clone());
        if !columns.is_empty() && self.rng.random_bool(0.5) {
            let column = columns[self.rng.random_range(0..columns.len())].clone();
            return Expr::Column(column);
        }

        // Otherwise, generate a constant literal
        if let Some(fuzzer_type) = FuzzerDataType::from_datafusion_type(&target_type) {
            let scalar_value = generate_scalar_literal(&self.ctx, &mut self.rng, &fuzzer_type);
            Expr::Literal(scalar_value, None)
        } else {
            // Fallback to a simple boolean literal for unsupported types
            let scalar_value =
                generate_scalar_literal(&self.ctx, &mut self.rng, &FuzzerDataType::Boolean);
            Expr::Literal(scalar_value, None)
        }
    }

    fn generate_valid_boolean_leaf(&mut self) -> Expr {
        match self.rng.random_range(0..4) {
            0 => self.generate_leaf_expr(DataType::Boolean),
            1 => self.generate_valid_equality_expr(),
            2 => self.generate_valid_ordering_expr(),
            _ => self.generate_valid_like_expr(),
        }
    }

    fn generate_valid_equality_expr(&mut self) -> Expr {
        let comparable_types: Vec<DataType> = get_available_data_types()
            .iter()
            .filter(|ty| !matches!(ty, FuzzerDataType::IntervalMonthDayNano))
            .map(|ty| ty.to_datafusion_type())
            .collect();
        let data_type = comparable_types[self.rng.random_range(0..comparable_types.len())].clone();
        let left = self.generate_leaf_expr(data_type.clone());
        let right = self.generate_leaf_expr(data_type);
        let operator = match self.rng.random_range(0..2) {
            0 => Operator::Eq,
            _ => Operator::NotEq,
        };

        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), operator, Box::new(right)))
    }

    fn generate_valid_ordering_expr(&mut self) -> Expr {
        let orderable_types: Vec<DataType> = get_available_data_types()
            .iter()
            .filter(|ty| {
                ty.is_numeric()
                    || matches!(
                        ty,
                        FuzzerDataType::Date32
                            | FuzzerDataType::Time64Nanosecond
                            | FuzzerDataType::Timestamp
                    )
            })
            .map(|ty| ty.to_datafusion_type())
            .collect();
        let data_type = orderable_types[self.rng.random_range(0..orderable_types.len())].clone();
        let left = self.generate_leaf_expr(data_type.clone());
        let right = self.generate_leaf_expr(data_type);
        let operator = match self.rng.random_range(0..4) {
            0 => Operator::Lt,
            1 => Operator::LtEq,
            2 => Operator::Gt,
            _ => Operator::GtEq,
        };

        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), operator, Box::new(right)))
    }

    fn generate_valid_like_expr(&mut self) -> Expr {
        let left = self.generate_leaf_expr(DataType::Utf8);
        let right = self.generate_leaf_expr(DataType::Utf8);
        let operator = match self.rng.random_range(0..4) {
            0 => Operator::LikeMatch,
            1 => Operator::ILikeMatch,
            2 => Operator::NotLikeMatch,
            _ => Operator::NotILikeMatch,
        };

        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), operator, Box::new(right)))
    }

    fn get_all_columns_of_type(&self, target_type: DataType) -> Vec<Column> {
        // Use the actual column information from the tables
        let mut matching_columns = Vec::new();

        for column in self.src_columns.as_ref() {
            // Try to find the table this column belongs to
            if let Some(table_ref) = &column.relation {
                let table_name = table_ref.to_string();

                // Get the table from the fuzzer context
                let tables_lock = self.ctx.runtime_context.registered_tables.read().unwrap();
                if let Some(logical_table) = tables_lock.get(&table_name) {
                    // Find the matching logical column
                    for logical_column in &logical_table.columns {
                        if logical_column.name == column.name {
                            // Check if the data type matches
                            let column_df_type = logical_column.data_type.to_datafusion_type();
                            if column_df_type == target_type {
                                matching_columns.push(column.clone());
                            }
                            break;
                        }
                    }
                }
            }
        }

        matching_columns
    }

    /// If the number of childs is not correct, it will try to fix automatically.
    /// Note this function does not guarantee to return a valid expression on purpose
    /// , because invalid expression (like `true + 1`) can provide more test coverage.
    fn build_with_childs(&self, base_expr: BaseExpr, child_exprs: &[Expr]) -> Expr {
        // TODO: validate the number of `child_exprs`
        let expr_impl = base_expr.to_impl();
        expr_impl.build_expr(child_exprs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{LogicalColumn, init_available_data_types};

    #[tokio::test]
    async fn valid_boolean_exprs_execute_successfully() {
        init_available_data_types();
        let ctx = Arc::new(crate::fuzz_context::GlobalContext::default());
        let session_ctx = ctx.runtime_context.get_session_context();

        session_ctx
            .sql(
                "CREATE TABLE t0 (
                    b BOOLEAN,
                    i BIGINT,
                    f DOUBLE,
                    d DATE,
                    tm TIME,
                    ts TIMESTAMP,
                    s VARCHAR
                )",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        session_ctx
            .sql(
                "INSERT INTO t0 VALUES (
                    true,
                    1,
                    1.5,
                    CAST('2024-01-01' AS DATE),
                    CAST('12:00:00' AS TIME),
                    CAST('2024-01-01T12:00:00' AS TIMESTAMP),
                    'abc'
                )",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let logical_table = Arc::new(LogicalTable::with_columns(
            "t0".to_string(),
            vec![
                LogicalColumn {
                    name: "b".to_string(),
                    data_type: FuzzerDataType::Boolean,
                },
                LogicalColumn {
                    name: "i".to_string(),
                    data_type: FuzzerDataType::Int64,
                },
                LogicalColumn {
                    name: "f".to_string(),
                    data_type: FuzzerDataType::Float64,
                },
                LogicalColumn {
                    name: "d".to_string(),
                    data_type: FuzzerDataType::Date32,
                },
                LogicalColumn {
                    name: "tm".to_string(),
                    data_type: FuzzerDataType::Time64Nanosecond,
                },
                LogicalColumn {
                    name: "ts".to_string(),
                    data_type: FuzzerDataType::Timestamp,
                },
                LogicalColumn {
                    name: "s".to_string(),
                    data_type: FuzzerDataType::String,
                },
            ],
        ));

        ctx.runtime_context
            .registered_tables
            .write()
            .unwrap()
            .insert("t0".to_string(), Arc::clone(&logical_table));

        let src_columns = Arc::new(ExprGenerator::tables_to_columns(&[logical_table], &ctx));

        for seed in 0..32 {
            let mut expr_gen = ExprGenerator::new(seed, Arc::clone(&ctx))
                .with_src_columns(Arc::clone(&src_columns));
            let expr = expr_gen.generate_valid_boolean_expr(0);
            let expr_sql = crate::common::util::to_sql_string(&expr).unwrap();
            let query = format!("SELECT * FROM t0 WHERE {}", expr_sql);
            session_ctx
                .sql(&query)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
        }
    }
}
