use std::sync::Arc;

use datafusion::{prelude::Expr, sql::unparser::expr_to_sql};
use rand::prelude::IndexedRandom;
use rand::{Rng, RngCore, rngs::StdRng};

use crate::{
    common::{LogicalTable, Result, fuzzer_err, get_available_data_types, rng::rng_from_seed},
    fuzz_context::GlobalContext,
};

use super::expr_gen::ExprGenerator;

// ================
// Select Statement
// ================
pub struct SelectStatement {
    select_exprs: Vec<Expr>,
    from_clause: FromClause,
    where_clause: Option<Expr>,
}

impl SelectStatement {
    /// Formats the SELECT statement as a SQL string with pretty formatting
    pub fn to_sql_string(&self) -> Result<String> {
        let mut sql = String::from("SELECT ");

        if self.select_exprs.is_empty() {
            sql.push('*');
        } else {
            let expr_strings: Result<Vec<String>> = self
                .select_exprs
                .iter()
                .map(|expr| {
                    let unparsed_expr = expr_to_sql(expr)?;
                    Ok(unparsed_expr.to_string())
                })
                .collect();
            sql.push_str(&expr_strings?.join(", "));
        }

        sql.push_str("\nFROM ");

        let table_strings: Vec<String> = self
            .from_clause
            .from_list
            .iter()
            .map(|(table, alias)| {
                if let Some(alias_name) = alias {
                    format!("{} AS {}", table.name, alias_name)
                } else {
                    table.name.clone()
                }
            })
            .collect();

        sql.push_str(&table_strings.join(", "));

        // Add WHERE clause if present
        if let Some(where_expr) = &self.where_clause {
            let where_string = expr_to_sql(where_expr)?;
            sql.push_str(&format!("\nWHERE {}", where_string));
        }

        Ok(sql)
    }
}

struct FromClause {
    // vector of (table, alias)
    from_list: Vec<(LogicalTable, Option<String>)>,
}

// ================
// Select Builder
// ================
pub struct SelectStatementBuilder {
    rng: StdRng,
    ctx: Arc<GlobalContext>,

    // ==== Configuration ====
    // Configurations related to `SelectStatementBuilder`'s generation policy
    // This is possible to set by:
    // 1. Global configuration
    // 2. Oracle-specific requirements (e.g., limiting tables for view testing, and
    // there won't be large joins, the fuzzing speed can be improved)

    // Max number of tables in the `FROM` clause
    max_table_count: Option<u32>,

    // Allow using views and subqueries in the FROM clause
    allow_derived_tables: bool,

    // ==== Intermediate states to build the final select stmt ====
    src_tables: Vec<LogicalTable>,
}

impl SelectStatementBuilder {
    pub fn new(seed: u64, context: Arc<GlobalContext>) -> Self {
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
            max_table_count: None,
            allow_derived_tables: false,
            src_tables: Vec::new(),
        }
    }

    /// Override the maximum number of tables to select from
    /// If not set, uses the global configuration value
    pub fn with_max_table_count(mut self, max_table_count: u32) -> Self {
        self.max_table_count = Some(max_table_count);
        self
    }

    /// Enable or disable the use of derived tables (views and subqueries) in the FROM clause
    pub fn with_allow_derived_tables(mut self, allow_derived_tables: bool) -> Self {
        self.allow_derived_tables = allow_derived_tables;
        self
    }

    pub fn generate_stmt(&mut self) -> Result<SelectStatement> {
        // 1. Pick src tables
        self.pick_src_tables()?;

        // 2. Generate select exprs
        let expr_seed = self.rng.next_u64();
        let expr_gen = ExprGenerator::new(expr_seed, self.ctx.clone());
        let src_columns = ExprGenerator::tables_to_columns(&self.src_tables, &self.ctx);
        let mut expr_gen = expr_gen.with_src_columns(Arc::new(src_columns));

        // Build SELECT clause: generate expression list
        let select_exprs = self.generate_select_exprs(&mut expr_gen)?;

        // Build WHERE clause (optional)
        let where_clause = self.generate_where_clause(&mut expr_gen)?;

        // Build FROM clause
        Ok(SelectStatement {
            select_exprs,
            from_clause: FromClause {
                from_list: self
                    .src_tables
                    .iter()
                    .map(|table| (table.clone(), None))
                    .collect(),
            },
            where_clause,
        })
    }

    // ==== Helper functions for `generate_stmt()` ====
    pub fn pick_src_tables(&mut self) -> Result<()> {
        // TODO: Support duplicate table like `... from t1, t1 as t1_2` in the future

        // ==== Pick some unique tables and store inside builder ====
        // Use local override if available, otherwise use global config
        let cfg_max_table_count = self
            .max_table_count
            .unwrap_or(self.ctx.runner_config.max_table_count);
        let num_src_tables = self.rng.random_range(1..=cfg_max_table_count);

        // Get all available tables, filtered by allow_derived_tables setting
        let tables_lock = self.ctx.runtime_context.registered_tables.read().unwrap();
        let mut available_tables: Vec<Arc<LogicalTable>> = tables_lock.values().cloned().collect();

        // Sort tables by name to ensure deterministic ordering
        available_tables.sort_by(|a, b| a.name.cmp(&b.name));

        if available_tables.is_empty() {
            return Err(fuzzer_err(
                "No available tables registered inside fuzzer context.",
            ));
        }

        // Determine how many tables to pick (bounded by available tables)
        let num_tables = std::cmp::min(num_src_tables, available_tables.len() as u32) as usize;

        // Use sample API for more elegant random selection
        let selected_tables = available_tables
            .choose_multiple(&mut self.rng, num_tables)
            .map(|table| (**table).clone())
            .collect::<Vec<_>>();

        self.src_tables.extend(selected_tables);

        Ok(())
    }

    /// Generate a random WHERE clause expression (returns None for no WHERE clause)
    fn generate_where_clause(&mut self, expr_gen: &mut ExprGenerator) -> Result<Option<Expr>> {
        // 50% chance to generate a WHERE clause
        if self.rng.random_bool(0.9) {
            // Generate a boolean expression for the WHERE clause
            let where_expr =
                expr_gen.generate_random_expr(datafusion::arrow::datatypes::DataType::Boolean, 0);
            Ok(Some(where_expr))
        } else {
            Ok(None)
        }
    }

    /// Generate a random list of SELECT expressions
    fn generate_select_exprs(&mut self, expr_gen: &mut ExprGenerator) -> Result<Vec<Expr>> {
        let cfg_max_select_exprs = self.ctx.runner_config.max_expr_level as usize;
        let num_select_exprs = self.rng.random_range(1..=cfg_max_select_exprs);

        let available_types = get_available_data_types();
        let select_exprs = (0..num_select_exprs)
            .map(|_| {
                // Pick a random type from available types instead of hardcoded Int64
                let fuzzer_type = &available_types[self.rng.random_range(0..available_types.len())];
                let data_type = fuzzer_type.to_datafusion_type();
                expr_gen.generate_random_expr(data_type, 0)
            })
            .collect::<Vec<_>>();

        Ok(select_exprs)
    }
}
