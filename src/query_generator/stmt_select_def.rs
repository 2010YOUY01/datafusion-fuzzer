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

    // ==== Intermediate states to build the final select stmt ====
    src_tables: Vec<LogicalTable>,
}

impl SelectStatementBuilder {
    pub fn new(seed: u64, context: Arc<GlobalContext>) -> Self {
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
            src_tables: Vec::new(),
        }
    }

    pub fn build(&mut self) -> Result<SelectStatement> {
        // 1. Pick src tables
        self.pick_src_tables()?;

        // 2. Generate select exprs
        let expr_seed = self.rng.next_u64();
        let expr_gen = ExprGenerator::new(expr_seed, self.ctx.clone());
        let src_columns = ExprGenerator::tables_to_columns(&self.src_tables);
        let mut expr_gen = expr_gen.with_src_columns(Arc::new(src_columns));

        // Build SELECT clause: generate expression list
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
        })
    }

    // ==== Helper functions for `build()` ====
    pub fn pick_src_tables(&mut self) -> Result<()> {
        // TODO: Support duplicate table like `... from t1, t1 as t1_2` in the future

        // ==== Pick some unique tables and store inside builder ====
        let num_src_tables = self.rng.random_range(1..=3);

        // Get all available tables
        let tables_lock = self.ctx.runtime_context.registered_tables.read().unwrap();
        let available_tables: Vec<Arc<LogicalTable>> = tables_lock.values().cloned().collect();

        if available_tables.is_empty() {
            return Err(fuzzer_err(
                "No available tables regsitered inside fuzzer context.",
            ));
        }

        // Determine how many tables to pick (bounded by available tables)
        let num_tables = std::cmp::min(num_src_tables, available_tables.len());

        // Use sample API for more elegant random selection
        let selected_tables = available_tables
            .choose_multiple(&mut self.rng, num_tables)
            .map(|table| (**table).clone())
            .collect::<Vec<_>>();

        self.src_tables.extend(selected_tables);

        Ok(())
    }
}
