use std::sync::Arc;

// use datafusion::sqlparser::ast;
use datafusion::{arrow::datatypes::DataType, prelude::Expr};
// Removed unused import: IndexedRandom
use rand::{Rng, RngCore, rngs::StdRng, seq::SliceRandom};

use crate::{
    common::{
        InclusionConfig, LogicalTable, Result, fuzzer_err, get_available_data_types,
        rng::rng_from_seed,
    },
    fuzz_context::GlobalContext,
};

use super::expr_gen::ExprGenerator;
use super::stmt_select_join::{JoinClause, JoinType};

// ================
// Select Statement
// ================
pub struct SelectStatement {
    select_exprs: Vec<Expr>,
    from_clause: FromClause,
    /// Empty vector means no JOIN clauses
    join_clauses: Vec<Arc<JoinClause>>,
    /// None means no WHERE clause
    where_clause: Option<Expr>,
}

impl SelectStatement {
    fn format_from_tables_sql(&self) -> String {
        self.from_clause
            .from_list
            .iter()
            .map(|(table, alias)| {
                if let Some(alias_name) = alias {
                    format!("{} AS {}", table.name, alias_name)
                } else {
                    table.name.clone()
                }
            })
            .collect::<Vec<String>>()
            .join(", ")
    }

    /// Formats the FROM and optional JOIN section as SQL.
    pub fn to_from_join_sql(&self) -> Result<String> {
        let mut sql = format!("FROM {}", self.format_from_tables_sql());
        for join_clause in &self.join_clauses {
            let join_string = join_clause.to_sql_string()?;
            sql.push_str(&format!("\n{}", join_string));
        }
        Ok(sql)
    }

    /// Returns the WHERE expression if one was generated.
    pub fn where_expr(&self) -> Option<&Expr> {
        self.where_clause.as_ref()
    }

    /// Formats the SELECT statement as a SQL string with pretty formatting
    pub fn to_sql_string(&self) -> Result<String> {
        // ==== SELECT clause ====
        let mut sql = String::from("SELECT ");

        if self.select_exprs.is_empty() {
            sql.push('*');
        } else {
            let expr_strings: Result<Vec<String>> = self
                .select_exprs
                .iter()
                .map(|expr| {
                    let unparsed_expr = crate::common::util::to_sql_string(expr)?;
                    Ok(unparsed_expr)
                })
                .collect();
            sql.push_str(&expr_strings?.join(", "));
        }

        // ==== FROM/JOIN clauses ====
        sql.push('\n');
        sql.push_str(&self.to_from_join_sql()?);

        // ==== WHERE clause ====
        // Add WHERE clause if present
        if let Some(where_expr) = &self.where_clause {
            let where_string = crate::common::util::to_sql_string(where_expr)?;
            sql.push_str(&format!("\nWHERE {}", where_string));
        }

        Ok(sql)
    }
}

struct FromClause {
    // vector of (table, alias)
    from_list: Vec<(Arc<LogicalTable>, Option<String>)>,
}

// ================
// Select Builder
// ================

/// Generates SELECT statement:
///
/// SELECT (* | select_expr [, ...])
/// [ FROM from_table [, ...] ]
/// [ JOIN_KEYWORD join_table ON join_on_expr ]
/// [ WHERE where_expr ]
///
/// JOIN_KEYWORD := JOIN | INNER JOIN | LEFT JOIN | RIGHT JOIN | FULL JOIN | LEFT ANTI JOIN | LEFT SEMI JOIN | RIGHT ANTI JOIN | RIGHT SEMI JOIN | CROSS JOIN
pub struct SelectStatementBuilder {
    rng: StdRng,
    ctx: Arc<GlobalContext>,

    // ==== Configuration ====
    // Configurations related to `SelectStatementBuilder`'s generation policy
    // This is possible to set by:
    // 1. Global configuration
    // 2. Oracle-specific requirements (e.g., limiting tables for view testing, and
    // there won't be large joins, the fuzzing speed can be improved)
    /// Max number of tables in the `FROM` clause
    max_table_count: Option<u32>,

    // Allow using views and subqueries in the FROM clause
    allow_derived_tables: bool,

    // ---- SQL Features Configurations ----
    enable_where_clause: InclusionConfig,
    /// Control whether JOIN clauses are generated
    enable_join_clause: InclusionConfig,

    // ==== Intermediate states to build the final select stmt ====
    /// Tables in the FROM clause
    /// Initialized to empty, will be constructed during the stmt build
    from_tables: Vec<Arc<LogicalTable>>,
    /// Join Clauses
    /// Initialized to empty, will be constructed during the stmt build
    join_clauses: Vec<Arc<JoinClause>>,
}

impl SelectStatementBuilder {
    pub fn new(
        seed: u64,
        context: Arc<GlobalContext>,
        enable_where_clause: InclusionConfig,
        enable_join_clause: InclusionConfig,
    ) -> Self {
        Self {
            rng: rng_from_seed(seed),
            ctx: context,
            max_table_count: None,
            allow_derived_tables: false,
            from_tables: Vec::new(),
            join_clauses: Vec::new(),
            enable_where_clause,
            enable_join_clause,
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
        // ==== Pick src tables ====
        let src_tables = self.pick_src_tables()?;

        // ==== Generate FROM list and JOIN clauses ====
        let (from_tables, join_clauses) = self.partition_tables_into_from_and_joins(src_tables)?;
        self.from_tables = from_tables;
        self.join_clauses = join_clauses;

        // ==== Generate select exprs ====
        let expr_seed = self.rng.next_u64();
        let expr_gen = ExprGenerator::new(expr_seed, self.ctx.clone());
        let src_columns = ExprGenerator::tables_to_columns(&self.from_tables, &self.ctx);
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
                    .from_tables
                    .iter()
                    .map(|table| (table.clone(), None))
                    .collect(),
            },
            join_clauses: self.join_clauses.clone(),
            where_clause,
        })
    }

    // ==== Helper functions for `generate_stmt()` ====
    pub fn pick_src_tables(&mut self) -> Result<Vec<Arc<LogicalTable>>> {
        // TODO: Support duplicate table like `... from t1, t1 as t1_2` in the future

        // ==== Pick some unique tables and return them ====
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

        // Use sample API for more elegant random selection without replacement
        // to avoid duplicate tables in the FROM clause
        let mut available_tables_clone = available_tables.clone();
        let mut selected_tables = Vec::new();

        for _ in 0..num_tables {
            if available_tables_clone.is_empty() {
                break;
            }
            let index = self.rng.random_range(0..available_tables_clone.len());
            let table = available_tables_clone.remove(index);
            selected_tables.push(Arc::clone(&table));
        }

        Ok(selected_tables)
    }

    /// Partition source tables into FROM tables and JOIN clauses
    /// Returns (from_tables, join_clauses) as a pure function
    ///
    /// This function is a pure function, it doesn't modify self's inner states,
    /// `&mut self` is used only for `rng`
    fn partition_tables_into_from_and_joins(
        &mut self,
        mut src_tables: Vec<Arc<LogicalTable>>,
    ) -> Result<(Vec<Arc<LogicalTable>>, Vec<Arc<JoinClause>>)> {
        // e.g. the src tables are t1, t2, t3, t4
        // it might choose FROM tables t1, t2, and JOIN tables t3, t4
        // the generated query will look like
        //  SELECT *
        //  FROM t1, t2
        //  JOIN t3 ON ...
        //  JOIN t4 ON ...
        //  WHERE ...

        // Randomize the source table order first
        src_tables.shuffle(&mut self.rng);

        // If JOIN generation is disabled, place all tables in FROM and return no JOINs
        if !self.enable_join_clause.should_enable(Some(&mut self.rng)) {
            return Ok((src_tables, Vec::new()));
        }

        // Randomly split the src tables into from_tables and join_tables
        let split_index = self.rng.random_range(1..=src_tables.len());

        // Next, build the join expressions iteratively
        // e.g.
        // select *
        // from t1
        // join t2 on (expr_ref_t1_t2)
        // join t3 on (expr_ref_t1_t2_t3)

        // For the current iteration for building `JOIN ON` clause, the referenced
        // tables
        let mut referenced_tables = src_tables[..split_index].to_vec();
        let join_tables = src_tables[split_index..].to_vec();
        let from_tables = referenced_tables.clone();

        let mut join_clauses = Vec::new();

        for join_table in join_tables {
            // Build join on expression
            let src_columns = ExprGenerator::tables_to_columns(&referenced_tables, &self.ctx);
            let mut expr_gen = ExprGenerator::new(self.rng.next_u64(), self.ctx.clone())
                .with_src_columns(Arc::new(src_columns));
            // TODO(coverage): generate the expression with columns in all src
            // tables, this way we can test some invalid expressions like
            // select * from t1 join t2 on t1.v1=t3.v1;

            let join_type = JoinType::get_random(&mut self.rng);

            let join_on_expr = expr_gen.generate_random_expr(DataType::Boolean, 0);
            let join_on_expr = {
                // Genreate some invalid expr for better coverage
                let flip = self.rng.random_bool(0.01);
                match join_type {
                    JoinType::CrossJoin | JoinType::NaturalJoin => {
                        if flip {
                            Some(Arc::new(join_on_expr))
                        } else {
                            None
                        }
                    }
                    _ => {
                        if flip {
                            None
                        } else {
                            Some(Arc::new(join_on_expr))
                        }
                    }
                }
            };

            join_clauses.push(Arc::new(JoinClause {
                join_table: Arc::clone(&join_table),
                join_type,
                join_on_expr,
            }));
            referenced_tables.push(join_table);
        }

        Ok((from_tables, join_clauses))
    }

    /// Generate a random WHERE clause expression (returns None for no WHERE clause)
    fn generate_where_clause(&mut self, expr_gen: &mut ExprGenerator) -> Result<Option<Expr>> {
        // Decide if the WHERE clause should be generated
        if self.enable_where_clause.should_enable(Some(&mut self.rng)) {
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
