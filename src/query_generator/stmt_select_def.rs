use crate::common::LogicalTable;

use super::expr_def::ExprWrapper;

struct SelectStatement {
    // ---- Intermediate states ----
    src_tables: Vec<LogicalTable>,

    // ---- Final 'select' compoments ----
    select_exprs: Vec<ExprWrapper>,
    from_clause: FromClause,
}

struct FromClause {
    // vector of (table, alias)
    from_list: Vec<(LogicalTable, Option<String>)>,
}
