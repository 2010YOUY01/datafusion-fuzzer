use datafusion::{prelude::Expr, sql::unparser::expr_to_sql};

use super::Result;

/// Convert a DataFusion `Expr` into a SQL string using DataFusion's unparser.
pub fn to_sql_string(expr: &Expr) -> Result<String> {
    let unparsed = expr_to_sql(expr)?;
    Ok(unparsed.to_string())
}
