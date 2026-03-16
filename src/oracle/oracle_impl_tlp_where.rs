use crate::common::{InclusionConfig, Result, fuzzer_err};
use crate::oracle::{Oracle, QueryContext, QueryExecutionResult};
use crate::query_generator::stmt_select_def::SelectStatementBuilder;
use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use std::sync::Arc;

/// TLP-WHERE oracle (v1).
///
/// It validates the count identity over a predicate `p`:
/// count(all) == count(p) + count(not p) + count(p is null)
pub struct TlpWhereOracle {
    seed: u64,
    ctx: Arc<crate::fuzz_context::GlobalContext>,
}

impl TlpWhereOracle {
    pub fn new(seed: u64, ctx: Arc<crate::fuzz_context::GlobalContext>) -> Self {
        Self { seed, ctx }
    }

    fn extract_single_count(batches: &[RecordBatch]) -> Result<i64> {
        let mut values = Vec::new();

        for batch in batches {
            if batch.num_columns() != 1 {
                return Err(fuzzer_err(&format!(
                    "Expected one-column COUNT result, got {} columns",
                    batch.num_columns()
                )));
            }

            let array = batch.column(0);
            let int64_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                fuzzer_err(&format!(
                    "Expected Int64 COUNT result, got {:?}",
                    array.data_type()
                ))
            })?;

            for row_idx in 0..int64_array.len() {
                if int64_array.is_null(row_idx) {
                    return Err(fuzzer_err("COUNT result unexpectedly contains NULL"));
                }
                values.push(int64_array.value(row_idx));
            }
        }

        if values.len() != 1 {
            return Err(fuzzer_err(&format!(
                "Expected exactly one COUNT value, got {}",
                values.len()
            )));
        }

        Ok(values[0])
    }
}

#[async_trait::async_trait]
impl Oracle for TlpWhereOracle {
    fn name(&self) -> &'static str {
        "TlpWhereOracle"
    }

    fn generate_query_group(&mut self) -> Result<Vec<QueryContext>> {
        let mut stmt_builder = SelectStatementBuilder::new(
            self.seed,
            Arc::clone(&self.ctx),
            InclusionConfig::Always(true),
            InclusionConfig::Always(false),
        )
        .with_allow_derived_tables(false)
        .with_max_table_count(1);

        let stmt = stmt_builder.generate_stmt()?;
        let source_sql = stmt.to_from_join_sql()?;
        let predicate = stmt
            .where_expr()
            .ok_or_else(|| fuzzer_err("TLP-WHERE expected a generated WHERE predicate"))?;
        let predicate_sql = crate::common::util::to_sql_string(predicate)?;

        let q_all = format!("SELECT CAST(COUNT(*) AS BIGINT) AS cnt\n{}", source_sql);
        let q_p = format!(
            "SELECT CAST(COUNT(*) AS BIGINT) AS cnt\n{}\nWHERE ({})",
            source_sql, predicate_sql
        );
        let q_not_p = format!(
            "SELECT CAST(COUNT(*) AS BIGINT) AS cnt\n{}\nWHERE NOT ({})",
            source_sql, predicate_sql
        );
        let q_null_p = format!(
            "SELECT CAST(COUNT(*) AS BIGINT) AS cnt\n{}\nWHERE ({}) IS NULL",
            source_sql, predicate_sql
        );

        let session_context = self.ctx.runtime_context.get_session_context();
        Ok(vec![
            QueryContext::with_description(
                q_all,
                Arc::clone(&session_context),
                "TLP-WHERE all".to_string(),
            ),
            QueryContext::with_description(
                q_p,
                Arc::clone(&session_context),
                "TLP-WHERE p".to_string(),
            ),
            QueryContext::with_description(
                q_not_p,
                Arc::clone(&session_context),
                "TLP-WHERE not p".to_string(),
            ),
            QueryContext::with_description(
                q_null_p,
                session_context,
                "TLP-WHERE p is null".to_string(),
            ),
        ])
    }

    async fn validate_consistency(&self, results: &[QueryExecutionResult]) -> Result<()> {
        if results.len() != 4 {
            return Err(fuzzer_err(&format!(
                "TLP-WHERE expects 4 query results, got {}",
                results.len()
            )));
        }

        // Skip validation for this run when any branch fails.
        if results.iter().any(|r| r.result.is_err()) {
            return Ok(());
        }

        let counts: Vec<i64> = results
            .iter()
            .map(|result| {
                let batches = result
                    .result
                    .as_ref()
                    .map_err(|e| fuzzer_err(&e.to_string()))?;
                Self::extract_single_count(batches)
            })
            .collect::<Result<Vec<_>>>()?;

        let all_count = counts[0];
        let p_count = counts[1];
        let not_p_count = counts[2];
        let null_p_count = counts[3];
        let rhs = p_count + not_p_count + null_p_count;

        if all_count != rhs {
            return Err(fuzzer_err(&format!(
                "TLP-WHERE identity violated: all={} vs p+not_p+null_p={} (p={}, not_p={}, null_p={})",
                all_count, rhs, p_count, not_p_count, null_p_count
            )));
        }

        Ok(())
    }

    fn create_error_report(&self, results: &[QueryExecutionResult]) -> Result<String> {
        let mut report = String::new();
        report.push_str("TLP-WHERE Oracle Test Failed\n");
        report.push_str("===========================\n\n");

        let labels = ["all", "p", "not p", "p is null"];
        for (idx, result) in results.iter().enumerate() {
            let label = labels.get(idx).copied().unwrap_or("unknown");
            report.push_str(&format!(
                "Q{} [{}]:\n{}\n",
                idx + 1,
                label,
                result.query_context.query
            ));

            match &result.result {
                Ok(batches) => match Self::extract_single_count(batches) {
                    Ok(v) => report.push_str(&format!("  status: ok, count={}\n\n", v)),
                    Err(e) => report.push_str(&format!("  status: ok, parse_error={}\n\n", e)),
                },
                Err(e) => report.push_str(&format!("  status: error, details={}\n\n", e)),
            }
        }

        if results.len() == 4 && results.iter().all(|r| r.result.is_ok()) {
            let count_result = results
                .iter()
                .map(|result| {
                    if let Ok(batches) = &result.result {
                        Self::extract_single_count(batches)
                    } else {
                        Err(fuzzer_err(
                            "unexpected error state while building error report",
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>();

            if let Ok(counts) = count_result {
                report.push_str(&format!(
                    "Equation check: {} == {} + {} + {} (rhs={})\n",
                    counts[0],
                    counts[1],
                    counts[2],
                    counts[3],
                    counts[1] + counts[2] + counts[3]
                ));
            }
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::LogicalTable;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    fn make_query_context(label: &str) -> Arc<QueryContext> {
        Arc::new(QueryContext::new(
            format!("SELECT {}", label),
            Arc::new(SessionContext::new()),
        ))
    }

    fn make_success_result(label: &str, values: Vec<i64>) -> QueryExecutionResult {
        let schema = Arc::new(Schema::new(vec![Field::new("cnt", DataType::Int64, false)]));
        let array = Arc::new(Int64Array::from(values)) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        QueryExecutionResult {
            query_context: make_query_context(label),
            result: Ok(vec![batch]),
        }
    }

    fn make_error_result(label: &str) -> QueryExecutionResult {
        QueryExecutionResult {
            query_context: make_query_context(label),
            result: Err(fuzzer_err("expected execution error in test")),
        }
    }

    #[tokio::test]
    async fn tlp_where_validate_passes_for_matching_counts() {
        let oracle =
            TlpWhereOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![10]),
            make_success_result("p", vec![4]),
            make_success_result("not_p", vec![5]),
            make_success_result("null_p", vec![1]),
        ];

        assert!(oracle.validate_consistency(&results).await.is_ok());
    }

    #[tokio::test]
    async fn tlp_where_validate_fails_for_mismatch() {
        let oracle =
            TlpWhereOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![9]),
            make_success_result("p", vec![4]),
            make_success_result("not_p", vec![5]),
            make_success_result("null_p", vec![1]),
        ];

        let err = oracle.validate_consistency(&results).await.unwrap_err();
        assert!(err.to_string().contains("TLP-WHERE identity violated"));
    }

    #[tokio::test]
    async fn tlp_where_validate_skips_when_any_query_errors() {
        let oracle =
            TlpWhereOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![9]),
            make_success_result("p", vec![4]),
            make_error_result("not_p"),
            make_success_result("null_p", vec![1]),
        ];

        assert!(oracle.validate_consistency(&results).await.is_ok());
    }

    #[tokio::test]
    async fn tlp_where_validate_fails_for_malformed_count_output() {
        let oracle =
            TlpWhereOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![10]),
            make_success_result("p", vec![4, 5]),
            make_success_result("not_p", vec![5]),
            make_success_result("null_p", vec![1]),
        ];

        let err = oracle.validate_consistency(&results).await.unwrap_err();
        assert!(err.to_string().contains("Expected exactly one COUNT value"));
    }

    #[test]
    fn tlp_where_generates_expected_query_group_shape() {
        let ctx = Arc::new(crate::fuzz_context::GlobalContext::default());
        ctx.runtime_context
            .registered_tables
            .write()
            .unwrap()
            .insert(
                "t0".to_string(),
                Arc::new(LogicalTable::new("t0".to_string())),
            );

        let mut oracle = TlpWhereOracle::new(123, Arc::clone(&ctx));
        let query_group = oracle.generate_query_group().unwrap();
        let queries = QueryContext::get_queries(&query_group);

        assert_eq!(queries.len(), 4);
        assert!(queries[0].contains("SELECT CAST(COUNT(*) AS BIGINT) AS cnt"));
        assert!(!queries[0].contains("\nWHERE "));
        assert!(queries[1].contains("\nWHERE ("));
        assert!(queries[2].contains("WHERE NOT ("));
        assert!(queries[3].contains(") IS NULL"));
    }
}
