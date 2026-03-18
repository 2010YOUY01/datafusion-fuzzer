use crate::common::util;
use crate::common::{InclusionConfig, Result, fuzzer_err};
use crate::oracle::oracle_common;
use crate::oracle::{Oracle, QueryContext, QueryExecutionResult};
use crate::query_generator::stmt_select_def::SelectStatementBuilder;
use std::sync::Arc;

/// TLP-WHERE oracle.
///
/// It validates value-level multiset equivalence:
/// q_all == q_p UNION ALL q_not_p UNION ALL q_p_is_null
///
/// ### Example:
///
/// SELECT * FROM t;
///
/// should return the same multiset as
///
/// SELECT * FROM t
/// WHERE date_col = DATE '2000-01-01'
/// UNION ALL
/// SELECT * FROM t
/// WHERE NOT (date_col = DATE '2000-01-01')
/// UNION ALL
/// SELECT * FROM t
/// WHERE (date_col = DATE '2000-01-01') IS NULL;
///
pub struct TlpWhereOracle {
    seed: u64,
    ctx: Arc<crate::fuzz_context::GlobalContext>,
}

impl TlpWhereOracle {
    pub fn new(seed: u64, ctx: Arc<crate::fuzz_context::GlobalContext>) -> Self {
        Self { seed, ctx }
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

        let q_all = format!("SELECT *\n{}", source_sql);
        let q_partition_union = format!(
            "SELECT *\n{}\nWHERE ({})\nUNION ALL\nSELECT *\n{}\nWHERE NOT ({})\nUNION ALL\nSELECT *\n{}\nWHERE ({}) IS NULL",
            source_sql, predicate_sql, source_sql, predicate_sql, source_sql, predicate_sql
        );

        let session_context = self.ctx.runtime_context.get_session_context();
        Ok(vec![
            QueryContext::with_description(
                q_all,
                Arc::clone(&session_context),
                "TLP-WHERE all".to_string(),
            ),
            QueryContext::with_description(
                q_partition_union,
                Arc::clone(&session_context),
                "TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL".to_string(),
            ),
        ])
    }

    async fn validate_consistency(&self, results: &[QueryExecutionResult]) -> Result<()> {
        if results.len() != 2 {
            return Err(fuzzer_err(&format!(
                "TLP-WHERE expects 2 query results, got {}",
                results.len()
            )));
        }

        // Skip validation for this run when any branch fails.
        if results.iter().any(|r| r.result.is_err()) {
            return Ok(());
        }

        oracle_common::validate_value_equivalence(results, 0, 1, "TLP-WHERE")
    }

    fn create_error_report(&self, results: &[QueryExecutionResult]) -> Result<String> {
        let mut report = String::new();
        report.push_str("TLP-WHERE Oracle Test Failed\n");
        report.push_str("===========================\n\n");

        let labels = ["all", "p UNION ALL NOT p UNION ALL p IS NULL"];
        for (idx, result) in results.iter().enumerate() {
            let label = labels.get(idx).copied().unwrap_or("unknown");
            report.push_str(&format!(
                "Q{} [{}]:\n{}\n",
                idx + 1,
                label,
                result.query_context.query
            ));

            match &result.result {
                Ok(batches) => report.push_str(&format!(
                    "  status: ok, rows={}\n\n",
                    util::count_total_rows(batches)
                )),
                Err(e) => report.push_str(&format!("  status: error, details={}\n\n", e)),
            }
        }

        if results.len() == 2 && results.iter().all(|r| r.result.is_ok()) {
            let q_all_batches = results[0]
                .result
                .as_ref()
                .map_err(|e| fuzzer_err(&e.to_string()))?;
            let q_union_batches = results[1]
                .result
                .as_ref()
                .map_err(|e| fuzzer_err(&e.to_string()))?;

            report.push_str(&format!(
                "Row counts: all={}, partition_union={}\n",
                util::count_total_rows(q_all_batches),
                util::count_total_rows(q_union_batches)
            ));

            let all_multiset = util::batches_to_row_multiset(q_all_batches)?;
            let partition_multiset = util::batches_to_row_multiset(q_union_batches)?;

            if all_multiset != partition_multiset {
                report.push_str("\nTop multiset differences:\n");
                report.push_str(&util::format_row_multiset_diff(
                    &all_multiset,
                    &partition_multiset,
                ));
                report.push('\n');
            } else {
                report.push_str("Multiset equivalence: true\n");
            }
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::LogicalTable;
    use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
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
    async fn tlp_where_validate_passes_for_matching_values() {
        let oracle =
            TlpWhereOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![1, 2, 3]),
            make_success_result("partition_union", vec![1, 2, 3]),
        ];

        assert!(oracle.validate_consistency(&results).await.is_ok());
    }

    #[tokio::test]
    async fn tlp_where_validate_fails_for_value_mismatch() {
        let oracle =
            TlpWhereOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![1, 2]),
            make_success_result("partition_union", vec![1, 2, 2]),
        ];

        let err = oracle.validate_consistency(&results).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("TLP-WHERE value equivalence violated")
        );
    }

    #[tokio::test]
    async fn tlp_where_validate_skips_when_any_query_errors() {
        let oracle =
            TlpWhereOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![1, 2]),
            make_error_result("partition_union"),
        ];

        assert!(oracle.validate_consistency(&results).await.is_ok());
    }

    #[tokio::test]
    async fn tlp_where_validate_fails_for_schema_mismatch() {
        let one_col_schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int64, false)]));
        let one_col_batch = RecordBatch::try_new(
            one_col_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>],
        )
        .unwrap();

        let two_col_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Int64, false),
        ]));
        let two_col_batch = RecordBatch::try_new(
            two_col_schema,
            vec![
                Arc::new(Int64Array::from(vec![1])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![9])) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let oracle =
            TlpWhereOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            QueryExecutionResult {
                query_context: make_query_context("all"),
                result: Ok(vec![one_col_batch.clone()]),
            },
            QueryExecutionResult {
                query_context: make_query_context("partition_union"),
                result: Ok(vec![two_col_batch]),
            },
        ];

        let err = oracle.validate_consistency(&results).await.unwrap_err();
        assert!(err.to_string().contains("value equivalence violated"));
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

        assert_eq!(queries.len(), 2);
        assert!(queries[0].contains("SELECT *"));
        assert!(!queries[0].contains("\nWHERE "));
        assert!(queries[1].contains("UNION ALL"));
        assert!(queries[1].contains("\nWHERE ("));
        assert!(queries[1].contains("WHERE NOT ("));
        assert!(queries[1].contains(") IS NULL"));
    }
}
