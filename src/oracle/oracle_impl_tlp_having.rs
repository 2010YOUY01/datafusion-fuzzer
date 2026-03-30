use crate::common::{InclusionConfig, Result, fuzzer_err};
use crate::oracle::{Oracle, QueryContext, QueryExecutionResult};
use crate::query_generator::stmt_select_def::SelectStatementBuilder;
use std::sync::Arc;

/// TLP-HAVING oracle.
///
/// It validates value-level multiset equivalence:
/// q_all_groups == q_having_p UNION ALL q_having_not_p UNION ALL q_having_p_is_null
pub struct TlpHavingOracle {
    seed: u64,
    ctx: Arc<crate::fuzz_context::GlobalContext>,
}

impl TlpHavingOracle {
    pub fn new(seed: u64, ctx: Arc<crate::fuzz_context::GlobalContext>) -> Self {
        Self { seed, ctx }
    }
}

#[async_trait::async_trait]
impl Oracle for TlpHavingOracle {
    fn name(&self) -> &'static str {
        "TlpHavingOracle"
    }

    fn generate_query_group(&mut self) -> Result<Vec<QueryContext>> {
        let mut stmt_builder = SelectStatementBuilder::new(
            self.seed,
            Arc::clone(&self.ctx),
            InclusionConfig::Maybe(0.7),
            InclusionConfig::Always(false),
        )
        .with_allow_derived_tables(false)
        .with_max_table_count(1)
        .with_enable_group_by_clause(InclusionConfig::Always(true))
        .with_enable_having_clause(InclusionConfig::Always(true));

        let stmt = stmt_builder.generate_stmt()?;
        let source_sql = stmt.to_from_join_sql()?;
        let group_by_sql = stmt
            .to_group_by_sql()?
            .ok_or_else(|| fuzzer_err("TLP-HAVING expected generated GROUP BY expressions"))?;
        let predicate = stmt
            .having_expr()
            .ok_or_else(|| fuzzer_err("TLP-HAVING expected a generated HAVING predicate"))?;
        let predicate_sql = crate::common::util::to_sql_string(predicate)?;

        let mut base_grouped_query = format!("SELECT {}\n{}", group_by_sql, source_sql);
        if let Some(where_expr) = stmt.where_expr() {
            let where_sql = crate::common::util::to_sql_string(where_expr)?;
            base_grouped_query.push_str(&format!("\nWHERE {}", where_sql));
        }
        base_grouped_query.push_str(&format!("\nGROUP BY {}", group_by_sql));

        let q_all = base_grouped_query.clone();
        let q_partition_union = format!(
            "{}\nHAVING ({})\nUNION ALL\n{}\nHAVING NOT ({})\nUNION ALL\n{}\nHAVING ({}) IS NULL",
            base_grouped_query,
            predicate_sql,
            base_grouped_query,
            predicate_sql,
            base_grouped_query,
            predicate_sql
        );

        let session_context = self.ctx.runtime_context.get_session_context();
        Ok(vec![
            QueryContext::with_description(
                q_all,
                Arc::clone(&session_context),
                "TLP-HAVING all groups".to_string(),
            ),
            QueryContext::with_description(
                q_partition_union,
                Arc::clone(&session_context),
                "TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL".to_string(),
            ),
        ])
    }

    async fn validate_consistency(&self, results: &[QueryExecutionResult]) -> Result<()> {
        if results.len() != 2 {
            return Err(fuzzer_err(&format!(
                "TLP-HAVING expects 2 query results, got {}",
                results.len()
            )));
        }

        // Skip validation for this run when any branch fails.
        if results.iter().any(|r| r.result.is_err()) {
            return Ok(());
        }

        crate::oracle::tlp_shared::validate_value_equivalence(results, "TLP-HAVING")
    }

    fn create_error_report(&self, results: &[QueryExecutionResult]) -> Result<String> {
        let mut report = String::new();
        report.push_str("TLP-HAVING Oracle Test Failed\n");
        report.push_str("============================\n\n");

        let labels = ["all groups", "p UNION ALL NOT p UNION ALL p IS NULL"];
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
                    crate::oracle::tlp_shared::count_total_rows(batches)
                )),
                Err(e) => report.push_str(&format!("  status: error, details={}\n\n", e)),
            }
        }

        crate::oracle::tlp_shared::append_value_equivalence_report(&mut report, results)?;

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{FuzzerDataType, LogicalColumn, LogicalTable, init_available_data_types};
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn make_query_context(label: &str) -> Arc<QueryContext> {
        Arc::new(QueryContext::new(
            format!("SELECT {}", label),
            Arc::new(SessionContext::new()),
        ))
    }

    fn make_success_result(label: &str, values: Vec<i64>) -> QueryExecutionResult {
        let schema = Arc::new(Schema::new(vec![Field::new("g", DataType::Int64, false)]));
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
    async fn tlp_having_validate_passes_for_matching_values() {
        let oracle =
            TlpHavingOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![1, 2, 3]),
            make_success_result("partition_union", vec![1, 2, 3]),
        ];

        assert!(oracle.validate_consistency(&results).await.is_ok());
    }

    #[tokio::test]
    async fn tlp_having_validate_fails_for_value_mismatch() {
        let oracle =
            TlpHavingOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![1, 2]),
            make_success_result("partition_union", vec![1, 2, 2]),
        ];

        let err = oracle.validate_consistency(&results).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("TLP-HAVING value equivalence violated")
        );
    }

    #[tokio::test]
    async fn tlp_having_validate_skips_when_any_query_errors() {
        let oracle =
            TlpHavingOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
        let results = vec![
            make_success_result("all", vec![1, 2]),
            make_error_result("partition_union"),
        ];

        assert!(oracle.validate_consistency(&results).await.is_ok());
    }

    #[tokio::test]
    async fn tlp_having_validate_fails_for_schema_mismatch() {
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
            TlpHavingOracle::new(1, Arc::new(crate::fuzz_context::GlobalContext::default()));
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
    fn tlp_having_generates_expected_query_group_shape() {
        init_available_data_types();
        let ctx = Arc::new(crate::fuzz_context::GlobalContext::default());
        ctx.runtime_context
            .registered_tables
            .write()
            .unwrap()
            .insert(
                "t0".to_string(),
                Arc::new(LogicalTable::with_columns(
                    "t0".to_string(),
                    vec![LogicalColumn {
                        name: "c0".to_string(),
                        data_type: FuzzerDataType::Int64,
                    }],
                )),
            );

        let mut oracle = TlpHavingOracle::new(123, Arc::clone(&ctx));
        let query_group = oracle.generate_query_group().unwrap();
        let queries = QueryContext::get_queries(&query_group);

        assert_eq!(queries.len(), 2);
        assert!(queries[0].contains("SELECT "));
        assert!(queries[0].contains("\nGROUP BY "));
        assert!(!queries[0].contains("\nHAVING "));
        assert!(queries[1].contains("UNION ALL"));
        assert!(queries[1].contains("\nHAVING ("));
        assert!(queries[1].contains("HAVING NOT ("));
        assert!(queries[1].contains(") IS NULL"));
    }
}
