use crate::common::{Result, fuzzer_err};
use crate::oracle::QueryExecutionResult;
use datafusion::arrow::array::RecordBatch;
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;

type RowMultiset = HashMap<Vec<ScalarValue>, usize>;

pub(crate) fn batches_to_row_multiset(batches: &[RecordBatch]) -> Result<RowMultiset> {
    let mut multiset: RowMultiset = HashMap::new();
    let mut expected_num_cols: Option<usize> = None;

    for batch in batches {
        if let Some(expected) = expected_num_cols {
            if batch.num_columns() != expected {
                return Err(fuzzer_err(&format!(
                    "Mismatched column count across batches: expected {}, got {}",
                    expected,
                    batch.num_columns()
                )));
            }
        } else {
            expected_num_cols = Some(batch.num_columns());
        }

        let num_cols = batch.num_columns();
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut row_key = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let value = ScalarValue::try_from_array(batch.column(col_idx).as_ref(), row_idx)
                    .map(|v| v.compacted())
                    .map_err(|e| {
                        fuzzer_err(&format!(
                            "Failed to convert value at row {} to ScalarValue: {}",
                            row_idx, e
                        ))
                    })?;
                row_key.push(value);
            }
            *multiset.entry(row_key).or_insert(0) += 1;
        }
    }

    Ok(multiset)
}

pub(crate) fn format_multiset_diff(left: &RowMultiset, right: &RowMultiset) -> String {
    let mut lines = Vec::new();
    for (row, left_count) in left {
        let right_count = right.get(row).copied().unwrap_or(0);
        if *left_count != right_count {
            lines.push(format!(
                "row={:?}, left_count={}, right_count={}",
                row, left_count, right_count
            ));
        }
    }
    for (row, right_count) in right {
        if !left.contains_key(row) {
            lines.push(format!(
                "row={:?}, left_count=0, right_count={}",
                row, right_count
            ));
        }
    }

    lines.sort();
    let preview = lines.into_iter().take(20).collect::<Vec<_>>();
    if preview.is_empty() {
        "no row differences".to_string()
    } else {
        preview.join("\n")
    }
}

pub(crate) fn count_total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

pub(crate) fn validate_value_equivalence(
    results: &[QueryExecutionResult],
    oracle_name: &str,
) -> Result<()> {
    if results.len() < 2 {
        return Err(fuzzer_err(&format!(
            "{} expects at least 2 query results, got {}",
            oracle_name,
            results.len()
        )));
    }

    let q_all_batches = results[0]
        .result
        .as_ref()
        .map_err(|e| fuzzer_err(&e.to_string()))?;
    let q_union_batches = results[1]
        .result
        .as_ref()
        .map_err(|e| fuzzer_err(&e.to_string()))?;

    let all_multiset = batches_to_row_multiset(q_all_batches)?;
    let partition_multiset = batches_to_row_multiset(q_union_batches)?;

    if all_multiset != partition_multiset {
        return Err(fuzzer_err(&format!(
            "{} value equivalence violated:\n{}",
            oracle_name,
            format_multiset_diff(&all_multiset, &partition_multiset)
        )));
    }

    Ok(())
}

pub(crate) fn append_value_equivalence_report(
    report: &mut String,
    results: &[QueryExecutionResult],
) -> Result<()> {
    if results.len() != 2 || results.iter().any(|r| r.result.is_err()) {
        return Ok(());
    }

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
        count_total_rows(q_all_batches),
        count_total_rows(q_union_batches)
    ));

    let all_multiset = batches_to_row_multiset(q_all_batches)?;
    let partition_multiset = batches_to_row_multiset(q_union_batches)?;

    if all_multiset != partition_multiset {
        report.push_str("\nTop multiset differences:\n");
        report.push_str(&format_multiset_diff(&all_multiset, &partition_multiset));
        report.push('\n');
    } else {
        report.push_str("Multiset equivalence: true\n");
    }

    Ok(())
}
