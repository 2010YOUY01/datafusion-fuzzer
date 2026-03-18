use crate::common::{Result, fuzzer_err, util};
use crate::oracle::QueryExecutionResult;

pub(crate) fn validate_value_equivalence(
    results: &[QueryExecutionResult],
    left_idx: usize,
    right_idx: usize,
    oracle_name: &str,
) -> Result<()> {
    let left_result = results
        .get(left_idx)
        .ok_or_else(|| fuzzer_err(&format!("Missing result at index {}", left_idx)))?;
    let right_result = results
        .get(right_idx)
        .ok_or_else(|| fuzzer_err(&format!("Missing result at index {}", right_idx)))?;

    let left_batches = left_result
        .result
        .as_ref()
        .map_err(|e| fuzzer_err(&e.to_string()))?;
    let right_batches = right_result
        .result
        .as_ref()
        .map_err(|e| fuzzer_err(&e.to_string()))?;

    util::validate_batches_value_equivalence(left_batches, right_batches, oracle_name)
}
