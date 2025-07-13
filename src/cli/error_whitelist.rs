use regex::Regex;
use std::sync::LazyLock;

/// Error pattern matching strategies
#[derive(Debug, Clone)]
pub enum ErrorPattern {
    /// Exact string match - checks if the error message contains this exact substring
    Contains(&'static str),
    /// Regex pattern match - checks if the error message matches this regex pattern
    RegexMatch(&'static str),
}

/// Configuration for error whitelist patterns
///
/// This module provides flexible error pattern matching for the fuzzer.
/// You can whitelist errors using either exact string matching or regex patterns.
///
/// # Examples
///
/// ## Adding Exact String Patterns
/// ```rust
/// # use datafusion_fuzzer::cli::error_whitelist::ErrorPattern;
/// // This will match any error containing "Arrow error: Divide by zero error"
/// ErrorPattern::Contains("Arrow error: Divide by zero error");
/// ```
///
/// ## Adding Regex Patterns
/// ```rust
/// # use datafusion_fuzzer::cli::error_whitelist::ErrorPattern;
/// // This will match any division by zero error with flexible formatting
/// ErrorPattern::RegexMatch(r"(?i)divide\s*by\s*zero");
///
/// // This will match any Arrow error with specific error codes
/// ErrorPattern::RegexMatch(r"Arrow error: (Divide by zero|Invalid argument)");
///
/// // This will match memory-related errors
/// ErrorPattern::RegexMatch(r"(?i)(out of memory|memory.*exhausted|allocation.*failed)");
/// ```
///
/// ## Regex Pattern Tips
/// - Use `(?i)` at the start for case-insensitive matching
/// - Use `\s*` or `\s+` for flexible whitespace matching
/// - Use `.*` for wildcard matching
/// - Use `(option1|option2)` for alternative matching
/// - Use `\b` for word boundaries to avoid partial matches
///
/// # Performance Note
/// Regex patterns are compiled once and cached for performance.
/// Exact string patterns use simple substring matching and are faster.
static ERROR_PATTERNS: LazyLock<Vec<ErrorPattern>> = LazyLock::new(|| {
    vec![
        // =========================
        // False Positives
        // =========================

        // select 1 / 0;
        ErrorPattern::Contains("Arrow error: Divide by zero error"),
        // select Null * Null;
        ErrorPattern::RegexMatch(
            r"Error during planning: Cannot coerce arithmetic expression (.+) to valid types",
        ),
        // TODO: check if expected
        // This is a type coersion error: DuckDB also fails but I'm not sure if this
        // should be expected.
        // CREATE TABLE t3 (col_t3_5_uint64 UBIGINT);
        // INSERT INTO t3 VALUES (52);
        // SELECT (86 / ((t3.col_t3_5_uint64 - 117) % t3.col_t3_5_uint64)) FROM t3;
        ErrorPattern::RegexMatch(
            r"(?i)Query execution failed: Arrow error: Cast error: value of (.+) is out of range uint(.+)",
        ),
        // timestamp * timestamp
        ErrorPattern::Contains("Invalid timestamp arithmetic operation"),
        // =========================
        // Known Issues
        // =========================

        // https://github.com/apache/datafusion/issues/13558
        ErrorPattern::Contains("Projections require unique expression names"),
    ]
});

/// Compiled regex patterns cache
static COMPILED_REGEXES: LazyLock<Vec<Option<Regex>>> = LazyLock::new(|| {
    ERROR_PATTERNS
        .iter()
        .map(|pattern| match pattern {
            ErrorPattern::Contains(_) => None, // No regex compilation needed for exact matches
            ErrorPattern::RegexMatch(regex_str) => match Regex::new(regex_str) {
                Ok(regex) => Some(regex),
                Err(e) => {
                    eprintln!("Warning: Invalid regex pattern '{}': {}", regex_str, e);
                    None
                }
            },
        })
        .collect()
});

/// Check if an error message matches any pattern in the whitelist
///
/// This function checks both exact string patterns and regex patterns.
/// It returns true if the error message matches any whitelisted pattern.
///
/// # Arguments
/// * `error_msg` - The error message to check
///
/// # Returns
/// * `true` if the error message matches any whitelisted pattern
/// * `false` if no patterns match
///
/// # Examples
/// ```rust
/// use datafusion_fuzzer::cli::error_whitelist::is_error_whitelisted;
///
/// // These should match if the patterns are configured
/// assert!(is_error_whitelisted("Query failed: Arrow error: Divide by zero error"));
/// assert!(is_error_whitelisted("Some context: Arrow error: Divide by zero error here"));
///
/// // This should not match
/// assert!(!is_error_whitelisted("Unexpected segmentation fault"));
/// ```
pub fn is_error_whitelisted(error_msg: &str) -> bool {
    for (i, pattern) in ERROR_PATTERNS.iter().enumerate() {
        match pattern {
            ErrorPattern::Contains(exact_str) => {
                if error_msg.contains(exact_str) {
                    return true;
                }
            }
            ErrorPattern::RegexMatch(_) => {
                if let Some(Some(regex)) = COMPILED_REGEXES.get(i) {
                    if regex.is_match(error_msg) {
                        return true;
                    }
                }
            }
        }
    }

    false
}

/// Get a list of all configured error patterns for debugging/logging
pub fn get_configured_patterns() -> Vec<String> {
    ERROR_PATTERNS
        .iter()
        .map(|pattern| match pattern {
            ErrorPattern::Contains(s) => format!("Exact: {}", s),
            ErrorPattern::RegexMatch(s) => format!("Regex: {}", s),
        })
        .collect()
}
