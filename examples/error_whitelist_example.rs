use datafusion_fuzzer::{
    cli::error_whitelist::{self, get_configured_patterns},
    common::Result,
};

/// Example demonstrating the error whitelist functionality
///
/// This example shows how the fuzzer now handles errors with a flexible pattern system:
/// 1. Whitelisted errors (exact strings or regex patterns) are accepted
/// 2. Non-whitelisted errors are logged and flagged as issues  
/// 3. The NoCrashOracle now focuses only on truly unexpected crashes
fn main() -> Result<()> {
    println!("Error Whitelist Example");
    println!("=======================");

    // Show currently configured patterns
    let configured_patterns = get_configured_patterns();
    println!("Currently configured error patterns:");
    for (i, pattern) in configured_patterns.iter().enumerate() {
        println!("  {}. {}", i + 1, pattern);
    }

    println!("\nHow the new system works:");
    println!("1. Error patterns are defined in src/cli/error_whitelist.rs");
    println!("2. Two types of patterns are supported:");
    println!("   - Exact string matching (faster)");
    println!("   - Regex pattern matching (more flexible)");
    println!("3. When a query fails, the error is checked against all patterns");
    println!("4. If any pattern matches, the error is considered acceptable");
    println!("5. Non-matching errors are flagged as potential stability issues");

    println!("\n=== Pattern Types ===");

    println!("\n1. Exact String Patterns:");
    println!("   ErrorPattern::Exact(\"Arrow error: Divide by zero error\")");
    println!("   - Fast substring matching");
    println!("   - Case-sensitive");
    println!("   - Good for known exact error messages");

    println!("\n2. Regex Patterns:");
    println!("   ErrorPattern::Regex(r\"(?i)divide\\s*by\\s*zero\")");
    println!("   - Flexible pattern matching");
    println!("   - Supports case-insensitive matching with (?i)");
    println!("   - Good for variable error message formats");

    println!("\n=== Adding New Patterns ===");
    println!(
        "To add new error patterns, edit src/cli/error_whitelist.rs and modify the ERROR_PATTERNS vector:"
    );
    println!();
    println!("static ERROR_PATTERNS: LazyLock<Vec<ErrorPattern>> = LazyLock::new(|| {{");
    println!("    vec![");
    println!("        // Exact patterns");
    println!("        ErrorPattern::Exact(\"Arrow error: Divide by zero error\"),");
    println!("        ErrorPattern::Exact(\"Schema validation failed\"),");
    println!();
    println!("        // Regex patterns");
    println!("        ErrorPattern::Regex(r\"(?i)timeout.*expired\"),");
    println!("        ErrorPattern::Regex(r\"Arrow error: (Invalid|Unsupported)\"),");
    println!("        ErrorPattern::Regex(r\"(?i)(out of memory|allocation failed)\"),");
    println!("    ]");
    println!("}});");

    println!("\n=== Regex Pattern Examples ===");
    println!("// Case-insensitive divide by zero");
    println!("ErrorPattern::Regex(r\"(?i)divide\\s*by\\s*zero\"),");
    println!();
    println!("// Multiple Arrow error types");
    println!(
        "ErrorPattern::Regex(r\"Arrow error: (Divide by zero|Invalid argument|Schema mismatch)\"),"
    );
    println!();
    println!("// Memory-related errors");
    println!("ErrorPattern::Regex(r\"(?i)(out of memory|memory.*exhausted|allocation.*failed)\"),");
    println!();
    println!("// Timeout errors with flexible formatting");
    println!("ErrorPattern::Regex(r\"(?i)(timeout|timed out).*\"),");
    println!();
    println!("// Network-related errors");
    println!(
        "ErrorPattern::Regex(r\"(?i)(connection.*(refused|reset|timeout)|network.*unreachable)\"),"
    );

    println!("\n=== Regex Tips ===");
    println!("- Use (?i) at the start for case-insensitive matching");
    println!("- Use \\s* or \\s+ for flexible whitespace matching");
    println!("- Use .* for wildcard matching");
    println!("- Use (option1|option2) for alternative matching");
    println!("- Use \\b for word boundaries to avoid partial matches");
    println!("- Test your regex patterns thoroughly!");

    println!("\n=== Testing Examples ===");

    // Test some examples
    let test_cases = [
        ("Arrow error: Divide by zero error", true),
        ("Query failed: Arrow error: Divide by zero error", true),
        ("Some random network error", false),
        ("Segmentation fault", false),
        ("", false),
    ];

    for (error_msg, expected) in test_cases.iter() {
        let result = error_whitelist::is_error_whitelisted(error_msg);
        let status = if result == *expected { "✅" } else { "❌" };
        println!(
            "{} Test: '{}' -> {} (expected: {})",
            status,
            error_msg,
            if result { "WHITELISTED" } else { "FLAGGED" },
            if *expected { "WHITELISTED" } else { "FLAGGED" }
        );
    }

    println!("\n=== Benefits ===");
    println!("- Reduces false positives for expected/acceptable errors");
    println!("- Flexible pattern matching with both exact strings and regex");
    println!("- Performance optimized (regex compiled once and cached)");
    println!("- Easy to extend with new error patterns");
    println!("- Makes the fuzzer more precise in identifying real issues");
    println!("- Allows mathematical operations that may legitimately fail");

    Ok(())
}
