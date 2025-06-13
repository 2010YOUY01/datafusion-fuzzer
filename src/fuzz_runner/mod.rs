use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Live statistics for the fuzzing process
#[derive(Debug, Clone)]
pub struct FuzzerStats {
    // Basic counters
    pub rounds_completed: u32,
    pub total_rounds: u32,
    pub queries_executed: u64,
    pub queries_succeeded: u64,

    // Timers
    pub start_time: Instant,

    // Sample a recent query every t seconds
    pub last_sample_time: Instant,
    pub recent_query: String,
}

// Struct to hold formatted stats for display in a TUI
#[derive(Debug, Clone)]
pub struct TuiStats {
    pub rounds_completed: u32,
    pub total_rounds: u32,
    pub queries_executed: u64,
    pub queries_succeeded: u64,
    pub success_rate: f64,
    pub queries_per_second: f64,
    pub running_time_secs: f64,
    pub recent_query: String,
}

impl FuzzerStats {
    /// Create new FuzzerStats with the specified total rounds
    pub fn new(total_rounds: u32) -> Self {
        Self {
            rounds_completed: 0,
            total_rounds,
            queries_executed: 0,
            queries_succeeded: 0,
            start_time: Instant::now(),
            last_sample_time: Instant::now(),
            recent_query: String::new(),
        }
    }

    /// Record a query execution for display and statistics purposes
    ///
    /// This method serves two main purposes:
    /// 1. **CLI Live Display**: Shows the most recent query being tested in the TUI/CLI interface
    /// 2. **Statistics Tracking**: Updates execution counters and success rates for reporting
    ///
    /// **When to call this method:**
    /// - Call once per generated query group, typically right after successful query generation
    /// - The `success` parameter should reflect the final validation outcome
    /// - For CLI display purposes, the query text is more important than the success status
    ///
    /// **Sampling behavior:**
    /// - Queries are sampled for display based on time intervals to avoid overwhelming the UI
    /// - All queries contribute to statistics, but only sampled queries are shown in the interface
    ///
    /// # Arguments
    /// * `query` - The SQL query string to display and track
    /// * `success` - Whether the query validation/execution succeeded
    /// * `sample_interval_secs` - The interval in seconds for sampling queries for display
    pub fn record_query(&mut self, query: &str, success: bool, sample_interval_secs: u64) {
        self.queries_executed += 1;
        self.queries_succeeded += if success { 1 } else { 0 };

        let now = Instant::now();

        // Sample the query if enough time has passed since the last sample OR if there's no existing query
        if self.recent_query.is_empty()
            || now.duration_since(self.last_sample_time).as_secs() >= sample_interval_secs
        {
            self.last_sample_time = now;
            // Store the query as is to preserve multiline formatting
            self.recent_query = query.to_string();
        }
    }

    /// Complete a round of fuzzing
    pub fn complete_round(&mut self) {
        self.rounds_completed += 1;
    }

    /// Get statistics formatted for display in a TUI
    pub fn get_tui_stats(&self) -> TuiStats {
        let elapsed = self.start_time.elapsed();

        let elapsed_secs = elapsed.as_secs_f64();
        let qps = if elapsed_secs > 0.0 {
            self.queries_executed as f64 / elapsed_secs
        } else {
            0.0
        };

        let success_rate = if self.queries_executed > 0 {
            (self.queries_succeeded as f64 / self.queries_executed as f64) * 100.0
        } else {
            0.0
        };

        TuiStats {
            rounds_completed: self.rounds_completed,
            total_rounds: self.total_rounds,
            queries_executed: self.queries_executed,
            queries_succeeded: self.queries_succeeded,
            success_rate,
            queries_per_second: qps,
            running_time_secs: elapsed_secs,
            recent_query: self.recent_query.clone(),
        }
    }
}

/// Helper function to create a new shared FuzzerStats instance
pub fn create_fuzzer_stats(total_rounds: u32) -> Arc<Mutex<FuzzerStats>> {
    Arc::new(Mutex::new(FuzzerStats::new(total_rounds)))
}

/// Helper function to record a query execution
pub fn record_query(
    stats: &Arc<Mutex<FuzzerStats>>,
    query: &str,
    success: bool,
    sample_interval_secs: u64,
) {
    let mut stats_guard = stats.lock().unwrap();
    stats_guard.record_query(query, success, sample_interval_secs);
}

/// Helper function to complete a fuzzing round
pub fn complete_round(stats: &Arc<Mutex<FuzzerStats>>) {
    let mut stats_guard = stats.lock().unwrap();
    stats_guard.complete_round();
}

/// Helper function to get TUI stats
pub fn get_tui_stats(stats: &Arc<Mutex<FuzzerStats>>) -> TuiStats {
    let stats_guard = stats.lock().unwrap();
    stats_guard.get_tui_stats()
}
