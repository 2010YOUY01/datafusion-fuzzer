use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A struct for storing and tracking live fuzzing statistics
#[derive(Debug)]
pub struct FuzzerRunner {
    // Fuzzing runtime statistics
    stats: Arc<Mutex<FuzzerStats>>,

    // Sample interval for storing recent queries
    sample_interval_secs: u64,
}

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
    pub running_time_secs: u64,
    pub recent_query: String,
}

impl FuzzerRunner {
    /// Create a new FuzzerRunner with the specified total rounds
    pub fn new(total_rounds: u32) -> Self {
        let stats = FuzzerStats {
            rounds_completed: 0,
            total_rounds,
            queries_executed: 0,
            queries_succeeded: 0,
            start_time: Instant::now(),
            last_sample_time: Instant::now(),
            recent_query: String::new(),
        };

        Self {
            stats: Arc::new(Mutex::new(stats)),
            sample_interval_secs: 5, // Sample queries every 5 seconds
        }
    }

    /// Get a clone of the current statistics
    pub fn get_stats(&self) -> FuzzerStats {
        self.stats.lock().unwrap().clone()
    }

    pub fn complete_round(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.rounds_completed += 1;
    }

    /// Record a query execution
    pub fn record_query(&self, query: &str, success: bool) {
        let mut stats = self.stats.lock().unwrap();
        stats.queries_executed += 1;
        stats.queries_succeeded += if success { 1 } else { 0 };

        let now = Instant::now();

        // Sample the query if enough time has passed since the last sample OR if there's no existing query
        if stats.recent_query.is_empty()
            || now.duration_since(stats.last_sample_time).as_secs() >= self.sample_interval_secs
        {
            stats.last_sample_time = now;
            // Store the query as is to preserve multiline formatting
            stats.recent_query = query.to_string();
        }
    }

    /// Get statistics formatted for display in a TUI
    pub fn get_tui_stats(&self) -> TuiStats {
        let stats = self.get_stats();
        let elapsed = stats.start_time.elapsed();

        let qps = if elapsed.as_secs() > 0 {
            stats.queries_executed as f64 / elapsed.as_secs() as f64
        } else {
            0.0
        };

        let success_rate = if stats.queries_executed > 0 {
            (stats.queries_succeeded as f64 / stats.queries_executed as f64) * 100.0
        } else {
            0.0
        };

        let recent_query = stats.recent_query;

        TuiStats {
            rounds_completed: stats.rounds_completed,
            total_rounds: stats.total_rounds,
            queries_executed: stats.queries_executed,
            queries_succeeded: stats.queries_succeeded,
            success_rate,
            queries_per_second: qps,
            running_time_secs: elapsed.as_secs(),
            recent_query: recent_query,
        }
    }
}
