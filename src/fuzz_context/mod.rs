pub mod ctx_observability;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicU32, Ordering},
};

use datafusion::{common::HashMap, prelude::SessionContext};
use serde::{Deserialize, Serialize};

use crate::common::value_generator::ValueGenerationConfig;
use crate::common::{LogicalTable, Result, fuzzer_err};
use crate::fuzz_runner::FuzzerStats;

/// Create a default DataFusion SessionContext with standard configuration
/// This ensures consistency between initial creation and reset operations
fn default_df_session_context() -> Arc<SessionContext> {
    Arc::new(SessionContext::new())
}

pub struct GlobalContext {
    pub runner_config: RunnerConfig,
    pub runtime_context: RuntimeContext,
    pub fuzzer_stats: Arc<Mutex<FuzzerStats>>,
}

impl GlobalContext {
    pub fn new(
        runner_config: RunnerConfig,
        runtime_context: RuntimeContext,
        fuzzer_stats: Arc<Mutex<FuzzerStats>>,
    ) -> Self {
        Self {
            runner_config,
            runtime_context,
            fuzzer_stats,
        }
    }

    pub fn default() -> Self {
        let default_config = RunnerConfig::default();
        let fuzzer_stats = Arc::new(Mutex::new(FuzzerStats::new(default_config.rounds)));

        Self {
            runner_config: default_config,
            runtime_context: RuntimeContext::default(),
            fuzzer_stats,
        }
    }

    /// Reset the DataFusion context to drop all registered tables
    /// This creates a fresh SessionContext and clears all table registrations
    pub fn reset_datafusion_context(&self) {
        use std::sync::atomic::Ordering;

        // Create a new SessionContext to completely reset the DataFusion state
        let new_session_context = default_df_session_context();

        // Replace the existing SessionContext with a new one
        {
            let mut df_ctx = self.runtime_context.df_ctx.write().unwrap();
            *df_ctx = new_session_context;
        }

        // Clear the fuzzer's table registry
        {
            let mut tables = self.runtime_context.registered_tables.write().unwrap();
            tables.clear();
        }

        // Reset the table counter
        self.runtime_context
            .current_table_idx
            .store(0, Ordering::Relaxed);
    }
}

/// Unified configuration for the DataFusion fuzzer.
///
/// This configuration controls both:
/// 1. The overall fuzzing process (rounds, queries, timeout)
/// 2. The table and query generation parameters
/// 3. UI and display parameters
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunnerConfig {
    // General fuzzing parameters
    pub seed: u64,
    pub rounds: u32,
    pub queries_per_round: u32,
    pub timeout_seconds: u64,
    pub log_path: Option<PathBuf>,

    // UI and display parameters
    pub display_logs: bool,
    pub enable_tui: bool,
    pub sample_interval_secs: u64,

    // Table and query generation parameters
    pub max_column_count: u64,
    pub max_row_count: u64,
    pub max_expr_level: u32,
    pub max_table_count: u32,
    pub max_insert_per_table: u32,
}

impl RunnerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn with_rounds(mut self, rounds: u32) -> Self {
        self.rounds = rounds;
        self
    }

    pub fn with_queries_per_round(mut self, queries_per_round: u32) -> Self {
        self.queries_per_round = queries_per_round;
        self
    }

    pub fn with_timeout_seconds(mut self, timeout_seconds: u64) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    pub fn with_log_path(mut self, log_path: Option<PathBuf>) -> Self {
        self.log_path = log_path;
        self
    }

    pub fn with_display_logs(mut self, display_logs: bool) -> Self {
        self.display_logs = display_logs;
        self
    }

    pub fn with_enable_tui(mut self, enable_tui: bool) -> Self {
        self.enable_tui = enable_tui;
        self
    }

    pub fn with_sample_interval_secs(mut self, sample_interval_secs: u64) -> Self {
        self.sample_interval_secs = sample_interval_secs;
        self
    }

    pub fn with_max_column_count(mut self, max_column_count: u64) -> Self {
        self.max_column_count = max_column_count;
        self
    }

    pub fn with_max_row_count(mut self, max_row_count: u64) -> Self {
        self.max_row_count = max_row_count;
        self
    }

    pub fn with_max_expr_level(mut self, max_expr_level: u32) -> Self {
        self.max_expr_level = max_expr_level;
        self
    }

    pub fn with_max_table_count(mut self, max_table_count: u32) -> Self {
        self.max_table_count = max_table_count;
        self
    }

    pub fn with_max_insert_per_table(mut self, max_insert_per_table: u32) -> Self {
        self.max_insert_per_table = max_insert_per_table;
        self
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .map_err(|e| fuzzer_err(&format!("Failed to read config file: {}", e)))?;

        let config: Self = toml::from_str(&content)
            .map_err(|e| fuzzer_err(&format!("Failed to parse config file: {}", e)))?;

        Ok(config)
    }

    pub fn from_cli(cli: &crate::cli::Cli) -> Result<Self> {
        // Start with default or config file if provided
        let mut config = if let Some(config_path) = &cli.config {
            Self::from_file(config_path)?
        } else {
            Self::default()
        };

        // Override with CLI arguments if provided
        if cli.seed != 42 {
            config.seed = cli.seed;
        }

        if let Some(rounds) = cli.rounds {
            config.rounds = rounds;
        }

        if let Some(queries) = cli.queries_per_round {
            config.queries_per_round = queries;
        }

        if let Some(timeout) = cli.timeout {
            config.timeout_seconds = timeout;
        }

        if let Some(log_path) = &cli.log_path {
            config.log_path = Some(log_path.clone());
        }

        // Set display_logs from CLI argument
        config.display_logs = cli.display_logs;

        // Set enable_tui from CLI argument
        config.enable_tui = cli.enable_tui;

        Ok(config)
    }

    pub fn default() -> Self {
        Self {
            seed: 42,
            rounds: 3,
            queries_per_round: 10,
            timeout_seconds: 2,
            log_path: Some(PathBuf::from("logs")),
            display_logs: false,
            enable_tui: true,
            sample_interval_secs: 5,
            max_column_count: 5,
            max_row_count: 100,
            max_expr_level: 3,
            max_table_count: 3,
            max_insert_per_table: 20,
        }
    }
}

pub struct RuntimeContext {
    pub df_ctx: Arc<RwLock<Arc<SessionContext>>>,
    pub registered_tables: Arc<RwLock<HashMap<String, Arc<LogicalTable>>>>,
    current_table_idx: AtomicU32,
    // Cached value generation config for performance (nullable by default)
    pub value_generation_config: ValueGenerationConfig,
}

impl RuntimeContext {
    pub fn new(df_ctx: Arc<SessionContext>) -> Self {
        Self {
            df_ctx: Arc::new(RwLock::new(df_ctx)),
            registered_tables: Arc::new(RwLock::new(HashMap::new())),
            current_table_idx: AtomicU32::new(0),
            value_generation_config: ValueGenerationConfig::default(), // Non-nullable by default
        }
    }

    pub fn default() -> Self {
        Self {
            df_ctx: Arc::new(RwLock::new(default_df_session_context())),
            registered_tables: Arc::new(RwLock::new(HashMap::new())),
            current_table_idx: AtomicU32::new(0),
            value_generation_config: ValueGenerationConfig::default(), // Non-nullable by default
        }
    }

    pub fn next_table_name(&self) -> String {
        format!(
            "t{}",
            self.current_table_idx.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Reset the table counter to 0 for deterministic naming
    pub fn reset_table_counter(&self) {
        self.current_table_idx.store(0, Ordering::Relaxed);
    }

    /// Get a clone of the current DataFusion SessionContext
    pub fn get_session_context(&self) -> Arc<SessionContext> {
        let df_ctx = self.df_ctx.read().unwrap();
        Arc::clone(&*df_ctx)
    }
}
