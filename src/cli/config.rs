use super::Cli;
use crate::common::Result;
use crate::common::fuzzer_err;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Config for the fuzzer runner.
///
/// This configuration controls both:
/// 1. The overall fuzzing process (rounds, queries, timeout)
/// 2. The table and query generation parameters that will be propagated to RunnerConfig
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FuzzerRunnerConfig {
    // General fuzzing parameters
    pub seed: u64,
    pub rounds: u32,
    pub queries_per_round: u32,
    pub timeout_seconds: u64,
    pub log_path: Option<PathBuf>,

    // Parameters propagated to RunnerConfig
    pub max_column_count: u64,
    pub max_row_count: u64,
    pub max_expr_level: u32,
}

impl Default for FuzzerRunnerConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            rounds: 3,
            queries_per_round: 10,
            timeout_seconds: 30,
            log_path: None,
            max_column_count: 5,
            max_row_count: 100,
            max_expr_level: 3,
        }
    }
}

impl FuzzerRunnerConfig {
    /// Convert this FuzzerRunnerConfig into a RunnerConfig
    ///
    /// This method extracts and transfers all parameters that should be propagated
    /// to the runner configuration.
    pub fn to_runner_config(&self) -> crate::fuzz_context::RunnerConfig {
        crate::fuzz_context::RunnerConfig::default()
            .with_max_column_count(self.max_column_count)
            .with_max_row_count(self.max_row_count)
            .with_max_expr_level(self.max_expr_level)
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .map_err(|e| fuzzer_err(&format!("Failed to read config file: {}", e)))?;

        let config: Self = toml::from_str(&content)
            .map_err(|e| fuzzer_err(&format!("Failed to parse config file: {}", e)))?;

        Ok(config)
    }

    pub fn from_cli(cli: &Cli) -> Result<Self> {
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

        Ok(config)
    }
}
