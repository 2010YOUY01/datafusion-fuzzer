use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU32, Ordering},
};

use datafusion::{common::HashMap, prelude::SessionContext};

use crate::common::LogicalTable;

pub struct GlobalContext {
    pub runner_config: RunnerConfig,
    pub runtime_context: RuntimeContext,
}

impl GlobalContext {
    pub fn new(runner_config: RunnerConfig, runtime_context: RuntimeContext) -> Self {
        Self {
            runner_config,
            runtime_context,
        }
    }

    pub fn default() -> Self {
        Self {
            runner_config: RunnerConfig::default(),
            runtime_context: RuntimeContext::default(),
        }
    }
}

pub struct RunnerConfig {
    /// Random table generation policy
    pub max_column_count: u64,
    pub max_row_count: u64,
}

impl RunnerConfig {
    pub fn new(max_column_count: u64, max_row_count: u64) -> Self {
        Self {
            max_column_count,
            max_row_count,
        }
    }

    pub fn default() -> Self {
        Self {
            max_column_count: 5,
            max_row_count: 100,
        }
    }
}

pub struct RuntimeContext {
    pub df_ctx: Arc<SessionContext>,
    pub registered_tables: Arc<RwLock<HashMap<String, Arc<LogicalTable>>>>,
    current_table_idx: AtomicU32,
}

impl RuntimeContext {
    pub fn new(df_ctx: Arc<SessionContext>) -> Self {
        Self {
            df_ctx,
            registered_tables: Arc::new(RwLock::new(HashMap::new())),
            current_table_idx: AtomicU32::new(0),
        }
    }

    pub fn default() -> Self {
        Self {
            df_ctx: Arc::new(SessionContext::new()),
            registered_tables: Arc::new(RwLock::new(HashMap::new())),
            current_table_idx: AtomicU32::new(0),
        }
    }

    pub fn next_table_name(&self) -> String {
        format!(
            "t{}",
            self.current_table_idx.fetch_add(1, Ordering::Relaxed)
        )
    }
}
