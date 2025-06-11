pub mod ctx_observability;

use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU32, Ordering},
};

use datafusion::{common::HashMap, prelude::SessionContext};

use crate::common::LogicalTable;

/// Create a default DataFusion SessionContext with standard configuration
/// This ensures consistency between initial creation and reset operations
fn default_df_session_context() -> Arc<SessionContext> {
    Arc::new(SessionContext::new())
}

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

pub struct RunnerConfig {
    pub display_logs: bool,
    /// Random table generation policy
    pub max_column_count: u64,
    pub max_row_count: u64,

    pub max_expr_level: u32,
}

impl RunnerConfig {
    pub fn new() -> Self {
        Self::default()
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

    pub fn with_display_logs(mut self, display_logs: bool) -> Self {
        self.display_logs = display_logs;
        self
    }

    pub fn default() -> Self {
        Self {
            display_logs: false,
            max_column_count: 5,
            max_row_count: 100,
            max_expr_level: 3,
        }
    }
}

pub struct RuntimeContext {
    pub df_ctx: Arc<RwLock<Arc<SessionContext>>>,
    pub registered_tables: Arc<RwLock<HashMap<String, Arc<LogicalTable>>>>,
    current_table_idx: AtomicU32,
}

impl RuntimeContext {
    pub fn new(df_ctx: Arc<SessionContext>) -> Self {
        Self {
            df_ctx: Arc::new(RwLock::new(df_ctx)),
            registered_tables: Arc::new(RwLock::new(HashMap::new())),
            current_table_idx: AtomicU32::new(0),
        }
    }

    pub fn default() -> Self {
        Self {
            df_ctx: Arc::new(RwLock::new(default_df_session_context())),
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

    /// Get a clone of the current DataFusion SessionContext
    pub fn get_session_context(&self) -> Arc<SessionContext> {
        let df_ctx = self.df_ctx.read().unwrap();
        Arc::clone(&*df_ctx)
    }
}
