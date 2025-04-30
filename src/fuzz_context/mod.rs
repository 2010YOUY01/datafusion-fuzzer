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
            runtime_context: RuntimeContext::new(),
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

pub struct RuntimeContext {}

impl RuntimeContext {
    pub fn new() -> Self {
        Self {}
    }
}
