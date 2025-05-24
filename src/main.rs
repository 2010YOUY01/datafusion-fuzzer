use clap::Parser;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt, prelude::*};

use datafuzzer::{
    cli::{Cli, FuzzerRunnerConfig, TuiApp, init, restore, run_fuzzer},
    common::Result,
    fuzz_runner::FuzzerRunner,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = FuzzerRunnerConfig::from_cli(&cli)?;
    let _log_guards = setup_logging(&config)?;
    let fuzzer = Arc::new(FuzzerRunner::new(config.rounds));

    // Spawn TUI in a separate thread (if enabled)
    if config.enable_tui {
        let tui_fuzzer = Arc::clone(&fuzzer);
        tokio::spawn(async move {
            let mut terminal = init();
            let _ = TuiApp::new(tui_fuzzer).run(&mut terminal);
            restore();
        });
    }

    // Run the fuzzer concurrently
    run_fuzzer(config, Arc::clone(&fuzzer)).await
}

/// RAII logging workers
struct LogGuards {
    _trace_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    _error_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

/// Logging guide:
/// - For general logs use `info!` macro
/// - For system under test (DataFusion)'s found bugs, use `error!` macro
///
/// When configured with a log directory (e.g., `logs/`), the system creates two log files:
/// - `logs/trace.log`: Contains all logs generated using the `info!` macro
/// - `logs/error.log`: Contains logs specifically related to system under test
/// bugs using the `error!` macro
fn setup_logging(config: &FuzzerRunnerConfig) -> Result<LogGuards> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let format = fmt::format()
        .with_level(true)
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .compact();

    let mut log_guards = LogGuards {
        _trace_guard: None,
        _error_guard: None,
    };

    if let Some(log_dir) = &config.log_path {
        // Create log directory if it doesn't exist
        if !log_dir.exists() {
            std::fs::create_dir_all(log_dir)?;
        }

        // Create the log files
        let trace_path = log_dir.join("trace.log");
        let error_path = log_dir.join("error.log");

        // Create appenders for the log files
        let trace_file = tracing_appender::rolling::never(
            trace_path.parent().unwrap(),
            trace_path.file_name().unwrap(),
        );
        let error_file = tracing_appender::rolling::never(
            error_path.parent().unwrap(),
            error_path.file_name().unwrap(),
        );

        // Create non-blocking writers
        let (trace_writer, trace_guard) = tracing_appender::non_blocking(trace_file);
        let (error_writer, error_guard) = tracing_appender::non_blocking(error_file);

        // Store the guards
        log_guards._trace_guard = Some(trace_guard);
        log_guards._error_guard = Some(error_guard);

        // Set up the subscriber
        let subscriber = tracing_subscriber::registry().with(env_filter);

        // Add stdout layer if display_logs is enabled
        if config.display_logs {
            let stdout_layer = fmt::Layer::default()
                .with_writer(std::io::stdout)
                .event_format(format.clone());

            // Add error-only layer
            let error_layer = fmt::Layer::default()
                .with_writer(error_writer)
                .event_format(format.clone())
                .with_filter(LevelFilter::ERROR);

            // Add all-logs layer
            let trace_layer = fmt::Layer::default()
                .with_writer(trace_writer)
                .event_format(format.clone());

            subscriber
                .with(stdout_layer)
                .with(error_layer)
                .with(trace_layer)
                .init();
        } else {
            // Add error-only layer
            let error_layer = fmt::Layer::default()
                .with_writer(error_writer)
                .event_format(format.clone())
                .with_filter(LevelFilter::ERROR);

            // Add all-logs layer
            let trace_layer = fmt::Layer::default()
                .with_writer(trace_writer)
                .event_format(format.clone());

            subscriber.with(error_layer).with(trace_layer).init();
        }

        info!("Logging initialized to directory: {:?}", log_dir);
        info!("All logs go to trace.log, errors go to error.log");
    } else {
        // Just log to stdout
        if config.display_logs {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    fmt::Layer::default()
                        .with_writer(std::io::stdout)
                        .event_format(format),
                )
                .init();

            info!("Logging initialized to stdout only");
        }
    }

    Ok(log_guards)
}
