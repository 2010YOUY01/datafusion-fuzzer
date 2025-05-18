use clap::Parser;
use std::path::Path;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use datafuzzer::{
    cli::{Cli, FuzzerRunnerConfig, run_fuzzer},
    common::Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    // Build config from CLI args and config file
    let config = FuzzerRunnerConfig::from_cli(&cli)?;

    // Setup logging
    setup_logging(&config)?;

    // Run the fuzzer
    run_fuzzer(config).await
}

fn setup_logging(config: &FuzzerRunnerConfig) -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let format = fmt::format()
        .with_level(false)
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .compact();

    if let Some(log_path) = &config.log_path {
        // Create log directory if it doesn't exist
        if let Some(parent) = log_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // Create a non-blocking file writer for the logs
        let file_appender = tracing_appender::rolling::never(
            log_path.parent().unwrap_or(Path::new(".")),
            log_path.file_name().unwrap(),
        );
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

        // Register the file writer and the stdout writer
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::Layer::default()
                    .with_writer(std::io::stdout)
                    .event_format(format.clone()),
            )
            .with(
                fmt::Layer::default()
                    .with_writer(non_blocking)
                    .event_format(format),
            )
            .init();

        info!("Logging initialized to file: {:?}", log_path);
    } else {
        // Just log to stdout
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

    Ok(())
}
