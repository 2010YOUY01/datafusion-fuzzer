use clap::Parser;
use std::io;
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

// TUI related
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    DefaultTerminal, Frame,
    buffer::Buffer,
    layout::Rect,
    style::Stylize,
    symbols::border,
    text::{Line, Text},
    widgets::{Block, Paragraph, Widget},
};

use datafuzzer::{
    cli::{Cli, FuzzerRunnerConfig, run_fuzzer},
    common::Result,
    fuzz_runner::FuzzerRunner,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    // Build config from CLI args and config file
    let config = FuzzerRunnerConfig::from_cli(&cli)?;

    // Setup logging
    setup_logging(&config)?;

    // Create the fuzzer runner that will track statistics
    let fuzzer = Arc::new(FuzzerRunner::new(config.rounds));

    // Spawn TUI in a separate thread
    {
        let tui_fuzzer = Arc::clone(&fuzzer);
        tokio::spawn(async move {
            let mut terminal = ratatui::init();
            let _ = TuiApp::new(tui_fuzzer).run(&mut terminal);
            ratatui::restore();
        });
    }

    // Run the fuzzer concurrently
    run_fuzzer(config, Arc::clone(&fuzzer)).await
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

    Ok(())
}

#[derive(Debug)]
pub struct TuiApp {
    fuzzer: Arc<FuzzerRunner>,
    exit: bool,
}

#[cfg(test)]
impl Default for TuiApp {
    fn default() -> Self {
        // Create a minimal FuzzerRunner for testing with 10 rounds
        let fuzzer = Arc::new(FuzzerRunner::new(10));

        Self {
            fuzzer,
            exit: false,
        }
    }
}

impl TuiApp {
    pub fn new(fuzzer: Arc<FuzzerRunner>) -> Self {
        Self {
            fuzzer,
            exit: false,
        }
    }

    /// runs the application's main loop until the user quits
    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn draw(&self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
    }

    /// updates the application's state based on user input
    fn handle_events(&mut self) -> io::Result<()> {
        // Poll for events with a timeout of 100ms
        if event::poll(std::time::Duration::from_millis(100))? {
            match event::read()? {
                // it's important to check that the event is a key press event as
                // crossterm also emits key release and repeat events on Windows.
                Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                    self.handle_key_event(key_event)
                }
                _ => {}
            }
        } else {
            // Auto update the stats
        }
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }
}

impl Widget for &TuiApp {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = Line::from("DataFusion Fuzzer Running Status".bold());

        let block = Block::bordered()
            .title(title.centered())
            .border_set(border::THICK);

        let stats = self.fuzzer.get_tui_stats();

        // Create the basic stats text
        let mut lines = vec![
            Line::from(vec![
                "Rounds Completed: ".into(),
                stats.rounds_completed.to_string().yellow(),
            ]),
            Line::from(vec![
                "Total Rounds: ".into(),
                stats.total_rounds.to_string().yellow(),
            ]),
            Line::from(vec![
                "Queries Executed: ".into(),
                stats.queries_executed.to_string().yellow(),
            ]),
            Line::from(vec![
                "Queries Success Rate: ".into(),
                stats.success_rate.to_string().yellow(),
            ]),
            Line::from(vec![
                "Queries Per Second: ".into(),
                format!("{:.2}", stats.queries_per_second).yellow(),
            ]),
            Line::from(vec![
                "Running Time: ".into(),
                stats.running_time_secs.to_string().yellow(),
            ]),
            Line::from(""),
            Line::from("─".repeat(40).cyan()), // Add a separator line before query
            Line::from(vec!["Recent Query:".bold().cyan().into()]),
        ];

        // Add the query lines with proper formatting
        let query_lines: Vec<Line> = stats
            .recent_query
            .lines()
            .map(|line| Line::from(line.to_string().yellow()))
            .collect();
        lines.extend(query_lines);

        // Add a separator after the query
        lines.push(Line::from("─".repeat(40).cyan()));

        let stats_text = Text::from(lines);

        Paragraph::new(stats_text)
            .left_aligned()
            .block(block)
            .render(area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::style::Style;

    #[test]
    fn test_exit() -> io::Result<()> {
        let mut app = TuiApp::default();
        app.handle_key_event(KeyCode::Char('q').into());
        assert!(app.exit);
        Ok(())
    }
}
