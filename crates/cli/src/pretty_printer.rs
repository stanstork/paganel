use crate::format::commas;
use crossterm::execute;
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use engine_infra::event_bus::bus::EventBus;
use model::events::migration::MigrationEvent;
use std::collections::HashMap;
use std::io::{self, Stdout, Write, stdout};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Pretty-prints migration progress with colors and Unicode symbols.
pub struct PrettyPrinter<W: Write = Stdout> {
    out: W,
    start_time: Instant,
    pipeline_start_times: HashMap<String, Instant>,
    pipeline_names: HashMap<String, String>,
    stats: GlobalStats,
}

/// Accumulated statistics across all pipelines
#[derive(Debug, Default)]
struct GlobalStats {
    total_rows: u64,
    total_skipped: u64,
    total_failed: u64,
    pipelines_completed: usize,
}

impl PrettyPrinter<Stdout> {
    pub fn new(pipeline_names: HashMap<String, String>) -> Self {
        Self::with_writer(stdout(), pipeline_names)
    }

    /// Runs the pretty printer, subscribing to events from the event bus
    pub async fn run(
        event_bus: EventBus,
        shutdown: CancellationToken,
        pipeline_names: HashMap<String, String>,
    ) -> io::Result<()> {
        let (tx, mut rx) = mpsc::channel(1000);

        // Subscribe to all migration events
        let _subscription = event_bus.subscribe::<MigrationEvent>(tx).await;

        // Drop event bus reference so it can be cleaned up
        drop(event_bus);

        let mut printer = Self::new(pipeline_names);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                event = rx.recv() => {
                    match event {
                        Some(evt) => printer.handle_event(&evt)?,
                        None => break,
                    }
                }
            }
        }

        printer.print_final_summary()?;
        Ok(())
    }
}

impl<W: Write> PrettyPrinter<W> {
    /// Creates a pretty printer over an arbitrary writer.
    pub fn with_writer(out: W, pipeline_names: HashMap<String, String>) -> Self {
        Self {
            out,
            start_time: Instant::now(),
            pipeline_start_times: HashMap::new(),
            pipeline_names,
            stats: GlobalStats::default(),
        }
    }

    /// Handles a single migration event
    pub fn handle_event(&mut self, event: &MigrationEvent) -> io::Result<()> {
        match event {
            MigrationEvent::Started { run_id, .. } => {
                self.print_started(run_id)?;
            }
            MigrationEvent::ProducerStarted { item_id, mode, .. } => {
                self.handle_producer_started(item_id, &mode.to_string())?;
            }
            MigrationEvent::SnapshotStarted {
                item_id,
                estimated_rows,
                ..
            } => {
                self.handle_snapshot_started(item_id, estimated_rows)?;
            }
            MigrationEvent::Progress {
                item_id,
                rows_processed,
                rows_skipped,
                rows_failed,
                ..
            } => {
                self.print_progress(item_id, *rows_processed, *rows_skipped, *rows_failed)?;
            }
            MigrationEvent::Completed {
                item_id,
                rows_processed,
                rows_skipped,
                rows_failed,
                duration_ms,
                ..
            } => {
                self.handle_completed(
                    item_id,
                    *rows_processed,
                    *rows_skipped,
                    *rows_failed,
                    *duration_ms,
                )?;
            }
            MigrationEvent::Failed { item_id, error, .. } => {
                self.print_failed(item_id, error)?;
            }
            MigrationEvent::SnapshotCompleted {
                rows_processed,
                duration_ms,
                ..
            } => {
                self.print_snapshot_completed(*rows_processed, *duration_ms)?;
            }
            MigrationEvent::IntegrityStarted {
                item_id, tables, ..
            } => {
                self.print_integrity_started(item_id, *tables)?;
            }
            MigrationEvent::IntegritySealing { table, .. } => {
                self.print_integrity_sealing(table)?;
            }
            MigrationEvent::IntegrityReceipt {
                table, rows, root, ..
            } => {
                self.print_integrity_receipt(table, *rows, root)?;
            }
            _ => {} // Ignore other events
        }

        Ok(())
    }

    fn handle_producer_started(&mut self, item_id: &str, mode: &str) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id).to_string();
        self.pipeline_start_times
            .insert(item_id.to_string(), Instant::now());
        self.print_line(
            Color::Cyan,
            "◉",
            &format!("Pipeline '{}' started ({} mode)", name, mode),
        )
    }

    fn handle_snapshot_started(
        &mut self,
        item_id: &str,
        estimated_rows: &Option<u64>,
    ) -> io::Result<()> {
        if let Some(est) = estimated_rows {
            let name = self.get_pipeline_name(item_id).to_string();
            self.print_line(
                Color::Cyan,
                "◉",
                &format!("Pipeline '{}' started ({} rows)", name, commas(*est)),
            )?;
        }
        Ok(())
    }

    fn handle_completed(
        &mut self,
        item_id: &str,
        rows_processed: u64,
        rows_skipped: u64,
        rows_failed: u64,
        duration_ms: u64,
    ) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id).to_string();
        let duration = Duration::from_millis(duration_ms);
        let throughput = format_throughput(rows_processed, duration);
        let summary = build_summary(rows_processed, rows_skipped, rows_failed, Some(duration));

        self.print_line(
            Color::Green,
            "✓",
            &format!(
                "Pipeline '{}' completed: {} ({})",
                name, summary, throughput
            ),
        )?;

        self.stats.total_rows += rows_processed;
        self.stats.total_skipped += rows_skipped;
        self.stats.total_failed += rows_failed;
        self.stats.pipelines_completed += 1;

        Ok(())
    }

    fn print_integrity_started(&mut self, item_id: &str, tables: usize) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id).to_string();
        let noun = if tables == 1 { "table" } else { "tables" };
        self.print_line(
            Color::Cyan,
            "◆",
            &format!("Finalizing integrity for '{}': {} {}", name, tables, noun),
        )
    }

    fn print_integrity_sealing(&mut self, table: &str) -> io::Result<()> {
        self.print_line(
            Color::Yellow,
            "⧗",
            &format!("Sealing '{}' (sorting & merging row hashes)…", table),
        )
    }

    fn print_integrity_receipt(&mut self, table: &str, rows: u64, root: &str) -> io::Result<()> {
        self.print_line(
            Color::Green,
            "✓",
            &format!("Receipt '{}': {} rows, root {}", table, commas(rows), root),
        )
    }

    fn print_started(&mut self, run_id: &str) -> io::Result<()> {
        self.print_line(Color::Blue, "▶", &format!("Starting migration: {}", run_id))
    }

    fn print_progress(
        &mut self,
        item_id: &str,
        rows_processed: u64,
        rows_skipped: u64,
        rows_failed: u64,
    ) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id).to_string();
        let details = build_row_details(rows_processed, rows_skipped, rows_failed);
        self.print_line(Color::Yellow, "→", &format!("{} {}", name, details))
    }

    fn print_failed(&mut self, item_id: &str, error: &str) -> io::Result<()> {
        let name = self.get_pipeline_name(item_id).to_string();
        self.print_line(
            Color::Red,
            "✗",
            &format!("Pipeline '{}' failed: {}", name, error),
        )
    }

    fn print_snapshot_completed(
        &mut self,
        rows_processed: u64,
        duration_ms: u64,
    ) -> io::Result<()> {
        let duration = Duration::from_millis(duration_ms);
        let throughput = format_throughput(rows_processed, duration);

        self.print_line(Color::Green, "✓", "Snapshot completed!")?;

        writeln!(self.out, "   Total rows: {}", commas(rows_processed))?;
        writeln!(self.out, "   Duration:   {}", format_duration(duration))?;
        writeln!(self.out, "   Throughput: {}", throughput)?;
        self.out.flush()?;

        Ok(())
    }

    fn print_final_summary(&mut self) -> io::Result<()> {
        if self.stats.pipelines_completed == 0 {
            return Ok(());
        }

        let total_duration = self.start_time.elapsed();
        let throughput = format_throughput(self.stats.total_rows, total_duration);
        let summary = build_row_details(
            self.stats.total_rows,
            self.stats.total_skipped,
            self.stats.total_failed,
        );

        self.print_line(Color::Green, "✓", "Migration completed!")?;

        writeln!(self.out, "   Total:      {}", summary)?;
        writeln!(
            self.out,
            "   Pipelines:  {}",
            self.stats.pipelines_completed
        )?;
        writeln!(
            self.out,
            "   Duration:   {}",
            format_duration(total_duration)
        )?;
        writeln!(self.out, "   Throughput: {}", throughput)?;
        self.out.flush()?;

        Ok(())
    }

    fn print_line(&mut self, color: Color, symbol: &str, message: &str) -> io::Result<()> {
        let timestamp = self.format_timestamp();
        execute!(
            self.out,
            SetForegroundColor(Color::DarkGrey),
            Print(timestamp),
            Print(" "),
            SetForegroundColor(color),
            Print(symbol),
            Print(" "),
            ResetColor,
            Print(message),
            Print("\n")
        )?;
        self.out.flush()?;
        Ok(())
    }

    fn get_pipeline_name<'a>(&'a self, item_id: &'a str) -> &'a str {
        self.pipeline_names
            .get(item_id)
            .map(|s| s.as_str())
            .unwrap_or(item_id)
    }

    fn format_timestamp(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        format!("[{:>7.3}s]", elapsed)
    }
}

/// Formats duration in seconds with 2 decimal places
fn format_duration(duration: Duration) -> String {
    format!("{:.2}s", duration.as_secs_f64())
}

/// Formats throughput as rows/sec, thousands-separated to match row counts.
fn format_throughput(rows: u64, duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs > 0.001 {
        let rate = (rows as f64 / secs).round() as u64;
        format!("{}/s", commas(rate))
    } else {
        "-".to_string()
    }
}

/// Builds row details string (e.g., "1,234 rows, 5 skipped")
fn build_row_details(rows_processed: u64, rows_skipped: u64, rows_failed: u64) -> String {
    let mut s = format!("{} rows", commas(rows_processed));

    if rows_skipped > 0 {
        s.push_str(&format!(", {} skipped", commas(rows_skipped)));
    }
    if rows_failed > 0 {
        s.push_str(&format!(", {} failed", commas(rows_failed)));
    }
    s
}

/// Builds complete summary with duration
fn build_summary(
    rows_processed: u64,
    rows_skipped: u64,
    rows_failed: u64,
    duration: Option<Duration>,
) -> String {
    let mut s = build_row_details(rows_processed, rows_skipped, rows_failed);

    if let Some(d) = duration {
        s.push_str(&format!(" in {}", format_duration(d)));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use model::events::migration::{MigrationEvent, ProducerMode};

    /// Strip ANSI SGR escape sequences so the captured stream can be read and
    /// asserted as plain text.
    fn strip_ansi(bytes: &[u8]) -> String {
        let s = String::from_utf8_lossy(bytes);
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                // Skip until the final byte of the CSI sequence ('m' for SGR).
                for e in chars.by_ref() {
                    if e.is_ascii_alphabetic() {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    /// Normalize the `[  0.000s]` timestamp so snapshots don't depend on wall time.
    fn normalize_timestamps(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut rest = s;
        while let Some(open) = rest.find('[') {
            out.push_str(&rest[..open]);
            if let Some(close) = rest[open..].find(']') {
                let inner = &rest[open + 1..open + close];
                if inner.ends_with('s') && inner.trim_end_matches('s').trim().parse::<f64>().is_ok()
                {
                    out.push_str("[  T.TTTs]");
                    rest = &rest[open + close + 1..];
                    continue;
                }
            }
            out.push('[');
            rest = &rest[open + 1..];
        }
        out.push_str(rest);
        out
    }

    fn render(events: &[MigrationEvent], names: &[(&str, &str)]) -> String {
        let name_map: HashMap<String, String> = names
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut buf: Vec<u8> = Vec::new();
        let mut printer = PrettyPrinter::with_writer(&mut buf, name_map);
        for e in events {
            printer.handle_event(e).expect("handle event");
        }
        printer.print_final_summary().expect("final summary");
        normalize_timestamps(&strip_ansi(&buf))
    }

    fn started(item: &str) -> MigrationEvent {
        MigrationEvent::Started {
            run_id: "run-1".into(),
            item_id: item.into(),
            source: "mysql".into(),
            destination: "postgres".into(),
            timestamp: Utc::now(),
        }
    }

    fn producer(item: &str) -> MigrationEvent {
        MigrationEvent::ProducerStarted {
            run_id: "run-1".into(),
            item_id: item.into(),
            mode: ProducerMode::Snapshot,
            timestamp: Utc::now(),
        }
    }

    fn progress(item: &str, rows: u64, skipped: u64, failed: u64) -> MigrationEvent {
        MigrationEvent::Progress {
            run_id: "run-1".into(),
            item_id: item.into(),
            rows_processed: rows,
            rows_skipped: skipped,
            rows_failed: failed,
            bytes_transferred: 0,
            rows_per_second: 0.0,
            timestamp: Utc::now(),
        }
    }

    fn completed(item: &str, rows: u64, skipped: u64, failed: u64, ms: u64) -> MigrationEvent {
        MigrationEvent::Completed {
            run_id: "run-1".into(),
            item_id: item.into(),
            rows_processed: rows,
            rows_skipped: skipped,
            rows_failed: failed,
            duration_ms: ms,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn renders_a_full_migration_sequence() {
        let out = render(
            &[
                started("p1"),
                producer("p1"),
                progress("p1", 1_000, 0, 0),
                progress("p1", 500_000, 12, 3),
                completed("p1", 1_000_000, 12, 3, 2_500),
            ],
            &[("p1", "migrate_actor")],
        );

        // The whole rendered stream, for eyeballing under --nocapture.
        println!("\n----- pretty output -----\n{out}-------------------------\n");

        assert!(out.contains("Starting migration: run-1"));
        assert!(out.contains("(snapshot mode)"), "mode without debug quotes");
        assert!(out.contains("→ migrate_actor"), "unicode progress arrow");
        assert!(out.contains("1,000,000 rows"));
        assert!(out.contains("12 skipped"));
        assert!(out.contains("400,000/s"), "throughput thousands-separated");
        assert!(out.contains("Migration completed!"));
    }

    #[test]
    fn renders_integrity_finalization_phases() {
        let out = render(
            &[
                completed("p1", 1_000_000, 0, 0, 2_500),
                MigrationEvent::IntegrityStarted {
                    run_id: "run-1".into(),
                    item_id: "p1".into(),
                    tables: 2,
                    timestamp: Utc::now(),
                },
                MigrationEvent::IntegritySealing {
                    run_id: "run-1".into(),
                    item_id: "p1".into(),
                    table: "actor".into(),
                    timestamp: Utc::now(),
                },
                MigrationEvent::IntegrityReceipt {
                    run_id: "run-1".into(),
                    item_id: "p1".into(),
                    table: "actor".into(),
                    rows: 1_000_000,
                    root: "a3f1b2c4".into(),
                    timestamp: Utc::now(),
                },
            ],
            &[("p1", "migrate_actor")],
        );

        println!("\n----- integrity phases -----\n{out}----------------------------\n");

        assert!(out.contains("Finalizing integrity for 'migrate_actor': 2 tables"));
        assert!(out.contains("Sealing 'actor' (sorting & merging row hashes)"));
        assert!(out.contains("Receipt 'actor': 1,000,000 rows, root a3f1b2c4"));
    }

    #[test]
    fn renders_a_failure() {
        let out = render(
            &[
                started("p1"),
                producer("p1"),
                MigrationEvent::Failed {
                    run_id: "run-1".into(),
                    item_id: "p1".into(),
                    error: "connection reset by peer".into(),
                    error_code: None,
                    rows_processed: 42,
                    timestamp: Utc::now(),
                },
            ],
            &[("p1", "migrate_actor")],
        );

        println!("\n----- pretty failure -----\n{out}--------------------------\n");

        assert!(out.contains("failed: connection reset by peer"));
        // No pipeline completed, so no final summary block.
        assert!(!out.contains("Migration completed!"));
    }
}
