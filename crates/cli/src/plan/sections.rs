use super::fmt::{fmt_rows, fmt_seconds, is_unknown_count, pad, plural};
use super::section;
use super::style::Sty;
use engine_planner::plan::{
    diagnostics::diagnostic::Diagnostic, diagnostics::level::DiagnosticLevel,
    execution::migration_report::MigrationReport, execution::summary::PlanStatus,
    schema::types::SchemaChangeType,
};
use std::fmt::Write as _;
use std::path::Path;

pub(super) fn header(out: &mut String, r: &MigrationReport, s: &Sty) {
    let name = Path::new(&r.config_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&r.config_path);

    let sum = &r.summary;
    let tables = tables_to_create(r);
    let _ = writeln!(out, "{} {} {}", s.bold("pag plan"), s.dim("·"), name);
    let _ = writeln!(
        out,
        "{}",
        s.dim(&format!(
            "{} pipeline{} · {} connection{} · {} rows · {} table{} to create",
            sum.total_pipelines,
            plural(sum.total_pipelines),
            sum.total_connections,
            plural(sum.total_connections),
            fmt_rows(&sum.total_source_rows),
            tables,
            plural(tables),
        )),
    );
    out.push('\n');
}

pub(super) fn execution_rail(out: &mut String, r: &MigrationReport, s: &Sty) {
    if r.execution_order.is_empty() {
        return;
    }
    section(out, s, "Execution");

    let n = r.execution_order.len();
    for (i, stage) in r.execution_order.iter().enumerate() {
        let why = if stage.pipelines.len() > 1 {
            format!("parallel ×{}", stage.pipelines.len())
        } else if i > 0 {
            format!("waits for stage {}", stage.stage)
        } else {
            String::new()
        };
        let head = format!(
            "  {} {}   ",
            s.cyan(s.glyph_stage()),
            s.cyan(&format!("stage {}", stage.stage + 1)),
        );
        let pipes = stage.pipelines.join(" · ");
        if why.is_empty() {
            let _ = writeln!(out, "{head}{pipes}");
        } else {
            // Pad to a column, but always keep at least 2 spaces so a long name
            // list never runs into the `why` note.
            let gap = 34usize.saturating_sub(pipes.chars().count()).max(2);
            let _ = writeln!(out, "{head}{pipes}{}{}", " ".repeat(gap), s.dim(&why));
        }
        if i + 1 < n {
            let _ = writeln!(out, "  {}", s.cyan("│"));
        }
    }
    out.push('\n');
}

pub(super) fn diagnostics(out: &mut String, r: &MigrationReport, s: &Sty) {
    let mut items: Vec<(Option<&str>, &Diagnostic)> = Vec::new();
    for d in &r.diagnostics {
        items.push((d.pipeline.as_deref(), d));
    }
    for p in &r.pipelines {
        for d in &p.diagnostics {
            items.push((d.pipeline.as_deref().or(Some(p.name.as_str())), d));
        }
    }
    let important: Vec<_> = items
        .iter()
        .filter(|(_, d)| matches!(d.level, DiagnosticLevel::Error | DiagnosticLevel::Warning))
        .collect();
    let notes = items.len() - important.len();

    if important.is_empty() && notes == 0 {
        return;
    }
    section(out, s, "Diagnostics");

    for (scope, d) in &important {
        let glyph = match d.level {
            DiagnosticLevel::Error => s.red("✗"),
            _ => s.yellow("⚠"),
        };
        let where_ = scope.map(|p| s.dim(p)).unwrap_or_default();
        let _ = writeln!(
            out,
            "  {} {}  {}   {}",
            glyph,
            s.dim(&d.code),
            d.message,
            where_
        );
        if let Some(sg) = &d.suggestion {
            let _ = writeln!(out, "     {} {}", s.dim("↳"), s.dim(sg));
        }
    }
    if notes > 0 {
        let _ = writeln!(
            out,
            "  {}",
            s.dim(&format!(
                "· {} note{} hidden - see --json",
                notes,
                plural(notes)
            )),
        );
    }
    out.push('\n');
}

pub(super) fn ddl(out: &mut String, r: &MigrationReport, s: &Sty) {
    section(out, s, "DDL");

    let mut count = 0usize;
    for p in &r.pipelines {
        for c in &p.schema_changes {
            let Some(sql) = &c.ddl else { continue };
            count += 1;

            // `-- <pipeline> · <what it does>`, tagging risk so the annotation
            // rides along when the statement is pasted into a change ticket.
            let mut comment = format!("-- {} · {}", p.name, c.description);
            if c.is_breaking {
                comment.push_str(" · breaking");
            }
            if !c.is_reversible {
                comment.push_str(" · irreversible");
            }
            let _ = writeln!(out, "  {}", s.dim(&comment));
            for line in sql.trim().lines() {
                let _ = writeln!(out, "  {line}");
            }
            out.push('\n');
        }
    }

    if count == 0 {
        let _ = writeln!(
            out,
            "  {}",
            s.dim("-- no schema changes - nothing to create")
        );
    } else {
        let _ = writeln!(
            out,
            "  {}",
            s.dim(&format!(
                "{} statement{} · verbatim - exactly what `pag apply` runs, in order",
                count,
                plural(count),
            )),
        );
    }
    out.push('\n');
}

pub(super) fn estimates(out: &mut String, r: &MigrationReport, s: &Sty) {
    let e = &r.estimations;
    section(out, s, "Estimates");

    if is_unknown_count(&r.summary.total_source_rows) {
        let _ = writeln!(
            out,
            "  {}",
            s.dim("unavailable - source row count is unknown at plan time; run apply to measure"),
        );
        out.push('\n');
        return;
    }

    let _ = writeln!(
        out,
        "  {}  {}   {}",
        pad("duration", 10),
        s.bold(&e.duration.formatted),
        s.dim(&format!(
            "best {} · worst {}",
            fmt_seconds(e.duration.min_seconds),
            fmt_seconds(e.duration.max_seconds),
        )),
    );
    if !e.duration.calibrated {
        let _ = writeln!(
            out,
            "  {}  {}",
            pad("", 10),
            s.dim("rough - a prior guess; run apply once to calibrate to this machine"),
        );
    }
    let _ = writeln!(
        out,
        "  {}  ~{} MB peak",
        pad("memory", 10),
        e.peak_memory_mb
    );
    let _ = writeln!(
        out,
        "  {}  {}   {}",
        pad("transfer", 10),
        fmt_data_size(e.network_transfer_mb),
        s.dim(&format!(
            "· {} batch{}",
            e.total_batches,
            if e.total_batches == 1 { "" } else { "es" }
        )),
    );
    out.push('\n');
}

fn fmt_data_size(mb: f64) -> String {
    if mb < 1.0 {
        format!("~{:.0} KB", (mb * 1024.0).round())
    } else if mb < 1024.0 {
        format!("~{mb:.1} MB")
    } else {
        format!("~{:.1} GB", mb / 1024.0)
    }
}

pub(super) fn verdict(out: &mut String, r: &MigrationReport, s: &Sty) {
    let _ = writeln!(out, "{}", s.dim(&"─".repeat(72)));

    let tables = tables_to_create(r);
    let conversions: usize = r
        .pipelines
        .iter()
        .map(|p| p.data_flow_summary.type_conversions)
        .sum();
    let lossy: usize = r
        .pipelines
        .iter()
        .map(|p| p.data_flow_summary.unsafe_conversions)
        .sum();

    let mut parts: Vec<String> = Vec::new();

    if tables > 0 {
        parts.push(s.green(&format!("{} table{} to create", tables, plural(tables))));
    }

    if is_unknown_count(&r.summary.total_source_rows) {
        parts.push("row count unknown at plan time".to_string());
    } else {
        parts.push(format!(
            "{} rows to copy",
            fmt_rows(&r.summary.total_source_rows)
        ));
    }

    if conversions > 0 {
        let lossy_tag = if lossy > 0 {
            s.yellow(&format!(" ({lossy} lossy)"))
        } else {
            String::new()
        };
        parts.push(format!(
            "{} conversion{}{}",
            conversions,
            plural(conversions),
            lossy_tag
        ));
    }
    let _ = writeln!(out, "Plan: {}", parts.join(" · "));

    let cmd = format!("pag apply -c {}", r.config_path);
    match r.summary.status {
        PlanStatus::Ready => {
            let _ = writeln!(
                out,
                "{} {}  {}  {}",
                s.green("✓"),
                s.bold("Ready to apply"),
                s.dim("→"),
                s.cyan(&cmd),
            );
        }
        PlanStatus::ReadyWithWarnings => {
            let wc = r.summary.warning_count;
            let _ = writeln!(
                out,
                "{} {}  {}  {}",
                s.yellow("⚠"),
                s.bold(&format!("Ready to apply with {} warning{}", wc, plural(wc))),
                s.dim("→"),
                s.cyan(&cmd),
            );
        }
        PlanStatus::NotExecutable => {
            let reason = r
                .blocking_reason
                .as_deref()
                .unwrap_or("blocking errors present");
            let _ = writeln!(
                out,
                "{} {} {} {}",
                s.red("✗"),
                s.bold("Not executable"),
                s.dim("·"),
                reason,
            );
        }
    }
}

fn tables_to_create(r: &MigrationReport) -> usize {
    r.pipelines
        .iter()
        .map(|p| {
            if p.cascade_tables.is_empty() {
                p.schema_changes
                    .iter()
                    .filter(|c| c.change_type == SchemaChangeType::CreateTable)
                    .count()
            } else {
                p.cascade_tables.iter().filter(|t| !t.dest_exists).count()
            }
        })
        .sum()
}
