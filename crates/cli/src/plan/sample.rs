use super::fmt::{pad, plural, trunc};
use super::style::Sty;
use engine_planner::plan::sample::row::SampleValue;
use engine_planner::plan::{
    sample::preview::SampleDataPreview,
    sample::row::{SampleRow, SampleRowStatus},
    transform::mapping::ColumnMapping,
};
use std::collections::HashMap;
use std::fmt::Write as _;

const SAMPLE_MAX_ROWS: usize = 5;
const SAMPLE_MAX_COLS: usize = 6;
const SAMPLE_CELL_W: usize = 22;

/// Render a `--sample` preview as the card's last tree branch.
pub(super) fn card_sample(
    out: &mut String,
    mappings: &[ColumnMapping],
    sp: &SampleDataPreview,
    s: &Sty,
) {
    let cols = sample_columns(mappings, sp);
    let st = &sp.stats;
    let stat_line = if st.warnings + st.skipped + st.errors > 0 {
        let mut parts = Vec::new();
        if st.ok > 0 {
            parts.push(format!("{} ok", st.ok));
        }
        if st.warnings > 0 {
            parts.push(format!("{} warning{}", st.warnings, plural(st.warnings)));
        }
        if st.skipped > 0 {
            parts.push(format!("{} skipped", st.skipped));
        }
        if st.errors > 0 {
            parts.push(format!("{} failed", st.errors));
        }
        format!("  {}", parts.join(" · "))
    } else {
        String::new()
    };

    if let Some(reason) = &sp.unavailable_reason {
        let _ = writeln!(
            out,
            "    {} {} {}",
            s.dim("└"),
            s.dim("▪"),
            s.dim(&format!("sample unavailable · {reason}")),
        );
        return;
    }

    let _ = writeln!(
        out,
        "    {} {} {}{}",
        s.dim("└"),
        s.dim("▪"),
        s.dim(&format!("sample · {}", sample_caption(sp))),
        s.dim(&stat_line),
    );

    if cols.is_empty() {
        let _ = writeln!(
            out,
            "        {}",
            s.dim("no mapped columns to preview (add a select block)")
        );
        return;
    }

    let shown: Vec<&SampleRow> = sp.rows.iter().take(SAMPLE_MAX_ROWS).collect();
    let show_status = shown.iter().any(|r| r.status != SampleRowStatus::Ok);
    let widths: Vec<usize> = cols
        .iter()
        .map(|c| {
            shown
                .iter()
                .map(|row| cell(row, c).chars().count())
                .chain(std::iter::once(c.chars().count()))
                .max()
                .unwrap_or(0)
                .min(SAMPLE_CELL_W)
        })
        .collect();

    let row_line = |cells: &[String]| {
        cells
            .iter()
            .zip(&widths)
            .map(|(v, w)| pad(&trunc(v, *w), *w))
            .collect::<Vec<_>>()
            .join(" │ ")
    };

    let _ = writeln!(out, "        {}", s.dim(&row_line(&cols)));
    for row in &shown {
        let cells: Vec<String> = cols.iter().map(|c| cell(row, c)).collect();
        if show_status {
            let _ = writeln!(
                out,
                "      {} {}",
                status_glyph(&row.status, s),
                row_line(&cells)
            );
        } else {
            let _ = writeln!(out, "        {}", row_line(&cells));
        }
    }
    if sp.rows.len() > SAMPLE_MAX_ROWS {
        let _ = writeln!(
            out,
            "        {}",
            s.dim(&format!("… {} more rows", sp.rows.len() - SAMPLE_MAX_ROWS))
        );
    }
}

fn sample_columns(mappings: &[ColumnMapping], sp: &SampleDataPreview) -> Vec<String> {
    let Some(first) = sp.rows.first() else {
        return Vec::new();
    };
    let cells = row_cells(first);
    if cells.is_empty() {
        return Vec::new();
    }

    let mut cols: Vec<String> = mappings
        .iter()
        .map(|m| m.target.clone())
        .filter(|t| cells.contains_key(t))
        .collect();

    let mut extra: Vec<String> = cells
        .keys()
        .filter(|k| !cols.contains(k))
        .cloned()
        .collect();

    extra.sort();
    cols.extend(extra);

    cols.truncate(SAMPLE_MAX_COLS);
    cols
}

fn sample_caption(sp: &SampleDataPreview) -> String {
    use engine_planner::plan::sample::method::SamplingMethod;
    let n = sp.sample_size;
    match sp.sampling_method {
        SamplingMethod::First => format!("first {n} rows"),
        SamplingMethod::Random => format!("{n} random rows"),
        SamplingMethod::Stratified => format!("{n} stratified rows"),
        SamplingMethod::ById => format!("{n} rows by id"),
    }
}

fn row_cells(row: &SampleRow) -> &HashMap<String, SampleValue> {
    match &row.output {
        Some(o) if !o.is_empty() => o,
        _ => &row.input,
    }
}

fn status_glyph(status: &SampleRowStatus, s: &Sty) -> String {
    match status {
        SampleRowStatus::Ok => s.green("✓"),
        SampleRowStatus::Warning => s.yellow("⚠"),
        SampleRowStatus::Skipped => s.dim("⊘"),
        SampleRowStatus::Failed => s.red("✗"),
    }
}

fn cell(row: &SampleRow, col: &str) -> String {
    match row_cells(row).get(col) {
        Some(v) if v.is_null => "NULL".to_string(),
        Some(v) => trunc(&v.display, SAMPLE_CELL_W),
        None => String::new(),
    }
}
