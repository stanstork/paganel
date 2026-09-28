use super::fmt::{BAR_W, bar, fmt_rows, plural, truncate_list};
use super::sample::card_sample;
use super::section;
use super::style::Sty;
use engine_planner::plan::{
    execution::migration_report::MigrationReport,
    pipeline::{cascade::CascadeTablePlan, plan::PipelinePlan},
    sample::preview::SampleDataPreview,
    schema::types::SchemaChangeType,
    transform::mapping::ColumnMapping,
};
use std::collections::BTreeMap;
use std::fmt::Write as _;

const NAME_W: usize = 18;
const FLOW_W: usize = 26;
const ROWS_W: usize = 9;
const LABEL_W: usize = 32;

/// One branch line under a pipeline card. `label` is plain (so it can be padded);
/// `dim_label` dims the whole label for structural facts (joins, dependencies).
struct Branch {
    glyph: String,
    label: String,
    detail: String,
    dim_label: bool,
}

pub(super) fn pipeline_cards(out: &mut String, r: &MigrationReport, s: &Sty) {
    if r.pipelines.is_empty() {
        return;
    }
    section(out, s, "Pipelines");

    let max_rows = if r.pipelines.len() > 1 {
        r.pipelines
            .iter()
            .map(|p| p.source.effective_row_count().value)
            .max()
            .unwrap_or(0)
    } else {
        0
    };

    for p in &r.pipelines {
        // A graph/cascade pipeline fans out into its FK closure; show one card per
        // discovered table instead of the parent's own branches.
        if !p.cascade_tables.is_empty() {
            cascade_group(out, p, s);
            continue;
        }

        card_header(out, p, s, max_rows);
        render_branches(
            out,
            &build_branches(p, s),
            p.sample.as_ref(),
            &p.mappings,
            s,
        );
        out.push('\n');
    }
}

/// Render a card's tree branches, followed by the sample block (if enabled),
/// which always closes the tree with `└`.
fn render_branches(
    out: &mut String,
    branches: &[Branch],
    sample: Option<&SampleDataPreview>,
    mappings: &[ColumnMapping],
    s: &Sty,
) {
    let has_sample = sample.is_some_and(|sp| sp.enabled);
    for (j, b) in branches.iter().enumerate() {
        // The sample block (if any) is the final branch, so it takes └.
        let last = j + 1 == branches.len() && !has_sample;
        let tree = s.dim(if last { "└" } else { "├" });
        if b.detail.is_empty() {
            // Structural facts (joins, deps) have no detail column - don't
            // pad the label into trailing whitespace.
            let label = if b.dim_label {
                s.dim(&b.label)
            } else {
                b.label.clone()
            };
            let _ = writeln!(out, "    {} {} {}", tree, b.glyph, label);
        } else {
            let padded = format!("{:<LABEL_W$}", b.label);
            let label = if b.dim_label { s.dim(&padded) } else { padded };
            let _ = writeln!(
                out,
                "    {} {} {}  {}",
                tree,
                b.glyph,
                label,
                s.dim(&b.detail)
            );
        }
    }

    if let Some(sp) = sample.filter(|sp| sp.enabled) {
        card_sample(out, mappings, sp, s);
    }
}

/// Render a graph/cascade pipeline as a group of per-table cards under a header
/// noting it is still one pipeline.
fn cascade_group(out: &mut String, p: &PipelinePlan, s: &Sty) {
    let n = p.cascade_tables.len();
    let _ = writeln!(
        out,
        "  {} {}  {}",
        s.bold(s.glyph_pipe()),
        s.bold(&format!("{:<NAME_W$}", p.name)),
        s.dim(&format!(
            "cascade from {} · {} table{} · 1 pipeline",
            p.source.table,
            n,
            plural(n),
        )),
    );

    let max_rows = p
        .cascade_tables
        .iter()
        .map(|t| t.row_count.value)
        .max()
        .unwrap_or(0);

    for t in &p.cascade_tables {
        cascade_card(out, t, s, max_rows);
    }
    out.push('\n');
}

fn cascade_card(out: &mut String, t: &CascadeTablePlan, s: &Sty, max_rows: u64) {
    let flow = format!("{} → {}", t.source_table, t.dest_table);
    let bar_field = format!("{:<BAR_W$}", bar(t.row_count.value, max_rows));
    let bar_str = if t.row_count.value == max_rows && max_rows > 0 {
        s.green(&bar_field)
    } else {
        s.dim(&bar_field)
    };
    let _ = writeln!(
        out,
        "  {} {:<NAME_W$}  {} {}  {:>ROWS_W$} rows",
        s.dim(s.glyph_pipe()),
        t.source_table,
        s.dim(&format!("{:<FLOW_W$}", flow)),
        bar_str,
        fmt_rows(&t.row_count),
    );

    render_branches(
        out,
        &cascade_branches(t, s),
        t.sample.as_ref(),
        &t.mappings,
        s,
    );
}

/// Branches for a single cascade table card: create-table, then type conversions.
fn cascade_branches(t: &CascadeTablePlan, s: &Sty) -> Vec<Branch> {
    let mut br = Vec::new();

    if !t.dest_exists {
        let pk = if t.primary_key.is_empty() {
            String::new()
        } else {
            format!(" · pk {}", t.primary_key.join(", "))
        };
        br.push(Branch {
            glyph: s.green("+"),
            label: format!("create table {}", t.dest_table),
            detail: format!("{} columns{pk}", t.columns),
            dim_label: false,
        });
    }

    br.extend(conversion_branches(&t.mappings, s));
    br
}

fn card_header(out: &mut String, p: &PipelinePlan, s: &Sty, max_rows: u64) {
    let rc = p.source.effective_row_count();
    let flow = format!("{} → {}", p.source.table, p.destination.table);
    let bar_field = format!("{:<BAR_W$}", bar(rc.value, max_rows));
    let bar_str = if rc.value == max_rows && max_rows > 0 {
        s.green(&bar_field)
    } else {
        s.dim(&bar_field)
    };

    let _ = writeln!(
        out,
        "  {} {}  {} {}  {:>ROWS_W$} rows",
        s.bold(s.glyph_pipe()),
        s.bold(&format!("{:<NAME_W$}", p.name)),
        s.dim(&format!("{:<FLOW_W$}", flow)),
        bar_str,
        fmt_rows(rc),
    );
}

fn build_branches(p: &PipelinePlan, s: &Sty) -> Vec<Branch> {
    let mut br = Vec::new();

    // + create table (single-table pipelines; cascade pipelines render per-table
    // cards via cascade_group instead).
    let creates_table = !p.destination.exists
        || p.schema_changes
            .iter()
            .any(|c| c.change_type == SchemaChangeType::CreateTable);
    if creates_table {
        let ncols = if !p.destination.columns.is_empty() {
            p.destination.columns.len()
        } else {
            p.source.columns.len()
        };
        let pk = if p.source.primary_key.is_empty() {
            String::new()
        } else {
            format!(" · pk {}", p.source.primary_key.join(", "))
        };
        br.push(Branch {
            glyph: s.green("+"),
            label: format!("create table {}", p.destination.table),
            detail: format!("{ncols} columns{pk}"),
            dim_label: false,
        });
    }

    // type conversions, grouped by (from, to, safe)
    br.extend(conversion_branches(&p.mappings, s));

    // − source columns not carried to the destination
    let excluded = &p.data_flow_summary.excluded_columns;
    if !excluded.is_empty() {
        let refs: Vec<&str> = excluded.iter().map(String::as_str).collect();
        let count = excluded.len();
        br.push(Branch {
            glyph: s.red("−"),
            label: format!("excludes {count} column{}", plural(count)),
            detail: truncate_list(&refs, 5),
            dim_label: false,
        });
    }

    // joins (structural)
    if !p.joins.is_empty() {
        let tables: Vec<&str> = p.joins.iter().map(|j| j.source_table.as_str()).collect();
        br.push(Branch {
            glyph: s.dim(s.glyph_join()),
            label: format!("joins {}", tables.join(", ")),
            detail: String::new(),
            dim_label: true,
        });
    }

    // dependencies (structural)
    if !p.depends_on.is_empty() {
        br.push(Branch {
            glyph: s.dim(s.glyph_dep()),
            label: format!("after {}", p.depends_on.join(", ")),
            detail: String::new(),
            dim_label: true,
        });
    }

    br
}

/// Type-conversion branches, grouped by (from, to, safe).
fn conversion_branches(mappings: &[ColumnMapping], s: &Sty) -> Vec<Branch> {
    let mut groups: BTreeMap<(String, String, bool), Vec<String>> = BTreeMap::new();
    for m in mappings {
        if let Some(c) = &m.type_conversion {
            groups
                .entry((c.from_type.clone(), c.to_type.clone(), c.is_safe))
                .or_default()
                .push(m.target.clone());
        }
    }
    let mut ordered: Vec<_> = groups.into_iter().collect();
    ordered.sort_by_key(|entry| !entry.0.2);

    ordered
        .into_iter()
        .map(|((from, to, safe), cols)| {
            let col_refs: Vec<&str> = cols.iter().map(String::as_str).collect();
            let cols_str = truncate_list(&col_refs, 4);
            if safe {
                Branch {
                    glyph: s.cyan("~"),
                    label: format!("{from} → {to}"),
                    detail: format!("{cols_str} · safe"),
                    dim_label: false,
                }
            } else {
                Branch {
                    glyph: s.yellow("⚠"),
                    label: format!("{from} → {to}"),
                    detail: format!("{cols_str} · lossy"),
                    dim_label: false,
                }
            }
        })
        .collect()
}
