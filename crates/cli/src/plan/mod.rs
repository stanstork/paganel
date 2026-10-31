mod cards;
mod fmt;
pub mod json;
mod sample;
mod sections;
mod style;

use engine_core::plan::pair::PairSpec;
use engine_planner::plan::execution::migration_report::MigrationReport;
use std::fmt::Write as _;
use style::Sty;

/// Build the human summary. Pure (returns a `String`) so it is easy to test and
/// to route to either stdout or a file. ANSI styling is applied only when
/// `color` is true; `show_ddl` appends the exact DDL statements. `pairs` are
/// listed after the pipelines: `apply` skips them, `pag drift` checks them.
pub fn render(report: &MigrationReport, pairs: &[PairSpec], color: bool, show_ddl: bool) -> String {
    let s = Sty { color };
    let mut out = String::new();

    sections::header(&mut out, report, pairs.len(), &s);
    sections::execution_rail(&mut out, report, &s);
    cards::pipeline_cards(&mut out, report, &s);
    cards::pair_cards(&mut out, pairs, &s);
    sections::diagnostics(&mut out, report, &s);
    if show_ddl {
        sections::ddl(&mut out, report, &s);
    }
    sections::estimates(&mut out, report, &s);
    sections::verdict(&mut out, report, &s);

    // Trim trailing whitespace left by column padding
    out.lines().map(|l| format!("{}\n", l.trim_end())).collect()
}

/// Just the config's `pair` blocks, for a config with no pipelines to plan.
pub fn pairs(pairs: &[PairSpec], color: bool) -> String {
    let s = Sty { color };
    let mut out = String::new();

    cards::pair_cards(&mut out, pairs, &s);
    out.lines().map(|l| format!("{}\n", l.trim_end())).collect()
}

/// A bold, upper-cased section heading.
fn section(out: &mut String, s: &Sty, title: &str) {
    let _ = writeln!(out, "{}", s.bold(&title.to_uppercase()));
}
