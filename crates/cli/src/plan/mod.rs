mod cards;
mod fmt;
mod sample;
mod sections;
mod style;

use engine_planner::plan::execution::migration_report::MigrationReport;
use std::fmt::Write as _;
use style::Sty;

/// Build the human summary. Pure (returns a `String`) so it is easy to test and
/// to route to either stdout or a file. ANSI styling is applied only when
/// `color` is true; `show_ddl` appends the exact DDL statements.
pub fn render(report: &MigrationReport, color: bool, show_ddl: bool) -> String {
    let s = Sty { color };
    let mut out = String::new();

    sections::header(&mut out, report, &s);
    sections::execution_rail(&mut out, report, &s);
    cards::pipeline_cards(&mut out, report, &s);
    sections::diagnostics(&mut out, report, &s);
    if show_ddl {
        sections::ddl(&mut out, report, &s);
    }
    sections::estimates(&mut out, report, &s);
    sections::verdict(&mut out, report, &s);

    // Trim trailing whitespace left by column padding
    out.lines().map(|l| format!("{}\n", l.trim_end())).collect()
}

/// A bold, upper-cased section heading.
fn section(out: &mut String, s: &Sty, title: &str) {
    let _ = writeln!(out, "{}", s.bold(&title.to_uppercase()));
}
