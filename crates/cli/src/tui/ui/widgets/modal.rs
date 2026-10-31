use crate::format::commas;
use crate::tui::{
    app::{command::MigrationCommand, core::App, state::IntegrityProgress},
    ui::{
        constants::{colors, styles},
        formatters::{format_compact_number, format_duration},
    },
};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

/// Modal state variants
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ModalState {
    #[default]
    None,
    QuitConfirmation,
    ConfirmAction {
        title: String,
        message: String,
        command: MigrationCommand,
    },
    IntegrityFinalizing,
    Notice {
        title: String,
        message: String,
    },
    MigrationCompleted {
        total_rows: u64,
        duration: std::time::Duration,
        avg_throughput: f64,
        warnings: usize,
        errors: usize,
        skipped: u64,
    },
    MigrationFailed {
        pipeline_name: String,
        error_message: String,
        error_count: usize,
    },
}

/// Render modal overlay based on the app's current modal state.
pub fn render(frame: &mut Frame, app: &App) {
    match &app.modal_state {
        ModalState::None => {}
        ModalState::QuitConfirmation => render_quit_confirmation(frame),
        ModalState::ConfirmAction { title, message, .. } => {
            render_confirm_action(frame, title, message)
        }
        ModalState::IntegrityFinalizing => {
            render_integrity_finalizing(frame, &app.integrity, app.run_finished)
        }
        ModalState::Notice { title, message } => render_notice(frame, title, message),
        ModalState::MigrationCompleted {
            total_rows,
            duration,
            avg_throughput,
            warnings,
            errors,
            skipped,
        } => render_migration_completed(
            frame,
            *total_rows,
            *duration,
            *avg_throughput,
            *warnings,
            *errors,
            *skipped,
        ),
        ModalState::MigrationFailed {
            pipeline_name,
            error_message,
            error_count,
        } => render_migration_failed(frame, pipeline_name, error_message, *error_count),
    }
}

/// A yes/no confirmation dialog for a destructive control.
fn render_confirm_action(frame: &mut Frame, title: &str, message: &str) {
    let area = centered_rect(60, 35, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::STATUS_PAUSED))
        .title(format!(" ⚠ {title} "))
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(colors::BACKGROUND));

    frame.render_widget(block, area);

    let inner = area.inner(ratatui::layout::Margin {
        vertical: 2,
        horizontal: 2,
    });

    let mut content = vec![Line::from("")];
    for line in message.lines() {
        content.push(Line::from(Span::styled(
            line.to_string(),
            styles::value_bold(),
        )));
    }
    content.push(Line::from(""));
    content.push(Line::from(""));
    content.push(Line::from(vec![
        Span::raw("    ["),
        Span::styled(
            "y",
            Style::default()
                .fg(colors::STATUS_PAUSED)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("] "),
        Span::styled("Yes", styles::value_bold()),
        Span::raw("     ["),
        Span::styled(
            "n",
            Style::default()
                .fg(colors::STATUS_PAUSED)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("] "),
        Span::styled("No", styles::value_bold()),
    ]));

    let paragraph = Paragraph::new(content)
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true });

    frame.render_widget(paragraph, inner);
}

/// A terminal informational modal (paused / cancelled).
fn render_notice(frame: &mut Frame, title: &str, message: &str) {
    let area = centered_rect(60, 30, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::STATUS_PAUSED))
        .title(format!(" {title} "))
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(colors::BACKGROUND));

    frame.render_widget(block, area);

    let inner = area.inner(ratatui::layout::Margin {
        vertical: 2,
        horizontal: 2,
    });

    let mut content = vec![Line::from("")];
    for line in message.lines() {
        content.push(Line::from(Span::styled(
            line.to_string(),
            styles::value_bold(),
        )));
    }
    content.push(Line::from(""));
    content.push(Line::from(""));
    content.push(Line::from(vec![
        Span::raw("    Press ["),
        Span::styled(
            "q",
            Style::default()
                .fg(colors::STATUS_PAUSED)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("] to exit"),
    ]));

    let paragraph = Paragraph::new(content)
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true });

    frame.render_widget(paragraph, inner);
}

/// A modal shown while `--integrity` seals and commits receipts.
fn render_integrity_finalizing(frame: &mut Frame, integrity: &IntegrityProgress, done: bool) {
    let area = centered_rect(60, 40, frame.area());
    frame.render_widget(Clear, area);

    let (border, title) = if done {
        (colors::STATUS_RUNNING, " ✓ INTEGRITY RECEIPTS COMMITTED ")
    } else {
        (colors::STATUS_QUEUED, " ⧗ SEALING INTEGRITY RECEIPTS ")
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border))
        .title(title)
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(colors::BACKGROUND));

    frame.render_widget(block, area);

    let inner = area.inner(ratatui::layout::Margin {
        vertical: 2,
        horizontal: 2,
    });

    let activity = if done {
        "All verification receipts committed.".to_string()
    } else {
        match &integrity.current_table {
            Some(table) => format!("Sealing '{table}'…"),
            None => "Folding Merkle roots…".to_string(),
        }
    };

    let footer = if done {
        Line::from(vec![
            Span::raw("            Press ["),
            Span::styled(
                "Enter",
                Style::default()
                    .fg(colors::STATUS_RUNNING)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("] to continue"),
        ])
    } else {
        Line::from(Span::styled(
            "Committing verification receipts - please wait.",
            styles::muted(),
        ))
    };

    let content = vec![
        Line::from(""),
        Line::from(Span::styled(activity, styles::value_bold())),
        Line::from(""),
        Line::from(vec![
            Span::styled("Receipts committed:  ", styles::label()),
            Span::styled(integrity.receipts_done.to_string(), styles::value_bold()),
        ]),
        Line::from(""),
        footer,
    ];

    let paragraph = Paragraph::new(content)
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true })
        .style(styles::value_bold());

    frame.render_widget(paragraph, inner);
}

fn render_quit_confirmation(frame: &mut Frame) {
    let area = centered_rect(60, 30, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::STATUS_PAUSED))
        .title(" ⚠ Confirm Quit ")
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(colors::BACKGROUND));

    frame.render_widget(block, area);

    let inner = area.inner(ratatui::layout::Margin {
        vertical: 2,
        horizontal: 2,
    });

    let content = vec![
        Line::from(""),
        Line::from("Migration is still in progress."),
        Line::from("A checkpoint will be saved."),
        Line::from(""),
        Line::from("Are you sure you want to quit?"),
        Line::from(""),
        Line::from(""),
        Line::from(vec![
            Span::raw("    ["),
            Span::styled(
                "y",
                Style::default()
                    .fg(colors::STATUS_PAUSED)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("] "),
            Span::styled("Yes, quit", styles::value_bold()),
        ]),
        Line::from(vec![
            Span::raw("    ["),
            Span::styled(
                "n",
                Style::default()
                    .fg(colors::STATUS_PAUSED)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("] "),
            Span::styled("No, continue", styles::value_bold()),
        ]),
    ];

    let paragraph = Paragraph::new(content)
        .alignment(Alignment::Left)
        .style(styles::value_bold());

    frame.render_widget(paragraph, inner);
}

fn render_migration_completed(
    frame: &mut Frame,
    total_rows: u64,
    duration: std::time::Duration,
    avg_throughput: f64,
    warnings: usize,
    errors: usize,
    skipped: u64,
) {
    let area = centered_rect(60, 40, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::STATUS_RUNNING))
        .title(" ✓ MIGRATION COMPLETED ")
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(colors::BACKGROUND));

    frame.render_widget(block, area);

    let inner = area.inner(ratatui::layout::Margin {
        vertical: 2,
        horizontal: 2,
    });

    let duration_str = format_duration(duration);

    let content = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("Total Rows:      ", styles::label()),
            Span::styled(format!("{:>10}", commas(total_rows)), styles::value_bold()),
        ]),
        Line::from(vec![
            Span::styled("Duration:        ", styles::label()),
            Span::styled(format!("{:>10}", duration_str), styles::value_bold()),
        ]),
        Line::from(vec![
            Span::styled("Avg Throughput:  ", styles::label()),
            Span::styled(
                format!(
                    "{:>10} rows/sec",
                    format_compact_number(avg_throughput as u64)
                ),
                styles::value_bold(),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Warnings:        ", styles::label()),
            Span::styled(
                format!("{:>10}", warnings),
                if warnings > 0 {
                    styles::warning()
                } else {
                    styles::value_bold()
                },
            ),
        ]),
        Line::from(vec![
            Span::styled("Errors:          ", styles::label()),
            Span::styled(
                format!("{:>10}", errors),
                if errors > 0 {
                    styles::error()
                } else {
                    styles::value_bold()
                },
            ),
        ]),
        Line::from(vec![
            Span::styled("Skipped:         ", styles::label()),
            Span::styled(format!("{:>10} rows", skipped), styles::value_bold()),
        ]),
        Line::from(""),
        Line::from(""),
        Line::from(vec![
            Span::raw("            Press ["),
            Span::styled(
                "q",
                Style::default()
                    .fg(colors::STATUS_RUNNING)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("] to exit"),
        ]),
        Line::from(vec![
            Span::raw("            Press ["),
            Span::styled(
                "e",
                Style::default()
                    .fg(colors::STATUS_RUNNING)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("] to view warnings"),
        ]),
    ];

    let paragraph = Paragraph::new(content)
        .alignment(Alignment::Left)
        .style(styles::value_bold());

    frame.render_widget(paragraph, inner);
}

fn render_migration_failed(
    frame: &mut Frame,
    pipeline_name: &str,
    error_message: &str,
    error_count: usize,
) {
    let area = centered_rect(70, 40, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::STATUS_FAILED))
        .title(" ✗ MIGRATION FAILED ")
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(colors::BACKGROUND));

    frame.render_widget(block, area);

    let inner = area.inner(ratatui::layout::Margin {
        vertical: 2,
        horizontal: 2,
    });

    let content = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("Failed Pipeline: ", styles::label()),
            Span::styled(
                pipeline_name,
                Style::default()
                    .fg(colors::STATUS_FAILED)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Error: ", styles::label()),
            Span::styled(error_message, styles::value_bold()),
        ]),
        Line::from(""),
        Line::from("Checkpoint saved. Run again to resume."),
        Line::from(""),
        Line::from(""),
        Line::from(vec![
            Span::raw("            Press ["),
            Span::styled(
                "r",
                Style::default()
                    .fg(colors::STATUS_FAILED)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("] to retry failed pipeline"),
        ]),
        Line::from(vec![
            Span::raw("            Press ["),
            Span::styled(
                "e",
                Style::default()
                    .fg(colors::STATUS_FAILED)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("] to view errors ("),
            Span::styled(
                format!("{}", error_count),
                Style::default().fg(colors::STATUS_FAILED),
            ),
            Span::raw(")"),
        ]),
        Line::from(vec![
            Span::raw("            Press ["),
            Span::styled(
                "q",
                Style::default()
                    .fg(colors::STATUS_FAILED)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("] to exit"),
        ]),
    ];

    let paragraph = Paragraph::new(content)
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true })
        .style(styles::value_bold());

    frame.render_widget(paragraph, inner);
}

/// Create a centered rectangle for modal positioning
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_modal_state_default() {
        assert_eq!(ModalState::default(), ModalState::None);
    }
}
