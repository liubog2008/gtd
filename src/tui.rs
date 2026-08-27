use std::{io, time::Duration as StdDuration};

use anyhow::{Context, Result, bail};
use chrono::{Duration, Utc};
use crossterm::{
    event::{self, Event as CrosstermEvent, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};
use unicode_width::UnicodeWidthStr;

use crate::{
    client::{ApiClient, labels_from_pairs},
    domain::{ContextPatch, Task, TaskAction, TaskFilter, TaskList, TaskState, TransitionRequest},
};

type GtdTerminal = Terminal<CrosstermBackend<io::Stdout>>;

pub fn pick_task(client: &ApiClient) -> Result<Option<i32>> {
    let tasks = client.list(&TaskFilter {
        list: Some(TaskList::NextAction),
        state: Some(TaskState::Pending),
        ..TaskFilter::default()
    })?;
    if tasks.is_empty() {
        return Ok(None);
    }
    let mut terminal = TerminalSession::new()?;
    select_task(&mut terminal.terminal, "Pick a next action", &tasks)
        .map(|selection| selection.map(|index| tasks[index].id))
}

pub fn process_inbox(client: &ApiClient) -> Result<usize> {
    let tasks = client.list(&TaskFilter {
        list: Some(TaskList::Inbox),
        state: Some(TaskState::Pending),
        ..TaskFilter::default()
    })?;
    if tasks.is_empty() {
        return Ok(0);
    }

    let mut terminal = TerminalSession::new()?;
    let mut processed = 0;
    for (index, task) in tasks.iter().enumerate() {
        let title = format!("Process inbox · {} of {}", index + 1, tasks.len());
        let Some(actionable) = choose(
            &mut terminal.terminal,
            &title,
            task,
            "Is this actionable?",
            &[("y", "yes"), ("n", "no"), ("q", "quit")],
        )?
        else {
            break;
        };

        let continue_processing = if actionable == 'y' {
            process_actionable(&mut terminal.terminal, client, task, &title)?
        } else {
            process_non_actionable(&mut terminal.terminal, client, task, &title)?
        };
        if !continue_processing {
            break;
        }
        processed += 1;
    }
    Ok(processed)
}

pub fn review(client: &ApiClient) -> Result<usize> {
    let next_actions = client.list(&TaskFilter {
        list: Some(TaskList::NextAction),
        ..TaskFilter::default()
    })?;
    let someday = client.list(&TaskFilter {
        list: Some(TaskList::SomedayMaybe),
        state: Some(TaskState::Pending),
        ..TaskFilter::default()
    })?;
    if next_actions.is_empty() && someday.is_empty() {
        return Ok(0);
    }

    let mut terminal = TerminalSession::new()?;
    let mut reviewed = 0;

    for (index, task) in next_actions.iter().enumerate() {
        let title = format!(
            "Review next actions · {} of {}",
            index + 1,
            next_actions.len()
        );
        let Some(choice) = choose(
            &mut terminal.terminal,
            &title,
            task,
            "Keep it active, move it to someday/maybe, or trash it?",
            &[("k", "keep"), ("m", "maybe"), ("x", "trash"), ("q", "quit")],
        )?
        else {
            return Ok(reviewed);
        };
        match choice {
            'k' => {}
            'm' => {
                let revisit_at = ask_revisit_at(&mut terminal.terminal, &title, task, false)?;
                client.transition(
                    task.id,
                    TaskAction::Maybe,
                    TransitionRequest {
                        revisit_at,
                        ..TransitionRequest::default()
                    },
                )?;
            }
            'x' => {
                client.transition(task.id, TaskAction::Trash, TransitionRequest::default())?;
            }
            _ => unreachable!(),
        }
        reviewed += 1;
    }

    for (index, task) in someday.iter().enumerate() {
        let title = format!("Review someday/maybe · {} of {}", index + 1, someday.len());
        let Some(choice) = choose(
            &mut terminal.terminal,
            &title,
            task,
            "Keep it for later, activate it, or trash it?",
            &[
                ("k", "keep"),
                ("a", "activate"),
                ("x", "trash"),
                ("q", "quit"),
            ],
        )?
        else {
            return Ok(reviewed);
        };
        match choice {
            'k' => {}
            'a' => {
                client.transition(task.id, TaskAction::Activate, TransitionRequest::default())?;
            }
            'x' => {
                client.transition(task.id, TaskAction::Trash, TransitionRequest::default())?;
            }
            _ => unreachable!(),
        }
        reviewed += 1;
    }
    Ok(reviewed)
}

fn process_actionable(
    terminal: &mut GtdTerminal,
    client: &ApiClient,
    task: &Task,
    title: &str,
) -> Result<bool> {
    let Some(choice) = choose(
        terminal,
        title,
        task,
        "What should happen next?",
        &[
            ("d", "do it now"),
            ("f", "defer"),
            ("g", "delegate"),
            ("q", "quit"),
        ],
    )?
    else {
        return Ok(false);
    };

    match choice {
        'd' => {
            client.transition(task.id, TaskAction::Start, TransitionRequest::default())?;
            let Some(outcome) = choose(
                terminal,
                title,
                task,
                "Do it now · what was the outcome?",
                &[("d", "done"), ("x", "trash"), ("q", "quit and leave doing")],
            )?
            else {
                return Ok(false);
            };
            match outcome {
                'd' => {
                    let context = edit_context(terminal, title, task)?;
                    client.transition(
                        task.id,
                        TaskAction::Done,
                        TransitionRequest {
                            context,
                            revisit_at: None,
                        },
                    )?;
                }
                'x' => {
                    client.transition(task.id, TaskAction::Trash, TransitionRequest::default())?;
                }
                _ => unreachable!(),
            }
        }
        'f' | 'g' => {
            let context = edit_context(terminal, title, task)?;
            let action = if choice == 'f' {
                TaskAction::Defer
            } else {
                TaskAction::Delegate
            };
            client.transition(
                task.id,
                action,
                TransitionRequest {
                    context,
                    revisit_at: None,
                },
            )?;
        }
        _ => unreachable!(),
    }
    Ok(true)
}

fn process_non_actionable(
    terminal: &mut GtdTerminal,
    client: &ApiClient,
    task: &Task,
    title: &str,
) -> Result<bool> {
    let Some(choice) = choose(
        terminal,
        title,
        task,
        "What should happen to it?",
        &[("x", "trash"), ("m", "maybe"), ("q", "quit")],
    )?
    else {
        return Ok(false);
    };
    match choice {
        'x' => {
            client.transition(task.id, TaskAction::Trash, TransitionRequest::default())?;
        }
        'm' => {
            let revisit_at = ask_revisit_at(terminal, title, task, true)?;
            client.transition(
                task.id,
                TaskAction::Maybe,
                TransitionRequest {
                    revisit_at,
                    ..TransitionRequest::default()
                },
            )?;
        }
        _ => unreachable!(),
    }
    Ok(true)
}

fn edit_context(terminal: &mut GtdTerminal, title: &str, task: &Task) -> Result<ContextPatch> {
    let labels = input(
        terminal,
        title,
        task,
        "Labels (comma-separated key:value; blank is fine)",
    )?
    .unwrap_or_default();
    let pairs = labels
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let labels = labels_from_pairs(&pairs)?;
    let note = input(terminal, title, task, "Context note (blank is fine)")?
        .filter(|value| !value.trim().is_empty());
    Ok(ContextPatch { labels, note })
}

fn ask_revisit_at(
    terminal: &mut GtdTerminal,
    title: &str,
    task: &Task,
    required: bool,
) -> Result<Option<chrono::DateTime<Utc>>> {
    loop {
        let prompt = if required {
            "When should it return to inbox? Examples: 30m, 2h, 7d, 4w"
        } else {
            "Optional return delay (30m, 2h, 7d, 4w); leave blank for someday"
        };
        let Some(raw) = input(terminal, title, task, prompt)? else {
            if required {
                continue;
            }
            return Ok(None);
        };
        if raw.trim().is_empty() && !required {
            return Ok(None);
        }
        match parse_delay(&raw) {
            Ok(delay) => return Ok(Some(Utc::now() + delay)),
            Err(error) => {
                show_message(terminal, title, task, &error.to_string())?;
            }
        }
    }
}

pub fn parse_delay(value: &str) -> Result<Duration> {
    let value = value.trim();
    if value.len() < 2 {
        bail!("delay must look like 30m, 2h, 7d, or 4w");
    }
    let (amount, unit) = value.split_at(value.len() - 1);
    let amount: i64 = amount
        .parse()
        .with_context(|| format!("invalid delay '{value}'"))?;
    if amount <= 0 {
        bail!("delay must be greater than zero");
    }
    let delay = match unit {
        "m" => Duration::try_minutes(amount),
        "h" => Duration::try_hours(amount),
        "d" => Duration::try_days(amount),
        "w" => Duration::try_weeks(amount),
        _ => bail!("unknown delay unit '{unit}'; use m, h, d, or w"),
    };
    delay.context("delay is too large")
}

struct TerminalSession {
    terminal: GtdTerminal,
}

impl TerminalSession {
    fn new() -> Result<Self> {
        enable_raw_mode().context("failed to enable terminal raw mode")?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(stdout, EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error).context("failed to enter alternate screen");
        }
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).context("failed to initialize terminal")?;
        Ok(Self { terminal })
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

fn choose(
    terminal: &mut GtdTerminal,
    title: &str,
    task: &Task,
    prompt: &str,
    choices: &[(&str, &str)],
) -> Result<Option<char>> {
    loop {
        terminal.draw(|frame| render_choice(frame, title, task, prompt, choices))?;
        if event::poll(StdDuration::from_millis(250))?
            && let CrosstermEvent::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Esc => return Ok(None),
                KeyCode::Char(character) => {
                    let character = character.to_ascii_lowercase();
                    if character == 'q' {
                        return Ok(None);
                    }
                    if choices
                        .iter()
                        .any(|(shortcut, _)| shortcut.starts_with(character))
                    {
                        return Ok(Some(character));
                    }
                }
                _ => {}
            }
        }
    }
}

fn input(
    terminal: &mut GtdTerminal,
    title: &str,
    task: &Task,
    prompt: &str,
) -> Result<Option<String>> {
    let mut value = String::new();
    loop {
        terminal.draw(|frame| render_input(frame, title, task, prompt, &value))?;
        if let CrosstermEvent::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Esc => return Ok(None),
                KeyCode::Enter => return Ok(Some(value)),
                KeyCode::Backspace => {
                    value.pop();
                }
                KeyCode::Char(character) => value.push(character),
                _ => {}
            }
        }
    }
}

fn select_task(terminal: &mut GtdTerminal, title: &str, tasks: &[Task]) -> Result<Option<usize>> {
    let mut selected = 0usize;
    loop {
        terminal.draw(|frame| {
            let area = frame.area();
            let items = tasks
                .iter()
                .map(|task| ListItem::new(format!("#{:<4} {}", task.id, task.description)))
                .collect::<Vec<_>>();
            let list = List::new(items)
                .block(Block::default().title(title).borders(Borders::ALL))
                .highlight_style(
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
                .highlight_symbol("› ");
            let mut state = ListState::default().with_selected(Some(selected));
            frame.render_stateful_widget(list, area, &mut state);
        })?;
        if let CrosstermEvent::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => return Ok(None),
                KeyCode::Enter => return Ok(Some(selected)),
                KeyCode::Up | KeyCode::Char('k') => selected = selected.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    selected = (selected + 1).min(tasks.len() - 1)
                }
                _ => {}
            }
        }
    }
}

fn render_choice(
    frame: &mut Frame,
    title: &str,
    task: &Task,
    prompt: &str,
    choices: &[(&str, &str)],
) {
    let areas = Layout::vertical([
        Constraint::Length(5),
        Constraint::Length(4),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .split(frame.area());
    frame.render_widget(task_widget(title, task), areas[0]);
    frame.render_widget(
        Paragraph::new(prompt)
            .style(Style::default().add_modifier(Modifier::BOLD))
            .block(Block::default().title("Decision").borders(Borders::ALL)),
        areas[1],
    );
    let choice_line = choices
        .iter()
        .flat_map(|(key, label)| {
            [
                Span::styled(
                    format!(" [{key}] "),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!("{label}  ")),
            ]
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(Line::from(choice_line)).wrap(Wrap { trim: true }),
        areas[2],
    );
    frame.render_widget(
        Paragraph::new("Esc/q quits · each decision is saved immediately"),
        areas[3],
    );
}

fn render_input(frame: &mut Frame, title: &str, task: &Task, prompt: &str, value: &str) {
    let areas = Layout::vertical([
        Constraint::Length(5),
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(1),
    ])
    .split(frame.area());
    frame.render_widget(task_widget(title, task), areas[0]);
    frame.render_widget(Paragraph::new(prompt).wrap(Wrap { trim: true }), areas[1]);
    frame.render_widget(
        Paragraph::new(value).block(Block::default().title("Input").borders(Borders::ALL)),
        areas[2],
    );
    frame.render_widget(
        Paragraph::new("Enter confirms · Esc leaves this field blank"),
        areas[3],
    );
    let input_width = UnicodeWidthStr::width(value).min(u16::MAX as usize) as u16;
    let cursor_x = areas[2]
        .x
        .saturating_add(1)
        .saturating_add(input_width)
        .min(areas[2].x + areas[2].width.saturating_sub(2));
    frame.set_cursor_position((cursor_x, areas[2].y + 1));
}

fn task_widget<'a>(title: &'a str, task: &'a Task) -> Paragraph<'a> {
    Paragraph::new(vec![
        Line::from(Span::styled(
            format!("#{}  {}", task.id, task.description),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(format!("{} / {}", task.list, task.state)),
    ])
    .block(Block::default().title(title).borders(Borders::ALL))
    .wrap(Wrap { trim: true })
}

fn show_message(terminal: &mut GtdTerminal, title: &str, task: &Task, message: &str) -> Result<()> {
    loop {
        terminal.draw(|frame| {
            let areas = Layout::vertical([
                Constraint::Length(5),
                Constraint::Min(3),
                Constraint::Length(1),
            ])
            .split(frame.area());
            frame.render_widget(task_widget(title, task), areas[0]);
            frame.render_widget(
                Paragraph::new(message)
                    .style(Style::default().fg(Color::Red))
                    .block(
                        Block::default()
                            .title("Invalid input")
                            .borders(Borders::ALL),
                    ),
                areas[1],
            );
            frame.render_widget(Paragraph::new("Press any key to try again"), areas[2]);
        })?;
        if let CrosstermEvent::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_delays() {
        assert_eq!(parse_delay("30m").unwrap(), Duration::minutes(30));
        assert_eq!(parse_delay("2h").unwrap(), Duration::hours(2));
        assert_eq!(parse_delay("7d").unwrap(), Duration::days(7));
        assert_eq!(parse_delay("4w").unwrap(), Duration::weeks(4));
    }

    #[test]
    fn rejects_non_positive_or_unknown_delays() {
        assert!(parse_delay("0d").is_err());
        assert!(parse_delay("2months").is_err());
    }
}
