use std::{
    io::{self, IsTerminal},
    net::SocketAddr,
    path::PathBuf,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use gtd::{
    client::{ApiClient, labels_from_pairs},
    domain::{ContextPatch, Task, TaskAction, TaskFilter, TaskList, TaskState, TransitionRequest},
    server, tui,
};

#[derive(Debug, Parser)]
#[command(name = "gtd", version, about = "A pragmatic GTD task manager")]
struct Cli {
    /// Base URL of the GTD server.
    #[arg(
        long,
        global = true,
        env = "GTD_SERVER_URL",
        default_value = "http://127.0.0.1:4040"
    )]
    server_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the HTTP server and initialize its SQLite database.
    Server {
        #[arg(long, default_value = "127.0.0.1:4040")]
        bind: SocketAddr,
        #[arg(long, env = "GTD_DATABASE", default_value = "gtd.db")]
        database: PathBuf,
    },
    /// Capture a short description in the in list.
    Add {
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        description: Vec<String>,
    },
    /// Select a pending next action and mark it doing.
    Pick {
        /// Task ID. Omit it to choose interactively.
        id: Option<i32>,
    },
    /// Mark a doing task done and move it to archive.
    Done {
        id: i32,
        /// Context label in key:value form. May be repeated.
        #[arg(short, long = "label")]
        labels: Vec<String>,
        /// Optional context note to add.
        #[arg(long)]
        note: Option<String>,
    },
    /// List tasks in one GTD list.
    List {
        /// in, next-action, waiting-for, someday-maybe, or archive.
        list: TaskList,
        /// Require a label in key:value form. May be repeated.
        #[arg(short, long = "label")]
        labels: Vec<String>,
        /// Optionally restrict the task state.
        #[arg(long)]
        state: Option<TaskState>,
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Interactively clarify every pending task in the in list.
    Process,
    /// Interactively review pending next-action and someday/maybe tasks.
    Review,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Server { bind, database } => {
            let database = database
                .to_str()
                .context("database path is not valid UTF-8")?
                .to_owned();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to create Tokio runtime")?
                .block_on(server::run(bind, &database))
        }
        command => run_client_command(&cli.server_url, command),
    }
}

fn run_client_command(server_url: &str, command: Command) -> Result<()> {
    let client = ApiClient::new(server_url)?;
    match command {
        Command::Add { description } => {
            let task = client.create(description.join(" "))?;
            println!(
                "captured #{} in {}: {}",
                task.id, task.list, task.description
            );
        }
        Command::Pick { id } => {
            let id = match id {
                Some(id) => id,
                None if io::stdin().is_terminal() && io::stdout().is_terminal() => {
                    let Some(id) = tui::pick_task(&client)? else {
                        println!("no pending task in next-action");
                        return Ok(());
                    };
                    id
                }
                None => {
                    let tasks = client.list(&TaskFilter {
                        list: Some(TaskList::NextAction),
                        state: Some(TaskState::Pending),
                        ..TaskFilter::default()
                    })?;
                    if tasks.is_empty() {
                        println!("no pending task in next-action");
                        return Ok(());
                    }
                    let ids = tasks
                        .iter()
                        .map(|task| task.id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    bail!(
                        "pick needs a task ID when stdin is not a terminal; available IDs: {ids}"
                    );
                }
            };
            let task = client.transition(id, TaskAction::Pick, TransitionRequest::default())?;
            println!("doing #{}: {}", task.id, task.description);
        }
        Command::Done { id, labels, note } => {
            let task = client.transition(
                id,
                TaskAction::Done,
                TransitionRequest {
                    context: ContextPatch {
                        labels: labels_from_pairs(&labels)?,
                        note,
                    },
                    revisit_at: None,
                },
            )?;
            println!("archived #{} as done: {}", task.id, task.description);
        }
        Command::List {
            list,
            labels,
            state,
            json,
        } => {
            let tasks = client.list(&TaskFilter {
                list: Some(list),
                state,
                labels: labels_from_pairs(&labels)?,
            })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&tasks)?);
            } else {
                print_tasks(&tasks);
            }
        }
        Command::Process => {
            require_terminal("process")?;
            let count = tui::process_inbox(&client)?;
            println!("processed {count} inbox task(s)");
        }
        Command::Review => {
            require_terminal("review")?;
            let count = tui::review(&client)?;
            println!("reviewed {count} task(s)");
        }
        Command::Server { .. } => unreachable!(),
    }
    Ok(())
}

fn require_terminal(command: &str) -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!("'{command}' requires an interactive terminal");
    }
    Ok(())
}

fn print_tasks(tasks: &[Task]) {
    if tasks.is_empty() {
        println!("no tasks");
        return;
    }
    for task in tasks {
        let labels = task
            .context
            .labels
            .iter()
            .map(|(key, value)| format!("{key}:{value}"))
            .collect::<Vec<_>>()
            .join(",");
        let context = match (&task.context.note, labels.is_empty()) {
            (None, true) => String::new(),
            (Some(note), true) => format!(" · {note}"),
            (None, false) => format!(" · [{labels}]"),
            (Some(note), false) => format!(" · [{labels}] {note}"),
        };
        let revisit = task
            .revisit_at
            .map(|date| format!(" · returns {}", date.to_rfc3339()))
            .unwrap_or_default();
        println!(
            "#{:<4} {:<7} {}{}{}",
            task.id, task.state, task.description, context, revisit
        );
    }
}
