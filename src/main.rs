use std::{
    io::{self, IsTerminal},
    net::SocketAddr,
    path::PathBuf,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use gtd::{
    client::{ApiClient, labels_from_pairs},
    domain::{Task, TaskEdit, TaskFilter, TaskList, TaskState},
    repository::{SqliteRepository, TaskRepository},
    server, tui,
};
use uuid::Uuid;

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
    /// Compact local task event history through the given revision.
    Compact {
        revision: i64,
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
        id: Option<Uuid>,
    },
    /// Mark a doing task done and move it to archive.
    Done {
        id: Uuid,
        /// Label in key:value form. May be repeated.
        #[arg(short, long = "label")]
        labels: Vec<String>,
        /// Replace the task description while completing it.
        #[arg(long)]
        description: Option<String>,
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
        Command::Compact { revision, database } => {
            let database = database
                .to_str()
                .context("database path is not valid UTF-8")?;
            let repository = SqliteRepository::new(database)
                .with_context(|| format!("failed to open SQLite database '{database}'"))?;
            let state = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to create Tokio runtime")?
                .block_on(repository.compact(revision))?;
            println!(
                "compacted task events: scheduled={}, finished={}, current={}",
                state.scheduled_revision, state.finished_revision, state.current_revision
            );
            Ok(())
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
                task.metadata.id, task.list, task.description
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
                    let tasks = client
                        .list(&TaskFilter {
                            list: Some(TaskList::NextAction),
                            state: Some(TaskState::Pending),
                            ..TaskFilter::default()
                        })?
                        .items;
                    if tasks.is_empty() {
                        println!("no pending task in next-action");
                        return Ok(());
                    }
                    let ids = tasks
                        .iter()
                        .map(|task| task.metadata.id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    bail!(
                        "pick needs a task ID when stdin is not a terminal; available IDs: {ids}"
                    );
                }
            };
            let task = client.update_state(
                id,
                TaskList::NextAction,
                TaskState::Doing,
                TaskEdit::default(),
            )?;
            println!("doing #{}: {}", task.metadata.id, task.description);
        }
        Command::Done {
            id,
            labels,
            description,
        } => {
            let task = client.update_state(
                id,
                TaskList::Archive,
                TaskState::Done,
                TaskEdit {
                    labels: labels_from_pairs(&labels)?,
                    description,
                },
            )?;
            println!(
                "archived #{} as done: {}",
                task.metadata.id, task.description
            );
        }
        Command::List {
            list,
            labels,
            state,
            json,
        } => {
            let tasks = client
                .list(&TaskFilter {
                    list: Some(list),
                    state,
                    labels: labels_from_pairs(&labels)?,
                })?
                .items;
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
        Command::Server { .. } | Command::Compact { .. } => unreachable!(),
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
            .labels
            .iter()
            .map(|(key, value)| format!("{key}:{value}"))
            .collect::<Vec<_>>()
            .join(",");
        let labels = if labels.is_empty() {
            String::new()
        } else {
            format!(" · [{labels}]")
        };
        println!(
            "#{} {:<7} {}{}",
            task.metadata.id, task.state, task.description, labels
        );
    }
}
