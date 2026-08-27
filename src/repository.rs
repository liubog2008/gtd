use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
    sqlite::SqliteConnection,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use thiserror::Error;

use crate::{
    domain::{
        ContextPatch, Task, TaskAction, TaskContext, TaskFilter, TaskList, TaskState,
        TransitionRequest,
    },
    schema::{labels, tasks},
};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

type DbPool = Pool<ConnectionManager<SqliteConnection>>;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("task {0} was not found")]
    NotFound(i32),
    #[error("invalid request: {0}")]
    Validation(String),
    #[error("task transition conflict: {0}")]
    Conflict(String),
    #[error("database error: {0}")]
    Database(String),
}

impl StoreError {
    fn database(error: impl std::fmt::Display) -> Self {
        Self::Database(error.to_string())
    }
}

impl From<diesel::result::Error> for StoreError {
    fn from(value: diesel::result::Error) -> Self {
        Self::database(value)
    }
}

#[async_trait]
pub trait TaskRepository: Send + Sync {
    async fn create(&self, description: String) -> Result<Task, StoreError>;
    async fn get(&self, id: i32) -> Result<Task, StoreError>;
    async fn list(&self, filter: TaskFilter) -> Result<Vec<Task>, StoreError>;
    async fn transition(
        &self,
        id: i32,
        action: TaskAction,
        request: TransitionRequest,
    ) -> Result<Task, StoreError>;
    async fn revive_due(&self, now: DateTime<Utc>) -> Result<Vec<Task>, StoreError>;
}

#[derive(Clone)]
pub struct SqliteRepository {
    pool: DbPool,
}

impl SqliteRepository {
    pub fn new(database_url: &str) -> Result<Self, StoreError> {
        let manager = ConnectionManager::<SqliteConnection>::new(database_url);
        let max_size = if database_url == ":memory:" { 1 } else { 8 };
        let pool = Pool::builder()
            .max_size(max_size)
            .build(manager)
            .map_err(StoreError::database)?;

        {
            let mut connection = pool.get().map_err(StoreError::database)?;
            configure_connection(&mut connection)?;
            connection
                .run_pending_migrations(MIGRATIONS)
                .map_err(StoreError::database)?;
        }

        Ok(Self { pool })
    }

    async fn run<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> Result<T, StoreError> + Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = pool.get().map_err(StoreError::database)?;
            configure_connection(&mut connection)?;
            operation(&mut connection)
        })
        .await
        .map_err(StoreError::database)?
    }
}

pub type DynRepository = Arc<dyn TaskRepository>;

#[async_trait]
impl TaskRepository for SqliteRepository {
    async fn create(&self, description: String) -> Result<Task, StoreError> {
        let description = description.trim().to_owned();
        if description.is_empty() {
            return Err(StoreError::Validation(
                "task description must not be empty".to_owned(),
            ));
        }
        if description.chars().count() > 10_000 {
            return Err(StoreError::Validation(
                "task description must not exceed 10,000 characters".to_owned(),
            ));
        }

        self.run(move |connection| {
            let now = Utc::now().naive_utc();
            let row = NewTaskRow {
                description: &description,
                list_name: TaskList::Inbox.as_str(),
                state: TaskState::Pending.as_str(),
                context_note: None,
                revisit_at: None,
                created_at: now,
                updated_at: now,
            };

            diesel::insert_into(tasks::table)
                .values(&row)
                .execute(connection)
                .map_err(StoreError::database)?;

            let id = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
                "last_insert_rowid()",
            ))
            .get_result::<i64>(connection)
            .map_err(StoreError::database)?
            .try_into()
            .map_err(StoreError::database)?;
            load_task(connection, id)
        })
        .await
    }

    async fn get(&self, id: i32) -> Result<Task, StoreError> {
        self.run(move |connection| load_task(connection, id)).await
    }

    async fn list(&self, filter: TaskFilter) -> Result<Vec<Task>, StoreError> {
        self.run(move |connection| {
            let mut query = tasks::table.into_boxed();
            if let Some(list) = filter.list {
                query = query.filter(tasks::list_name.eq(list.as_str()));
            }
            if let Some(state) = filter.state {
                query = query.filter(tasks::state.eq(state.as_str()));
            }

            let rows = query
                .order((tasks::created_at.asc(), tasks::id.asc()))
                .select(TaskRow::as_select())
                .load(connection)
                .map_err(StoreError::database)?;
            let mut result = hydrate_tasks(connection, rows)?;
            if !filter.labels.is_empty() {
                result.retain(|task| {
                    filter
                        .labels
                        .iter()
                        .all(|(key, value)| task.context.labels.get(key) == Some(value))
                });
            }
            Ok(result)
        })
        .await
    }

    async fn transition(
        &self,
        id: i32,
        action: TaskAction,
        request: TransitionRequest,
    ) -> Result<Task, StoreError> {
        self.run(move |connection| {
            connection.immediate_transaction::<Task, StoreError, _>(|connection| {
                let current = tasks::table
                    .find(id)
                    .select(TaskRow::as_select())
                    .first(connection)
                    .optional()
                    .map_err(StoreError::database)?
                    .ok_or(StoreError::NotFound(id))?;

                let current_list = current
                    .list_name
                    .parse::<TaskList>()
                    .map_err(StoreError::Database)?;
                let current_state = current
                    .state
                    .parse::<TaskState>()
                    .map_err(StoreError::Database)?;
                let (target_list, target_state, revisit_at) =
                    transition_target(id, current_list, current_state, action, request.revisit_at)?;

                validate_context(&request.context)?;
                let note = request
                    .context
                    .note
                    .as_deref()
                    .map(str::trim)
                    .filter(|note| !note.is_empty())
                    .map(str::to_owned)
                    .or(current.context_note);

                diesel::update(tasks::table.find(id))
                    .set((
                        tasks::list_name.eq(target_list.as_str()),
                        tasks::state.eq(target_state.as_str()),
                        tasks::context_note.eq(note),
                        tasks::revisit_at.eq(revisit_at.map(|date| date.naive_utc())),
                        tasks::updated_at.eq(Utc::now().naive_utc()),
                    ))
                    .execute(connection)
                    .map_err(StoreError::database)?;

                merge_labels(connection, id, request.context.labels)?;
                load_task(connection, id)
            })
        })
        .await
    }

    async fn revive_due(&self, now: DateTime<Utc>) -> Result<Vec<Task>, StoreError> {
        self.run(move |connection| {
            connection.immediate_transaction::<Vec<Task>, StoreError, _>(|connection| {
                let ids = tasks::table
                    .filter(tasks::list_name.eq(TaskList::SomedayMaybe.as_str()))
                    .filter(tasks::state.eq(TaskState::Pending.as_str()))
                    .filter(tasks::revisit_at.le(now.naive_utc()))
                    .select(tasks::id)
                    .load::<i32>(connection)
                    .map_err(StoreError::database)?;

                if ids.is_empty() {
                    return Ok(Vec::new());
                }

                diesel::update(tasks::table.filter(tasks::id.eq_any(&ids)))
                    .set((
                        tasks::list_name.eq(TaskList::Inbox.as_str()),
                        tasks::revisit_at.eq::<Option<NaiveDateTime>>(None),
                        tasks::updated_at.eq(now.naive_utc()),
                    ))
                    .execute(connection)
                    .map_err(StoreError::database)?;

                let rows = tasks::table
                    .filter(tasks::id.eq_any(ids))
                    .order(tasks::id.asc())
                    .select(TaskRow::as_select())
                    .load(connection)
                    .map_err(StoreError::database)?;
                hydrate_tasks(connection, rows)
            })
        })
        .await
    }
}

fn configure_connection(connection: &mut SqliteConnection) -> Result<(), StoreError> {
    diesel::sql_query("PRAGMA foreign_keys = ON")
        .execute(connection)
        .map_err(StoreError::database)?;
    diesel::sql_query("PRAGMA busy_timeout = 5000")
        .execute(connection)
        .map_err(StoreError::database)?;
    Ok(())
}

fn transition_target(
    id: i32,
    list: TaskList,
    state: TaskState,
    action: TaskAction,
    revisit_at: Option<DateTime<Utc>>,
) -> Result<(TaskList, TaskState, Option<DateTime<Utc>>), StoreError> {
    let pending_in = |expected: TaskList| list == expected && state == TaskState::Pending;
    let conflict = || {
        StoreError::Conflict(format!(
            "cannot {action} task {id} while it is {list}/{state}"
        ))
    };

    match action {
        TaskAction::Start if pending_in(TaskList::Inbox) => {
            Ok((TaskList::Inbox, TaskState::Doing, None))
        }
        TaskAction::Pick if pending_in(TaskList::NextAction) => {
            Ok((TaskList::NextAction, TaskState::Doing, None))
        }
        TaskAction::Done if state == TaskState::Doing && list != TaskList::Archive => {
            Ok((TaskList::Archive, TaskState::Done, None))
        }
        TaskAction::Trash if list != TaskList::Archive => {
            Ok((TaskList::Archive, TaskState::Trash, None))
        }
        TaskAction::Defer if pending_in(TaskList::Inbox) => {
            Ok((TaskList::NextAction, TaskState::Pending, None))
        }
        TaskAction::Delegate if pending_in(TaskList::Inbox) => {
            Ok((TaskList::WaitingFor, TaskState::Pending, None))
        }
        TaskAction::Maybe
            if pending_in(TaskList::Inbox)
                || (list == TaskList::NextAction
                    && matches!(state, TaskState::Pending | TaskState::Doing)) =>
        {
            Ok((TaskList::SomedayMaybe, TaskState::Pending, revisit_at))
        }
        TaskAction::Activate if pending_in(TaskList::SomedayMaybe) => {
            Ok((TaskList::NextAction, TaskState::Pending, None))
        }
        _ => Err(conflict()),
    }
}

fn validate_context(context: &ContextPatch) -> Result<(), StoreError> {
    for (key, value) in &context.labels {
        if key.trim().is_empty() || value.trim().is_empty() {
            return Err(StoreError::Validation(
                "label keys and values must not be empty".to_owned(),
            ));
        }
        if key.contains([':', '=']) {
            return Err(StoreError::Validation(format!(
                "label key '{key}' must not contain ':' or '='"
            )));
        }
    }
    Ok(())
}

fn merge_labels(
    connection: &mut SqliteConnection,
    id: i32,
    new_labels: BTreeMap<String, String>,
) -> Result<(), StoreError> {
    for (key, value) in new_labels {
        let key = key.trim().to_owned();
        let value = value.trim().to_owned();
        diesel::delete(labels::table.find((id, &key)))
            .execute(connection)
            .map_err(StoreError::database)?;
        diesel::insert_into(labels::table)
            .values(NewLabelRow {
                task_id: id,
                key: &key,
                value: &value,
            })
            .execute(connection)
            .map_err(StoreError::database)?;
    }
    Ok(())
}

fn load_task(connection: &mut SqliteConnection, id: i32) -> Result<Task, StoreError> {
    let row = tasks::table
        .find(id)
        .select(TaskRow::as_select())
        .first(connection)
        .optional()
        .map_err(StoreError::database)?
        .ok_or(StoreError::NotFound(id))?;
    hydrate_tasks(connection, vec![row])?
        .pop()
        .ok_or(StoreError::NotFound(id))
}

fn hydrate_tasks(
    connection: &mut SqliteConnection,
    rows: Vec<TaskRow>,
) -> Result<Vec<Task>, StoreError> {
    let ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
    let label_rows = if ids.is_empty() {
        Vec::new()
    } else {
        labels::table
            .filter(labels::task_id.eq_any(ids))
            .order((labels::task_id.asc(), labels::key.asc()))
            .select(LabelRow::as_select())
            .load(connection)
            .map_err(StoreError::database)?
    };
    let mut labels_by_task: BTreeMap<i32, BTreeMap<String, String>> = BTreeMap::new();
    for label in label_rows {
        labels_by_task
            .entry(label.task_id)
            .or_default()
            .insert(label.key, label.value);
    }

    rows.into_iter()
        .map(|row| {
            Ok(Task {
                id: row.id,
                description: row.description,
                list: row.list_name.parse().map_err(StoreError::Database)?,
                state: row.state.parse().map_err(StoreError::Database)?,
                context: TaskContext {
                    labels: labels_by_task.remove(&row.id).unwrap_or_default(),
                    note: row.context_note,
                },
                revisit_at: row.revisit_at.map(as_utc),
                created_at: as_utc(row.created_at),
                updated_at: as_utc(row.updated_at),
            })
        })
        .collect()
}

fn as_utc(value: NaiveDateTime) -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(value, Utc)
}

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = tasks)]
struct TaskRow {
    id: i32,
    description: String,
    list_name: String,
    state: String,
    context_note: Option<String>,
    revisit_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Insertable)]
#[diesel(table_name = tasks)]
struct NewTaskRow<'a> {
    description: &'a str,
    list_name: &'a str,
    state: &'a str,
    context_note: Option<&'a str>,
    revisit_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = labels)]
struct LabelRow {
    task_id: i32,
    key: String,
    value: String,
}

#[derive(Insertable)]
#[diesel(table_name = labels)]
struct NewLabelRow<'a> {
    task_id: i32,
    key: &'a str,
    value: &'a str,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Duration;

    use super::*;

    fn repository() -> (tempfile::TempDir, SqliteRepository) {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("test.db");
        let repository = SqliteRepository::new(database.to_str().unwrap()).unwrap();
        (directory, repository)
    }

    #[tokio::test]
    async fn complete_lifecycle_preserves_context() {
        let (_directory, repository) = repository();
        let task = repository
            .create("Write the guide".to_owned())
            .await
            .unwrap();
        assert_eq!(task.list, TaskList::Inbox);

        let deferred = repository
            .transition(
                task.id,
                TaskAction::Defer,
                TransitionRequest {
                    context: ContextPatch {
                        labels: BTreeMap::from([("project".to_owned(), "gtd".to_owned())]),
                        note: Some("Draft first".to_owned()),
                    },
                    revisit_at: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(deferred.list, TaskList::NextAction);

        let picked = repository
            .transition(task.id, TaskAction::Pick, TransitionRequest::default())
            .await
            .unwrap();
        assert_eq!(picked.state, TaskState::Doing);

        let done = repository
            .transition(task.id, TaskAction::Done, TransitionRequest::default())
            .await
            .unwrap();
        assert_eq!(
            (done.list, done.state),
            (TaskList::Archive, TaskState::Done)
        );
        assert_eq!(done.context.labels["project"], "gtd");
        assert_eq!(done.context.note.as_deref(), Some("Draft first"));
    }

    #[tokio::test]
    async fn due_someday_task_returns_to_inbox() {
        let (_directory, repository) = repository();
        let task = repository
            .create("Reconsider this".to_owned())
            .await
            .unwrap();
        repository
            .transition(
                task.id,
                TaskAction::Maybe,
                TransitionRequest {
                    revisit_at: Some(Utc::now() - Duration::minutes(1)),
                    ..TransitionRequest::default()
                },
            )
            .await
            .unwrap();

        let revived = repository.revive_due(Utc::now()).await.unwrap();
        assert_eq!(revived.len(), 1);
        assert_eq!(revived[0].list, TaskList::Inbox);
        assert!(revived[0].revisit_at.is_none());
    }

    #[tokio::test]
    async fn label_filter_requires_every_label() {
        let (_directory, repository) = repository();
        let task = repository.create("Filtered task".to_owned()).await.unwrap();
        repository
            .transition(
                task.id,
                TaskAction::Defer,
                TransitionRequest {
                    context: ContextPatch {
                        labels: BTreeMap::from([
                            ("project".to_owned(), "gtd".to_owned()),
                            ("place".to_owned(), "home".to_owned()),
                        ]),
                        note: None,
                    },
                    revisit_at: None,
                },
            )
            .await
            .unwrap();

        let tasks = repository
            .list(TaskFilter {
                list: Some(TaskList::NextAction),
                labels: BTreeMap::from([("project".to_owned(), "gtd".to_owned())]),
                ..TaskFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
    }

    #[tokio::test]
    async fn invalid_transition_is_rejected() {
        let (_directory, repository) = repository();
        let task = repository.create("Not ready".to_owned()).await.unwrap();
        let error = repository
            .transition(task.id, TaskAction::Done, TransitionRequest::default())
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));
    }
}
