use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::{
    dsl::max,
    prelude::*,
    r2d2::{ConnectionManager, Pool},
    sql_types::{BigInt, Text},
    sqlite::SqliteConnection,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    domain::{
        Task, TaskEvent, TaskEventType, TaskFilter, TaskList, TaskMetadata, TaskState,
        TaskWatchEvent, UpdateTaskRequest,
    },
    schema::{task_event_compaction, task_events},
};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
const COMPACTION_BATCH_SIZE: i64 = 1_000;
const LIST_ALL_LATEST_EVENTS_SQL: &str = r#"
WITH RECURSIVE
task_ids(task_id) AS (
    SELECT MIN(task_id)
    FROM task_events

    UNION ALL

    SELECT (
        SELECT MIN(next.task_id)
        FROM task_events AS next
        WHERE next.task_id > task_ids.task_id
    )
    FROM task_ids
    WHERE task_id IS NOT NULL
),
latest_revisions(task_id, revision) AS MATERIALIZED (
    SELECT
        ids.task_id,
        (
            SELECT event.revision
            FROM task_events AS event
            WHERE event.task_id = ids.task_id
              AND event.revision <= ?
            ORDER BY event.revision DESC
            LIMIT 1
        )
    FROM task_ids AS ids
    WHERE ids.task_id IS NOT NULL
)
SELECT event.*
FROM latest_revisions AS latest
CROSS JOIN task_events AS event
WHERE latest.revision IS NOT NULL
  AND event.revision = latest.revision
ORDER BY event.created_at ASC, event.task_id ASC
"#;
const LIST_FILTERED_LATEST_EVENTS_SQL: &str = r#"
WITH RECURSIVE
candidate_task_ids(task_id) AS (
    SELECT MIN(task_id)
    FROM task_events
    WHERE list_name = ?

    UNION ALL

    SELECT (
        SELECT MIN(next.task_id)
        FROM task_events AS next
        WHERE next.list_name = ?
          AND next.task_id > candidate_task_ids.task_id
    )
    FROM candidate_task_ids
    WHERE task_id IS NOT NULL
),
latest_revisions(task_id, revision) AS MATERIALIZED (
    SELECT
        candidate.task_id,
        (
            SELECT event.revision
            FROM task_events AS event
            WHERE event.task_id = candidate.task_id
              AND event.revision <= ?
            ORDER BY event.revision DESC
            LIMIT 1
        )
    FROM candidate_task_ids AS candidate
    WHERE candidate.task_id IS NOT NULL
)
SELECT event.*
FROM latest_revisions AS latest
CROSS JOIN task_events AS event
WHERE latest.revision IS NOT NULL
  AND event.revision = latest.revision
  AND event.list_name = ?
ORDER BY event.created_at ASC, event.task_id ASC
"#;

type DbPool = Pool<ConnectionManager<SqliteConnection>>;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("task {0} was not found")]
    NotFound(Uuid),
    #[error("invalid request: {0}")]
    Validation(String),
    #[error("task state change conflict: {0}")]
    Conflict(String),
    #[error(
        "task {task_id} revision conflict: expected revision {expected_revision}, current revision {current_revision}"
    )]
    RevisionConflict {
        task_id: Uuid,
        expected_revision: i64,
        current_revision: i64,
    },
    #[error("revision {requested} has been compacted at revision {compacted_revision}")]
    Compacted {
        requested: i64,
        compacted_revision: i64,
    },
    #[error("revision {requested} is ahead of current revision {current_revision}")]
    FutureRevision {
        requested: i64,
        current_revision: i64,
    },
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

#[derive(Debug, Clone)]
pub struct MutationResult {
    pub event: TaskEvent,
}

#[derive(Debug, Clone)]
pub struct ListResult {
    pub revision: i64,
    pub tasks: Vec<Task>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RevisionState {
    pub current_revision: i64,
    pub scheduled_revision: i64,
    pub finished_revision: i64,
}

#[async_trait]
pub trait TaskRepository: Send + Sync {
    async fn create(&self, description: String) -> Result<MutationResult, StoreError>;
    async fn get(&self, id: Uuid) -> Result<Task, StoreError>;
    async fn list(&self, filter: TaskFilter) -> Result<ListResult, StoreError>;
    async fn update(
        &self,
        id: Uuid,
        request: UpdateTaskRequest,
    ) -> Result<MutationResult, StoreError>;
    async fn revision_state(&self) -> Result<RevisionState, StoreError>;
    async fn events_from(
        &self,
        start_revision: i64,
        limit: i64,
    ) -> Result<Vec<TaskWatchEvent>, StoreError>;
    async fn compact(&self, target_revision: i64) -> Result<RevisionState, StoreError>;
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
            resume_pending_compaction(&mut connection)?;
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
    async fn create(&self, description: String) -> Result<MutationResult, StoreError> {
        let description = description.trim().to_owned();
        validate_description(&description)?;

        self.run(move |connection| {
            connection.immediate_transaction::<MutationResult, StoreError, _>(|connection| {
                let task_id = loop {
                    let candidate = Uuid::now_v7();
                    let exists = diesel::select(diesel::dsl::exists(
                        task_events::table.filter(task_events::task_id.eq(candidate.to_string())),
                    ))
                    .get_result::<bool>(connection)
                    .map_err(StoreError::database)?;
                    if !exists {
                        break candidate;
                    }
                };

                let now = Utc::now();
                let task = Task {
                    metadata: TaskMetadata {
                        id: task_id,
                        revision: 0,
                        created_at: now,
                        updated_at: now,
                    },
                    description,
                    list: TaskList::Inbox,
                    state: TaskState::Pending,
                    labels: Default::default(),
                };
                let event = append_event(connection, TaskEventType::Created, &task, None, None)?;
                Ok(MutationResult { event })
            })
        })
        .await
    }

    async fn get(&self, id: Uuid) -> Result<Task, StoreError> {
        self.run(move |connection| Ok(load_latest_event(connection, id)?.task()))
            .await
    }

    async fn list(&self, filter: TaskFilter) -> Result<ListResult, StoreError> {
        self.run(move |connection| {
            connection.transaction::<ListResult, StoreError, _>(|connection| {
                let revision = current_revision(connection)?;
                let rows: Vec<TaskEventRow> = match filter.list {
                    Some(list) => {
                        let list_name = list.as_str();
                        diesel::sql_query(LIST_FILTERED_LATEST_EVENTS_SQL)
                            .bind::<Text, _>(list_name)
                            .bind::<Text, _>(list_name)
                            .bind::<BigInt, _>(revision)
                            .bind::<Text, _>(list_name)
                            .load(connection)
                            .map_err(StoreError::database)?
                    }
                    None => diesel::sql_query(LIST_ALL_LATEST_EVENTS_SQL)
                        .bind::<BigInt, _>(revision)
                        .load(connection)
                        .map_err(StoreError::database)?,
                };
                let mut tasks = rows
                    .into_iter()
                    .map(TaskEvent::try_from)
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(|event| event.task())
                    .collect::<Vec<_>>();

                // TODO(labels-index): labels currently live only in the event JSON. Keep the
                // filter correct here until a dedicated indexing design is accepted.
                tasks.retain(|task| filter.matches(task));

                Ok(ListResult { revision, tasks })
            })
        })
        .await
    }

    async fn update(
        &self,
        id: Uuid,
        request: UpdateTaskRequest,
    ) -> Result<MutationResult, StoreError> {
        self.run(move |connection| {
            connection.immediate_transaction::<MutationResult, StoreError, _>(|connection| {
                let before_event = load_latest_event(connection, id)?;
                if request.metadata.revision != before_event.revision {
                    return Err(StoreError::RevisionConflict {
                        task_id: id,
                        expected_revision: request.metadata.revision,
                        current_revision: before_event.revision,
                    });
                }

                let before = before_event.task();
                validate_description(request.description.trim())?;
                validate_state_change(id, before.list, before.state, request.list, request.state)?;
                let labels = normalize_labels(request.labels)?;
                let description = request.description.trim().to_owned();
                if description == before.description
                    && request.list == before.list
                    && request.state == before.state
                    && labels == before.labels
                {
                    return Err(StoreError::Validation(
                        "task update must change at least one field".to_owned(),
                    ));
                }
                let now = Utc::now().max(before.metadata.updated_at);
                let after = Task {
                    metadata: TaskMetadata {
                        id,
                        revision: before.metadata.revision,
                        created_at: before.metadata.created_at,
                        updated_at: now,
                    },
                    description,
                    list: request.list,
                    state: request.state,
                    labels,
                };

                let event = append_event(
                    connection,
                    TaskEventType::Updated,
                    &after,
                    Some(before_event.revision),
                    Some(&before),
                )?;
                Ok(MutationResult { event })
            })
        })
        .await
    }

    async fn revision_state(&self) -> Result<RevisionState, StoreError> {
        self.run(|connection| {
            connection.transaction::<RevisionState, StoreError, _>(load_revision_state)
        })
        .await
    }

    async fn events_from(
        &self,
        start_revision: i64,
        limit: i64,
    ) -> Result<Vec<TaskWatchEvent>, StoreError> {
        self.run(move |connection| {
            connection.transaction::<Vec<TaskWatchEvent>, StoreError, _>(|connection| {
                let state = load_revision_state(connection)?;
                validate_start_revision(start_revision, state)?;
                let rows = task_events::table
                    .filter(task_events::revision.ge(start_revision))
                    .order(task_events::revision.asc())
                    .limit(limit.clamp(1, 1_000))
                    .select(TaskEventRow::as_select())
                    .load(connection)
                    .map_err(StoreError::database)?;
                let events = rows
                    .into_iter()
                    .map(TaskEvent::try_from)
                    .collect::<Result<Vec<_>, _>>()?;

                events
                    .into_iter()
                    .map(|event| {
                        let prev_task = event.prev_task();
                        if event.prev_revision.is_some() && prev_task.is_none() {
                            return Err(StoreError::Database(format!(
                                "task event {} has an incomplete previous state",
                                event.revision
                            )));
                        }
                        Ok(TaskWatchEvent {
                            event_type: event.event_type,
                            task: event.task(),
                            prev_task,
                        })
                    })
                    .collect()
            })
        })
        .await
    }

    async fn compact(&self, target_revision: i64) -> Result<RevisionState, StoreError> {
        self.run(move |connection| {
            connection.immediate_transaction::<(), StoreError, _>(|connection| {
                let state = load_revision_state(connection)?;
                if target_revision < state.scheduled_revision {
                    return Err(StoreError::Conflict(format!(
                        "compact revision cannot move backward from {} to {target_revision}",
                        state.scheduled_revision
                    )));
                }
                if target_revision > state.current_revision {
                    return Err(StoreError::FutureRevision {
                        requested: target_revision,
                        current_revision: state.current_revision,
                    });
                }
                diesel::update(task_event_compaction::table.find(1))
                    .set(task_event_compaction::scheduled_revision.eq(target_revision))
                    .execute(connection)
                    .map_err(StoreError::database)?;
                Ok(())
            })?;
            compact_to(connection, target_revision)?;
            load_revision_state(connection)
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

fn validate_description(description: &str) -> Result<(), StoreError> {
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
    Ok(())
}

fn validate_state_change(
    id: Uuid,
    before_list: TaskList,
    before_state: TaskState,
    target_list: TaskList,
    target_state: TaskState,
) -> Result<(), StoreError> {
    if before_list == target_list && before_state == target_state {
        return Ok(());
    }
    match (before_list, before_state, target_list, target_state) {
        (TaskList::Inbox, TaskState::Pending, TaskList::Inbox, TaskState::Doing) => Ok(()),
        (TaskList::NextAction, TaskState::Pending, TaskList::NextAction, TaskState::Doing) => {
            Ok(())
        }
        (list, TaskState::Doing, TaskList::Archive, TaskState::Done)
            if list != TaskList::Archive =>
        {
            Ok(())
        }
        (list, _, TaskList::Archive, TaskState::Trash) if list != TaskList::Archive => Ok(()),
        (TaskList::Inbox, TaskState::Pending, TaskList::NextAction, TaskState::Pending) => Ok(()),
        (TaskList::Inbox, TaskState::Pending, TaskList::WaitingFor, TaskState::Pending) => Ok(()),
        (TaskList::Inbox, TaskState::Pending, TaskList::SomedayMaybe, TaskState::Pending)
        | (
            TaskList::NextAction,
            TaskState::Pending | TaskState::Doing,
            TaskList::SomedayMaybe,
            TaskState::Pending,
        ) => Ok(()),
        (TaskList::SomedayMaybe, TaskState::Pending, TaskList::NextAction, TaskState::Pending) => {
            Ok(())
        }
        _ => Err(StoreError::Conflict(format!(
            "cannot update task {id} from {before_list}/{before_state} to {target_list}/{target_state}"
        ))),
    }
}

fn normalize_labels(
    labels: std::collections::BTreeMap<String, String>,
) -> Result<std::collections::BTreeMap<String, String>, StoreError> {
    for (key, value) in &labels {
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

    Ok(labels
        .into_iter()
        .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
        .collect())
}

fn append_event(
    connection: &mut SqliteConnection,
    event_type: TaskEventType,
    after: &Task,
    prev_revision: Option<i64>,
    before: Option<&Task>,
) -> Result<TaskEvent, StoreError> {
    let new_event = NewTaskEventRow {
        task_id: after.metadata.id.to_string(),
        prev_revision,
        event_type: event_type.as_str().to_owned(),
        description: after.description.clone(),
        list_name: after.list.as_str().to_owned(),
        state: after.state.as_str().to_owned(),
        labels: serde_json::to_string(&after.labels).map_err(StoreError::database)?,
        created_at: after.metadata.created_at.naive_utc(),
        updated_at: after.metadata.updated_at.naive_utc(),
        prev_description: before.map(|task| task.description.clone()),
        prev_list_name: before.map(|task| task.list.as_str().to_owned()),
        prev_state: before.map(|task| task.state.as_str().to_owned()),
        prev_labels: before
            .map(|task| serde_json::to_string(&task.labels))
            .transpose()
            .map_err(StoreError::database)?,
        prev_updated_at: before.map(|task| task.metadata.updated_at.naive_utc()),
    };

    diesel::insert_into(task_events::table)
        .values(&new_event)
        .execute(connection)
        .map_err(StoreError::database)?;
    let revision = diesel::select(diesel::dsl::sql::<BigInt>("last_insert_rowid()"))
        .get_result::<i64>(connection)
        .map_err(StoreError::database)?;
    let row = task_events::table
        .find(revision)
        .select(TaskEventRow::as_select())
        .first(connection)
        .map_err(StoreError::database)?;
    TaskEvent::try_from(row)
}

fn load_latest_event(connection: &mut SqliteConnection, id: Uuid) -> Result<TaskEvent, StoreError> {
    let row = task_events::table
        .filter(task_events::task_id.eq(id.to_string()))
        .order(task_events::revision.desc())
        .select(TaskEventRow::as_select())
        .first(connection)
        .optional()
        .map_err(StoreError::database)?
        .ok_or(StoreError::NotFound(id))?;
    TaskEvent::try_from(row)
}

fn current_revision(connection: &mut SqliteConnection) -> Result<i64, StoreError> {
    task_events::table
        .select(max(task_events::revision))
        .first::<Option<i64>>(connection)
        .map(|revision| revision.unwrap_or(0))
        .map_err(StoreError::database)
}

fn load_revision_state(connection: &mut SqliteConnection) -> Result<RevisionState, StoreError> {
    let current_revision = current_revision(connection)?;
    let row = task_event_compaction::table
        .find(1)
        .select(CompactionRow::as_select())
        .first(connection)
        .map_err(StoreError::database)?;
    Ok(RevisionState {
        current_revision,
        scheduled_revision: row.scheduled_revision,
        finished_revision: row.finished_revision,
    })
}

fn validate_start_revision(start_revision: i64, state: RevisionState) -> Result<(), StoreError> {
    if start_revision <= state.scheduled_revision {
        return Err(StoreError::Compacted {
            requested: start_revision,
            compacted_revision: state.scheduled_revision,
        });
    }
    if start_revision > state.current_revision + 1 {
        return Err(StoreError::FutureRevision {
            requested: start_revision,
            current_revision: state.current_revision,
        });
    }
    Ok(())
}

fn resume_pending_compaction(connection: &mut SqliteConnection) -> Result<(), StoreError> {
    let row = task_event_compaction::table
        .find(1)
        .select(CompactionRow::as_select())
        .first(connection)
        .map_err(StoreError::database)?;
    if row.finished_revision < row.scheduled_revision {
        compact_to(connection, row.scheduled_revision)?;
    }
    Ok(())
}

fn compact_to(connection: &mut SqliteConnection, target_revision: i64) -> Result<(), StoreError> {
    loop {
        let deleted = connection.immediate_transaction::<usize, StoreError, _>(|connection| {
            diesel::sql_query(
                "DELETE FROM task_events
                 WHERE revision IN (
                     SELECT old.revision
                     FROM task_events AS old
                     WHERE old.revision <= ?
                       AND EXISTS (
                           SELECT 1
                           FROM task_events AS newer
                           WHERE newer.task_id = old.task_id
                             AND newer.revision > old.revision
                             AND newer.revision <= ?
                       )
                     ORDER BY old.revision ASC
                     LIMIT ?
                 )",
            )
            .bind::<BigInt, _>(target_revision)
            .bind::<BigInt, _>(target_revision)
            .bind::<BigInt, _>(COMPACTION_BATCH_SIZE)
            .execute(connection)
            .map_err(StoreError::database)
        })?;
        if deleted == 0 {
            break;
        }
    }

    connection.immediate_transaction::<(), StoreError, _>(|connection| {
        diesel::update(task_event_compaction::table.find(1))
            .filter(task_event_compaction::finished_revision.le(target_revision))
            .filter(task_event_compaction::scheduled_revision.ge(target_revision))
            .set(task_event_compaction::finished_revision.eq(target_revision))
            .execute(connection)
            .map_err(StoreError::database)?;
        Ok(())
    })
}

fn as_utc(value: NaiveDateTime) -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(value, Utc)
}

#[derive(Debug, Queryable, QueryableByName, Selectable, Identifiable)]
#[diesel(table_name = task_events, primary_key(revision))]
struct TaskEventRow {
    revision: i64,
    task_id: String,
    prev_revision: Option<i64>,
    event_type: String,
    description: String,
    list_name: String,
    state: String,
    labels: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    prev_description: Option<String>,
    prev_list_name: Option<String>,
    prev_state: Option<String>,
    prev_labels: Option<String>,
    prev_updated_at: Option<NaiveDateTime>,
}

impl TryFrom<TaskEventRow> for TaskEvent {
    type Error = StoreError;

    fn try_from(row: TaskEventRow) -> Result<Self, Self::Error> {
        Ok(Self {
            revision: row.revision,
            task_id: Uuid::parse_str(&row.task_id).map_err(StoreError::database)?,
            prev_revision: row.prev_revision,
            event_type: row.event_type.parse().map_err(StoreError::Database)?,
            description: row.description,
            list_name: row.list_name.parse().map_err(StoreError::Database)?,
            state: row.state.parse().map_err(StoreError::Database)?,
            labels: serde_json::from_str(&row.labels).map_err(StoreError::database)?,
            created_at: as_utc(row.created_at),
            updated_at: as_utc(row.updated_at),
            prev_description: row.prev_description,
            prev_list_name: row
                .prev_list_name
                .map(|value| value.parse().map_err(StoreError::Database))
                .transpose()?,
            prev_state: row
                .prev_state
                .map(|value| value.parse().map_err(StoreError::Database))
                .transpose()?,
            prev_labels: row
                .prev_labels
                .map(|value| serde_json::from_str(&value).map_err(StoreError::database))
                .transpose()?,
            prev_updated_at: row.prev_updated_at.map(as_utc),
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = task_events)]
struct NewTaskEventRow {
    task_id: String,
    prev_revision: Option<i64>,
    event_type: String,
    description: String,
    list_name: String,
    state: String,
    labels: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    prev_description: Option<String>,
    prev_list_name: Option<String>,
    prev_state: Option<String>,
    prev_labels: Option<String>,
    prev_updated_at: Option<NaiveDateTime>,
}

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = task_event_compaction)]
struct CompactionRow {
    #[allow(dead_code)]
    singleton: i32,
    scheduled_revision: i64,
    finished_revision: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::domain::UpdateTaskMetadata;

    use super::*;

    #[derive(QueryableByName)]
    struct QueryPlanDetail {
        #[diesel(sql_type = Text)]
        detail: String,
    }

    fn repository() -> (tempfile::TempDir, SqliteRepository) {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("test.db");
        let repository = SqliteRepository::new(database.to_str().unwrap()).unwrap();
        (directory, repository)
    }

    async fn update_state(
        repository: &SqliteRepository,
        id: Uuid,
        list: TaskList,
        state: TaskState,
        labels: Option<BTreeMap<String, String>>,
    ) -> Result<MutationResult, StoreError> {
        let current = repository.get(id).await?;
        repository
            .update(
                id,
                UpdateTaskRequest {
                    metadata: UpdateTaskMetadata {
                        revision: current.metadata.revision,
                    },
                    description: current.description,
                    list,
                    state,
                    labels: labels.unwrap_or(current.labels),
                },
            )
            .await
    }

    #[test]
    fn valid_state_changes_are_accepted() {
        let task_id = Uuid::now_v7();
        let cases = [
            (
                (TaskList::Inbox, TaskState::Pending),
                (TaskList::Inbox, TaskState::Doing),
            ),
            (
                (TaskList::NextAction, TaskState::Pending),
                (TaskList::NextAction, TaskState::Doing),
            ),
            (
                (TaskList::NextAction, TaskState::Doing),
                (TaskList::Archive, TaskState::Done),
            ),
            (
                (TaskList::WaitingFor, TaskState::Pending),
                (TaskList::Archive, TaskState::Trash),
            ),
            (
                (TaskList::Inbox, TaskState::Pending),
                (TaskList::NextAction, TaskState::Pending),
            ),
            (
                (TaskList::Inbox, TaskState::Pending),
                (TaskList::WaitingFor, TaskState::Pending),
            ),
            (
                (TaskList::Inbox, TaskState::Pending),
                (TaskList::SomedayMaybe, TaskState::Pending),
            ),
            (
                (TaskList::SomedayMaybe, TaskState::Pending),
                (TaskList::NextAction, TaskState::Pending),
            ),
        ];

        for ((before_list, before_state), (target_list, target_state)) in cases {
            validate_state_change(
                task_id,
                before_list,
                before_state,
                target_list,
                target_state,
            )
            .unwrap();
        }
    }

    #[tokio::test]
    async fn list_loose_scan_handles_an_empty_event_log() {
        let (_directory, repository) = repository();
        let result = repository.list(TaskFilter::default()).await.unwrap();
        assert_eq!(result.revision, 0);
        assert!(result.tasks.is_empty());
    }

    #[tokio::test]
    async fn list_loose_scan_plans_use_the_expected_indexes() {
        let (_directory, repository) = repository();
        let (all_details, filtered_details) = repository
            .run(|connection| {
                let all_rows: Vec<QueryPlanDetail> =
                    diesel::sql_query(format!("EXPLAIN QUERY PLAN {LIST_ALL_LATEST_EVENTS_SQL}"))
                        .bind::<BigInt, _>(0_i64)
                        .load(connection)
                        .map_err(StoreError::database)?;
                let filtered_rows: Vec<QueryPlanDetail> = diesel::sql_query(format!(
                    "EXPLAIN QUERY PLAN {LIST_FILTERED_LATEST_EVENTS_SQL}"
                ))
                .bind::<Text, _>("in")
                .bind::<Text, _>("in")
                .bind::<BigInt, _>(0_i64)
                .bind::<Text, _>("in")
                .load(connection)
                .map_err(StoreError::database)?;
                Ok((
                    all_rows
                        .into_iter()
                        .map(|row| row.detail)
                        .collect::<Vec<_>>(),
                    filtered_rows
                        .into_iter()
                        .map(|row| row.detail)
                        .collect::<Vec<_>>(),
                ))
            })
            .await
            .unwrap();

        assert!(all_details.iter().any(|detail| {
            detail.contains("SEARCH next USING COVERING INDEX task_events_task_revision_idx")
        }));
        assert!(all_details.iter().any(|detail| {
            detail.contains("SEARCH event USING COVERING INDEX task_events_task_revision_idx")
                && detail.contains("task_id=?")
        }));
        assert!(filtered_details.iter().any(|detail| {
            detail.contains("SEARCH next USING COVERING INDEX task_events_list_task_revision_idx")
                && detail.contains("list_name=?")
                && detail.contains("task_id>?")
        }));
        assert!(filtered_details.iter().any(|detail| {
            detail.contains("SEARCH event USING COVERING INDEX task_events_task_revision_idx")
                && detail.contains("task_id=?")
        }));
    }

    #[tokio::test]
    async fn complete_lifecycle_appends_events_and_preserves_before_state() {
        let (_directory, repository) = repository();
        let created = repository
            .create("Write the guide".to_owned())
            .await
            .unwrap();
        assert_eq!(created.event.revision, 1);
        assert_eq!(created.event.event_type, TaskEventType::Created);
        assert!(created.event.prev_revision.is_none());
        assert!(created.event.prev_description.is_none());
        assert!(created.event.prev_list_name.is_none());
        assert!(created.event.prev_state.is_none());
        assert!(created.event.prev_labels.is_none());
        assert!(created.event.prev_updated_at.is_none());
        assert_eq!(created.event.task_id.get_version_num(), 7);

        let moved_to_next = update_state(
            &repository,
            created.event.task_id,
            TaskList::NextAction,
            TaskState::Pending,
            Some(BTreeMap::from([("project".to_owned(), "gtd".to_owned())])),
        )
        .await
        .unwrap();
        assert_eq!(moved_to_next.event.revision, 2);
        assert_eq!(moved_to_next.event.event_type, TaskEventType::Updated);
        assert_eq!(moved_to_next.event.prev_revision, Some(1));
        assert_eq!(
            moved_to_next.event.prev_description.as_deref(),
            Some(created.event.description.as_str())
        );
        assert_eq!(moved_to_next.event.prev_list_name, Some(TaskList::Inbox));
        assert_eq!(moved_to_next.event.prev_state, Some(TaskState::Pending));
        assert_eq!(moved_to_next.event.prev_labels, Some(BTreeMap::new()));
        assert_eq!(
            moved_to_next.event.prev_updated_at,
            Some(created.event.updated_at)
        );
        assert_eq!(moved_to_next.event.list_name, TaskList::NextAction);

        let doing = update_state(
            &repository,
            created.event.task_id,
            TaskList::NextAction,
            TaskState::Doing,
            None,
        )
        .await
        .unwrap();
        let archived = update_state(
            &repository,
            created.event.task_id,
            TaskList::Archive,
            TaskState::Done,
            None,
        )
        .await
        .unwrap();
        assert_eq!(doing.event.revision, 3);
        assert_eq!(doing.event.event_type, TaskEventType::Updated);
        assert_eq!(archived.event.revision, 4);
        assert_eq!(archived.event.event_type, TaskEventType::Updated);
        assert_eq!(
            (archived.event.list_name, archived.event.state),
            (TaskList::Archive, TaskState::Done)
        );
        assert_eq!(archived.event.labels["project"], "gtd");
    }

    #[tokio::test]
    async fn conditional_update_checks_revision_and_replaces_description_and_labels() {
        let (_directory, repository) = repository();
        let created = repository
            .create("Initial description".to_owned())
            .await
            .unwrap();

        let moved = repository
            .update(
                created.event.task_id,
                UpdateTaskRequest {
                    metadata: UpdateTaskMetadata { revision: 1 },
                    description: "Expanded description".to_owned(),
                    list: TaskList::NextAction,
                    state: TaskState::Pending,
                    labels: BTreeMap::from([("project".to_owned(), "gtd".to_owned())]),
                },
            )
            .await
            .unwrap();
        assert_eq!(moved.event.event_type, TaskEventType::Updated);
        assert_eq!(moved.event.task().metadata.revision, 2);
        assert_eq!(moved.event.description, "Expanded description");
        assert_eq!(
            moved.event.prev_description.as_deref(),
            Some("Initial description")
        );

        let conflict = repository
            .update(
                created.event.task_id,
                UpdateTaskRequest {
                    metadata: UpdateTaskMetadata { revision: 1 },
                    description: "Stale update".to_owned(),
                    list: TaskList::NextAction,
                    state: TaskState::Doing,
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            StoreError::RevisionConflict {
                expected_revision: 1,
                current_revision: 2,
                ..
            }
        ));
        assert_eq!(
            repository.revision_state().await.unwrap().current_revision,
            2
        );

        let updated = repository
            .update(
                created.event.task_id,
                UpdateTaskRequest {
                    metadata: UpdateTaskMetadata { revision: 2 },
                    description: "Expanded description".to_owned(),
                    list: TaskList::NextAction,
                    state: TaskState::Doing,
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.event.event_type, TaskEventType::Updated);
        assert!(updated.event.labels.is_empty());
        assert_eq!(updated.event.prev_labels.unwrap()["project"], "gtd");
    }

    #[tokio::test]
    async fn description_can_change_without_a_state_change_and_no_op_is_rejected() {
        let (_directory, repository) = repository();
        let created = repository.create("Short".to_owned()).await.unwrap();

        let edited = repository
            .update(
                created.event.task_id,
                UpdateTaskRequest {
                    metadata: UpdateTaskMetadata {
                        revision: created.event.revision,
                    },
                    description: "Short, with supporting details".to_owned(),
                    list: TaskList::Inbox,
                    state: TaskState::Pending,
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(edited.event.description, "Short, with supporting details");
        assert_eq!(edited.event.prev_description.as_deref(), Some("Short"));

        let error = repository
            .update(
                created.event.task_id,
                UpdateTaskRequest {
                    metadata: UpdateTaskMetadata {
                        revision: edited.event.revision,
                    },
                    description: edited.event.description,
                    list: edited.event.list_name,
                    state: edited.event.state,
                    labels: edited.event.labels,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Validation(_)));
        assert_eq!(
            repository.revision_state().await.unwrap().current_revision,
            edited.event.revision
        );
    }

    #[tokio::test]
    async fn list_uses_only_latest_events_and_filters_labels() {
        let (_directory, repository) = repository();
        let created = repository.create("Filtered task".to_owned()).await.unwrap();
        update_state(
            &repository,
            created.event.task_id,
            TaskList::NextAction,
            TaskState::Pending,
            Some(BTreeMap::from([
                ("project".to_owned(), "gtd".to_owned()),
                ("place".to_owned(), "home".to_owned()),
            ])),
        )
        .await
        .unwrap();
        update_state(
            &repository,
            created.event.task_id,
            TaskList::NextAction,
            TaskState::Doing,
            None,
        )
        .await
        .unwrap();

        let inbox = repository
            .list(TaskFilter {
                list: Some(TaskList::Inbox),
                ..TaskFilter::default()
            })
            .await
            .unwrap();
        assert!(inbox.tasks.is_empty());

        let stale_state = repository
            .list(TaskFilter {
                list: Some(TaskList::NextAction),
                state: Some(TaskState::Pending),
                ..TaskFilter::default()
            })
            .await
            .unwrap();
        assert!(stale_state.tasks.is_empty());

        let result = repository
            .list(TaskFilter {
                list: Some(TaskList::NextAction),
                state: Some(TaskState::Doing),
                labels: BTreeMap::from([("project".to_owned(), "gtd".to_owned())]),
            })
            .await
            .unwrap();
        assert_eq!(result.revision, 3);
        assert_eq!(result.tasks.len(), 1);
    }

    #[tokio::test]
    async fn list_loose_scan_returns_one_latest_event_for_each_task() {
        let (_directory, repository) = repository();
        let first = repository.create("First".to_owned()).await.unwrap();
        let second = repository.create("Second".to_owned()).await.unwrap();
        let third = repository.create("Third".to_owned()).await.unwrap();

        update_state(
            &repository,
            first.event.task_id,
            TaskList::Inbox,
            TaskState::Doing,
            None,
        )
        .await
        .unwrap();
        update_state(
            &repository,
            second.event.task_id,
            TaskList::NextAction,
            TaskState::Pending,
            None,
        )
        .await
        .unwrap();

        let result = repository.list(TaskFilter::default()).await.unwrap();
        assert_eq!(result.revision, 5);
        assert_eq!(result.tasks.len(), 3);
        assert_eq!(
            result
                .tasks
                .iter()
                .find(|task| task.metadata.id == first.event.task_id)
                .map(|task| (task.list, task.state)),
            Some((TaskList::Inbox, TaskState::Doing))
        );
        assert_eq!(
            result
                .tasks
                .iter()
                .find(|task| task.metadata.id == second.event.task_id)
                .map(|task| (task.list, task.state)),
            Some((TaskList::NextAction, TaskState::Pending))
        );
        assert_eq!(
            result
                .tasks
                .iter()
                .find(|task| task.metadata.id == third.event.task_id)
                .map(|task| (task.list, task.state)),
            Some((TaskList::Inbox, TaskState::Pending))
        );
    }

    #[tokio::test]
    async fn invalid_transition_does_not_append_an_event() {
        let (_directory, repository) = repository();
        let created = repository.create("Not ready".to_owned()).await.unwrap();
        let error = update_state(
            &repository,
            created.event.task_id,
            TaskList::Archive,
            TaskState::Done,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));
        assert_eq!(
            repository.revision_state().await.unwrap().current_revision,
            1
        );
    }

    #[tokio::test]
    async fn watch_replays_committed_events_in_revision_order() {
        let (_directory, repository) = repository();
        let first = repository.create("First".to_owned()).await.unwrap();
        let second = repository.create("Second".to_owned()).await.unwrap();
        update_state(
            &repository,
            first.event.task_id,
            TaskList::Inbox,
            TaskState::Doing,
            None,
        )
        .await
        .unwrap();

        let events = repository.events_from(1, 100).await.unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.task.metadata.revision)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(events[0].task.metadata.id, first.event.task_id);
        assert_eq!(events[1].task.metadata.id, second.event.task_id);
        assert!(events[0].prev_task.is_none());
        let prev_task = events[2].prev_task.as_ref().unwrap();
        assert_eq!(prev_task.metadata.id, first.event.task_id);
        assert_eq!(prev_task.metadata.revision, 1);
        assert_eq!(prev_task.metadata.created_at, first.event.created_at);
        assert_eq!(prev_task.metadata.updated_at, first.event.updated_at);
        assert_eq!(prev_task.description, "First");
        assert_eq!(prev_task.state, TaskState::Pending);
    }

    #[tokio::test]
    async fn watch_builds_prev_task_without_loading_prev_revision() {
        let (_directory, repository) = repository();
        let created = repository
            .create("Self-contained event".to_owned())
            .await
            .unwrap();
        let updated = update_state(
            &repository,
            created.event.task_id,
            TaskList::Inbox,
            TaskState::Doing,
            None,
        )
        .await
        .unwrap();

        repository
            .run(move |connection| {
                diesel::delete(task_events::table.find(created.event.revision))
                    .execute(connection)
                    .map_err(StoreError::database)?;
                Ok(())
            })
            .await
            .unwrap();

        let events = repository
            .events_from(updated.event.revision, 1)
            .await
            .unwrap();
        let prev_task = events[0].prev_task.as_ref().unwrap();
        assert_eq!(prev_task.metadata.revision, created.event.revision);
        assert_eq!(prev_task.metadata.created_at, created.event.created_at);
        assert_eq!(prev_task.metadata.updated_at, created.event.updated_at);
        assert_eq!(prev_task.description, created.event.description);
    }

    #[tokio::test]
    async fn concurrent_writers_get_distinct_ids_and_revisions() {
        let (_directory, repository) = repository();
        let other = repository.clone();
        let (first, second) = tokio::join!(
            repository.create("First writer".to_owned()),
            other.create("Second writer".to_owned())
        );
        let first = first.unwrap().event;
        let second = second.unwrap().event;
        assert_ne!(first.task_id, second.task_id);
        let mut revisions = [first.revision, second.revision];
        revisions.sort_unstable();
        assert_eq!(revisions, [1, 2]);
    }

    #[tokio::test]
    async fn database_replay_survives_repository_restart() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("restart.db");
        let task_id = {
            let repository = SqliteRepository::new(database.to_str().unwrap()).unwrap();
            repository
                .create("Persisted event".to_owned())
                .await
                .unwrap()
                .event
                .task_id
        };

        let repository = SqliteRepository::new(database.to_str().unwrap()).unwrap();
        let events = repository.events_from(1, 100).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].task.metadata.id, task_id);
        assert!(events[0].prev_task.is_none());
    }

    #[tokio::test]
    async fn task_event_rows_reject_updates() {
        let (_directory, repository) = repository();
        repository.create("Immutable".to_owned()).await.unwrap();
        let error = repository
            .run(|connection| {
                diesel::sql_query("UPDATE task_events SET description = 'changed'")
                    .execute(connection)
                    .map_err(StoreError::database)?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Database(_)));
    }

    #[tokio::test]
    async fn compaction_keeps_anchor_and_never_reuses_revision() {
        let (_directory, repository) = repository();
        let created = repository.create("Compact me".to_owned()).await.unwrap();
        update_state(
            &repository,
            created.event.task_id,
            TaskList::Inbox,
            TaskState::Doing,
            None,
        )
        .await
        .unwrap();
        update_state(
            &repository,
            created.event.task_id,
            TaskList::Archive,
            TaskState::Trash,
            None,
        )
        .await
        .unwrap();

        let state = repository.compact(2).await.unwrap();
        assert_eq!(state.scheduled_revision, 2);
        assert_eq!(state.finished_revision, 2);
        assert!(matches!(
            repository.events_from(2, 100).await.unwrap_err(),
            StoreError::Compacted { .. }
        ));
        assert_eq!(repository.events_from(3, 100).await.unwrap().len(), 1);
        assert_eq!(
            repository.get(created.event.task_id).await.unwrap().state,
            TaskState::Trash
        );

        let next = repository.create("After compact".to_owned()).await.unwrap();
        assert_eq!(next.event.revision, 4);
    }
}
