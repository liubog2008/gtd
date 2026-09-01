use std::{collections::BTreeMap, convert::Infallible, time::Duration};

use async_stream::stream;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    domain::{
        CreateTaskRequest, Task, TaskFilter, TaskList, TaskListMetadata, TaskListResponse,
        TaskState, UpdateTaskRequest, parse_label,
    },
    repository::{DynRepository, StoreError},
};

const WATCH_BATCH_SIZE: i64 = 256;
const WATCH_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct AppState {
    pub repository: DynRepository,
    pub revisions: broadcast::Sender<i64>,
}

pub fn router(repository: DynRepository) -> (Router, AppState) {
    let (revisions, _) = broadcast::channel(128);
    let state = AppState {
        repository,
        revisions,
    };
    let api = Router::new()
        .route("/tasks", get(tasks).post(create_task))
        .route("/tasks/{id}", get(get_task).put(update_task));
    let router = Router::new()
        .route("/health", get(health))
        .nest("/api/v1", api)
        .with_state(state.clone());
    (router, state)
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn create_task(
    State(state): State<AppState>,
    Json(request): Json<CreateTaskRequest>,
) -> Result<(StatusCode, HeaderMap, Json<Task>), ApiError> {
    let result = state.repository.create(request.description).await?;
    let revision = result.event.revision;
    let task = result.event.task();
    notify(&state, revision);
    Ok((StatusCode::CREATED, revision_headers(revision)?, Json(task)))
}

async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<(HeaderMap, Json<Task>), ApiError> {
    let task = state.repository.get(id).await?;
    Ok((revision_headers(task.metadata.revision)?, Json(task)))
}

async fn tasks(
    State(state): State<AppState>,
    Query(query): Query<TaskQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let watch = query.watch;
    let revision = query.revision;
    let filter = query.try_into()?;

    if watch {
        return Ok(task_watch(state, revision, headers, filter)
            .await?
            .into_response());
    }
    if revision.is_some() {
        return Err(StoreError::Validation("revision requires watch=true".to_owned()).into());
    }

    let result = state.repository.list(filter).await?;
    let response = TaskListResponse {
        metadata: TaskListMetadata {
            revision: result.revision,
        },
        items: result.tasks,
    };
    Ok((revision_headers(result.revision)?, Json(response)).into_response())
}

async fn update_task(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateTaskRequest>,
) -> Result<(HeaderMap, Json<Task>), ApiError> {
    let result = state.repository.update(id, request).await?;
    let revision = result.event.revision;
    let task = result.event.task();
    notify(&state, revision);
    Ok((revision_headers(revision)?, Json(task)))
}

async fn task_watch(
    state: AppState,
    revision: Option<i64>,
    headers: HeaderMap,
    filter: TaskFilter,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Subscribe before reading the database watermark. Polling is the durable fallback, while
    // this ordering avoids an unnecessary one-second delay for a concurrent commit.
    let mut receiver = state.revisions.subscribe();
    let revision_state = state.repository.revision_state().await?;
    let mut next_revision =
        resolve_watch_revision(revision, &headers, revision_state.current_revision)?;
    validate_watch_revision(
        next_revision,
        revision_state.current_revision,
        revision_state.scheduled_revision,
    )?;
    let repository = state.repository.clone();

    let events = stream! {
        loop {
            match repository.events_from(next_revision, WATCH_BATCH_SIZE).await {
                Ok(batch) if !batch.is_empty() => {
                    for task_event in batch {
                        let revision = task_event.task.metadata.revision;
                        next_revision = revision + 1;
                        if !task_event.matches_filter(&filter) {
                            continue;
                        }
                        if let Ok(event) = Event::default()
                            .id(revision.to_string())
                            .event(task_event.event_type.as_str())
                            .json_data(&task_event)
                        {
                            yield Ok(event);
                        }
                    }
                    continue;
                }
                Ok(_) => {}
                Err(StoreError::Compacted { compacted_revision, .. }) => {
                    let current_revision = repository
                        .revision_state()
                        .await
                        .map(|state| state.current_revision)
                        .unwrap_or(compacted_revision);
                    if let Ok(event) = Event::default().event("compacted").json_data(
                        CompactedEvent {
                            compacted_revision,
                            current_revision,
                        },
                    ) {
                        yield Ok(event);
                    }
                    break;
                }
                Err(error) => {
                    yield Ok(Event::default().event("error").data(error.to_string()));
                    break;
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(WATCH_POLL_INTERVAL) => {}
                notification = receiver.recv() => {
                    match notification {
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => {
                            tokio::time::sleep(WATCH_POLL_INTERVAL).await;
                        }
                    }
                }
            }
        }
    };

    Ok(Sse::new(events).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

fn notify(state: &AppState, revision: i64) {
    let _ = state.revisions.send(revision);
}

fn revision_headers(revision: i64) -> Result<HeaderMap, ApiError> {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::from_str(&revision.to_string())
        .map_err(|error| StoreError::Database(error.to_string()))?;
    headers.insert("x-revision", value);
    Ok(headers)
}

fn resolve_watch_revision(
    revision: Option<i64>,
    headers: &HeaderMap,
    current_revision: i64,
) -> Result<i64, ApiError> {
    if let Some(revision) = revision {
        return Ok(revision);
    }
    if let Some(last_event_id) = headers.get("last-event-id") {
        let value = last_event_id
            .to_str()
            .map_err(|_| StoreError::Validation("Last-Event-ID must be an integer".to_owned()))?;
        let revision = value
            .parse::<i64>()
            .map_err(|_| StoreError::Validation("Last-Event-ID must be an integer".to_owned()))?;
        return revision
            .checked_add(1)
            .ok_or_else(|| StoreError::Validation("Last-Event-ID is too large".to_owned()).into());
    }
    current_revision
        .checked_add(1)
        .ok_or_else(|| StoreError::Database("current revision overflow".to_owned()).into())
}

fn validate_watch_revision(
    revision: i64,
    current_revision: i64,
    scheduled_revision: i64,
) -> Result<(), ApiError> {
    if revision <= scheduled_revision {
        return Err(StoreError::Compacted {
            requested: revision,
            compacted_revision: scheduled_revision,
        }
        .into());
    }
    if revision > current_revision + 1 {
        return Err(StoreError::FutureRevision {
            requested: revision,
            current_revision,
        }
        .into());
    }
    Ok(())
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskQuery {
    #[serde(default)]
    watch: bool,
    #[serde(default)]
    revision: Option<i64>,
    #[serde(default, rename = "list")]
    list_name: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    labels: Option<String>,
}

impl TryFrom<TaskQuery> for TaskFilter {
    type Error = ApiError;

    fn try_from(query: TaskQuery) -> Result<Self, Self::Error> {
        let list = query
            .list_name
            .map(|value| value.parse::<TaskList>())
            .transpose()
            .map_err(StoreError::Validation)?;
        let state = query
            .state
            .map(|value| value.parse::<TaskState>())
            .transpose()
            .map_err(StoreError::Validation)?;
        let mut labels = BTreeMap::new();
        if let Some(raw_labels) = query.labels {
            for raw_label in raw_labels.split(',').filter(|value| !value.is_empty()) {
                let (key, value) = parse_label(raw_label).map_err(StoreError::Validation)?;
                labels.insert(key, value);
            }
        }
        Ok(TaskFilter {
            list,
            state,
            labels,
        })
    }
}

#[derive(Serialize)]
struct CompactedEvent {
    compacted_revision: i64,
    current_revision: i64,
}

#[derive(Debug)]
pub struct ApiError(StoreError);

impl From<StoreError> for ApiError {
    fn from(value: StoreError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            StoreError::NotFound(_) => StatusCode::NOT_FOUND,
            StoreError::Validation(_) | StoreError::FutureRevision { .. } => {
                StatusCode::BAD_REQUEST
            }
            StoreError::Conflict(_) | StoreError::RevisionConflict { .. } => StatusCode::CONFLICT,
            StoreError::Compacted { .. } => StatusCode::GONE,
            StoreError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = self.0.to_string();
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::repository::{SqliteRepository, TaskRepository};

    use super::*;

    #[tokio::test]
    async fn api_creates_and_lists_an_inbox_task_with_revision_headers() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("api.db");
        let repository = Arc::new(SqliteRepository::new(database.to_str().unwrap()).unwrap());
        let (app, _) = router(repository);

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/tasks")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"description":"Capture me"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-revision"], "1");
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["labels"], serde_json::json!({}));
        assert_eq!(json["metadata"]["revision"], 1);
        assert!(json["metadata"].get("id").is_some());
        assert!(json["metadata"].get("created_at").is_some());
        assert!(json["metadata"].get("updated_at").is_some());
        assert!(json.get("id").is_none());
        assert!(json.get("revision").is_none());
        assert!(json.get("created_at").is_none());
        assert!(json.get("updated_at").is_none());
        assert!(json.get("context").is_none());
        let task: Task = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(task.metadata.revision, 1);
        assert_eq!(task.metadata.id.get_version_num(), 7);

        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/tasks/{}", task.metadata.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-revision"], "1");

        let response = app
            .clone()
            .oneshot(
                Request::get("/api/v1/tasks?list=in")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-revision"], "1");
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let tasks: TaskListResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(tasks.metadata.revision, 1);
        assert_eq!(tasks.items.len(), 1);
        assert_eq!(tasks.items[0].metadata.revision, 1);
        assert_eq!(tasks.items[0].description, "Capture me");

        let response = app
            .clone()
            .oneshot(
                Request::put(format!("/api/v1/tasks/{}", task.metadata.id))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"metadata":{"revision":1},"description":"legacy","list":"in","state":"doing","context":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let response = app
            .clone()
            .oneshot(
                Request::put(format!("/api/v1/tasks/{}", task.metadata.id))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"metadata":{"revision":1},"description":"Capture me with details","list":"in","state":"doing","labels":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-revision"], "2");
        let task: Task =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(task.metadata.revision, 2);
        assert_eq!(task.state, TaskState::Doing);
        assert_eq!(task.description, "Capture me with details");

        let response = app
            .oneshot(
                Request::put(format!("/api/v1/tasks/{}", task.metadata.id))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"metadata":{"revision":1},"description":"stale","list":"archive","state":"done","labels":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains("expected revision 1, current revision 2"));
    }

    #[tokio::test]
    async fn legacy_and_unversioned_task_routes_are_not_exposed() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("routes.db");
        let repository = Arc::new(SqliteRepository::new(database.to_str().unwrap()).unwrap());
        let (app, _) = router(repository);

        for request in [
            Request::get("/api/tasks").body(Body::empty()).unwrap(),
            Request::get("/api/events").body(Body::empty()).unwrap(),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        let response = app
            .oneshot(
                Request::get("/api/v1/tasks?watch=true&start_revision=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn watch_revision_prefers_query_over_last_event_id() {
        let mut headers = HeaderMap::new();
        headers.insert("last-event-id", HeaderValue::from_static("4"));
        assert_eq!(resolve_watch_revision(Some(7), &headers, 9).unwrap(), 7);
    }

    #[tokio::test]
    async fn watch_replays_persisted_events_with_revision_ids() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("watch.db");
        let repository = Arc::new(SqliteRepository::new(database.to_str().unwrap()).unwrap());
        repository.create("Watch me".to_owned()).await.unwrap();
        let (app, _) = router(repository);

        let response = app
            .oneshot(
                Request::get("/api/v1/tasks?watch=true&revision=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let payload = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
        assert!(payload.contains("id: 1"));
        assert!(payload.contains("event: task.created"));
        let data = payload
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap();
        let event: serde_json::Value = serde_json::from_str(data).unwrap();
        assert_eq!(event["event_type"], "task.created");
        assert_eq!(event["task"]["metadata"]["revision"], 1);
        assert!(event.get("prev_task").is_none());
    }

    #[tokio::test]
    async fn watch_uses_generic_updated_events_without_client_operation_fields() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("updated-watch.db");
        let repository = Arc::new(SqliteRepository::new(database.to_str().unwrap()).unwrap());
        let created = repository.create("Update me".to_owned()).await.unwrap();
        repository
            .update(
                created.event.task_id,
                UpdateTaskRequest {
                    metadata: crate::domain::UpdateTaskMetadata {
                        revision: created.event.revision,
                    },
                    description: "Update me with details".to_owned(),
                    list: TaskList::NextAction,
                    state: TaskState::Pending,
                    labels: Default::default(),
                },
            )
            .await
            .unwrap();
        let (app, _) = router(repository);

        let response = app
            .oneshot(
                Request::get("/api/v1/tasks?watch=true&revision=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let payload = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
        assert!(payload.contains("event: task.updated"));
        let data = payload
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap();
        let event: serde_json::Value = serde_json::from_str(data).unwrap();
        let fields = event.as_object().unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(event["event_type"], "task.updated");
        assert_eq!(event["task"]["metadata"]["revision"], 2);
        assert_eq!(event["task"]["description"], "Update me with details");
        assert_eq!(event["prev_task"]["metadata"]["revision"], 1);
        assert_eq!(
            event["prev_task"]["metadata"]["created_at"],
            serde_json::to_value(created.event.created_at).unwrap()
        );
        assert_eq!(
            event["prev_task"]["metadata"]["updated_at"],
            serde_json::to_value(created.event.updated_at).unwrap()
        );
        assert_eq!(event["prev_task"]["description"], "Update me");
        assert!(!payload.contains("\"action\":"));
        assert!(!payload.contains("prev_created_at"));
        assert!(!payload.contains("prev_updated_at"));
        assert!(!payload.contains("context"));
    }

    #[tokio::test]
    async fn watch_supports_list_state_and_label_filters() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("filtered-watch.db");
        let repository = Arc::new(SqliteRepository::new(database.to_str().unwrap()).unwrap());
        let created = repository
            .create("Filtered watch".to_owned())
            .await
            .unwrap();
        let pending = repository
            .update(
                created.event.task_id,
                UpdateTaskRequest {
                    metadata: crate::domain::UpdateTaskMetadata {
                        revision: created.event.revision,
                    },
                    description: created.event.description.clone(),
                    list: TaskList::NextAction,
                    state: TaskState::Pending,
                    labels: BTreeMap::from([("project".to_owned(), "gtd".to_owned())]),
                },
            )
            .await
            .unwrap();
        repository
            .update(
                created.event.task_id,
                UpdateTaskRequest {
                    metadata: crate::domain::UpdateTaskMetadata {
                        revision: pending.event.revision,
                    },
                    description: pending.event.description,
                    list: TaskList::NextAction,
                    state: TaskState::Doing,
                    labels: pending.event.labels,
                },
            )
            .await
            .unwrap();
        let (app, _) = router(repository);

        let response = app
            .oneshot(
                Request::get(
                    "/api/v1/tasks?watch=true&revision=1&list=next-action&state=doing&labels=project:gtd",
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let payload = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
        assert!(payload.contains("id: 3"));
        assert!(!payload.contains("id: 1"));
        assert!(!payload.contains("id: 2"));
    }

    #[tokio::test]
    async fn watch_rejects_an_already_compacted_revision() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("compacted-watch.db");
        let repository = Arc::new(SqliteRepository::new(database.to_str().unwrap()).unwrap());
        repository.create("Compact me".to_owned()).await.unwrap();
        repository.compact(1).await.unwrap();
        let (app, _) = router(repository);

        let response = app
            .oneshot(
                Request::get("/api/v1/tasks?watch=true&revision=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::GONE);
    }
}
