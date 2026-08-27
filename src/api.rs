use std::{collections::BTreeMap, convert::Infallible, time::Duration};

use async_stream::stream;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::{get, post},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::{
    domain::{
        CreateTaskRequest, Task, TaskEvent, TaskFilter, TaskList, TaskState, TransitionRequest,
        parse_label,
    },
    repository::{DynRepository, StoreError},
};

#[derive(Clone)]
pub struct AppState {
    pub repository: DynRepository,
    pub events: broadcast::Sender<TaskEvent>,
}

pub fn router(repository: DynRepository) -> (Router, AppState) {
    let (events, _) = broadcast::channel(128);
    let state = AppState { repository, events };
    let router = Router::new()
        .route("/health", get(health))
        .route("/api/tasks", get(list_tasks).post(create_task))
        .route("/api/tasks/{id}", get(get_task))
        .route("/api/tasks/{id}/actions/{action}", post(transition_task))
        .route("/api/events", get(task_events))
        .with_state(state.clone());
    (router, state)
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn create_task(
    State(state): State<AppState>,
    Json(request): Json<CreateTaskRequest>,
) -> Result<(StatusCode, Json<Task>), ApiError> {
    let task = state.repository.create(request.description).await?;
    publish(&state, "created", &task);
    Ok((StatusCode::CREATED, Json(task)))
}

async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<i32>,
) -> Result<Json<Task>, ApiError> {
    Ok(Json(state.repository.get(id).await?))
}

async fn list_tasks(
    State(state): State<AppState>,
    Query(query): Query<TaskQuery>,
) -> Result<Json<Vec<Task>>, ApiError> {
    revive_and_publish(&state).await?;
    let filter = query.try_into()?;
    Ok(Json(state.repository.list(filter).await?))
}

async fn transition_task(
    State(state): State<AppState>,
    Path((id, action)): Path<(i32, String)>,
    Json(request): Json<TransitionRequest>,
) -> Result<Json<Task>, ApiError> {
    let action = action.parse().map_err(StoreError::Validation)?;
    let task = state.repository.transition(id, action, request).await?;
    publish(&state, "updated", &task);
    Ok(Json(task))
}

async fn task_events(
    State(state): State<AppState>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let mut receiver = state.events.subscribe();
    let events = stream! {
        loop {
            match receiver.recv().await {
                Ok(task_event) => {
                    if let Ok(event) = Event::default()
                        .event(&task_event.kind)
                        .json_data(&task_event)
                    {
                        yield Ok(event);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(events).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

pub async fn revive_and_publish(state: &AppState) -> Result<(), ApiError> {
    for task in state.repository.revive_due(Utc::now()).await? {
        publish(state, "revived", &task);
    }
    Ok(())
}

fn publish(state: &AppState, kind: &str, task: &Task) {
    let _ = state.events.send(TaskEvent {
        kind: kind.to_owned(),
        task: task.clone(),
    });
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Debug, Default, Deserialize)]
struct TaskQuery {
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
            StoreError::Validation(_) => StatusCode::BAD_REQUEST,
            StoreError::Conflict(_) => StatusCode::CONFLICT,
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

    use crate::repository::SqliteRepository;

    use super::*;

    #[tokio::test]
    async fn api_creates_and_lists_an_inbox_task() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("api.db");
        let repository = Arc::new(SqliteRepository::new(database.to_str().unwrap()).unwrap());
        let (app, _) = router(repository);

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/tasks")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"description":"Capture me"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::get("/api/tasks?list=in")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let tasks: Vec<Task> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].description, "Capture me");
    }
}
