use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::blocking::{Client, Response};
use serde::{Deserialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::domain::{
    CreateTaskRequest, Task, TaskEdit, TaskFilter, TaskList, TaskListResponse, TaskState,
    UpdateTaskMetadata, UpdateTaskRequest,
};

const API_V1: &str = "/api/v1";

#[derive(Clone)]
pub struct ApiClient {
    base_url: String,
    client: Client,
}

impl ApiClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_owned();
        if !base_url.starts_with("http://") {
            bail!("server URL must start with http://");
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self { base_url, client })
    }

    pub fn create(&self, description: String) -> Result<Task> {
        let response = self
            .client
            .post(format!("{}{API_V1}/tasks", self.base_url))
            .json(&CreateTaskRequest { description })
            .send()
            .context("could not reach the GTD server")?;
        decode(response)
    }

    pub fn get(&self, id: Uuid) -> Result<Task> {
        let response = self
            .client
            .get(format!("{}{API_V1}/tasks/{id}", self.base_url))
            .send()
            .context("could not reach the GTD server")?;
        decode(response)
    }

    pub fn list(&self, filter: &TaskFilter) -> Result<TaskListResponse> {
        let mut query = Vec::new();
        if let Some(list) = filter.list {
            query.push(("list".to_owned(), list.to_string()));
        }
        if let Some(state) = filter.state {
            query.push(("state".to_owned(), state.to_string()));
        }
        if !filter.labels.is_empty() {
            let labels = filter
                .labels
                .iter()
                .map(|(key, value)| format!("{key}:{value}"))
                .collect::<Vec<_>>()
                .join(",");
            query.push(("labels".to_owned(), labels));
        }
        let response = self
            .client
            .get(format!("{}{API_V1}/tasks", self.base_url))
            .query(&query)
            .send()
            .context("could not reach the GTD server")?;
        decode(response)
    }

    pub fn update_state(
        &self,
        id: Uuid,
        list: TaskList,
        state: TaskState,
        edit: TaskEdit,
    ) -> Result<Task> {
        let current = self.get(id)?;
        let mut labels = current.labels;
        for (key, value) in edit.labels {
            labels.insert(key, value);
        }
        let description = edit.description.unwrap_or(current.description);

        self.update(
            id,
            UpdateTaskRequest {
                metadata: UpdateTaskMetadata {
                    revision: current.metadata.revision,
                },
                description,
                list,
                state,
                labels,
            },
        )
    }

    pub fn update(&self, id: Uuid, request: UpdateTaskRequest) -> Result<Task> {
        let response = self
            .client
            .put(format!("{}{API_V1}/tasks/{id}", self.base_url))
            .json(&request)
            .send()
            .context("could not reach the GTD server")?;
        decode(response)
    }

    pub fn watch_url(&self) -> String {
        format!("{}{API_V1}/tasks?watch=true", self.base_url)
    }
}

fn decode<T: DeserializeOwned>(response: Response) -> Result<T> {
    let status = response.status();
    if status.is_success() {
        return response
            .json()
            .context("server returned an invalid JSON response");
    }

    let body = response.text().unwrap_or_default();
    let message = serde_json::from_str::<ErrorBody>(&body)
        .map(|body| body.error)
        .unwrap_or_else(|_| body.trim().to_owned());
    bail!("server returned {status}: {message}")
}

pub fn labels_from_pairs(pairs: &[String]) -> Result<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    for pair in pairs {
        let (key, value) = crate::domain::parse_label(pair).map_err(anyhow::Error::msg)?;
        labels.insert(key, value);
    }
    Ok(labels)
}

#[derive(Deserialize)]
struct ErrorBody {
    error: String,
}
