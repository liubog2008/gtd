use std::{collections::BTreeMap, fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub fn parse_label(value: &str) -> Result<(String, String), String> {
    let (key, value) = value
        .split_once('=')
        .or_else(|| value.split_once(':'))
        .ok_or_else(|| format!("invalid label '{value}'; expected key:value"))?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        return Err("label key and value must not be empty".to_owned());
    }
    if key.contains([':', '=']) {
        return Err(format!("label key '{key}' must not contain ':' or '='"));
    }
    Ok((key.to_owned(), value.to_owned()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskList {
    #[serde(rename = "in")]
    Inbox,
    NextAction,
    WaitingFor,
    SomedayMaybe,
    Archive,
}

impl TaskList {
    pub const ACTIVE: [Self; 4] = [
        Self::Inbox,
        Self::NextAction,
        Self::WaitingFor,
        Self::SomedayMaybe,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inbox => "in",
            Self::NextAction => "next-action",
            Self::WaitingFor => "waiting-for",
            Self::SomedayMaybe => "someday-maybe",
            Self::Archive => "archive",
        }
    }
}

impl fmt::Display for TaskList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskList {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "in" | "inbox" => Ok(Self::Inbox),
            "next-action" | "next" => Ok(Self::NextAction),
            "waiting-for" | "waiting" => Ok(Self::WaitingFor),
            "someday-maybe" | "someday" | "maybe" => Ok(Self::SomedayMaybe),
            "archive" => Ok(Self::Archive),
            _ => Err(format!(
                "unknown list '{value}'; expected in, next-action, waiting-for, someday-maybe, or archive"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskState {
    Pending,
    Doing,
    Done,
    Trash,
}

impl TaskState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Doing => "doing",
            Self::Done => "done",
            Self::Trash => "trash",
        }
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "doing" => Ok(Self::Doing),
            "done" => Ok(Self::Done),
            "trash" => Ok(Self::Trash),
            _ => Err(format!(
                "unknown state '{value}'; expected pending, doing, done, or trash"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMetadata {
    pub id: Uuid,
    /// Revision of the event that produced this task state.
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub metadata: TaskMetadata,
    pub description: String,
    pub list: TaskList,
    pub state: TaskState,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskListMetadata {
    /// Global event revision at which the list snapshot was read.
    pub revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskListResponse {
    pub metadata: TaskListMetadata,
    pub items: Vec<Task>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskWatchEvent {
    pub event_type: TaskEventType,
    pub task: Task,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_task: Option<Task>,
}

impl TaskWatchEvent {
    pub fn matches_filter(&self, filter: &TaskFilter) -> bool {
        filter.matches(&self.task)
            || self
                .prev_task
                .as_ref()
                .is_some_and(|task| filter.matches(task))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTaskRequest {
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskEventType {
    #[serde(rename = "task.created")]
    Created,
    #[serde(rename = "task.updated")]
    Updated,
}

impl TaskEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "task.created",
            Self::Updated => "task.updated",
        }
    }
}

impl FromStr for TaskEventType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "task.created" => Ok(Self::Created),
            "task.updated" => Ok(Self::Updated),
            _ => Err(format!("unknown task event type '{value}'")),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskEdit {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateTaskMetadata {
    pub revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateTaskRequest {
    pub metadata: UpdateTaskMetadata,
    pub description: String,
    pub list: TaskList,
    pub state: TaskState,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    pub list: Option<TaskList>,
    pub state: Option<TaskState>,
    pub labels: BTreeMap<String, String>,
}

impl TaskFilter {
    pub fn matches(&self, task: &Task) -> bool {
        self.list.is_none_or(|list| task.list == list)
            && self.state.is_none_or(|state| task.state == state)
            && self
                .labels
                .iter()
                .all(|(key, value)| task.labels.get(key) == Some(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEvent {
    pub revision: i64,
    pub task_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_revision: Option<i64>,
    pub event_type: TaskEventType,

    pub description: String,
    pub list_name: TaskList,
    pub state: TaskState,
    pub labels: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_list_name: Option<TaskList>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_state: Option<TaskState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_labels: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_updated_at: Option<DateTime<Utc>>,
}

impl TaskEvent {
    pub fn task(&self) -> Task {
        Task {
            metadata: TaskMetadata {
                id: self.task_id,
                revision: self.revision,
                created_at: self.created_at,
                updated_at: self.updated_at,
            },
            description: self.description.clone(),
            list: self.list_name,
            state: self.state,
            labels: self.labels.clone(),
        }
    }

    pub fn prev_task(&self) -> Option<Task> {
        Some(Task {
            metadata: TaskMetadata {
                id: self.task_id,
                revision: self.prev_revision?,
                created_at: self.created_at,
                updated_at: self.prev_updated_at?,
            },
            description: self.prev_description.clone()?,
            list: self.prev_list_name?,
            state: self.prev_state?,
            labels: self.prev_labels.clone()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_aliases_parse_to_canonical_names() {
        assert_eq!("inbox".parse(), Ok(TaskList::Inbox));
        assert_eq!("next".parse(), Ok(TaskList::NextAction));
        assert_eq!(TaskList::SomedayMaybe.to_string(), "someday-maybe");
    }

    #[test]
    fn event_type_uses_stable_names() {
        assert_eq!(TaskEventType::Updated.as_str(), "task.updated");
        assert_eq!("task.updated".parse(), Ok(TaskEventType::Updated));
    }

    #[test]
    fn none_options_are_omitted_from_json() {
        let edit = serde_json::to_value(TaskEdit::default()).unwrap();
        assert!(edit.get("description").is_none());

        let task = Task {
            metadata: TaskMetadata {
                id: Uuid::now_v7(),
                revision: 1,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            description: "Created task".to_owned(),
            list: TaskList::Inbox,
            state: TaskState::Pending,
            labels: BTreeMap::new(),
        };
        let event = serde_json::to_value(TaskEvent {
            revision: task.metadata.revision,
            task_id: task.metadata.id,
            prev_revision: None,
            event_type: TaskEventType::Created,
            description: task.description.clone(),
            list_name: task.list,
            state: task.state,
            labels: task.labels.clone(),
            created_at: task.metadata.created_at,
            updated_at: task.metadata.updated_at,
            prev_description: None,
            prev_list_name: None,
            prev_state: None,
            prev_labels: None,
            prev_updated_at: None,
        })
        .unwrap();
        for field in [
            "prev_revision",
            "prev_description",
            "prev_list_name",
            "prev_state",
            "prev_labels",
            "prev_updated_at",
        ] {
            assert!(event.get(field).is_none(), "{field} must be omitted");
        }

        let watch = serde_json::to_value(TaskWatchEvent {
            event_type: TaskEventType::Created,
            task,
            prev_task: None,
        })
        .unwrap();
        assert!(watch.get("prev_task").is_none());
    }

    #[test]
    fn watch_filter_matches_both_sides_of_a_change() {
        let now = Utc::now();
        let id = Uuid::now_v7();
        let before = Task {
            metadata: TaskMetadata {
                id,
                revision: 1,
                created_at: now,
                updated_at: now,
            },
            description: "Move out of the filtered set".to_owned(),
            list: TaskList::NextAction,
            state: TaskState::Doing,
            labels: BTreeMap::from([("project".to_owned(), "gtd".to_owned())]),
        };
        let after = Task {
            metadata: TaskMetadata {
                revision: 2,
                ..before.metadata.clone()
            },
            list: TaskList::Archive,
            state: TaskState::Done,
            ..before.clone()
        };
        let filter = TaskFilter {
            list: Some(TaskList::NextAction),
            state: Some(TaskState::Doing),
            labels: BTreeMap::from([("project".to_owned(), "gtd".to_owned())]),
        };
        let event = TaskWatchEvent {
            event_type: TaskEventType::Updated,
            task: after,
            prev_task: Some(before),
        };

        assert!(event.matches_filter(&filter));
        assert!(!filter.matches(&event.task));
    }
}
