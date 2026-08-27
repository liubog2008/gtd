use std::{collections::BTreeMap, fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskContext {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: i32,
    pub description: String,
    pub list: TaskList,
    pub state: TaskState,
    pub context: TaskContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revisit_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskRequest {
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskAction {
    Start,
    Pick,
    Done,
    Trash,
    Defer,
    Delegate,
    Maybe,
    Activate,
}

impl TaskAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Pick => "pick",
            Self::Done => "done",
            Self::Trash => "trash",
            Self::Defer => "defer",
            Self::Delegate => "delegate",
            Self::Maybe => "maybe",
            Self::Activate => "activate",
        }
    }
}

impl fmt::Display for TaskAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskAction {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "start" => Ok(Self::Start),
            "pick" => Ok(Self::Pick),
            "done" => Ok(Self::Done),
            "trash" => Ok(Self::Trash),
            "defer" => Ok(Self::Defer),
            "delegate" => Ok(Self::Delegate),
            "maybe" => Ok(Self::Maybe),
            "activate" => Ok(Self::Activate),
            _ => Err(format!("unknown task action '{value}'")),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContextPatch {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransitionRequest {
    #[serde(default)]
    pub context: ContextPatch,
    #[serde(default)]
    pub revisit_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    pub list: Option<TaskList>,
    pub state: Option<TaskState>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    pub kind: String,
    pub task: Task,
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
}
