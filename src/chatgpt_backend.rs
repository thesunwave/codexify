use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::types::AppConfig;

const MAX_COMMAND_BYTES: usize = 64 * 1024;
const MAX_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_COMMANDS: usize = 64;
const MAX_RETAINED_COMMANDS: usize = 32;
const MAX_RETAINED_EVENTS: usize = 32;
const MAX_RETAINED_TOOL_ACTIVITIES: usize = 32;
const MAX_TOOL_NAME_BYTES: usize = 1024;
const MAX_TOOL_PREVIEW_BYTES: usize = 16 * 1024;
pub const DEFAULT_WAIT_MS: u64 = 115_000;
const WAIT_LEASE_GRACE_MS: u64 = 30_000;
const READY_STALE_MS: u64 = 180_000;
const WORKING_STALE_MS: u64 = 600_000;
pub const CONTROL_FIELD: &str = "chatgpt_backend_control";
pub const CONTROL_DESCRIPTION: &str = "Pending ChatGPT backend steer/cancel command for the active coding task. This JSON string is injected into ordinary Codexify tool results so control can reach a long-running task before the next backend exchange. Apply steer immediately; on cancel stop the active task and return an error through chatgpt_backend_exchange. Acknowledge the control command sequence through chatgpt_backend_exchange; until acknowledged it may be replayed.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendCommandKind {
    Task,
    Steer,
    Cancel,
    Finish,
}

impl BackendCommandKind {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "task" => Ok(Self::Task),
            "steer" => Ok(Self::Steer),
            "cancel" => Ok(Self::Cancel),
            "finish" => Ok(Self::Finish),
            _ => Err("command kind must be task, steer, cancel, or finish".into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendEventKind {
    Ready,
    Result,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCommand {
    pub seq: u64,
    pub id: String,
    pub kind: BackendCommandKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_task_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    pub created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged_at_ms: Option<u64>,
}

fn control_targets_task(command: &BackendCommand, task_seq: u64) -> bool {
    command.target_task_seq == Some(task_seq)
        || (command.target_task_seq.is_none() && command.seq > task_seq)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendEvent {
    pub seq: u64,
    pub kind: BackendEventKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_seq: Option<u64>,
    pub content: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendToolActivityStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendToolActivity {
    pub seq: u64,
    pub task_seq: u64,
    pub tool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_preview: Option<String>,
    pub status: BackendToolActivityStatus,
    pub started_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendWorkspace {
    pub active_root: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_project_root: Option<String>,
    pub managed_worktree: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree_git_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendSession {
    pub id: String,
    pub worker: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub last_heartbeat_at_ms: u64,
    pub next_command_seq: u64,
    pub next_event_seq: u64,
    #[serde(default = "default_next_tool_activity_seq")]
    pub next_tool_activity_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_task_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draining_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_reason: Option<String>,
    pub waiting: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_wait_started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_wait_returned_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<BackendWorkspace>,
    #[serde(default)]
    pub dropped_commands: u64,
    #[serde(default)]
    pub dropped_events: u64,
    #[serde(default)]
    pub idempotent_retries: u64,
    #[serde(default)]
    pub completed_tasks: u64,
    #[serde(default)]
    pub failed_tasks: u64,
    pub commands: Vec<BackendCommand>,
    pub events: Vec<BackendEvent>,
    #[serde(default)]
    pub tool_activities: Vec<BackendToolActivity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendLifecycleState {
    Attaching,
    Ready,
    Waiting,
    Queued,
    Working,
    Cancelling,
    Draining,
    Finished,
    Failed,
    Stale,
}

impl BackendLifecycleState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Attaching => "attaching",
            Self::Ready => "ready",
            Self::Waiting => "waiting",
            Self::Queued => "queued",
            Self::Working => "working",
            Self::Cancelling => "cancelling",
            Self::Draining => "draining",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendSessionInspection {
    pub session_id: String,
    pub state: BackendLifecycleState,
    pub live: bool,
    pub accepting_tasks: bool,
    pub last_activity_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    pub draining: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drain_reason: Option<String>,
    pub retained_commands: usize,
    pub retained_events: usize,
    pub pending_commands: usize,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub idempotent_retries: u64,
    pub dropped_commands: u64,
    pub dropped_events: u64,
    pub next_command_seq: u64,
    pub next_event_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_task_seq: Option<u64>,
    pub waiting: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<BackendWorkspace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event: Option<BackendEventSummary>,
    pub tasks: Vec<BackendTaskInspection>,
    pub tool_activities: Vec<BackendToolActivity>,
    pub timeline: Vec<BackendTimelineEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendEventSummary {
    pub seq: u64,
    pub kind: BackendEventKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_seq: Option<u64>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendTaskInspection {
    pub command_seq: u64,
    pub queued_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<BackendEventKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendTimelineEntry {
    pub at_ms: u64,
    pub kind: &'static str,
    pub seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_kind: Option<BackendCommandKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_kind: Option<BackendEventKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_status: Option<BackendToolActivityStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_preview: Option<String>,
}

fn default_next_tool_activity_seq() -> u64 {
    1
}

impl BackendSession {
    pub fn state(&self) -> &'static str {
        self.lifecycle_state_at(self.updated_at_ms).as_str()
    }

    fn last_activity_at_ms(&self) -> u64 {
        self.last_heartbeat_at_ms
    }

    fn pending_cancel_for_active_task(&self) -> bool {
        let Some(active) = self.active_task_seq else {
            return false;
        };
        self.commands.iter().any(|command| {
            command.kind == BackendCommandKind::Cancel
                && command.acknowledged_at_ms.is_none()
                && control_targets_task(command, active)
        })
    }

    fn stale_reason_at(&self, now: u64) -> Option<String> {
        if self.closed_at_ms.is_some() || self.failed_at_ms.is_some() || self.stale_at_ms.is_some()
        {
            return self.stale_at_ms.map(|_| {
                self.stale_reason
                    .clone()
                    .unwrap_or_else(|| "session was previously marked stale".to_string())
            });
        }
        if self.waiting {
            let started = self
                .last_wait_started_at_ms
                .unwrap_or(self.last_activity_at_ms());
            if now.saturating_sub(started) > DEFAULT_WAIT_MS.saturating_add(WAIT_LEASE_GRACE_MS) {
                return Some("exchange wait lease expired".into());
            }
            return None;
        }
        let age = now.saturating_sub(self.last_activity_at_ms());
        if self.active_task_seq.is_some() {
            (age > WORKING_STALE_MS).then(|| "active task heartbeat expired".into())
        } else {
            (age > READY_STALE_MS).then(|| "backend turn stopped renewing exchange".into())
        }
    }

    fn lifecycle_state_at(&self, now: u64) -> BackendLifecycleState {
        if self.failed_at_ms.is_some() {
            BackendLifecycleState::Failed
        } else if self.closed_at_ms.is_some() {
            BackendLifecycleState::Finished
        } else if self.stale_at_ms.is_some() || self.stale_reason_at(now).is_some() {
            BackendLifecycleState::Stale
        } else if self.pending_cancel_for_active_task() {
            BackendLifecycleState::Cancelling
        } else if self.draining_at_ms.is_some() {
            BackendLifecycleState::Draining
        } else if self.active_task_seq.is_some() {
            BackendLifecycleState::Working
        } else if self.commands.iter().any(|command| {
            command.kind == BackendCommandKind::Task && command.acknowledged_at_ms.is_none()
        }) {
            BackendLifecycleState::Queued
        } else if self.waiting {
            BackendLifecycleState::Waiting
        } else {
            BackendLifecycleState::Ready
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeOutbound {
    pub kind: BackendEventKind,
    pub command_seq: u64,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeOutcome {
    Command(BackendCommand),
    Idle,
    TimedOut,
    Cancelled,
    Closed,
}

#[derive(Debug, Clone)]
pub struct ChatGptBackendStore {
    directory: PathBuf,
}

impl ChatGptBackendStore {
    pub fn for_current_user(config: &AppConfig) -> Result<Self, String> {
        if !config.experimental.chatgpt_bridge {
            return Err("experimental.chatgptBridge is disabled".into());
        }
        let home = crate::util::home_dir()
            .ok_or("experimental.chatgptBridge requires a home directory")?;
        Ok(Self::new_at(
            home.join(".codexify/chatgpt-backend")
                .join(scope_key(config)),
        ))
    }

    pub fn new_at(directory: PathBuf) -> Self {
        Self { directory }
    }

    pub fn attach(&self, worker: &str) -> Result<BackendSession, String> {
        self.attach_with_workspace(worker, None)
    }

    pub fn attach_with_workspace(
        &self,
        worker: &str,
        workspace: Option<BackendWorkspace>,
    ) -> Result<BackendSession, String> {
        validate_worker(worker)?;
        self.with_lock(|| {
            let mut sessions = self.read_all()?;
            sessions.sort_by_key(|session| session.created_at_ms);
            if let Some(mut session) = sessions
                .into_iter()
                .rev()
                .find(|session| session.worker == worker && session.closed_at_ms.is_none())
            {
                let now = now_ms()?;
                if let Some(reason) = session.stale_reason_at(now) {
                    session.stale_at_ms = Some(now);
                    session.stale_reason = Some(reason);
                    session.waiting = false;
                    session.updated_at_ms = now;
                    self.write_session(&session)?;
                } else {
                    if session.workspace != workspace {
                        return Err(format!(
                            "ChatGPT backend worker already has live session {} bound to a different workspace",
                            session.id
                        ));
                    }
                    return Ok(session);
                }
            }
            let now = now_ms()?;
            let ready_content = workspace
                .as_ref()
                .map(|workspace| {
                    format!(
                        "ChatGPT coding backend attached and ready in {}",
                        workspace.active_root
                    )
                })
                .unwrap_or_else(|| "ChatGPT coding backend attached".into());
            let mut session = BackendSession {
                id: new_id()?,
                worker: worker.to_string(),
                created_at_ms: now,
                updated_at_ms: now,
                last_heartbeat_at_ms: now,
                next_command_seq: 1,
                next_event_seq: 2,
                next_tool_activity_seq: 1,
                active_task_seq: None,
                closed_at_ms: None,
                stale_at_ms: None,
                stale_reason: None,
                failed_at_ms: None,
                draining_at_ms: None,
                drain_reason: None,
                waiting: false,
                last_wait_started_at_ms: None,
                last_wait_returned_at_ms: None,
                workspace,
                dropped_commands: 0,
                dropped_events: 0,
                idempotent_retries: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                commands: Vec::new(),
                events: vec![BackendEvent {
                    seq: 1,
                    kind: BackendEventKind::Ready,
                    command_seq: None,
                    content: ready_content,
                    created_at_ms: now,
                }],
                tool_activities: Vec::new(),
            };
            self.write_session(&session)?;
            session.updated_at_ms = now;
            Ok(session)
        })
    }

    pub fn list(&self) -> Result<Vec<BackendSession>, String> {
        self.with_lock(|| {
            let mut sessions = self.read_all()?;
            sessions.sort_by_key(|session| session.created_at_ms);
            Ok(sessions)
        })
    }

    pub fn list_inspections(&self) -> Result<Vec<BackendSessionInspection>, String> {
        self.with_lock(|| {
            let now = now_ms()?;
            let mut sessions = self.read_all()?;
            sessions.sort_by_key(|session| session.created_at_ms);
            let mut inspections = Vec::with_capacity(sessions.len());
            for mut session in sessions {
                let changed = mark_session_stale_if_expired(&mut session, now);
                if changed {
                    self.write_session(&session)?;
                    session = self.read_session(&session.id)?;
                }
                inspections.push(inspect_session(session, now));
            }
            Ok(inspections)
        })
    }

    pub fn session(&self, session_id: &str) -> Result<BackendSession, String> {
        validate_id(session_id)?;
        self.with_lock(|| self.read_session(session_id))
    }

    pub fn inspection(&self, session_id: &str) -> Result<BackendSessionInspection, String> {
        validate_id(session_id)?;
        self.with_lock(|| {
            let now = now_ms()?;
            let mut session = self.read_session(session_id)?;
            if mark_session_stale_if_expired(&mut session, now) {
                self.write_session(&session)?;
                session = self.read_session(session_id)?;
            }
            Ok(inspect_session(session, now))
        })
    }

    pub fn abandon_session(
        &self,
        session_id: &str,
        reason: impl Into<String>,
    ) -> Result<BackendSession, String> {
        validate_id(session_id)?;
        let reason = reason.into();
        validate_text("abandon reason", &reason, MAX_COMMAND_BYTES)?;
        self.with_lock(|| {
            let now = now_ms()?;
            let mut session = self.read_session(session_id)?;
            if session.closed_at_ms.is_some() || session.failed_at_ms.is_some() {
                return Err(format!("ChatGPT backend session {session_id} is not live"));
            }
            if session.stale_at_ms.is_none() {
                session.stale_at_ms = Some(now);
                session.stale_reason = Some(reason);
                session.waiting = false;
                session.updated_at_ms = now;
                self.write_session(&session)?;
            }
            Ok(session)
        })
    }

    pub fn drain_session(
        &self,
        session_id: &str,
        reason: impl Into<String>,
    ) -> Result<BackendSession, String> {
        validate_id(session_id)?;
        let reason = reason.into();
        validate_text("drain reason", &reason, MAX_COMMAND_BYTES)?;
        self.with_lock(|| {
            let now = now_ms()?;
            let mut session = self.read_session(session_id)?;
            if session.closed_at_ms.is_some() || session.failed_at_ms.is_some() {
                return Err(format!("ChatGPT backend session {session_id} is not live"));
            }
            if mark_session_stale_if_expired(&mut session, now) {
                self.write_session(&session)?;
            }
            if session.stale_at_ms.is_some() {
                return Err(format!(
                    "ChatGPT backend session {session_id} is stale; attach a fresh backend session"
                ));
            }
            if session.draining_at_ms.is_none() {
                session.draining_at_ms = Some(now);
                session.drain_reason = Some(reason);
                session.updated_at_ms = now;
                self.write_session(&session)?;
            }
            Ok(session)
        })
    }

    pub fn touch_worker_activity(&self, worker: &str) -> Result<(), String> {
        validate_worker(worker)?;
        self.with_lock(|| {
            let now = now_ms()?;
            let mut sessions = self.read_all()?;
            sessions.sort_by_key(|session| session.created_at_ms);
            let Some(mut session) = sessions.into_iter().rev().find(|session| {
                session.worker == worker
                    && session.closed_at_ms.is_none()
                    && session.failed_at_ms.is_none()
                    && session.stale_at_ms.is_none()
            }) else {
                return Ok(());
            };
            session.last_heartbeat_at_ms = now;
            session.updated_at_ms = now;
            self.write_session(&session)
        })
    }

    pub fn active_task_for_worker(
        &self,
        worker: &str,
    ) -> Result<Option<(String, u64, Option<BackendWorkspace>)>, String> {
        validate_worker(worker)?;
        self.with_lock(|| {
            let mut sessions = self.read_all()?;
            sessions.sort_by_key(|session| session.created_at_ms);
            let Some(mut session) = sessions.into_iter().rev().find(|session| {
                session.worker == worker
                    && session.closed_at_ms.is_none()
                    && session.failed_at_ms.is_none()
                    && session.stale_at_ms.is_none()
                    && session.active_task_seq.is_some()
            }) else {
                return Ok(None);
            };
            let now = now_ms()?;
            session.last_heartbeat_at_ms = now;
            session.updated_at_ms = now;
            let task_seq = session
                .active_task_seq
                .expect("active backend session search guarantees an active task");
            let id = session.id.clone();
            let workspace = session.workspace.clone();
            self.write_session(&session)?;
            Ok(Some((id, task_seq, workspace)))
        })
    }

    pub fn start_tool_activity(
        &self,
        session_id: &str,
        task_seq: u64,
        tool: &str,
    ) -> Result<u64, String> {
        self.start_tool_activity_with_preview(session_id, task_seq, tool, None)
    }

    pub fn start_tool_activity_with_preview(
        &self,
        session_id: &str,
        task_seq: u64,
        tool: &str,
        request_preview: Option<String>,
    ) -> Result<u64, String> {
        validate_id(session_id)?;
        validate_text("tool name", tool, MAX_TOOL_NAME_BYTES)?;
        if let Some(preview) = request_preview.as_deref() {
            validate_text("tool request preview", preview, MAX_TOOL_PREVIEW_BYTES)?;
        }
        self.with_lock(|| {
            let now = now_ms()?;
            let mut session = self.read_session(session_id)?;
            if session.closed_at_ms.is_some()
                || session.failed_at_ms.is_some()
                || session.stale_at_ms.is_some()
                || session.active_task_seq != Some(task_seq)
            {
                return Err(format!(
                    "ChatGPT backend session {session_id} no longer has task {task_seq} active"
                ));
            }
            let seq = session.next_tool_activity_seq.max(1);
            session.next_tool_activity_seq = seq.saturating_add(1);
            session.last_heartbeat_at_ms = now;
            session.updated_at_ms = now;
            session.tool_activities.push(BackendToolActivity {
                seq,
                task_seq,
                tool: tool.to_string(),
                request_preview,
                response_preview: None,
                status: BackendToolActivityStatus::Running,
                started_at_ms: now,
                completed_at_ms: None,
            });
            self.write_session(&session)?;
            Ok(seq)
        })
    }

    pub fn complete_tool_activity(
        &self,
        session_id: &str,
        activity_seq: u64,
        status: BackendToolActivityStatus,
    ) -> Result<(), String> {
        self.complete_tool_activity_with_preview(session_id, activity_seq, status, None)
    }

    pub fn complete_tool_activity_with_preview(
        &self,
        session_id: &str,
        activity_seq: u64,
        status: BackendToolActivityStatus,
        response_preview: Option<String>,
    ) -> Result<(), String> {
        validate_id(session_id)?;
        if status == BackendToolActivityStatus::Running {
            return Err("completed tool activity status must be terminal".into());
        }
        if let Some(preview) = response_preview.as_deref() {
            validate_text("tool response preview", preview, MAX_TOOL_PREVIEW_BYTES)?;
        }
        self.with_lock(|| {
            let now = now_ms()?;
            let mut session = self.read_session(session_id)?;
            let Some(activity) = session
                .tool_activities
                .iter_mut()
                .find(|activity| activity.seq == activity_seq)
            else {
                return Err(format!(
                    "unknown ChatGPT backend tool activity {activity_seq} in session {session_id}"
                ));
            };
            if activity.completed_at_ms.is_none() {
                activity.status = status;
                activity.response_preview = response_preview;
                activity.completed_at_ms = Some(now);
                session.last_heartbeat_at_ms = now;
                session.updated_at_ms = now;
                self.write_session(&session)?;
            }
            Ok(())
        })
    }

    pub fn pending_control_for_worker(
        &self,
        worker: &str,
    ) -> Result<Option<(String, BackendCommand)>, String> {
        validate_worker(worker)?;
        self.with_lock(|| {
            let mut sessions = self.read_all()?;
            sessions.sort_by_key(|session| session.created_at_ms);
            let Some(mut session) = sessions.into_iter().rev().find(|session| {
                session.worker == worker
                    && session.closed_at_ms.is_none()
                    && session.active_task_seq.is_some()
            }) else {
                return Ok(None);
            };

            let active_seq = session
                .active_task_seq
                .expect("active backend session search guarantees an active task");

            let boundary_now = now_ms()?;
            session.last_heartbeat_at_ms = boundary_now;
            session.updated_at_ms = boundary_now;

            // Reaching any ordinary Codexify tool boundary proves the model already
            // received and started processing the active task. Mark it acknowledged
            // here so a subsequent non-blocking exchange used only to ACK injected
            // control cannot replay the original task.
            if let Some(task) = session
                .commands
                .iter_mut()
                .find(|command| command.seq == active_seq)
                && task.acknowledged_at_ms.is_none()
            {
                task.acknowledged_at_ms = Some(boundary_now);
            }

            let targets_active =
                |command: &BackendCommand| control_targets_task(command, active_seq);
            let is_pending_control = |command: &BackendCommand| {
                matches!(
                    command.kind,
                    BackendCommandKind::Steer | BackendCommandKind::Cancel
                ) && command.acknowledged_at_ms.is_none()
                    && targets_active(command)
            };

            // Never skip a control the model may already have seen. Otherwise prefer
            // cancellation over an as-yet-undelivered steer, because cancel is the
            // stronger control signal and completing it will supersede later controls.
            let index = session
                .commands
                .iter()
                .position(|command| {
                    is_pending_control(command) && command.delivered_at_ms.is_some()
                })
                .or_else(|| {
                    session.commands.iter().position(|command| {
                        is_pending_control(command) && command.kind == BackendCommandKind::Cancel
                    })
                })
                .or_else(|| session.commands.iter().position(is_pending_control));
            let Some(index) = index else {
                self.write_session(&session)?;
                return Ok(None);
            };

            if session.commands[index].delivered_at_ms.is_none() {
                let now = now_ms()?;
                session.commands[index].delivered_at_ms = Some(now);
                session.updated_at_ms = now;
            }
            self.write_session(&session)?;
            Ok(Some((session.id.clone(), session.commands[index].clone())))
        })
    }

    /// Return a pending cancel for the worker's active task and mark it delivered.
    ///
    /// Unlike `pending_control_for_worker`, this is safe to poll while a Codexify
    /// tool call is still running. It lets the daemon cancel that tool's child
    /// cancellation token before the model reaches its next tool-result boundary.
    /// The command remains unacknowledged and is therefore replayed to the model
    /// after the interrupted tool returns.
    pub fn pending_cancel_for_worker(
        &self,
        worker: &str,
    ) -> Result<Option<(String, BackendCommand)>, String> {
        validate_worker(worker)?;
        self.with_lock(|| {
            let mut sessions = self.read_all()?;
            sessions.sort_by_key(|session| session.created_at_ms);
            let Some(mut session) = sessions.into_iter().rev().find(|session| {
                session.worker == worker
                    && session.closed_at_ms.is_none()
                    && session.active_task_seq.is_some()
            }) else {
                return Ok(None);
            };
            let active_seq = session
                .active_task_seq
                .expect("active backend session search guarantees an active task");
            let Some(index) = session.commands.iter().position(|command| {
                command.kind == BackendCommandKind::Cancel
                    && command.acknowledged_at_ms.is_none()
                    && control_targets_task(command, active_seq)
            }) else {
                return Ok(None);
            };
            if session.commands[index].delivered_at_ms.is_none() {
                let now = now_ms()?;
                session.commands[index].delivered_at_ms = Some(now);
                session.updated_at_ms = now;
                self.write_session(&session)?;
            }
            Ok(Some((session.id.clone(), session.commands[index].clone())))
        })
    }

    pub fn enqueue_task_on_available_session(
        &self,
        workspace: Option<&str>,
        content: String,
    ) -> Result<Option<(String, BackendCommand)>, String> {
        self.enqueue_task_on_available_session_inner(workspace, None, content)
    }

    pub fn enqueue_task_on_available_session_rebinding(
        &self,
        workspace: BackendWorkspace,
        content: String,
    ) -> Result<Option<(String, BackendCommand)>, String> {
        let active_root = workspace.active_root.clone();
        self.enqueue_task_on_available_session_inner(Some(&active_root), Some(workspace), content)
    }

    fn enqueue_task_on_available_session_inner(
        &self,
        workspace: Option<&str>,
        rebind_workspace: Option<BackendWorkspace>,
        content: String,
    ) -> Result<Option<(String, BackendCommand)>, String> {
        validate_text("command", &content, MAX_COMMAND_BYTES)?;
        self.with_lock(|| {
            let now = now_ms()?;
            let mut sessions = self.read_all()?;
            let mut exact = None::<(usize, u64, u64)>;
            let mut fallback = None::<(usize, u64, u64)>;
            for (index, session) in sessions.iter_mut().enumerate() {
                if mark_session_stale_if_expired(session, now) {
                    self.write_session(session)?;
                }
                if !session_accepts_task(session, now) {
                    continue;
                }
                let key = (index, session.last_activity_at_ms(), session.created_at_ms);
                let matches_workspace = workspace.is_none_or(|expected| {
                    session
                        .workspace
                        .as_ref()
                        .is_some_and(|actual| actual.active_root == expected)
                });
                if matches_workspace {
                    if exact.as_ref().is_none_or(|(_, activity, created)| {
                        key.1 > *activity || (key.1 == *activity && key.2 > *created)
                    }) {
                        exact = Some(key);
                    }
                } else if rebind_workspace.is_some()
                    && fallback.as_ref().is_none_or(|(_, activity, created)| {
                        key.1 > *activity || (key.1 == *activity && key.2 > *created)
                    })
                {
                    fallback = Some(key);
                }
            }
            let Some((index, _, _)) = exact.or(fallback) else {
                return Ok(None);
            };
            let session = &mut sessions[index];
            if let Some(workspace) = rebind_workspace
                && session
                    .workspace
                    .as_ref()
                    .is_none_or(|current| current.active_root != workspace.active_root)
            {
                session.workspace = Some(workspace);
                session.updated_at_ms = now;
            }
            let command =
                enqueue_command_in_session(session, BackendCommandKind::Task, content, now)?;
            let session_id = session.id.clone();
            self.write_session(session)?;
            Ok(Some((session_id, command)))
        })
    }

    pub fn enqueue_command(
        &self,
        session_id: &str,
        kind: BackendCommandKind,
        content: String,
    ) -> Result<BackendCommand, String> {
        validate_id(session_id)?;
        match kind {
            BackendCommandKind::Task | BackendCommandKind::Steer => {
                validate_text("command", &content, MAX_COMMAND_BYTES)?;
            }
            BackendCommandKind::Cancel | BackendCommandKind::Finish => {
                if content.len() > MAX_COMMAND_BYTES {
                    return Err(format!(
                        "command exceeds the {MAX_COMMAND_BYTES}-byte limit"
                    ));
                }
            }
        }
        self.with_lock(|| {
            let mut session = self.read_session(session_id)?;
            let now = now_ms()?;
            if mark_session_stale_if_expired(&mut session, now) {
                self.write_session(&session)?;
            }
            let command = enqueue_command_in_session(&mut session, kind, content, now)?;
            self.write_session(&session)?;
            Ok(command)
        })
    }

    pub fn prepare_exchange(
        &self,
        worker: &str,
        session_id: &str,
        ack_command_seq: Option<u64>,
        outbound: Option<ExchangeOutbound>,
    ) -> Result<(), String> {
        validate_worker(worker)?;
        validate_id(session_id)?;
        self.with_lock(|| {
            let mut session = self.read_session(session_id)?;
            ensure_worker(&session, worker)?;
            if session.closed_at_ms.is_some() {
                return Err(format!("ChatGPT backend session {session_id} is closed"));
            }
            let now = now_ms()?;
            if mark_session_stale_if_expired(&mut session, now) {
                self.write_session(&session)?;
            }
            if session.stale_at_ms.is_some() {
                return Err(format!(
                    "ChatGPT backend session {session_id} is stale; call chatgpt_backend_attach to start a fresh session"
                ));
            }
            session.last_heartbeat_at_ms = now;
            session.updated_at_ms = now;
            let mut observed_retry = false;

            if let Some(seq) = ack_command_seq {
                let command = session
                    .commands
                    .iter_mut()
                    .find(|command| command.seq == seq)
                    .ok_or_else(|| format!("unknown command sequence {seq}"))?;
                if command.delivered_at_ms.is_none() {
                    return Err(format!("command sequence {seq} has not been delivered"));
                }
                if command.acknowledged_at_ms.is_none() {
                    command.acknowledged_at_ms = Some(now);
                } else {
                    observed_retry = true;
                }
            }

            if let Some(outbound) = outbound {
                if !matches!(outbound.kind, BackendEventKind::Result | BackendEventKind::Error) {
                    return Err("model outbound event must be result or error".into());
                }
                validate_text("event content", &outbound.content, MAX_EVENT_BYTES)?;
                let task_index = session
                    .commands
                    .iter()
                    .position(|command| command.seq == outbound.command_seq)
                    .ok_or_else(|| format!("unknown task sequence {}", outbound.command_seq))?;
                if session.commands[task_index].kind != BackendCommandKind::Task {
                    return Err("outbound result/error must reference a task command".into());
                }
                if session.commands[task_index].delivered_at_ms.is_none() {
                    return Err(format!(
                        "task sequence {} has not been delivered",
                        outbound.command_seq
                    ));
                }

                // A control that was already injected into a model-visible tool result must
                // be explicitly acknowledged. Otherwise accepting a terminal task result
                // would silently discard a steer/cancel that the model had an opportunity
                // to observe. Controls that never reached a tool result lose the race to
                // task completion and are marked acknowledged-but-undelivered below.
                if let Some(control) = session.commands.iter().find(|command| {
                    matches!(command.kind, BackendCommandKind::Steer | BackendCommandKind::Cancel)
                        && command.acknowledged_at_ms.is_none()
                        && command.delivered_at_ms.is_some()
                        && control_targets_task(command, outbound.command_seq)
                }) {
                    return Err(format!(
                        "task {} has unacknowledged delivered control sequence {}",
                        outbound.command_seq, control.seq
                    ));
                }

                let now = now_ms()?;
                for control in session.commands.iter_mut().filter(|command| {
                    matches!(command.kind, BackendCommandKind::Steer | BackendCommandKind::Cancel)
                        && command.acknowledged_at_ms.is_none()
                        && command.delivered_at_ms.is_none()
                        && control_targets_task(command, outbound.command_seq)
                }) {
                    control.acknowledged_at_ms = Some(now);
                }

                // Returning a terminal event is itself a definitive acknowledgement of the
                // task. This leaves ack_command_seq available for an injected steer/cancel
                // command in the same exchange call.
                if session.commands[task_index].acknowledged_at_ms.is_none() {
                    session.commands[task_index].acknowledged_at_ms = Some(now);
                }

                if let Some(existing) = session.events.iter().find(|event| {
                    event.command_seq == Some(outbound.command_seq)
                        && matches!(event.kind, BackendEventKind::Result | BackendEventKind::Error)
                }) {
                    if existing.kind != outbound.kind || existing.content != outbound.content {
                        return Err(format!(
                            "task {} already has a different terminal event",
                            outbound.command_seq
                        ));
                    }
                    // Exact retries are idempotent even after active_task_seq was cleared.
                    observed_retry = true;
                    session.active_task_seq = None;
                } else {
                    if session.active_task_seq != Some(outbound.command_seq) {
                        return Err(format!(
                            "outbound event command_seq {} does not match active task {:?}",
                            outbound.command_seq, session.active_task_seq
                        ));
                    }
                    let now = now_ms()?;
                    session.events.push(BackendEvent {
                        seq: session.next_event_seq,
                        kind: outbound.kind,
                        command_seq: Some(outbound.command_seq),
                        content: outbound.content,
                        created_at_ms: now,
                    });
                    session.completed_tasks = session.completed_tasks.saturating_add(1);
                    if outbound.kind == BackendEventKind::Error {
                        session.failed_tasks = session.failed_tasks.saturating_add(1);
                    }
                    session.next_event_seq = session.next_event_seq.saturating_add(1);
                    session.active_task_seq = None;
                    session.updated_at_ms = now;
                }
            }
            if observed_retry {
                session.idempotent_retries = session.idempotent_retries.saturating_add(1);
            }
            self.write_session(&session)
        })
    }

    pub async fn exchange_wait(
        &self,
        worker: &str,
        session_id: &str,
        wait: bool,
        timeout_ms: u64,
        cancellation: CancellationToken,
    ) -> Result<ExchangeOutcome, String> {
        validate_worker(worker)?;
        validate_id(session_id)?;
        let timeout_ms = timeout_ms.min(300_000);
        self.mark_waiting(worker, session_id, wait)?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if cancellation.is_cancelled() {
                self.mark_wait_returned(worker, session_id)?;
                return Ok(ExchangeOutcome::Cancelled);
            }
            if let Some(outcome) = self.try_take_command(worker, session_id)? {
                self.mark_wait_returned(worker, session_id)?;
                return Ok(outcome);
            }
            if !wait {
                self.mark_wait_returned(worker, session_id)?;
                return Ok(ExchangeOutcome::Idle);
            }
            if tokio::time::Instant::now() >= deadline {
                self.mark_wait_returned(worker, session_id)?;
                return Ok(ExchangeOutcome::TimedOut);
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    self.mark_wait_returned(worker, session_id)?;
                    return Ok(ExchangeOutcome::Cancelled);
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
            }
        }
    }

    fn try_take_command(
        &self,
        worker: &str,
        session_id: &str,
    ) -> Result<Option<ExchangeOutcome>, String> {
        self.with_lock(|| {
            let mut session = self.read_session(session_id)?;
            ensure_worker(&session, worker)?;
            if session.closed_at_ms.is_some() {
                return Ok(Some(ExchangeOutcome::Closed));
            }
            if let Some(command) = session
                .commands
                .iter()
                .find(|command| {
                    command.delivered_at_ms.is_some() && command.acknowledged_at_ms.is_none()
                })
                .cloned()
            {
                return Ok(Some(ExchangeOutcome::Command(command)));
            }
            let Some(index) = session
                .commands
                .iter()
                .position(|command| command.delivered_at_ms.is_none())
            else {
                return Ok(None);
            };
            let now = now_ms()?;
            session.commands[index].delivered_at_ms = Some(now);
            let command = session.commands[index].clone();
            if command.kind == BackendCommandKind::Task {
                session.active_task_seq = Some(command.seq);
            }
            if command.kind == BackendCommandKind::Finish {
                session.closed_at_ms = Some(now);
            }
            session.updated_at_ms = now;
            self.write_session(&session)?;
            Ok(Some(ExchangeOutcome::Command(command)))
        })
    }

    fn mark_waiting(&self, worker: &str, session_id: &str, waiting: bool) -> Result<(), String> {
        self.with_lock(|| {
            let mut session = self.read_session(session_id)?;
            ensure_worker(&session, worker)?;
            let now = now_ms()?;
            session.waiting = waiting;
            if waiting {
                session.last_wait_started_at_ms = Some(now);
            }
            session.last_heartbeat_at_ms = now;
            session.updated_at_ms = now;
            self.write_session(&session)
        })
    }

    fn mark_wait_returned(&self, worker: &str, session_id: &str) -> Result<(), String> {
        self.with_lock(|| {
            let mut session = self.read_session(session_id)?;
            ensure_worker(&session, worker)?;
            let now = now_ms()?;
            session.waiting = false;
            session.last_wait_returned_at_ms = Some(now);
            session.last_heartbeat_at_ms = now;
            session.updated_at_ms = now;
            self.write_session(&session)
        })
    }

    fn with_lock<T>(&self, operation: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
        self.ensure_directory()?;
        let lock_path = self.directory.join("sessions.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| format!("open ChatGPT backend lock: {error}"))?;
        lock.lock()
            .map_err(|error| format!("lock ChatGPT backend sessions: {error}"))?;
        operation()
    }

    fn ensure_directory(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.directory)
            .map_err(|error| format!("create ChatGPT backend state directory: {error}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.directory, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| format!("secure ChatGPT backend state directory: {error}"))?;
        }
        Ok(())
    }

    fn read_all(&self) -> Result<Vec<BackendSession>, String> {
        let mut sessions = Vec::new();
        for entry in std::fs::read_dir(&self.directory)
            .map_err(|error| format!("read ChatGPT backend state directory: {error}"))?
        {
            let entry =
                entry.map_err(|error| format!("read ChatGPT backend state entry: {error}"))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|error| {
                format!("read ChatGPT backend session {}: {error}", path.display())
            })?;
            let session = serde_json::from_slice(&bytes).map_err(|error| {
                format!("parse ChatGPT backend session {}: {error}", path.display())
            })?;
            sessions.push(session);
        }
        Ok(sessions)
    }

    fn read_session(&self, session_id: &str) -> Result<BackendSession, String> {
        let path = self.session_path(session_id);
        let bytes = std::fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!("unknown ChatGPT backend session: {session_id}")
            } else {
                format!("read ChatGPT backend session {session_id}: {error}")
            }
        })?;
        serde_json::from_slice(&bytes)
            .map_err(|error| format!("parse ChatGPT backend session {session_id}: {error}"))
    }

    fn write_session(&self, session: &BackendSession) -> Result<(), String> {
        let mut persisted = session.clone();
        compact_history(&mut persisted);
        let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)
            .map_err(|error| format!("create ChatGPT backend session temp file: {error}"))?;
        serde_json::to_writer(&mut temporary, &persisted)
            .map_err(|error| format!("serialize ChatGPT backend session: {error}"))?;
        temporary
            .write_all(b"\n")
            .map_err(|error| format!("write ChatGPT backend session: {error}"))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| format!("sync ChatGPT backend session: {error}"))?;
        temporary
            .persist(self.session_path(&session.id))
            .map_err(|error| format!("persist ChatGPT backend session: {}", error.error))?;
        Ok(())
    }

    fn session_path(&self, session_id: &str) -> PathBuf {
        self.directory.join(format!("{session_id}.json"))
    }
}

fn enqueue_command_in_session(
    session: &mut BackendSession,
    kind: BackendCommandKind,
    content: String,
    now: u64,
) -> Result<BackendCommand, String> {
    let session_id = &session.id;
    if session.closed_at_ms.is_some() {
        return Err(format!("ChatGPT backend session {session_id} is closed"));
    }
    if session.failed_at_ms.is_some() {
        return Err(format!("ChatGPT backend session {session_id} is failed"));
    }
    if session.stale_at_ms.is_some() {
        return Err(format!(
            "ChatGPT backend session {session_id} is stale; attach a fresh backend session"
        ));
    }
    let pending = session
        .commands
        .iter()
        .filter(|command| command.acknowledged_at_ms.is_none())
        .count();
    if pending >= MAX_PENDING_COMMANDS {
        return Err(format!(
            "ChatGPT backend session {session_id} already has {MAX_PENDING_COMMANDS} pending commands"
        ));
    }
    if kind == BackendCommandKind::Task && session.draining_at_ms.is_some() {
        return Err(format!(
            "ChatGPT backend session {session_id} is draining and does not accept new tasks"
        ));
    }
    if kind == BackendCommandKind::Task
        && (session.active_task_seq.is_some()
            || session.commands.iter().any(|command| {
                command.kind == BackendCommandKind::Task && command.acknowledged_at_ms.is_none()
            }))
    {
        return Err("a ChatGPT backend task is already active or queued".into());
    }
    if matches!(kind, BackendCommandKind::Steer | BackendCommandKind::Cancel)
        && session.active_task_seq.is_none()
        && !session.commands.iter().any(|command| {
            command.kind == BackendCommandKind::Task && command.acknowledged_at_ms.is_none()
        })
    {
        return Err("steer/cancel requires an active or queued task".into());
    }
    let target_task_seq = match kind {
        BackendCommandKind::Steer | BackendCommandKind::Cancel => {
            session.active_task_seq.or_else(|| {
                session
                    .commands
                    .iter()
                    .find(|command| {
                        command.kind == BackendCommandKind::Task
                            && command.acknowledged_at_ms.is_none()
                    })
                    .map(|command| command.seq)
            })
        }
        BackendCommandKind::Task | BackendCommandKind::Finish => None,
    };
    let command = BackendCommand {
        seq: session.next_command_seq,
        id: new_id()?,
        kind,
        target_task_seq,
        content,
        created_at_ms: now,
        delivered_at_ms: None,
        acknowledged_at_ms: None,
    };
    session.next_command_seq = session.next_command_seq.saturating_add(1);
    session.updated_at_ms = now;
    session.commands.push(command.clone());
    Ok(command)
}

fn inspect_session(session: BackendSession, now: u64) -> BackendSessionInspection {
    let stale_reason = session.stale_reason_at(now);
    let state = session.lifecycle_state_at(now);
    let accepting_tasks = session_accepts_task(&session, now);
    let tasks = session
        .commands
        .iter()
        .filter(|command| command.kind == BackendCommandKind::Task)
        .map(|command| {
            let terminal = session.events.iter().find(|event| {
                event.command_seq == Some(command.seq)
                    && matches!(
                        event.kind,
                        BackendEventKind::Result | BackendEventKind::Error
                    )
            });
            BackendTaskInspection {
                command_seq: command.seq,
                queued_at_ms: command.created_at_ms,
                delivered_at_ms: command.delivered_at_ms,
                acknowledged_at_ms: command.acknowledged_at_ms,
                completed_at_ms: terminal.map(|event| event.created_at_ms),
                outcome: terminal.map(|event| event.kind),
                delivery_latency_ms: command
                    .delivered_at_ms
                    .map(|delivered| delivered.saturating_sub(command.created_at_ms)),
                completion_latency_ms: terminal
                    .map(|event| event.created_at_ms.saturating_sub(command.created_at_ms)),
            }
        })
        .collect::<Vec<_>>();
    let pending_commands = session
        .commands
        .iter()
        .filter(|command| command.acknowledged_at_ms.is_none())
        .count();
    let retained_completed = session
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                BackendEventKind::Result | BackendEventKind::Error
            )
        })
        .count() as u64;
    let retained_failed = session
        .events
        .iter()
        .filter(|event| event.kind == BackendEventKind::Error)
        .count() as u64;
    let mut timeline = Vec::new();
    for command in &session.commands {
        timeline.push(BackendTimelineEntry {
            at_ms: command.created_at_ms,
            kind: "command_queued",
            seq: command.seq,
            command_seq: None,
            command_kind: Some(command.kind),
            event_kind: None,
            tool: None,
            tool_status: None,
            duration_ms: None,
            request_preview: None,
            response_preview: None,
        });
        if let Some(at_ms) = command.delivered_at_ms {
            timeline.push(BackendTimelineEntry {
                at_ms,
                kind: "command_delivered",
                seq: command.seq,
                command_seq: None,
                command_kind: Some(command.kind),
                event_kind: None,
                tool: None,
                tool_status: None,
                duration_ms: None,
                request_preview: None,
                response_preview: None,
            });
        }
        if let Some(at_ms) = command.acknowledged_at_ms {
            timeline.push(BackendTimelineEntry {
                at_ms,
                kind: "command_acknowledged",
                seq: command.seq,
                command_seq: None,
                command_kind: Some(command.kind),
                event_kind: None,
                tool: None,
                tool_status: None,
                duration_ms: None,
                request_preview: None,
                response_preview: None,
            });
        }
    }
    for event in &session.events {
        timeline.push(BackendTimelineEntry {
            at_ms: event.created_at_ms,
            kind: "event",
            seq: event.seq,
            command_seq: event.command_seq,
            command_kind: None,
            event_kind: Some(event.kind),
            tool: None,
            tool_status: None,
            duration_ms: None,
            request_preview: None,
            response_preview: None,
        });
    }
    for activity in &session.tool_activities {
        timeline.push(BackendTimelineEntry {
            at_ms: activity.started_at_ms,
            kind: "tool_started",
            seq: activity.seq,
            command_seq: Some(activity.task_seq),
            command_kind: None,
            event_kind: None,
            tool: Some(activity.tool.clone()),
            tool_status: Some(BackendToolActivityStatus::Running),
            duration_ms: None,
            request_preview: activity.request_preview.clone(),
            response_preview: None,
        });
        if let Some(at_ms) = activity.completed_at_ms {
            timeline.push(BackendTimelineEntry {
                at_ms,
                kind: "tool_completed",
                seq: activity.seq,
                command_seq: Some(activity.task_seq),
                command_kind: None,
                event_kind: None,
                tool: Some(activity.tool.clone()),
                tool_status: Some(activity.status),
                duration_ms: Some(at_ms.saturating_sub(activity.started_at_ms)),
                request_preview: activity.request_preview.clone(),
                response_preview: activity.response_preview.clone(),
            });
        }
    }
    timeline.sort_by_key(|entry| (entry.at_ms, entry.kind, entry.seq));
    BackendSessionInspection {
        session_id: session.id.clone(),
        live: !matches!(
            state,
            BackendLifecycleState::Finished
                | BackendLifecycleState::Failed
                | BackendLifecycleState::Stale
        ),
        state,
        accepting_tasks,
        last_activity_at_ms: session.last_activity_at_ms(),
        stale_reason,
        draining: session.draining_at_ms.is_some(),
        drain_reason: session.drain_reason.clone(),
        retained_commands: session.commands.len(),
        retained_events: session.events.len(),
        pending_commands,
        completed_tasks: session.completed_tasks.max(retained_completed),
        failed_tasks: session.failed_tasks.max(retained_failed),
        idempotent_retries: session.idempotent_retries,
        dropped_commands: session.dropped_commands,
        dropped_events: session.dropped_events,
        next_command_seq: session.next_command_seq,
        next_event_seq: session.next_event_seq,
        active_task_seq: session.active_task_seq,
        waiting: session.waiting,
        workspace: session.workspace.clone(),
        last_event: session.events.last().map(|event| BackendEventSummary {
            seq: event.seq,
            kind: event.kind,
            command_seq: event.command_seq,
            created_at_ms: event.created_at_ms,
        }),
        tasks,
        tool_activities: session.tool_activities.clone(),
        timeline,
    }
}

fn session_accepts_task(session: &BackendSession, now: u64) -> bool {
    matches!(
        session.lifecycle_state_at(now),
        BackendLifecycleState::Ready | BackendLifecycleState::Waiting
    ) && !session.commands.iter().any(|command| {
        command.kind == BackendCommandKind::Finish && command.acknowledged_at_ms.is_none()
    })
}

fn mark_session_stale_if_expired(session: &mut BackendSession, now: u64) -> bool {
    if session.closed_at_ms.is_some()
        || session.failed_at_ms.is_some()
        || session.stale_at_ms.is_some()
    {
        return false;
    }
    if let Some(reason) = session.stale_reason_at(now) {
        session.stale_at_ms = Some(now);
        session.stale_reason = Some(reason);
        session.waiting = false;
        session.updated_at_ms = now;
        return true;
    }
    false
}

fn compact_history(session: &mut BackendSession) {
    // Older experimental records predate durable counters. Recover their totals
    // before pruning so the first write under the new format does not lose the
    // historical completion/error counts.
    if session.completed_tasks == 0 {
        session.completed_tasks = session
            .events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    BackendEventKind::Result | BackendEventKind::Error
                )
            })
            .count() as u64;
    }
    if session.failed_tasks == 0 {
        session.failed_tasks = session
            .events
            .iter()
            .filter(|event| event.kind == BackendEventKind::Error)
            .count() as u64;
    }

    let mut acknowledged = session
        .commands
        .iter()
        .filter(|command| command.acknowledged_at_ms.is_some())
        .map(|command| command.seq)
        .collect::<Vec<_>>();
    acknowledged.sort_unstable();
    let drop_count = acknowledged.len().saturating_sub(MAX_RETAINED_COMMANDS);
    if drop_count > 0 {
        let drop_through = acknowledged[drop_count - 1];
        let before = session.commands.len();
        session
            .commands
            .retain(|command| command.acknowledged_at_ms.is_none() || command.seq > drop_through);
        session.dropped_commands = session
            .dropped_commands
            .saturating_add((before - session.commands.len()) as u64);
    }

    if session.events.len() > MAX_RETAINED_EVENTS {
        let remove = session.events.len() - MAX_RETAINED_EVENTS;
        session.events.drain(0..remove);
        session.dropped_events = session.dropped_events.saturating_add(remove as u64);
    }

    let completed_tool_activities = session
        .tool_activities
        .iter()
        .filter(|activity| activity.completed_at_ms.is_some())
        .count();
    let mut remove = completed_tool_activities.saturating_sub(MAX_RETAINED_TOOL_ACTIVITIES);
    if remove > 0 {
        session.tool_activities.retain(|activity| {
            if remove > 0 && activity.completed_at_ms.is_some() {
                remove -= 1;
                false
            } else {
                true
            }
        });
    }
}

fn ensure_worker(session: &BackendSession, worker: &str) -> Result<(), String> {
    if session.worker == worker {
        Ok(())
    } else {
        Err(format!(
            "ChatGPT backend session {} belongs to another conversation",
            session.id
        ))
    }
}

fn scope_key(config: &AppConfig) -> String {
    let mut tunnel_ids = config
        .configured_openai_tunnels()
        .map(|tunnel| tunnel.tunnel_id.as_str())
        .collect::<Vec<_>>();
    tunnel_ids.sort_unstable();
    let mut hash = Sha256::new();
    hash.update(b"chatgpt-backend-v1\0");
    if tunnel_ids.is_empty() {
        hash.update(b"http\0");
        hash.update(config.work_dir.to_string_lossy().as_bytes());
        hash.update(b"\0");
        hash.update(config.port.to_le_bytes());
    } else {
        hash.update(b"tunnels\0");
        for tunnel_id in tunnel_ids {
            hash.update(tunnel_id.as_bytes());
            hash.update(b"\0");
        }
    }
    format!("{:x}", hash.finalize())
}

fn now_ms() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))
}

fn new_id() -> Result<String, String> {
    let mut bytes = [0u8; 12];
    getrandom::getrandom(&mut bytes)
        .map_err(|error| format!("generate ChatGPT backend id: {error}"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn validate_id(value: &str) -> Result<(), String> {
    if (1..=80).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        Ok(())
    } else {
        Err("invalid ChatGPT backend id".into())
    }
}

fn validate_worker(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        Err("invalid ChatGPT backend worker identity".into())
    } else {
        Ok(())
    }
}

pub fn run_sessions_cli(
    config: &AppConfig,
    args: crate::config::ChatGptBackendSessionsArgs,
) -> anyhow::Result<()> {
    let store = ChatGptBackendStore::for_current_user(config).map_err(anyhow::Error::msg)?;
    let sessions = store.list_inspections().map_err(anyhow::Error::msg)?;
    if args.json {
        crate::terminal::write_stdout(&format!("{}\n", serde_json::to_string_pretty(&sessions)?))?;
        return Ok(());
    }
    if sessions.is_empty() {
        crate::terminal::write_stdout("No ChatGPT backend sessions.\n")?;
        return Ok(());
    }
    for inspection in sessions {
        let workspace = inspection
            .workspace
            .as_ref()
            .map(|workspace| workspace.active_root.as_str())
            .unwrap_or("-");
        crate::terminal::write_stdout(&format!(
            "{}\t{}\tlive={}\tworkspace={}\tcommands={}+{}d\tevents={}+{}d\n",
            inspection.session_id,
            inspection.state.as_str(),
            inspection.live,
            workspace,
            inspection.retained_commands,
            inspection.dropped_commands,
            inspection.retained_events,
            inspection.dropped_events
        ))?;
    }
    Ok(())
}

pub fn run_send_cli(
    config: &AppConfig,
    args: crate::config::ChatGptBackendSendArgs,
) -> anyhow::Result<()> {
    let store = ChatGptBackendStore::for_current_user(config).map_err(anyhow::Error::msg)?;
    let kind = BackendCommandKind::parse(&args.kind).map_err(anyhow::Error::msg)?;
    let command = store
        .enqueue_command(&args.session_id, kind, args.content.unwrap_or_default())
        .map_err(anyhow::Error::msg)?;
    if args.json {
        crate::terminal::write_stdout(&format!("{}\n", serde_json::to_string_pretty(&command)?))?;
    } else {
        crate::terminal::write_stdout(&format!(
            "Queued {:?} command seq={} id={} for session {}\n",
            command.kind, command.seq, command.id, args.session_id
        ))?;
    }
    Ok(())
}

pub fn run_status_cli(
    config: &AppConfig,
    args: crate::config::ChatGptBackendStatusArgs,
) -> anyhow::Result<()> {
    let store = ChatGptBackendStore::for_current_user(config).map_err(anyhow::Error::msg)?;
    let inspection = store
        .inspection(&args.session_id)
        .map_err(anyhow::Error::msg)?;
    if args.json {
        crate::terminal::write_stdout(&format!(
            "{}\n",
            serde_json::to_string_pretty(&inspection)?
        ))?;
    } else {
        crate::terminal::write_stdout(&format!(
            "session={} state={} live={} commands={}+{}d events={}+{}d active_task={:?} waiting={} last_activity_at_ms={} completed={} failed={} retries={}\n",
            inspection.session_id,
            inspection.state.as_str(),
            inspection.live,
            inspection.retained_commands,
            inspection.dropped_commands,
            inspection.retained_events,
            inspection.dropped_events,
            inspection.active_task_seq,
            inspection.waiting,
            inspection.last_activity_at_ms,
            inspection.completed_tasks,
            inspection.failed_tasks,
            inspection.idempotent_retries
        ))?;
        if let Some(reason) = &inspection.stale_reason {
            crate::terminal::write_stdout(&format!("stale_reason={reason}\n"))?;
        }
        if let Some(workspace) = &inspection.workspace {
            crate::terminal::write_stdout(&format!(
                "workspace={} managed_worktree={}\n",
                workspace.active_root, workspace.managed_worktree
            ))?;
        }
        if let Some(event) = &inspection.last_event {
            crate::terminal::write_stdout(&format!(
                "last_event seq={} kind={:?} command_seq={:?} created_at_ms={}\n",
                event.seq, event.kind, event.command_seq, event.created_at_ms
            ))?;
        }
        if args.timeline {
            for entry in &inspection.timeline {
                crate::terminal::write_stdout(&format!(
                    "timeline at_ms={} kind={} seq={} command_seq={:?} command_kind={:?} event_kind={:?}\n",
                    entry.at_ms,
                    entry.kind,
                    entry.seq,
                    entry.command_seq,
                    entry.command_kind,
                    entry.event_kind
                ))?;
            }
        }
    }
    Ok(())
}

fn validate_text(name: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if value.len() > max_bytes {
        return Err(format!("{name} exceeds the {max_bytes}-byte limit"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, ChatGptBackendStore) {
        let root = tempfile::tempdir().unwrap();
        let store = ChatGptBackendStore::new_at(root.path().join("backend"));
        (root, store)
    }

    #[test]
    fn attach_is_idempotent_per_live_worker() {
        let (_root, store) = store();
        let first = store.attach("worker-a").unwrap();
        let second = store.attach("worker-a").unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(first.events[0].kind, BackendEventKind::Ready);
    }

    #[test]
    fn attach_persists_workspace_and_rejects_live_workspace_mismatch() {
        let (_root, store) = store();
        let workspace = BackendWorkspace {
            active_root: "/tmp/worktree-a".into(),
            source_project_root: Some("/tmp/project-a".into()),
            managed_worktree: true,
            worktree_git_root: Some("/tmp/worktree-a".into()),
            repository_url: Some("https://example.com/repo.git".into()),
        };
        let first = store
            .attach_with_workspace("worker-a", Some(workspace.clone()))
            .unwrap();
        assert_eq!(first.workspace.as_ref(), Some(&workspace));
        assert!(first.events[0].content.contains("/tmp/worktree-a"));

        let second = store
            .attach_with_workspace("worker-a", Some(workspace.clone()))
            .unwrap();
        assert_eq!(first.id, second.id);

        let different = BackendWorkspace {
            active_root: "/tmp/worktree-b".into(),
            ..workspace
        };
        let error = store
            .attach_with_workspace("worker-a", Some(different))
            .unwrap_err();
        assert!(error.contains("different workspace"));
    }

    #[test]
    fn expired_wait_is_marked_stale_and_fresh_attach_replaces_it() {
        let (_root, store) = store();
        let first = store.attach("worker-a").unwrap();
        let mut persisted = store.session(&first.id).unwrap();
        let now = now_ms().unwrap();
        let expired = now
            .saturating_sub(DEFAULT_WAIT_MS)
            .saturating_sub(WAIT_LEASE_GRACE_MS)
            .saturating_sub(1_000);
        persisted.waiting = true;
        persisted.last_wait_started_at_ms = Some(expired);
        persisted.last_heartbeat_at_ms = expired;
        persisted.updated_at_ms = expired;
        store.write_session(&persisted).unwrap();

        let stale = store.inspection(&first.id).unwrap();
        assert_eq!(stale.state, BackendLifecycleState::Stale);
        assert!(!stale.live);
        assert_eq!(
            stale.stale_reason.as_deref(),
            Some("exchange wait lease expired")
        );

        let replacement = store.attach("worker-a").unwrap();
        assert_ne!(replacement.id, first.id);
        assert_eq!(
            store.inspection(&replacement.id).unwrap().state,
            BackendLifecycleState::Ready
        );
    }

    #[test]
    fn drain_is_live_idempotent_and_preserves_first_reason() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();

        let first = store
            .drain_session(&session.id, "planned rotation")
            .unwrap();
        assert!(first.draining_at_ms.is_some());
        assert_eq!(first.drain_reason.as_deref(), Some("planned rotation"));

        let second = store.drain_session(&session.id, "later reason").unwrap();
        assert_eq!(second.draining_at_ms, first.draining_at_ms);
        assert_eq!(second.drain_reason.as_deref(), Some("planned rotation"));

        let inspection = store.inspection(&session.id).unwrap();
        assert_eq!(inspection.state, BackendLifecycleState::Draining);
        assert!(inspection.live);
        assert!(inspection.draining);
        assert_eq!(inspection.drain_reason.as_deref(), Some("planned rotation"));

        let error = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "new work".into())
            .unwrap_err();
        assert!(error.contains("draining"));
    }

    #[test]
    fn explicit_abandon_marks_session_stale_and_allows_fresh_attach() {
        let (_root, store) = store();
        let first = store.attach("worker-a").unwrap();
        store
            .enqueue_command(&first.id, BackendCommandKind::Task, "work".into())
            .unwrap();

        store
            .abandon_session(&first.id, "interrupt acknowledgement timed out")
            .unwrap();
        let stale = store.inspection(&first.id).unwrap();
        assert_eq!(stale.state, BackendLifecycleState::Stale);
        assert!(!stale.live);
        assert_eq!(
            stale.stale_reason.as_deref(),
            Some("interrupt acknowledgement timed out")
        );

        let replacement = store.attach("worker-a").unwrap();
        assert_ne!(replacement.id, first.id);
        assert_eq!(
            store.inspection(&replacement.id).unwrap().state,
            BackendLifecycleState::Ready
        );
    }

    #[tokio::test]
    async fn active_tool_boundary_renews_working_heartbeat() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        let outcome = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(outcome, ExchangeOutcome::Command(ref command) if command.seq == task.seq)
        );

        let mut persisted = store.session(&session.id).unwrap();
        let old = now_ms().unwrap().saturating_sub(WORKING_STALE_MS + 1_000);
        persisted.last_heartbeat_at_ms = old;
        persisted.updated_at_ms = old;
        store.write_session(&persisted).unwrap();

        assert_eq!(
            store.active_task_for_worker("worker-a").unwrap(),
            Some((session.id.clone(), task.seq, None))
        );
        let inspection = store.inspection(&session.id).unwrap();
        assert_eq!(inspection.state, BackendLifecycleState::Working);
        assert!(inspection.live);
        assert!(inspection.last_activity_at_ms > old);
    }

    #[tokio::test]
    async fn queued_controls_do_not_keep_an_unresponsive_worker_live() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        let outcome = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(outcome, ExchangeOutcome::Command(ref command) if command.seq == task.seq)
        );

        store
            .enqueue_command(&session.id, BackendCommandKind::Cancel, "stop".into())
            .unwrap();
        let mut persisted = store.session(&session.id).unwrap();
        let externally_updated_at = persisted.updated_at_ms;
        let expired = now_ms().unwrap().saturating_sub(WORKING_STALE_MS + 1_000);
        persisted.last_heartbeat_at_ms = expired;
        persisted.updated_at_ms = externally_updated_at;
        store.write_session(&persisted).unwrap();

        let inspection = store.inspection(&session.id).unwrap();
        assert_eq!(inspection.state, BackendLifecycleState::Stale);
        assert!(!inspection.live);
        assert_eq!(inspection.last_activity_at_ms, expired);
        assert_eq!(
            inspection.stale_reason.as_deref(),
            Some("active task heartbeat expired")
        );
    }

    #[tokio::test]
    async fn completed_history_is_compacted_but_sequence_numbers_remain_monotonic() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();

        for index in 0..40u64 {
            let task = store
                .enqueue_command(
                    &session.id,
                    BackendCommandKind::Task,
                    format!("task-{index}"),
                )
                .unwrap();
            let outcome = store
                .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
                .await
                .unwrap();
            assert!(
                matches!(outcome, ExchangeOutcome::Command(ref command) if command.seq == task.seq)
            );
            store
                .prepare_exchange(
                    "worker-a",
                    &session.id,
                    None,
                    Some(ExchangeOutbound {
                        kind: BackendEventKind::Result,
                        command_seq: task.seq,
                        content: format!("result-{index}"),
                    }),
                )
                .unwrap();
        }

        let persisted = store.session(&session.id).unwrap();
        assert_eq!(persisted.next_command_seq, 41);
        assert_eq!(persisted.next_event_seq, 42);
        assert!(persisted.commands.len() <= MAX_RETAINED_COMMANDS);
        assert!(persisted.events.len() <= MAX_RETAINED_EVENTS);
        assert!(persisted.dropped_commands > 0);
        assert!(persisted.dropped_events > 0);
        assert_eq!(persisted.completed_tasks, 40);
        assert_eq!(persisted.failed_tasks, 0);
        assert_eq!(persisted.commands.last().unwrap().seq, 40);
        assert_eq!(persisted.events.last().unwrap().command_seq, Some(40));
    }

    #[tokio::test]
    async fn task_result_round_trip_preserves_sequence_and_single_flight() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let command = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "do work".into())
            .unwrap();
        assert!(
            store
                .enqueue_command(&session.id, BackendCommandKind::Task, "too soon".into())
                .unwrap_err()
                .contains("already active or queued")
        );
        let outcome = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(outcome, ExchangeOutcome::Command(ref value) if value.seq == command.seq));
        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                Some(command.seq),
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Result,
                    command_seq: command.seq,
                    content: "done".into(),
                }),
            )
            .unwrap();
        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                Some(command.seq),
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Result,
                    command_seq: command.seq,
                    content: "done".into(),
                }),
            )
            .unwrap();
        let session = store.session(&session.id).unwrap();
        assert_eq!(session.active_task_seq, None);
        assert_eq!(session.events.last().unwrap().content, "done");
        assert!(session.commands[0].acknowledged_at_ms.is_some());
        assert_eq!(session.completed_tasks, 1);
        assert_eq!(session.failed_tasks, 0);
        assert_eq!(session.idempotent_retries, 1);
    }

    #[tokio::test]
    async fn inspection_exposes_control_plane_timeline_without_payloads_or_worker_identity() {
        let (_root, store) = store();
        let session = store.attach("worker-secret-identity").unwrap();
        let task = store
            .enqueue_command(
                &session.id,
                BackendCommandKind::Task,
                "TOP_SECRET_PROMPT".into(),
            )
            .unwrap();
        store
            .exchange_wait(
                "worker-secret-identity",
                &session.id,
                false,
                0,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        store
            .prepare_exchange(
                "worker-secret-identity",
                &session.id,
                None,
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Result,
                    command_seq: task.seq,
                    content: "TOP_SECRET_RESULT".into(),
                }),
            )
            .unwrap();

        let inspection = store.inspection(&session.id).unwrap();
        assert_eq!(inspection.completed_tasks, 1);
        assert_eq!(inspection.failed_tasks, 0);
        assert!(
            inspection
                .timeline
                .iter()
                .any(|entry| entry.kind == "command_delivered" && entry.seq == task.seq)
        );
        assert!(inspection.timeline.iter().any(|entry| {
            entry.kind == "event"
                && entry.command_seq == Some(task.seq)
                && entry.event_kind == Some(BackendEventKind::Result)
        }));
        let json = serde_json::to_string(&inspection).unwrap();
        assert!(!json.contains("worker-secret-identity"));
        assert!(!json.contains("TOP_SECRET_PROMPT"));
        assert!(!json.contains("TOP_SECRET_RESULT"));
    }

    #[tokio::test]
    async fn tool_activity_is_persisted_and_exposed_as_progress_timeline() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("backend");
        let store = ChatGptBackendStore::new_at(directory.clone());
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();

        let activity_seq = store
            .start_tool_activity_with_preview(
                &session.id,
                task.seq,
                "exec_command",
                Some(r#"{"cmd":"git status --short --branch"}"#.into()),
            )
            .unwrap();
        let running = store.inspection(&session.id).unwrap();
        assert_eq!(running.tool_activities.len(), 1);
        assert_eq!(
            running.tool_activities[0].status,
            BackendToolActivityStatus::Running
        );
        assert!(running.timeline.iter().any(|entry| {
            entry.kind == "tool_started"
                && entry.seq == activity_seq
                && entry.command_seq == Some(task.seq)
                && entry.tool.as_deref() == Some("exec_command")
                && entry.tool_status == Some(BackendToolActivityStatus::Running)
                && entry.request_preview.as_deref()
                    == Some(r#"{"cmd":"git status --short --branch"}"#)
                && entry.response_preview.is_none()
        }));

        store
            .complete_tool_activity_with_preview(
                &session.id,
                activity_seq,
                BackendToolActivityStatus::Succeeded,
                Some(r#"{"structuredContent":{"output":"clean","exit_code":0}}"#.into()),
            )
            .unwrap();
        drop(store);

        let reopened = ChatGptBackendStore::new_at(directory);
        let completed = reopened.inspection(&session.id).unwrap();
        assert_eq!(
            completed.tool_activities[0].status,
            BackendToolActivityStatus::Succeeded
        );
        assert!(completed.tool_activities[0].completed_at_ms.is_some());
        assert!(completed.timeline.iter().any(|entry| {
            entry.kind == "tool_completed"
                && entry.seq == activity_seq
                && entry.command_seq == Some(task.seq)
                && entry.tool.as_deref() == Some("exec_command")
                && entry.tool_status == Some(BackendToolActivityStatus::Succeeded)
                && entry.duration_ms.is_some()
                && entry.request_preview.as_deref()
                    == Some(r#"{"cmd":"git status --short --branch"}"#)
                && entry.response_preview.as_deref()
                    == Some(r#"{"structuredContent":{"output":"clean","exit_code":0}}"#)
        }));
    }

    #[tokio::test]
    async fn completed_tool_activity_history_is_bounded_and_sequence_stays_monotonic() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();

        let total = MAX_RETAINED_TOOL_ACTIVITIES + 8;
        let mut last_seq = 0;
        for _ in 0..total {
            last_seq = store
                .start_tool_activity(&session.id, task.seq, "read_file")
                .unwrap();
            store
                .complete_tool_activity(&session.id, last_seq, BackendToolActivityStatus::Succeeded)
                .unwrap();
        }

        let current = store.session(&session.id).unwrap();
        assert_eq!(current.tool_activities.len(), MAX_RETAINED_TOOL_ACTIVITIES);
        assert_eq!(current.tool_activities.last().unwrap().seq, last_seq);
        assert_eq!(current.next_tool_activity_seq, last_seq + 1);
        assert!(current.tool_activities.first().unwrap().seq > 1);
    }

    #[tokio::test]
    async fn session_inspection_survives_store_reconstruction() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("backend");
        let store = ChatGptBackendStore::new_at(directory.clone());
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        let outcome = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(outcome, ExchangeOutcome::Command(ref command) if command.seq == task.seq)
        );
        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                None,
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Result,
                    command_seq: task.seq,
                    content: "done".into(),
                }),
            )
            .unwrap();
        store.mark_waiting("worker-a", &session.id, true).unwrap();
        drop(store);

        let reopened = ChatGptBackendStore::new_at(directory);
        let inspection = reopened.inspection(&session.id).unwrap();
        assert_eq!(inspection.state, BackendLifecycleState::Waiting);
        assert!(inspection.live);
        assert_eq!(inspection.completed_tasks, 1);
        assert_eq!(inspection.failed_tasks, 0);
        assert_eq!(inspection.pending_commands, 0);
        assert!(inspection.waiting);
        assert!(inspection.timeline.iter().any(|entry| {
            entry.kind == "event"
                && entry.command_seq == Some(task.seq)
                && entry.event_kind == Some(BackendEventKind::Result)
        }));
    }

    #[test]
    fn pending_command_queue_is_bounded() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();

        for index in 1..MAX_PENDING_COMMANDS {
            store
                .enqueue_command(
                    &session.id,
                    BackendCommandKind::Steer,
                    format!("steer-{index}"),
                )
                .unwrap();
        }

        let error = store
            .enqueue_command(
                &session.id,
                BackendCommandKind::Steer,
                "one-too-many".into(),
            )
            .unwrap_err();
        assert!(error.contains("already has 64 pending commands"));
    }

    #[test]
    fn steer_and_cancel_require_an_active_or_queued_task() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        for kind in [BackendCommandKind::Steer, BackendCommandKind::Cancel] {
            let error = store
                .enqueue_command(&session.id, kind, "control".into())
                .unwrap_err();
            assert!(error.contains("requires an active or queued task"));
        }
    }

    #[tokio::test]
    async fn injected_control_replays_until_ack_and_can_share_exchange_with_task_result() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        let delivered = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(delivered, ExchangeOutcome::Command(ref command) if command.seq == task.seq)
        );

        let steer = store
            .enqueue_command(
                &session.id,
                BackendCommandKind::Steer,
                "change direction".into(),
            )
            .unwrap();
        for _ in 0..2 {
            let (session_id, control) = store
                .pending_control_for_worker("worker-a")
                .unwrap()
                .expect("steer should be injected while task is active");
            assert_eq!(session_id, session.id);
            assert_eq!(control.seq, steer.seq);
            assert_eq!(control.kind, BackendCommandKind::Steer);
            assert!(control.delivered_at_ms.is_some());
            assert!(control.acknowledged_at_ms.is_none());
        }

        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                Some(steer.seq),
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Result,
                    command_seq: task.seq,
                    content: "done after steer".into(),
                }),
            )
            .unwrap();

        assert!(
            store
                .pending_control_for_worker("worker-a")
                .unwrap()
                .is_none()
        );
        let current = store.session(&session.id).unwrap();
        assert!(current.active_task_seq.is_none());
        assert!(
            current
                .commands
                .iter()
                .find(|command| command.seq == task.seq)
                .unwrap()
                .acknowledged_at_ms
                .is_some()
        );
        assert!(
            current
                .commands
                .iter()
                .find(|command| command.seq == steer.seq)
                .unwrap()
                .acknowledged_at_ms
                .is_some()
        );
        assert!(current.events.iter().any(|event| {
            event.command_seq == Some(task.seq)
                && event.kind == BackendEventKind::Result
                && event.content == "done after steer"
        }));
    }

    #[tokio::test]
    async fn cancelled_wait_can_resume_and_deliver_the_next_command() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let outcome = store
            .exchange_wait("worker-a", &session.id, true, 5_000, cancellation)
            .await
            .unwrap();
        assert!(matches!(outcome, ExchangeOutcome::Cancelled));
        assert!(!store.session(&session.id).unwrap().waiting);

        let command = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "resume".into())
            .unwrap();
        let outcome = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(outcome, ExchangeOutcome::Command(ref value) if value.seq == command.seq));
    }

    #[tokio::test]
    async fn finish_closes_the_session_and_next_attach_creates_a_new_one() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let finish = store
            .enqueue_command(&session.id, BackendCommandKind::Finish, String::new())
            .unwrap();
        let outcome = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(outcome, ExchangeOutcome::Command(ref value) if value.seq == finish.seq && value.kind == BackendCommandKind::Finish)
        );
        assert!(store.session(&session.id).unwrap().closed_at_ms.is_some());
        assert!(
            store
                .enqueue_command(&session.id, BackendCommandKind::Task, "too late".into())
                .unwrap_err()
                .contains("is closed")
        );
        let closed = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(closed, ExchangeOutcome::Closed));

        let replacement = store.attach("worker-a").unwrap();
        assert_ne!(replacement.id, session.id);
        assert!(replacement.closed_at_ms.is_none());
    }

    #[tokio::test]
    async fn delivered_command_is_replayed_until_acknowledged() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let command = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        for _ in 0..2 {
            let outcome = store
                .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
                .await
                .unwrap();
            assert!(
                matches!(outcome, ExchangeOutcome::Command(ref value) if value.seq == command.seq)
            );
        }
    }

    #[tokio::test]
    async fn cancel_preempts_undelivered_steer_and_supersedes_it_on_task_cancellation() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();

        let steer = store
            .enqueue_command(
                &session.id,
                BackendCommandKind::Steer,
                "later direction".into(),
            )
            .unwrap();
        let cancel = store
            .enqueue_command(&session.id, BackendCommandKind::Cancel, "stop now".into())
            .unwrap();
        assert_eq!(steer.target_task_seq, Some(task.seq));
        assert_eq!(cancel.target_task_seq, Some(task.seq));

        let (_, control) = store
            .pending_control_for_worker("worker-a")
            .unwrap()
            .expect("cancel should preempt undelivered steer");
        assert_eq!(control.seq, cancel.seq);
        assert_eq!(control.kind, BackendCommandKind::Cancel);

        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                Some(cancel.seq),
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Error,
                    command_seq: task.seq,
                    content: "cancelled".into(),
                }),
            )
            .unwrap();

        let current = store.session(&session.id).unwrap();
        let stored_steer = current
            .commands
            .iter()
            .find(|command| command.seq == steer.seq)
            .unwrap();
        assert!(stored_steer.delivered_at_ms.is_none());
        assert!(stored_steer.acknowledged_at_ms.is_some());
        assert!(current.active_task_seq.is_none());
        assert_eq!(current.events.last().unwrap().kind, BackendEventKind::Error);
    }

    #[tokio::test]
    async fn terminal_result_rejects_a_delivered_unacknowledged_control() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        let steer = store
            .enqueue_command(
                &session.id,
                BackendCommandKind::Steer,
                "change direction".into(),
            )
            .unwrap();
        store
            .pending_control_for_worker("worker-a")
            .unwrap()
            .expect("steer should be delivered");

        let error = store
            .prepare_exchange(
                "worker-a",
                &session.id,
                None,
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Result,
                    command_seq: task.seq,
                    content: "ignored steer".into(),
                }),
            )
            .unwrap_err();
        assert!(error.contains(&format!(
            "unacknowledged delivered control sequence {}",
            steer.seq
        )));
        assert!(
            store
                .session(&session.id)
                .unwrap()
                .active_task_seq
                .is_some()
        );
    }

    #[tokio::test]
    async fn acknowledging_injected_steer_does_not_replay_the_active_task() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();

        let steer = store
            .enqueue_command(
                &session.id,
                BackendCommandKind::Steer,
                "change course".into(),
            )
            .unwrap();
        let (_, control) = store
            .pending_control_for_worker("worker-a")
            .unwrap()
            .expect("steer should be injected");
        assert_eq!(control.seq, steer.seq);

        let current = store.session(&session.id).unwrap();
        assert!(
            current
                .commands
                .iter()
                .find(|command| command.seq == task.seq)
                .unwrap()
                .acknowledged_at_ms
                .is_some(),
            "ordinary tool boundary should acknowledge the active task"
        );

        store
            .prepare_exchange("worker-a", &session.id, Some(steer.seq), None)
            .unwrap();
        let outcome = store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(outcome, ExchangeOutcome::Idle));
        assert_eq!(
            store.session(&session.id).unwrap().active_task_seq,
            Some(task.seq)
        );
    }

    #[tokio::test]
    async fn terminal_task_event_acks_task_while_control_uses_the_explicit_ack_slot() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();
        let cancel = store
            .enqueue_command(&session.id, BackendCommandKind::Cancel, String::new())
            .unwrap();
        let (_, delivered_cancel) = store
            .pending_control_for_worker("worker-a")
            .unwrap()
            .expect("cancel should be injected");
        assert_eq!(delivered_cancel.seq, cancel.seq);

        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                Some(cancel.seq),
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Error,
                    command_seq: task.seq,
                    content: "cancelled".into(),
                }),
            )
            .unwrap();

        let session = store.session(&session.id).unwrap();
        assert!(
            session
                .commands
                .iter()
                .find(|command| command.seq == task.seq)
                .unwrap()
                .acknowledged_at_ms
                .is_some()
        );
        assert!(
            session
                .commands
                .iter()
                .find(|command| command.seq == cancel.seq)
                .unwrap()
                .acknowledged_at_ms
                .is_some()
        );
        assert_eq!(session.active_task_seq, None);
        assert_eq!(session.events.last().unwrap().kind, BackendEventKind::Error);
        assert_eq!(session.events.last().unwrap().content, "cancelled");
    }

    #[tokio::test]
    async fn pending_cancel_marks_delivery_and_replays_until_model_acknowledges_it() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let task = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "long work".into())
            .unwrap();
        store
            .exchange_wait("worker-a", &session.id, false, 0, CancellationToken::new())
            .await
            .unwrap();

        let cancel = store
            .enqueue_command(&session.id, BackendCommandKind::Cancel, "stop".into())
            .unwrap();
        assert_eq!(cancel.target_task_seq, Some(task.seq));

        for _ in 0..2 {
            let (control_session, delivered) = store
                .pending_cancel_for_worker("worker-a")
                .unwrap()
                .expect("cancel should interrupt the active tool call");
            assert_eq!(control_session, session.id);
            assert_eq!(delivered.seq, cancel.seq);
            assert_eq!(delivered.kind, BackendCommandKind::Cancel);
            assert!(delivered.delivered_at_ms.is_some());
            assert!(delivered.acknowledged_at_ms.is_none());
        }

        // The ordinary tool result boundary must replay the exact same cancel to the
        // model; daemon-side interruption alone never consumes controller intent.
        let (_, replayed) = store
            .pending_control_for_worker("worker-a")
            .unwrap()
            .expect("cancel remains pending for model acknowledgement");
        assert_eq!(replayed.seq, cancel.seq);

        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                Some(cancel.seq),
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Error,
                    command_seq: task.seq,
                    content: "cancelled".into(),
                }),
            )
            .unwrap();
        assert!(
            store
                .pending_cancel_for_worker("worker-a")
                .unwrap()
                .is_none()
        );
    }
}
