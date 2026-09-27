use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::chatgpt_backend::{
    BackendCommand, BackendCommandKind, BackendEventKind, BackendLifecycleState, BackendSession,
    BackendSessionInspection, BackendTaskInspection, BackendTimelineEntry, BackendWorkspace,
    ChatGptBackendStore,
};
use crate::types::AppConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendAdapterErrorCode {
    InvalidArgument,
    NotFound,
    Unavailable,
    Busy,
    Conflict,
    TimedOut,
    Internal,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendAdapterError {
    pub code: BackendAdapterErrorCode,
    pub message: String,
}

impl BackendAdapterError {
    fn new(code: BackendAdapterErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn from_store(message: String) -> Self {
        let code = if message.contains("unknown ChatGPT backend session") {
            BackendAdapterErrorCode::NotFound
        } else if message.contains("already active or queued")
            || message.contains("requires an active or queued task")
        {
            BackendAdapterErrorCode::Busy
        } else if message.contains("stale")
            || message.contains("closed")
            || message.contains("failed")
        {
            BackendAdapterErrorCode::Unavailable
        } else if message.contains("different workspace") {
            BackendAdapterErrorCode::Conflict
        } else if message.contains("invalid") || message.contains("must not be empty") {
            BackendAdapterErrorCode::InvalidArgument
        } else {
            BackendAdapterErrorCode::Internal
        };
        Self::new(code, message)
    }
}

impl std::fmt::Display for BackendAdapterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for BackendAdapterError {}

pub type BackendAdapterResult<T> = Result<T, BackendAdapterError>;

/// Stable process-neutral control-plane request used by Paseo and other local
/// controllers. This deliberately exposes backend semantics rather than the
/// underlying ChatGPT/MCP bridge protocol.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackendControllerRequest {
    Sessions,
    Pool {
        #[serde(default)]
        workspace: Option<String>,
    },
    Acquire {
        #[serde(default)]
        workspace: Option<String>,
    },
    Dispatch {
        #[serde(default)]
        workspace: Option<String>,
        prompt: String,
    },
    Status {
        session_id: String,
    },
    Submit {
        session_id: String,
        prompt: String,
    },
    Run {
        session_id: String,
        run_id: String,
    },
    Wait {
        session_id: String,
        run_id: String,
        #[serde(default = "default_controller_wait_ms")]
        timeout_ms: u64,
    },
    Steer {
        session_id: String,
        run_id: String,
        instruction: String,
    },
    Cancel {
        session_id: String,
        run_id: String,
        #[serde(default)]
        reason: Option<String>,
    },
    Drain {
        session_id: String,
        #[serde(default)]
        reason: Option<String>,
    },
    Abandon {
        session_id: String,
        #[serde(default)]
        reason: Option<String>,
    },
    Finish {
        session_id: String,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum BackendControllerResponse {
    Sessions(Vec<ChatGptBackendSessionView>),
    Pool(ChatGptBackendPoolView),
    Session(ChatGptBackendSessionView),
    Status(ChatGptBackendStatusView),
    Run(ChatGptBackendRunView),
    Receipt(BackendControlReceipt),
}

const MAX_CONTROLLER_WAIT_MS: u64 = 300_000;

fn default_controller_wait_ms() -> u64 {
    MAX_CONTROLLER_WAIT_MS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendRunState {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Stale,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatGptBackendSessionView {
    pub session_id: String,
    pub state: BackendLifecycleState,
    pub live: bool,
    pub accepting_tasks: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drain_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<BackendWorkspace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_run_id: Option<String>,
    pub pending_commands: usize,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub last_activity_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatGptBackendPoolView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    pub total_sessions: usize,
    pub available_capacity: usize,
    pub busy_sessions: usize,
    pub draining_sessions: usize,
    pub unavailable_sessions: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatGptBackendStatusView {
    pub session: ChatGptBackendSessionView,
    pub tasks: Vec<BackendTaskInspection>,
    pub timeline: Vec<BackendTimelineEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatGptBackendRunView {
    pub session_id: String,
    pub run_id: String,
    pub state: BackendRunState,
    pub queued_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendControlReceipt {
    pub accepted: bool,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub action: &'static str,
}

#[derive(Debug, Clone)]
pub struct ChatGptBackendAdapter {
    store: ChatGptBackendStore,
}

impl ChatGptBackendAdapter {
    pub fn for_current_user(config: &AppConfig) -> BackendAdapterResult<Self> {
        Ok(Self {
            store: ChatGptBackendStore::for_current_user(config)
                .map_err(BackendAdapterError::from_store)?,
        })
    }

    pub fn new_at(directory: std::path::PathBuf) -> Self {
        Self {
            store: ChatGptBackendStore::new_at(directory),
        }
    }

    pub async fn handle_controller_request(
        &self,
        request: BackendControllerRequest,
    ) -> BackendAdapterResult<BackendControllerResponse> {
        match request {
            BackendControllerRequest::Sessions => {
                Ok(BackendControllerResponse::Sessions(self.sessions()?))
            }
            BackendControllerRequest::Pool { workspace } => Ok(
                BackendControllerResponse::Pool(self.pool(workspace.as_deref())?),
            ),
            BackendControllerRequest::Acquire { workspace } => Ok(
                BackendControllerResponse::Session(self.acquire(workspace.as_deref())?),
            ),
            BackendControllerRequest::Dispatch { workspace, prompt } => Ok(
                BackendControllerResponse::Run(self.dispatch_task(workspace.as_deref(), prompt)?),
            ),
            BackendControllerRequest::Status { session_id } => {
                Ok(BackendControllerResponse::Status(self.status(&session_id)?))
            }
            BackendControllerRequest::Submit { session_id, prompt } => Ok(
                BackendControllerResponse::Run(self.submit_task(&session_id, prompt)?),
            ),
            BackendControllerRequest::Run { session_id, run_id } => Ok(
                BackendControllerResponse::Run(self.run(&session_id, &run_id)?),
            ),
            BackendControllerRequest::Wait {
                session_id,
                run_id,
                timeout_ms,
            } => {
                if timeout_ms > MAX_CONTROLLER_WAIT_MS {
                    return Err(BackendAdapterError::new(
                        BackendAdapterErrorCode::InvalidArgument,
                        format!("timeout_ms must be at most {MAX_CONTROLLER_WAIT_MS}"),
                    ));
                }
                Ok(BackendControllerResponse::Run(
                    self.wait_run(&session_id, &run_id, Duration::from_millis(timeout_ms))
                        .await?,
                ))
            }
            BackendControllerRequest::Steer {
                session_id,
                run_id,
                instruction,
            } => Ok(BackendControllerResponse::Receipt(self.steer(
                &session_id,
                &run_id,
                instruction,
            )?)),
            BackendControllerRequest::Cancel {
                session_id,
                run_id,
                reason,
            } => Ok(BackendControllerResponse::Receipt(self.cancel(
                &session_id,
                &run_id,
                reason,
            )?)),
            BackendControllerRequest::Drain { session_id, reason } => Ok(
                BackendControllerResponse::Receipt(self.drain(&session_id, reason)?),
            ),
            BackendControllerRequest::Abandon { session_id, reason } => Ok(
                BackendControllerResponse::Receipt(self.abandon(&session_id, reason)?),
            ),
            BackendControllerRequest::Finish { session_id } => Ok(
                BackendControllerResponse::Receipt(self.finish(&session_id)?),
            ),
        }
    }

    pub fn sessions(&self) -> BackendAdapterResult<Vec<ChatGptBackendSessionView>> {
        self.store
            .list_inspections()
            .map_err(BackendAdapterError::from_store)?
            .into_iter()
            .map(|inspection| self.session_view(inspection))
            .collect()
    }

    pub fn session(&self, session_id: &str) -> BackendAdapterResult<ChatGptBackendSessionView> {
        let inspection = self
            .store
            .inspection(session_id)
            .map_err(BackendAdapterError::from_store)?;
        self.session_view(inspection)
    }

    pub fn pool(&self, workspace: Option<&str>) -> BackendAdapterResult<ChatGptBackendPoolView> {
        let inspections = self
            .store
            .list_inspections()
            .map_err(BackendAdapterError::from_store)?;
        let mut view = ChatGptBackendPoolView {
            workspace: workspace.map(str::to_string),
            total_sessions: 0,
            available_capacity: 0,
            busy_sessions: 0,
            draining_sessions: 0,
            unavailable_sessions: 0,
        };
        for inspection in inspections.into_iter().filter(|inspection| {
            workspace.is_none_or(|expected| {
                inspection
                    .workspace
                    .as_ref()
                    .is_some_and(|actual| actual.active_root == expected)
            })
        }) {
            view.total_sessions += 1;
            if inspection.accepting_tasks {
                view.available_capacity += 1;
            } else if inspection.state == BackendLifecycleState::Draining {
                view.draining_sessions += 1;
            } else if inspection.live {
                view.busy_sessions += 1;
            } else {
                view.unavailable_sessions += 1;
            }
        }
        Ok(view)
    }

    pub fn status(&self, session_id: &str) -> BackendAdapterResult<ChatGptBackendStatusView> {
        let inspection = self
            .store
            .inspection(session_id)
            .map_err(BackendAdapterError::from_store)?;
        let tasks = inspection.tasks.clone();
        let timeline = inspection.timeline.clone();
        Ok(ChatGptBackendStatusView {
            session: self.session_view(inspection)?,
            tasks,
            timeline,
        })
    }

    /// Select the freshest available ChatGPT worker, optionally pinned to one
    /// exact active workspace. The adapter never creates a ChatGPT turn itself;
    /// an operator/bootstrap flow must have attached a worker first.
    pub fn acquire(
        &self,
        workspace: Option<&str>,
    ) -> BackendAdapterResult<ChatGptBackendSessionView> {
        let mut candidates = self
            .store
            .list_inspections()
            .map_err(BackendAdapterError::from_store)?
            .into_iter()
            .filter(|inspection| {
                inspection.accepting_tasks
                    && workspace.is_none_or(|expected| {
                        inspection
                            .workspace
                            .as_ref()
                            .is_some_and(|actual| actual.active_root == expected)
                    })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|inspection| inspection.last_activity_at_ms);
        let inspection = candidates.pop().ok_or_else(|| {
            BackendAdapterError::new(
                BackendAdapterErrorCode::Unavailable,
                match workspace {
                    Some(workspace) => format!(
                        "no ready ChatGPT backend session is available for workspace {workspace}"
                    ),
                    None => "no ready ChatGPT backend session is available".to_string(),
                },
            )
        })?;
        self.session_view(inspection)
    }

    pub fn dispatch_task(
        &self,
        workspace: Option<&str>,
        prompt: String,
    ) -> BackendAdapterResult<ChatGptBackendRunView> {
        let Some((session_id, command)) = self
            .store
            .enqueue_task_on_available_session(workspace, prompt)
            .map_err(BackendAdapterError::from_store)?
        else {
            let capacity = self.pool(workspace)?;
            return Err(BackendAdapterError::new(
                BackendAdapterErrorCode::Unavailable,
                match workspace {
                    Some(workspace) => format!(
                        "no ChatGPT backend capacity is available for workspace {workspace} (busy={}, draining={}, unavailable={})",
                        capacity.busy_sessions,
                        capacity.draining_sessions,
                        capacity.unavailable_sessions,
                    ),
                    None => format!(
                        "no ChatGPT backend capacity is available (busy={}, draining={}, unavailable={})",
                        capacity.busy_sessions,
                        capacity.draining_sessions,
                        capacity.unavailable_sessions,
                    ),
                },
            ));
        };
        self.run(&session_id, &command.id)
    }

    pub fn submit_task(
        &self,
        session_id: &str,
        prompt: String,
    ) -> BackendAdapterResult<ChatGptBackendRunView> {
        let session = self.session(session_id)?;
        if !session.live {
            return Err(BackendAdapterError::new(
                BackendAdapterErrorCode::Unavailable,
                format!(
                    "ChatGPT backend session {session_id} is not live ({:?})",
                    session.state
                ),
            ));
        }
        if !session.accepting_tasks {
            return Err(BackendAdapterError::new(
                BackendAdapterErrorCode::Busy,
                format!(
                    "ChatGPT backend session {session_id} is not ready for a new task ({:?})",
                    session.state
                ),
            ));
        }
        let command = self
            .store
            .enqueue_command(session_id, BackendCommandKind::Task, prompt)
            .map_err(BackendAdapterError::from_store)?;
        self.run(session_id, &command.id)
    }

    pub fn run(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> BackendAdapterResult<ChatGptBackendRunView> {
        let session = self
            .store
            .session(session_id)
            .map_err(BackendAdapterError::from_store)?;
        let inspection = self
            .store
            .inspection(session_id)
            .map_err(BackendAdapterError::from_store)?;
        let task = find_task(&session, run_id)?;
        Ok(run_view(&session, &inspection, task))
    }

    pub async fn wait_run(
        &self,
        session_id: &str,
        run_id: &str,
        timeout: Duration,
    ) -> BackendAdapterResult<ChatGptBackendRunView> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let run = self.run(session_id, run_id)?;
            if matches!(
                run.state,
                BackendRunState::Succeeded
                    | BackendRunState::Failed
                    | BackendRunState::Cancelled
                    | BackendRunState::Stale
            ) {
                return Ok(run);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BackendAdapterError::new(
                    BackendAdapterErrorCode::TimedOut,
                    format!("timed out waiting for backend run {run_id}"),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn steer(
        &self,
        session_id: &str,
        run_id: &str,
        instruction: String,
    ) -> BackendAdapterResult<BackendControlReceipt> {
        self.require_active_run(session_id, run_id)?;
        self.store
            .enqueue_command(session_id, BackendCommandKind::Steer, instruction)
            .map_err(BackendAdapterError::from_store)?;
        Ok(BackendControlReceipt {
            accepted: true,
            session_id: session_id.to_string(),
            run_id: Some(run_id.to_string()),
            action: "steer",
        })
    }

    pub fn cancel(
        &self,
        session_id: &str,
        run_id: &str,
        reason: Option<String>,
    ) -> BackendAdapterResult<BackendControlReceipt> {
        self.require_active_run(session_id, run_id)?;
        self.store
            .enqueue_command(
                session_id,
                BackendCommandKind::Cancel,
                reason.unwrap_or_default(),
            )
            .map_err(BackendAdapterError::from_store)?;
        Ok(BackendControlReceipt {
            accepted: true,
            session_id: session_id.to_string(),
            run_id: Some(run_id.to_string()),
            action: "cancel",
        })
    }

    pub fn drain(
        &self,
        session_id: &str,
        reason: Option<String>,
    ) -> BackendAdapterResult<BackendControlReceipt> {
        self.store
            .drain_session(
                session_id,
                reason.unwrap_or_else(|| "draining by controller".to_string()),
            )
            .map_err(BackendAdapterError::from_store)?;
        Ok(BackendControlReceipt {
            accepted: true,
            session_id: session_id.to_string(),
            run_id: None,
            action: "drain",
        })
    }

    pub fn abandon(
        &self,
        session_id: &str,
        reason: Option<String>,
    ) -> BackendAdapterResult<BackendControlReceipt> {
        self.store
            .abandon_session(
                session_id,
                reason.unwrap_or_else(|| "abandoned by controller".to_string()),
            )
            .map_err(BackendAdapterError::from_store)?;
        Ok(BackendControlReceipt {
            accepted: true,
            session_id: session_id.to_string(),
            run_id: None,
            action: "abandon",
        })
    }

    pub fn finish(&self, session_id: &str) -> BackendAdapterResult<BackendControlReceipt> {
        let session = self
            .store
            .session(session_id)
            .map_err(BackendAdapterError::from_store)?;
        if session.active_task_seq.is_some()
            || session.commands.iter().any(|command| {
                command.kind == BackendCommandKind::Task && command.acknowledged_at_ms.is_none()
            })
        {
            return Err(BackendAdapterError::new(
                BackendAdapterErrorCode::Busy,
                "cannot finish a ChatGPT backend session while a task is active or queued",
            ));
        }
        self.store
            .enqueue_command(session_id, BackendCommandKind::Finish, String::new())
            .map_err(BackendAdapterError::from_store)?;
        Ok(BackendControlReceipt {
            accepted: true,
            session_id: session_id.to_string(),
            run_id: None,
            action: "finish",
        })
    }

    fn require_active_run(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> BackendAdapterResult<()> {
        let session = self
            .store
            .session(session_id)
            .map_err(BackendAdapterError::from_store)?;
        let task = find_task(&session, run_id)?;
        if session.active_task_seq == Some(task.seq)
            || (task.delivered_at_ms.is_none() && task.acknowledged_at_ms.is_none())
        {
            Ok(())
        } else {
            Err(BackendAdapterError::new(
                BackendAdapterErrorCode::Conflict,
                format!("backend run {run_id} is no longer active"),
            ))
        }
    }

    fn session_view(
        &self,
        inspection: BackendSessionInspection,
    ) -> BackendAdapterResult<ChatGptBackendSessionView> {
        let raw = self
            .store
            .session(&inspection.session_id)
            .map_err(BackendAdapterError::from_store)?;
        let active_run_id = raw.active_task_seq.and_then(|seq| {
            raw.commands
                .iter()
                .find(|command| command.seq == seq && command.kind == BackendCommandKind::Task)
                .map(|command| command.id.clone())
        });
        Ok(ChatGptBackendSessionView {
            session_id: inspection.session_id,
            state: inspection.state,
            live: inspection.live,
            accepting_tasks: inspection.accepting_tasks,
            drain_reason: inspection.drain_reason,
            workspace: inspection.workspace,
            active_run_id,
            pending_commands: inspection.pending_commands,
            completed_tasks: inspection.completed_tasks,
            failed_tasks: inspection.failed_tasks,
            last_activity_at_ms: inspection.last_activity_at_ms,
        })
    }
}

fn find_task<'a>(session: &'a BackendSession, run_id: &str) -> BackendAdapterResult<&'a BackendCommand> {
    session
        .commands
        .iter()
        .find(|command| command.kind == BackendCommandKind::Task && command.id == run_id)
        .ok_or_else(|| {
            BackendAdapterError::new(
                BackendAdapterErrorCode::NotFound,
                format!("unknown backend run {run_id} in session {}", session.id),
            )
        })
}

fn run_view(
    session: &BackendSession,
    inspection: &BackendSessionInspection,
    task: &BackendCommand,
) -> ChatGptBackendRunView {
    let terminal = session.events.iter().find(|event| {
        event.command_seq == Some(task.seq)
            && matches!(event.kind, BackendEventKind::Result | BackendEventKind::Error)
    });
    let cancelled = session.commands.iter().any(|command| {
        command.kind == BackendCommandKind::Cancel
            && command.target_task_seq == Some(task.seq)
            && command.acknowledged_at_ms.is_some()
    });
    let cancelling = session.commands.iter().any(|command| {
        command.kind == BackendCommandKind::Cancel
            && command.target_task_seq == Some(task.seq)
            && command.acknowledged_at_ms.is_none()
    });

    let state = match terminal.map(|event| event.kind) {
        Some(BackendEventKind::Result) => BackendRunState::Succeeded,
        Some(BackendEventKind::Error) if cancelled => BackendRunState::Cancelled,
        Some(BackendEventKind::Error) => BackendRunState::Failed,
        Some(BackendEventKind::Ready) => unreachable!("ready is not a terminal task event"),
        None if inspection.state == BackendLifecycleState::Stale => BackendRunState::Stale,
        None if cancelling => BackendRunState::Cancelling,
        None if task.delivered_at_ms.is_some() => BackendRunState::Running,
        None => BackendRunState::Queued,
    };

    ChatGptBackendRunView {
        session_id: session.id.clone(),
        run_id: task.id.clone(),
        state,
        queued_at_ms: task.created_at_ms,
        started_at_ms: task.delivered_at_ms,
        completed_at_ms: terminal.map(|event| event.created_at_ms),
        result: terminal
            .filter(|event| event.kind == BackendEventKind::Result)
            .map(|event| event.content.clone()),
        error: terminal
            .filter(|event| event.kind == BackendEventKind::Error)
            .map(|event| event.content.clone())
            .or_else(|| {
                (state == BackendRunState::Stale)
                    .then(|| inspection.stale_reason.clone())
                    .flatten()
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chatgpt_backend::{ExchangeOutbound, ExchangeOutcome};
    use tokio_util::sync::CancellationToken;

    fn adapter() -> (tempfile::TempDir, ChatGptBackendAdapter, ChatGptBackendStore) {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("backend");
        (
            root,
            ChatGptBackendAdapter::new_at(directory.clone()),
            ChatGptBackendStore::new_at(directory),
        )
    }

    #[tokio::test]
    async fn adapter_maps_task_result_without_exposing_command_sequence_to_caller() {
        let (_root, adapter, store) = adapter();
        let session = store.attach("worker-a").unwrap();
        let acquired = adapter.acquire(None).unwrap();
        assert_eq!(acquired.session_id, session.id);

        let queued = adapter
            .submit_task(&session.id, "implement the change".into())
            .unwrap();
        assert_eq!(queued.state, BackendRunState::Queued);
        let outcome = store
            .exchange_wait(
                "worker-a",
                &session.id,
                false,
                0,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let command = match outcome {
            ExchangeOutcome::Command(command) => command,
            other => panic!("expected task command, got {other:?}"),
        };
        assert_eq!(command.id, queued.run_id);
        assert_eq!(
            adapter.run(&session.id, &queued.run_id).unwrap().state,
            BackendRunState::Running
        );

        let activity_seq = store
            .start_tool_activity(&session.id, command.seq, "exec_command")
            .unwrap();
        let progress = adapter.status(&session.id).unwrap();
        assert!(progress.timeline.iter().any(|entry| {
            entry.kind == "tool_started"
                && entry.seq == activity_seq
                && entry.command_seq == Some(command.seq)
                && entry.tool.as_deref() == Some("exec_command")
        }));
        store
            .complete_tool_activity(
                &session.id,
                activity_seq,
                crate::chatgpt_backend::BackendToolActivityStatus::Succeeded,
            )
            .unwrap();

        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                None,
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Result,
                    command_seq: command.seq,
                    content: "done".into(),
                }),
            )
            .unwrap();
        let finished = adapter.run(&session.id, &queued.run_id).unwrap();
        assert_eq!(finished.state, BackendRunState::Succeeded);
        assert_eq!(finished.result.as_deref(), Some("done"));
        assert!(finished.error.is_none());

        let waited = adapter
            .wait_run(&session.id, &queued.run_id, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(waited.state, BackendRunState::Succeeded);
        assert_eq!(waited.result.as_deref(), Some("done"));

        let status = adapter.status(&session.id).unwrap();
        assert_eq!(status.session.session_id, session.id);
        assert!(status.timeline.iter().any(|entry| {
            entry.kind == "event" && entry.command_seq == Some(command.seq)
        }));
        assert!(status.timeline.iter().any(|entry| {
            entry.kind == "tool_completed"
                && entry.seq == activity_seq
                && entry.tool_status
                    == Some(crate::chatgpt_backend::BackendToolActivityStatus::Succeeded)
        }));
    }

    #[tokio::test]
    async fn wait_run_times_out_without_exposing_bridge_details() {
        let (_root, adapter, store) = adapter();
        let session = store.attach("worker-a").unwrap();
        let run = adapter.submit_task(&session.id, "work".into()).unwrap();
        let error = adapter
            .wait_run(&session.id, &run.run_id, Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(error.code, BackendAdapterErrorCode::TimedOut);
        assert!(error.message.contains(&run.run_id));
        assert!(!error.message.contains("chatgpt_backend_exchange"));
    }

    #[tokio::test]
    async fn adapter_maps_steer_cancel_and_cancelled_terminal_state() {
        let (_root, adapter, store) = adapter();
        let session = store.attach("worker-a").unwrap();
        let run = adapter.submit_task(&session.id, "long work".into()).unwrap();
        let outcome = store
            .exchange_wait(
                "worker-a",
                &session.id,
                false,
                0,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let task = match outcome {
            ExchangeOutcome::Command(command) => command,
            other => panic!("expected task command, got {other:?}"),
        };

        let steer = adapter
            .steer(&session.id, &run.run_id, "change direction".into())
            .unwrap();
        assert_eq!(steer.action, "steer");
        let control = store.pending_control_for_worker("worker-a").unwrap().unwrap().1;
        store
            .prepare_exchange("worker-a", &session.id, Some(control.seq), None)
            .unwrap();

        let cancel = adapter
            .cancel(&session.id, &run.run_id, Some("stop".into()))
            .unwrap();
        assert_eq!(cancel.action, "cancel");
        assert_eq!(
            adapter.run(&session.id, &run.run_id).unwrap().state,
            BackendRunState::Cancelling
        );
        let cancel_command = store.pending_control_for_worker("worker-a").unwrap().unwrap().1;
        store
            .prepare_exchange(
                "worker-a",
                &session.id,
                Some(cancel_command.seq),
                Some(ExchangeOutbound {
                    kind: BackendEventKind::Error,
                    command_seq: task.seq,
                    content: "cancelled".into(),
                }),
            )
            .unwrap();
        let cancelled = adapter.run(&session.id, &run.run_id).unwrap();
        assert_eq!(cancelled.state, BackendRunState::Cancelled);
        assert_eq!(cancelled.error.as_deref(), Some("cancelled"));
    }

    #[test]
    fn acquire_filters_workspace_and_finish_rejects_busy_session() {
        let (_root, adapter, store) = adapter();
        let workspace = BackendWorkspace {
            active_root: "/workspace/a".into(),
            source_project_root: Some("/workspace/a".into()),
            managed_worktree: false,
            worktree_git_root: None,
            repository_url: None,
        };
        let session = store
            .attach_with_workspace("worker-a", Some(workspace))
            .unwrap();
        assert_eq!(
            adapter.acquire(Some("/workspace/a")).unwrap().session_id,
            session.id
        );
        assert_eq!(
            adapter.acquire(Some("/workspace/b")).unwrap_err().code,
            BackendAdapterErrorCode::Unavailable
        );
        let run = adapter.submit_task(&session.id, "work".into()).unwrap();
        let queued_session = adapter.session(&session.id).unwrap();
        assert_eq!(queued_session.state, BackendLifecycleState::Queued);
        assert!(!queued_session.accepting_tasks);
        assert_eq!(
            adapter.acquire(Some("/workspace/a")).unwrap_err().code,
            BackendAdapterErrorCode::Unavailable
        );
        assert_eq!(
            adapter.finish(&session.id).unwrap_err().code,
            BackendAdapterErrorCode::Busy
        );
        assert_eq!(run.state, BackendRunState::Queued);
    }

    #[test]
    fn pool_reports_explicit_capacity_and_dispatch_routes_across_workspace_workers() {
        let (_root, adapter, store) = adapter();
        let workspace = BackendWorkspace {
            active_root: "/workspace/a".into(),
            source_project_root: Some("/workspace/a".into()),
            managed_worktree: false,
            worktree_git_root: None,
            repository_url: None,
        };
        store
            .attach_with_workspace("worker-a", Some(workspace.clone()))
            .unwrap();
        store
            .attach_with_workspace("worker-b", Some(workspace))
            .unwrap();

        let initial = adapter.pool(Some("/workspace/a")).unwrap();
        assert_eq!(initial.total_sessions, 2);
        assert_eq!(initial.available_capacity, 2);
        assert_eq!(initial.busy_sessions, 0);
        assert_eq!(initial.draining_sessions, 0);
        assert_eq!(initial.unavailable_sessions, 0);

        let first = adapter
            .dispatch_task(Some("/workspace/a"), "first".into())
            .unwrap();
        let after_first = adapter.pool(Some("/workspace/a")).unwrap();
        assert_eq!(after_first.available_capacity, 1);
        assert_eq!(after_first.busy_sessions, 1);

        let second = adapter
            .dispatch_task(Some("/workspace/a"), "second".into())
            .unwrap();
        assert_ne!(first.session_id, second.session_id);
        let saturated = adapter.pool(Some("/workspace/a")).unwrap();
        assert_eq!(saturated.available_capacity, 0);
        assert_eq!(saturated.busy_sessions, 2);

        let error = adapter
            .dispatch_task(Some("/workspace/a"), "backpressure".into())
            .unwrap_err();
        assert_eq!(error.code, BackendAdapterErrorCode::Unavailable);
        assert!(error.message.contains("busy=2"));
    }

    #[test]
    fn concurrent_dispatch_has_one_winner_for_one_worker() {
        let (_root, adapter, store) = adapter();
        store.attach("worker-a").unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut threads = Vec::new();
        for prompt in ["first", "second"] {
            let adapter = adapter.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                adapter.dispatch_task(None, prompt.into())
            }));
        }
        barrier.wait();
        let results = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(error) if error.code == BackendAdapterErrorCode::Unavailable))
                .count(),
            1
        );
    }

    #[test]
    fn drain_excludes_worker_from_capacity_and_routes_to_healthy_peer() {
        let (_root, adapter, store) = adapter();
        let workspace = BackendWorkspace {
            active_root: "/workspace/a".into(),
            source_project_root: Some("/workspace/a".into()),
            managed_worktree: false,
            worktree_git_root: None,
            repository_url: None,
        };
        let first = store
            .attach_with_workspace("worker-a", Some(workspace.clone()))
            .unwrap();
        let second = store
            .attach_with_workspace("worker-b", Some(workspace))
            .unwrap();

        let receipt = adapter
            .drain(&second.id, Some("planned rotation".into()))
            .unwrap();
        assert_eq!(receipt.action, "drain");
        let drained = adapter.session(&second.id).unwrap();
        assert_eq!(drained.state, BackendLifecycleState::Draining);
        assert!(drained.live);
        assert_eq!(drained.drain_reason.as_deref(), Some("planned rotation"));
        let pool = adapter.pool(Some("/workspace/a")).unwrap();
        assert_eq!(pool.available_capacity, 1);
        assert_eq!(pool.draining_sessions, 1);
        assert_eq!(pool.busy_sessions, 0);

        assert_eq!(
            adapter.acquire(Some("/workspace/a")).unwrap().session_id,
            first.id
        );
        assert_eq!(
            adapter.submit_task(&second.id, "new work".into()).unwrap_err().code,
            BackendAdapterErrorCode::Busy
        );
    }

    #[tokio::test]
    async fn draining_active_worker_finishes_current_run_then_accepts_finish() {
        let (_root, adapter, store) = adapter();
        let session = store.attach("worker-a").unwrap();
        let run = adapter.submit_task(&session.id, "current work".into()).unwrap();
        let outcome = store
            .exchange_wait(
                "worker-a",
                &session.id,
                false,
                0,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let task = match outcome {
            ExchangeOutcome::Command(command) => command,
            other => panic!("expected task command, got {other:?}"),
        };

        adapter
            .drain(&session.id, Some("rotate after current run".into()))
            .unwrap();
        assert_eq!(
            adapter.session(&session.id).unwrap().state,
            BackendLifecycleState::Draining
        );
        assert_eq!(
            adapter.run(&session.id, &run.run_id).unwrap().state,
            BackendRunState::Running
        );
        assert_eq!(
            adapter.finish(&session.id).unwrap_err().code,
            BackendAdapterErrorCode::Busy
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
        assert_eq!(
            adapter.run(&session.id, &run.run_id).unwrap().state,
            BackendRunState::Succeeded
        );
        assert_eq!(
            adapter.session(&session.id).unwrap().state,
            BackendLifecycleState::Draining
        );
        assert_eq!(adapter.finish(&session.id).unwrap().action, "finish");
    }

    #[test]
    fn abandon_terminalizes_a_stuck_run_as_stale() {
        let (_root, adapter, store) = adapter();
        let session = store.attach("worker-a").unwrap();
        let run = adapter.submit_task(&session.id, "work".into()).unwrap();

        let receipt = adapter
            .abandon(&session.id, Some("interrupt timed out".into()))
            .unwrap();
        assert_eq!(receipt.action, "abandon");
        let abandoned = adapter.run(&session.id, &run.run_id).unwrap();
        assert_eq!(abandoned.state, BackendRunState::Stale);
        assert_eq!(abandoned.error.as_deref(), Some("interrupt timed out"));
        assert!(!adapter.session(&session.id).unwrap().live);
    }

    #[tokio::test]
    async fn controller_request_round_trip_stays_on_adapter_contract() {
        let (_root, adapter, store) = adapter();
        let session = store.attach("worker-a").unwrap();

        let pool_request: BackendControllerRequest = serde_json::from_value(serde_json::json!({
            "op": "pool"
        }))
        .unwrap();
        let response = adapter.handle_controller_request(pool_request).await.unwrap();
        let BackendControllerResponse::Pool(pool) = response else {
            panic!("expected pool controller response");
        };
        assert_eq!(pool.available_capacity, 1);

        let drain_request: BackendControllerRequest = serde_json::from_value(serde_json::json!({
            "op": "drain",
            "session_id": session.id,
            "reason": "controller rotation"
        }))
        .unwrap();
        let response = adapter.handle_controller_request(drain_request).await.unwrap();
        let BackendControllerResponse::Receipt(receipt) = response else {
            panic!("expected drain controller receipt");
        };
        assert_eq!(receipt.action, "drain");
        assert_eq!(
            adapter.session(&session.id).unwrap().state,
            BackendLifecycleState::Draining
        );

        let fresh = store.attach("worker-b").unwrap();
        let request: BackendControllerRequest = serde_json::from_value(serde_json::json!({
            "op": "dispatch",
            "prompt": "controller task"
        }))
        .unwrap();
        let response = adapter.handle_controller_request(request).await.unwrap();
        let BackendControllerResponse::Run(run) = response else {
            panic!("expected run controller response");
        };
        assert_eq!(run.state, BackendRunState::Queued);
        assert_eq!(run.session_id, fresh.id);

        let encoded = serde_json::to_value(&run).unwrap();
        assert!(encoded.get("run_id").is_some());
        assert!(encoded.get("command_seq").is_none());
        assert!(encoded.get("worker").is_none());

        let invalid = serde_json::from_value::<BackendControllerRequest>(serde_json::json!({
            "op": "submit",
            "session_id": session.id,
            "prompt": "x",
            "bridge_internal": true
        }));
        assert!(invalid.is_err());
    }
}
