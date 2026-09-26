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
pub const DEFAULT_WAIT_MS: u64 = 115_000;

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
    #[serde(skip_serializing_if = "String::is_empty")]
    pub content: String,
    pub created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged_at_ms: Option<u64>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendSession {
    pub id: String,
    pub worker: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub next_command_seq: u64,
    pub next_event_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_task_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<u64>,
    pub waiting: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_wait_started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_wait_returned_at_ms: Option<u64>,
    pub commands: Vec<BackendCommand>,
    pub events: Vec<BackendEvent>,
}

impl BackendSession {
    pub fn state(&self) -> &'static str {
        if self.closed_at_ms.is_some() {
            "closed"
        } else if self.active_task_seq.is_some() {
            "busy"
        } else if self.waiting {
            "waiting"
        } else {
            "attached"
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
        validate_worker(worker)?;
        self.with_lock(|| {
            let mut sessions = self.read_all()?;
            sessions.sort_by_key(|session| session.created_at_ms);
            if let Some(session) = sessions
                .into_iter()
                .rev()
                .find(|session| session.worker == worker && session.closed_at_ms.is_none())
            {
                return Ok(session);
            }
            let now = now_ms()?;
            let mut session = BackendSession {
                id: new_id()?,
                worker: worker.to_string(),
                created_at_ms: now,
                updated_at_ms: now,
                next_command_seq: 1,
                next_event_seq: 2,
                active_task_seq: None,
                closed_at_ms: None,
                waiting: false,
                last_wait_started_at_ms: None,
                last_wait_returned_at_ms: None,
                commands: Vec::new(),
                events: vec![BackendEvent {
                    seq: 1,
                    kind: BackendEventKind::Ready,
                    command_seq: None,
                    content: "ChatGPT coding backend attached".into(),
                    created_at_ms: now,
                }],
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

    pub fn session(&self, session_id: &str) -> Result<BackendSession, String> {
        validate_id(session_id)?;
        self.with_lock(|| self.read_session(session_id))
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
                    return Err(format!("command exceeds the {MAX_COMMAND_BYTES}-byte limit"));
                }
            }
        }
        self.with_lock(|| {
            let mut session = self.read_session(session_id)?;
            if session.closed_at_ms.is_some() {
                return Err(format!("ChatGPT backend session {session_id} is closed"));
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
            if kind == BackendCommandKind::Task
                && (session.active_task_seq.is_some()
                    || session.commands.iter().any(|command| {
                        command.kind == BackendCommandKind::Task
                            && command.acknowledged_at_ms.is_none()
                    }))
            {
                return Err("a ChatGPT backend task is already active or queued".into());
            }
            if matches!(kind, BackendCommandKind::Steer | BackendCommandKind::Cancel)
                && session.active_task_seq.is_none()
                && !session.commands.iter().any(|command| {
                    command.kind == BackendCommandKind::Task
                        && command.acknowledged_at_ms.is_none()
                })
            {
                return Err("steer/cancel requires an active or queued task".into());
            }
            let now = now_ms()?;
            let command = BackendCommand {
                seq: session.next_command_seq,
                id: new_id()?,
                kind,
                content,
                created_at_ms: now,
                delivered_at_ms: None,
                acknowledged_at_ms: None,
            };
            session.next_command_seq = session.next_command_seq.saturating_add(1);
            session.updated_at_ms = now;
            session.commands.push(command.clone());
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
                    command.acknowledged_at_ms = Some(now_ms()?);
                }
            }

            if let Some(outbound) = outbound {
                if !matches!(outbound.kind, BackendEventKind::Result | BackendEventKind::Error) {
                    return Err("model outbound event must be result or error".into());
                }
                validate_text("event content", &outbound.content, MAX_EVENT_BYTES)?;
                let task = session
                    .commands
                    .iter()
                    .find(|command| command.seq == outbound.command_seq)
                    .ok_or_else(|| format!("unknown task sequence {}", outbound.command_seq))?;
                if task.kind != BackendCommandKind::Task {
                    return Err("outbound result/error must reference a task command".into());
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
                    session.next_event_seq = session.next_event_seq.saturating_add(1);
                    session.active_task_seq = None;
                    session.updated_at_ms = now;
                }
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
                .find(|command| command.delivered_at_ms.is_some() && command.acknowledged_at_ms.is_none())
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
            session.waiting = waiting;
            if waiting {
                session.last_wait_started_at_ms = Some(now_ms()?);
            }
            session.updated_at_ms = now_ms()?;
            self.write_session(&session)
        })
    }

    fn mark_wait_returned(&self, worker: &str, session_id: &str) -> Result<(), String> {
        self.with_lock(|| {
            let mut session = self.read_session(session_id)?;
            ensure_worker(&session, worker)?;
            session.waiting = false;
            session.last_wait_returned_at_ms = Some(now_ms()?);
            session.updated_at_ms = now_ms()?;
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
            let entry = entry.map_err(|error| format!("read ChatGPT backend state entry: {error}"))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("read ChatGPT backend session {}: {error}", path.display()))?;
            let session = serde_json::from_slice(&bytes)
                .map_err(|error| format!("parse ChatGPT backend session {}: {error}", path.display()))?;
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
        let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)
            .map_err(|error| format!("create ChatGPT backend session temp file: {error}"))?;
        serde_json::to_writer(&mut temporary, session)
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
    let sessions = store.list().map_err(anyhow::Error::msg)?;
    if args.json {
        crate::terminal::write_stdout(&format!("{}\n", serde_json::to_string_pretty(&sessions)?))?;
        return Ok(());
    }
    if sessions.is_empty() {
        crate::terminal::write_stdout("No ChatGPT backend sessions.\n")?;
        return Ok(());
    }
    for session in sessions {
        crate::terminal::write_stdout(&format!(
            "{}\t{}\tworker={}\tcommands={}\tevents={}\n",
            session.id,
            session.state(),
            &session.worker[..session.worker.len().min(12)],
            session.commands.len(),
            session.events.len()
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
    let session = store.session(&args.session_id).map_err(anyhow::Error::msg)?;
    if args.json {
        crate::terminal::write_stdout(&format!("{}\n", serde_json::to_string_pretty(&session)?))?;
    } else {
        crate::terminal::write_stdout(&format!(
            "session={} state={} worker={} commands={} events={} active_task={:?} waiting={}\n",
            session.id,
            session.state(),
            &session.worker[..session.worker.len().min(12)],
            session.commands.len(),
            session.events.len(),
            session.active_task_seq,
            session.waiting
        ))?;
        if let Some(event) = session.events.last() {
            crate::terminal::write_stdout(&format!(
                "last_event seq={} kind={:?} command_seq={:?} content={}\n",
                event.seq, event.kind, event.command_seq, event.content
            ))?;
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
            .exchange_wait(
                "worker-a",
                &session.id,
                false,
                0,
                CancellationToken::new(),
            )
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

    #[tokio::test]
    async fn delivered_command_is_replayed_until_acknowledged() {
        let (_root, store) = store();
        let session = store.attach("worker-a").unwrap();
        let command = store
            .enqueue_command(&session.id, BackendCommandKind::Task, "work".into())
            .unwrap();
        for _ in 0..2 {
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
            assert!(matches!(outcome, ExchangeOutcome::Command(ref value) if value.seq == command.seq));
        }
    }
}
