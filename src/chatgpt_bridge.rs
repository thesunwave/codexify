use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::types::AppConfig;

const MAX_PROMPT_BYTES: usize = 64 * 1024;
const MAX_RESULT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeRequestStatus {
    Queued,
    Claimed,
    Completed,
    Failed,
}

impl BridgeRequestStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeRequest {
    pub id: String,
    pub prompt: String,
    pub status: BridgeRequestStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    Idle,
    Busy { request_id: String },
    Claimed(BridgeRequest),
}

#[derive(Debug, Clone)]
pub struct ChatGptBridgeStore {
    directory: PathBuf,
}

impl ChatGptBridgeStore {
    pub fn for_current_user(config: &AppConfig) -> Result<Self, String> {
        if !config.experimental.chatgpt_bridge {
            return Err("experimental.chatgptBridge is disabled".into());
        }
        if !config.ui_widgets {
            return Err("experimental.chatgptBridge requires uiWidgets".into());
        }
        let home = crate::util::home_dir()
            .ok_or("experimental.chatgptBridge requires a home directory")?;
        Ok(Self::new_at(
            home.join(".codexify/chatgpt-bridge")
                .join(scope_key(config)),
        ))
    }

    pub fn new_at(directory: PathBuf) -> Self {
        Self { directory }
    }

    pub fn enqueue(&self, prompt: String) -> Result<BridgeRequest, String> {
        validate_text("prompt", &prompt, MAX_PROMPT_BYTES)?;
        self.with_lock(|| {
            let now = now_ms()?;
            let request = BridgeRequest {
                id: new_id()?,
                prompt,
                status: BridgeRequestStatus::Queued,
                created_at_ms: now,
                updated_at_ms: now,
                claimed_by: None,
                result: None,
            };
            self.write_request(&request)?;
            Ok(request)
        })
    }

    pub fn claim_next(&self, worker: &str) -> Result<ClaimOutcome, String> {
        validate_worker(worker)?;
        self.with_lock(|| {
            let mut requests = self.read_all()?;
            if let Some(request) = requests.iter().find(|request| {
                request.status == BridgeRequestStatus::Claimed
                    && request.claimed_by.as_deref() == Some(worker)
            }) {
                return Ok(ClaimOutcome::Busy {
                    request_id: request.id.clone(),
                });
            }

            requests.sort_by(|left, right| {
                (left.created_at_ms, &left.id).cmp(&(right.created_at_ms, &right.id))
            });
            let Some(mut request) = requests
                .into_iter()
                .find(|request| request.status == BridgeRequestStatus::Queued)
            else {
                return Ok(ClaimOutcome::Idle);
            };
            request.status = BridgeRequestStatus::Claimed;
            request.claimed_by = Some(worker.to_string());
            request.updated_at_ms = now_ms()?;
            self.write_request(&request)?;
            Ok(ClaimOutcome::Claimed(request))
        })
    }

    pub fn submit_result(
        &self,
        worker: &str,
        request_id: &str,
        status: BridgeRequestStatus,
        result: String,
    ) -> Result<BridgeRequest, String> {
        validate_worker(worker)?;
        validate_id(request_id)?;
        if !matches!(status, BridgeRequestStatus::Completed | BridgeRequestStatus::Failed) {
            return Err("result status must be completed or failed".into());
        }
        validate_text("result", &result, MAX_RESULT_BYTES)?;

        self.with_lock(|| {
            let mut request = self.read_request(request_id)?;
            if request.status.is_terminal() {
                if request.status == status && request.result.as_deref() == Some(result.as_str()) {
                    return Ok(request);
                }
                return Err(format!(
                    "request {request_id} already finished with status {:?}",
                    request.status
                ));
            }
            if request.status != BridgeRequestStatus::Claimed {
                return Err(format!("request {request_id} has not been claimed"));
            }
            if request.claimed_by.as_deref() != Some(worker) {
                return Err(format!("request {request_id} belongs to another ChatGPT worker"));
            }
            request.status = status;
            request.result = Some(result);
            request.updated_at_ms = now_ms()?;
            self.write_request(&request)?;
            Ok(request)
        })
    }

    pub fn request(&self, request_id: &str) -> Result<BridgeRequest, String> {
        validate_id(request_id)?;
        self.with_lock(|| self.read_request(request_id))
    }

    fn with_lock<T>(&self, operation: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
        self.ensure_directory()?;
        let lock_path = self.directory.join("queue.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| format!("open ChatGPT bridge lock: {error}"))?;
        lock.lock()
            .map_err(|error| format!("lock ChatGPT bridge queue: {error}"))?;
        operation()
    }

    fn ensure_directory(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.directory)
            .map_err(|error| format!("create ChatGPT bridge state directory: {error}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.directory, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| format!("secure ChatGPT bridge state directory: {error}"))?;
        }
        Ok(())
    }

    fn read_all(&self) -> Result<Vec<BridgeRequest>, String> {
        let mut requests = Vec::new();
        for entry in std::fs::read_dir(&self.directory)
            .map_err(|error| format!("read ChatGPT bridge state directory: {error}"))?
        {
            let entry = entry.map_err(|error| format!("read ChatGPT bridge state entry: {error}"))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("read ChatGPT bridge request {}: {error}", path.display()))?;
            let request = serde_json::from_slice(&bytes)
                .map_err(|error| format!("parse ChatGPT bridge request {}: {error}", path.display()))?;
            requests.push(request);
        }
        Ok(requests)
    }

    fn read_request(&self, request_id: &str) -> Result<BridgeRequest, String> {
        let path = self.request_path(request_id);
        let bytes = std::fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!("unknown ChatGPT bridge request: {request_id}")
            } else {
                format!("read ChatGPT bridge request {request_id}: {error}")
            }
        })?;
        serde_json::from_slice(&bytes)
            .map_err(|error| format!("parse ChatGPT bridge request {request_id}: {error}"))
    }

    fn write_request(&self, request: &BridgeRequest) -> Result<(), String> {
        let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)
            .map_err(|error| format!("create ChatGPT bridge request temp file: {error}"))?;
        serde_json::to_writer(&mut temporary, request)
            .map_err(|error| format!("serialize ChatGPT bridge request: {error}"))?;
        temporary
            .write_all(b"\n")
            .map_err(|error| format!("write ChatGPT bridge request: {error}"))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| format!("sync ChatGPT bridge request: {error}"))?;
        temporary
            .persist(self.request_path(&request.id))
            .map_err(|error| format!("persist ChatGPT bridge request: {}", error.error))?;
        Ok(())
    }

    fn request_path(&self, request_id: &str) -> PathBuf {
        self.directory.join(format!("{request_id}.json"))
    }
}

pub async fn run_ask_cli(
    config: &AppConfig,
    args: crate::config::ChatGptBridgeAskArgs,
) -> anyhow::Result<()> {
    let store = ChatGptBridgeStore::for_current_user(config).map_err(anyhow::Error::msg)?;
    let request = store.enqueue(args.prompt).map_err(anyhow::Error::msg)?;
    if !args.json {
        crate::terminal::write_stderr(&format!(
            "Queued ChatGPT bridge request {}; waiting for a mounted worker…\n",
            request.id
        ))?;
    }

    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(args.timeout_seconds);
    loop {
        let current = store.request(&request.id).map_err(anyhow::Error::msg)?;
        match current.status {
            BridgeRequestStatus::Completed => {
                if args.json {
                    crate::terminal::write_stdout(&format!(
                        "{}\n",
                        serde_json::to_string_pretty(&current)?
                    ))?;
                } else {
                    crate::terminal::write_stdout(current.result.as_deref().unwrap_or_default())?;
                    crate::terminal::write_stdout("\n")?;
                }
                return Ok(());
            }
            BridgeRequestStatus::Failed => {
                if args.json {
                    crate::terminal::write_stdout(&format!(
                        "{}\n",
                        serde_json::to_string_pretty(&current)?
                    ))?;
                }
                anyhow::bail!(
                    "ChatGPT bridge request {} failed: {}",
                    current.id,
                    current.result.as_deref().unwrap_or("no error text returned")
                );
            }
            BridgeRequestStatus::Queued | BridgeRequestStatus::Claimed => {}
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for ChatGPT bridge request {} after {} seconds; current status is {:?}",
                current.id,
                args.timeout_seconds,
                current.status
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

fn scope_key(config: &AppConfig) -> String {
    let mut tunnel_ids = config
        .configured_openai_tunnels()
        .map(|tunnel| tunnel.tunnel_id.as_str())
        .collect::<Vec<_>>();
    tunnel_ids.sort_unstable();
    let mut hash = Sha256::new();
    hash.update(b"chatgpt-bridge-v1\0");
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
        .map_err(|error| format!("generate ChatGPT bridge request id: {error}"))?;
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
        Err("invalid ChatGPT bridge request id".into())
    }
}

fn validate_worker(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        Err("invalid ChatGPT bridge worker identity".into())
    } else {
        Ok(())
    }
}

fn validate_text(name: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if value.len() > max_bytes {
        return Err(format!("{name} exceeds the {max_bytes}-byte PoC limit"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, ChatGptBridgeStore) {
        let root = tempfile::tempdir().unwrap();
        let store = ChatGptBridgeStore::new_at(root.path().join("bridge"));
        (root, store)
    }

    #[test]
    fn request_round_trip_is_single_worker_and_idempotent() {
        let (_root, store) = store();
        let first = store.enqueue("answer this".into()).unwrap();
        let second = store.enqueue("answer later".into()).unwrap();

        let claimed = store.claim_next("worker-a").unwrap();
        let ClaimOutcome::Claimed(claimed) = claimed else {
            panic!("expected first request to be claimed");
        };
        assert_eq!(claimed.id, first.id);
        assert_eq!(
            store.claim_next("worker-a").unwrap(),
            ClaimOutcome::Busy {
                request_id: first.id.clone()
            }
        );
        assert!(matches!(
            store.claim_next("worker-b").unwrap(),
            ClaimOutcome::Claimed(request) if request.id == second.id
        ));

        let completed = store
            .submit_result(
                "worker-a",
                &first.id,
                BridgeRequestStatus::Completed,
                "done".into(),
            )
            .unwrap();
        assert_eq!(completed.result.as_deref(), Some("done"));
        assert_eq!(
            store
                .submit_result(
                    "worker-a",
                    &first.id,
                    BridgeRequestStatus::Completed,
                    "done".into(),
                )
                .unwrap(),
            completed
        );
        assert!(
            store
                .submit_result(
                    "worker-a",
                    &first.id,
                    BridgeRequestStatus::Completed,
                    "different".into(),
                )
                .is_err()
        );
    }

    #[test]
    fn result_must_come_from_the_claiming_worker() {
        let (_root, store) = store();
        let request = store.enqueue("secret work".into()).unwrap();
        assert!(matches!(
            store.claim_next("worker-a").unwrap(),
            ClaimOutcome::Claimed(_)
        ));
        let error = store
            .submit_result(
                "worker-b",
                &request.id,
                BridgeRequestStatus::Failed,
                "nope".into(),
            )
            .unwrap_err();
        assert!(error.contains("another ChatGPT worker"));
    }

    #[test]
    fn queued_request_cannot_be_completed_without_a_claim() {
        let (_root, store) = store();
        let request = store.enqueue("work".into()).unwrap();
        assert!(
            store
                .submit_result(
                    "worker-a",
                    &request.id,
                    BridgeRequestStatus::Completed,
                    "done".into(),
                )
                .unwrap_err()
                .contains("has not been claimed")
        );
    }
}
