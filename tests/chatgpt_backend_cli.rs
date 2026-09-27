use std::fs;
use std::process::Command;

use codexify::chatgpt_backend::{BackendLifecycleState, ChatGptBackendStore};
use codexify::chatgpt_backend_adapter::{BackendRunState, ChatGptBackendAdapter};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_codexify")
}

fn backend_store_dir(home: &std::path::Path, work_dir: &std::path::Path, port: u16) -> std::path::PathBuf {
    let mut hash = Sha256::new();
    hash.update(b"chatgpt-backend-v1\0");
    hash.update(b"http\0");
    hash.update(work_dir.to_string_lossy().as_bytes());
    hash.update(b"\0");
    hash.update(port.to_le_bytes());
    home.join(".codexify/chatgpt-backend")
        .join(format!("{:x}", hash.finalize()))
}

#[test]
fn chatgpt_backend_abandon_dispatches_and_prints_clean_json_receipt() {
    let root = TempDir::new().unwrap();
    let home = root.path().join("home");
    let work_dir = root.path().join("project");
    let config_path = home.join(".codexify/codexify.config.json");
    let port = 34789;
    fs::create_dir_all(&home).unwrap();
    fs::create_dir_all(&work_dir).unwrap();
    fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&json!({
            "workDir": work_dir,
            "port": port,
            "experimental": {"chatgptBridge": true}
        }))
        .unwrap(),
    )
    .unwrap();

    let store_dir = backend_store_dir(&home, &work_dir, port);
    let store = ChatGptBackendStore::new_at(store_dir.clone());
    let session = store.attach("test-worker").unwrap();
    let adapter = ChatGptBackendAdapter::new_at(store_dir);
    let run = adapter
        .submit_task(&session.id, "stay busy".to_string())
        .unwrap();

    let output = Command::new(binary())
        .args([
            "chatgpt-backend",
            "abandon",
            &session.id,
            "--reason",
            "controller timed out",
        ])
        .current_dir(root.path())
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("NO_COLOR", "1")
        .env_remove("CLICOLOR_FORCE")
        .env_remove("CODEXIFY_CONFIG")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        receipt,
        json!({
            "accepted": true,
            "session_id": session.id,
            "action": "abandon"
        })
    );
    assert_eq!(
        store.inspection(&session.id).unwrap().state,
        BackendLifecycleState::Stale
    );
    assert_eq!(
        adapter.run(&session.id, &run.run_id).unwrap().state,
        BackendRunState::Stale
    );
}
