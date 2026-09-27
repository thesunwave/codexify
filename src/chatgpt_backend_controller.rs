#[cfg(unix)]
mod unix {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    use std::path::Path;
    use std::sync::Arc;

    use serde::Serialize;
    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};

    use crate::chatgpt_backend_adapter::{
        BackendAdapterError, BackendAdapterErrorCode, BackendControllerRequest,
        ChatGptBackendAdapter,
    };
    use crate::types::AppConfig;

    const MAX_REQUEST_BYTES: u64 = 1024 * 1024;
    #[derive(Debug, Serialize)]
    struct ControllerResponse {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<BackendAdapterError>,
    }

    pub fn spawn(config: Arc<AppConfig>) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
        let Some(path) = config.experimental.chatgpt_backend_controller_socket.clone() else {
            return Ok(None);
        };
        if !config.experimental.chatgpt_bridge {
            anyhow::bail!(
                "experimental.chatgptBackendControllerSocket requires experimental.chatgptBridge"
            );
        }
        let cross_user = config
            .experimental
            .chatgpt_backend_controller_allowed_uid
            .is_some();
        validate_socket_parent(&path, cross_user)?;
        prepare_socket_path(&path)?;
        let listener = UnixListener::bind(&path)
            .map_err(|error| anyhow::anyhow!("bind ChatGPT backend controller socket: {error}"))?;
        std::fs::set_permissions(
            &path,
            std::fs::Permissions::from_mode(if cross_user { 0o666 } else { 0o600 }),
        )
            .map_err(|error| anyhow::anyhow!("secure ChatGPT backend controller socket: {error}"))?;

        let path_for_task = path.clone();
        let handle = tokio::spawn(async move {
            tracing::info!(path = %path_for_task.display(), "ChatGPT backend controller socket listening");
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        if let Err(error) = authorize_peer(&config, &stream) {
                            tracing::warn!(%error, "rejected ChatGPT backend controller peer");
                            continue;
                        }
                        let config = config.clone();
                        tokio::spawn(async move {
                            if let Err(error) = handle_connection(config, stream).await {
                                tracing::warn!(%error, "ChatGPT backend controller connection failed");
                            }
                        });
                    }
                    Err(error) => {
                        tracing::error!(%error, "ChatGPT backend controller socket accept failed");
                        break;
                    }
                }
            }
            let _ = std::fs::remove_file(&path_for_task);
        });
        Ok(Some(handle))
    }

    async fn handle_connection(config: Arc<AppConfig>, stream: UnixStream) -> anyhow::Result<()> {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half).take(MAX_REQUEST_BYTES + 1);
        let mut bytes = Vec::new();
        reader.read_until(b'\n', &mut bytes).await?;
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() as u64 > MAX_REQUEST_BYTES {
            write_response(
                &mut write_half,
                ControllerResponse {
                    id: None,
                    ok: false,
                    result: None,
                    error: Some(BackendAdapterError {
                        code: BackendAdapterErrorCode::InvalidArgument,
                        message: format!(
                            "controller request exceeds the {MAX_REQUEST_BYTES}-byte limit"
                        ),
                    }),
                },
            )
            .await?;
            return Ok(());
        }
        if !bytes.ends_with(b"\n") {
            write_response(
                &mut write_half,
                ControllerResponse {
                    id: None,
                    ok: false,
                    result: None,
                    error: Some(BackendAdapterError {
                        code: BackendAdapterErrorCode::InvalidArgument,
                        message: "controller request must end with a newline".into(),
                    }),
                },
            )
            .await?;
            return Ok(());
        }

        let (id, request) = match parse_request(&bytes) {
            Ok(request) => request,
            Err(error) => {
                write_response(
                    &mut write_half,
                    ControllerResponse {
                        id: None,
                        ok: false,
                        result: None,
                        error: Some(BackendAdapterError {
                            code: BackendAdapterErrorCode::InvalidArgument,
                            message: format!("invalid controller request: {error}"),
                        }),
                    },
                )
                .await?;
                return Ok(());
            }
        };
        if id.as_ref().is_some_and(|value| value.len() > 128 || value.chars().any(char::is_control)) {
            write_response(
                &mut write_half,
                ControllerResponse {
                    id: None,
                    ok: false,
                    result: None,
                    error: Some(BackendAdapterError {
                        code: BackendAdapterErrorCode::InvalidArgument,
                        message: "controller request id is invalid".into(),
                    }),
                },
            )
            .await?;
            return Ok(());
        }

        let adapter = match ChatGptBackendAdapter::for_current_user(&config) {
            Ok(adapter) => adapter,
            Err(error) => {
                write_response(
                    &mut write_half,
                    ControllerResponse {
                        id,
                        ok: false,
                        result: None,
                        error: Some(error),
                    },
                )
                .await?;
                return Ok(());
            }
        };
        let result = adapter.handle_controller_request(request).await;
        let response = match result {
            Ok(result) => ControllerResponse {
                id,
                ok: true,
                result: Some(to_value(result)?),
                error: None,
            },
            Err(error) => ControllerResponse {
                id,
                ok: false,
                result: None,
                error: Some(error),
            },
        };
        write_response(&mut write_half, response).await?;
        Ok(())
    }

    fn parse_request(bytes: &[u8]) -> Result<(Option<String>, BackendControllerRequest), String> {
        let mut value: Value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| "controller request must be a JSON object".to_string())?;
        let id = match object.remove("id") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value),
            Some(_) => return Err("controller request id must be a string".into()),
        };
        let request = serde_json::from_value(value).map_err(|error| error.to_string())?;
        Ok((id, request))
    }

    fn to_value(value: impl Serialize) -> Result<Value, BackendAdapterError> {
        serde_json::to_value(value).map_err(|error| BackendAdapterError {
            code: BackendAdapterErrorCode::Internal,
            message: format!("serialize controller response: {error}"),
        })
    }

    async fn write_response(
        writer: &mut tokio::net::unix::OwnedWriteHalf,
        response: ControllerResponse,
    ) -> anyhow::Result<()> {
        let mut bytes = serde_json::to_vec(&response)?;
        bytes.push(b'\n');
        writer.write_all(&bytes).await?;
        writer.shutdown().await?;
        Ok(())
    }

    fn authorize_peer(config: &AppConfig, stream: &UnixStream) -> anyhow::Result<()> {
        let credentials = stream
            .peer_cred()
            .map_err(|error| anyhow::anyhow!("inspect ChatGPT backend controller peer: {error}"))?;
        let service_uid = unsafe { libc::geteuid() };
        let peer_uid = credentials.uid();
        if peer_uid_allowed(
            service_uid,
            config.experimental.chatgpt_backend_controller_allowed_uid,
            peer_uid,
        ) {
            return Ok(());
        }
        anyhow::bail!("unauthorized controller peer uid {peer_uid}")
    }

    fn peer_uid_allowed(service_uid: u32, allowed_uid: Option<u32>, peer_uid: u32) -> bool {
        peer_uid == service_uid || allowed_uid == Some(peer_uid)
    }

    fn validate_socket_parent(path: &Path, cross_user: bool) -> anyhow::Result<()> {
        if !path.is_absolute() {
            anyhow::bail!("ChatGPT backend controller socket path must be absolute");
        }
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("ChatGPT backend controller socket has no parent"))?;
        let metadata = std::fs::symlink_metadata(parent).map_err(|error| {
            anyhow::anyhow!(
                "inspect ChatGPT backend controller socket directory {}: {error}",
                parent.display()
            )
        })?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            anyhow::bail!(
                "ChatGPT backend controller socket parent must be a real directory: {}",
                parent.display()
            );
        }
        let uid = unsafe { libc::geteuid() };
        if metadata.uid() != uid {
            anyhow::bail!(
                "ChatGPT backend controller socket directory must be owned by the Codexify user: {}",
                parent.display()
            );
        }
        let mode = metadata.permissions().mode();
        if (!cross_user && mode & 0o077 != 0) || (cross_user && mode & 0o022 != 0) {
            anyhow::bail!(
                "ChatGPT backend controller socket directory permissions are too broad: {}",
                parent.display()
            );
        }
        if cross_user && mode & 0o011 == 0 {
            anyhow::bail!(
                "cross-user ChatGPT backend controller socket directory must grant traversal permission: {}",
                parent.display()
            );
        }
        Ok(())
    }

    fn prepare_socket_path(path: &Path) -> anyhow::Result<()> {
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "inspect ChatGPT backend controller socket {}: {error}",
                    path.display()
                ));
            }
        };
        if !metadata.file_type().is_socket() {
            anyhow::bail!(
                "refusing to replace non-socket ChatGPT backend controller path: {}",
                path.display()
            );
        }
        if std::os::unix::net::UnixStream::connect(path).is_ok() {
            anyhow::bail!(
                "ChatGPT backend controller socket is already in use: {}",
                path.display()
            );
        }
        std::fs::remove_file(path).map_err(|error| {
            anyhow::anyhow!(
                "remove stale ChatGPT backend controller socket {}: {error}",
                path.display()
            )
        })?;
        Ok(())
    }

    pub async fn request(path: &Path, request: Value) -> anyhow::Result<Value> {
        let mut stream = UnixStream::connect(path).await?;
        let mut bytes = serde_json::to_vec(&request)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        stream.shutdown().await?;
        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).await?;
        Ok(serde_json::from_str(&response)?)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn controller_round_trips_adapter_operations_over_unix_socket() {
            let root = tempfile::tempdir().unwrap();
            let socket_dir = root.path().join("controller");
            std::fs::create_dir(&socket_dir).unwrap();
            std::fs::set_permissions(&socket_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            let socket = socket_dir.join("backend.sock");
            let state = root.path().join("state");
            let adapter = ChatGptBackendAdapter::new_at(state.clone());
            let store = crate::chatgpt_backend::ChatGptBackendStore::new_at(state);
            let session = store.attach("worker-a").unwrap();

            let listener = UnixListener::bind(&socket).unwrap();
            let config = Arc::new(crate::config::default_config(root.path().to_path_buf()));
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read_half, mut write_half) = stream.into_split();
                let mut reader = BufReader::new(read_half);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let (id, request) = parse_request(line.as_bytes()).unwrap();
                let result = adapter
                    .handle_controller_request(request)
                    .await
                    .unwrap();
                write_response(
                    &mut write_half,
                    ControllerResponse {
                        id,
                        ok: true,
                        result: Some(to_value(result).unwrap()),
                        error: None,
                    },
                )
                .await
                .unwrap();
                drop(config);
            });

            let response = request(
                &socket,
                serde_json::json!({
                    "op": "status",
                    "id": "req-1",
                    "session_id": session.id,
                }),
            )
            .await
            .unwrap();
            assert_eq!(response["ok"], true);
            assert_eq!(response["id"], "req-1");
            assert_eq!(response["result"]["session"]["state"], "ready");
            assert!(response["result"].get("worker").is_none());
            server.await.unwrap();
        }

        #[test]
        fn controller_parent_must_be_private_and_owned() {
            let root = tempfile::tempdir().unwrap();
            let private = root.path().join("private");
            std::fs::create_dir(&private).unwrap();
            std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).unwrap();
            validate_socket_parent(&private.join("backend.sock"), false).unwrap();

            std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o770)).unwrap();
            assert!(validate_socket_parent(&private.join("backend.sock"), false).is_err());

            std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o711)).unwrap();
            validate_socket_parent(&private.join("backend.sock"), true).unwrap();
        }

        #[test]
        fn cross_user_controller_accepts_only_service_or_explicit_peer_uid() {
            assert!(peer_uid_allowed(502, Some(501), 502));
            assert!(peer_uid_allowed(502, Some(501), 501));
            assert!(!peer_uid_allowed(502, Some(501), 503));
            assert!(!peer_uid_allowed(502, None, 501));
        }
    }
}

#[cfg(unix)]
pub use unix::{request, spawn};

#[cfg(not(unix))]
pub fn spawn(
    _config: std::sync::Arc<crate::types::AppConfig>,
) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
    Ok(None)
}
