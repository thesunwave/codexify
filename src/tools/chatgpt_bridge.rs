use async_trait::async_trait;
use rmcp::model::MetaObject;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::chatgpt_bridge::{BridgeRequestStatus, ChatGptBridgeStore, ClaimOutcome};
use crate::exec_sessions::SessionState;
use crate::tool::{Tool, ToolBehavior, ToolRequestContext, empty_object_schema, parse_tool_args};
use crate::types::{AppConfig, ToolResult};

pub enum ChatGptBridgeTool {
    Worker,
    Next,
    Submit,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitArgs {
    request_id: String,
    status: BridgeRequestStatus,
    result: String,
}

fn worker_key(context: &ToolRequestContext) -> Result<&str, Box<ToolResult>> {
    context
        .conversation
        .as_ref()
        .map(|identity| identity.stable_key())
        .ok_or_else(|| {
            Box::new(ToolResult::error(
                "ChatGPT bridge requires a stable ChatGPT conversation identity.",
            ))
        })
}

#[async_trait]
impl Tool for ChatGptBridgeTool {
    fn name(&self) -> &'static str {
        match self {
            Self::Worker => "chatgpt_bridge_worker",
            Self::Next => "chatgpt_bridge_next",
            Self::Submit => "chatgpt_bridge_submit_result",
        }
    }

    fn title(&self) -> String {
        match self {
            Self::Worker => "Open ChatGPT bridge worker",
            Self::Next => "Claim next bridge request",
            Self::Submit => "Submit ChatGPT bridge result",
        }
        .into()
    }

    fn description(&self) -> String {
        match self {
            Self::Worker => "Open the experimental Codexify ChatGPT bridge worker for this conversation. The widget can claim locally queued requests and turn them into ChatGPT follow-up messages.",
            Self::Next => "App-only: claim the next locally queued ChatGPT bridge request for this conversation, or report that this worker is idle/busy. At most one request may be claimed by a worker conversation at a time.",
            Self::Submit => "Complete the external Codexify ChatGPT bridge request identified by request_id. Use this tool when a bridge-generated follow-up asks you to process an external request. Before ending that turn, call this exactly once with status completed and your final answer, or status failed and a concise error. Repeating the identical terminal result is idempotent.",
        }
        .into()
    }

    fn meta(&self) -> Option<MetaObject> {
        match self {
            Self::Worker => Some(crate::chatgpt_bridge_ui::tool_meta()),
            Self::Next => Some(
                serde_json::from_value(json!({
                    "ui": { "visibility": ["app"] },
                    "openai/visibility": "private",
                    "openai/widgetAccessible": true
                }))
                .expect("static ChatGPT bridge app-only metadata"),
            ),
            Self::Submit => None,
        }
    }

    fn behavior(&self) -> ToolBehavior {
        match self {
            Self::Worker => ToolBehavior::new(
                true,
                false,
                true,
                false,
                "Opening the bridge worker only renders local connector UI and reads no external state.",
            ),
            Self::Next => ToolBehavior::new(
                false,
                false,
                true,
                false,
                "Claiming a queued local request mutates only Codexify bridge state; repeated polls for the same worker return busy rather than claiming another request.",
            ),
            Self::Submit => ToolBehavior::new(
                false,
                false,
                true,
                false,
                "Submitting a result mutates only the local correlated bridge request and identical terminal submissions are idempotent.",
            ),
        }
    }

    fn input_schema(&self) -> Value {
        match self {
            Self::Worker | Self::Next => empty_object_schema(),
            Self::Submit => json!({
                "type": "object",
                "properties": {
                    "request_id": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9_-]{1,80}$"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["completed", "failed"]
                    },
                    "result": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 262144
                    }
                },
                "required": ["request_id", "status", "result"],
                "additionalProperties": false
            }),
        }
    }

    fn output_schema(&self) -> Option<Value> {
        Some(match self {
            Self::Worker => json!({
                "type": "object",
                "properties": {
                    "state": { "type": "string", "enum": ["ready"] }
                },
                "required": ["state"],
                "additionalProperties": false
            }),
            Self::Next => json!({
                "type": "object",
                "properties": {
                    "state": { "type": "string", "enum": ["idle", "busy", "claimed"] },
                    "request_id": { "type": "string" },
                    "prompt": { "type": "string" }
                },
                "required": ["state"],
                "additionalProperties": false
            }),
            Self::Submit => json!({
                "type": "object",
                "properties": {
                    "request_id": { "type": "string" },
                    "status": { "type": "string", "enum": ["completed", "failed"] }
                },
                "required": ["request_id", "status"],
                "additionalProperties": false
            }),
        })
    }

    fn requires_project_root(&self) -> bool {
        false
    }

    async fn call(&self, _args: Value, _config: &AppConfig, _session: &SessionState) -> ToolResult {
        ToolResult::error("ChatGPT bridge tools require ChatGPT conversation context.")
    }

    async fn call_with_context(
        &self,
        args: Value,
        config: &AppConfig,
        _session: &SessionState,
        context: &ToolRequestContext,
    ) -> ToolResult {
        if !config.experimental.chatgpt_bridge || !config.ui_widgets {
            return ToolResult::error(
                "ChatGPT bridge requires experimental.chatgptBridge and uiWidgets.",
            );
        }
        if context.cancellation.is_cancelled() {
            return ToolResult::error("ChatGPT bridge request was cancelled.");
        }
        let store = match ChatGptBridgeStore::for_current_user(config) {
            Ok(store) => store,
            Err(error) => return ToolResult::error(error),
        };

        match self {
            Self::Worker => ToolResult::text("ChatGPT bridge worker is ready.")
                .with_structured(json!({ "state": "ready" })),
            Self::Next => {
                let worker = match worker_key(context) {
                    Ok(worker) => worker,
                    Err(error) => return *error,
                };
                match store.claim_next(worker) {
                    Ok(ClaimOutcome::Idle) => ToolResult::text("No queued ChatGPT bridge request.")
                        .with_structured(json!({ "state": "idle" })),
                    Ok(ClaimOutcome::Busy { request_id }) => ToolResult::text(
                        "This ChatGPT bridge worker already has a request in flight.",
                    )
                    .with_structured(json!({
                        "state": "busy",
                        "request_id": request_id
                    })),
                    Ok(ClaimOutcome::Claimed(request)) => ToolResult::text(
                        "Claimed one queued ChatGPT bridge request for this worker.",
                    )
                    .with_structured(json!({
                        "state": "claimed",
                        "request_id": request.id,
                        "prompt": request.prompt
                    })),
                    Err(error) => ToolResult::error(error),
                }
            }
            Self::Submit => {
                let worker = match worker_key(context) {
                    Ok(worker) => worker,
                    Err(error) => return *error,
                };
                let SubmitArgs {
                    request_id,
                    status,
                    result,
                } = match parse_tool_args(args) {
                    Ok(args) => args,
                    Err(error) => return *error,
                };
                match store.submit_result(worker, &request_id, status, result) {
                    Ok(request) => ToolResult::text(format!(
                        "ChatGPT bridge request {} recorded as {:?}.",
                        request.id, request.status
                    ))
                    .with_structured(json!({
                        "request_id": request.id,
                        "status": match request.status {
                            BridgeRequestStatus::Completed => "completed",
                            BridgeRequestStatus::Failed => "failed",
                            _ => unreachable!("submit_result only returns terminal requests"),
                        }
                    })),
                    Err(error) => ToolResult::error(error),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_only_claim_tool_is_hidden_from_the_model() {
        let meta = ChatGptBridgeTool::Next.meta().unwrap();
        assert_eq!(meta.get("openai/visibility"), Some(&json!("private")));
        assert_eq!(meta["ui"]["visibility"], json!(["app"]));
    }

    #[test]
    fn worker_is_linked_to_the_bridge_widget() {
        let meta = ChatGptBridgeTool::Worker.meta().unwrap();
        assert_eq!(
            meta["ui"]["resourceUri"],
            json!(crate::chatgpt_bridge_ui::CHATGPT_BRIDGE_UI_URI)
        );
        assert_eq!(meta["openai/widgetAccessible"], json!(true));
    }
}
