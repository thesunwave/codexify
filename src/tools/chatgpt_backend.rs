use async_trait::async_trait;
use rmcp::model::MetaObject;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::chatgpt_backend::{
    BackendCommandKind, ChatGptBackendStore, DEFAULT_WAIT_MS, ExchangeOutcome,
    ExchangeOutbound,
};
use crate::exec_sessions::SessionState;
use crate::tool::{Tool, ToolBehavior, ToolRequestContext, empty_object_schema, parse_tool_args};
use crate::types::{AppConfig, ToolResult};

pub enum ChatGptBackendTool {
    Attach,
    Exchange,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExchangeArgs {
    session_id: String,
    #[serde(default)]
    ack_command_seq: Option<u64>,
    #[serde(default)]
    outbound: Option<ExchangeOutbound>,
    #[serde(default = "default_wait")]
    wait: bool,
}

fn default_wait() -> bool {
    true
}

fn worker_key(context: &ToolRequestContext) -> Result<&str, Box<ToolResult>> {
    context
        .conversation
        .as_ref()
        .map(|identity| identity.stable_key())
        .ok_or_else(|| {
            Box::new(ToolResult::error(
                "ChatGPT backend requires a stable ChatGPT conversation identity.",
            ))
        })
}

fn command_result(session_id: &str, command: crate::chatgpt_backend::BackendCommand) -> ToolResult {
    let finish = command.kind == BackendCommandKind::Finish;
    let required = if finish {
        "end_turn"
    } else {
        "process_command"
    };
    ToolResult::text(if finish {
        "Paseo closed this ChatGPT backend session. You may end the assistant turn now.".into()
    } else {
        format!(
            "Paseo delivered {:?} command {}. Process it before the next backend exchange.",
            command.kind, command.seq
        )
    })
    .with_structured(json!({
        "state": "command",
        "session_id": session_id,
        "command": command,
        "required_next_action": required,
        "assistant_turn_may_end": finish
    }))
}

#[async_trait]
impl Tool for ChatGptBackendTool {
    fn name(&self) -> &'static str {
        match self {
            Self::Attach => "chatgpt_backend_attach",
            Self::Exchange => "chatgpt_backend_exchange",
        }
    }

    fn title(&self) -> String {
        match self {
            Self::Attach => "Attach ChatGPT coding backend",
            Self::Exchange => "Exchange with Paseo backend session",
        }
        .into()
    }

    fn description(&self) -> String {
        match self {
            Self::Attach => "NON-TERMINAL TOOL. Attach this ChatGPT conversation as a long-lived coding backend session for Paseo. After a successful attach, immediately call chatgpt_backend_exchange with the returned session_id and wait=true. Do not emit a normal final response or end the assistant turn while the session is open.",
            Self::Exchange => "NON-TERMINAL TOOL while the backend session is open. This is the bidirectional control point between the current ChatGPT turn and Paseo. When idle, call with wait=true and keep the turn alive across timeout/cancellation by calling it again. When a task command is returned, use the normal Codexify coding tools to execute it. While a task is active, inspect every ordinary Codexify tool result for an optional `chatgpt_backend_control` field. If it contains a steer command, apply the new instruction immediately and acknowledge that control sequence with chatgpt_backend_exchange(wait=false). If it contains cancel, stop the task promptly and call chatgpt_backend_exchange with ack_command_seq set to the cancel sequence plus outbound={kind:'error', command_seq:<active task seq>, content:<cancellation result>}. The terminal outbound event automatically acknowledges its task, so ack_command_seq remains available for injected control. When a task completes normally, send outbound={kind:'result'|'error', command_seq:<task seq>, content:<final backend result>} and then wait for the next command. A finish command is the only normal condition that permits ending the assistant turn.",
        }
        .into()
    }

    fn meta(&self) -> Option<MetaObject> {
        Some(
            serde_json::from_value(json!({"ui":{"visibility":["model"]}}))
                .expect("static ChatGPT backend visibility"),
        )
    }

    fn behavior(&self) -> ToolBehavior {
        ToolBehavior::new(
            false,
            false,
            true,
            false,
            "Reads and mutates only local experimental ChatGPT backend session state used to bridge the current model turn to Paseo.",
        )
    }

    fn input_schema(&self) -> Value {
        match self {
            Self::Attach => empty_object_schema(),
            Self::Exchange => json!({
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9_-]{1,80}$"
                    },
                    "ack_command_seq": { "type": "integer", "minimum": 1 },
                    "outbound": {
                        "type": "object",
                        "properties": {
                            "kind": { "type": "string", "enum": ["result", "error"] },
                            "command_seq": { "type": "integer", "minimum": 1 },
                            "content": { "type": "string", "minLength": 1, "maxLength": 262144 }
                        },
                        "required": ["kind", "command_seq", "content"],
                        "additionalProperties": false
                    },
                    "wait": { "type": "boolean", "default": true }
                },
                "required": ["session_id"],
                "additionalProperties": false
            }),
        }
    }

    fn output_schema(&self) -> Option<Value> {
        Some(match self {
            Self::Attach => json!({
                "type":"object",
                "properties":{
                    "state":{"type":"string","enum":["attached"]},
                    "session_id":{"type":"string"},
                    "required_next_action":{"type":"string","enum":["chatgpt_backend_exchange"]},
                    "assistant_turn_may_end":{"type":"boolean","const":false}
                },
                "required":["state","session_id","required_next_action","assistant_turn_may_end"],
                "additionalProperties":false
            }),
            Self::Exchange => json!({
                "type":"object",
                "properties":{
                    "state":{"type":"string","enum":["command","idle","timeout","cancelled","closed"]},
                    "session_id":{"type":"string"},
                    "command":{"type":"object"},
                    "required_next_action":{"type":"string","enum":["process_command","chatgpt_backend_exchange","end_turn"]},
                    "assistant_turn_may_end":{"type":"boolean"}
                },
                "required":["state","session_id","required_next_action","assistant_turn_may_end"],
                "additionalProperties":false
            }),
        })
    }

    fn manages_model_output_budget(&self) -> bool {
        true
    }

    fn requires_project_root(&self) -> bool {
        false
    }

    async fn call(&self, _args: Value, _config: &AppConfig, _session: &SessionState) -> ToolResult {
        ToolResult::error("ChatGPT backend tools require ChatGPT conversation context.")
    }

    async fn call_with_context(
        &self,
        args: Value,
        config: &AppConfig,
        _session: &SessionState,
        context: &ToolRequestContext,
    ) -> ToolResult {
        if !config.experimental.chatgpt_bridge {
            return ToolResult::error("ChatGPT backend requires experimental.chatgptBridge.");
        }
        let worker = match worker_key(context) {
            Ok(worker) => worker,
            Err(error) => return *error,
        };
        let store = match ChatGptBackendStore::for_current_user(config) {
            Ok(store) => store,
            Err(error) => return ToolResult::error(error),
        };

        match self {
            Self::Attach => match store.attach(worker) {
                Ok(session) => ToolResult::text(format!(
                    "ChatGPT coding backend session {} is attached. Immediately call chatgpt_backend_exchange and keep this assistant turn alive until Paseo sends finish.",
                    session.id
                ))
                .with_structured(json!({
                    "state":"attached",
                    "session_id":session.id,
                    "required_next_action":"chatgpt_backend_exchange",
                    "assistant_turn_may_end":false
                })),
                Err(error) => ToolResult::error(error),
            },
            Self::Exchange => {
                let ExchangeArgs {
                    session_id,
                    ack_command_seq,
                    outbound,
                    wait,
                } = match parse_tool_args(args) {
                    Ok(args) => args,
                    Err(error) => return *error,
                };
                if let Err(error) = store.prepare_exchange(
                    worker,
                    &session_id,
                    ack_command_seq,
                    outbound,
                ) {
                    return ToolResult::error(error);
                }
                match store
                    .exchange_wait(
                        worker,
                        &session_id,
                        wait,
                        DEFAULT_WAIT_MS,
                        context.cancellation.clone(),
                    )
                    .await
                {
                    Ok(ExchangeOutcome::Command(command)) => command_result(&session_id, command),
                    Ok(ExchangeOutcome::Idle) => ToolResult::text(
                        "No Paseo control command is pending. Continue the active task if there is one; otherwise call chatgpt_backend_exchange with wait=true. Do not end the turn.",
                    )
                    .with_structured(json!({
                        "state":"idle","session_id":session_id,
                        "required_next_action":"chatgpt_backend_exchange",
                        "assistant_turn_may_end":false
                    })),
                    Ok(ExchangeOutcome::TimedOut) => ToolResult::text(
                        "Paseo has not sent another command yet. The backend session remains active. Call chatgpt_backend_exchange again with wait=true and do not end the turn.",
                    )
                    .with_structured(json!({
                        "state":"timeout","session_id":session_id,
                        "required_next_action":"chatgpt_backend_exchange",
                        "assistant_turn_may_end":false
                    })),
                    Ok(ExchangeOutcome::Cancelled) => ToolResult::text(
                        "The current wait was cancelled by the host, but the backend session remains open. Resume chatgpt_backend_exchange unless a higher-priority instruction requires otherwise.",
                    )
                    .with_structured(json!({
                        "state":"cancelled","session_id":session_id,
                        "required_next_action":"chatgpt_backend_exchange",
                        "assistant_turn_may_end":false
                    })),
                    Ok(ExchangeOutcome::Closed) => ToolResult::text(
                        "The ChatGPT backend session is closed. You may end the assistant turn.",
                    )
                    .with_structured(json!({
                        "state":"closed","session_id":session_id,
                        "required_next_action":"end_turn",
                        "assistant_turn_may_end":true
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
    fn backend_tools_are_model_visible_and_non_terminal_by_contract() {
        for tool in [ChatGptBackendTool::Attach, ChatGptBackendTool::Exchange] {
            let meta = tool.meta().unwrap();
            assert_eq!(meta["ui"]["visibility"], json!(["model"]));
        }
        let schema = ChatGptBackendTool::Attach.output_schema().unwrap();
        assert_eq!(
            schema["properties"]["assistant_turn_may_end"]["const"],
            json!(false)
        );
    }
}
