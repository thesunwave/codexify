use rmcp::model::{MetaObject, Resource, ResourceContents};
use serde_json::json;

pub const CHATGPT_BRIDGE_UI_URI: &str = "ui://codexify/chatgpt-bridge/v1/mcp-app.html";
pub const CHATGPT_BRIDGE_UI_MIME_TYPE: &str = "text/html;profile=mcp-app";
pub const CHATGPT_BRIDGE_UI_HTML: &str = include_str!("chatgpt_bridge_ui.html");

pub fn tool_meta() -> MetaObject {
    serde_json::from_value(json!({
        "ui": {
            "resourceUri": CHATGPT_BRIDGE_UI_URI,
            "visibility": ["model", "app"]
        },
        "ui/resourceUri": CHATGPT_BRIDGE_UI_URI,
        "openai/outputTemplate": CHATGPT_BRIDGE_UI_URI,
        "openai/widgetAccessible": true
    }))
    .expect("static ChatGPT bridge tool metadata")
}

fn resource_meta() -> MetaObject {
    serde_json::from_value(json!({
        "ui": {
            "prefersBorder": false,
            "csp": {
                "connectDomains": [],
                "resourceDomains": []
            }
        },
        "openai/widgetPrefersBorder": false,
        "openai/widgetCSP": {
            "connect_domains": [],
            "resource_domains": []
        },
        "openai/widgetDescription": "Experimental Codexify worker that claims locally queued bridge requests and dispatches them into this ChatGPT conversation. Keep this widget open while the worker is active."
    }))
    .expect("static ChatGPT bridge resource metadata")
}

pub fn resource() -> Resource {
    Resource::new(CHATGPT_BRIDGE_UI_URI, "codexify-chatgpt-bridge")
        .with_title("Codexify ChatGPT bridge worker")
        .with_description("Experimental reverse-RPC worker for locally queued Codexify requests")
        .with_mime_type(CHATGPT_BRIDGE_UI_MIME_TYPE)
        .with_size(CHATGPT_BRIDGE_UI_HTML.len() as u64)
        .with_meta(resource_meta())
}

pub fn contents_for_uri(uri: &str) -> Option<ResourceContents> {
    (uri == CHATGPT_BRIDGE_UI_URI).then(|| {
        ResourceContents::text(CHATGPT_BRIDGE_UI_HTML, uri)
            .with_mime_type(CHATGPT_BRIDGE_UI_MIME_TYPE)
            .with_meta(resource_meta())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widget_uses_standard_mcp_apps_bridge_with_chatgpt_fallbacks() {
        assert!(CHATGPT_BRIDGE_UI_HTML.contains("ui/initialize"));
        assert!(CHATGPT_BRIDGE_UI_HTML.contains("tools/call"));
        assert!(CHATGPT_BRIDGE_UI_HTML.contains("ui/message"));
        assert!(CHATGPT_BRIDGE_UI_HTML.contains("window.openai?.callTool"));
        assert!(CHATGPT_BRIDGE_UI_HTML.contains("window.openai?.sendFollowUpMessage"));
        assert!(CHATGPT_BRIDGE_UI_HTML.contains("chatgpt_bridge_next"));
        assert!(CHATGPT_BRIDGE_UI_HTML.contains("chatgpt_bridge_submit_result"));
    }

    #[test]
    fn resource_contract_links_the_worker_tool_to_the_widget() {
        let meta = tool_meta();
        assert_eq!(meta["ui"]["resourceUri"], json!(CHATGPT_BRIDGE_UI_URI));
        assert_eq!(meta["ui"]["visibility"], json!(["model", "app"]));
        assert_eq!(meta["openai/widgetAccessible"], json!(true));
        assert_eq!(resource().uri, CHATGPT_BRIDGE_UI_URI);
        assert!(contents_for_uri(CHATGPT_BRIDGE_UI_URI).is_some());
    }
}
