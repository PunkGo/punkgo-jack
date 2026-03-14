use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::{json, Value};

use super::{HookAdapter, IngestEvent};

pub struct CursorAdapter;

impl HookAdapter for CursorAdapter {
    fn transform(&self, raw: &Value) -> Result<IngestEvent> {
        let hook_event = raw
            .get("hook_event_name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");

        // Session lifecycle events.
        match hook_event {
            "sessionStart" | "sessionEnd" => {
                return Ok(IngestEvent {
                    actor_id: "cursor".into(),
                    target: format!("session:{hook_event}"),
                    event_type: snake_case(hook_event),
                    content: format!("Cursor {hook_event}"),
                    metadata: build_session_metadata(raw),
                    source: "cursor".into(),
                });
            }
            "beforeSubmitPrompt" => {
                let prompt_text = truncate(str_field(raw, "prompt"), 200);
                let mut meta = build_common_metadata(raw);
                if let Some(v) = raw.get("prompt") {
                    meta.insert("prompt".into(), v.clone());
                }
                return Ok(IngestEvent {
                    actor_id: "cursor".into(),
                    target: "user:prompt".into(),
                    event_type: "user_prompt".into(),
                    content: format!("User prompt: {prompt_text}"),
                    metadata: meta,
                    source: "cursor".into(),
                });
            }
            _ => {}
        }

        let tool_name = raw
            .get("tool_name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let tool_input = raw.get("tool_input").cloned().unwrap_or(json!({}));

        let mut event_type = map_tool_to_event_type(tool_name);
        if hook_event == "postToolUseFailure" {
            event_type = format!("{event_type}_failed");
        } else if hook_event == "preToolUse" {
            event_type = format!("{event_type}_pre");
        }

        Ok(IngestEvent {
            actor_id: "cursor".into(),
            target: derive_target(tool_name, &tool_input),
            event_type,
            content: derive_content(tool_name, &tool_input),
            metadata: build_tool_metadata(raw),
            source: "cursor".into(),
        })
    }
}

/// Map Cursor tool names to semantic event types.
/// Cursor uses different names than Claude Code:
///   Shell (not Bash), Write (covers Edit too), etc.
fn map_tool_to_event_type(tool_name: &str) -> String {
    match tool_name {
        "Shell" => "command_execution",
        "Read" => "file_read",
        "Write" => "file_write",
        "Grep" => "content_search",
        "Delete" => "file_delete",
        "Task" => "subagent_spawn",
        // Bash/Edit/Glob/WebFetch — Claude Code names, in case Cursor
        // passes them through from claude-user config hooks.
        "Bash" => "command_execution",
        "Edit" => "file_edit",
        "Glob" => "file_search",
        "WebFetch" => "web_request",
        "WebSearch" => "web_search",
        t if t.starts_with("MCP:") || t.starts_with("mcp__") => "mcp_tool_call",
        _ => "tool_call",
    }
    .into()
}

fn derive_target(tool_name: &str, tool_input: &Value) -> String {
    match tool_name {
        "Shell" | "Bash" => format!("bash:{}", truncate(str_field(tool_input, "command"), 100)),
        "Read" | "Write" | "Edit" => format!("file:{}", str_field(tool_input, "file_path")),
        "Grep" => format!("grep:{}", str_field(tool_input, "pattern")),
        "Glob" => format!("glob:{}", str_field(tool_input, "pattern")),
        "Delete" => format!("file:{}", str_field(tool_input, "file_path")),
        "Task" => format!("task:{}", str_field(tool_input, "subagent_type")),
        "WebFetch" => format!("url:{}", str_field(tool_input, "url")),
        "WebSearch" => format!("search:{}", str_field(tool_input, "query")),
        t if t.starts_with("MCP:") || t.starts_with("mcp__") => format!("mcp:{t}"),
        _ => format!("tool:{tool_name}"),
    }
}

fn derive_content(tool_name: &str, tool_input: &Value) -> String {
    match tool_name {
        "Shell" | "Bash" => format!(
            "Execute command: {}",
            truncate(str_field(tool_input, "command"), 200)
        ),
        "Write" => format!("Write file: {}", str_field(tool_input, "file_path")),
        "Edit" => format!("Edit file: {}", str_field(tool_input, "file_path")),
        "Read" => format!("Read file: {}", str_field(tool_input, "file_path")),
        "Grep" => format!("Grep pattern: {}", str_field(tool_input, "pattern")),
        "Glob" => format!("Glob pattern: {}", str_field(tool_input, "pattern")),
        "Delete" => format!("Delete file: {}", str_field(tool_input, "file_path")),
        "Task" => format!("Spawn subagent: {}", str_field(tool_input, "subagent_type")),
        "WebFetch" => format!("Fetch URL: {}", str_field(tool_input, "url")),
        "WebSearch" => format!("Web search: {}", str_field(tool_input, "query")),
        _ => format!("Tool call: {tool_name}"),
    }
}

/// Build metadata for tool events, including Cursor-specific fields.
fn build_tool_metadata(raw: &Value) -> BTreeMap<String, Value> {
    let mut meta = build_common_metadata(raw);

    // Tool-specific fields.
    for key in ["tool_use_id", "tool_name"] {
        if let Some(v) = raw.get(key) {
            meta.insert(key.into(), v.clone());
        }
    }

    // tool_input: externalize large fields to blob store.
    if let Some(v) = raw.get("tool_input") {
        match crate::blob::externalize_tool_input(v) {
            Ok((compacted, refs)) => {
                meta.insert("tool_input".into(), compacted);
                if !refs.is_empty() {
                    meta.insert("content_refs".into(), json!(refs));
                }
            }
            Err(_) => {
                meta.insert("tool_input".into(), v.clone());
            }
        }
    }

    // tool_output (Cursor uses tool_output, not tool_response).
    let tool_output = raw.get("tool_output").or_else(|| raw.get("tool_response"));
    if let Some(resp) = tool_output {
        let capture_mode =
            std::env::var("PUNKGO_CAPTURE_RESPONSE").unwrap_or_else(|_| "summary".to_string());
        match capture_mode.as_str() {
            "full" => match crate::blob::externalize_tool_input(resp) {
                Ok((compacted, refs)) => {
                    meta.insert("tool_response".into(), compacted);
                    if !refs.is_empty() {
                        if let Some(existing) = meta.get("content_refs").and_then(Value::as_array) {
                            let mut all: Vec<Value> = existing.clone();
                            all.extend(refs.into_iter().map(Value::String));
                            meta.insert("content_refs".into(), json!(all));
                        } else {
                            meta.insert("content_refs".into(), json!(refs));
                        }
                    }
                }
                Err(_) => {
                    meta.insert("tool_response".into(), resp.clone());
                }
            },
            "none" => {}
            _ => {
                // "summary" (default).
                meta.insert("has_response".into(), json!(true));
                if let Some(code) = resp.get("exit_code") {
                    meta.insert("exit_code".into(), code.clone());
                }
            }
        }
    }

    // Cursor-specific: duration (ms) for tool execution.
    if let Some(v) = raw.get("duration") {
        meta.insert("duration_ms".into(), v.clone());
    }

    meta
}

/// Common metadata fields present in all Cursor hook payloads.
fn build_common_metadata(raw: &Value) -> BTreeMap<String, Value> {
    let mut meta = BTreeMap::new();

    // Cursor uses conversation_id (Claude Code uses session_id).
    // Store both under session_id for consistency.
    if let Some(v) = raw.get("conversation_id").or_else(|| raw.get("session_id")) {
        meta.insert("session_id".into(), v.clone());
    }

    for (src, dst) in [
        ("hook_event_name", "hook_event"),
        ("cwd", "cwd"),
        ("cursor_version", "cursor_version"),
        ("model", "model"),
        ("user_email", "user_email"),
        ("generation_id", "generation_id"),
    ] {
        if let Some(v) = raw.get(src) {
            meta.insert(dst.into(), v.clone());
        }
    }

    // workspace_roots → first entry as workspace.
    if let Some(roots) = raw.get("workspace_roots").and_then(Value::as_array) {
        if let Some(first) = roots.first() {
            meta.insert("workspace".into(), first.clone());
        }
    }

    meta
}

fn build_session_metadata(raw: &Value) -> BTreeMap<String, Value> {
    let mut meta = build_common_metadata(raw);

    // sessionStart extras.
    if let Some(v) = raw.get("is_background_agent") {
        meta.insert("is_background_agent".into(), v.clone());
    }
    if let Some(v) = raw.get("composer_mode") {
        meta.insert("composer_mode".into(), v.clone());
    }

    // sessionEnd extras.
    if let Some(v) = raw.get("reason") {
        meta.insert("reason".into(), v.clone());
    }
    if let Some(v) = raw.get("duration_ms") {
        meta.insert("duration_ms".into(), v.clone());
    }
    if let Some(v) = raw.get("final_status") {
        meta.insert("final_status".into(), v.clone());
    }

    meta
}

fn str_field<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_cursor_post_tool_use() -> Value {
        json!({
            "conversation_id": "conv_abc123",
            "generation_id": "gen_001",
            "model": "claude-sonnet-4-6",
            "hook_event_name": "postToolUse",
            "cursor_version": "1.7.2",
            "workspace_roots": ["/home/user/project"],
            "user_email": "user@example.com",
            "tool_name": "Shell",
            "tool_input": {
                "command": "npm test"
            },
            "tool_output": "All 42 tests passed",
            "tool_use_id": "toolu_01ABC",
            "cwd": "/home/user/project",
            "duration": 1234
        })
    }

    #[test]
    fn transform_shell_post_tool_use() {
        let adapter = CursorAdapter;
        let event = adapter.transform(&sample_cursor_post_tool_use()).unwrap();

        assert_eq!(event.actor_id, "cursor");
        assert_eq!(event.source, "cursor");
        assert_eq!(event.event_type, "command_execution");
        assert_eq!(event.target, "bash:npm test");
        assert_eq!(event.content, "Execute command: npm test");
        assert_eq!(
            event.metadata.get("session_id"),
            Some(&json!("conv_abc123"))
        );
        assert_eq!(event.metadata.get("cursor_version"), Some(&json!("1.7.2")));
        assert_eq!(
            event.metadata.get("model"),
            Some(&json!("claude-sonnet-4-6"))
        );
        assert_eq!(event.metadata.get("duration_ms"), Some(&json!(1234)));
        assert_eq!(
            event.metadata.get("workspace"),
            Some(&json!("/home/user/project"))
        );
    }

    #[test]
    fn transform_pre_tool_use() {
        let adapter = CursorAdapter;
        let raw = json!({
            "hook_event_name": "preToolUse",
            "cursor_version": "1.7.2",
            "tool_name": "Read",
            "tool_input": { "file_path": "/src/main.rs" },
            "conversation_id": "conv_001",
            "cwd": "/project"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "file_read_pre");
        assert_eq!(event.target, "file:/src/main.rs");
    }

    #[test]
    fn transform_failure_event() {
        let adapter = CursorAdapter;
        let raw = json!({
            "hook_event_name": "postToolUseFailure",
            "cursor_version": "1.7.2",
            "tool_name": "Shell",
            "tool_input": { "command": "rm -rf /" },
            "error_message": "permission denied",
            "failure_type": "error",
            "conversation_id": "conv_001"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "command_execution_failed");
    }

    #[test]
    fn transform_session_start() {
        let adapter = CursorAdapter;
        let raw = json!({
            "hook_event_name": "sessionStart",
            "cursor_version": "1.7.2",
            "conversation_id": "conv_001",
            "session_id": "sess_001",
            "is_background_agent": false,
            "composer_mode": "agent"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "session_start");
        assert_eq!(event.content, "Cursor sessionStart");
        assert_eq!(event.metadata.get("composer_mode"), Some(&json!("agent")));
        assert_eq!(
            event.metadata.get("is_background_agent"),
            Some(&json!(false))
        );
    }

    #[test]
    fn transform_before_submit_prompt() {
        let adapter = CursorAdapter;
        let raw = json!({
            "hook_event_name": "beforeSubmitPrompt",
            "cursor_version": "1.7.2",
            "prompt": "fix the bug in main.rs",
            "conversation_id": "conv_001"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "user_prompt");
        assert_eq!(event.target, "user:prompt");
        assert!(event.content.contains("fix the bug"));
    }

    #[test]
    fn transform_write_tool() {
        let adapter = CursorAdapter;
        let raw = json!({
            "hook_event_name": "postToolUse",
            "cursor_version": "1.7.2",
            "tool_name": "Write",
            "tool_input": { "file_path": "/src/lib.rs", "content": "pub fn hello() {}" },
            "conversation_id": "conv_001"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "file_write");
        assert_eq!(event.target, "file:/src/lib.rs");
    }

    #[test]
    fn transform_delete_tool() {
        let adapter = CursorAdapter;
        let raw = json!({
            "hook_event_name": "postToolUse",
            "cursor_version": "1.7.2",
            "tool_name": "Delete",
            "tool_input": { "file_path": "/tmp/old.txt" },
            "conversation_id": "conv_001"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "file_delete");
        assert_eq!(event.target, "file:/tmp/old.txt");
    }

    #[test]
    fn transform_mcp_tool() {
        let adapter = CursorAdapter;
        let raw = json!({
            "hook_event_name": "postToolUse",
            "cursor_version": "1.7.2",
            "tool_name": "MCP:playwright_click",
            "tool_input": { "selector": "#btn" },
            "conversation_id": "conv_001"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "mcp_tool_call");
        assert_eq!(event.target, "mcp:MCP:playwright_click");
    }

    #[test]
    fn conversation_id_maps_to_session_id() {
        let adapter = CursorAdapter;
        let event = adapter.transform(&sample_cursor_post_tool_use()).unwrap();
        // Cursor's conversation_id should be stored as session_id for consistency.
        assert_eq!(
            event.metadata.get("session_id"),
            Some(&json!("conv_abc123"))
        );
    }
}
