use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::{json, Value};

use super::{HookAdapter, IngestEvent};

pub struct ClaudeCodeAdapter;

impl HookAdapter for ClaudeCodeAdapter {
    fn transform(&self, raw: &Value) -> Result<IngestEvent> {
        let hook_event = raw
            .get("hook_event_name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");

        // Non-tool events: session lifecycle, agent stop, subagent, notification.
        match hook_event {
            "SessionStart" | "SessionEnd" => {
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: format!("session:{hook_event}"),
                    event_type: snake_case(hook_event),
                    content: format!("Claude Code {hook_event}"),
                    metadata: build_session_metadata(raw),
                    source: "claude-code".into(),
                });
            }
            "Stop" => {
                let mut meta = build_session_metadata(raw);
                if let Some(v) = raw.get("last_assistant_message") {
                    let text = v.as_str().unwrap_or("");
                    let value = crate::blob::externalize_or_inline(
                        "last_assistant_message",
                        text,
                        max_inline_bytes(),
                    )
                    .unwrap_or_else(|_| json!({ "inline": text }));
                    meta.insert("last_assistant_message".into(), value);
                }
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: "session:Stop".into(),
                    event_type: "agent_stop".into(),
                    content: "Claude Code agent stopped".into(),
                    metadata: meta,
                    source: "claude-code".into(),
                });
            }
            "SubagentStart" | "SubagentStop" => {
                let mut meta = build_session_metadata(raw);
                for key in [
                    "agent_id",
                    "agent_type",
                    "agent_transcript_path",
                    "last_assistant_message",
                ] {
                    if let Some(v) = raw.get(key) {
                        meta.insert(key.into(), v.clone());
                    }
                }
                let agent_type = str_field(raw, "agent_type");
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: format!("subagent:{agent_type}"),
                    event_type: snake_case(hook_event),
                    content: format!("Claude Code {hook_event}: {agent_type}"),
                    metadata: meta,
                    source: "claude-code".into(),
                });
            }
            "Notification" => {
                let mut meta = build_session_metadata(raw);
                for key in ["notification_type", "message", "title"] {
                    if let Some(v) = raw.get(key) {
                        meta.insert(key.into(), v.clone());
                    }
                }
                let ntype = str_field(raw, "notification_type");
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: format!("notification:{ntype}"),
                    event_type: "notification".into(),
                    content: format!("Notification: {}", str_field(raw, "message")),
                    metadata: meta,
                    source: "claude-code".into(),
                });
            }
            // v0.6.0 Lane B — 5 additional hooks. Each captures the
            // PayloadShape-specific fields defensively: if a field the plan
            // expects is absent, the adapter still produces a valid event
            // with empty metadata (never panics on missing keys).
            "InstructionsLoaded" => {
                let mut meta = build_session_metadata(raw);
                for key in ["file_path", "memory_type", "load_reason", "source"] {
                    if let Some(v) = raw.get(key) {
                        meta.insert(key.into(), v.clone());
                    }
                }
                let memory_type = str_field(raw, "memory_type");
                let file_path = str_field(raw, "file_path");
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: format!("instructions:{memory_type}"),
                    event_type: "instructions_loaded".into(),
                    content: format!("Instructions loaded: {file_path}"),
                    metadata: meta,
                    source: "claude-code".into(),
                });
            }
            "PreCompact" => {
                let mut meta = build_session_metadata(raw);
                for key in ["trigger", "input_tokens", "total_tokens"] {
                    if let Some(v) = raw.get(key) {
                        meta.insert(key.into(), v.clone());
                    }
                }
                let trigger = str_field(raw, "trigger");
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: format!("compact:pre:{trigger}"),
                    event_type: "pre_compact".into(),
                    content: format!("Pre-compact ({trigger})"),
                    metadata: meta,
                    source: "claude-code".into(),
                });
            }
            "PostCompact" => {
                let mut meta = build_session_metadata(raw);
                for key in [
                    "trigger",
                    "input_tokens_before",
                    "input_tokens_after",
                    "summary",
                    "summary_tokens",
                ] {
                    if let Some(v) = raw.get(key) {
                        // Externalize summary body if large. All other keys
                        // are numeric / short strings and stay inline.
                        if key == "summary" {
                            if let Some(s) = v.as_str() {
                                let value = crate::blob::externalize_or_inline(
                                    "compact_summary",
                                    s,
                                    max_inline_bytes(),
                                )
                                .unwrap_or_else(|_| json!({ "inline": s }));
                                meta.insert("compact_summary".into(), value);
                                continue;
                            }
                        }
                        meta.insert(key.into(), v.clone());
                    }
                }
                let trigger = str_field(raw, "trigger");
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: format!("compact:post:{trigger}"),
                    event_type: "post_compact".into(),
                    content: format!("Post-compact ({trigger})"),
                    metadata: meta,
                    source: "claude-code".into(),
                });
            }
            "StopFailure" => {
                let mut meta = build_session_metadata(raw);
                for key in [
                    "error_type",
                    "error_message",
                    "last_api_request_id",
                    "last_model",
                ] {
                    if let Some(v) = raw.get(key) {
                        meta.insert(key.into(), v.clone());
                    }
                }
                let err_type = str_field(raw, "error_type");
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: format!("session:StopFailure:{err_type}"),
                    event_type: "stop_failure".into(),
                    content: format!("Stop failure: {err_type}"),
                    metadata: meta,
                    source: "claude-code".into(),
                });
            }
            "PermissionDenied" => {
                let mut meta = build_session_metadata(raw);
                for key in ["tool_name", "action", "reason", "requested_by"] {
                    if let Some(v) = raw.get(key) {
                        meta.insert(key.into(), v.clone());
                    }
                }
                let tool_name = str_field(raw, "tool_name");
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: format!("permission_denied:{tool_name}"),
                    event_type: "permission_denied".into(),
                    content: format!("Permission denied for tool: {tool_name}"),
                    metadata: meta,
                    source: "claude-code".into(),
                });
            }
            "UserPromptSubmit" => {
                let mut meta = build_prompt_metadata(raw);
                let image_count = meta.get("image_count").and_then(Value::as_u64).unwrap_or(0);
                let full_prompt = str_field(raw, "prompt");

                // Full prompt always preserved in metadata via externalize_or_inline.
                // Small prompts stay inline, large prompts go to blob store —
                // no byte limit at adapter layer.
                if !full_prompt.is_empty() {
                    let value = crate::blob::externalize_or_inline(
                        "prompt_body",
                        full_prompt,
                        max_inline_bytes(),
                    )
                    .unwrap_or_else(|_| json!({ "inline": full_prompt }));
                    meta.insert("prompt_body".into(), value);
                }

                // Display label: short human preview (≤200 chars) of the prompt.
                // This is the UI `event.content` string, NOT the data store.
                let display_preview = display_preview(full_prompt, 200);
                let content = if image_count > 0 {
                    format!(
                        "User prompt (+{} image{}): {}",
                        image_count,
                        if image_count > 1 { "s" } else { "" },
                        display_preview
                    )
                } else {
                    format!("User prompt: {}", display_preview)
                };
                return Ok(IngestEvent {
                    actor_id: "claude-code".into(),
                    target: "user:prompt".into(),
                    event_type: "user_prompt".into(),
                    content,
                    metadata: meta,
                    source: "claude-code".into(),
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
        // Mark failures and pre-execution events explicitly.
        if hook_event == "PostToolUseFailure" {
            event_type = format!("{event_type}_failed");
        } else if hook_event == "PreToolUse" {
            event_type = format!("{event_type}_pre");
        }

        Ok(IngestEvent {
            actor_id: "claude-code".into(),
            target: derive_target(tool_name, &tool_input),
            event_type,
            content: derive_content(tool_name, &tool_input),
            metadata: build_metadata(raw),
            source: "claude-code".into(),
        })
    }
}

fn map_tool_to_event_type(tool_name: &str) -> String {
    match tool_name {
        "Bash" => "command_execution",
        "Read" => "file_read",
        "Write" => "file_write",
        "Edit" => "file_edit",
        "Glob" => "file_search",
        "Grep" => "content_search",
        "WebFetch" => "web_request",
        "WebSearch" => "web_search",
        "Task" => "subagent_spawn",
        t if t.starts_with("mcp__") => "mcp_tool_call",
        _ => "tool_call",
    }
    .into()
}

fn derive_target(tool_name: &str, tool_input: &Value) -> String {
    match tool_name {
        "Bash" => format!("bash:{}", truncate(str_field(tool_input, "command"), 100)),
        "Read" | "Write" | "Edit" => format!("file:{}", str_field(tool_input, "file_path")),
        "Glob" => format!("glob:{}", str_field(tool_input, "pattern")),
        "Grep" => format!("grep:{}", str_field(tool_input, "pattern")),
        "WebFetch" => format!("url:{}", str_field(tool_input, "url")),
        "WebSearch" => format!("search:{}", str_field(tool_input, "query")),
        "Task" => format!("task:{}", str_field(tool_input, "subagent_type")),
        t if t.starts_with("mcp__") => format!("mcp:{t}"),
        _ => format!("tool:{tool_name}"),
    }
}

fn derive_content(tool_name: &str, tool_input: &Value) -> String {
    match tool_name {
        // No byte limit — the command is already durably stored intact in
        // meta["tool_input"]["command"] (NEVER_EXTERNALIZE). Display the full
        // command here; downstream UIs can clamp at render time.
        "Bash" => format!("Execute command: {}", str_field(tool_input, "command")),
        "Write" => format!("Write file: {}", str_field(tool_input, "file_path")),
        "Edit" => format!("Edit file: {}", str_field(tool_input, "file_path")),
        "Read" => format!("Read file: {}", str_field(tool_input, "file_path")),
        "Glob" => format!("Glob pattern: {}", str_field(tool_input, "pattern")),
        "Grep" => format!("Grep pattern: {}", str_field(tool_input, "pattern")),
        "WebFetch" => format!("Fetch URL: {}", str_field(tool_input, "url")),
        "WebSearch" => format!("Web search: {}", str_field(tool_input, "query")),
        "Task" => format!("Spawn subagent: {}", str_field(tool_input, "subagent_type")),
        _ => format!("Tool call: {tool_name}"),
    }
}

fn capture_response_mode() -> String {
    std::env::var("PUNKGO_CAPTURE_RESPONSE").unwrap_or_else(|_| "summary".to_string())
}

/// Max bytes for a content field to stay inline before being offloaded to the
/// blob store. Adapter call sites for the 3 legacy truncation points
/// (prompt, last_assistant_message, bash command display) read this instead
/// of hardcoding a limit. Default 4096; override via env for emergencies.
fn max_inline_bytes() -> usize {
    std::env::var("PUNKGO_MAX_INLINE_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4096)
}

/// Display-only preview: truncate-to-char-boundary for the UI `event.content`
/// label. Loses bytes by design — the full value is stored in metadata via
/// `externalize_or_inline`. Never use this for data storage.
fn display_preview(s: &str, max: usize) -> &str {
    truncate(s, max)
}

fn build_metadata(raw: &Value) -> BTreeMap<String, Value> {
    build_metadata_with_mode(raw, &capture_response_mode())
}

fn build_metadata_with_mode(raw: &Value, capture_mode: &str) -> BTreeMap<String, Value> {
    let mut meta = BTreeMap::new();

    // Always-preserved fields.
    for (src, dst) in [
        ("session_id", "session_id"),
        ("cwd", "cwd"),
        ("tool_use_id", "tool_use_id"),
        ("hook_event_name", "hook_event"),
        ("tool_name", "tool_name"),
    ] {
        if let Some(v) = raw.get(src) {
            meta.insert(dst.into(), v.clone());
        }
    }

    // tool_input: externalize large fields to blob store, keep hashes inline.
    if let Some(v) = raw.get("tool_input") {
        match crate::blob::externalize_tool_input(v) {
            Ok((compacted, refs)) => {
                meta.insert("tool_input".into(), compacted);
                if !refs.is_empty() {
                    meta.insert("content_refs".into(), json!(refs));
                }
            }
            Err(_) => {
                // Blob store failure is non-fatal — fall back to inline storage.
                meta.insert("tool_input".into(), v.clone());
            }
        }
    }

    // tool_response: capture level controlled by capture_mode.
    // Values: "full" (store entire response), "summary" (default: has_response + exit_code),
    //         "none" (store nothing).
    if let Some(resp) = raw.get("tool_response") {
        match capture_mode {
            "full" => {
                // Externalize large response fields (stdout, stderr, etc.)
                match crate::blob::externalize_tool_input(resp) {
                    Ok((compacted, refs)) => {
                        meta.insert("tool_response".into(), compacted);
                        if !refs.is_empty() {
                            if let Some(existing) =
                                meta.get("content_refs").and_then(Value::as_array)
                            {
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
                }
            }
            "none" => {}
            _ => {
                // "summary" (default): just record existence and exit_code.
                meta.insert("has_response".into(), json!(true));
                if let Some(code) = resp.get("exit_code") {
                    meta.insert("exit_code".into(), code.clone());
                }
            }
        }
    }

    meta
}

fn build_prompt_metadata(raw: &Value) -> BTreeMap<String, Value> {
    let mut meta = BTreeMap::new();
    for key in ["session_id", "cwd", "hook_event_name"] {
        if let Some(v) = raw.get(key) {
            meta.insert(key.into(), v.clone());
        }
    }
    // Capture the prompt text.
    if let Some(v) = raw.get("prompt") {
        meta.insert("prompt".into(), v.clone());
    }
    // Detect images from transcript (hook stdin doesn't include image data,
    // but transcript_path points to a JSONL that contains base64 image blocks).
    if let Some(images) = detect_prompt_images(raw) {
        if !images.is_empty() {
            let count = images.len();
            meta.insert("image_count".into(), json!(count));
            meta.insert("images".into(), json!(images));
        }
    }
    meta
}

/// Detect images in the current user prompt by scanning the transcript.
///
/// Strategy: UserPromptSubmit fires right when the user submits. The current
/// prompt is therefore the **last** user entry in the transcript (excluding
/// tool_result entries which are also type "user"). We read the transcript
/// from the end, find the last user entry with `imagePasteIds`, and extract
/// image metadata via string scanning (no full JSON parse of multi-MB base64).
fn detect_prompt_images(raw: &Value) -> Option<Vec<Value>> {
    let transcript_path = raw.get("transcript_path")?.as_str()?;
    let path = std::path::Path::new(transcript_path);
    if !path.exists() {
        return None;
    }

    let file_len = std::fs::metadata(path).ok()?.len();

    // The last user entry is near the end of the file. If it has images, it can
    // be huge (multi-MB base64), so we try progressively larger tail reads.
    use std::io::{BufRead, BufReader, Seek, SeekFrom};
    let chunk_sizes: &[u64] = &[500_000, 5_000_000, 50_000_000];

    for &chunk in chunk_sizes {
        let file = std::fs::File::open(path).ok()?;
        let mut reader = BufReader::new(file);

        if chunk >= file_len {
            reader.seek(SeekFrom::Start(0)).ok()?;
        } else {
            reader.seek(SeekFrom::End(-(chunk as i64))).ok()?;
            let mut discard = String::new();
            let _ = reader.read_line(&mut discard);
        }

        // Find the LAST user entry that has imagePasteIds.
        // Since UserPromptSubmit fires on submit, the current prompt is the
        // last user entry in the transcript. If it has images, imagePasteIds
        // will be present. We keep the last match (scanning forward = chronological).
        let mut last_image_user_line: Option<String> = None;
        // Also track the very last user entry (with or without images) to know
        // if the current prompt is the one with images.
        let mut last_user_uuid: Option<String> = None;

        for line_result in reader.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(_) => continue,
            };

            let is_user = line.contains("\"type\":\"user\"") || line.contains("\"type\": \"user\"");
            if !is_user {
                continue;
            }

            // Skip tool_result entries (also type "user" in transcript).
            // Real prompt entries have "uuid" and "timestamp"; tool_result
            // entries have "tool_use_id" instead.
            if line.contains("\"tool_use_id\"") && !line.contains("\"uuid\"") {
                continue;
            }

            // Track this as the latest user prompt entry.
            if let Some(uuid) = extract_json_string_value(&line, "uuid") {
                last_user_uuid = Some(uuid);
            }

            if line.contains("\"imagePasteIds\"") {
                last_image_user_line = Some(line);
            }
        }

        // Only report images if the last user entry (current prompt) IS the
        // one with images. If a later text-only prompt came after the image
        // prompt, don't attribute images to the current prompt.
        if let (Some(ref img_line), Some(ref last_uuid)) = (&last_image_user_line, &last_user_uuid)
        {
            if let Some(img_uuid) = extract_json_string_value(img_line, "uuid") {
                if img_uuid == *last_uuid {
                    let images = extract_image_metadata_from_line(img_line);
                    if !images.is_empty() {
                        return Some(images);
                    }
                }
            }
        }

        if chunk >= file_len {
            break;
        }
    }

    None
}

/// Extract image metadata from a transcript line using string scanning.
/// Avoids full JSON parse of potentially multi-MB lines.
fn extract_image_metadata_from_line(line: &str) -> Vec<Value> {
    let mut images = Vec::new();

    // Find each image block by locating `"type":"image"` or `"type": "image"`.
    // For each one, look backwards/forwards for `"media_type"` and measure
    // the `"data"` field length.
    let search_patterns = ["\"type\":\"image\"", "\"type\": \"image\""];

    let mut search_start = 0;
    loop {
        let pos = search_patterns
            .iter()
            .filter_map(|pat| line[search_start..].find(pat).map(|p| p + search_start))
            .min();

        let Some(img_pos) = pos else { break };
        search_start = img_pos + 10;

        // Find the enclosing source object by looking for "media_type" near this position.
        // Scan a window around the image marker.
        let window_start = img_pos.saturating_sub(500);
        let window_end = (img_pos + 1000).min(line.len());
        let window = &line[window_start..window_end];

        let media_type = extract_json_string_value(window, "media_type")
            .unwrap_or_else(|| "unknown".to_string());

        // Find the base64 data field. The "data" value starts after `"data":"` and
        // can be enormous. We just need its length, so find start and end quotes.
        let data_len = if let Some(data_start) = find_data_field_start(line, img_pos) {
            measure_string_length(line, data_start)
        } else {
            0
        };

        let byte_size = data_len * 3 / 4;
        images.push(json!({
            "media_type": media_type,
            "base64_len": data_len,
            "approx_bytes": byte_size,
        }));
    }

    images
}

/// Extract a JSON string value by key from a text window. E.g. for key="media_type",
/// finds `"media_type":"image/jpeg"` and returns `image/jpeg`.
fn extract_json_string_value(text: &str, key: &str) -> Option<String> {
    let patterns = [format!("\"{}\":\"", key), format!("\"{}\": \"", key)];
    for pat in &patterns {
        if let Some(idx) = text.find(pat.as_str()) {
            let val_start = idx + pat.len();
            if let Some(val_end) = text[val_start..].find('"') {
                return Some(text[val_start..val_start + val_end].to_string());
            }
        }
    }
    None
}

/// Find the start of a `"data":"<base64>"` string value near an image block position.
/// Returns the byte offset of the first character of the base64 content.
fn find_data_field_start(line: &str, near_pos: usize) -> Option<usize> {
    // The "data" field is in the "source" object near the image type marker.
    // Search forward from the image position for `"data":"`.
    let search = &line[near_pos..];
    let patterns = ["\"data\":\"", "\"data\": \""];
    for pat in patterns {
        if let Some(idx) = search.find(pat) {
            return Some(near_pos + idx + pat.len());
        }
    }
    None
}

/// Measure the length of a JSON string value starting at `start` (first char
/// after the opening quote). Scans forward to find the closing unescaped quote.
/// For base64 data this is efficient: base64 never contains backslashes.
fn measure_string_length(line: &str, start: usize) -> usize {
    let bytes = line.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            return i - start;
        }
        if bytes[i] == b'\\' {
            i += 2; // skip escape sequence
        } else {
            i += 1;
        }
    }
    0 // unterminated string
}

fn build_session_metadata(raw: &Value) -> BTreeMap<String, Value> {
    let mut meta = BTreeMap::new();
    for key in ["session_id", "cwd", "hook_event_name"] {
        if let Some(v) = raw.get(key) {
            meta.insert(key.into(), v.clone());
        }
    }
    meta
}

/// Extract a string field from a JSON value, returning "" if missing.
fn str_field<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// Truncate a string to at most `max` bytes at a char boundary.
fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    // Find the last char boundary at or before `max`.
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Convert PascalCase to snake_case (simple: just lowercase with _ before uppercase).
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

    fn sample_post_tool_use() -> Value {
        json!({
            "session_id": "ses_abc123",
            "transcript_path": "/tmp/transcript.jsonl",
            "cwd": "/home/user/project",
            "permission_mode": "default",
            "hook_event_name": "PostToolUse",
            "tool_name": "Bash",
            "tool_input": {
                "command": "npm test",
                "description": "Run test suite",
                "timeout": 120000
            },
            "tool_response": {
                "stdout": "All 42 tests passed",
                "exit_code": 0
            },
            "tool_use_id": "toolu_01ABC123"
        })
    }

    #[test]
    fn transform_bash_post_tool_use() {
        let adapter = ClaudeCodeAdapter;
        let event = adapter.transform(&sample_post_tool_use()).unwrap();

        assert_eq!(event.actor_id, "claude-code");
        assert_eq!(event.source, "claude-code");
        assert_eq!(event.event_type, "command_execution");
        assert_eq!(event.target, "bash:npm test");
        assert_eq!(event.content, "Execute command: npm test");
        assert_eq!(event.metadata.get("session_id"), Some(&json!("ses_abc123")));
        assert_eq!(event.metadata.get("exit_code"), Some(&json!(0)));
        assert_eq!(event.metadata.get("has_response"), Some(&json!(true)));
    }

    #[test]
    fn transform_file_write() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "PostToolUse",
            "tool_name": "Write",
            "tool_input": { "file_path": "/src/main.rs", "content": "fn main() {}" },
            "session_id": "ses_001"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "file_write");
        assert_eq!(event.target, "file:/src/main.rs");
        assert_eq!(event.content, "Write file: /src/main.rs");
    }

    #[test]
    fn transform_failure_event() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "PostToolUseFailure",
            "tool_name": "Bash",
            "tool_input": { "command": "rm -rf /" },
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "command_execution_failed");
    }

    #[test]
    fn transform_session_start() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "SessionStart",
            "session_id": "ses_xyz",
            "cwd": "/projects/foo"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "session_start");
        assert_eq!(event.target, "session:SessionStart");
        assert_eq!(event.content, "Claude Code SessionStart");
    }

    #[test]
    fn transform_mcp_tool() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "PostToolUse",
            "tool_name": "mcp__github__create_issue",
            "tool_input": { "title": "bug" }
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "mcp_tool_call");
        assert_eq!(event.target, "mcp:mcp__github__create_issue");
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("hello world", 5), "hello");
        assert_eq!(truncate("hi", 100), "hi");
        // Multi-byte: "你好" is 6 bytes. Truncate at 4 should not split.
        let s = "你好";
        let t = truncate(s, 4);
        assert_eq!(t, "你"); // 3 bytes, next char starts at 3
    }

    #[test]
    fn snake_case_converts_pascal() {
        assert_eq!(snake_case("SessionStart"), "session_start");
        assert_eq!(snake_case("PostToolUse"), "post_tool_use");
        assert_eq!(snake_case("SessionEnd"), "session_end");
    }

    #[test]
    fn transform_pre_tool_use() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": { "command": "cargo build" }
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "command_execution_pre");
        assert_eq!(event.target, "bash:cargo build");
    }

    #[test]
    fn transform_user_prompt_submit() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "UserPromptSubmit",
            "session_id": "ses_xyz",
            "cwd": "/projects/foo",
            "prompt": "Fix the login bug"
        });
        let event = adapter.transform(&raw).unwrap();

        assert_eq!(event.event_type, "user_prompt");
        assert_eq!(event.target, "user:prompt");
        assert_eq!(event.content, "User prompt: Fix the login bug");
        assert_eq!(
            event.metadata.get("prompt"),
            Some(&json!("Fix the login bug"))
        );
    }

    #[test]
    fn response_capture_full_mode() {
        let raw = sample_post_tool_use();
        let meta = build_metadata_with_mode(&raw, "full");
        assert!(meta.contains_key("tool_response"));
        assert!(!meta.contains_key("has_response"));
    }

    #[test]
    fn response_capture_none_mode() {
        let raw = sample_post_tool_use();
        let meta = build_metadata_with_mode(&raw, "none");
        assert!(!meta.contains_key("tool_response"));
        assert!(!meta.contains_key("has_response"));
        assert!(!meta.contains_key("exit_code"));
    }

    #[test]
    fn response_capture_summary_mode() {
        let raw = sample_post_tool_use();
        let meta = build_metadata_with_mode(&raw, "summary");
        assert!(!meta.contains_key("tool_response"));
        assert_eq!(meta.get("has_response"), Some(&json!(true)));
        assert_eq!(meta.get("exit_code"), Some(&json!(0)));
    }

    #[test]
    fn transform_stop() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "Stop",
            "session_id": "ses_xyz",
            "last_assistant_message": "Done! All tests pass."
        });
        let event = adapter.transform(&raw).unwrap();
        assert_eq!(event.event_type, "agent_stop");
        assert_eq!(event.target, "session:Stop");
        assert!(event.metadata.contains_key("last_assistant_message"));
    }

    #[test]
    fn transform_subagent_stop() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "SubagentStop",
            "session_id": "ses_xyz",
            "agent_id": "agent_001",
            "agent_type": "code-reviewer"
        });
        let event = adapter.transform(&raw).unwrap();
        assert_eq!(event.event_type, "subagent_stop");
        assert_eq!(event.target, "subagent:code-reviewer");
    }

    // ------------------------------------------------------------------
    // Regression tests for v0.6.0 truncation removal (IRON RULE §6.4).
    // Three mandatory cases — no skipping.
    // ------------------------------------------------------------------

    mod truncation {
        use super::super::*;
        use serde_json::{json, Value};

        /// Per-test scoped PUNKGO_DATA_DIR. Sets the env var for the lifetime
        /// of the TempDir, enabling externalize() filesystem writes. Tests
        /// that run in parallel within the same process share process env,
        /// so we serialize on the crate-wide `session::PUNKGO_DATA_DIR_LOCK`
        /// which is also acquired by any other test that reads the env var.
        fn with_temp_data_dir<F, R>(f: F) -> R
        where
            F: FnOnce() -> R,
        {
            let _guard = crate::session::PUNKGO_DATA_DIR_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let tmp = tempfile::TempDir::new().expect("tempdir");
            let prev = std::env::var_os("PUNKGO_DATA_DIR");
            std::env::set_var("PUNKGO_DATA_DIR", tmp.path());
            let result = f();
            match prev {
                Some(v) => std::env::set_var("PUNKGO_DATA_DIR", v),
                None => std::env::remove_var("PUNKGO_DATA_DIR"),
            }
            result
        }

        /// Resolve a `{"inline": ...}` or `{"blob_hash": ..., ...}` value back
        /// to its original content. For blob refs, reads from the blob store.
        fn resolve_externalized(value: &Value) -> String {
            if let Some(inline) = value.get("inline").and_then(Value::as_str) {
                return inline.to_string();
            }
            if let Some(hash_ref) = value.get("blob_hash").and_then(Value::as_str) {
                let hex = hash_ref.strip_prefix("sha256:").unwrap_or(hash_ref);
                let path = crate::blob::blob_dir().unwrap().join(hex);
                return std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("blob read {}: {e}", path.display()));
            }
            panic!("value is neither inline nor blob_hash: {value}");
        }

        fn distinctive_pattern(len: usize) -> String {
            (0..len).map(|i| (b'a' + (i % 26) as u8) as char).collect()
        }

        #[test]
        fn large_prompt_preserved_in_metadata() {
            with_temp_data_dir(|| {
                let prompt = distinctive_pattern(10_240);
                assert_eq!(prompt.len(), 10_240);

                let adapter = ClaudeCodeAdapter;
                let raw = json!({
                    "hook_event_name": "UserPromptSubmit",
                    "session_id": "ses_large",
                    "cwd": "/tmp",
                    "prompt": prompt.clone()
                });
                let event = adapter.transform(&raw).unwrap();

                // prompt_body is the durable data store. Must round-trip
                // byte-for-byte regardless of inline vs blob path.
                let stored = event
                    .metadata
                    .get("prompt_body")
                    .expect("prompt_body must be in metadata");
                let recovered = resolve_externalized(stored);
                assert_eq!(
                    recovered.len(),
                    prompt.len(),
                    "length mismatch after round-trip"
                );
                assert_eq!(recovered, prompt, "byte-for-byte preservation failed");

                // The display label is allowed to be short — it's UI-only.
                assert!(event.content.starts_with("User prompt: "));
            });
        }

        #[test]
        fn large_assistant_message_preserved_in_metadata() {
            with_temp_data_dir(|| {
                let msg = distinctive_pattern(10_240);
                assert_eq!(msg.len(), 10_240);

                let adapter = ClaudeCodeAdapter;
                let raw = json!({
                    "hook_event_name": "Stop",
                    "session_id": "ses_large_stop",
                    "last_assistant_message": msg.clone()
                });
                let event = adapter.transform(&raw).unwrap();

                let stored = event
                    .metadata
                    .get("last_assistant_message")
                    .expect("last_assistant_message must be in metadata");
                let recovered = resolve_externalized(stored);
                assert_eq!(
                    recovered.len(),
                    msg.len(),
                    "length mismatch after round-trip"
                );
                assert_eq!(recovered, msg, "byte-for-byte preservation failed");
            });
        }

        /// For each of the 10 pre-existing hook event names, run the adapter
        /// with SMALL inputs (< 1 KB) and assert the v0.5.4-compatible
        /// event shape and metadata keys are unchanged by the truncation-
        /// removal work. Purpose: guarantee we didn't break the small-input
        /// happy path.
        #[test]
        fn existing_hook_paths_identical_to_v054() {
            with_temp_data_dir(|| {
                let adapter = ClaudeCodeAdapter;
                let small_prompt = "Fix the login bug";
                let small_msg = "Done! All tests pass.";

                struct Case {
                    hook: &'static str,
                    raw: Value,
                    expected_event_type: &'static str,
                    expected_target: &'static str,
                }

                let cases = vec![
                    Case {
                        hook: "SessionStart",
                        raw: json!({
                            "hook_event_name": "SessionStart",
                            "session_id": "ses_01",
                            "cwd": "/p"
                        }),
                        expected_event_type: "session_start",
                        expected_target: "session:SessionStart",
                    },
                    Case {
                        hook: "SessionEnd",
                        raw: json!({
                            "hook_event_name": "SessionEnd",
                            "session_id": "ses_01",
                            "cwd": "/p"
                        }),
                        expected_event_type: "session_end",
                        expected_target: "session:SessionEnd",
                    },
                    Case {
                        hook: "Stop",
                        raw: json!({
                            "hook_event_name": "Stop",
                            "session_id": "ses_01",
                            "cwd": "/p",
                            "last_assistant_message": small_msg
                        }),
                        expected_event_type: "agent_stop",
                        expected_target: "session:Stop",
                    },
                    Case {
                        hook: "SubagentStart",
                        raw: json!({
                            "hook_event_name": "SubagentStart",
                            "session_id": "ses_01",
                            "cwd": "/p",
                            "agent_type": "code-reviewer"
                        }),
                        expected_event_type: "subagent_start",
                        expected_target: "subagent:code-reviewer",
                    },
                    Case {
                        hook: "SubagentStop",
                        raw: json!({
                            "hook_event_name": "SubagentStop",
                            "session_id": "ses_01",
                            "cwd": "/p",
                            "agent_type": "code-reviewer"
                        }),
                        expected_event_type: "subagent_stop",
                        expected_target: "subagent:code-reviewer",
                    },
                    Case {
                        hook: "Notification",
                        raw: json!({
                            "hook_event_name": "Notification",
                            "session_id": "ses_01",
                            "cwd": "/p",
                            "notification_type": "permission_prompt",
                            "message": "Allow?",
                            "title": "Permission"
                        }),
                        expected_event_type: "notification",
                        expected_target: "notification:permission_prompt",
                    },
                    Case {
                        hook: "UserPromptSubmit",
                        raw: json!({
                            "hook_event_name": "UserPromptSubmit",
                            "session_id": "ses_01",
                            "cwd": "/p",
                            "prompt": small_prompt
                        }),
                        expected_event_type: "user_prompt",
                        expected_target: "user:prompt",
                    },
                    Case {
                        hook: "PreToolUse",
                        raw: json!({
                            "hook_event_name": "PreToolUse",
                            "session_id": "ses_01",
                            "cwd": "/p",
                            "tool_name": "Bash",
                            "tool_input": { "command": "ls" }
                        }),
                        expected_event_type: "command_execution_pre",
                        expected_target: "bash:ls",
                    },
                    Case {
                        hook: "PostToolUse",
                        raw: json!({
                            "hook_event_name": "PostToolUse",
                            "session_id": "ses_01",
                            "cwd": "/p",
                            "tool_name": "Bash",
                            "tool_input": { "command": "ls" }
                        }),
                        expected_event_type: "command_execution",
                        expected_target: "bash:ls",
                    },
                    Case {
                        hook: "PostToolUseFailure",
                        raw: json!({
                            "hook_event_name": "PostToolUseFailure",
                            "session_id": "ses_01",
                            "cwd": "/p",
                            "tool_name": "Bash",
                            "tool_input": { "command": "ls" }
                        }),
                        expected_event_type: "command_execution_failed",
                        expected_target: "bash:ls",
                    },
                ];

                for case in &cases {
                    let event = adapter.transform(&case.raw).unwrap_or_else(|e| {
                        panic!("hook {} failed: {e}", case.hook);
                    });
                    assert_eq!(
                        event.event_type, case.expected_event_type,
                        "hook {} event_type",
                        case.hook
                    );
                    assert_eq!(
                        event.target, case.expected_target,
                        "hook {} target",
                        case.hook
                    );
                    assert_eq!(event.actor_id, "claude-code", "hook {} actor_id", case.hook);
                    assert_eq!(event.source, "claude-code", "hook {} source", case.hook);
                    assert_eq!(
                        event.metadata.get("session_id"),
                        Some(&json!("ses_01")),
                        "hook {} metadata.session_id",
                        case.hook
                    );
                    assert_eq!(
                        event.metadata.get("cwd"),
                        Some(&json!("/p")),
                        "hook {} metadata.cwd",
                        case.hook
                    );
                    // hook_event is the v0.5.4 compat key (renamed from hook_event_name).
                    // Session-style events (SessionStart/End/Stop/Subagent*/Notification/
                    // UserPromptSubmit) store the raw name under "hook_event_name";
                    // tool-use events store it under "hook_event". Assert either form.
                    let hook_name_ok = event
                        .metadata
                        .get("hook_event")
                        .or_else(|| event.metadata.get("hook_event_name"))
                        .and_then(Value::as_str)
                        == Some(case.hook);
                    assert!(
                        hook_name_ok,
                        "hook {} missing hook_event metadata key",
                        case.hook
                    );
                }
            });
        }
    }

    #[test]
    fn transform_notification() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "Notification",
            "session_id": "ses_xyz",
            "notification_type": "permission_prompt",
            "message": "Claude wants to run rm -rf",
            "title": "Permission required"
        });
        let event = adapter.transform(&raw).unwrap();
        assert_eq!(event.event_type, "notification");
        assert_eq!(event.target, "notification:permission_prompt");
        assert!(event.content.contains("Claude wants to run"));
    }

    // ---- v0.6.0 Lane B: 5 new hook adapter tests ----

    #[test]
    fn transform_instructions_loaded() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "InstructionsLoaded",
            "session_id": "ses_il",
            "cwd": "/work/project",
            "file_path": "/work/project/CLAUDE.md",
            "memory_type": "project",
            "load_reason": "session_start"
        });
        let event = adapter.transform(&raw).unwrap();
        assert_eq!(event.event_type, "instructions_loaded");
        assert_eq!(event.target, "instructions:project");
        assert!(event.content.contains("CLAUDE.md"));
        assert_eq!(
            event.metadata.get("file_path"),
            Some(&json!("/work/project/CLAUDE.md"))
        );
        assert_eq!(event.metadata.get("memory_type"), Some(&json!("project")));
        assert_eq!(
            event.metadata.get("load_reason"),
            Some(&json!("session_start"))
        );
    }

    #[test]
    fn transform_pre_compact() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "PreCompact",
            "session_id": "ses_pc",
            "trigger": "auto",
            "input_tokens": 180_000,
            "total_tokens": 195_000
        });
        let event = adapter.transform(&raw).unwrap();
        assert_eq!(event.event_type, "pre_compact");
        assert_eq!(event.target, "compact:pre:auto");
        assert_eq!(event.metadata.get("trigger"), Some(&json!("auto")));
        assert_eq!(event.metadata.get("input_tokens"), Some(&json!(180_000)));
    }

    #[test]
    fn transform_post_compact_small_summary() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "PostCompact",
            "session_id": "ses_pc",
            "trigger": "auto",
            "input_tokens_before": 180_000,
            "input_tokens_after": 42_000,
            "summary_tokens": 1_500,
            "summary": "compressed conversation state"
        });
        let event = adapter.transform(&raw).unwrap();
        assert_eq!(event.event_type, "post_compact");
        assert_eq!(event.target, "compact:post:auto");
        assert_eq!(
            event.metadata.get("input_tokens_before"),
            Some(&json!(180_000))
        );
        // Summary externalized through blob helper; small → inline shape.
        let compact_summary = event
            .metadata
            .get("compact_summary")
            .expect("compact_summary key missing");
        assert!(
            compact_summary.get("inline").is_some(),
            "small summary should be inline"
        );
    }

    #[test]
    fn transform_stop_failure() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "StopFailure",
            "session_id": "ses_sf",
            "error_type": "api_error",
            "error_message": "upstream 529 overloaded",
            "last_api_request_id": "req_abc123",
            "last_model": "claude-opus-4-6"
        });
        let event = adapter.transform(&raw).unwrap();
        assert_eq!(event.event_type, "stop_failure");
        assert_eq!(event.target, "session:StopFailure:api_error");
        assert!(event.content.contains("api_error"));
        assert_eq!(
            event.metadata.get("last_api_request_id"),
            Some(&json!("req_abc123"))
        );
    }

    #[test]
    fn transform_permission_denied() {
        let adapter = ClaudeCodeAdapter;
        let raw = json!({
            "hook_event_name": "PermissionDenied",
            "session_id": "ses_pd",
            "tool_name": "Bash",
            "action": "rm -rf /tmp/foo",
            "reason": "user_declined"
        });
        let event = adapter.transform(&raw).unwrap();
        assert_eq!(event.event_type, "permission_denied");
        assert_eq!(event.target, "permission_denied:Bash");
        assert!(event.content.contains("Bash"));
        assert_eq!(event.metadata.get("reason"), Some(&json!("user_declined")));
    }

    /// Privacy audit: all 5 new event types must never leak raw body
    /// content into any string field we construct. PostCompact is the
    /// highest-risk surface because it carries a summary payload.
    #[test]
    fn lane_b_events_privacy_summary_large_goes_to_blob() {
        let adapter = ClaudeCodeAdapter;
        // 10 KB of distinctive pattern as a summary. Must go through the
        // blob store (not inline) and must not appear verbatim in
        // event.content or any metadata value besides the blob-hash ref.
        let big = "SECRET_COMPACT_SUMMARY_NEEDLE_"
            .chars()
            .cycle()
            .take(10 * 1024)
            .collect::<String>();
        // Redirect PUNKGO_DATA_DIR to a tempdir so the blob write happens
        // in an isolated location.
        let _lock = crate::session::PUNKGO_DATA_DIR_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::TempDir::new().unwrap();
        let prev = std::env::var_os("PUNKGO_DATA_DIR");
        std::env::set_var("PUNKGO_DATA_DIR", tmp.path());

        let raw = json!({
            "hook_event_name": "PostCompact",
            "session_id": "ses_priv",
            "trigger": "auto",
            "summary": big,
        });
        let event = adapter.transform(&raw).unwrap();

        // Restore env var before assertions so a failure doesn't pollute.
        match prev {
            Some(v) => std::env::set_var("PUNKGO_DATA_DIR", v),
            None => std::env::remove_var("PUNKGO_DATA_DIR"),
        }

        assert_eq!(event.event_type, "post_compact");
        // The summary must be in the blob store, not inline.
        let cs = event.metadata.get("compact_summary").unwrap();
        assert!(
            cs.get("blob_hash").is_some(),
            "large summary should be routed to blob store: {cs}"
        );
        assert!(
            cs.get("inline").is_none(),
            "large summary must NOT be inline"
        );
        // Double-check: the event.content display label must not contain
        // the needle (it's a short `Post-compact (auto)` string).
        assert!(
            !event.content.contains("SECRET_COMPACT_SUMMARY_NEEDLE_"),
            "needle leaked into event.content"
        );
        // And serialized metadata (excluding the blob ref object) must not
        // contain the needle either.
        let serialized = serde_json::to_string(&event.metadata).unwrap();
        // The blob_hash ref itself is fine (it's a sha256, no needle).
        // Anywhere ELSE the needle should not appear.
        let needle_count = serialized.matches("SECRET_COMPACT_SUMMARY_NEEDLE_").count();
        assert_eq!(
            needle_count, 0,
            "needle leaked into serialized metadata: {needle_count} occurrences"
        );
    }
}
