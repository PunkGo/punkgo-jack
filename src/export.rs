use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::ipc_client::{new_request_id, IpcClient};
use punkgo_core::protocol::{RequestEnvelope, RequestType};

/// Parsed CLI arguments for `punkgo-jack export`.
#[derive(Debug)]
pub struct ExportArgs {
    pub session: Option<String>,
    pub last: Option<u64>,
    pub format: ExportFormat,
    pub output: Option<String>,
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExportFormat {
    Markdown,
    Json,
}

pub fn parse_args(args: &mut impl Iterator<Item = String>) -> Result<ExportArgs> {
    let mut session = None;
    let mut last = None;
    let mut format = ExportFormat::Markdown;
    let mut output = None;
    let mut actor = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--session" | "-s" => {
                session = Some(args.next().context("--session requires a value")?);
            }
            "--last" | "-n" => {
                last = Some(
                    args.next()
                        .context("--last requires a number")?
                        .parse::<u64>()
                        .context("--last must be a number")?,
                );
            }
            "--format" | "-f" => {
                let fmt = args.next().context("--format requires a value")?;
                format = match fmt.as_str() {
                    "markdown" | "md" => ExportFormat::Markdown,
                    "json" => ExportFormat::Json,
                    other => bail!("unknown format: {other} (expected: markdown, json)"),
                };
            }
            "--output" | "-o" => {
                output = Some(args.next().context("--output requires a file path")?);
            }
            "--actor" => {
                actor = Some(args.next().context("--actor requires a value")?);
            }
            other => bail!("unknown export option: {other}"),
        }
    }

    Ok(ExportArgs {
        session,
        last,
        format,
        output,
        actor,
    })
}

pub fn run_export(args: ExportArgs) -> Result<()> {
    let client = IpcClient::from_env(None);

    // Resolve actor_id: explicit > session > None.
    let actor_id = args.actor.clone().or_else(|| {
        crate::session::latest_session()
            .ok()
            .flatten()
            .map(|s| s.actor_id)
    });

    // Fetch all events with pagination.
    let all_events = fetch_all_events(&client, actor_id.as_deref(), args.last)?;

    // Filter by session if specified.
    let events: Vec<&Value> = if let Some(ref session_id) = args.session {
        all_events
            .iter()
            .filter(|e| event_matches_session(e, session_id))
            .collect()
    } else {
        all_events.iter().collect()
    };

    if events.is_empty() {
        eprintln!("No events found.");
        return Ok(());
    }

    // Generate output.
    let output_text = match args.format {
        ExportFormat::Markdown => format_markdown(&events, args.session.as_deref()),
        ExportFormat::Json => format_json(&events)?,
    };

    // Write to file or stdout.
    if let Some(ref path) = args.output {
        std::fs::write(path, &output_text)
            .with_context(|| format!("failed to write export to {path}"))?;
        eprintln!("Exported {} events to {path}", events.len());
    } else {
        print!("{output_text}");
    }

    Ok(())
}

/// Fetch events from kernel with pagination. Returns events in chronological
/// order (oldest first).
fn fetch_all_events(
    client: &IpcClient,
    actor_id: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<Value>> {
    let mut all_events: Vec<Value> = Vec::new();
    let mut before_index: Option<i64> = None;
    let page_size: u64 = 500; // Kernel max is 500.

    // If --last N is specified and N <= page_size, fetch just one page.
    let target_count = limit.unwrap_or(u64::MAX);

    loop {
        let mut payload = json!({ "kind": "events", "limit": page_size });
        if let Some(actor) = actor_id {
            payload["actor_id"] = json!(actor);
        }
        if let Some(before) = before_index {
            payload["before_index"] = json!(before);
        }

        let req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Read,
            payload,
        };

        let resp = client.send(&req)?;
        if resp.status != "ok" {
            let msg = resp
                .payload
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            bail!("failed to query events: {msg}. Is punkgo-kerneld running?");
        }

        let events = resp
            .payload
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let has_more = resp
            .payload
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        if events.is_empty() {
            break;
        }

        // next_cursor is the smallest log_index in this page (last element,
        // since results are ordered DESC). Use it as before_index for next page.
        let next_cursor = resp.payload.get("next_cursor").and_then(Value::as_i64);

        all_events.extend(events);

        // Check if we have enough events for --last N.
        if all_events.len() as u64 >= target_count {
            break;
        }

        if !has_more {
            break;
        }

        // Set before_index for next page.
        if let Some(cursor) = next_cursor {
            before_index = Some(cursor);
        } else {
            break;
        }
    }

    // Events come in DESC order from kernel. Reverse to chronological order.
    all_events.reverse();

    // If --last N, take the last N events (which are now at the end after reverse).
    if let Some(n) = limit {
        let n = n as usize;
        if all_events.len() > n {
            all_events = all_events.split_off(all_events.len() - n);
        }
    }

    Ok(all_events)
}

fn event_matches_session(event: &Value, session_id: &str) -> bool {
    // Check payload.metadata.session_id or payload.metadata.punkgo_session_id.
    let meta = event.get("payload").and_then(|p| p.get("metadata"));

    if let Some(m) = meta {
        if let Some(sid) = m.get("session_id").and_then(Value::as_str) {
            if sid == session_id || sid.starts_with(session_id) {
                return true;
            }
        }
        if let Some(sid) = m.get("punkgo_session_id").and_then(Value::as_str) {
            if sid == session_id || sid.starts_with(session_id) {
                return true;
            }
        }
    }

    false
}

// ---------------------------------------------------------------------------
// Markdown formatter
// ---------------------------------------------------------------------------

fn format_markdown(events: &[&Value], session_id: Option<&str>) -> String {
    let mut out = String::new();

    // Header.
    out.push_str("# PunkGo Export\n\n");
    if let Some(sid) = session_id {
        out.push_str(&format!("**Session:** `{sid}`\n"));
    }
    out.push_str(&format!("**Events:** {}\n", events.len()));

    // Time range.
    let first_ts = events.first().and_then(|e| parse_event_timestamp_ms(e));
    let last_ts = events.last().and_then(|e| parse_event_timestamp_ms(e));
    if let (Some(start), Some(end)) = (first_ts, last_ts) {
        out.push_str(&format!(
            "**Time range:** {} — {}\n",
            format_timestamp_full(start),
            format_timestamp_full(end)
        ));
        let duration_secs = (end.saturating_sub(start)) / 1000;
        let mins = duration_secs / 60;
        let secs = duration_secs % 60;
        out.push_str(&format!("**Duration:** {mins}m {secs:02}s\n"));
    }
    out.push('\n');

    // Stats summary.
    let mut type_counts: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();
    let mut total_energy: u64 = 0;
    for event in events {
        let event_type = event
            .get("payload")
            .and_then(|p| p.get("event_type"))
            .and_then(Value::as_str)
            .or_else(|| event.get("action_type").and_then(Value::as_str))
            .unwrap_or("unknown");
        *type_counts.entry(event_type.to_string()).or_default() += 1;
        if let Some(e) = event.get("settled_energy").and_then(Value::as_u64) {
            total_energy += e;
        }
    }

    out.push_str("## Summary\n\n");
    out.push_str("| Type | Count |\n|------|-------|\n");
    for (t, c) in &type_counts {
        out.push_str(&format!("| {t} | {c} |\n"));
    }
    out.push_str(&format!("\n**Total energy:** {total_energy}\n\n"));

    // Timeline.
    out.push_str("## Timeline\n\n");

    let mut current_date = String::new();

    for event in events.iter() {
        let ts_ms = parse_event_timestamp_ms(event);
        let time_str = ts_ms
            .map(format_timestamp_time)
            .unwrap_or_else(|| "??:??:??".into());
        let date_str = ts_ms
            .map(format_timestamp_date)
            .unwrap_or_else(|| "unknown".into());

        // Print date header when date changes.
        if date_str != current_date {
            current_date = date_str.clone();
            out.push_str(&format!("### {date_str}\n\n"));
        }

        let event_type = event
            .get("payload")
            .and_then(|p| p.get("event_type"))
            .and_then(Value::as_str)
            .or_else(|| event.get("action_type").and_then(Value::as_str))
            .unwrap_or("unknown");

        let target = event
            .get("target")
            .and_then(Value::as_str)
            .unwrap_or("unknown");

        let content = event
            .get("payload")
            .and_then(|p| p.get("content"))
            .and_then(Value::as_str)
            .unwrap_or("");

        let log_index = event.get("log_index").and_then(Value::as_u64);
        let energy = event
            .get("settled_energy")
            .and_then(Value::as_u64)
            .unwrap_or(0);

        out.push_str(&format!("- **{time_str}** `{event_type}` — {target}"));
        if energy > 0 {
            out.push_str(&format!(" (energy: {energy})"));
        }
        if let Some(idx) = log_index {
            out.push_str(&format!(" `#{idx}`"));
        }
        out.push('\n');

        // Show content for interesting event types.
        if !content.is_empty()
            && !matches!(event_type, "file_read" | "file_search" | "content_search")
        {
            out.push_str(&format!("  > {content}\n"));
        }

        // Show user prompt text.
        if event_type == "user_prompt" {
            if let Some(prompt) = event
                .get("payload")
                .and_then(|p| p.get("metadata"))
                .and_then(|m| m.get("prompt"))
                .and_then(Value::as_str)
            {
                let display = if prompt.len() > 200 {
                    let end = truncate_at_char_boundary(prompt, 200);
                    format!("{end}...")
                } else {
                    prompt.to_string()
                };
                out.push_str(&format!("  > \"{display}\"\n"));
            }
        }
    }

    out
}

// ---------------------------------------------------------------------------
// JSON formatter
// ---------------------------------------------------------------------------

fn format_json(events: &[&Value]) -> Result<String> {
    let export = json!({
        "schema": "punkgo-jack-export-v1",
        "exported_at": chrono::Utc::now().to_rfc3339(),
        "event_count": events.len(),
        "events": events,
    });
    Ok(serde_json::to_string_pretty(&export)?)
}

/// Truncate a string to at most `max` bytes at a char boundary.
fn truncate_at_char_boundary(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

fn parse_event_timestamp_ms(event: &Value) -> Option<u64> {
    // Prefer client_timestamp, then kernel timestamp.
    if let Some(ct) = event
        .pointer("/payload/client_timestamp")
        .and_then(|v| v.as_u64())
    {
        return Some(ct);
    }
    match event.get("timestamp")? {
        Value::String(s) if s.chars().all(|c| c.is_ascii_digit()) => s.parse::<u64>().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn format_timestamp_full(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "unknown".into())
}

fn format_timestamp_date(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%Y-%m-%d")
                .to_string()
        })
        .unwrap_or_else(|| "unknown".into())
}

fn format_timestamp_time(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "??:??:??".into())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_defaults() {
        let mut args = Vec::<String>::new().into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(parsed.session.is_none());
        assert!(parsed.last.is_none());
        assert_eq!(parsed.format, ExportFormat::Markdown);
        assert!(parsed.output.is_none());
    }

    #[test]
    fn parse_args_full() {
        let args_vec = vec![
            "--session",
            "ses_abc",
            "--last",
            "100",
            "--format",
            "json",
            "--output",
            "export.json",
            "--actor",
            "claude-code",
        ];
        let mut args = args_vec.into_iter().map(String::from);
        let parsed = parse_args(&mut args).unwrap();
        assert_eq!(parsed.session.as_deref(), Some("ses_abc"));
        assert_eq!(parsed.last, Some(100));
        assert_eq!(parsed.format, ExportFormat::Json);
        assert_eq!(parsed.output.as_deref(), Some("export.json"));
        assert_eq!(parsed.actor.as_deref(), Some("claude-code"));
    }

    #[test]
    fn parse_args_short_flags() {
        let args_vec = vec!["-s", "ses_123", "-n", "50", "-f", "md", "-o", "out.md"];
        let mut args = args_vec.into_iter().map(String::from);
        let parsed = parse_args(&mut args).unwrap();
        assert_eq!(parsed.session.as_deref(), Some("ses_123"));
        assert_eq!(parsed.last, Some(50));
        assert_eq!(parsed.format, ExportFormat::Markdown);
        assert_eq!(parsed.output.as_deref(), Some("out.md"));
    }

    #[test]
    fn parse_args_unknown_format() {
        let args_vec = vec!["--format", "xml"];
        let mut args = args_vec.into_iter().map(String::from);
        assert!(parse_args(&mut args).is_err());
    }

    #[test]
    fn event_matches_session_by_session_id() {
        let event = json!({
            "payload": {
                "metadata": {
                    "session_id": "ses_abc123"
                }
            }
        });
        assert!(event_matches_session(&event, "ses_abc123"));
        assert!(event_matches_session(&event, "ses_abc")); // prefix match
        assert!(!event_matches_session(&event, "ses_xyz"));
    }

    #[test]
    fn event_matches_session_by_punkgo_session_id() {
        let event = json!({
            "payload": {
                "metadata": {
                    "punkgo_session_id": "uuid-1234"
                }
            }
        });
        assert!(event_matches_session(&event, "uuid-1234"));
        assert!(!event_matches_session(&event, "other"));
    }

    #[test]
    fn format_markdown_basic() {
        let events = vec![
            json!({
                "id": "evt_001",
                "log_index": 0,
                "action_type": "observe",
                "target": "bash:cargo test",
                "settled_energy": 20,
                "timestamp": "1710000000000",
                "payload": {
                    "event_type": "command_execution",
                    "content": "Execute command: cargo test"
                }
            }),
            json!({
                "id": "evt_002",
                "log_index": 1,
                "action_type": "observe",
                "target": "file:src/main.rs",
                "settled_energy": 20,
                "timestamp": "1710000060000",
                "payload": {
                    "event_type": "file_write",
                    "content": "Write file: src/main.rs"
                }
            }),
        ];
        let refs: Vec<&Value> = events.iter().collect();
        let md = format_markdown(&refs, Some("test-session"));
        assert!(md.contains("# PunkGo Export"));
        assert!(md.contains("**Session:** `test-session`"));
        assert!(md.contains("**Events:** 2"));
        assert!(md.contains("command_execution"));
        assert!(md.contains("file_write"));
        assert!(md.contains("bash:cargo test"));
    }

    #[test]
    fn format_json_structure() {
        let events = vec![json!({
            "id": "evt_001",
            "log_index": 0,
            "action_type": "observe",
            "target": "bash:echo hi",
            "timestamp": "1710000000000",
            "payload": { "event_type": "command_execution" }
        })];
        let refs: Vec<&Value> = events.iter().collect();
        let json_str = format_json(&refs).unwrap();
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["schema"], "punkgo-jack-export-v1");
        assert_eq!(parsed["event_count"], 1);
        assert!(parsed["events"].is_array());
        assert!(parsed["exported_at"].is_string());
    }
}
