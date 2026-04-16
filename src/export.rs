use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::data_fetch::{
    event_matches_session, fetch_all_events, format_timestamp_date, format_timestamp_full,
    format_timestamp_time, parse_event_timestamp_ms,
};
use crate::ipc_client::IpcClient;

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

    let actor_id = args.actor.clone();

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

        // Show user prompt text. v0.6.0: the adapter-level truncation
        // is removed for claude-code events (full content stored via
        // externalize_or_inline). Cursor adapter still truncates at
        // the adapter layer. We no longer add a second display-level
        // cap here — the export shows whatever the metadata contains.
        // For old pre-v0.6.0 events, the metadata is already truncated
        // at 200 bytes; for new events it's the full prompt.
        if event_type == "user_prompt" {
            if let Some(prompt) = event
                .get("payload")
                .and_then(|p| p.get("metadata"))
                .and_then(|m| m.get("prompt"))
                .and_then(Value::as_str)
            {
                out.push_str(&format!("  > \"{prompt}\"\n"));
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

// truncate_at_char_boundary was removed in v0.6.0 — no longer needed
// after the export prompt display cap was dropped (plan line 261).

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
        let events = [
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
        let events = [json!({
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
