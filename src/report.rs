use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use punkgo_core::protocol::{RequestEnvelope, RequestType};
use serde_json::{json, Value};

use crate::ipc_client::{new_request_id, IpcClient};
use crate::session;

/// Parsed CLI arguments for `punkgo-jack report`.
#[derive(Debug)]
pub struct ReportArgs {
    pub session_id: Option<String>,
}

/// A single action within a turn.
struct Action {
    event_type: String,
    target: String,
    receipt_id: String,
    timestamp_ms: Option<u64>,
    is_failure: bool,
}

/// A turn: one user prompt + the actions Claude took in response.
struct Turn {
    prompt: String,
    timestamp_ms: Option<u64>,
    actions: Vec<Action>,
}

/// Accumulator for session-level action classification.
#[derive(Default)]
struct ActionAccum {
    files_read: BTreeSet<String>,
    files_written: BTreeSet<String>,
    files_edited: BTreeSet<String>,
    commands: Vec<String>,
    searches: Vec<String>,
    failures: Vec<String>,
}

/// Aggregated session report data.
struct SessionReport {
    session_id: String,
    started_at: Option<u64>,
    ended_at: Option<u64>,
    turns: Vec<Turn>,
    files_read: BTreeSet<String>,
    files_written: BTreeSet<String>,
    files_edited: BTreeSet<String>,
    commands: Vec<String>,
    searches: Vec<String>,
    failures: Vec<String>,
    total_events: usize,
    merkle_tree_size: Option<u64>,
    merkle_root: Option<String>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub fn parse_args(args: &mut impl Iterator<Item = String>) -> Result<ReportArgs> {
    let session_id = args.next();
    Ok(ReportArgs { session_id })
}

pub fn run_report(args: ReportArgs) -> Result<()> {
    let client = IpcClient::from_env(None);

    // Resolve session_id: explicit arg > active session > error.
    let session_id = if let Some(id) = args.session_id {
        id
    } else if let Ok(Some(state)) = session::latest_session() {
        state.session_id
    } else {
        bail!(
            "No session specified and no active session.\n\
             Usage: punkgo-jack report [SESSION_ID]"
        );
    };

    // Fetch all events for this session via paginated API.
    let events = fetch_session_events(&client, &session_id)?;

    if events.is_empty() {
        eprintln!(
            "No events found for session {}",
            &session_id[..session_id.len().min(8)]
        );
        return Ok(());
    }

    // Build the report.
    let report = build_report(&session_id, &events, &client);

    // Render markdown to stdout.
    print_report(&report);

    Ok(())
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------

/// Fetch all events for a session using cursor-based pagination.
/// Events are returned in chronological order (oldest first).
fn fetch_session_events(client: &IpcClient, session_id: &str) -> Result<Vec<Value>> {
    let mut all_events: Vec<Value> = Vec::new();
    let mut before_index: Option<i64> = None;

    loop {
        let mut payload = json!({
            "kind": "events",
            "limit": 500
        });
        if let Some(bi) = before_index {
            payload["before_index"] = json!(bi);
        }

        let req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Read,
            payload,
        };

        let resp = client.send(&req).context("failed to query events")?;
        if resp.status != "ok" {
            let msg = resp
                .payload
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            bail!("failed to query events from kernel: {msg}. Is punkgo-kerneld running?");
        }

        let events = resp
            .payload
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        if events.is_empty() {
            break;
        }

        // Filter to events belonging to this session.
        let session_events: Vec<Value> = events
            .into_iter()
            .filter(|e| {
                e.pointer("/payload/metadata/punkgo_session_id")
                    .and_then(Value::as_str)
                    == Some(session_id)
            })
            .collect();

        all_events.extend(session_events);

        // Check pagination.
        let has_more = resp
            .payload
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let next_cursor = resp.payload.get("next_cursor").and_then(Value::as_i64);

        if !has_more || next_cursor.is_none() {
            break;
        }
        before_index = next_cursor;
    }

    // Reverse to chronological order (API returns newest first).
    all_events.reverse();
    Ok(all_events)
}

// ---------------------------------------------------------------------------
// Report building
// ---------------------------------------------------------------------------

fn build_report(session_id: &str, events: &[Value], client: &IpcClient) -> SessionReport {
    let mut turns: Vec<Turn> = Vec::new();
    let mut current_actions: Vec<Action> = Vec::new();
    let mut accum = ActionAccum::default();
    let mut started_at: Option<u64> = None;
    let mut ended_at: Option<u64> = None;

    for event in events {
        let event_type = event
            .pointer("/payload/event_type")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let target = event
            .get("target")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let receipt_id = event
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let timestamp_ms = parse_timestamp(event);

        match event_type {
            "session_start" => {
                started_at = timestamp_ms;
            }
            "session_end" => {
                ended_at = timestamp_ms;
            }
            "user_prompt" => {
                // Start a new turn. Save previous actions as an "implicit" turn
                // if there are actions before the first prompt.
                if !current_actions.is_empty() {
                    let prev_prompt = if turns.is_empty() {
                        "(session start)".to_string()
                    } else {
                        "(continued)".to_string()
                    };
                    turns.push(Turn {
                        prompt: prev_prompt,
                        timestamp_ms: current_actions.first().and_then(|a| a.timestamp_ms),
                        actions: std::mem::take(&mut current_actions),
                    });
                }

                let prompt = event
                    .pointer("/payload/metadata/prompt")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();

                turns.push(Turn {
                    prompt,
                    timestamp_ms,
                    actions: Vec::new(),
                });
            }
            _ => {
                // Skip _pre events from the action list — they're observe-only.
                if event_type.ends_with("_pre") {
                    continue;
                }

                let is_failure = event_type.contains("failed") || event_type.contains("failure");

                // Extract file/command info for summary.
                let clean_target = target.strip_prefix("hook/").unwrap_or(target);
                classify_action(event_type, clean_target, event, is_failure, &mut accum);

                let action = Action {
                    event_type: event_type.to_string(),
                    target: clean_target.to_string(),
                    receipt_id: receipt_id[..receipt_id.len().min(8)].to_string(),
                    timestamp_ms,
                    is_failure,
                };

                // Append to the latest turn, or to current_actions if no turn yet.
                if let Some(last_turn) = turns.last_mut() {
                    last_turn.actions.push(action);
                } else {
                    current_actions.push(action);
                }
            }
        }
    }

    // Flush remaining actions.
    if !current_actions.is_empty() {
        turns.push(Turn {
            prompt: "(no prompt)".to_string(),
            timestamp_ms: current_actions.first().and_then(|a| a.timestamp_ms),
            actions: std::mem::take(&mut current_actions),
        });
    }

    // Fetch Merkle checkpoint.
    let (merkle_tree_size, merkle_root) = fetch_checkpoint(client);

    SessionReport {
        session_id: session_id.to_string(),
        started_at,
        ended_at,
        turns,
        files_read: accum.files_read,
        files_written: accum.files_written,
        files_edited: accum.files_edited,
        commands: accum.commands,
        searches: accum.searches,
        failures: accum.failures,
        total_events: events.len(),
        merkle_tree_size,
        merkle_root,
    }
}

fn classify_action(
    event_type: &str,
    target: &str,
    event: &Value,
    is_failure: bool,
    accum: &mut ActionAccum,
) {
    // Extract file path from metadata.tool_input if available.
    let file_path = event
        .pointer("/payload/metadata/tool_input/file_path")
        .and_then(Value::as_str);
    let command = event
        .pointer("/payload/metadata/tool_input/command")
        .and_then(Value::as_str);
    let pattern = event
        .pointer("/payload/metadata/tool_input/pattern")
        .and_then(Value::as_str);

    match event_type {
        "file_read" => {
            if let Some(fp) = file_path {
                accum.files_read.insert(fp.to_string());
            }
        }
        "file_write" => {
            if let Some(fp) = file_path {
                accum.files_written.insert(fp.to_string());
            }
        }
        "file_edit" => {
            if let Some(fp) = file_path {
                accum.files_edited.insert(fp.to_string());
            }
        }
        "command_execution" => {
            if let Some(cmd) = command {
                let short = truncate(cmd, 80);
                accum.commands.push(short);
            }
        }
        "content_search" | "web_search" => {
            if let Some(p) = pattern {
                accum.searches.push(truncate(p, 60));
            } else {
                let query = target.strip_prefix("search:").unwrap_or(target);
                accum.searches.push(truncate(query, 60));
            }
        }
        "command_execution_failed" => {
            let desc = command.unwrap_or(target);
            accum.failures.push(truncate(desc, 80));
        }
        _ => {
            if is_failure {
                accum
                    .failures
                    .push(format!("{event_type}: {}", truncate(target, 60)));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn print_report(report: &SessionReport) {
    let short_session = &report.session_id[..report.session_id.len().min(8)];
    let duration = format_duration(report.started_at, report.ended_at);

    // Count non-empty turns (turns with actions).
    let active_turns = report
        .turns
        .iter()
        .filter(|t| !t.actions.is_empty())
        .count();
    let total_actions: usize = report.turns.iter().map(|t| t.actions.len()).sum();

    println!("# PunkGo Session Report");
    println!();
    println!("- **Session**: `{}`", short_session);
    println!("- **Duration**: {duration}");
    println!(
        "- **Actions**: {total_actions} recorded across {active_turns} turns ({} events total)",
        report.total_events
    );
    if let (Some(ts), Some(root)) = (report.merkle_tree_size, &report.merkle_root) {
        let short_root = &root[..root.len().min(16)];
        println!("- **Merkle**: tree_size={ts}, root=0x{short_root}...");
    }
    println!();

    // Turns.
    for (i, turn) in report.turns.iter().enumerate() {
        let turn_time = turn
            .timestamp_ms
            .map(format_time_hhmm)
            .unwrap_or_else(|| "??:??".into());

        println!("## Turn {} ({})", i + 1, turn_time);
        println!();

        // Show prompt (truncated if very long).
        let prompt_display = if turn.prompt.len() > 200 {
            format!("{}...", &turn.prompt[..200])
        } else {
            turn.prompt.clone()
        };
        println!("> {}", prompt_display.replace('\n', " "));
        println!();

        if turn.actions.is_empty() {
            println!("_No tool actions in this turn._");
            println!();
            continue;
        }

        // Group actions by type for a concise summary.
        let mut type_counts: BTreeMap<&str, usize> = BTreeMap::new();
        let mut turn_files_changed: Vec<String> = Vec::new();

        for action in &turn.actions {
            *type_counts.entry(action.event_type.as_str()).or_insert(0) += 1;

            // Collect file changes in this turn.
            if matches!(action.event_type.as_str(), "file_write" | "file_edit") {
                let file = action
                    .target
                    .strip_prefix("file:")
                    .unwrap_or(&action.target);
                turn_files_changed.push(file.to_string());
            }
        }

        // Summary line.
        let summary_parts: Vec<String> = type_counts
            .iter()
            .map(|(k, v)| {
                let short_name = match *k {
                    "command_execution" => "Bash",
                    "file_read" => "Read",
                    "file_write" => "Write",
                    "file_edit" => "Edit",
                    "content_search" => "Grep",
                    "file_search" => "Glob",
                    "web_search" => "WebSearch",
                    "web_request" => "WebFetch",
                    "tool_call" => "Agent",
                    "command_execution_failed" => "FAILED",
                    other => other,
                };
                if *v == 1 {
                    short_name.to_string()
                } else {
                    format!("{short_name} x{v}")
                }
            })
            .collect();
        println!("{}", summary_parts.join(" | "));

        // Action table.
        println!();
        println!("| {:<22} | {:<42} | Receipt |", "Type", "Target");
        println!("|{:-<24}|{:-<44}|{:-<10}|", "", "", "");

        for action in &turn.actions {
            let marker = if action.is_failure { " ✗" } else { "" };
            let target_display = truncate(&action.target, 42);
            println!(
                "| {:<22} | {:<42} | {} |{}",
                action.event_type, target_display, action.receipt_id, marker
            );
        }
        println!();

        // Files changed in this turn.
        if !turn_files_changed.is_empty() {
            let unique: BTreeSet<&str> = turn_files_changed.iter().map(|s| s.as_str()).collect();
            println!(
                "Files changed: {}",
                unique.into_iter().collect::<Vec<_>>().join(", ")
            );
            println!();
        }
    }

    // Summary section.
    println!("---");
    println!();
    println!("## Summary");
    println!();

    if !report.files_edited.is_empty() {
        println!(
            "- **Files edited** ({}): {}",
            report.files_edited.len(),
            report
                .files_edited
                .iter()
                .map(|f| short_path(f))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !report.files_written.is_empty() {
        println!(
            "- **Files created** ({}): {}",
            report.files_written.len(),
            report
                .files_written
                .iter()
                .map(|f| short_path(f))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !report.files_read.is_empty() {
        println!("- **Files read**: {}", report.files_read.len());
    }
    if !report.commands.is_empty() {
        println!("- **Commands run**: {}", report.commands.len());
    }
    if !report.searches.is_empty() {
        println!("- **Searches**: {}", report.searches.len());
    }
    if !report.failures.is_empty() {
        println!("- **Failures** ({}):", report.failures.len());
        for f in &report.failures {
            println!("  - `{f}`");
        }
    }

    // Danger flags.
    let dangerous_cmds: Vec<&str> = report
        .commands
        .iter()
        .filter(|c| {
            let lower = c.to_lowercase();
            lower.contains("rm -rf")
                || lower.contains("git push")
                || lower.contains("git reset")
                || lower.contains("drop table")
                || lower.contains("taskkill")
                || lower.contains("force")
        })
        .map(|s| s.as_str())
        .collect();

    if !dangerous_cmds.is_empty() {
        println!();
        println!("### ⚠ Flagged Commands");
        println!();
        for cmd in &dangerous_cmds {
            println!("- `{cmd}`");
        }
    }

    println!();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_timestamp(event: &Value) -> Option<u64> {
    // Prefer client_timestamp over kernel timestamp.
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

fn format_time_hhmm(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| dt.with_timezone(&chrono::Local).format("%H:%M").to_string())
        .unwrap_or_else(|| "??:??".into())
}

fn format_duration(start: Option<u64>, end: Option<u64>) -> String {
    match (start, end) {
        (Some(s), Some(e)) if e > s => {
            let secs = (e - s) / 1000;
            let mins = secs / 60;
            let rem_secs = secs % 60;
            if mins > 0 {
                format!("{mins}m {rem_secs}s")
            } else {
                format!("{rem_secs}s")
            }
        }
        (Some(_), None) => "ongoing".into(),
        _ => "unknown".into(),
    }
}

fn truncate(s: &str, max: usize) -> String {
    // Truncate at char boundary.
    if s.len() <= max {
        s.to_string()
    } else {
        let end = s
            .char_indices()
            .take_while(|(i, _)| *i < max - 3)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(max - 3);
        format!("{}...", &s[..end])
    }
}

fn short_path(path: &str) -> String {
    // Show just the filename or last 2 components.
    let parts: Vec<&str> = path.split(['/', '\\']).collect();
    if parts.len() <= 2 {
        path.to_string()
    } else {
        parts[parts.len() - 2..].join("/")
    }
}

fn fetch_checkpoint(client: &IpcClient) -> (Option<u64>, Option<String>) {
    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "audit_checkpoint" }),
    };
    match client.send(&req) {
        Ok(resp) if resp.status == "ok" => {
            let tree_size = resp.payload.get("tree_size").and_then(Value::as_u64);
            let root = resp
                .payload
                .get("root_hash")
                .and_then(Value::as_str)
                .map(String::from);
            (tree_size, root)
        }
        _ => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_no_session() {
        let args = Vec::<String>::new();
        let parsed = parse_args(&mut args.into_iter()).unwrap();
        assert!(parsed.session_id.is_none());
    }

    #[test]
    fn parse_args_with_session() {
        let args = vec!["abc123".to_string()];
        let parsed = parse_args(&mut args.into_iter()).unwrap();
        assert_eq!(parsed.session_id.unwrap(), "abc123");
    }

    #[test]
    fn truncate_short_string() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_long_string() {
        let long = "a".repeat(50);
        let t = truncate(&long, 20);
        assert!(t.len() <= 20);
        assert!(t.ends_with("..."));
    }

    #[test]
    fn short_path_extracts_tail() {
        assert_eq!(
            short_path("E:\\github\\punkgo-jack\\src\\main.rs"),
            "src/main.rs"
        );
        assert_eq!(short_path("src/main.rs"), "src/main.rs");
        assert_eq!(short_path("main.rs"), "main.rs");
    }

    #[test]
    fn format_duration_known() {
        assert_eq!(format_duration(Some(0), Some(125_000)), "2m 5s");
        assert_eq!(format_duration(Some(0), Some(45_000)), "45s");
        assert_eq!(format_duration(Some(0), None), "ongoing");
        assert_eq!(format_duration(None, None), "unknown");
    }
}
