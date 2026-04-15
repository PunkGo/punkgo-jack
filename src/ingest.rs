use std::io::Read;

use anyhow::{bail, Context, Result};
use punkgo_core::protocol::{RequestEnvelope, RequestType, ResponseEnvelope};
use serde_json::{json, Value};
use tracing::{debug, error, info};

use crate::adapters::{self, IngestEvent};
use crate::ipc_client::{new_request_id, IpcClient};
use crate::session;

/// Parsed CLI arguments for `punkgo-jack ingest`.
#[derive(Debug)]
pub struct IngestArgs {
    pub source: String,
    pub event_type_override: Option<String>,
    pub endpoint: Option<String>,
    pub dry_run: bool,
    pub quiet: bool,
    pub receipt: bool,
    pub summary: bool,
}

/// Run the ingest subcommand: stdin → adapter → IPC → stdout.
///
/// Exit codes:
/// - 0: success (or dry-run)
/// - 1: failure (never exit 2 — that blocks Claude Code tool calls)
pub fn run(args: IngestArgs) -> Result<()> {
    // Cursor requires valid JSON on stdout even when ingest fails.
    // Wrap the real logic so we can output a fallback response on error.
    let result = run_inner(&args);
    if let Err(ref e) = result {
        debug!(error = %e, "ingest failed, outputting fallback JSON");
        // Output a permissive response so Cursor doesn't block.
        // Use write+flush to ensure output arrives before process exits.
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        let _ = out.write_all(b"{\"permission\":\"allow\",\"continue\":true}");
        let _ = out.flush();
        // Swallow the error — Cursor hooks must exit 0 with valid JSON.
        return Ok(());
    }
    result
}

fn run_inner(args: &IngestArgs) -> Result<()> {
    debug!(source = %args.source, dry_run = args.dry_run, "ingest started");

    // 0. Fast skip: Cursor sets CURSOR_VERSION for all hook subprocesses.
    //    When a claude-code hook is triggered by Cursor, skip immediately —
    //    the dedicated cursor hook (--source cursor) handles recording.
    //    No stdin read, no IPC, no stderr. Zero overhead.
    if args.source == "claude-code" && std::env::var("CURSOR_VERSION").is_ok() {
        let raw = read_stdin().unwrap_or_else(|_| json!({}));
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        let _ = out.write_all(cursor_default_response(&raw).to_string().as_bytes());
        let _ = out.flush();
        return Ok(());
    }

    // 1. Read stdin. Cursor may send empty stdin for some events.
    let raw_json = read_stdin().unwrap_or_else(|_| json!({}));
    let is_cursor = raw_json.get("cursor_version").is_some();

    // 2. Select adapter.
    let adapter = adapters::adapter_for_source(&args.source).with_context(|| {
        format!(
            "unknown source: '{}'. Supported: claude-code, cursor",
            args.source
        )
    })?;

    // 3. Transform.
    let mut event = adapter
        .transform(&raw_json)
        .context("adapter transform failed")?;

    // Allow --event-type to override the adapter's derived type.
    if let Some(ref override_type) = args.event_type_override {
        debug!(override_type, "event_type overridden by CLI flag");
        event.event_type = override_type.clone();
        // Fix target when stdin was empty (Cursor sometimes sends no stdin
        // for session events) and adapter produced "tool:unknown".
        if event.target == "tool:unknown" {
            event.target = format!("session:{override_type}");
        }
    }

    let detected_source = if is_cursor { "cursor" } else { &args.source };
    event.source = detected_source.to_string();
    event.actor_id = detected_source.to_string();

    // Extract session_id from the raw hook input for per-session state files.
    // Cursor uses "conversation_id", Claude Code uses "session_id".
    let claude_session_id = raw_json
        .get("session_id")
        .or_else(|| raw_json.get("conversation_id"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();

    // Session lifecycle management (client-side, kernel has no session concept).
    match event.event_type.as_str() {
        "session_start" => {
            // Auto-flush spillover from previous sessions before starting new one.
            if let Err(e) = crate::spillover::flush() {
                debug!(error = %e, "spillover flush on session_start failed (non-fatal)");
            }
            // Auto-recharge: check actor energy and top up if low.
            let client = IpcClient::from_env(args.endpoint.as_deref());
            check_energy_level(&client, &event.actor_id);
            match session::start_session(&claude_session_id, &event.actor_id) {
                Ok(state) => {
                    debug!(session_id = %state.session_id, "session started");
                    event
                        .metadata
                        .insert("punkgo_session_id".into(), json!(state.session_id));
                }
                Err(e) => {
                    debug!(error = %e, "failed to start session (non-fatal)");
                }
            }
        }
        "session_end" => match session::end_session(&claude_session_id) {
            Ok(state) => {
                debug!(
                    session_id = %state.session_id,
                    event_count = state.event_count,
                    "session ended"
                );
                event
                    .metadata
                    .insert("punkgo_session_id".into(), json!(state.session_id));
                event
                    .metadata
                    .insert("punkgo_event_count".into(), json!(state.event_count));
                event
                    .metadata
                    .insert("punkgo_started_at".into(), json!(state.started_at));
            }
            Err(e) => {
                debug!(error = %e, "failed to end session (non-fatal)");
            }
        },
        _ => {
            // For tool events: attach session_id (energy tracking happens after submit).
            if let Ok(Some(state)) = session::current_session(&claude_session_id) {
                event
                    .metadata
                    .insert("punkgo_session_id".into(), json!(state.session_id));
            }
        }
    }

    debug!(
        event_type = %event.event_type,
        target = %event.target,
        actor_id = %event.actor_id,
        "adapter transform complete"
    );

    // Build the default quiet response once (Cursor needs typed responses).
    let quiet_response = if is_cursor {
        cursor_default_response(&raw_json)
    } else {
        json!({})
    };

    // 4. Dry-run: print the event and exit.
    if args.dry_run {
        info!(event_type = %event.event_type, target = %event.target, "dry-run, skipping IPC submit");
        let preview = event_to_preview(&event);
        if args.quiet {
            println!("{quiet_response}");
        } else {
            println!("{}", serde_json::to_string_pretty(&preview)?);
        }
        return Ok(());
    }

    // 5. Submit via IPC (with retry on transient errors + spillover fallback).
    let client = IpcClient::from_env(args.endpoint.as_deref());
    let payload = event_to_submit_payload(&event);

    let resp = match submit_with_retry(&client, &payload, args) {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "submit failed");
            if let Err(spill_err) = crate::spillover::spill(&payload) {
                error!(error = %spill_err, "failed to write spillover");
            }
            // Output permissive response so Cursor doesn't block.
            println!("{quiet_response}");
            return Ok(());
        }
    };

    // 6a. Lane D: dispatch transcript scan on hook events that signal a
    // turn boundary. Fire-and-forget — never fail the hook on indexer errors.
    if resp.status == "ok" && args.source == "claude-code" {
        dispatch_transcript_scan(&event.event_type, &claude_session_id, &raw_json);
    }

    // 6. Output result.
    match resp.status.as_str() {
        "ok" => {
            let event_id = resp
                .payload
                .get("event_id")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let settled_cost = resp
                .payload
                .get("settled_cost")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let log_index = resp.payload.get("log_index").and_then(Value::as_u64);
            info!(event_id, event_type = %event.event_type, target = %event.target, "ingest succeeded");
            // Track event count + energy in session and daily counter.
            if let Err(e) = session::record_event(&claude_session_id, settled_cost) {
                debug!(error = %e, "failed to record event in session");
            }
            if args.quiet {
                println!("{quiet_response}");
            } else {
                println!(
                    "{}",
                    json!({ "ok": true, "event_id": event_id, "payload": resp.payload })
                );
            }
            // One-line receipt to stderr (visible in Claude Code hook output).
            if args.receipt || !args.quiet {
                let short_id = &event_id[..event_id.len().min(8)];
                let idx_str = log_index.map(|i| format!("#{i}")).unwrap_or_default();
                eprintln!(
                    "[punkgo] \u{2713} {} | receipt: {} | energy: {} | {}",
                    event.target, short_id, settled_cost, idx_str
                );
            }
        }
        _ => {
            // Non-retryable error — spill for later replay.
            let msg = resp
                .payload
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            error!(error = msg, status = %resp.status, "ingest failed (non-retryable)");
            if let Err(spill_err) = crate::spillover::spill(&payload) {
                error!(error = %spill_err, "failed to write spillover after kernel error");
            }
            if args.quiet {
                println!("{quiet_response}");
            } else {
                println!("{}", json!({ "ok": false, "error": msg }));
            }
        }
    }

    // Print session summary on session_end if requested.
    if resp.status == "ok" && args.summary && event.event_type == "session_end" {
        print_session_summary(&event, &client);
    }

    // Fail with exit 1 on error status (caller handles process::exit).
    if resp.status != "ok" {
        let msg = resp
            .payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("kernel returned error");
        bail!("ingest failed: {msg}");
    }

    Ok(())
}

/// Parse ingest CLI args from an iterator (after "ingest" has been consumed).
pub fn parse_args(args: &mut impl Iterator<Item = String>) -> Result<IngestArgs> {
    let mut source: Option<String> = None;
    let mut event_type_override: Option<String> = None;
    let mut endpoint: Option<String> = None;
    let mut dry_run = false;
    let mut quiet = false;
    let mut receipt = false;
    let mut summary = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source" => {
                source = Some(
                    args.next()
                        .context("--source requires a value (e.g. claude-code)")?,
                );
            }
            "--event-type" => {
                event_type_override = Some(args.next().context("--event-type requires a value")?);
            }
            "--endpoint" => {
                endpoint = Some(args.next().context("--endpoint requires a value")?);
            }
            "--dry-run" => dry_run = true,
            "--quiet" => quiet = true,
            "--receipt" => receipt = true,
            "--summary" => summary = true,
            other => bail!("unknown ingest option: {other}"),
        }
    }

    let source = source.context("--source is required (e.g. --source claude-code)")?;

    Ok(IngestArgs {
        source,
        event_type_override,
        endpoint,
        dry_run,
        quiet,
        receipt,
        summary,
    })
}

/// Lane D: fire-and-forget dispatch to `indexer::scan_on_trigger` for hook
/// events that signal a turn/session boundary. The async task writes a
/// `pending_scans` row (durable) and spawns a background drainer. Indexer
/// errors are logged but never fail the hook.
fn dispatch_transcript_scan(event_type: &str, session_id: &str, raw_json: &Value) {
    let triggers = matches!(
        event_type,
        "agent_stop" | "subagent_stop" | "session_end" | "stop_failure"
    );
    if !triggers || session_id == "unknown" {
        return;
    }
    let transcript_path = raw_json
        .get("transcript_path")
        .and_then(Value::as_str)
        .map(String::from);
    let session_id = session_id.to_string();

    // We're inside a sync `run_inner` — spawn a short-lived runtime to
    // call the async enqueue. Building a current-thread runtime is cheap
    // (~µs) and keeps the indexer module's public API uniformly async.
    let result = std::thread::Builder::new()
        .name("punkgo-indexer-dispatch".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    debug!(error = %e, "indexer dispatch: failed to build runtime");
                    return;
                }
            };
            if let Err(e) =
                rt.block_on(crate::indexer::scan_on_trigger(session_id, transcript_path))
            {
                debug!(error = %e, "indexer scan_on_trigger returned error");
            }
        });
    if let Err(e) = result {
        debug!(error = %e, "indexer dispatch: failed to spawn thread");
    }
}

fn read_stdin() -> Result<Value> {
    let mut buf = String::new();
    std::io::stdin()
        .read_to_string(&mut buf)
        .context("failed to read stdin")?;
    // Cursor on Windows prepends UTF-8 BOM (\xEF\xBB\xBF) to stdin.
    let json_str = buf.strip_prefix('\u{FEFF}').unwrap_or(&buf);
    serde_json::from_str(json_str).context("failed to parse stdin as JSON")
}

/// Return the correct default JSON response for a Cursor hook event.
/// Each hook type expects a specific format:
///   preToolUse → {"permission":"allow"}
///   beforeSubmitPrompt → {"continue":true}
///   others → {}
fn cursor_default_response(raw: &Value) -> Value {
    let hook = raw
        .get("hook_event_name")
        .and_then(Value::as_str)
        .unwrap_or("");
    match hook {
        "preToolUse" | "PreToolUse" => json!({"permission": "allow"}),
        "beforeSubmitPrompt" | "UserPromptSubmit" => json!({"continue": true}),
        // Cursor's subagentStart can block — allow it.
        "subagentStart" | "SubagentStart" => json!({"permission": "allow"}),
        _ => json!({}),
    }
}

fn event_to_submit_payload(event: &IngestEvent) -> Value {
    // client_timestamp: epoch-ms when the event actually occurred (not when
    // kernel commits it). Used by history display to show accurate times,
    // especially for events replayed from spillover.
    let client_ts = chrono::Utc::now().timestamp_millis();
    json!({
        "actor_id": event.actor_id,
        "action_type": "observe",
        "target": format!("hook/{}", event.target),
        "payload": {
            "schema": "punkgo-jack-ingest-v1",
            "event_type": event.event_type,
            "message": event.content,
            "source": event.source,
            "metadata": event.metadata,
            "client_timestamp": client_ts
        }
    })
}

fn print_session_summary(event: &IngestEvent, client: &IpcClient) {
    let session_id = event
        .metadata
        .get("punkgo_session_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let event_count = event
        .metadata
        .get("punkgo_event_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let started_at = event
        .metadata
        .get("punkgo_started_at")
        .and_then(Value::as_str)
        .unwrap_or("");

    // Calculate duration.
    let duration_str = if !started_at.is_empty() {
        if let Ok(start) = chrono::DateTime::parse_from_rfc3339(started_at) {
            let elapsed = chrono::Utc::now().signed_duration_since(start);
            let mins = elapsed.num_minutes();
            let secs = elapsed.num_seconds() % 60;
            format!("{mins}m {secs:02}s")
        } else {
            "unknown".into()
        }
    } else {
        "unknown".into()
    };

    // Query actor energy balance.
    let actor_id = &event.actor_id;
    let mut energy_balance: Option<u64> = None;
    let energy_req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "actor_energy", "actor_id": actor_id }),
    };
    if let Ok(resp) = client.send(&energy_req) {
        if resp.status == "ok" {
            energy_balance = resp.payload.get("energy_balance").and_then(Value::as_u64);
        }
    }

    // Query Merkle checkpoint from kernel.
    let mut tree_size: u64 = 0;
    let mut root_hash = String::from("unavailable");

    let cp_req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "audit_checkpoint" }),
    };
    if let Ok(resp) = client.send(&cp_req) {
        if resp.status == "ok" {
            if let Some(ts) = resp.payload.get("tree_size").and_then(Value::as_u64) {
                tree_size = ts;
            }
            if let Some(rh) = resp.payload.get("root_hash").and_then(Value::as_str) {
                root_hash = format!(
                    "0x{}...{}",
                    &rh[..rh.len().min(8)],
                    &rh[rh.len().saturating_sub(8)..]
                );
            }
        }
    }

    let energy_str = match energy_balance {
        Some(bal) => format!("{bal} remaining"),
        None => "unavailable".into(),
    };

    let short_session = &session_id[..session_id.len().min(8)];
    eprintln!();
    eprintln!("\u{2550}\u{2550}\u{2550} PunkGo Session Receipt \u{2550}\u{2550}\u{2550}");
    eprintln!("Session:  {short_session}");
    eprintln!("Duration: {duration_str}");
    eprintln!("Actions:  {event_count} recorded");
    eprintln!("Energy:   {energy_str}");
    eprintln!("Merkle:   tree_size={tree_size}, root={root_hash}");
    eprintln!("\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}");
    eprintln!();
}

/// Submit an event to the kernel with retry logic for transient errors.
///
/// Retries up to 3 times with exponential backoff (100ms, 200ms, 400ms) for
/// "database is locked" errors. Falls back to daemon auto-start on IPC
/// connection failures. Spills to spillover.jsonl only as last resort.
fn submit_with_retry(
    client: &IpcClient,
    payload: &Value,
    args: &IngestArgs,
) -> Result<ResponseEnvelope> {
    let max_retries = 3;
    let base_delay_ms = 100u64;

    // First attempt — may need daemon auto-start.
    let mut resp = match send_or_autostart(client, payload, args) {
        Ok(r) => r,
        Err(_) => {
            // Connection completely failed even after auto-start — spill.
            if let Err(e) = crate::spillover::spill(payload) {
                error!(error = %e, "failed to write spillover");
            }
            // Return a synthetic error response so the caller handles it uniformly.
            return Ok(ResponseEnvelope {
                request_id: String::new(),
                status: "error".into(),
                payload: json!({"message": "daemon unreachable, spilled"}),
            });
        }
    };

    // Retry on transient kernel errors (database locked, busy, etc.).
    for attempt in 1..=max_retries {
        if resp.status == "ok" {
            break;
        }
        let msg = resp
            .payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !is_retryable_error(msg) {
            break;
        }
        let delay = base_delay_ms * (1 << (attempt - 1)); // 100, 200, 400ms
        debug!(
            attempt,
            delay_ms = delay,
            error = msg,
            "retrying after transient error"
        );
        std::thread::sleep(std::time::Duration::from_millis(delay));

        let req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Submit,
            payload: payload.clone(),
        };
        match client.send(&req) {
            Ok(r) => resp = r,
            Err(e) => {
                debug!(error = %e, attempt, "IPC send failed during retry");
                break;
            }
        }
    }

    // If still failed after retries, spill for later flush.
    if resp.status != "ok" {
        let msg = resp
            .payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("");
        if is_retryable_error(msg) {
            debug!("still failed after {max_retries} retries, spilling");
            if let Err(e) = crate::spillover::spill(payload) {
                error!(error = %e, "failed to write spillover after retries");
            }
        }
    }

    Ok(resp)
}

/// Send to kernel, with connection retry + daemon auto-start on failure.
fn send_or_autostart(
    client: &IpcClient,
    payload: &Value,
    args: &IngestArgs,
) -> Result<ResponseEnvelope> {
    // Try connecting with retry (handles named pipe congestion under high concurrency).
    // 3 attempts with jittered backoff before falling back to auto-start.
    let connect_retries = 3;
    for attempt in 0..=connect_retries {
        let req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Submit,
            payload: payload.clone(),
        };

        match client.send(&req) {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                if attempt < connect_retries {
                    // Jittered backoff: 50-150ms, 100-300ms, 200-600ms
                    let base = 50u64 * (1 << attempt);
                    let jitter = (std::process::id() as u64 + attempt as u64 * 37) % base;
                    let delay = base + jitter;
                    debug!(attempt, delay_ms = delay, error = %e, "IPC connect retry");
                    std::thread::sleep(std::time::Duration::from_millis(delay));
                    continue;
                }
                // All connect retries exhausted — try auto-start.
                debug!(error = %e, "IPC send failed after retries, attempting auto-start");
                match crate::daemon::ensure_kernel_running(client) {
                    Ok(start_msg) => {
                        if let Some(msg) = start_msg {
                            if !args.quiet {
                                println!("{}", json!({ "systemMessage": msg }));
                            }
                        }
                        // Re-read daemon.addr for the fresh daemon's endpoint
                        let fresh_client = IpcClient::from_env(None);
                        let retry_req = RequestEnvelope {
                            request_id: new_request_id(),
                            request_type: RequestType::Submit,
                            payload: payload.clone(),
                        };
                        return fresh_client
                            .send(&retry_req)
                            .context("IPC retry failed after auto-start");
                    }
                    Err(err_msg) => {
                        bail!("auto-start failed: {err_msg}")
                    }
                }
            }
        }
    }
    unreachable!()
}

/// Check if a kernel error message is a transient error worth retrying.
fn is_retryable_error(msg: &str) -> bool {
    msg.contains("database is locked") || msg.contains("database is busy")
}

/// Check actor energy balance at session start and warn if critically low.
/// The kernel's EnergyProducer tick loop keeps agents funded via energy_share.
/// This check catches misconfigurations early (e.g. after kernel upgrade).
fn check_energy_level(client: &IpcClient, actor_id: &str) {
    const LOW_ENERGY_THRESHOLD: i64 = 100;

    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "actor_energy", "actor_id": actor_id }),
    };
    let (balance, reserved) = match client.send(&req) {
        Ok(resp) if resp.status == "ok" => {
            let b = resp
                .payload
                .get("energy_balance")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let r = resp
                .payload
                .get("reserved_energy")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            (b, r)
        }
        _ => {
            debug!("could not query energy balance (non-fatal)");
            return;
        }
    };

    let available = balance - reserved;
    if available >= LOW_ENERGY_THRESHOLD {
        debug!(available, "energy sufficient");
        return;
    }

    eprintln!(
        "[punkgo] warning: actor '{actor_id}' energy low ({available} available). \
         Ensure kernel is updated and energy_share > 0 for this actor."
    );
}

fn event_to_preview(event: &IngestEvent) -> Value {
    json!({
        "dry_run": true,
        "actor_id": event.actor_id,
        "target": format!("hook/{}", event.target),
        "event_type": event.event_type,
        "content": event.content,
        "source": event.source,
        "metadata": event.metadata
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_to_submit_payload_has_correct_schema() {
        let event = IngestEvent {
            actor_id: "claude-code".into(),
            target: "bash:npm test".into(),
            event_type: "command_execution".into(),
            content: "Execute command: npm test".into(),
            metadata: Default::default(),
            source: "claude-code".into(),
        };
        let payload = event_to_submit_payload(&event);

        assert_eq!(payload["actor_id"], "claude-code");
        assert_eq!(payload["action_type"], "observe");
        assert_eq!(payload["target"], "hook/bash:npm test");
        assert_eq!(payload["payload"]["schema"], "punkgo-jack-ingest-v1");
        assert_eq!(payload["payload"]["event_type"], "command_execution");
    }

    #[test]
    fn parse_args_requires_source() {
        let mut args = Vec::<String>::new().into_iter();
        let result = parse_args(&mut args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--source"));
    }

    #[test]
    fn parse_args_all_options() {
        let args_vec = vec![
            "--source",
            "claude-code",
            "--event-type",
            "custom_event",
            "--endpoint",
            "//./pipe/test",
            "--dry-run",
            "--quiet",
        ];
        let mut args = args_vec.into_iter().map(String::from);
        let parsed = parse_args(&mut args).unwrap();

        assert_eq!(parsed.source, "claude-code");
        assert_eq!(parsed.event_type_override.as_deref(), Some("custom_event"));
        assert_eq!(parsed.endpoint.as_deref(), Some("//./pipe/test"));
        assert!(parsed.dry_run);
        assert!(parsed.quiet);
    }

    #[test]
    fn parse_args_receipt_and_summary_flags() {
        let args_vec = vec!["--source", "claude-code", "--receipt", "--summary"];
        let mut args = args_vec.into_iter().map(String::from);
        let parsed = parse_args(&mut args).unwrap();
        assert!(parsed.receipt);
        assert!(parsed.summary);
    }

    #[test]
    fn parse_args_rejects_unknown() {
        let args_vec = vec!["--source", "claude-code", "--bogus"];
        let mut args = args_vec.into_iter().map(String::from);
        let result = parse_args(&mut args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--bogus"));
    }
}
