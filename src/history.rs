use anyhow::{bail, Context, Result};
use punkgo_core::protocol::{RequestEnvelope, RequestType};
use serde_json::{json, Value};

use crate::ipc_client::{new_request_id, IpcClient};

/// Parsed CLI arguments for `punkgo-jack history`.
#[derive(Debug)]
pub struct HistoryArgs {
    pub limit: u64,
    pub today: bool,
    pub actor: Option<String>,
}

/// Parsed CLI arguments for `punkgo-jack show`.
#[derive(Debug)]
pub struct ShowArgs {
    pub event_id: Option<String>,
    pub json: bool,
    pub checkpoint: bool,
}

/// Parsed CLI arguments for `punkgo-jack receipt`.
#[derive(Debug)]
pub struct ReceiptArgs {
    pub session_id: Option<String>,
}

// ---------------------------------------------------------------------------
// history
// ---------------------------------------------------------------------------

pub fn run_history(args: HistoryArgs) -> Result<()> {
    let client = IpcClient::from_env(None);
    let limit = args.limit;

    let actor_id = resolve_actor_id(args.actor.as_deref());
    let mut payload = json!({ "kind": "events", "limit": limit });
    if let Some(ref actor) = actor_id {
        payload["actor_id"] = json!(actor);
    }
    // If --today, we'll filter client-side after fetching.
    if args.today {
        // Fetch more to ensure we get today's events.
        payload["limit"] = json!(100);
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
        bail!("failed to query events from kernel: {msg}. Is punkgo-kerneld running?");
    }

    let events = resp
        .payload
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let events = if args.today {
        let today_start = today_start_ms();
        let filtered: Vec<Value> = events
            .into_iter()
            .filter(|e| {
                parse_event_timestamp_ms(e)
                    .map(|ts| ts >= today_start)
                    .unwrap_or(false)
            })
            .collect();
        filtered
    } else {
        events
    };

    if events.is_empty() {
        eprintln!("No events found.");
        return Ok(());
    }

    // Print table header.
    println!(
        "{:>4} | {:<5} | {:<16} | {:<30} | {:>6} | Receipt",
        "#", "Time", "Type", "Target", "Energy"
    );
    println!("{}", "-".repeat(85));

    for (i, event) in events.iter().enumerate() {
        let time = format_event_time(event);
        let action_type = event
            .get("action_type")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let target = event
            .get("target")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let energy = event
            .get("settled_energy")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let event_id = event.get("id").and_then(Value::as_str).unwrap_or("unknown");
        let short_id = &event_id[..event_id.len().min(8)];

        // Derive event_type from payload if available.
        let event_type = event
            .get("payload")
            .and_then(|p| p.get("event_type"))
            .and_then(Value::as_str)
            .unwrap_or(action_type);

        // Truncate target for display.
        let display_target = if target.len() > 30 {
            format!("{}...", &target[..27])
        } else {
            target.to_string()
        };

        println!(
            "{:>4} | {:<5} | {:<16} | {:<30} | {:>6} | {}",
            i + 1,
            time,
            event_type,
            display_target,
            energy,
            short_id
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// show
// ---------------------------------------------------------------------------

pub fn run_show(args: ShowArgs) -> Result<()> {
    let client = IpcClient::from_env(None);

    // --checkpoint mode: print C2SP checkpoint text and exit.
    if args.checkpoint {
        let cp_req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Read,
            payload: json!({ "kind": "audit_checkpoint" }),
        };
        let cp_resp = client.send(&cp_req)?;
        if cp_resp.status != "ok" {
            let msg = cp_resp
                .payload
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            bail!("failed to get checkpoint: {msg}");
        }
        if args.json {
            println!("{}", serde_json::to_string_pretty(&cp_resp.payload)?);
        } else if let Some(text) = cp_resp
            .payload
            .get("checkpoint_text")
            .and_then(Value::as_str)
        {
            print!("{text}");
        } else {
            println!("{}", serde_json::to_string_pretty(&cp_resp.payload)?);
        }
        return Ok(());
    }

    let needle = args.event_id.as_deref().unwrap();

    // Fetch recent events and find the one matching the event_id (prefix match).
    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "events", "limit": 100 }),
    };
    let resp = client.send(&req)?;
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

    let event = events
        .iter()
        .find(|e| {
            e.get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.starts_with(needle))
        })
        .ok_or_else(|| anyhow::anyhow!("no event found matching '{needle}'"))?;

    // Fetch Merkle proof for this event.
    let log_index = event.get("log_index").and_then(Value::as_u64);
    let proof_data = log_index.and_then(|idx| {
        let proof_req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Read,
            payload: json!({ "kind": "audit_inclusion_proof", "log_index": idx }),
        };
        client.send(&proof_req).ok().and_then(|r| {
            if r.status == "ok" {
                Some(r.payload)
            } else {
                None
            }
        })
    });

    // --json mode: output structured JSON with event + proof.
    if args.json {
        let mut output = event.clone();
        if let Some(proof) = &proof_data {
            output["proof"] = proof.clone();
        }
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }

    // Human-readable output.
    let event_id = event.get("id").and_then(Value::as_str).unwrap_or("unknown");
    let action_type = event
        .get("action_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let target = event
        .get("target")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let actor_id = event
        .get("actor_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let time = format_event_time_full(event);
    let energy_reserved = event
        .get("reserved_energy")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let energy_settled = event
        .get("settled_energy")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let event_type = event
        .get("payload")
        .and_then(|p| p.get("event_type"))
        .and_then(Value::as_str)
        .unwrap_or(action_type);

    println!("Event:    {event_id}");
    println!("Type:     {event_type}");
    println!("Target:   {target}");
    println!("Actor:    {actor_id}");
    println!("Time:     {time}");
    println!("Energy:   {energy_settled} (reserved: {energy_reserved}, settled: {energy_settled})");

    // Show payload.
    if let Some(payload) = event.get("payload") {
        if let Ok(pretty) = serde_json::to_string_pretty(payload) {
            println!("Payload:  {}", pretty.replace('\n', "\n          "));
        }
    }

    // Merkle proof.
    if let Some(idx) = log_index {
        println!("Merkle:   log_index={idx}");

        if let Some(proof) = &proof_data {
            let tree_size = proof.get("tree_size").and_then(Value::as_u64).unwrap_or(0);
            let proof_hashes = proof
                .get("proof")
                .and_then(Value::as_array)
                .map(|a| a.len())
                .unwrap_or(0);
            println!(
                "Proof:    \u{2713} inclusion verified (tree_size={tree_size}, proof_hashes={proof_hashes})"
            );
        } else {
            println!("Proof:    \u{2717} could not verify inclusion");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// receipt
// ---------------------------------------------------------------------------

pub fn run_receipt(args: ReceiptArgs) -> Result<()> {
    let client = IpcClient::from_env(None);

    // Determine session_id: from arg, or from current session file, or latest from kernel.
    let session_id = if let Some(sid) = args.session_id {
        sid
    } else if let Ok(Some(state)) = crate::session::latest_session() {
        state.session_id
    } else {
        // Try to find the most recent session from kernel events.
        eprintln!("No session specified and no active session. Showing latest events.");
        String::new()
    };

    let receipt_payload = json!({ "kind": "events", "limit": 100 });
    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: receipt_payload,
    };
    let resp = client.send(&req)?;
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

    // Filter events by session_id if we have one.
    let session_events: Vec<&Value> = if session_id.is_empty() {
        events.iter().collect()
    } else {
        events
            .iter()
            .filter(|e| {
                e.get("payload")
                    .and_then(|p| p.get("metadata"))
                    .and_then(|m| m.get("punkgo_session_id"))
                    .and_then(Value::as_str)
                    .is_some_and(|sid| sid == session_id)
            })
            .collect()
    };

    // Calculate stats.
    let event_count = session_events.len();
    let mut energy_consumed: u64 = 0;
    let mut min_ts: Option<u64> = None;
    let mut max_ts: Option<u64> = None;

    for evt in &session_events {
        if let Some(cost) = evt.get("settled_energy").and_then(Value::as_u64) {
            energy_consumed += cost;
        }
        if let Some(ts) = parse_event_timestamp_ms(evt) {
            min_ts = Some(min_ts.map_or(ts, |m: u64| m.min(ts)));
            max_ts = Some(max_ts.map_or(ts, |m: u64| m.max(ts)));
        }
    }

    let duration_str = match (min_ts, max_ts) {
        (Some(start), Some(end)) if end > start => {
            let secs = (end - start) / 1000;
            let mins = secs / 60;
            let secs = secs % 60;
            format!("{mins}m {secs:02}s")
        }
        _ => "unknown".into(),
    };

    // Get checkpoint for Merkle root.
    let mut tree_size: u64 = 0;
    let mut root_hash = String::from("unavailable");

    let cp_req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "audit_checkpoint" }),
    };
    if let Ok(cp_resp) = client.send(&cp_req) {
        if cp_resp.status == "ok" {
            if let Some(ts) = cp_resp.payload.get("tree_size").and_then(Value::as_u64) {
                tree_size = ts;
            }
            if let Some(rh) = cp_resp.payload.get("root_hash").and_then(Value::as_str) {
                if rh.len() > 16 {
                    root_hash = format!("0x{}...{}", &rh[..8], &rh[rh.len() - 8..]);
                } else {
                    root_hash = format!("0x{rh}");
                }
            }
        }
    }

    // Try consistency proof if we have a session start tree_size.
    let mut consistency_status = String::new();
    if !session_id.is_empty() && tree_size > 1 {
        // Find the earliest log_index in this session to use as old_size.
        let earliest_idx = session_events
            .iter()
            .filter_map(|e| e.get("log_index").and_then(Value::as_u64))
            .min();
        if let Some(old_size) = earliest_idx {
            if old_size > 0 && old_size < tree_size {
                let cons_req = RequestEnvelope {
                    request_id: new_request_id(),
                    request_type: RequestType::Read,
                    payload: json!({
                        "kind": "audit_consistency_proof",
                        "old_size": old_size,
                        "tree_size": tree_size
                    }),
                };
                match client.send(&cons_req) {
                    Ok(r) if r.status == "ok" => {
                        consistency_status = "\u{2713} consistency verified".into();
                    }
                    _ => {
                        consistency_status = "\u{2717} consistency check failed".into();
                    }
                }
            }
        }
    }

    let display_session = if session_id.is_empty() {
        "all (no session filter)"
    } else {
        &session_id[..session_id.len().min(8)]
    };

    println!();
    println!("\u{2550}\u{2550}\u{2550} PunkGo Session Receipt \u{2550}\u{2550}\u{2550}");
    println!("Session:  {display_session}");
    println!("Duration: {duration_str}");
    println!("Actions:  {event_count} recorded");
    println!("Energy:   {energy_consumed} consumed");
    println!("Merkle:   tree_size={tree_size}, root={root_hash}");
    if !consistency_status.is_empty() {
        println!("Audit:    {consistency_status}");
    }
    // TSA anchor status: find the nearest anchored checkpoint at or above this tree_size.
    // The anchor may cover an earlier checkpoint that includes this session's events.
    print_receipt_tsa_status(tree_size);
    println!("\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}");
    println!();

    Ok(())
}

// ---------------------------------------------------------------------------
// Arg parsing
// ---------------------------------------------------------------------------

pub fn parse_history_args(args: &mut impl Iterator<Item = String>) -> Result<HistoryArgs> {
    let mut limit: u64 = 20;
    let mut today = false;
    let mut actor: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--limit" => {
                limit = args
                    .next()
                    .context("--limit requires a value")?
                    .parse()
                    .context("--limit must be a number")?;
            }
            "--today" => today = true,
            "--actor" => {
                actor = Some(
                    args.next()
                        .context("--actor requires a value (e.g. claude-code)")?,
                );
            }
            other => bail!("unknown history option: {other}"),
        }
    }

    Ok(HistoryArgs {
        limit,
        today,
        actor,
    })
}

pub fn parse_show_args(args: &mut impl Iterator<Item = String>) -> Result<ShowArgs> {
    let mut event_id = None;
    let mut json = false;
    let mut checkpoint = false;

    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            "--checkpoint" => checkpoint = true,
            _ if event_id.is_none() => event_id = Some(arg),
            other => bail!("unknown show option: {other}"),
        }
    }

    if !checkpoint && event_id.is_none() {
        bail!("usage: punkgo-jack show <event_id> [--json] or punkgo-jack show --checkpoint");
    }

    Ok(ShowArgs {
        event_id,
        json,
        checkpoint,
    })
}

pub fn parse_receipt_args(args: &mut impl Iterator<Item = String>) -> Result<ReceiptArgs> {
    let session_id = args.next();
    Ok(ReceiptArgs { session_id })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Find and display TSA anchor status for a receipt.
/// Scans TSR files to find one covering the given tree_size (exact or nearest above).
fn print_receipt_tsa_status(tree_size: u64) {
    let Some(tsa_dir) = crate::config::tsa_state_dir() else {
        return;
    };
    if !tsa_dir.exists() {
        println!("TSA:      not configured");
        return;
    }

    // Scan TSR files: find the smallest tree_size >= current tree_size.
    let mut best: Option<(i64, std::path::PathBuf)> = None;
    if let Ok(entries) = std::fs::read_dir(&tsa_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(stem) = name_str.strip_suffix(".tsr") {
                if let Ok(ts) = stem.parse::<i64>() {
                    if ts >= tree_size as i64 && best.as_ref().is_none_or(|(b, _)| ts < *b) {
                        best = Some((ts, entry.path()));
                    }
                }
            }
        }
    }

    match best {
        Some((ts, path)) => {
            if let Ok(tsr_bytes) = std::fs::read(&path) {
                match crate::tsa_verify::verify_tsr(&tsr_bytes, None) {
                    Ok(info) => {
                        if ts == tree_size as i64 {
                            println!("TSA:      \u{2713} anchored at {}", info.gen_time);
                        } else {
                            println!(
                                "TSA:      \u{2713} covered by anchor at tree_size={ts} ({})",
                                info.gen_time
                            );
                        }
                    }
                    Err(_) => println!("TSA:      \u{2717} invalid TSR at {}", path.display()),
                }
            }
        }
        None => println!("TSA:      not anchored"),
    }
}

fn parse_event_timestamp_ms(event: &Value) -> Option<u64> {
    // Prefer client_timestamp (when the event actually occurred) over kernel
    // timestamp (when the event was committed). This matters for spillover
    // events that are replayed later.
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

fn format_event_time(event: &Value) -> String {
    parse_event_timestamp_ms(event)
        .and_then(|ms| {
            chrono::DateTime::from_timestamp_millis(ms as i64)
                .map(|dt| dt.with_timezone(&chrono::Local).format("%H:%M").to_string())
        })
        .unwrap_or_else(|| "??:??".into())
}

fn format_event_time_full(event: &Value) -> String {
    parse_event_timestamp_ms(event)
        .and_then(|ms| {
            chrono::DateTime::from_timestamp_millis(ms as i64).map(|dt| {
                dt.with_timezone(&chrono::Local)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            })
        })
        .unwrap_or_else(|| "unknown".into())
}

fn today_start_ms() -> u64 {
    let now = chrono::Utc::now();
    let today = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
    today.and_utc().timestamp_millis() as u64
}

/// Resolve the actor_id for queries.
/// Priority: explicit CLI flag > None (query all actors).
fn resolve_actor_id(explicit: Option<&str>) -> Option<String> {
    explicit.map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_history_args_defaults() {
        let mut args = Vec::<String>::new().into_iter();
        let parsed = parse_history_args(&mut args).unwrap();
        assert_eq!(parsed.limit, 20);
        assert!(!parsed.today);
        assert!(parsed.actor.is_none());
    }

    #[test]
    fn parse_history_args_with_options() {
        let args_vec = vec!["--limit", "50", "--today"];
        let mut args = args_vec.into_iter().map(String::from);
        let parsed = parse_history_args(&mut args).unwrap();
        assert_eq!(parsed.limit, 50);
        assert!(parsed.today);
    }

    #[test]
    fn parse_history_args_with_actor() {
        let args_vec = vec!["--actor", "openclaw", "--limit", "10"];
        let mut args = args_vec.into_iter().map(String::from);
        let parsed = parse_history_args(&mut args).unwrap();
        assert_eq!(parsed.limit, 10);
        assert_eq!(parsed.actor.as_deref(), Some("openclaw"));
    }

    #[test]
    fn parse_show_args_requires_event_id() {
        let mut args = Vec::<String>::new().into_iter();
        assert!(parse_show_args(&mut args).is_err());
    }

    #[test]
    fn format_event_time_from_epoch() {
        let event = json!({ "timestamp": "1709400000000" });
        let time = format_event_time(&event);
        assert!(!time.is_empty());
        assert_ne!(time, "??:??");
    }
}
