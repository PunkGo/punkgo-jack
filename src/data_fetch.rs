use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::ipc_client::{new_request_id, IpcClient};
use punkgo_core::protocol::{RequestEnvelope, RequestType};

/// Fetch events from kernel with pagination. Returns events in chronological
/// order (oldest first).
pub fn fetch_all_events(
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

pub fn event_matches_session(event: &Value, session_id: &str) -> bool {
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

pub fn parse_event_timestamp_ms(event: &Value) -> Option<u64> {
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

pub fn format_timestamp_full(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "unknown".into())
}

pub fn format_timestamp_date(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%Y-%m-%d")
                .to_string()
        })
        .unwrap_or_else(|| "unknown".into())
}

pub fn format_timestamp_time(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "??:??:??".into())
}

/// Fetch latest checkpoint from kernel (best-effort).
pub fn fetch_checkpoint(client: &IpcClient) -> Result<Value> {
    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "checkpoint" }),
    };
    let resp = client.send(&req)?;
    if resp.status != "ok" {
        bail!("checkpoint query failed");
    }
    Ok(resp.payload)
}

/// Extract the event_type string from an event Value.
pub fn event_type(event: &Value) -> &str {
    event
        .get("payload")
        .and_then(|p| p.get("event_type"))
        .and_then(Value::as_str)
        .or_else(|| event.get("action_type").and_then(Value::as_str))
        .unwrap_or("unknown")
}

/// Extract the target string from an event Value.
pub fn event_target(event: &Value) -> &str {
    event
        .get("target")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_type_extracts_from_payload() {
        let e = json!({"payload": {"event_type": "file_read"}});
        assert_eq!(event_type(&e), "file_read");
    }

    #[test]
    fn event_type_falls_back_to_action_type() {
        let e = json!({"action_type": "observe"});
        assert_eq!(event_type(&e), "observe");
    }

    #[test]
    fn event_type_returns_unknown_for_empty() {
        let e = json!({});
        assert_eq!(event_type(&e), "unknown");
    }

    #[test]
    fn parse_timestamp_prefers_client_timestamp() {
        let e = json!({"payload": {"client_timestamp": 1710000000000u64}, "timestamp": "9999"});
        assert_eq!(parse_event_timestamp_ms(&e), Some(1710000000000));
    }

    #[test]
    fn event_matches_session_prefix() {
        let e = json!({"payload": {"metadata": {"session_id": "abc-123-def"}}});
        assert!(event_matches_session(&e, "abc-123"));
        assert!(!event_matches_session(&e, "xyz"));
    }

    #[test]
    fn event_target_extracts() {
        let e = json!({"target": "hook/file:src/main.rs"});
        assert_eq!(event_target(&e), "hook/file:src/main.rs");
    }
}
