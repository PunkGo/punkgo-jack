use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use rmcp::model::CallToolResult;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::backend::KernelBackend;

const DEFAULT_QUERY_LIMIT: u64 = 20;
const DEFAULT_SCAN_LIMIT: u64 = 50;
const MAX_SCAN_LIMIT: u64 = 100;
const MS_PER_DAY: u64 = 86_400_000;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoLogParams {
    /// User-level event type label. Recommended values: `tool_call`, `user_note`,
    /// `decision`, `milestone`, `error`, `security_event`, `custom`.
    event_type: String,
    /// Human-readable description of the event.
    content: String,
    /// Optional structured metadata attached to the event.
    metadata: Option<BTreeMap<String, Value>>,
    /// Optional origin label. Defaults to `punkgo-jack`.
    source: Option<String>,
}

/// A time argument that accepts either epoch milliseconds (integer or numeric string)
/// or an ISO 8601 / RFC 3339 datetime string (e.g. `"2026-02-22T10:30:00Z"`).
/// Datetimes without a timezone offset are interpreted as UTC.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum EpochMillisArg {
    Integer(u64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum VerifyMode {
    Inclusion,
    Consistency,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoQueryParams {
    /// Optional actor filter (kernel-level).
    actor_id: Option<String>,
    /// Filter returned events by action_type.
    action_type: Option<String>,
    /// Alias of `action_type` for compatibility with earlier drafts.
    event_type: Option<String>,
    /// Case-insensitive search across fields and payload JSON.
    keyword: Option<String>,
    /// Inclusive lower bound. Epoch milliseconds or ISO 8601 (e.g. `"2026-02-22T10:30:00Z"`).
    time_from: Option<EpochMillisArg>,
    /// Inclusive upper bound. Epoch milliseconds or ISO 8601 (e.g. `"2026-02-22T10:30:00Z"`).
    time_to: Option<EpochMillisArg>,
    /// Max events returned after filtering.
    limit: Option<u64>,
    /// Kernel read window before local filtering (default 50, max 100).
    scan_limit: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoStatsParams {
    /// Optional actor filter (kernel-level).
    actor_id: Option<String>,
    /// Filter returned events by action_type.
    action_type: Option<String>,
    /// Alias of `action_type` for compatibility with earlier drafts.
    event_type: Option<String>,
    /// Case-insensitive search across fields and payload JSON.
    keyword: Option<String>,
    /// Inclusive lower bound. Epoch milliseconds or ISO 8601 (e.g. `"2026-02-22T10:30:00Z"`).
    time_from: Option<EpochMillisArg>,
    /// Inclusive upper bound. Epoch milliseconds or ISO 8601 (e.g. `"2026-02-22T10:30:00Z"`).
    time_to: Option<EpochMillisArg>,
    /// Recent events to sample for derived stats (default 50, max 100).
    scan_limit: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoVerifyParams {
    /// Proof type (default: inclusion).
    mode: Option<VerifyMode>,
    /// Required for inclusion proof (unless `event_id` is provided).
    log_index: Option<u64>,
    /// Look up `log_index` automatically by event ID. Ignored when `log_index` is provided.
    event_id: Option<String>,
    /// Optional for inclusion, required for consistency.
    tree_size: Option<u64>,
    /// Required for consistency proof.
    old_size: Option<u64>,
}

#[derive(Debug, Clone)]
struct QueryArgs {
    actor_id: Option<String>,
    action_type: Option<String>,
    keyword: Option<String>,
    time_from_ms: Option<u64>,
    time_to_ms: Option<u64>,
    limit: u64,
    scan_limit: u64,
}

struct QueryArgsInput {
    actor_id: Option<String>,
    action_type_direct: Option<String>,
    action_type_alias: Option<String>,
    keyword: Option<String>,
    time_from: Option<EpochMillisArg>,
    time_to: Option<EpochMillisArg>,
    limit: Option<u64>,
    scan_limit: Option<u64>,
}

fn parse_time_ms_arg(value: Option<EpochMillisArg>, key: &str) -> Result<Option<u64>> {
    match value {
        None => Ok(None),
        Some(EpochMillisArg::Integer(n)) => Ok(Some(n)),
        Some(EpochMillisArg::String(s)) => {
            if s.chars().all(|c| c.is_ascii_digit()) {
                s.parse::<u64>()
                    .map(Some)
                    .map_err(|_| anyhow!("{key} must be epoch milliseconds (u64)"))
            } else {
                // Try RFC 3339 / ISO 8601 with timezone (e.g. "2026-02-22T10:30:00Z")
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                    return Ok(Some(dt.timestamp_millis() as u64));
                }
                // Try ISO 8601 without timezone, treat as UTC (e.g. "2026-02-22T10:30:00")
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
                    return Ok(Some(ndt.and_utc().timestamp_millis() as u64));
                }
                bail!(
                    "{key} must be epoch milliseconds or ISO 8601 (e.g. \"2026-02-22T10:30:00Z\")"
                )
            }
        }
    }
}

fn parse_query_args_common(input: QueryArgsInput) -> Result<QueryArgs> {
    let QueryArgsInput {
        actor_id,
        action_type_direct,
        action_type_alias,
        keyword,
        time_from,
        time_to,
        limit,
        scan_limit,
    } = input;
    if let (Some(a), Some(b)) = (&action_type_direct, &action_type_alias) {
        if a != b {
            bail!("action_type and event_type must match when both are provided");
        }
    }
    let action_type = action_type_direct.or(action_type_alias);
    let time_from_ms = parse_time_ms_arg(time_from, "time_from")?;
    let time_to_ms = parse_time_ms_arg(time_to, "time_to")?;
    let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT).clamp(1, 100);
    let scan_limit = scan_limit
        .unwrap_or(DEFAULT_SCAN_LIMIT.max(limit))
        .clamp(1, MAX_SCAN_LIMIT);

    if let (Some(from), Some(to)) = (time_from_ms, time_to_ms) {
        if from > to {
            bail!("time_from must be <= time_to");
        }
    }

    Ok(QueryArgs {
        actor_id,
        action_type,
        keyword,
        time_from_ms,
        time_to_ms,
        limit,
        scan_limit,
    })
}

fn parse_query_args(params: PunkgoQueryParams) -> Result<QueryArgs> {
    parse_query_args_common(QueryArgsInput {
        actor_id: params.actor_id,
        action_type_direct: params.action_type,
        action_type_alias: params.event_type,
        keyword: params.keyword,
        time_from: params.time_from,
        time_to: params.time_to,
        limit: params.limit,
        scan_limit: params.scan_limit,
    })
}

fn parse_stats_query_args(params: PunkgoStatsParams) -> Result<QueryArgs> {
    parse_query_args_common(QueryArgsInput {
        actor_id: params.actor_id,
        action_type_direct: params.action_type,
        action_type_alias: params.event_type,
        keyword: params.keyword,
        time_from: params.time_from,
        time_to: params.time_to,
        limit: None,
        scan_limit: params.scan_limit,
    })
}

fn parse_event_timestamp_ms(event: &Value) -> Option<u64> {
    match event.get("timestamp")? {
        Value::String(s) if s.chars().all(|c| c.is_ascii_digit()) => s.parse::<u64>().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn event_text_blob(event: &Value) -> String {
    let mut parts = Vec::new();
    for key in ["id", "event_hash", "actor_id", "action_type", "target"] {
        if let Some(v) = event.get(key) {
            if let Some(s) = v.as_str() {
                parts.push(s.to_lowercase());
            } else {
                parts.push(v.to_string().to_lowercase());
            }
        }
    }
    if let Some(payload) = event.get("payload") {
        parts.push(payload.to_string().to_lowercase());
    }
    parts.join(" ")
}

fn event_matches_filters(event: &Value, q: &QueryArgs) -> bool {
    if let Some(action_type) = &q.action_type {
        let got = event
            .get("action_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if got != action_type {
            return false;
        }
    }

    if q.time_from_ms.is_some() || q.time_to_ms.is_some() {
        if let Some(ts) = parse_event_timestamp_ms(event) {
            if let Some(from) = q.time_from_ms {
                if ts < from {
                    return false;
                }
            }
            if let Some(to) = q.time_to_ms {
                if ts > to {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    if let Some(keyword) = &q.keyword {
        let needle = keyword.to_lowercase();
        if !event_text_blob(event).contains(&needle) {
            return false;
        }
    }

    true
}

fn get_events_from_payload(payload: &Value) -> Result<Vec<Value>> {
    let events = payload
        .get("events")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("backend query payload missing 'events' array"))?;
    Ok(events.clone())
}

fn apply_query_filters(payload: &Value, q: &QueryArgs) -> Result<Value> {
    let events = get_events_from_payload(payload)?;
    let scanned_count = events.len() as u64;
    let mut filtered: Vec<Value> = events
        .into_iter()
        .filter(|e| event_matches_filters(e, q))
        .collect();
    let matched_count = filtered.len() as u64;
    filtered.truncate(q.limit as usize);

    Ok(json!({
        "events": filtered,
        "filters": {
            "actor_id": q.actor_id,
            "action_type": q.action_type,
            "keyword": q.keyword,
            "time_from": q.time_from_ms,
            "time_to": q.time_to_ms,
            "limit": q.limit,
            "scan_limit": q.scan_limit
        },
        "scan": {
            "scanned_count": scanned_count,
            "matched_count": matched_count,
            "returned_count": (filtered.len() as u64),
            "note": "Filters are applied to the recent kernel window returned by read_events (no pagination yet)."
        }
    }))
}

fn build_stats(sample_events: &[Value]) -> Value {
    let mut by_action_type: BTreeMap<String, u64> = BTreeMap::new();
    let mut by_actor: BTreeMap<String, u64> = BTreeMap::new();
    let mut timeline_days: BTreeMap<u64, u64> = BTreeMap::new();

    let mut min_ts: Option<u64> = None;
    let mut max_ts: Option<u64> = None;

    for event in sample_events {
        if let Some(action_type) = event.get("action_type").and_then(Value::as_str) {
            *by_action_type.entry(action_type.to_string()).or_default() += 1;
        }
        if let Some(actor_id) = event.get("actor_id").and_then(Value::as_str) {
            *by_actor.entry(actor_id.to_string()).or_default() += 1;
        }
        if let Some(ts) = parse_event_timestamp_ms(event) {
            min_ts = Some(min_ts.map_or(ts, |m| m.min(ts)));
            max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
            let day_start = (ts / MS_PER_DAY) * MS_PER_DAY;
            *timeline_days.entry(day_start).or_default() += 1;
        }
    }

    let timeline_by_day_utc: Vec<Value> = timeline_days
        .into_iter()
        .map(|(day_start_ms, count)| json!({ "day_start_ms": day_start_ms, "count": count }))
        .collect();

    json!({
        "sample_event_count": sample_events.len(),
        "by_action_type": by_action_type,
        "by_actor": by_actor,
        "timeline_by_day_utc": timeline_by_day_utc,
        "time_range_ms": {
            "earliest": min_ts,
            "latest": max_ts
        }
    })
}

fn ok_tool_result(data: Value) -> CallToolResult {
    CallToolResult::structured(data)
}

fn err_tool_result(message: String) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "error": message
    }))
}

fn finalize_tool_call(result: Result<CallToolResult>) -> Result<CallToolResult> {
    match result {
        Ok(output) => Ok(output),
        Err(err) => Ok(err_tool_result(err.to_string())),
    }
}

pub fn punkgo_ping(backend: &dyn KernelBackend) -> Result<CallToolResult> {
    finalize_tool_call(backend.ping().map(ok_tool_result))
}

pub fn punkgo_log(backend: &dyn KernelBackend, params: PunkgoLogParams) -> Result<CallToolResult> {
    let PunkgoLogParams {
        event_type,
        content,
        metadata,
        source,
    } = params;
    let actor_id = "root".to_string();
    let target = "mcp/punkgo/log".to_string();
    let source = source.unwrap_or_else(|| "punkgo-jack".to_string());
    let metadata = metadata.map(|m| Value::Object(m.into_iter().collect()));

    let mut payload = Map::new();
    payload.insert(
        "schema".to_string(),
        Value::String("punkgo-jack-log-v1".to_string()),
    );
    payload.insert("event_type".to_string(), Value::String(event_type));
    payload.insert("content".to_string(), Value::String(content));
    payload.insert("source".to_string(), Value::String(source));
    if let Some(metadata) = metadata {
        payload.insert("metadata".to_string(), metadata);
    }

    let result = backend
        .log_observe(actor_id.clone(), target.clone(), Value::Object(payload))
        .map(|receipt| {
            ok_tool_result(json!({
                "receipt": receipt,
                "mapping": {
                    "kernel_action_type": "observe",
                    "actor_id": actor_id,
                    "target": target
                }
            }))
        });

    finalize_tool_call(result)
}

pub fn punkgo_query(
    backend: &dyn KernelBackend,
    params: PunkgoQueryParams,
) -> Result<CallToolResult> {
    let result = parse_query_args(params).and_then(|q| {
        backend
            .query(q.actor_id.clone(), Some(q.scan_limit))
            .and_then(|payload| apply_query_filters(&payload, &q).map(ok_tool_result))
    });

    finalize_tool_call(result)
}

/// Resolve `log_index` from `event_id` by querying the backend for matching events.
fn resolve_log_index_by_event_id(backend: &dyn KernelBackend, event_id: &str) -> Result<u64> {
    let payload = backend.query(None, Some(MAX_SCAN_LIMIT))?;
    let events = payload
        .get("events")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("backend query returned no events array"))?;

    for event in events {
        let id = event.get("id").and_then(Value::as_str).unwrap_or_default();
        if id == event_id {
            return event
                .get("log_index")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("event '{event_id}' has no log_index field"));
        }
    }

    bail!("no event found with id '{event_id}' in the recent kernel window (scanned {MAX_SCAN_LIMIT} events)")
}

pub fn punkgo_verify(
    backend: &dyn KernelBackend,
    params: PunkgoVerifyParams,
) -> Result<CallToolResult> {
    let inferred_consistency = matches!(params.mode.as_ref(), Some(VerifyMode::Consistency))
        || (params.mode.is_none() && params.old_size.is_some());

    let result = if inferred_consistency {
        let Some(old_size) = params.old_size else {
            return finalize_tool_call(Err(anyhow!("old_size is required for consistency proof")));
        };
        let Some(tree_size) = params.tree_size else {
            return finalize_tool_call(Err(anyhow!("tree_size is required for consistency proof")));
        };
        backend
            .verify_consistency(old_size, tree_size)
            .map(|proof| {
                ok_tool_result(json!({
                    "mode": "consistency",
                    "proof": proof
                }))
            })
    } else {
        // Resolve log_index: explicit param takes priority, then event_id lookup.
        let log_index = match (params.log_index, &params.event_id) {
            (Some(idx), _) => Ok(idx),
            (None, Some(eid)) => resolve_log_index_by_event_id(backend, eid),
            (None, None) => Err(anyhow!(
                "log_index or event_id is required for inclusion proof"
            )),
        };
        log_index.and_then(|idx| {
            let tree_size = params.tree_size;
            backend.verify(idx, tree_size).map(|proof| {
                ok_tool_result(json!({
                    "mode": "inclusion",
                    "proof": proof
                }))
            })
        })
    };

    finalize_tool_call(result)
}

pub fn punkgo_stats(
    backend: &dyn KernelBackend,
    params: PunkgoStatsParams,
) -> Result<CallToolResult> {
    let result = parse_stats_query_args(params).and_then(|q| {
        let total = backend.stats()?;
        let query_payload = backend.query(q.actor_id.clone(), Some(q.scan_limit))?;
        let filtered_query = apply_query_filters(&query_payload, &q)?;
        let sample_events = filtered_query
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let total_count = total.get("event_count").cloned().unwrap_or(Value::Null);
        let derived = build_stats(&sample_events);

        Ok(ok_tool_result(json!({
            "event_count_total": total_count,
            "derived_sample_stats": derived,
            "sample_scope": filtered_query.get("scan").cloned().unwrap_or(Value::Null),
            "filters": filtered_query.get("filters").cloned().unwrap_or(Value::Null),
            "kernel_stats_raw": total
        })))
    });

    finalize_tool_call(result)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoSessionReceiptParams {
    /// Optional session_id to query. If omitted, uses the current active session.
    session_id: Option<String>,
}

pub fn punkgo_session_receipt(
    backend: &dyn KernelBackend,
    params: PunkgoSessionReceiptParams,
) -> Result<CallToolResult> {
    let result = (|| -> Result<CallToolResult> {
        // Resolve session_id.
        let session_id = if let Some(sid) = params.session_id {
            sid
        } else if let Ok(Some(state)) = crate::session::latest_session() {
            state.session_id
        } else {
            return Ok(ok_tool_result(json!({
                "error": "no session_id provided and no active session"
            })));
        };

        let payload = backend.query(None, Some(100))?;
        let events = payload
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        // Filter by session_id in payload metadata.
        let session_events: Vec<&Value> = events
            .iter()
            .filter(|e| {
                e.get("payload")
                    .and_then(|p| p.get("metadata"))
                    .and_then(|m| m.get("punkgo_session_id"))
                    .and_then(Value::as_str)
                    .is_some_and(|sid| sid == session_id)
            })
            .collect();

        let event_count = session_events.len();
        let mut energy_consumed: u64 = 0;
        let mut event_types: BTreeMap<String, u64> = BTreeMap::new();

        for evt in &session_events {
            if let Some(cost) = evt.get("settled_energy").and_then(Value::as_u64) {
                energy_consumed += cost;
            }
            if let Some(action_type) = evt.get("action_type").and_then(Value::as_str) {
                *event_types.entry(action_type.to_string()).or_default() += 1;
            }
        }

        // Get checkpoint.
        let checkpoint = backend.checkpoint().ok();
        let tree_size = checkpoint
            .as_ref()
            .and_then(|cp| cp.get("tree_size"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let root_hash = checkpoint
            .as_ref()
            .and_then(|cp| cp.get("root_hash"))
            .and_then(Value::as_str)
            .unwrap_or("unavailable")
            .to_string();

        Ok(ok_tool_result(json!({
            "session_id": session_id,
            "event_count": event_count,
            "energy_consumed": energy_consumed,
            "event_types": event_types,
            "merkle": {
                "tree_size": tree_size,
                "root_hash": root_hash
            }
        })))
    })();

    finalize_tool_call(result)
}

pub fn punkgo_checkpoint(backend: &dyn KernelBackend) -> Result<CallToolResult> {
    let result = backend.checkpoint().map(|cp| {
        ok_tool_result(json!({
            "checkpoint": cp
        }))
    });

    finalize_tool_call(result)
}

// ---------------------------------------------------------------------------
// Lane D — transcript index tools
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoSessionListParams {
    /// ISO 8601 lower bound on `started_at` (e.g. `"2026-04-01T00:00:00Z"`).
    pub since: Option<String>,
    /// Filter sessions that contain at least one turn with this model variant.
    pub model_variant: Option<String>,
    /// Filter by source label (e.g. `"claude-code"`).
    pub source: Option<String>,
    pub limit: Option<u64>,
    /// Pagination cursor: `session_id` of the last row returned by a prior call.
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoSessionDetailParams {
    pub session_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoTurnDetailParams {
    pub turn_uuid: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoHiddenTokensParams {
    pub session_id: Option<String>,
    /// ISO 8601 lower bound on the turn timestamp.
    pub since: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoModelVariantsParams {}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PunkgoReindexParams {
    pub full: Option<bool>,
    pub since: Option<String>,
    pub session: Option<String>,
    pub dry_run: Option<bool>,
}

pub fn punkgo_session_list(params: PunkgoSessionListParams) -> Result<CallToolResult> {
    let result = (|| -> Result<CallToolResult> {
        let limit = params.limit.unwrap_or(DEFAULT_QUERY_LIMIT).clamp(1, 100) as usize;
        let conn = crate::index::open_jack_db()?;
        let rows = crate::index::sessions::list_sessions(
            &conn,
            params.since.as_deref(),
            params.model_variant.as_deref(),
            params.source.as_deref(),
            limit,
            params.cursor.as_deref(),
        )?;
        let next_cursor = rows.last().map(|r| r.session_id.clone());
        Ok(ok_tool_result(json!({
            "sessions": rows,
            "next_cursor": next_cursor,
            "count": rows.len(),
        })))
    })();
    finalize_tool_call(result)
}

pub fn punkgo_session_detail(params: PunkgoSessionDetailParams) -> Result<CallToolResult> {
    let result = (|| -> Result<CallToolResult> {
        let conn = crate::index::open_jack_db()?;
        let session = crate::index::sessions::get_session(&conn, &params.session_id)?;
        let Some(session) = session else {
            return Ok(err_tool_result(format!(
                "session {} not found",
                params.session_id
            )));
        };
        let turns = crate::index::turns::list_turns_for_session(&conn, &params.session_id)?;
        // Distinct variants seen across this session's signature rows.
        let mut stmt = conn.prepare(
            r#"
            SELECT DISTINCT extracted_model_variant
            FROM thinking_signatures
            WHERE turn_uuid IN (SELECT turn_uuid FROM turns WHERE session_id = ?1)
              AND extracted_model_variant IS NOT NULL
            "#,
        )?;
        let variants: Vec<String> = stmt
            .query_map([&params.session_id], |r| r.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(ok_tool_result(json!({
            "session": session,
            "turns": turns,
            "model_variants_seen": variants,
        })))
    })();
    finalize_tool_call(result)
}

pub fn punkgo_turn_detail(params: PunkgoTurnDetailParams) -> Result<CallToolResult> {
    let result = (|| -> Result<CallToolResult> {
        let conn = crate::index::open_jack_db()?;
        let turn = crate::index::turns::get_turn(&conn, &params.turn_uuid)?;
        let Some(turn) = turn else {
            return Ok(err_tool_result(format!(
                "turn {} not found",
                params.turn_uuid
            )));
        };
        let signatures =
            crate::index::signatures::list_signatures_for_turn(&conn, &params.turn_uuid)?;
        // content_blocks_meta is stored as a JSON string; parse so the caller
        // sees real structure rather than a quoted string.
        let cbm: serde_json::Value =
            serde_json::from_str(&turn.content_blocks_meta).unwrap_or(json!([]));
        Ok(ok_tool_result(json!({
            "turn": turn,
            "content_blocks_meta": cbm,
            "thinking_signatures": signatures,
        })))
    })();
    finalize_tool_call(result)
}

pub fn punkgo_hidden_tokens(params: PunkgoHiddenTokensParams) -> Result<CallToolResult> {
    let result = (|| -> Result<CallToolResult> {
        let conn = crate::index::open_jack_db()?;
        let mut sql = String::from(
            r#"
            SELECT COALESCE(SUM(estimated_hidden_tokens), 0),
                   COALESCE(SUM(thinking_block_count), 0),
                   COUNT(DISTINCT session_id)
            FROM turns WHERE 1=1
            "#,
        );
        let mut args: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(sid) = &params.session_id {
            sql.push_str(" AND session_id = ?");
            args.push(rusqlite::types::Value::Text(sid.clone()));
        }
        if let Some(since) = &params.since {
            sql.push_str(" AND timestamp >= ?");
            args.push(rusqlite::types::Value::Text(since.clone()));
        }
        let (hidden, thinking_blocks, session_count): (i64, i64, i64) =
            conn.query_row(&sql, rusqlite::params_from_iter(args.iter()), |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;

        // Breakdown by model variant.
        let mut sql2 = String::from(
            r#"
            SELECT model_variant, COALESCE(SUM(estimated_hidden_tokens), 0)
            FROM turns
            WHERE model_variant IS NOT NULL
            "#,
        );
        let mut args2: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(sid) = &params.session_id {
            sql2.push_str(" AND session_id = ?");
            args2.push(rusqlite::types::Value::Text(sid.clone()));
        }
        if let Some(since) = &params.since {
            sql2.push_str(" AND timestamp >= ?");
            args2.push(rusqlite::types::Value::Text(since.clone()));
        }
        sql2.push_str(" GROUP BY model_variant");
        let mut stmt = conn.prepare(&sql2)?;
        let mut breakdown: BTreeMap<String, i64> = BTreeMap::new();
        let iter = stmt.query_map(rusqlite::params_from_iter(args2.iter()), |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        for row in iter.flatten() {
            breakdown.insert(row.0, row.1);
        }

        Ok(ok_tool_result(json!({
            "total_hidden_tokens_est": hidden,
            "total_thinking_blocks": thinking_blocks,
            "session_count": session_count,
            "breakdown_by_variant": breakdown,
        })))
    })();
    finalize_tool_call(result)
}

pub fn punkgo_model_variants(_params: PunkgoModelVariantsParams) -> Result<CallToolResult> {
    let result = (|| -> Result<CallToolResult> {
        let conn = crate::index::open_jack_db()?;
        let stats = crate::index::signatures::list_distinct_variants(&conn)?;
        Ok(ok_tool_result(json!({
            "variants": stats,
            "count": stats.len(),
        })))
    })();
    finalize_tool_call(result)
}

pub fn punkgo_reindex(params: PunkgoReindexParams) -> Result<CallToolResult> {
    let result = (|| -> Result<CallToolResult> {
        let opts = crate::indexer::ReindexOptions {
            full: params.full.unwrap_or(false),
            since: params.since,
            session: params.session,
            dry_run: params.dry_run.unwrap_or(false),
        };
        if !opts.full && opts.since.is_none() && opts.session.is_none() {
            return Ok(err_tool_result(
                "punkgo_reindex requires at least one of: full=true, since, session".to_string(),
            ));
        }
        let report = crate::indexer::run_reindex(opts)?;
        Ok(ok_tool_result(serde_json::to_value(report)?))
    })();
    finalize_tool_call(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MockBackend;

    #[test]
    fn verify_consistency_mode_works_with_mock_backend() {
        let backend = MockBackend;
        let out = punkgo_verify(
            &backend,
            PunkgoVerifyParams {
                mode: Some(VerifyMode::Consistency),
                old_size: Some(1),
                tree_size: Some(2),
                ..Default::default()
            },
        )
        .expect("tool call should succeed");
        assert_eq!(out.is_error, Some(false));
        assert_eq!(
            out.structured_content
                .as_ref()
                .and_then(|v| v.get("mode"))
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "consistency"
        );
    }

    #[test]
    fn query_keyword_filter_reduces_results() {
        let backend = MockBackend;
        let out = punkgo_query(
            &backend,
            PunkgoQueryParams {
                keyword: Some("not-present".to_string()),
                ..Default::default()
            },
        )
        .expect("tool call should succeed");
        assert_eq!(out.is_error, Some(false));
        let events = out
            .structured_content
            .as_ref()
            .and_then(|v| v.get("events"))
            .and_then(Value::as_array)
            .expect("events array");
        assert!(events.is_empty());
    }

    #[test]
    fn punkgo_log_returns_receipt() {
        let backend = MockBackend;
        let out = punkgo_log(
            &backend,
            PunkgoLogParams {
                event_type: "user_note".to_string(),
                content: "hello".to_string(),
                metadata: None,
                source: None,
            },
        )
        .expect("tool call should succeed");
        assert_eq!(out.is_error, Some(false));
        assert!(out
            .structured_content
            .as_ref()
            .and_then(|v| v.get("receipt"))
            .and_then(|v| v.get("event_id"))
            .is_some_and(Value::is_string));
    }

    #[test]
    fn checkpoint_tool_returns_checkpoint() {
        let backend = MockBackend;
        let out = punkgo_checkpoint(&backend).expect("tool call should succeed");
        assert_eq!(out.is_error, Some(false));
        assert!(out
            .structured_content
            .as_ref()
            .and_then(|v| v.get("checkpoint"))
            .and_then(|v| v.get("tree_size"))
            .is_some_and(Value::is_number));
    }

    #[test]
    fn parse_time_ms_arg_accepts_epoch_integer() {
        let result = parse_time_ms_arg(Some(EpochMillisArg::Integer(1700000000000)), "t");
        assert_eq!(result.unwrap(), Some(1700000000000));
    }

    #[test]
    fn parse_time_ms_arg_accepts_epoch_string() {
        let result = parse_time_ms_arg(Some(EpochMillisArg::String("1700000000000".into())), "t");
        assert_eq!(result.unwrap(), Some(1700000000000));
    }

    #[test]
    fn parse_time_ms_arg_accepts_rfc3339() {
        let result = parse_time_ms_arg(
            Some(EpochMillisArg::String("2026-02-22T10:30:00Z".into())),
            "t",
        );
        assert_eq!(result.unwrap(), Some(1771756200000));
    }

    #[test]
    fn parse_time_ms_arg_accepts_rfc3339_with_offset() {
        let result = parse_time_ms_arg(
            Some(EpochMillisArg::String("2026-02-22T18:30:00+08:00".into())),
            "t",
        );
        // 18:30 +08:00 == 10:30 UTC
        assert_eq!(result.unwrap(), Some(1771756200000));
    }

    #[test]
    fn parse_time_ms_arg_accepts_naive_datetime_as_utc() {
        let result = parse_time_ms_arg(
            Some(EpochMillisArg::String("2026-02-22T10:30:00".into())),
            "t",
        );
        assert_eq!(result.unwrap(), Some(1771756200000));
    }

    #[test]
    fn parse_time_ms_arg_rejects_garbage() {
        let result = parse_time_ms_arg(
            Some(EpochMillisArg::String("not-a-date".into())),
            "time_from",
        );
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("ISO 8601"));
    }

    #[test]
    fn parse_time_ms_arg_none_returns_none() {
        assert_eq!(parse_time_ms_arg(None, "t").unwrap(), None);
    }

    #[test]
    fn verify_with_event_id_resolves_log_index() {
        let backend = MockBackend;
        let out = punkgo_verify(
            &backend,
            PunkgoVerifyParams {
                event_id: Some("evt_mock_0001".to_string()),
                ..Default::default()
            },
        )
        .expect("tool call should succeed");
        assert_eq!(out.is_error, Some(false));
        assert_eq!(
            out.structured_content
                .as_ref()
                .and_then(|v| v.get("mode"))
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "inclusion"
        );
    }

    #[test]
    fn session_receipt_without_session_returns_error() {
        let backend = MockBackend;
        let out = punkgo_session_receipt(&backend, PunkgoSessionReceiptParams { session_id: None })
            .expect("tool call should succeed");
        // Without a session file or explicit session_id, should return error info.
        assert!(
            out.structured_content
                .as_ref()
                .and_then(|v| v.get("error"))
                .is_some()
                || out
                    .structured_content
                    .as_ref()
                    .and_then(|v| v.get("session_id"))
                    .is_some()
        );
    }

    #[test]
    fn session_receipt_with_explicit_session_id() {
        let backend = MockBackend;
        let out = punkgo_session_receipt(
            &backend,
            PunkgoSessionReceiptParams {
                session_id: Some("test-session-123".to_string()),
            },
        )
        .expect("tool call should succeed");
        assert_eq!(out.is_error, Some(false));
        assert_eq!(
            out.structured_content
                .as_ref()
                .and_then(|v| v.get("session_id"))
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "test-session-123"
        );
    }

    #[test]
    fn verify_event_id_not_found_returns_error() {
        let backend = MockBackend;
        let out = punkgo_verify(
            &backend,
            PunkgoVerifyParams {
                event_id: Some("nonexistent_id".to_string()),
                ..Default::default()
            },
        )
        .expect("tool call should succeed");
        assert_eq!(out.is_error, Some(true));
    }
}
