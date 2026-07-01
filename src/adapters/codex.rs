//! Codex CLI rollout parser (Workstream B, P2).
//!
//! Codex records every session to a JSONL "rollout" file at
//! `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl`
//! (`$CODEX_HOME` defaults to `~/.codex`). Unlike Claude Code's transcript
//! (which jack already indexes as metadata-only), the Codex rollout is the
//! authoritative record of the model's input/output — recording it is the
//! core new capability of v0.7.0.
//!
//! This module defines the serde data model for the rollout format and a
//! **dry-run scanner** (`dry_run_scan`) that parses every rollout line and
//! reports what it found *without writing anything*. The dry-run is the P2
//! acceptance gate (plan test-gate #1): it proves the serde model round-trips
//! all real data (0 hard parse errors), surfaces the true shape distribution,
//! verifies `call_id` linkage, and produces the evidence that decides the
//! content data model (AD3).
//!
//! # Ground truth (measured against 410 local rollout files, 624 MB,
//! codex-cli 0.77.0 – 0.142.x, 2026-07-01)
//!
//! Envelope: `{ "timestamp": <iso>, "type": <kind>, "payload": {...} }`.
//! `type` (snake_case) is one of: `response_item`, `event_msg`,
//! `turn_context`, `session_meta`, `compacted`.
//!
//! `response_item.payload.type` (11 observed shapes):
//! `message`, `reasoning`, `function_call`, `function_call_output`,
//! `custom_tool_call`, `custom_tool_call_output`, `web_search_call`,
//! `ghost_snapshot`, `tool_search_call`, `tool_search_output`,
//! `image_generation_call`. New shapes appear across codex versions, so
//! deserialization is deliberately tolerant: unknown `response_item` shapes
//! deserialize to [`ResponseItem::Unknown`] rather than aborting the scan.
//!
//! `function_call` always carries `call_id` (`call_...`); some versions also
//! carry an OpenAI response id `id` (`fc_...`). **Linkage between a call and
//! its output is on `call_id`, never `id`** — verified 1:1 within every
//! sampled file. `function_call_output.output` is usually a string but is
//! occasionally an array, so it is modeled as an opaque [`serde_json::Value`].

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

/// One line of a rollout JSONL file: a timestamped, typed envelope wrapping
/// a `payload` whose shape depends on `kind`.
///
/// Parsed permissively: `payload` stays a raw [`Value`] so a single unusual
/// line never fails the whole scan. Callers dispatch on [`RolloutLine::kind`]
/// and deserialize `payload` into the appropriate typed struct.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RolloutLine {
    #[serde(default)]
    pub timestamp: Option<String>,
    /// Envelope discriminator: `response_item` / `event_msg` / `turn_context`
    /// / `session_meta` / `compacted` / (future).
    #[serde(rename = "type")]
    pub kind: String,
    pub payload: Value,
}

// ---------------------------------------------------------------------------
// response_item payload shapes
// ---------------------------------------------------------------------------

/// The `payload` of a `response_item` envelope. Internally tagged on the
/// payload's own `type` field.
///
/// [`ResponseItem::Unknown`] (via `#[serde(other)]`) absorbs any shape not
/// modeled here so version drift never turns into a hard parse error — the
/// scanner counts it and moves on.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseItem {
    /// A user/assistant/developer message with typed content parts.
    Message(CodexMessage),
    /// Model reasoning. `encrypted_content` is opaque (never decrypted);
    /// `summary` is a short visible header, not the full chain of thought.
    Reasoning(Reasoning),
    /// A native tool/function invocation. Linkage key: `call_id`.
    FunctionCall(FunctionCall),
    /// The result of a `function_call`, joined on `call_id`.
    FunctionCallOutput(FunctionCallOutput),
    /// A "custom" tool invocation (e.g. `apply_patch`). Linkage key: `call_id`.
    CustomToolCall(CustomToolCall),
    /// The result of a `custom_tool_call`, joined on `call_id`.
    CustomToolCallOutput(CustomToolCallOutput),
    /// A built-in web search invocation.
    WebSearchCall(WebSearchCall),
    /// A git "ghost commit" snapshot marker.
    GhostSnapshot(GhostSnapshot),
    /// Skill/tool discovery search (rare).
    ToolSearchCall(RareItem),
    /// Result of a tool search (rare).
    ToolSearchOutput(RareItem),
    /// Image generation invocation (rare).
    ImageGenerationCall(RareItem),
    /// Any response_item shape not modeled above (forward-compat).
    #[serde(other)]
    Unknown,
}

/// A message payload: `{ role, content: [...] }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CodexMessage {
    /// `user` | `assistant` | `developer`.
    pub role: String,
    #[serde(default)]
    pub content: Vec<MessageContent>,
}

/// One content part inside a message. Tagged on its own `type`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageContent {
    /// User/developer input text.
    InputText { text: String },
    /// Assistant output text.
    OutputText { text: String },
    /// Inline image (usually a `data:` URL). Body is captured but large.
    InputImage {
        #[serde(default)]
        image_url: Option<String>,
    },
    /// Any content part shape not modeled above.
    #[serde(other)]
    Unknown,
}

/// Model reasoning payload. Chain of thought is delivered encrypted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Reasoning {
    /// Opaque encrypted chain of thought. Never decrypted (≈ Claude thinking
    /// signature). May be absent on some versions.
    #[serde(default)]
    pub encrypted_content: Option<String>,
    /// Short visible reasoning summary (`[{type:"summary_text", text}]`).
    #[serde(default)]
    pub summary: Vec<ReasoningSummary>,
    /// Structured reasoning content; frequently `null`.
    #[serde(default)]
    pub content: Option<Vec<Value>>,
}

/// One `reasoning.summary` element.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReasoningSummary {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub text: String,
}

/// A native function/tool call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionCall {
    /// The join key to the matching `function_call_output` (`call_...`).
    pub call_id: String,
    /// OpenAI response item id (`fc_...`), present on some versions. NOT a
    /// linkage key.
    #[serde(default)]
    pub id: Option<String>,
    pub name: String,
    /// Tool arguments, delivered as a JSON-encoded string.
    #[serde(default)]
    pub arguments: String,
    #[serde(default)]
    pub status: Option<String>,
}

/// The output of a `function_call`, joined on `call_id`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionCallOutput {
    pub call_id: String,
    /// Usually a string; occasionally an array. Kept opaque.
    #[serde(default)]
    pub output: Value,
}

/// A custom tool call (e.g. `apply_patch`). `input` is a raw string.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomToolCall {
    pub call_id: String,
    #[serde(default)]
    pub id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub input: String,
    #[serde(default)]
    pub status: Option<String>,
}

/// The output of a `custom_tool_call`, joined on `call_id`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomToolCallOutput {
    pub call_id: String,
    #[serde(default)]
    pub output: Value,
}

/// A built-in web search invocation. `action` is `{type:"search", query}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct WebSearchCall {
    #[serde(default)]
    pub action: Value,
    #[serde(default)]
    pub status: Option<String>,
}

/// A git ghost-commit snapshot marker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct GhostSnapshot {
    #[serde(default)]
    pub ghost_commit: Value,
}

/// A minimally-modeled rare response_item (`tool_search_*`,
/// `image_generation_call`). We capture the optional `call_id` for linkage
/// bookkeeping and ignore the rest; full modeling waits until these become
/// common enough to matter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RareItem {
    #[serde(default)]
    pub call_id: Option<String>,
}

// ---------------------------------------------------------------------------
// session_meta / turn_context payloads
// ---------------------------------------------------------------------------

/// `session_meta.payload` — one per file, carrying session identity + origin.
/// The large `instructions` (system prompt) field is intentionally not
/// modeled; serde ignores it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SessionMeta {
    pub id: String,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub originator: Option<String>,
    #[serde(default)]
    pub cli_version: Option<String>,
    /// Session origin. Polymorphic across codex versions: a plain string
    /// (`cli` / `exec` / `vscode`) or an object (e.g. `{"subagent":"review"}`
    /// for subagent sessions). Kept opaque so both forms parse.
    #[serde(default)]
    pub source: Option<Value>,
    #[serde(default)]
    pub model_provider: Option<String>,
    #[serde(default)]
    pub git: Option<GitInfo>,
}

/// Git provenance embedded in `session_meta`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct GitInfo {
    #[serde(default)]
    pub commit_hash: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub repository_url: Option<String>,
}

/// `turn_context.payload` — per-turn model/effort/sandbox context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TurnContext {
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub approval_policy: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

// ---------------------------------------------------------------------------
// Filesystem discovery
// ---------------------------------------------------------------------------

/// Resolve the Codex sessions root: `$CODEX_HOME/sessions`, falling back to
/// `~/.codex/sessions`.
pub fn codex_sessions_root() -> Result<PathBuf> {
    if let Ok(codex_home) = std::env::var("CODEX_HOME") {
        if !codex_home.trim().is_empty() {
            return Ok(PathBuf::from(codex_home).join("sessions"));
        }
    }
    let home = crate::session::home_dir().ok_or_else(|| anyhow!("cannot resolve home directory"))?;
    Ok(home.join(".codex").join("sessions"))
}

/// Recursively collect `rollout-*.jsonl` files under `root`.
pub fn walk_rollouts(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_rollouts_into(root, &mut out);
    out
}

fn walk_rollouts_into(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_rollouts_into(&path, out);
        } else if is_rollout_file(&path) {
            out.push(path);
        }
    }
}

/// True for files named `rollout-*.jsonl`.
fn is_rollout_file(path: &Path) -> bool {
    let is_jsonl = path.extension().and_then(|e| e.to_str()) == Some("jsonl");
    let named_rollout = path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with("rollout-"))
        .unwrap_or(false);
    is_jsonl && named_rollout
}

// ---------------------------------------------------------------------------
// Dry-run report
// ---------------------------------------------------------------------------

/// What a dry-run scan found. Pure counts + evidence; no state is written.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodexDryRunReport {
    pub files_scanned: usize,
    /// Files that could not be opened / read.
    pub files_failed: usize,
    /// Files opened but cut short by a genuine mid-file I/O error (rare). The
    /// lines already read are still counted; this makes the truncation visible
    /// rather than silent (a non-UTF-8 line does NOT count here — it is decoded
    /// leniently and processed, never truncating the remainder).
    pub files_truncated: usize,
    pub lines_total: usize,
    pub lines_blank: usize,
    /// Lines whose JSON could not be parsed into the envelope. **Acceptance
    /// gate: this must be 0 on real data.**
    pub hard_parse_errors: usize,

    /// Envelope `type` → count.
    pub envelope_type_counts: BTreeMap<String, usize>,
    /// `response_item.payload.type` → count (raw, authoritative).
    pub response_item_type_counts: BTreeMap<String, usize>,
    /// Known `response_item` tag whose typed struct failed to deserialize
    /// (raw type → count). Signals a modeled shape drifting. Should be 0.
    pub typed_parse_warnings: BTreeMap<String, usize>,
    /// `response_item` shapes that fell through to `Unknown` (unmodeled).
    pub unknown_response_items: usize,

    /// message `role` → count.
    pub message_role_counts: BTreeMap<String, usize>,
    /// message content part `type` → count.
    pub message_content_type_counts: BTreeMap<String, usize>,

    pub reasoning_total: usize,
    pub reasoning_with_encrypted: usize,
    pub reasoning_with_summary: usize,

    // --- call_id linkage (function_call + custom_tool_call, per file) ---
    pub tool_calls: usize,
    pub tool_outputs: usize,
    pub calls_matched: usize,
    /// Calls with no matching output in the same file.
    pub orphan_calls: usize,
    /// Outputs with no matching call in the same file.
    pub orphan_outputs: usize,
    /// function_call/custom_tool_call carrying an `fc_...` id (informational).
    pub calls_with_fc_id: usize,
    /// tool outputs whose `output` was not a plain string (array/object).
    pub outputs_non_string: usize,

    // --- AD3 turn-granularity evidence ---
    /// Number of user-message boundaries seen (≈ number of user turns).
    pub user_message_turns: usize,
    /// Max response_items observed between two consecutive user messages —
    /// a single conversational turn. If >> 1, one jack turn spans many
    /// response_items and the single `content_blob_hash` column is
    /// insufficient (AD3 → `turn_content` table).
    pub max_items_per_turn: usize,
    /// Sum of response_items across all user-bounded turns (for averaging).
    pub items_in_turns: usize,

    pub session_meta_count: usize,
    pub duration_seconds: f64,
}

impl CodexDryRunReport {
    fn bump(map: &mut BTreeMap<String, usize>, key: &str) {
        *map.entry(key.to_string()).or_insert(0) += 1;
    }

    /// Mean response_items per user-bounded turn (AD3 headline number).
    pub fn avg_items_per_turn(&self) -> f64 {
        if self.user_message_turns == 0 {
            0.0
        } else {
            self.items_in_turns as f64 / self.user_message_turns as f64
        }
    }
}

// ---------------------------------------------------------------------------
// Dry-run scan
// ---------------------------------------------------------------------------

/// Walk every rollout file under `root` and report what a real ingest *would*
/// parse — without opening or touching jack.db. This is the P2 acceptance
/// gate. Individual malformed lines are counted (`hard_parse_errors`) and
/// skipped; a file that cannot be opened bumps `files_failed`; the scan never
/// aborts.
pub fn dry_run_scan(root: &Path) -> Result<CodexDryRunReport> {
    let started = Instant::now();
    let mut report = CodexDryRunReport::default();

    if !root.exists() {
        warn!(path = %root.display(), "codex sessions dir does not exist; nothing to scan");
        report.duration_seconds = started.elapsed().as_secs_f64();
        return Ok(report);
    }

    let files = walk_rollouts(root);
    debug!(file_count = files.len(), "codex dry-run starting");

    for path in &files {
        match scan_one_file(path, &mut report) {
            Ok(()) => report.files_scanned += 1,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "codex rollout file failed to open");
                report.files_failed += 1;
            }
        }
    }

    report.duration_seconds = started.elapsed().as_secs_f64();
    Ok(report)
}

/// Stream one rollout file line-by-line (memory-bounded — rollouts can reach
/// gigabytes), folding each line into `report`. Returns `Err` only if the
/// file cannot be opened; per-line problems are recorded in `report`.
fn scan_one_file(path: &Path, report: &mut CodexDryRunReport) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Per-file linkage state. call_ids are scoped to a single rollout.
    let mut call_ids: HashSet<String> = HashSet::new();
    let mut output_ids: HashSet<String> = HashSet::new();
    // Per-file turn-granularity state.
    let mut items_since_user: usize = 0;
    let mut saw_first_user = false;

    // Read raw bytes per line (not `read_line`, which aborts the whole file on
    // the first non-UTF-8 byte — and Codex captures raw process stdout in
    // `function_call_output`, which is frequently not valid UTF-8). A bad line
    // is decoded leniently and processed on its own; only a genuine I/O error
    // stops the file, and that is counted, never silent.
    let mut line_bytes: Vec<u8> = Vec::new();
    loop {
        line_bytes.clear();
        match reader.read_until(b'\n', &mut line_bytes) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                warn!(path = %path.display(), error = %e, "I/O error, stopping file early");
                report.files_truncated += 1;
                break;
            }
        };
        // Lenient decode: invalid UTF-8 bytes become U+FFFD. A line that is not
        // valid JSON after that (or never was) is recorded as a hard parse
        // error below — it does not truncate the rest of the file.
        let line_cow = String::from_utf8_lossy(&line_bytes);
        let line = line_cow.trim();
        report.lines_total += 1;
        if line.is_empty() {
            report.lines_blank += 1;
            continue;
        }

        let envelope: RolloutLine = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(_) => {
                report.hard_parse_errors += 1;
                continue;
            }
        };

        CodexDryRunReport::bump(&mut report.envelope_type_counts, &envelope.kind);

        match envelope.kind.as_str() {
            "response_item" => fold_response_item(
                envelope.payload,
                report,
                &mut call_ids,
                &mut output_ids,
                &mut items_since_user,
                &mut saw_first_user,
            ),
            "session_meta" => {
                report.session_meta_count += 1;
                // Validate the struct deserializes (evidence it is modeled
                // correctly); ignore the value in dry-run.
                validate_typed::<SessionMeta>(envelope.payload, "session_meta", report);
            }
            "turn_context" => {
                validate_typed::<TurnContext>(envelope.payload, "turn_context", report);
            }
            // event_msg / compacted / future: counted at the envelope level;
            // response_item is the authoritative content source (AD5).
            _ => {}
        }
    }

    // Close out the final turn of the file.
    if saw_first_user {
        report.max_items_per_turn = report.max_items_per_turn.max(items_since_user);
        report.items_in_turns += items_since_user;
    }

    // Fold per-file linkage into the aggregate.
    report.tool_calls += call_ids.len();
    report.tool_outputs += output_ids.len();
    let matched = call_ids.intersection(&output_ids).count();
    report.calls_matched += matched;
    report.orphan_calls += call_ids.difference(&output_ids).count();
    report.orphan_outputs += output_ids.difference(&call_ids).count();

    Ok(())
}

/// Deserialize `payload` into `T` purely to confirm the modeled struct fits
/// the real data; on mismatch, record a typed-parse warning keyed by `tag`.
/// The value is discarded (dry-run writes nothing).
fn validate_typed<T: serde::de::DeserializeOwned>(
    payload: Value,
    tag: &str,
    report: &mut CodexDryRunReport,
) {
    if serde_json::from_value::<T>(payload).is_err() {
        CodexDryRunReport::bump(&mut report.typed_parse_warnings, tag);
    }
}

/// Fold a single `response_item` payload into the report, updating linkage and
/// turn-granularity state.
fn fold_response_item(
    payload: Value,
    report: &mut CodexDryRunReport,
    call_ids: &mut HashSet<String>,
    output_ids: &mut HashSet<String>,
    items_since_user: &mut usize,
    saw_first_user: &mut bool,
) {
    // Raw type first — authoritative even if typed parse later fails.
    let raw_type = payload
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("(missing)")
        .to_string();
    CodexDryRunReport::bump(&mut report.response_item_type_counts, &raw_type);

    // Every response_item is part of the current turn's item stream.
    *items_since_user += 1;

    let item: ResponseItem = match serde_json::from_value(payload) {
        Ok(i) => i,
        Err(_) => {
            // Known-looking tag whose modeled struct didn't fit.
            CodexDryRunReport::bump(&mut report.typed_parse_warnings, &raw_type);
            return;
        }
    };

    match item {
        ResponseItem::Message(m) => {
            CodexDryRunReport::bump(&mut report.message_role_counts, &m.role);
            for part in &m.content {
                let ct = match part {
                    MessageContent::InputText { .. } => "input_text",
                    MessageContent::OutputText { .. } => "output_text",
                    MessageContent::InputImage { .. } => "input_image",
                    MessageContent::Unknown => "unknown",
                };
                CodexDryRunReport::bump(&mut report.message_content_type_counts, ct);
            }
            // A user message closes the previous turn and opens a new one.
            if m.role == "user" {
                if *saw_first_user {
                    // The user message itself was counted into the turn that
                    // is now closing; discount it so turns don't double-count
                    // the boundary marker.
                    let closed = items_since_user.saturating_sub(1);
                    report.max_items_per_turn = report.max_items_per_turn.max(closed);
                    report.items_in_turns += closed;
                }
                report.user_message_turns += 1;
                *saw_first_user = true;
                *items_since_user = 0;
            }
        }
        ResponseItem::Reasoning(r) => {
            report.reasoning_total += 1;
            if r.encrypted_content.as_deref().map(|s| !s.is_empty()).unwrap_or(false) {
                report.reasoning_with_encrypted += 1;
            }
            if !r.summary.is_empty() {
                report.reasoning_with_summary += 1;
            }
        }
        ResponseItem::FunctionCall(fc) => {
            call_ids.insert(fc.call_id);
            if fc.id.is_some() {
                report.calls_with_fc_id += 1;
            }
        }
        ResponseItem::CustomToolCall(c) => {
            call_ids.insert(c.call_id);
            if c.id.is_some() {
                report.calls_with_fc_id += 1;
            }
        }
        ResponseItem::FunctionCallOutput(o) => {
            if !o.output.is_null() && !o.output.is_string() {
                report.outputs_non_string += 1;
            }
            output_ids.insert(o.call_id);
        }
        ResponseItem::CustomToolCallOutput(o) => {
            if !o.output.is_null() && !o.output.is_string() {
                report.outputs_non_string += 1;
            }
            output_ids.insert(o.call_id);
        }
        ResponseItem::Unknown => {
            report.unknown_response_items += 1;
        }
        // web_search_call / ghost_snapshot / rare shapes: counted by raw type.
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Round-trip: every modeled `response_item` shape deserializes from a
    /// representative real line and re-serializes without loss of identity.
    #[test]
    fn response_item_round_trips_per_shape() {
        let cases = vec![
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}),
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"yo"}]}),
            json!({"type":"reasoning","encrypted_content":"AAAA","summary":[{"type":"summary_text","text":"**Plan**"}],"content":null}),
            json!({"type":"function_call","call_id":"call_abc","name":"shell_command","arguments":"{\"cmd\":\"ls\"}"}),
            json!({"type":"function_call","call_id":"call_xyz","id":"fc_123","name":"shell_command","arguments":"{}","status":"completed"}),
            json!({"type":"function_call_output","call_id":"call_abc","output":"done"}),
            json!({"type":"function_call_output","call_id":"call_arr","output":["a","b"]}),
            json!({"type":"custom_tool_call","call_id":"call_p","name":"apply_patch","input":"*** patch","status":"completed"}),
            json!({"type":"custom_tool_call_output","call_id":"call_p","output":"ok"}),
            json!({"type":"web_search_call","action":{"type":"search","query":"rust"},"status":"completed"}),
            json!({"type":"ghost_snapshot","ghost_commit":{"id":"abc","parent":"def"}}),
            json!({"type":"tool_search_call","call_id":"call_ts"}),
            json!({"type":"image_generation_call","call_id":"call_ig"}),
        ];
        for c in cases {
            let item: ResponseItem = serde_json::from_value(c.clone())
                .unwrap_or_else(|e| panic!("failed to parse {c}: {e}"));
            assert!(
                !matches!(item, ResponseItem::Unknown),
                "modeled shape parsed as Unknown: {c}"
            );
            let round = serde_json::from_value::<ResponseItem>(
                serde_json::to_value(&item).unwrap(),
            )
            .unwrap();
            assert_eq!(item, round, "round-trip mismatch for {c}");
        }
    }

    /// A genuinely-unmodeled response_item shape becomes `Unknown`, not an
    /// error — the tolerance guarantee the risk table depends on.
    #[test]
    fn unknown_response_item_is_tolerated() {
        let v = json!({"type":"some_future_shape_v99","weird":true});
        let item: ResponseItem = serde_json::from_value(v).unwrap();
        assert!(matches!(item, ResponseItem::Unknown));
    }

    /// The full envelope parses and carries an opaque payload.
    #[test]
    fn envelope_parses_with_opaque_payload() {
        let line = r#"{"timestamp":"2026-01-01T00:00:00.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"x"}]}}"#;
        let env: RolloutLine = serde_json::from_str(line).unwrap();
        assert_eq!(env.kind, "response_item");
        assert_eq!(env.timestamp.as_deref(), Some("2026-01-01T00:00:00.000Z"));
        let item: ResponseItem = serde_json::from_value(env.payload).unwrap();
        assert!(matches!(item, ResponseItem::Message(_)));
    }

    /// session_meta and turn_context deserialize; oversized `instructions` is
    /// ignored, not fatal.
    #[test]
    fn session_meta_and_turn_context_parse() {
        let sm = json!({
            "id":"019b-uuid","timestamp":"2026-01-01T00:00:00Z","cwd":"/w",
            "originator":"codex_vscode","cli_version":"0.142.5",
            "instructions":"<<a very long system prompt>>","source":"vscode",
            "model_provider":"openai",
            "git":{"commit_hash":"abc","branch":"main","repository_url":"git@x"}
        });
        let m: SessionMeta = serde_json::from_value(sm).unwrap();
        assert_eq!(m.id, "019b-uuid");
        assert_eq!(m.git.unwrap().branch.as_deref(), Some("main"));

        // `source` is polymorphic (string OR object) and `git` may be null —
        // both real variants must parse (regression for the 26 subagent
        // session_meta files whose source is `{"subagent":"review"}`).
        let subagent = json!({
            "id":"019c-sub","cwd":"/w","source":{"subagent":"review"},"git":null
        });
        let s: SessionMeta = serde_json::from_value(subagent).unwrap();
        assert_eq!(s.id, "019c-sub");
        assert!(s.git.is_none());
        assert_eq!(s.source.unwrap()["subagent"], json!("review"));

        let tc = json!({"cwd":"/w","approval_policy":"on-request","sandbox_policy":{"type":"read-only"},"model":"gpt-5.2-codex","effort":"medium","summary":"auto"});
        let t: TurnContext = serde_json::from_value(tc).unwrap();
        assert_eq!(t.model.as_deref(), Some("gpt-5.2-codex"));
        assert_eq!(t.effort.as_deref(), Some("medium"));
    }

    /// Build a synthetic rollout file and assert the dry-run's counts,
    /// call_id linkage, and turn-granularity evidence.
    #[test]
    fn dry_run_scan_counts_linkage_and_granularity() {
        use std::io::Write;
        let dir = tempfile::TempDir::new().unwrap();
        let day = dir.path().join("2026").join("01").join("01");
        std::fs::create_dir_all(&day).unwrap();
        let path = day.join("rollout-2026-01-01T00-00-00-uuid.jsonl");

        let lines = vec![
            json!({"timestamp":"t","type":"session_meta","payload":{"id":"s1","cwd":"/w"}}),
            // Turn 1: user → reasoning → function_call → output → assistant
            json!({"timestamp":"t","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"do it"}]}}),
            json!({"timestamp":"t","type":"turn_context","payload":{"model":"gpt-5.2-codex","effort":"high"}}),
            json!({"timestamp":"t","type":"response_item","payload":{"type":"reasoning","encrypted_content":"ENC","summary":[{"type":"summary_text","text":"**Plan**"}]}}),
            json!({"timestamp":"t","type":"response_item","payload":{"type":"function_call","call_id":"call_1","id":"fc_1","name":"shell_command","arguments":"{}"}}),
            json!({"timestamp":"t","type":"event_msg","payload":{"type":"token_count"}}),
            json!({"timestamp":"t","type":"response_item","payload":{"type":"function_call_output","call_id":"call_1","output":"ok"}}),
            json!({"timestamp":"t","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}),
            // Turn 2: user → orphan output (no matching call) + a new call w/o output
            json!({"timestamp":"t","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"again"}]}}),
            json!({"timestamp":"t","type":"response_item","payload":{"type":"function_call","call_id":"call_2","name":"x","arguments":"{}"}}),
            json!({"timestamp":"t","type":"response_item","payload":{"type":"function_call_output","call_id":"call_orphan","output":["arr"]}}),
            // An unmodeled future shape must be tolerated.
            json!({"timestamp":"t","type":"response_item","payload":{"type":"brand_new_shape","x":1}}),
            // A blank line (rollouts occasionally contain them).
            json!(""),
        ];
        let mut f = std::fs::File::create(&path).unwrap();
        for l in &lines {
            match l {
                Value::String(s) if s.is_empty() => writeln!(f).unwrap(),
                other => writeln!(f, "{other}").unwrap(),
            }
        }
        f.flush().unwrap();

        let report = dry_run_scan(dir.path()).unwrap();

        assert_eq!(report.files_scanned, 1);
        assert_eq!(report.files_failed, 0);
        assert_eq!(report.hard_parse_errors, 0, "no hard parse errors expected");
        assert_eq!(report.lines_blank, 1);
        assert_eq!(report.session_meta_count, 1);
        assert_eq!(report.typed_parse_warnings.len(), 0, "no typed warnings");
        assert_eq!(report.unknown_response_items, 1, "brand_new_shape tolerated");

        // Envelope + response_item type tallies.
        assert_eq!(report.envelope_type_counts.get("response_item"), Some(&9));
        assert_eq!(report.envelope_type_counts.get("event_msg"), Some(&1));
        assert_eq!(report.response_item_type_counts.get("function_call"), Some(&2));
        assert_eq!(report.response_item_type_counts.get("function_call_output"), Some(&2));

        // Reasoning.
        assert_eq!(report.reasoning_total, 1);
        assert_eq!(report.reasoning_with_encrypted, 1);
        assert_eq!(report.reasoning_with_summary, 1);

        // Linkage: calls {call_1, call_2}, outputs {call_1, call_orphan}.
        assert_eq!(report.tool_calls, 2);
        assert_eq!(report.tool_outputs, 2);
        assert_eq!(report.calls_matched, 1); // call_1
        assert_eq!(report.orphan_calls, 1); // call_2
        assert_eq!(report.orphan_outputs, 1); // call_orphan
        assert_eq!(report.calls_with_fc_id, 1); // fc_1
        assert_eq!(report.outputs_non_string, 1); // ["arr"]

        // Message tallies.
        assert_eq!(report.message_role_counts.get("user"), Some(&2));
        assert_eq!(report.message_role_counts.get("assistant"), Some(&1));

        // Turn granularity: 2 user turns; turn 1 held reasoning + call +
        // output + assistant = 4 items (the boundary user msg discounted).
        assert_eq!(report.user_message_turns, 2);
        assert_eq!(report.max_items_per_turn, 4);
        assert!(report.avg_items_per_turn() > 1.0, "turns span many items (AD3)");
    }

    /// Regression (P2a eng review): a line containing raw non-UTF-8 bytes
    /// (e.g. binary process stdout captured in a `function_call_output`) must
    /// NOT truncate the rest of the file. Lines after it are still parsed.
    #[test]
    fn non_utf8_line_does_not_truncate_file() {
        use std::io::Write;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("rollout-2026-01-01T00-00-00-uuid.jsonl");

        let good1 = serde_json::to_vec(&json!({"timestamp":"t","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"a"}]}})).unwrap();
        let good2 = serde_json::to_vec(&json!({"timestamp":"t","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"b"}]}})).unwrap();

        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&good1).unwrap();
        f.write_all(b"\n").unwrap();
        // A line with invalid UTF-8 bytes (0xFF is never valid UTF-8).
        f.write_all(&[0x7b, 0xff, 0xfe, 0x22, 0x7d]).unwrap();
        f.write_all(b"\n").unwrap();
        f.write_all(&good2).unwrap();
        f.write_all(b"\n").unwrap();
        f.flush().unwrap();

        let report = dry_run_scan(dir.path()).unwrap();
        assert_eq!(report.files_scanned, 1);
        assert_eq!(report.files_truncated, 0, "must not truncate on non-UTF-8");
        assert_eq!(report.lines_total, 3, "all 3 lines read, none skipped by truncation");
        // The bad line is an unparseable envelope → counted, not fatal.
        assert_eq!(report.hard_parse_errors, 1);
        // BOTH good lines after/around the bad line were still parsed.
        assert_eq!(report.message_role_counts.get("user"), Some(&1));
        assert_eq!(report.message_role_counts.get("assistant"), Some(&1));
    }

    #[test]
    fn is_rollout_file_matches_expected() {
        assert!(is_rollout_file(Path::new("/x/rollout-2026-01-01T00-00-00-uuid.jsonl")));
        assert!(!is_rollout_file(Path::new("/x/state_5.sqlite")));
        assert!(!is_rollout_file(Path::new("/x/notes.jsonl")));
        assert!(!is_rollout_file(Path::new("/x/rollout-abc.txt")));
    }

    #[test]
    fn missing_root_is_empty_not_error() {
        let report = dry_run_scan(Path::new("/nonexistent/codex/sessions")).unwrap();
        assert_eq!(report.files_scanned, 0);
        assert_eq!(report.hard_parse_errors, 0);
    }
}
