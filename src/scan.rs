//! Source-neutral scan contract (AD1).
//!
//! Both the Claude Code transcript scanner and the Codex rollout scanner
//! normalize their on-disk format into the same [`NormalizedTurn`] +
//! [`NormalizedBlock`] shapes, then feed the single shared write core
//! [`write_normalized_turn`]. There is no forked indexer: the turn/content
//! upsert primitive is identical across sources; only file discovery and
//! offset bookkeeping are per-source (AD2).
//!
//! # Privacy / capture policy
//!
//! [`CapturePolicy`] decides whether a source's block bodies are stored:
//! - `MetadataOnly` (Claude Code, Cursor): block `content` is ignored — no
//!   blob is written and no `turn_content` rows are created. Only the
//!   metadata JSON on the `turns` row is produced, exactly as before.
//! - `Full` (Codex `capture=full`): each block body with `content = Some(..)`
//!   is written to the content-addressed blob store and referenced by hash
//!   from a `turn_content` row. The `turns`/`turn_content` tables still hold
//!   only hashes + metadata — never raw text (the blob store holds bodies).
//!
//! Claude Code emits blocks with `content = None` (it never extracts bodies),
//! so it is structurally metadata-only regardless of policy.

use anyhow::Result;
use rusqlite::Connection;
use serde_json::json;

use crate::blob;
use crate::index::now_iso;
use crate::index::turn_content::{self, TurnContentRow};
use crate::index::turns::{self, TurnRow};

/// Whether a source's block bodies are captured to the blob store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapturePolicy {
    /// Store only metadata; never persist block bodies (Claude Code, Cursor).
    MetadataOnly,
    /// Store block bodies in the blob store, referenced by hash (Codex full).
    Full,
}

/// Per-turn token usage, source-neutral.
#[derive(Debug, Clone, Default)]
pub struct NormalizedUsage {
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_creation_tokens: Option<i64>,
}

/// One content block of a turn, source-neutral.
#[derive(Debug, Clone, Default)]
pub struct NormalizedBlock {
    /// `text` | `tool_use` | `tool_result` | `thinking` (Claude Code) or
    /// `input_text` | `output_text` | `reasoning` | `tool_call` |
    /// `tool_result` | `image` (Codex). The write core is agnostic to the set.
    pub kind: String,
    /// `user` | `assistant` | `developer` | `tool`, when meaningful per block.
    pub role: Option<String>,
    /// Tool name for `tool_use` / `tool_call` blocks.
    pub tool_name: Option<String>,
    /// True for error tool results.
    pub is_error: bool,
    /// True when the block carries an opaque signature (CC thinking) or
    /// encrypted reasoning — surfaced in the metadata but body is not stored.
    pub signature_present: bool,
    /// Byte length of the block body.
    pub byte_len: usize,
    /// Block body available for capture. `None` = opaque / not extracted.
    /// Ignored (never stored) under [`CapturePolicy::MetadataOnly`].
    pub content: Option<String>,
}

/// A source-neutral turn ready for the shared write core.
#[derive(Debug, Clone, Default)]
pub struct NormalizedTurn {
    pub source: String,
    pub turn_uuid: String,
    pub session_id: String,
    pub parent_turn_uuid: Option<String>,
    pub role: String,
    pub timestamp: String,

    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub is_sidechain: bool,
    pub slug: Option<String>,
    /// Producing tool version (mapped to the `claude_code_version` column,
    /// which is really "tool version").
    pub tool_version: Option<String>,

    pub request_id: Option<String>,
    pub message_id: Option<String>,
    pub model: Option<String>,
    pub model_variant: Option<String>,

    pub usage: Option<NormalizedUsage>,
    pub blocks: Vec<NormalizedBlock>,
}

/// Source-neutral session identity + provenance, parsed from a source file.
#[derive(Debug, Clone, Default)]
pub struct NormalizedSession {
    pub source: String,
    pub session_id: String,
    pub started_at: String,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub git_commit: Option<String>,
    /// Producing tool version string (e.g. `codex 0.142.5`).
    pub tool_version: Option<String>,
    pub model_initial: Option<String>,
}

/// The result of scanning one source file: its session plus ordered turns.
#[derive(Debug, Clone, Default)]
pub struct ScanResult {
    pub session: NormalizedSession,
    pub turns: Vec<NormalizedTurn>,
    /// Count of items whose envelope parsed but whose typed payload failed to
    /// deserialize (version drift). Surfaced so silent data loss is visible.
    pub parse_warnings: usize,
}

/// A source that parses its on-disk format into a [`ScanResult`]. File
/// discovery and offset bookkeeping stay per-source (AD2); this is the
/// "feed" side of the shared contract. The Claude Code path predates this
/// trait and feeds the write core directly via
/// `indexer::normalized_turn_from_cc_record`; Codex implements the trait.
pub trait SourceScanner {
    /// Source name written to `turns.source` / `sessions.source`.
    fn source(&self) -> &str;
    /// Capture policy for this source's block bodies.
    fn capture_policy(&self) -> CapturePolicy;
    /// Parse one on-disk file into its session + normalized turns (file order).
    fn scan_file(&self, path: &std::path::Path) -> Result<ScanResult>;
}

/// Upsert a session row from a [`NormalizedSession`]. Shared across sources.
/// PRIVACY: identity + provenance metadata only.
pub fn write_session(
    conn: &Connection,
    session: &NormalizedSession,
    transcript_path: Option<&str>,
) -> Result<()> {
    use crate::index::sessions::{self, SessionRow};
    let now = now_iso();
    let row = SessionRow {
        session_id: session.session_id.clone(),
        source: session.source.clone(),
        started_at: session.started_at.clone(),
        transcript_path: transcript_path.map(String::from),
        cwd_initial: session.cwd.clone(),
        git_branch_initial: session.git_branch.clone(),
        git_commit_initial: session.git_commit.clone(),
        tool_version: session.tool_version.clone(),
        model_initial: session.model_initial.clone(),
        created_at: now.clone(),
        updated_at: now,
        ..Default::default()
    };
    sessions::upsert_session(conn, &row)
}

// ---------------------------------------------------------------------------
// Shared write core
// ---------------------------------------------------------------------------

/// Kinds counted as visible text for the hidden-token estimate.
fn is_text_kind(kind: &str) -> bool {
    matches!(kind, "text" | "input_text" | "output_text")
}

/// Kinds counted as visible tool-use bytes.
fn is_tool_use_kind(kind: &str) -> bool {
    matches!(kind, "tool_use" | "tool_call")
}

/// Kinds counted as "thinking"/reasoning blocks.
fn is_thinking_kind(kind: &str) -> bool {
    matches!(kind, "thinking" | "reasoning")
}

/// Build the `content_blocks_meta` JSON array. Reproduces the Claude Code
/// shape exactly for the CC block kinds (serde sorts keys, so only the key
/// *set* per kind must match): `idx`, `kind`, `byte_len`, `content_hash`,
/// `signature_present` always; `tool_name` when present; `is_error` for
/// `tool_result`. `block_hashes[i]` is the block's `sha256:` ref (or `None`).
///
/// PRIVACY: emits byte lengths + hashes only, never raw block text.
fn build_blocks_meta(blocks: &[NormalizedBlock], block_hashes: &[Option<String>]) -> String {
    let arr: Vec<serde_json::Value> = blocks
        .iter()
        .enumerate()
        .map(|(idx, b)| {
            let hash = block_hashes.get(idx).and_then(|h| h.clone());
            let mut obj = json!({
                "idx": idx,
                "kind": b.kind,
                "byte_len": b.byte_len,
                "content_hash": hash,
                "signature_present": b.signature_present,
            });
            let map = obj.as_object_mut().expect("json object");
            if let Some(tn) = &b.tool_name {
                map.insert("tool_name".to_string(), json!(tn));
            }
            if b.kind == "tool_result" {
                map.insert("is_error".to_string(), json!(b.is_error));
            }
            obj
        })
        .collect();
    serde_json::Value::Array(arr).to_string()
}

/// Write a normalized turn (and, under `Full`, its content) to jack.db. This
/// is the single write path both sources use.
///
/// Under `Full`, each block with `content = Some(..)` is stored in the blob
/// store and gets a `turn_content` row carrying its hash; blocks without
/// content (opaque reasoning) still get a `turn_content` row with a null
/// hash so the block structure is queryable. Under `MetadataOnly`, no blobs
/// and no `turn_content` rows are written.
///
/// Returns `true` if the `turns` row was newly written, `false` if it was
/// fork-skipped (turn_uuid already owned by another session) — mirroring
/// [`turns::upsert_turn`].
///
/// Redaction (AD6) is the caller's responsibility: any secret scrubbing must
/// already be applied to `block.content` before this call.
pub fn write_normalized_turn(
    conn: &Connection,
    turn: &NormalizedTurn,
    turn_order: i64,
    policy: CapturePolicy,
    kernel_event_id: Option<String>,
) -> Result<bool> {
    let scanned = now_iso();

    // Per-block content hashes (Full only). Compute + persist bodies first so
    // the turns row's convenience hash can summarize them.
    let mut block_hashes: Vec<Option<String>> = Vec::with_capacity(turn.blocks.len());
    if policy == CapturePolicy::Full {
        for block in &turn.blocks {
            let hash = match &block.content {
                Some(body) => Some(blob::store(body)?),
                None => None,
            };
            block_hashes.push(hash);
        }
    } else {
        block_hashes.resize(turn.blocks.len(), None);
    }

    // Whole-turn convenience hash: digest of the present block hashes (Full).
    let content_blob_hash = if policy == CapturePolicy::Full {
        let joined: String = block_hashes.iter().flatten().cloned().collect::<Vec<_>>().join("\n");
        if joined.is_empty() {
            None
        } else {
            Some(format!("sha256:{}", blob::hash_bytes(joined.as_bytes())))
        }
    } else {
        None
    };

    // Derived metadata (identical formula to the legacy CC path).
    let visible_text_bytes: i64 = turn
        .blocks
        .iter()
        .filter(|b| is_text_kind(&b.kind))
        .map(|b| b.byte_len as i64)
        .sum();
    let visible_tool_use_bytes: i64 = turn
        .blocks
        .iter()
        .filter(|b| is_tool_use_kind(&b.kind))
        .map(|b| b.byte_len as i64)
        .sum();
    let thinking_block_count: i64 = turn
        .blocks
        .iter()
        .filter(|b| is_thinking_kind(&b.kind))
        .count() as i64;

    let usage = turn.usage.clone().unwrap_or_default();
    let visible_bytes = visible_text_bytes + visible_tool_use_bytes;
    let visible_tokens_est = (visible_bytes + 3) / 4;
    let output_tokens = usage.output_tokens.unwrap_or(0);
    let estimated_hidden_tokens = (output_tokens - visible_tokens_est).max(0);

    let content_blocks_meta = build_blocks_meta(&turn.blocks, &block_hashes);

    let row = TurnRow {
        // PRIVACY: metadata only, no body text.
        turn_uuid: turn.turn_uuid.clone(),
        session_id: turn.session_id.clone(),
        parent_turn_uuid: turn.parent_turn_uuid.clone(),
        turn_order,
        role: turn.role.clone(),
        timestamp: turn.timestamp.clone(),
        cwd: turn.cwd.clone(),
        git_branch: turn.git_branch.clone(),
        is_sidechain: turn.is_sidechain,
        slug: turn.slug.clone(),
        claude_code_version: turn.tool_version.clone(),
        request_id: turn.request_id.clone(),
        message_id: turn.message_id.clone(),
        model: turn.model.clone(),
        model_variant: turn.model_variant.clone(),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        cache_creation_tokens: usage.cache_creation_tokens,
        ephemeral_5m_tokens: None,
        ephemeral_1h_tokens: None,
        service_tier: None,
        stop_reason: None,
        content_blocks_meta,
        visible_text_bytes,
        visible_tool_use_bytes,
        thinking_block_count,
        estimated_hidden_tokens,
        source: Some(turn.source.clone()),
        content_blob_hash,
        kernel_event_id,
        scanned_at: scanned.clone(),
    };

    let written = turns::upsert_turn(conn, &row)?;

    // Persist per-block content rows (Full only). Done after the turns upsert
    // so a fork-skipped turn (written == false) does not orphan content rows.
    if policy == CapturePolicy::Full && written {
        for (idx, block) in turn.blocks.iter().enumerate() {
            turn_content::upsert_turn_content(
                conn,
                &TurnContentRow {
                    turn_uuid: turn.turn_uuid.clone(),
                    block_index: idx as i64,
                    kind: block.kind.clone(),
                    role: block.role.clone(),
                    content_hash: block_hashes[idx].clone(),
                    is_error: block.is_error,
                    byte_len: block.byte_len as i64,
                    scanned_at: scanned.clone(),
                },
            )?;
        }
    }

    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;
    use crate::index::turns::get_turn;

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();
        conn
    }

    fn text_block(kind: &str, body: &str) -> NormalizedBlock {
        NormalizedBlock {
            kind: kind.into(),
            role: Some("assistant".into()),
            byte_len: body.len(),
            content: Some(body.into()),
            ..Default::default()
        }
    }

    #[test]
    fn cc_metadata_shape_matches_legacy_keys() {
        // Build a CC-style block set with content=None (as the CC scanner
        // produces) and assert the meta JSON key sets match the legacy shape.
        let blocks = vec![
            NormalizedBlock { kind: "text".into(), byte_len: 10, ..Default::default() },
            NormalizedBlock { kind: "tool_use".into(), tool_name: Some("Bash".into()), byte_len: 5, ..Default::default() },
            NormalizedBlock { kind: "tool_result".into(), is_error: true, byte_len: 3, ..Default::default() },
            NormalizedBlock { kind: "thinking".into(), signature_present: true, byte_len: 0, ..Default::default() },
        ];
        let hashes = vec![None, None, None, None];
        let meta: serde_json::Value = serde_json::from_str(&build_blocks_meta(&blocks, &hashes)).unwrap();
        let arr = meta.as_array().unwrap();

        let keyset = |v: &serde_json::Value| {
            let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
            k.sort();
            k
        };
        assert_eq!(keyset(&arr[0]), vec!["byte_len", "content_hash", "idx", "kind", "signature_present"]);
        assert_eq!(keyset(&arr[1]), vec!["byte_len", "content_hash", "idx", "kind", "signature_present", "tool_name"]);
        assert_eq!(keyset(&arr[2]), vec!["byte_len", "content_hash", "idx", "is_error", "kind", "signature_present"]);
        assert_eq!(keyset(&arr[3]), vec!["byte_len", "content_hash", "idx", "kind", "signature_present"]);
        assert_eq!(arr[3]["signature_present"], json!(true));
        assert_eq!(arr[2]["is_error"], json!(true));
        assert_eq!(arr[1]["tool_name"], json!("Bash"));
    }

    #[test]
    fn metadata_only_writes_no_content_or_blobs() {
        let _guard = crate::session::PUNKGO_DATA_DIR_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::TempDir::new().unwrap();
        let prev = std::env::var_os("PUNKGO_DATA_DIR");
        std::env::set_var("PUNKGO_DATA_DIR", tmp.path());

        let conn = fresh_db();
        let turn = NormalizedTurn {
            source: "claude-code".into(),
            turn_uuid: "t1".into(),
            session_id: "s1".into(),
            role: "assistant".into(),
            timestamp: "2026-07-01T00:00:00Z".into(),
            // content present, but MetadataOnly must ignore it.
            blocks: vec![text_block("text", "hello secret body")],
            ..Default::default()
        };
        let written = write_normalized_turn(&conn, &turn, 0, CapturePolicy::MetadataOnly, None).unwrap();
        assert!(written);

        let row = get_turn(&conn, "t1").unwrap().unwrap();
        assert_eq!(row.source.as_deref(), Some("claude-code"));
        assert!(row.content_blob_hash.is_none());
        // No turn_content rows under metadata-only.
        assert!(turn_content::list_content_for_turn(&conn, "t1").unwrap().is_empty());
        // The body text never reached the DB row.
        assert!(!row.content_blocks_meta.contains("secret body"));

        if let Some(v) = prev { std::env::set_var("PUNKGO_DATA_DIR", v); } else { std::env::remove_var("PUNKGO_DATA_DIR"); }
    }

    #[test]
    fn full_capture_stores_blobs_and_turn_content() {
        let _guard = crate::session::PUNKGO_DATA_DIR_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::TempDir::new().unwrap();
        let prev = std::env::var_os("PUNKGO_DATA_DIR");
        std::env::set_var("PUNKGO_DATA_DIR", tmp.path());

        let conn = fresh_db();
        let turn = NormalizedTurn {
            source: "codex".into(),
            turn_uuid: "ct1".into(),
            session_id: "cs1".into(),
            role: "assistant".into(),
            timestamp: "2026-07-01T00:00:00Z".into(),
            blocks: vec![
                NormalizedBlock { kind: "input_text".into(), role: Some("user".into()), byte_len: 5, content: Some("hi you".into()), ..Default::default() },
                NormalizedBlock { kind: "output_text".into(), role: Some("assistant".into()), byte_len: 4, content: Some("done".into()), ..Default::default() },
                // opaque reasoning: no content -> null hash, still a row.
                NormalizedBlock { kind: "reasoning".into(), signature_present: true, byte_len: 900, content: None, ..Default::default() },
            ],
            ..Default::default()
        };
        let written = write_normalized_turn(&conn, &turn, 0, CapturePolicy::Full, None).unwrap();
        assert!(written);

        let blocks = turn_content::list_content_for_turn(&conn, "ct1").unwrap();
        assert_eq!(blocks.len(), 3);
        assert!(blocks[0].content_hash.as_deref().unwrap().starts_with("sha256:"));
        assert!(blocks[1].content_hash.as_deref().unwrap().starts_with("sha256:"));
        assert!(blocks[2].content_hash.is_none(), "opaque reasoning has null hash");

        // Body retrievable from the blob store by its hash.
        let h0 = blocks[0].content_hash.clone().unwrap();
        assert_eq!(blob::resolve(&h0).unwrap().as_deref(), Some("hi you"));

        let row = get_turn(&conn, "ct1").unwrap().unwrap();
        assert!(row.content_blob_hash.as_deref().unwrap().starts_with("sha256:"));

        if let Some(v) = prev { std::env::set_var("PUNKGO_DATA_DIR", v); } else { std::env::remove_var("PUNKGO_DATA_DIR"); }
    }
}
