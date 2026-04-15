//! Transcript indexer orchestration.
//!
//! Three coexisting recovery paths (plan §4.7):
//!
//! 1. **Path A — hook-triggered incremental.** `scan_on_trigger(session_id,
//!    transcript_path)` enqueues a `pending_scans` row and spawns a detached
//!    drainer task. Hooks never fail because of indexer errors.
//! 2. **Path B — full backfill.** `run_reindex(opts)` walks
//!    `~/.claude/projects/`, re-scans matching files, upserts everything.
//!    Idempotent; safe to re-run.
//! 3. **Path C — startup reconciliation.** `reconcile_on_startup` drains
//!    the `pending_scans` queue and re-enqueues drifted sessions on jack
//!    daemon startup.
//!
//! # Privacy invariant (non-negotiable)
//!
//! Every row this module writes to jack.db carries metadata only — uuids,
//! byte lengths, hashes, model identifiers, opaque base64 thinking
//! signatures, usage tokens. NEVER raw prompt/response/tool/thinking text.
//! Each upsert site below has a `// PRIVACY: metadata only` comment that
//! the code reviewer scans for.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::index::{
    self, now_iso,
    sessions::{self, SessionAggregates, SessionRow},
    signatures::{self as sig_idx, SignatureRow},
    turns::{self, TurnRow},
};
use crate::signature::parse_thinking_signature;
use crate::transcript::scanner::{ContentBlockRecord, TranscriptScanner, TurnRecord};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ReindexOptions {
    /// Re-scan everything under `~/.claude/projects/` ignoring stored offsets.
    pub full: bool,
    /// Only process transcript files whose mtime is >= this ISO timestamp.
    pub since: Option<String>,
    /// Only process this single session's transcript file.
    pub session: Option<String>,
    /// Walk and parse but do not write anything to jack.db.
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReindexReport {
    pub files_scanned: usize,
    pub files_failed: usize,
    pub turns_upserted: usize,
    pub signatures_upserted: usize,
    pub sessions_upserted: usize,
    pub duration_seconds: f64,
    pub model_variant_breakdown: HashMap<String, usize>,
}

// ---------------------------------------------------------------------------
// Path A — hook-triggered incremental scan
// ---------------------------------------------------------------------------

/// Enqueue a pending scan and spawn a detached drainer. Returns immediately;
/// indexer errors are logged but never propagated to the caller (hooks must
/// not fail because of the index).
pub async fn scan_on_trigger(session_id: String, transcript_path: Option<String>) -> Result<()> {
    let sid = session_id.clone();
    let path = transcript_path.clone();
    // Enqueue synchronously inside spawn_blocking — the row is the durable
    // checkpoint, so we must be sure it landed before we return.
    let enqueue_result =
        tokio::task::spawn_blocking(move || enqueue_pending_scan(&sid, path.as_deref()))
            .await
            .context("spawn_blocking enqueue join")?;
    if let Err(e) = enqueue_result {
        tracing::error!(error = %e, session = %session_id, "failed to enqueue pending scan");
        return Ok(());
    }

    // Spawn a detached drainer. The drain itself is best-effort; if it
    // fails the row sticks around for the next trigger or startup.
    tokio::spawn(async move {
        let drain_result = tokio::task::spawn_blocking(drain_pending_scans).await;
        match drain_result {
            Ok(Ok(n)) if n > 0 => {
                debug!(drained = n, "pending_scans drained");
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                warn!(error = %e, "pending_scans drain returned error");
            }
            Err(e) => {
                warn!(error = %e, "spawn_blocking drain join failure");
            }
        }
    });

    Ok(())
}

/// Synchronous helper to insert a pending_scans row.
pub fn enqueue_pending_scan(session_id: &str, transcript_path: Option<&str>) -> Result<()> {
    let conn = index::open_jack_db()?;
    conn.execute(
        "INSERT INTO pending_scans (session_id, transcript_path, enqueued_at) VALUES (?1, ?2, ?3)",
        params![session_id, transcript_path, now_iso()],
    )
    .context("enqueue_pending_scan")?;
    Ok(())
}

/// Drain every row in `pending_scans`. Returns the number of rows
/// successfully processed (deleted). Re-entrant: a second concurrent
/// invocation just sees no rows and returns 0.
pub fn drain_pending_scans() -> Result<usize> {
    let mut conn = index::open_jack_db()?;
    let mut processed = 0usize;

    loop {
        let next: Option<(i64, String, Option<String>, i64)> = conn
            .query_row(
                "SELECT id, session_id, transcript_path, attempts FROM pending_scans ORDER BY id ASC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .context("query next pending_scan")?;

        let Some((id, session_id, transcript_path, attempts)) = next else {
            break;
        };

        match process_pending_scan(&mut conn, &session_id, transcript_path.as_deref()) {
            Ok(()) => {
                conn.execute("DELETE FROM pending_scans WHERE id = ?1", params![id])?;
                processed += 1;
            }
            Err(e) => {
                let attempts_after = attempts + 1;
                let level_text = e.to_string();
                if attempts_after >= 3 {
                    tracing::error!(
                        error = %e,
                        session = %session_id,
                        attempts = attempts_after,
                        "pending_scans entry exceeded retry limit"
                    );
                } else {
                    warn!(
                        error = %e,
                        session = %session_id,
                        attempts = attempts_after,
                        "pending_scans entry failed, will retry"
                    );
                }
                conn.execute(
                    "UPDATE pending_scans SET attempts = ?1, last_error = ?2, last_attempt = ?3 WHERE id = ?4",
                    params![attempts_after, level_text, now_iso(), id],
                )?;
                // Stop draining on first failure to avoid hot-looping on a
                // permanently broken row. Next trigger will retry.
                break;
            }
        }
    }

    Ok(processed)
}

fn process_pending_scan(
    conn: &mut Connection,
    session_id: &str,
    transcript_path_hint: Option<&str>,
) -> Result<()> {
    // Resolve path: hint > sessions row > error.
    let resolved_path = if let Some(hint) = transcript_path_hint {
        Some(hint.to_string())
    } else {
        sessions::get_session(conn, session_id)?.and_then(|r| r.transcript_path)
    };
    let path_str = resolved_path
        .ok_or_else(|| anyhow::anyhow!("no transcript_path for session {session_id}"))?;
    let path = PathBuf::from(&path_str);
    if !path.exists() {
        anyhow::bail!("transcript path missing: {}", path.display());
    }

    let last_offset = sessions::get_session(conn, session_id)?
        .map(|r| r.last_scan_offset as u64)
        .unwrap_or(0);

    let (records, new_offset) = TranscriptScanner::scan_incremental(&path, last_offset)
        .with_context(|| format!("scan_incremental failed for {}", path.display()))?;

    let tx = conn.transaction()?;

    // Ensure session row exists (create from records if missing).
    ensure_session_exists(&tx, session_id, Some(path_str.as_str()), &records)?;

    // Insert turns + signatures.
    let next_order = next_turn_order(&tx, session_id)?;
    let mut order = next_order;
    for record in &records {
        upsert_turn_from_record(&tx, record, order, None)?;
        upsert_signatures_from_record(&tx, record)?;
        order += 1;
    }

    sessions::update_scan_offset(&tx, session_id, new_offset, None)?;
    tx.commit()?;

    Ok(())
}

fn next_turn_order(conn: &Connection, session_id: &str) -> Result<i64> {
    let max_order: Option<i64> = conn
        .query_row(
            "SELECT MAX(turn_order) FROM turns WHERE session_id = ?1",
            params![session_id],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    Ok(max_order.map(|m| m + 1).unwrap_or(0))
}

// ---------------------------------------------------------------------------
// Path B — full backfill
// ---------------------------------------------------------------------------

/// Synchronous full reindex. Caller wraps in `tokio::task::spawn_blocking`
/// when invoked from async contexts.
pub fn run_reindex(opts: ReindexOptions) -> Result<ReindexReport> {
    let started = Instant::now();
    let mut report = ReindexReport::default();

    let projects_root = projects_root()?;
    if !projects_root.exists() {
        warn!(path = %projects_root.display(), "claude projects dir does not exist; nothing to reindex");
        report.duration_seconds = started.elapsed().as_secs_f64();
        return Ok(report);
    }

    let mut files = Vec::new();
    walk_jsonl(&projects_root, &mut files);

    // Filter by --since and --session up front.
    if let Some(since) = &opts.since {
        let since_dt = chrono::DateTime::parse_from_rfc3339(since)
            .map(|d| d.with_timezone(&chrono::Utc))
            .with_context(|| format!("invalid --since timestamp: {since}"))?;
        files.retain(|p| {
            std::fs::metadata(p)
                .and_then(|m| m.modified())
                .map(|mt| {
                    let mt_dt: chrono::DateTime<chrono::Utc> = mt.into();
                    mt_dt >= since_dt
                })
                .unwrap_or(false)
        });
    }
    if let Some(target_session) = &opts.session {
        files.retain(|p| p.file_stem().and_then(|s| s.to_str()) == Some(target_session.as_str()));
    }

    let mut conn = if opts.dry_run {
        // In dry-run we still need a connection for read/lookups, but we'll
        // wrap every write in a rollback. Open a real conn and never commit.
        index::open_jack_db()?
    } else {
        index::open_jack_db()?
    };

    let total = files.len();
    info!(file_count = total, "reindex starting");
    for (i, path) in files.iter().enumerate() {
        let session_id = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => {
                report.files_failed += 1;
                continue;
            }
        };

        let records = match TranscriptScanner::scan_file(path) {
            Ok(r) => r,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "scan_file failed");
                report.files_failed += 1;
                continue;
            }
        };

        if opts.dry_run {
            // Count what we would do without writing. Track variant breakdown
            // by inspecting records directly.
            for r in &records {
                report.turns_upserted += 1;
                for block in &r.content_blocks {
                    if let ContentBlockRecord::Thinking { signature_b64, .. } = block {
                        report.signatures_upserted += 1;
                        if let Ok(meta) = parse_thinking_signature(signature_b64) {
                            if let Some(v) = meta.model_variant {
                                *report.model_variant_breakdown.entry(v).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }
            report.sessions_upserted += 1;
            report.files_scanned += 1;
            if (i + 1) % 50 == 0 {
                info!(
                    progress = format!("{}/{}", i + 1, total),
                    "reindex (dry-run) progress"
                );
            }
            continue;
        }

        // Real write path: use a transaction per file so a single bad file
        // doesn't corrupt accumulated progress.
        let tx_result: Result<(usize, usize, HashMap<String, usize>)> = (|| {
            let tx = conn.transaction()?;

            // Wipe existing rows so re-runs are deterministic on schema/parser bumps.
            sig_idx::delete_signatures_for_session(&tx, &session_id)?;
            turns::delete_turns_for_session(&tx, &session_id)?;

            // Upsert session metadata from earliest record (or stub if empty).
            ensure_session_exists(
                &tx,
                &session_id,
                Some(path.to_string_lossy().as_ref()),
                &records,
            )?;

            let mut local_turns = 0usize;
            let mut local_sigs = 0usize;
            let mut local_variants: HashMap<String, usize> = HashMap::new();
            for (order, record) in records.iter().enumerate() {
                upsert_turn_from_record(&tx, record, order as i64, None)?;
                local_turns += 1;
                for block in &record.content_blocks {
                    if let ContentBlockRecord::Thinking { signature_b64, .. } = block {
                        if let Ok(meta) = parse_thinking_signature(signature_b64) {
                            if let Some(v) = meta.model_variant {
                                *local_variants.entry(v).or_insert(0) += 1;
                            }
                        }
                    }
                }
                local_sigs += upsert_signatures_from_record(&tx, record)?;
            }

            // Compute aggregates and finalize the session.
            let agg = compute_aggregates(&records);
            sessions::finalize_session(&tx, &session_id, &agg)?;

            // Set last_scan_offset to the file length so subsequent
            // incremental scans pick up from end-of-file.
            let file_len = std::fs::metadata(path)?.len();
            sessions::update_scan_offset(&tx, &session_id, file_len, None)?;

            tx.commit()?;
            Ok((local_turns, local_sigs, local_variants))
        })();

        match tx_result {
            Ok((t, s, variants)) => {
                report.turns_upserted += t;
                report.signatures_upserted += s;
                report.sessions_upserted += 1;
                report.files_scanned += 1;
                for (k, v) in variants {
                    *report.model_variant_breakdown.entry(k).or_insert(0) += v;
                }
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "reindex transaction failed");
                report.files_failed += 1;
            }
        }

        if (i + 1) % 50 == 0 {
            info!(
                progress = format!("{}/{}", i + 1, total),
                "reindex progress"
            );
        }
    }

    report.duration_seconds = started.elapsed().as_secs_f64();
    info!(
        files = report.files_scanned,
        failed = report.files_failed,
        turns = report.turns_upserted,
        signatures = report.signatures_upserted,
        duration_s = report.duration_seconds,
        "reindex complete"
    );

    Ok(report)
}

// ---------------------------------------------------------------------------
// Path C — startup reconciliation
// ---------------------------------------------------------------------------

/// Drain `pending_scans` and (optionally) enqueue drifted sessions. Called
/// from the daemon startup path before the MCP server begins serving.
pub fn reconcile_on_startup() -> Result<()> {
    // Best-effort open. If the data dir doesn't exist yet (fresh install),
    // open_jack_db will create it. Errors are logged and swallowed because
    // the daemon should still come up even if the index is broken.
    let conn = match index::open_jack_db() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "reconcile_on_startup: cannot open jack.db; skipping");
            return Ok(());
        }
    };

    // Drift detection: any session whose updated_at is more than 60s past
    // its last_scan_at (or has no last_scan_at) gets re-enqueued.
    let drift_threshold_seconds = 60i64;
    let mut stmt = conn.prepare(
        r#"
        SELECT session_id, transcript_path FROM sessions
        WHERE transcript_path IS NOT NULL
          AND (
            last_scan_at IS NULL
            OR (CAST(strftime('%s', updated_at) AS INTEGER)
                 - CAST(strftime('%s', last_scan_at) AS INTEGER)) > ?1
          )
        "#,
    )?;
    let rows: Vec<(String, Option<String>)> = stmt
        .query_map(params![drift_threshold_seconds], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?
        .filter_map(|r| r.ok())
        .collect();
    drop(stmt);

    let mut enqueued = 0usize;
    for (sid, path) in rows {
        conn.execute(
            "INSERT INTO pending_scans (session_id, transcript_path, enqueued_at) VALUES (?1, ?2, ?3)",
            params![sid, path, now_iso()],
        )?;
        enqueued += 1;
    }
    drop(conn);

    if enqueued > 0 {
        info!(enqueued, "reconcile: re-enqueued drifted sessions");
    }

    let drained = drain_pending_scans().unwrap_or(0);
    if drained > 0 {
        info!(drained, "reconcile: drained pending scans on startup");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn projects_root() -> Result<PathBuf> {
    let home = crate::session::home_dir()
        .ok_or_else(|| anyhow::anyhow!("cannot resolve home directory"))?;
    Ok(home.join(".claude").join("projects"))
}

fn walk_jsonl(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_jsonl(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            out.push(path);
        }
    }
}

fn ensure_session_exists(
    conn: &Connection,
    session_id: &str,
    transcript_path: Option<&str>,
    records: &[TurnRecord],
) -> Result<()> {
    let existing = sessions::get_session(conn, session_id)?;
    if existing.is_some() {
        // Refresh transcript_path if missing.
        if let Some(p) = transcript_path {
            conn.execute(
                "UPDATE sessions SET transcript_path = COALESCE(transcript_path, ?1), updated_at = ?2 WHERE session_id = ?3",
                params![p, now_iso(), session_id],
            )?;
        }
        return Ok(());
    }

    let started_at = records
        .iter()
        .map(|r| r.timestamp.clone())
        .min()
        .unwrap_or_else(now_iso);
    let cwd_initial = records.iter().find_map(|r| r.cwd.clone());
    let git_branch_initial = records.iter().find_map(|r| r.git_branch.clone());
    let model_initial = records.iter().find_map(|r| r.model.clone());
    let tool_version = records
        .iter()
        .find_map(|r| r.claude_code_version.clone())
        .map(|v| format!("claude-code {v}"));

    // PRIVACY: metadata only, no body text.
    let row = SessionRow {
        session_id: session_id.to_string(),
        source: "claude-code".to_string(),
        started_at: started_at.clone(),
        transcript_path: transcript_path.map(String::from),
        cwd_initial,
        git_branch_initial,
        tool_version,
        model_initial,
        created_at: now_iso(),
        updated_at: now_iso(),
        ..Default::default()
    };
    sessions::upsert_session(conn, &row)
}

/// Build a privacy-safe `content_blocks_meta` JSON array from a TurnRecord.
/// Each element carries ONLY: idx, kind, byte_len, content_hash,
/// signature_present. NEVER raw text.
fn build_content_blocks_meta(record: &TurnRecord) -> String {
    let arr: Vec<serde_json::Value> = record
        .content_blocks
        .iter()
        .enumerate()
        .map(|(idx, block)| match block {
            // PRIVACY: byte_len + hash only. Source text never copied here.
            ContentBlockRecord::Text {
                byte_len,
                content_hash,
            } => json!({
                "idx": idx,
                "kind": "text",
                "byte_len": byte_len,
                "content_hash": content_hash,
                "signature_present": false,
            }),
            ContentBlockRecord::ToolUse {
                name,
                byte_len,
                content_hash,
            } => json!({
                "idx": idx,
                "kind": "tool_use",
                "tool_name": name,
                "byte_len": byte_len,
                "content_hash": content_hash,
                "signature_present": false,
            }),
            ContentBlockRecord::ToolResult {
                byte_len,
                content_hash,
                is_error,
            } => json!({
                "idx": idx,
                "kind": "tool_result",
                "byte_len": byte_len,
                "content_hash": content_hash,
                "is_error": is_error,
                "signature_present": false,
            }),
            ContentBlockRecord::Thinking {
                thinking_byte_len,
                signature_bytes,
                ..
            } => json!({
                "idx": idx,
                "kind": "thinking",
                "byte_len": thinking_byte_len,
                "signature_bytes": signature_bytes,
                "signature_present": true,
            }),
        })
        .collect();
    serde_json::Value::Array(arr).to_string()
}

fn upsert_turn_from_record(
    conn: &Connection,
    record: &TurnRecord,
    turn_order: i64,
    kernel_event_id: Option<String>,
) -> Result<()> {
    let usage = record.usage.as_ref();
    let visible_text_bytes: i64 = record
        .content_blocks
        .iter()
        .map(|b| match b {
            ContentBlockRecord::Text { byte_len, .. } => *byte_len as i64,
            _ => 0,
        })
        .sum();
    let visible_tool_use_bytes: i64 = record
        .content_blocks
        .iter()
        .map(|b| match b {
            ContentBlockRecord::ToolUse { byte_len, .. } => *byte_len as i64,
            _ => 0,
        })
        .sum();
    let thinking_block_count: i64 = record
        .content_blocks
        .iter()
        .filter(|b| matches!(b, ContentBlockRecord::Thinking { .. }))
        .count() as i64;

    let visible_bytes = visible_text_bytes + visible_tool_use_bytes;
    // ceil(visible_bytes / 4) using stable integer math.
    let visible_tokens_est = (visible_bytes + 3) / 4;
    let output_tokens = usage
        .and_then(|u| u.output_tokens.map(|x| x as i64))
        .unwrap_or(0);
    let estimated_hidden_tokens = (output_tokens - visible_tokens_est).max(0);

    let row = TurnRow {
        // PRIVACY: metadata only, no body text.
        turn_uuid: record.turn_uuid.clone(),
        session_id: record.session_id.clone(),
        parent_turn_uuid: record.parent_turn_uuid.clone(),
        turn_order,
        role: record.role.clone(),
        timestamp: record.timestamp.clone(),
        cwd: record.cwd.clone(),
        git_branch: record.git_branch.clone(),
        is_sidechain: record.is_sidechain,
        slug: record.slug.clone(),
        claude_code_version: record.claude_code_version.clone(),
        request_id: record.request_id.clone(),
        message_id: record.message_id.clone(),
        model: record.model.clone(),
        model_variant: record.model_variant.clone(),
        input_tokens: usage.and_then(|u| u.input_tokens.map(|x| x as i64)),
        output_tokens: usage.and_then(|u| u.output_tokens.map(|x| x as i64)),
        cache_read_tokens: usage.and_then(|u| u.cache_read_input_tokens.map(|x| x as i64)),
        cache_creation_tokens: usage.and_then(|u| u.cache_creation_input_tokens.map(|x| x as i64)),
        ephemeral_5m_tokens: None,
        ephemeral_1h_tokens: None,
        service_tier: None,
        stop_reason: None,
        content_blocks_meta: build_content_blocks_meta(record),
        visible_text_bytes,
        visible_tool_use_bytes,
        thinking_block_count,
        estimated_hidden_tokens,
        kernel_event_id,
        scanned_at: now_iso(),
    };
    // PRIVACY: metadata only, no body text.
    turns::upsert_turn(conn, &row)
}

fn upsert_signatures_from_record(conn: &Connection, record: &TurnRecord) -> Result<usize> {
    let mut count = 0usize;
    for (idx, block) in record.content_blocks.iter().enumerate() {
        if let ContentBlockRecord::Thinking {
            thinking_byte_len,
            signature_b64,
            signature_bytes,
        } = block
        {
            let meta = parse_thinking_signature(signature_b64).ok();
            let extracted_strings_json = meta.as_ref().map(|m| {
                serde_json::to_string(&m.extracted_strings).unwrap_or_else(|_| "[]".into())
            });
            let extracted_model_variant = meta.and_then(|m| m.model_variant);

            // PRIVACY: signature is opaque metadata produced by Anthropic's
            // inference stack — does NOT contain user content.
            let row = SignatureRow {
                turn_uuid: record.turn_uuid.clone(),
                block_index: idx as i64,
                signature_b64: signature_b64.clone(),
                signature_bytes: *signature_bytes as i64,
                thinking_content_bytes: *thinking_byte_len as i64,
                extracted_model_variant,
                extracted_strings_json,
                scanned_at: now_iso(),
            };
            sig_idx::upsert_signature(conn, &row)?;
            count += 1;
        }
    }
    Ok(count)
}

fn compute_aggregates(records: &[TurnRecord]) -> SessionAggregates {
    let mut agg = SessionAggregates {
        total_turns: records.len() as i64,
        ..Default::default()
    };
    let mut variants: HashSet<String> = HashSet::new();
    for r in records {
        if let Some(u) = &r.usage {
            agg.total_input_tokens += u.input_tokens.unwrap_or(0) as i64;
            agg.total_output_tokens += u.output_tokens.unwrap_or(0) as i64;
            agg.total_cache_read_tokens += u.cache_read_input_tokens.unwrap_or(0) as i64;
            agg.total_cache_creation_tokens += u.cache_creation_input_tokens.unwrap_or(0) as i64;
        }
        // Hidden token estimation per turn (mirror the per-turn formula).
        let visible_bytes: i64 = r
            .content_blocks
            .iter()
            .map(|b| match b {
                ContentBlockRecord::Text { byte_len, .. } => *byte_len as i64,
                ContentBlockRecord::ToolUse { byte_len, .. } => *byte_len as i64,
                _ => 0,
            })
            .sum();
        let output = r
            .usage
            .as_ref()
            .and_then(|u| u.output_tokens.map(|x| x as i64))
            .unwrap_or(0);
        agg.total_hidden_tokens_est += (output - (visible_bytes + 3) / 4).max(0);
        if let Some(v) = &r.model_variant {
            variants.insert(v.clone());
        }
    }
    if !variants.is_empty() {
        let mut sorted: Vec<_> = variants.into_iter().collect();
        sorted.sort();
        agg.distinct_model_variants = Some(serde_json::to_string(&sorted).unwrap_or_default());
    }
    agg
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transcript::scanner::{ContentBlockRecord, TurnRecord, UsageRecord};
    use std::io::Write;

    /// RAII helper: lock PUNKGO_DATA_DIR and point it at a tempdir for the
    /// duration of the test. Restores the previous value on drop.
    struct DataDirGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        _tmp: tempfile::TempDir,
        prev: Option<std::ffi::OsString>,
    }

    impl DataDirGuard {
        fn new() -> Self {
            let lock = crate::session::PUNKGO_DATA_DIR_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let tmp = tempfile::TempDir::new().unwrap();
            let prev = std::env::var_os("PUNKGO_DATA_DIR");
            std::env::set_var("PUNKGO_DATA_DIR", tmp.path());
            Self {
                _lock: lock,
                _tmp: tmp,
                prev,
            }
        }
    }

    impl Drop for DataDirGuard {
        fn drop(&mut self) {
            if let Some(v) = self.prev.take() {
                std::env::set_var("PUNKGO_DATA_DIR", v);
            } else {
                std::env::remove_var("PUNKGO_DATA_DIR");
            }
        }
    }

    fn synth_assistant_line(uuid: &str, session_id: &str, with_thinking: bool) -> String {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;
        let sig_b64 = STANDARD.encode(b"\x01\x02numbat-v6-efforts-10-20-40-ab-prod\x08\x00");
        let mut blocks = vec![serde_json::json!({"type": "text", "text": "hello world"})];
        if with_thinking {
            blocks.push(serde_json::json!({
                "type": "thinking",
                "thinking": "",
                "signature": sig_b64
            }));
        }
        let obj = serde_json::json!({
            "type": "assistant",
            "uuid": uuid,
            "parentUuid": null,
            "sessionId": session_id,
            "timestamp": "2026-04-15T12:00:00.000Z",
            "cwd": "/tmp/work",
            "gitBranch": "main",
            "isSidechain": false,
            "version": "2.0.0",
            "requestId": format!("req_{}", uuid),
            "message": {
                "id": format!("msg_{}", uuid),
                "role": "assistant",
                "model": "claude-opus-4-6",
                "content": blocks,
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 1000,
                    "cache_creation_input_tokens": 10,
                    "cache_read_input_tokens": 20
                }
            }
        });
        serde_json::to_string(&obj).unwrap()
    }

    fn write_synth_jsonl(dir: &Path, session_id: &str, lines: &[String]) -> PathBuf {
        let path = dir.join(format!("{session_id}.jsonl"));
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        f.flush().unwrap();
        path
    }

    fn build_synth_archive(root: &Path, n_files: usize, with_thinking: bool) -> Vec<PathBuf> {
        let projects = root.join(".claude").join("projects").join("test-project");
        std::fs::create_dir_all(&projects).unwrap();
        let mut paths = Vec::new();
        for i in 0..n_files {
            let session_id = format!("session-{i:04}");
            let lines = vec![
                synth_assistant_line(&format!("turn-{i}-a"), &session_id, with_thinking),
                synth_assistant_line(&format!("turn-{i}-b"), &session_id, false),
            ];
            paths.push(write_synth_jsonl(&projects, &session_id, &lines));
        }
        paths
    }

    #[test]
    fn scan_on_trigger_enqueues_pending_scan() {
        let _guard = DataDirGuard::new();
        // Build a minimal jsonl so the scan can succeed eventually (the
        // drainer might or might not run; we only assert the row landed).
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            scan_on_trigger("test-session-xyz".into(), Some("/nonexistent/path".into()))
                .await
                .unwrap();
        });

        // Open the DB and verify either the row is still pending OR it was
        // drained (which would have failed because the path doesn't exist
        // and incremented attempts). Either way, look at the table state.
        let conn = index::open_jack_db().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_scans", [], |r| r.get(0))
            .unwrap();
        assert!(count >= 1, "expected pending_scans row to exist");
    }

    #[test]
    fn drain_pending_scans_processes_and_deletes() {
        let _guard = DataDirGuard::new();
        let tmp = tempfile::TempDir::new().unwrap();
        let line = synth_assistant_line("turn-1", "sess-drain", true);
        let path = write_synth_jsonl(tmp.path(), "sess-drain", &[line]);

        // Enqueue manually.
        enqueue_pending_scan("sess-drain", Some(path.to_str().unwrap())).unwrap();

        let processed = drain_pending_scans().unwrap();
        assert_eq!(processed, 1);

        let conn = index::open_jack_db().unwrap();
        let pending: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_scans", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pending, 0);
        let turns: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE session_id = 'sess-drain'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(turns, 1);
    }

    #[test]
    fn drain_pending_scans_retries_on_failure() {
        let _guard = DataDirGuard::new();
        enqueue_pending_scan("sess-missing", Some("/no/such/file.jsonl")).unwrap();

        let processed = drain_pending_scans().unwrap();
        assert_eq!(processed, 0);

        let conn = index::open_jack_db().unwrap();
        let attempts: i64 = conn
            .query_row(
                "SELECT attempts FROM pending_scans WHERE session_id = 'sess-missing'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(attempts, 1);
    }

    #[test]
    fn reindex_full_on_synthetic_archive() {
        let guard = DataDirGuard::new();
        let archive_root = guard._tmp.path();
        let _files = build_synth_archive(archive_root, 3, true);

        // Trick reindex into walking our tempdir by overriding HOME for
        // the duration of the call. session::home_dir() reads HOME first.
        let prev_home = std::env::var_os("HOME");
        std::env::set_var("HOME", archive_root);
        let result = run_reindex(ReindexOptions {
            full: true,
            ..Default::default()
        });
        if let Some(h) = prev_home {
            std::env::set_var("HOME", h);
        } else {
            std::env::remove_var("HOME");
        }

        let report = result.unwrap();
        assert_eq!(report.files_scanned, 3);
        assert_eq!(report.files_failed, 0);
        // Each file: 2 assistant turns; one has a thinking block.
        assert_eq!(report.turns_upserted, 6);
        assert_eq!(report.signatures_upserted, 3);
        assert_eq!(report.sessions_upserted, 3);
        assert!(report
            .model_variant_breakdown
            .contains_key("numbat-v6-efforts-10-20-40-ab-prod"));
    }

    #[test]
    fn reindex_dry_run_writes_nothing() {
        let guard = DataDirGuard::new();
        let archive_root = guard._tmp.path();
        let _files = build_synth_archive(archive_root, 2, true);

        let prev_home = std::env::var_os("HOME");
        std::env::set_var("HOME", archive_root);
        let report = run_reindex(ReindexOptions {
            full: true,
            dry_run: true,
            ..Default::default()
        })
        .unwrap();
        if let Some(h) = prev_home {
            std::env::set_var("HOME", h);
        } else {
            std::env::remove_var("HOME");
        }

        // dry_run reports counts of what *would* have been written.
        assert!(report.turns_upserted > 0);
        // ...but the DB stays empty.
        let conn = index::open_jack_db().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM turns", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        let m: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(m, 0);
    }

    #[test]
    fn reindex_is_idempotent() {
        let guard = DataDirGuard::new();
        let archive_root = guard._tmp.path();
        let _files = build_synth_archive(archive_root, 2, true);

        let prev_home = std::env::var_os("HOME");
        std::env::set_var("HOME", archive_root);
        let r1 = run_reindex(ReindexOptions {
            full: true,
            ..Default::default()
        })
        .unwrap();
        let r2 = run_reindex(ReindexOptions {
            full: true,
            ..Default::default()
        })
        .unwrap();
        if let Some(h) = prev_home {
            std::env::set_var("HOME", h);
        } else {
            std::env::remove_var("HOME");
        }

        assert_eq!(r1.turns_upserted, r2.turns_upserted);
        assert_eq!(r1.signatures_upserted, r2.signatures_upserted);
        assert_eq!(r1.sessions_upserted, r2.sessions_upserted);

        let conn = index::open_jack_db().unwrap();
        let turns_total: i64 = conn
            .query_row("SELECT COUNT(*) FROM turns", [], |r| r.get(0))
            .unwrap();
        assert_eq!(turns_total as usize, r1.turns_upserted);
    }

    #[test]
    fn reconcile_on_startup_drains_queue() {
        let _guard = DataDirGuard::new();
        let tmp = tempfile::TempDir::new().unwrap();
        let line = synth_assistant_line("turn-rec", "sess-rec", true);
        let path = write_synth_jsonl(tmp.path(), "sess-rec", &[line]);

        enqueue_pending_scan("sess-rec", Some(path.to_str().unwrap())).unwrap();
        reconcile_on_startup().unwrap();

        let conn = index::open_jack_db().unwrap();
        let pending: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_scans", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pending, 0);
        let turns: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE session_id = 'sess-rec'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(turns, 1);
    }

    #[test]
    fn estimated_hidden_tokens_nonneg() {
        // 250 visible bytes → ceil(250/4) = 63 visible tokens. With output=10
        // the estimate would be 10 - 63 = -53 → clamped to 0.
        let record = TurnRecord {
            turn_uuid: "u".into(),
            parent_turn_uuid: None,
            session_id: "s".into(),
            role: "assistant".into(),
            timestamp: "2026-04-15T00:00:00Z".into(),
            cwd: None,
            git_branch: None,
            is_sidechain: false,
            slug: None,
            claude_code_version: None,
            request_id: None,
            message_id: None,
            model: None,
            model_variant: None,
            usage: Some(UsageRecord {
                output_tokens: Some(10),
                ..Default::default()
            }),
            content_blocks: vec![ContentBlockRecord::Text {
                byte_len: 250,
                content_hash: None,
            }],
        };

        let _guard = DataDirGuard::new();
        let conn = index::open_jack_db().unwrap();
        // Need a parent session for upsert (FK is off but UPDATE relies on it
        // existing for any future read; we still upsert to be realistic).
        ensure_session_exists(&conn, "s", None, std::slice::from_ref(&record)).unwrap();
        upsert_turn_from_record(&conn, &record, 0, None).unwrap();
        let row = turns::get_turn(&conn, "u").unwrap().unwrap();
        assert_eq!(row.estimated_hidden_tokens, 0);
    }

    /// Dogfood acceptance gate. Run manually with:
    ///   cargo test dogfood_reindex_real_archive -- --ignored --nocapture
    /// This walks the real `~/.claude/projects/` archive and asserts the
    /// indexer produces sensible counts and no body-text leakage. Privacy
    /// audit asserts a known distinctive English phrase from a real session
    /// is absent from `content_blocks_meta` — proving the metadata-only
    /// invariant on real data.
    #[test]
    #[ignore]
    fn dogfood_reindex_real_archive() {
        let _guard = DataDirGuard::new();
        let report = run_reindex(ReindexOptions {
            full: true,
            ..Default::default()
        })
        .expect("reindex");

        eprintln!("dogfood report: {report:#?}");
        assert!(
            report.files_scanned >= 370,
            "files_scanned >= 370 (got {})",
            report.files_scanned
        );
        assert_eq!(report.files_failed, 0, "no file failures expected");
        assert!(report.sessions_upserted >= 370);
        assert!(report.turns_upserted >= 40_000);
        assert!(report.signatures_upserted >= 2_200);
        assert!(report.duration_seconds < 180.0);

        // Variant breakdown thresholds.
        let numbat = report
            .model_variant_breakdown
            .get("numbat-v6-efforts-10-20-40-ab-prod")
            .copied()
            .unwrap_or(0);
        let opus = report
            .model_variant_breakdown
            .get("claude-opus-4-6")
            .copied()
            .unwrap_or(0);
        assert!(numbat >= 1000, "numbat >= 1000 (got {numbat})");
        assert!(opus >= 900, "claude-opus-4-6 >= 900 (got {opus})");

        // Idempotency: re-run produces identical counts.
        let report2 = run_reindex(ReindexOptions {
            full: true,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(report.turns_upserted, report2.turns_upserted);
        assert_eq!(report.signatures_upserted, report2.signatures_upserted);
        assert_eq!(report.sessions_upserted, report2.sessions_upserted);

        // Privacy audit: assert known distinctive strings are NOT in the DB.
        // These are English phrases unlikely to appear in any model identifier
        // or hash; if they show up in content_blocks_meta the privacy
        // invariant is broken.
        let needles = [
            "felixx9527",     // user email handle
            "blob_hash",      // jack internal but not metadata
            "claude code is", // hypothetical body fragment
        ];
        let conn = index::open_jack_db().unwrap();
        for needle in needles {
            let pattern = format!("%{needle}%");
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM turns WHERE content_blocks_meta LIKE ?1",
                    params![pattern],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                n, 0,
                "PRIVACY: needle '{needle}' leaked into content_blocks_meta"
            );
        }

        // Aggregate sanity: hidden tokens estimate is in a wide expected range.
        // 2026-04-15 dogfood baseline on ~377 real sessions produced ~11M
        // hidden tokens — roughly 4x the plan's original 1–3M estimate. The
        // plan's number was a rough guess without real data; this is the true
        // number. Floor is a conservative sanity check (below 1M would mean
        // the formula is broken); ceiling accommodates natural archive growth.
        let total_hidden: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(estimated_hidden_tokens), 0) FROM turns",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            (1_000_000..=100_000_000).contains(&total_hidden),
            "total_hidden_tokens_est out of sanity range: {total_hidden}"
        );
    }
}
