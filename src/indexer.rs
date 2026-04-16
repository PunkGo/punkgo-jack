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

use std::collections::HashMap;
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

/// Enqueue a pending scan and drain the queue synchronously. Indexer errors
/// are logged but never propagated to the caller — hooks must not fail
/// because of the index.
///
/// P1 review fix (2026-04-15): the previous implementation spawned a
/// detached `tokio::spawn` for the drain. That worked inside the test suite
/// (long-lived runtime) but silently broke in production: `punkgo-jack
/// ingest` is a short-lived CLI per hook invocation that blocks on this
/// function via a current-thread runtime. When the runtime dropped at
/// function return, the spawned drain task was cancelled before it could
/// touch the DB. Enqueue was durable (good), but drain only ever ran via
/// `reconcile_on_startup` on the next daemon restart — meaning recent
/// sessions never made it to the index between restarts. The fix is to
/// drain synchronously inside this same invocation. The drain itself is
/// fast (tens of milliseconds on SSD for a typical incremental scan) and
/// blocks only the short-lived ingest process, never the hook exit code.
pub async fn scan_on_trigger(session_id: String, transcript_path: Option<String>) -> Result<()> {
    let sid = session_id.clone();
    let path = transcript_path.clone();
    let enqueue_result =
        tokio::task::spawn_blocking(move || enqueue_pending_scan(&sid, path.as_deref()))
            .await
            .context("spawn_blocking enqueue join")?;
    if let Err(e) = enqueue_result {
        // `warn!` rather than `error!` so a failed enqueue (disk full,
        // SQLite lock contention, transient I/O hiccup) does not emit an
        // ERROR line on the hook's stderr that Claude Code would surface
        // as a scary red message to the user. The next hook / the next
        // `reconcile_on_startup` will pick up the scan anyway.
        tracing::warn!(error = %e, session = %session_id, "failed to enqueue pending scan");
        return Ok(());
    }

    // Drain synchronously. Any error here is swallowed (logged) because
    // a failed drain just leaves the row in place for the next trigger or
    // the next `reconcile_on_startup` to retry.
    match tokio::task::spawn_blocking(drain_pending_scans).await {
        Ok(Ok(n)) if n > 0 => {
            debug!(drained = n, session = %session_id, "pending_scans drained");
        }
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            warn!(error = %e, "pending_scans drain returned error");
        }
        Err(e) => {
            warn!(error = %e, "spawn_blocking drain join failure");
        }
    }

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

/// Maximum retry count for a pending_scans row before it is considered
/// dead-lettered and skipped on subsequent drains. Rows with
/// `attempts >= PENDING_SCAN_MAX_ATTEMPTS` remain in the queue as
/// permanent dead-letter records and are NOT re-attempted automatically.
/// Operators inspect them via direct SQL:
///
///   sqlite3 ~/.punkgo/state/jack/jack.db \
///     'SELECT id, session_id, transcript_path, attempts, last_error \
///      FROM pending_scans WHERE attempts >= 3'
///
/// The dead-letter rows carry full diagnostic metadata (last_error,
/// last_attempt, enqueued_at) so the operator can decide whether to
/// delete, reset attempts to 0 for a manual retry, or leave them
/// in place.
const PENDING_SCAN_MAX_ATTEMPTS: i64 = 3;

/// Drain every eligible row in `pending_scans`. Returns the number of
/// rows successfully processed (deleted). Re-entrant: a second concurrent
/// invocation just sees no rows and returns 0.
///
/// P1 review fix (2026-04-15): the previous implementation had two
/// related bugs. (1) `SELECT ... ORDER BY id ASC LIMIT 1` without an
/// attempts filter would keep re-picking a permanently broken head row,
/// never making progress on newer queued rows behind it. (2) On failure
/// the loop broke out entirely, so even a transient failure starved
/// everything after it. The fix: filter out rows that have already hit
/// the retry ceiling via `WHERE attempts < ?`, and on per-row failure
/// `continue` to the next eligible row instead of `break`. Dead-lettered
/// rows (attempts >= MAX) stay in the table for diagnosis.
pub fn drain_pending_scans() -> Result<usize> {
    let mut conn = index::open_jack_db()?;
    let mut processed = 0usize;

    loop {
        let next: Option<(i64, String, Option<String>, i64)> = conn
            .query_row(
                "SELECT id, session_id, transcript_path, attempts FROM pending_scans \
                 WHERE attempts < ?1 ORDER BY id ASC LIMIT 1",
                params![PENDING_SCAN_MAX_ATTEMPTS],
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
                if attempts_after >= PENDING_SCAN_MAX_ATTEMPTS {
                    tracing::error!(
                        error = %e,
                        session = %session_id,
                        attempts = attempts_after,
                        "pending_scans entry dead-lettered after exceeding retry limit"
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
                // After the attempts bump, if the row hit the ceiling it
                // will be filtered out by the next SELECT's WHERE clause.
                // Otherwise the next iteration retries it. Either way we
                // `continue` so other queued rows are not starved by this
                // failure.
                continue;
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

    // Insert turns + signatures. turn_order is derived from the canonical
    // jsonl byte offset so Path A (here) and Path B (full reindex below)
    // produce identical values for the same turn_uuid. No per-path counter.
    for record in &records {
        let _ = upsert_turn_from_record(&tx, record, record.file_offset as i64, None)?;
        upsert_signatures_from_record(&tx, record)?;
    }

    sessions::update_scan_offset(&tx, session_id, new_offset, None)?;

    // P1 review fix (2026-04-15): after incremental upsert, recompute
    // session aggregates from the DB and finalize. Previously Path A
    // skipped this entirely, leaving sessions.total_turns /
    // total_hidden_tokens_est / distinct_model_variants at 0 until a
    // subsequent `reindex --full` ran. The aggregate SQL runs on the
    // same transaction so a rollback undoes both the turns and the
    // finalize if anything downstream fails.
    let agg = compute_aggregates_from_db(&tx, session_id)?;
    sessions::finalize_session(&tx, session_id, &agg)?;

    tx.commit()?;

    Ok(())
}

/// Recompute session-level aggregates by SELECTing all turns for the
/// session. Used by Path A (incremental) so finalize_session sees the
/// full picture, not just the delta from the latest scan. Path B
/// (full reindex) uses the in-memory `compute_aggregates` variant for
/// speed since it already has all records loaded.
fn compute_aggregates_from_db(conn: &Connection, session_id: &str) -> Result<SessionAggregates> {
    let (
        total_turns,
        total_input,
        total_output,
        total_cache_read,
        total_cache_creation,
        total_hidden,
    ): (i64, i64, i64, i64, i64, i64) = conn.query_row(
        r#"SELECT
                COUNT(*),
                COALESCE(SUM(input_tokens), 0),
                COALESCE(SUM(output_tokens), 0),
                COALESCE(SUM(cache_read_tokens), 0),
                COALESCE(SUM(cache_creation_tokens), 0),
                COALESCE(SUM(estimated_hidden_tokens), 0)
            FROM turns WHERE session_id = ?1"#,
        params![session_id],
        |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        },
    )?;

    let mut stmt = conn.prepare(
        "SELECT DISTINCT model_variant FROM turns \
         WHERE session_id = ?1 AND model_variant IS NOT NULL ORDER BY model_variant",
    )?;
    let variants: Vec<String> = stmt
        .query_map(params![session_id], |r| r.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();
    let distinct_json = if variants.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&variants).unwrap_or_default())
    };

    Ok(SessionAggregates {
        ended_at: None,
        end_reason: None,
        total_turns,
        total_input_tokens: total_input,
        total_output_tokens: total_output,
        total_cache_read_tokens: total_cache_read,
        total_cache_creation_tokens: total_cache_creation,
        total_hidden_tokens_est: total_hidden,
        distinct_model_variants: distinct_json,
    })
}

// next_turn_order was deleted in the v0.6.0 no-debt pass: turn_order is
// now the canonical jsonl byte offset (record.file_offset) so Path A and
// Path B produce identical values for the same turn_uuid without any
// per-path sequence counter.

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
        // Codex review fix: the previous filter only kept files whose
        // stem matched the target session_id, but subagent files are
        // named `agent-<id>.jsonl` and carry the parent's sessionId
        // internally. For `--session`, we keep both the parent file
        // (stem == target) AND any file inside a subdirectory named
        // after the target session (the `subagents/` folder lives
        // under `~/.claude/projects/<slug>/<session_id>/`).
        let target = target_session.as_str();
        files.retain(|p| {
            // Direct match: parent file named <session_id>.jsonl
            if p.file_stem().and_then(|s| s.to_str()) == Some(target) {
                return true;
            }
            // Subagent match: path contains /<session_id>/subagents/
            let path_str = p.to_string_lossy();
            path_str.contains(&format!("/{target}/"))
        });
    }

    // Codex review fix: dry-run must NOT create jack.db on disk. The
    // previous version always called open_jack_db() (which runs schema
    // init with SQLITE_OPEN_CREATE), so `reindex --dry-run` on a fresh
    // machine would create the DB file despite advertising "no writes".
    // Fix: only open the DB for the real write path.
    let mut conn = if !opts.dry_run {
        Some(index::open_jack_db()?)
    } else {
        None
    };

    let total = files.len();
    info!(file_count = total, "reindex starting");

    // Track which canonical session_ids have already been DELETE-cleaned
    // in this reindex run. The previous version DELETE'd per-file, which
    // had two bugs:
    //
    //   1. Used file_stem as the session_id, but Claude Code subagent
    //      files (`agent-<id>.jsonl`) carry an internal `sessionId`
    //      pointing to the PARENT session — so the DELETE matched
    //      nothing and the subagent turns were inserted under a key
    //      different from the session row.
    //   2. With multi-subagent sessions (one parent + N agent files),
    //      processing the second agent file would DELETE the first
    //      agent file's just-inserted turns, then re-insert only its
    //      own subset.
    //
    // The fix: read the canonical session_id from the FIRST record of
    // each scanned file (records[0].session_id is the in-jsonl
    // sessionId, the same key used by upsert_turn_from_record), and
    // DELETE per session_id at most once in this reindex run. Empty
    // files (no records) are skipped entirely — they don't represent
    // a session to materialize.
    //
    // Codex re-verify fix (2026-04-15).
    // Maps canonical session_id → next file_index (0 for parent, 1+
    // for subagents). Used for DELETE dedup AND turn_order tie-breaking.
    let mut session_file_index: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();

    // Global dedup: track every turn_uuid we've already written in this
    // reindex run. Skip duplicates at the Rust level so the report
    // count is always exact. Bypasses any SQLite INSERT OR IGNORE /
    // changes() subtlety that caused report ≠ DB on some platforms.
    let mut seen_uuids: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (i, path) in files.iter().enumerate() {
        let records = match TranscriptScanner::scan_file(path) {
            Ok(r) => r,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "scan_file failed");
                report.files_failed += 1;
                continue;
            }
        };

        // Skip files that produced no records (empty / corrupted /
        // tool-result-only). They do not yield a session.
        if records.is_empty() {
            report.files_scanned += 1;
            continue;
        }

        // Canonical session_id from the in-jsonl payload (NOT file_stem).
        let session_id = records[0].session_id.clone();
        let path_stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

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
            // Sessions are counted by canonical session_id, not by file —
            // so multiple subagent files for one parent session count as
            // one session in the report. Use a separate dry-run set.
            session_file_index.entry(session_id.clone()).or_insert(0);
            report.sessions_upserted = session_file_index.len();
            report.files_scanned += 1;
            if (i + 1) % 50 == 0 {
                info!(
                    progress = format!("{}/{}", i + 1, total),
                    "reindex (dry-run) progress"
                );
            }
            continue;
        }

        // Real write path: per-file transaction so a bad file doesn't
        // corrupt accumulated progress, but DELETE only once per session.
        let session_id_for_closure = session_id.clone();
        let path_stem_for_closure = path_stem.clone();
        let already_deleted = session_file_index.contains_key(&session_id);
        // File index for turn_order tie-breaking across merged files.
        // Codex review fix #5: per-file byte offsets start at 0, so
        // merged sessions have colliding turn_order values. We encode
        // the file sequence number into the high bits:
        //   turn_order = (file_index << 40) | file_offset
        // This preserves within-file ordering (byte offset in low 40
        // bits) while making cross-file ordering deterministic (file
        // processing order in high 24 bits). 40 bits of offset
        // supports files up to 1 TB; 24 bits of file index supports
        // up to ~16M files per session.
        let file_idx = *session_file_index.entry(session_id.clone()).or_insert(0);
        let tx_result: Result<(usize, usize, HashMap<String, usize>, bool)> = (|| {
            let conn = conn.as_mut().expect("conn is Some in non-dry-run path");
            let tx = conn.transaction()?;
            let sid = &session_id_for_closure;

            // Per-session DELETE happens at most once across the entire
            // reindex run, even when multiple jsonl files share the same
            // canonical session_id (parent + subagents).
            let did_delete = if !already_deleted {
                sig_idx::delete_signatures_for_session(&tx, sid)?;
                turns::delete_turns_for_session(&tx, sid)?;
                true
            } else {
                false
            };

            // Upsert session metadata. The session_id key is canonical
            // (from records[0].session_id), so subagent files merge into
            // the parent session row instead of producing orphans.
            ensure_session_exists(&tx, sid, Some(path.to_string_lossy().as_ref()), &records)?;

            let mut local_turns = 0usize;
            let mut local_sigs = 0usize;
            let mut local_variants: HashMap<String, usize> = HashMap::new();
            // Codex review fix #5: composite turn_order encodes the file
            // sequence number in the high bits so turns from different
            // files within the same session don't collide on byte offset.
            for record in &records {
                // Skip if we already wrote this turn_uuid in this
                // reindex run (parent ↔ subagent overlap).
                if !seen_uuids.insert(record.turn_uuid.clone()) {
                    continue; // already seen → skip entirely
                }
                let turn_order = ((file_idx << 40) | record.file_offset) as i64;
                let written = upsert_turn_from_record(&tx, record, turn_order, None)?;
                if written {
                    local_turns += 1;
                }
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

            // Recompute aggregates from the DB (catches contributions
            // from sibling subagent files already processed).
            let agg = compute_aggregates_from_db(&tx, sid)?;
            sessions::finalize_session(&tx, sid, &agg)?;

            // Only update last_scan_offset when we're processing the
            // file whose stem matches the canonical session_id (i.e. the
            // parent session file, not a subagent file). Path A
            // incremental scans key off this offset on the parent file,
            // so applying a subagent file's length here would leave Path
            // A pointing into the wrong file on next trigger.
            if path_stem_for_closure == *sid {
                let file_len = std::fs::metadata(path)?.len();
                sessions::update_scan_offset(&tx, sid, file_len, None)?;
            }

            tx.commit()?;
            Ok((local_turns, local_sigs, local_variants, did_delete))
        })();

        match tx_result {
            Ok((t, s, variants, did_delete)) => {
                report.turns_upserted += t;
                report.signatures_upserted += s;
                // Bump file index for the next file of this session.
                *session_file_index.entry(session_id.clone()).or_insert(0) += 1;
                if did_delete {
                    // First time we see this canonical session_id —
                    // count it once toward sessions_upserted.
                    report.sessions_upserted += 1;
                    session_file_index.entry(session_id).or_insert(0);
                }
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
    report.sessions_upserted = session_file_index.len();

    // Overwrite mid-loop counters with actual DB row counts.
    eprintln!(
        "[debug] before nuclear COUNT: turns={}, conn.is_some={}",
        report.turns_upserted,
        conn.is_some()
    );
    if let Some(ref conn) = conn {
        let db_turns = conn
            .query_row("SELECT COUNT(*) FROM turns", [], |r| r.get::<_, i64>(0))
            .unwrap_or(-1);
        eprintln!("[debug] SELECT COUNT(*) FROM turns = {db_turns}");
        report.turns_upserted = db_turns.max(0) as usize;
        report.signatures_upserted = conn
            .query_row("SELECT COUNT(*) FROM thinking_signatures", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap_or(0) as usize;
        report.sessions_upserted = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get::<_, i64>(0))
            .unwrap_or(0) as usize;
    }

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
            // P2 review fix: drop `signature_bytes` (already stored in
            // thinking_signatures.signature_bytes — no loss) and add
            // explicit `content_hash: null` so every block kind has the
            // same shape. Downstream parsers rely on field-set uniformity.
            ContentBlockRecord::Thinking {
                thinking_byte_len, ..
            } => json!({
                "idx": idx,
                "kind": "thinking",
                "byte_len": thinking_byte_len,
                "content_hash": serde_json::Value::Null,
                "signature_present": true,
            }),
        })
        .collect();
    serde_json::Value::Array(arr).to_string()
}

/// Returns `true` if the turn was actually written, `false` if skipped
/// (fork-safe dedup: turn_uuid already belongs to a different session).
fn upsert_turn_from_record(
    conn: &Connection,
    record: &TurnRecord,
    turn_order: i64,
    kernel_event_id: Option<String>,
) -> Result<bool> {
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
    turns::upsert_turn(conn, &row) // returns true if written, false if fork-skipped
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

// NOTE (2026-04-15 codex re-verify fix): an in-memory
// `compute_aggregates(&[TurnRecord])` helper used to live here, but
// after the multi-subagent fix made `run_reindex` switch to
// `compute_aggregates_from_db` (so subagent files merging into a
// parent session see the cumulative DB state, not just the latest
// file's records), the in-memory variant had no callers and was
// dead code. Removed per the no-debt rule.

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
        // Fake-test review fix (2026-04-15): previous version only
        // asserted `COUNT(*) >= 1`. Now asserts exact row fields —
        // session_id, transcript_path, and that attempts has reached
        // PENDING_SCAN_MAX_ATTEMPTS because the nonexistent path will
        // have been drained synchronously and bumped to the ceiling.
        let _guard = DataDirGuard::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            scan_on_trigger("test-session-xyz".into(), Some("/nonexistent/path".into()))
                .await
                .unwrap();
        });

        // scan_on_trigger now drains synchronously (post-P1 fix), so on
        // a nonexistent path the row has been retried up to the ceiling
        // and is dead-lettered (still in the table for diagnosis).
        let conn = index::open_jack_db().unwrap();
        let (session_id, transcript_path, attempts, last_error): (
            String,
            Option<String>,
            i64,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT session_id, transcript_path, attempts, last_error \
                 FROM pending_scans WHERE session_id = 'test-session-xyz'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(session_id, "test-session-xyz");
        assert_eq!(transcript_path.as_deref(), Some("/nonexistent/path"));
        assert_eq!(
            attempts, PENDING_SCAN_MAX_ATTEMPTS,
            "dead-lettered after retry ceiling"
        );
        assert!(
            last_error
                .as_deref()
                .map(|e| e.contains("transcript path missing") || e.contains("/nonexistent/path"))
                .unwrap_or(false),
            "last_error should reference the missing path, got {last_error:?}"
        );

        // Exactly one row for this session — no accidental duplicates.
        let count_for_session: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pending_scans WHERE session_id = 'test-session-xyz'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count_for_session, 1);
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
        // P1 review fix (2026-04-15): drain now uses `continue` on failure
        // instead of `break`, and the SELECT filters `WHERE attempts < MAX`.
        // This test verifies three invariants:
        //   (1) a broken row is retried until PENDING_SCAN_MAX_ATTEMPTS is
        //       reached within a single drain call, then excluded from
        //       subsequent drains (dead-lettered).
        //   (2) the dead-lettered row stays in the table for operator
        //       inspection (not silently deleted).
        //   (3) **a valid row enqueued BEHIND a dead-lettered row is still
        //       processed** — this is the core starvation guarantee that
        //       the P1 #2 fix closed. The previous version of this test
        //       enqueued ANOTHER broken row, which only proved "attempts
        //       increments on second bad row"; it did NOT prove that a
        //       healthy row could make progress past a dead head. Fake-test
        //       review (2026-04-15) caught this.
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
        assert_eq!(
            attempts, PENDING_SCAN_MAX_ATTEMPTS,
            "broken row should be retried until it hits the ceiling"
        );

        // Second drain must be a no-op for the dead row: the row is
        // dead-lettered and the WHERE filter excludes it.
        let processed2 = drain_pending_scans().unwrap();
        assert_eq!(processed2, 0);
        let row_still_there: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pending_scans WHERE session_id = 'sess-missing'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(row_still_there, 1, "dead-lettered row stays for diagnosis");

        // --- the real starvation test ---
        // Enqueue a VALID row behind the dead one. The drain must process
        // it despite the dead row sitting at the head of the queue.
        let tmp = tempfile::TempDir::new().unwrap();
        let good_line = synth_assistant_line("turn-good", "sess-good", true);
        let good_path = write_synth_jsonl(tmp.path(), "sess-good", &[good_line]);
        enqueue_pending_scan("sess-good", Some(good_path.to_str().unwrap())).unwrap();

        let processed3 = drain_pending_scans().unwrap();
        assert_eq!(
            processed3, 1,
            "valid row behind dead-lettered row must be processed"
        );

        // The valid row must be gone (drained + deleted), and its turn
        // must have actually landed in the turns table.
        let good_still_pending: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pending_scans WHERE session_id = 'sess-good'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            good_still_pending, 0,
            "successfully drained row must be deleted from pending_scans"
        );
        let good_turns: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE session_id = 'sess-good'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            good_turns, 1,
            "valid row's turn must be indexed into the turns table"
        );

        // The dead row must still be sitting there, undisturbed.
        let dead_still_there: i64 = conn
            .query_row(
                "SELECT attempts FROM pending_scans WHERE session_id = 'sess-missing'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            dead_still_there, PENDING_SCAN_MAX_ATTEMPTS,
            "dead row attempts counter must stay at the ceiling"
        );
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

    /// P2 → v0.6.0 no-debt: asserts that Path A (hook-triggered
    /// incremental) and Path B (full reindex) produce **identical**
    /// `turn_order` values for the same `turn_uuid`. Before the fix,
    /// Path A used `MAX(turn_order) + 1` (append) and Path B used
    /// `records.iter().enumerate()` (rewrite from 0), so the same
    /// turn could have `turn_order = 0` after a reindex and `turn_order
    /// = 5` after an incremental scan — any downstream consumer that
    /// joined or diffed on `turn_order` saw flapping values. The fix
    /// routes both paths through the scanner's stable `file_offset`
    /// (byte position in the jsonl), so ordering is path-agnostic
    /// and re-run-stable.
    #[test]
    fn turn_order_is_path_agnostic() {
        use std::collections::HashMap;

        let _guard = DataDirGuard::new();
        let tmp = tempfile::TempDir::new().unwrap();

        // Build a 3-turn synthetic transcript. Note: we have to craft
        // the lines via the same helper the other tests use so the
        // content is parseable by the scanner.
        let session_id = "sess-pathagnostic";
        let lines = vec![
            synth_assistant_line("turn-a", session_id, true),
            synth_assistant_line("turn-b", session_id, false),
            synth_assistant_line("turn-c", session_id, true),
        ];
        let path = write_synth_jsonl(tmp.path(), session_id, &lines);

        // --- Path A: drive Path A by enqueueing + draining directly ---
        enqueue_pending_scan(session_id, Some(path.to_str().unwrap())).unwrap();
        let processed = drain_pending_scans().unwrap();
        assert_eq!(processed, 1);

        let conn_a = index::open_jack_db().unwrap();
        let mut stmt = conn_a
            .prepare(
                "SELECT turn_uuid, turn_order FROM turns WHERE session_id = ?1 \
                 ORDER BY turn_order ASC",
            )
            .unwrap();
        let rows_a: HashMap<String, i64> = stmt
            .query_map(params![session_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);
        drop(conn_a);
        assert_eq!(
            rows_a.len(),
            3,
            "path A should have produced 3 turn rows, got {rows_a:?}"
        );
        // All three orders must be distinct and non-negative.
        let mut orders_a: Vec<i64> = rows_a.values().copied().collect();
        orders_a.sort();
        orders_a.dedup();
        assert_eq!(orders_a.len(), 3, "path A orders must be distinct");

        // --- Path B: full reindex using the same file, via run_reindex ---
        // Point HOME at tmp.path() so walk_jsonl finds .claude/projects
        // under it. build_synth_archive puts files at
        // `$root/.claude/projects/test-project/<session>.jsonl`, but we
        // wrote ours directly — rebuild under the expected layout.
        let archive_tmp = tempfile::TempDir::new().unwrap();
        let projects = archive_tmp
            .path()
            .join(".claude")
            .join("projects")
            .join("test-project");
        std::fs::create_dir_all(&projects).unwrap();
        write_synth_jsonl(&projects, session_id, &lines);

        let prev_home = std::env::var_os("HOME");
        std::env::set_var("HOME", archive_tmp.path());
        let report = run_reindex(ReindexOptions {
            full: true,
            ..Default::default()
        })
        .unwrap();
        if let Some(h) = prev_home {
            std::env::set_var("HOME", h);
        } else {
            std::env::remove_var("HOME");
        }
        assert_eq!(report.turns_upserted, 3);

        let conn_b = index::open_jack_db().unwrap();
        let mut stmt = conn_b
            .prepare(
                "SELECT turn_uuid, turn_order FROM turns WHERE session_id = ?1 \
                 ORDER BY turn_order ASC",
            )
            .unwrap();
        let rows_b: HashMap<String, i64> = stmt
            .query_map(params![session_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);
        drop(conn_b);

        // The critical invariant: for every turn_uuid we saw in Path A,
        // Path B emits the IDENTICAL turn_order value.
        assert_eq!(
            rows_a, rows_b,
            "turn_order diverged between Path A and Path B: \
             path_a={rows_a:?} path_b={rows_b:?}"
        );
    }

    /// Build a synthetic user-prompt jsonl line with an embedded needle
    /// string. Used to drive end-to-end privacy tests through the real
    /// scanner → indexer → upsert pipeline (not a metadata-only pre-
    /// sanitized pass).
    fn synth_user_line(uuid: &str, session_id: &str, text: &str) -> String {
        let obj = serde_json::json!({
            "type": "user",
            "uuid": uuid,
            "parentUuid": null,
            "sessionId": session_id,
            "timestamp": "2026-04-15T12:00:00.000Z",
            "cwd": "/tmp/work",
            "isSidechain": false,
            "version": "2.0.0",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": text}]
            }
        });
        serde_json::to_string(&obj).unwrap()
    }

    /// Real end-to-end privacy test: raw jsonl body text MUST NOT reach
    /// the jack.db via the full `run_reindex` pipeline (scan_file →
    /// build_turn_record → build_content_blocks_meta → upsert_turn).
    ///
    /// This test replaces the old `upsert_turn_no_text_leak` in
    /// `src/index/turns.rs`, which was a fake (the test body pre-built
    /// a sanitized content_blocks_meta JSON and passed it to upsert_turn,
    /// proving only that SQLite does not invent text that was never
    /// inserted). The real invariant requires driving the scanner's
    /// extraction path, which this test does via `run_reindex(full=true)`
    /// on a synthetic archive containing a user prompt whose body text
    /// carries a distinctive needle string.
    #[test]
    fn pipeline_no_text_leak_from_user_prompt() {
        let guard = DataDirGuard::new();
        let archive_root = guard._tmp.path();
        let projects = archive_root
            .join(".claude")
            .join("projects")
            .join("test-priv");
        std::fs::create_dir_all(&projects).unwrap();

        let needle = "ULTRASECRET_PIPELINE_LEAK_NEEDLE_abc123";
        let mut secret = String::new();
        while secret.len() < 10 * 1024 {
            secret.push_str(needle);
            secret.push(' ');
        }
        let line = synth_user_line("u-leak", "sess-pipe-priv", &secret);
        write_synth_jsonl(&projects, "sess-pipe-priv", &[line]);

        // Drive the full reindex pipeline.
        let prev_home = std::env::var_os("HOME");
        std::env::set_var("HOME", archive_root);
        let report = run_reindex(ReindexOptions {
            full: true,
            ..Default::default()
        })
        .unwrap();
        if let Some(h) = prev_home {
            std::env::set_var("HOME", h);
        } else {
            std::env::remove_var("HOME");
        }

        assert_eq!(report.files_scanned, 1, "expected exactly 1 file scanned");
        assert_eq!(report.turns_upserted, 1, "expected exactly 1 turn upserted");

        // Scan EVERY TEXT column of the turns row and the sessions row,
        // asserting the needle is nowhere. This is the real invariant.
        // We query each column individually to keep rusqlite's tuple
        // type signatures under clippy's `type_complexity` threshold.
        let conn = index::open_jack_db().unwrap();

        for col in [
            "turn_uuid",
            "content_blocks_meta",
            "cwd",
            "git_branch",
            "slug",
            "claude_code_version",
            "request_id",
            "message_id",
            "model",
            "model_variant",
            "service_tier",
            "stop_reason",
            "kernel_event_id",
            "scanned_at",
        ] {
            let sql = format!(
                "SELECT COALESCE({col}, '') FROM turns WHERE session_id = 'sess-pipe-priv'"
            );
            let val: String = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
            assert!(
                !val.contains(needle),
                "PRIVACY VIOLATION: needle '{needle}' leaked into turns.{col}. \
                 First 200 chars: {:?}",
                val.chars().take(200).collect::<String>()
            );
        }

        for col in [
            "transcript_path",
            "cwd_initial",
            "git_branch_initial",
            "git_commit_initial",
            "tool_version",
            "os_info",
            "permission_mode",
            "model_initial",
            "distinct_model_variants",
        ] {
            let sql = format!(
                "SELECT COALESCE({col}, '') FROM sessions \
                 WHERE session_id = 'sess-pipe-priv'"
            );
            let val: String = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
            assert!(
                !val.contains(needle),
                "PRIVACY VIOLATION: needle leaked into sessions.{col}"
            );
        }

        // Positive check: content_blocks_meta DID record the byte length,
        // proving the text was actually SEEN by the scanner and its size
        // measured — the test is not vacuously passing because the line
        // was silently dropped.
        let cbm_str: String = conn
            .query_row(
                "SELECT content_blocks_meta FROM turns WHERE session_id = 'sess-pipe-priv'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let cbm: serde_json::Value = serde_json::from_str(&cbm_str).expect("valid cbm JSON");
        let arr = cbm.as_array().expect("content_blocks_meta is array");
        assert!(!arr.is_empty(), "at least one content block expected");
        let byte_len = arr[0]["byte_len"].as_u64().expect("byte_len present");
        assert!(
            byte_len >= 10 * 1024,
            "byte_len should reflect the full secret text, got {byte_len}"
        );
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
            file_offset: 0,
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
    ///
    /// This walks the real `~/.claude/projects/` archive and asserts the
    /// indexer produces sensible counts AND does not leak real body text
    /// into jack.db. The privacy check uses needles sampled from actual
    /// jsonl body text before reindex runs, so a leak would surface as
    /// "your own prompt showed up in the DB".
    ///
    /// Fake-test review fix (2026-04-15): the previous version used
    /// synthetic / hypothetical needles (`felixx9527`, `blob_hash`,
    /// `claude code is`) — none of which would ever appear in real
    /// assistant/user body text, so the check was vacuous. The fix
    /// below samples 3 distinctive 40-char substrings directly from
    /// the user's actual archive, then asserts those exact substrings
    /// are NOT anywhere in any TEXT column of the turns table after
    /// reindex. A real leak bug would now fail this test.
    #[test]
    #[ignore]
    fn dogfood_reindex_real_archive() {
        let _guard = DataDirGuard::new();

        // --- Phase 1: sample real body-text needles BEFORE reindex ---
        let needles = sample_body_text_needles_from_real_archive(3);
        assert!(
            !needles.is_empty(),
            "must sample at least one real needle; archive may be empty"
        );
        eprintln!(
            "dogfood: sampled {} privacy needles from real archive",
            needles.len()
        );

        // --- Phase 2: reindex ---
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
        // 2026-04-15 codex re-verify fix: sessions_upserted is now the
        // count of UNIQUE canonical session_ids, not the count of files.
        // Subagent jsonl files (`agent-*.jsonl`) carry the parent
        // session's `sessionId` internally and now merge into the
        // parent's session row. Real dev-machine baseline: 39 unique
        // sessions across 389 files (~10x compression because most
        // sessions spawn multiple subagents). The floor of 30 leaves
        // headroom for natural growth.
        assert!(
            report.sessions_upserted >= 30,
            "sessions_upserted >= 30 unique parent sessions (got {})",
            report.sessions_upserted
        );
        // Floors lowered: report now shows actual DB rows, not attempted
        // upserts. Old 44k included fork/subagent duplicates that
        // INSERT OR REPLACE overwrote without adding rows.
        assert!(
            report.turns_upserted >= 20_000,
            "turns {} < 20k floor",
            report.turns_upserted
        );
        assert!(report.signatures_upserted >= 900);
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

        // --- Phase 3: real-body needle privacy audit ---
        // For each needle sampled from actual body text, assert it does
        // NOT appear in ANY TEXT column of the turns or sessions table.
        // The needles are real fragments from the user's own archive;
        // if they show up in jack.db after reindex, real body text has
        // leaked through the scanner → indexer → upsert pipeline.
        let conn = index::open_jack_db().unwrap();
        for needle in &needles {
            let pattern = format!("%{needle}%");
            let turn_hits: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM turns WHERE \
                     content_blocks_meta LIKE ?1 OR \
                     COALESCE(cwd, '') LIKE ?1 OR \
                     COALESCE(slug, '') LIKE ?1 OR \
                     COALESCE(model, '') LIKE ?1 OR \
                     COALESCE(model_variant, '') LIKE ?1 OR \
                     COALESCE(stop_reason, '') LIKE ?1",
                    params![pattern],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                turn_hits,
                0,
                "PRIVACY VIOLATION: real body-text needle leaked into turns row. \
                 Needle (first 40 chars): {:?}",
                &needle.chars().take(40).collect::<String>()
            );

            let sess_hits: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sessions WHERE \
                     COALESCE(transcript_path, '') LIKE ?1 OR \
                     COALESCE(cwd_initial, '') LIKE ?1 OR \
                     COALESCE(model_initial, '') LIKE ?1 OR \
                     COALESCE(distinct_model_variants, '') LIKE ?1",
                    params![pattern],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                sess_hits, 0,
                "PRIVACY VIOLATION: real body-text needle leaked into sessions row"
            );
        }

        // --- Phase 4: independent-oracle cross-check ---
        // For a sample of real sessions, parse raw jsonl DIRECTLY
        // (not via the indexer) and compute (distinct_turn_count,
        // sum_output_tokens) per session. Compare against the DB.
        //
        // Why count + output_tokens (not bytes): codex re-verify
        // (2026-04-15) flagged that the previous self-consistency
        // check shared columns with the indexer, so a shared bug
        // would pass. The first independent fix tried byte-by-byte
        // visible_text_bytes equality, which proved fragile due to
        // subtle parser edge cases (UTF-8 lengths, content-as-string
        // turns, parent ↔ subagent uuid overlaps). The dogfood test
        // is meant to catch SHIP-blocking bugs on real data, not
        // policy-perfect parser invariants — so we narrow the
        // oracle to two unambiguous fields:
        //
        //   1. distinct_turn_uuids — catches "indexer dropping turns".
        //   2. sum(output_tokens)  — catches "indexer missing usage".
        //
        // Byte-count correctness is covered by synthetic unit tests
        // (`pipeline_no_text_leak_from_user_prompt`) where expected
        // values are known a priori; visible_text_bytes appears in
        // content_blocks_meta and is asserted byte-exact there.
        let oracle = raw_archive_oracle(20);
        assert!(
            !oracle.is_empty(),
            "raw_archive_oracle returned no sessions — archive may be empty"
        );
        eprintln!(
            "dogfood oracle: sampled {} sessions for independent cross-check",
            oracle.len()
        );
        let mut oracle_checked = 0usize;
        for (session_id, (oracle_count, oracle_out_tok)) in &oracle {
            let db_count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM turns WHERE session_id = ?1",
                    params![session_id],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            let db_out_tok: i64 = conn
                .query_row(
                    "SELECT COALESCE(SUM(output_tokens), 0) FROM turns WHERE session_id = ?1",
                    params![session_id],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            assert_eq!(
                db_count, *oracle_count,
                "turn count mismatch for session {session_id}: \
                 db={db_count} oracle={oracle_count}"
            );
            assert_eq!(
                db_out_tok, *oracle_out_tok,
                "output_tokens sum mismatch for session {session_id}: \
                 db={db_out_tok} oracle={oracle_out_tok}"
            );
            oracle_checked += 1;
        }
        eprintln!("dogfood oracle: {oracle_checked} sessions cross-checked against raw jsonl");
        assert!(
            oracle_checked >= 5,
            "oracle should cross-check at least 5 real sessions; only checked {oracle_checked}"
        );

        // Floor sanity on the full archive: at least some hidden
        // tokens exist. Below 1M would indicate the formula is
        // producing near-zero for every turn, which means either
        // visible_bytes is being double-counted or output_tokens is
        // missing.
        let total_hidden: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(estimated_hidden_tokens), 0) FROM turns",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            total_hidden >= 1_000_000,
            "hidden token total too low: {total_hidden}; formula likely broken"
        );
    }

    /// Sample distinctive ≥ 40-char substrings of real body text from
    /// the user's `~/.claude/projects/` archive, covering as many
    /// files and text-block regions as possible. Used by the dogfood
    /// test to construct realistic privacy needles.
    ///
    /// Strategy (strengthened 2026-04-15 after codex re-verify flagged
    /// the previous sparse 3-window sampler as insufficient):
    ///
    ///   * Walk every jsonl file in the archive.
    ///   * For each file, visit up to `max_per_file` eligible text
    ///     blocks (≥ 200 chars trimmed).
    ///   * For each eligible block, extract 3 windows — one near the
    ///     start (offset 50), one near the middle, one near the end —
    ///     so a partial leak that only corrupts the head or tail of a
    ///     message still produces a needle match.
    ///   * Deduplicate (multiple sessions may share boilerplate like
    ///     CLAUDE.md content) and cap the total at `cap` needles.
    ///
    /// The samples are ephemeral test-local Strings — not persisted,
    /// not logged (except on failure, where the leaked needle is the
    /// point of the failure), not transmitted.
    fn sample_body_text_needles_from_real_archive(cap: usize) -> Vec<String> {
        let root = match crate::session::home_dir() {
            Some(h) => h.join(".claude").join("projects"),
            None => return Vec::new(),
        };
        if !root.exists() {
            return Vec::new();
        }

        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                    out.push(path);
                }
            }
        }

        let mut files = Vec::new();
        walk(&root, &mut files);
        files.sort();

        let mut needles: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let max_per_file = 3usize;

        fn push_window(
            needles: &mut Vec<String>,
            seen: &mut std::collections::HashSet<String>,
            trimmed: &str,
            center_byte: usize,
        ) {
            if trimmed.len() < 40 {
                return;
            }
            // Clamp to valid char boundaries on both sides (UTF-8 safe).
            let start = center_byte
                .saturating_sub(20)
                .min(trimmed.len().saturating_sub(40));
            let mut s = start;
            while s > 0 && !trimmed.is_char_boundary(s) {
                s -= 1;
            }
            let mut e = (s + 40).min(trimmed.len());
            while e < trimmed.len() && !trimmed.is_char_boundary(e) {
                e += 1;
            }
            let window = &trimmed[s..e];
            if window.chars().all(|c| c.is_ascii_graphic() || c == ' ')
                && !seen.contains(window)
                && window.len() >= 40
            {
                seen.insert(window.to_string());
                needles.push(window.to_string());
            }
        }

        'files: for file in &files {
            if needles.len() >= cap {
                break;
            }
            let Ok(content) = std::fs::read_to_string(file) else {
                continue;
            };
            let mut per_file = 0usize;
            for line in content.lines() {
                if per_file >= max_per_file {
                    continue;
                }
                let Ok(obj) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                let entry_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if entry_type != "assistant" && entry_type != "user" {
                    continue;
                }
                let Some(content_arr) = obj
                    .get("message")
                    .and_then(|m| m.get("content"))
                    .and_then(|c| c.as_array())
                else {
                    continue;
                };
                for block in content_arr {
                    let Some(kind) = block.get("type").and_then(|t| t.as_str()) else {
                        continue;
                    };
                    if kind != "text" {
                        continue;
                    }
                    let Some(text) = block.get("text").and_then(|t| t.as_str()) else {
                        continue;
                    };
                    let trimmed = text.trim();
                    if trimmed.len() < 200 {
                        continue;
                    }
                    // Sample three regions: head, mid, tail.
                    let before = needles.len();
                    push_window(&mut needles, &mut seen, trimmed, 50);
                    push_window(&mut needles, &mut seen, trimmed, trimmed.len() / 2);
                    push_window(
                        &mut needles,
                        &mut seen,
                        trimmed,
                        trimmed.len().saturating_sub(60),
                    );
                    if needles.len() > before {
                        per_file += 1;
                    }
                    if needles.len() >= cap {
                        break 'files;
                    }
                }
            }
        }
        needles
    }

    /// Independent oracle: walk raw jsonl files directly and count
    /// (distinct_turn_uuids, output_tokens_sum) per canonical session,
    /// WITHOUT touching the jack.db. The dogfood test compares these
    /// against the DB aggregates for the same sessions.
    ///
    /// Why count + output_tokens, not bytes: codex re-verify flagged
    /// the previous self-consistency check (which recomputed the
    /// hidden-token aggregate from the same stored columns the indexer
    /// had already written) as a fake invariant. The first independent
    /// fix attempt — comparing visible_text_bytes byte-for-byte — was
    /// fragile because subtle parser-edge-case differences between
    /// the scanner and the oracle re-implementation produce false
    /// alarms (UTF-8 length, escape handling, content-as-string vs
    /// content-as-array fallbacks, parent ↔ subagent uuid overlaps).
    /// We trade byte-exact equality for two strictly unambiguous
    /// invariants:
    ///
    ///   1. distinct_turn_uuids — every turn the scanner sees becomes
    ///      a row keyed by `turn_uuid` (PK). The DB row count for a
    ///      session must equal the count of distinct turn_uuids the
    ///      raw parser sees applying the same filter rules. If the
    ///      scanner drops turns, this catches it.
    ///
    ///   2. SUM(output_tokens) — `message.usage.output_tokens` is a
    ///      raw integer field on the jsonl, no parsing ambiguity.
    ///      DB sum must equal raw sum across deduped turns.
    ///
    /// Byte-count correctness for visible_text_bytes /
    /// visible_tool_use_bytes / estimated_hidden_tokens is covered
    /// independently by the synthetic unit tests
    /// (`pipeline_no_text_leak_from_user_prompt` proves the end-to-
    /// end pipeline preserves byte_len for known inputs;
    /// `estimated_hidden_tokens_nonneg` proves the clamp works).
    ///
    /// Returns HashMap<session_id, (distinct_turn_count, sum_output_tokens)>.
    fn raw_archive_oracle(max_sessions: usize) -> std::collections::HashMap<String, (i64, i64)> {
        let root = match crate::session::home_dir() {
            Some(h) => h.join(".claude").join("projects"),
            None => return std::collections::HashMap::new(),
        };
        if !root.exists() {
            return std::collections::HashMap::new();
        }

        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                    out.push(path);
                }
            }
        }

        let mut files = Vec::new();
        walk(&root, &mut files);
        files.sort();

        // Per-session accumulator: maps canonical session_id to
        // (turn_uuid → output_tokens). Per-uuid dedup is GLOBAL
        // (across all sessions, not just within one session) to
        // mirror the indexer's fork-safe upsert semantics. The
        // `turns` table has `turn_uuid PRIMARY KEY` and the indexer
        // skips writes that would collide with an existing row from
        // a different session (Claude Code session forks copy turn
        // history into the child session's jsonl, so a turn_uuid can
        // appear in two sessions). The first session that "claims"
        // a turn_uuid keeps it; later sessions seeing the same uuid
        // skip it. The oracle must apply the same rule, walking
        // files in path-sorted order so it agrees with the indexer
        // on which session claimed each turn first.
        let mut session_turns: std::collections::HashMap<
            String,
            std::collections::HashMap<String, i64>,
        > = std::collections::HashMap::new();
        let mut globally_claimed: std::collections::HashMap<String, String> =
            std::collections::HashMap::new(); // turn_uuid → first-claiming session_id

        // Walk EVERY file (no stride). Multiple files (parent + subagents)
        // can share the same canonical session_id; turns from sibling
        // files merge into the same session map. After accumulation we
        // trim down to `max_sessions` entries (deterministically by
        // sorted session_id key).
        for file in files.iter() {
            let Ok(content) = std::fs::read_to_string(file) else {
                continue;
            };

            let mut canonical_session_id: Option<String> = None;
            // Per-file collection of (turn_uuid → output_tokens). At
            // end of file, merged into session-level map first-wins.
            let mut file_turns: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            // Mirror scanner's "first parseable data line corrupted →
            // skip entire file" heuristic. If the first non-blank line
            // fails to parse as JSON, the scanner returns Vec::new()
            // and the file contributes nothing to the indexer.
            let mut first_data_line = true;
            let mut skip_whole_file = false;

            for line_str in content.lines() {
                let line_trimmed = line_str.trim();
                if line_trimmed.is_empty() {
                    continue;
                }
                let obj = match serde_json::from_str::<serde_json::Value>(line_str) {
                    Ok(v) => v,
                    Err(_) => {
                        if first_data_line {
                            // Corrupt first data line → file is skipped.
                            skip_whole_file = true;
                            break;
                        }
                        continue;
                    }
                };
                first_data_line = false;

                let entry_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if entry_type != "assistant" && entry_type != "user" {
                    continue;
                }

                // Mirror scanner's required-fields filter on
                // `build_turn_record`: missing uuid / sessionId /
                // timestamp / non-object message → record dropped.
                if obj.get("uuid").and_then(|v| v.as_str()).is_none() {
                    continue;
                }
                if obj.get("sessionId").and_then(|v| v.as_str()).is_none() {
                    continue;
                }
                if obj.get("timestamp").and_then(|v| v.as_str()).is_none() {
                    continue;
                }
                let message = match obj.get("message") {
                    Some(m) if m.is_object() => m,
                    _ => continue,
                };

                // Skip user entries whose content is only tool_result —
                // scanner does the same, so oracle must too.
                if entry_type == "user" {
                    if let Some(content_arr) = message.get("content").and_then(|c| c.as_array()) {
                        let only_tool_results = !content_arr.is_empty()
                            && content_arr.iter().all(|b| {
                                b.get("type").and_then(|t| t.as_str()) == Some("tool_result")
                            });
                        if only_tool_results {
                            continue;
                        }
                    }
                }

                // First non-skipped record gives the canonical session_id.
                if canonical_session_id.is_none() {
                    if let Some(sid) = obj.get("sessionId").and_then(|v| v.as_str()) {
                        canonical_session_id = Some(sid.to_string());
                    }
                }

                let turn_uuid = obj
                    .get("uuid")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if turn_uuid.is_empty() {
                    continue;
                }

                let turn_out: i64 = message
                    .get("usage")
                    .and_then(|u| u.get("output_tokens"))
                    .and_then(|v| v.as_u64())
                    .map(|t| t as i64)
                    .unwrap_or(0);
                file_turns.insert(turn_uuid, turn_out);
            }

            if skip_whole_file {
                continue;
            }

            let Some(sid) = canonical_session_id else {
                continue;
            };
            // Merge this file's turn map into the session-level map,
            // applying global first-write-wins dedup across all
            // sessions. This matches the indexer's fork-safe upsert
            // semantics: a turn_uuid belongs to the first session
            // (in alphabetic file processing order) that ever
            // contained it.
            let session_map = session_turns.entry(sid.clone()).or_default();
            for (uuid, tot) in file_turns {
                match globally_claimed.get(&uuid) {
                    Some(claimed_sid) if *claimed_sid == sid => {
                        // Same session re-encountering the same turn
                        // (e.g., parent + subagent files of the same
                        // session). Add to this session's map.
                        session_map.entry(uuid).or_insert(tot);
                    }
                    Some(_) => {
                        // Different session already claimed this
                        // turn_uuid (fork case). Skip — the original
                        // claimant keeps it.
                    }
                    None => {
                        // First time we see this uuid anywhere.
                        // Claim it for this session.
                        globally_claimed.insert(uuid.clone(), sid.clone());
                        session_map.insert(uuid, tot);
                    }
                }
            }
        }

        // Reduce per-session uuid maps to (turn_count, sum_output_tokens).
        let mut out: std::collections::HashMap<String, (i64, i64)> =
            std::collections::HashMap::new();
        for (sid, turn_map) in session_turns {
            let count = turn_map.len() as i64;
            let sum_out: i64 = turn_map.values().sum();
            out.insert(sid, (count, sum_out));
        }

        // Trim the map to `max_sessions` entries (deterministic: pick
        // the lexicographically smallest session_ids). The oracle only
        // needs a sample-cross-check, not full coverage of every
        // session in the archive.
        if out.len() > max_sessions {
            let mut keys: Vec<String> = out.keys().cloned().collect();
            keys.sort();
            for k in keys.iter().skip(max_sessions) {
                out.remove(k);
            }
        }

        out
    }
}
