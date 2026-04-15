//! `sessions` table CRUD.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use super::{now_iso, SCANNER_VERSION};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionRow {
    pub session_id: String,
    pub source: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub end_reason: Option<String>,
    pub transcript_path: Option<String>,

    pub cwd_initial: Option<String>,
    pub git_branch_initial: Option<String>,
    pub git_commit_initial: Option<String>,
    pub tool_version: Option<String>,
    pub os_info: Option<String>,
    pub permission_mode: Option<String>,
    pub model_initial: Option<String>,

    pub loaded_instructions: Option<String>,
    pub env_snapshot: Option<String>,

    pub total_turns: i64,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_creation_tokens: i64,
    pub total_hidden_tokens_est: i64,
    pub distinct_model_variants: Option<String>,

    pub last_scan_offset: i64,
    pub last_scan_at: Option<String>,
    pub scan_failure_count: i64,

    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Default)]
pub struct SessionAggregates {
    pub ended_at: Option<String>,
    pub end_reason: Option<String>,
    pub total_turns: i64,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_creation_tokens: i64,
    pub total_hidden_tokens_est: i64,
    pub distinct_model_variants: Option<String>,
}

/// INSERT OR REPLACE that preserves `created_at` from the existing row if
/// one exists. The caller's `session.created_at` is used only when the row
/// is brand new.
pub fn upsert_session(conn: &Connection, session: &SessionRow) -> Result<()> {
    let existing_created_at: Option<String> = conn
        .query_row(
            "SELECT created_at FROM sessions WHERE session_id = ?1",
            params![session.session_id],
            |r| r.get(0),
        )
        .optional()
        .context("looking up existing session created_at")?;
    let created_at = existing_created_at.unwrap_or_else(|| session.created_at.clone());
    let updated_at = now_iso();

    conn.execute(
        r#"
        INSERT OR REPLACE INTO sessions (
            session_id, source, started_at, ended_at, end_reason, transcript_path,
            cwd_initial, git_branch_initial, git_commit_initial, tool_version, os_info,
            permission_mode, model_initial, loaded_instructions, env_snapshot,
            total_turns, total_input_tokens, total_output_tokens,
            total_cache_read_tokens, total_cache_creation_tokens, total_hidden_tokens_est,
            distinct_model_variants,
            last_scan_offset, last_scan_at, scan_failure_count,
            scanner_version, created_at, updated_at
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6,
            ?7, ?8, ?9, ?10, ?11,
            ?12, ?13, ?14, ?15,
            ?16, ?17, ?18,
            ?19, ?20, ?21,
            ?22,
            ?23, ?24, ?25,
            ?26, ?27, ?28
        )
        "#,
        params![
            session.session_id,
            session.source,
            session.started_at,
            session.ended_at,
            session.end_reason,
            session.transcript_path,
            session.cwd_initial,
            session.git_branch_initial,
            session.git_commit_initial,
            session.tool_version,
            session.os_info,
            session.permission_mode,
            session.model_initial,
            session.loaded_instructions,
            session.env_snapshot,
            session.total_turns,
            session.total_input_tokens,
            session.total_output_tokens,
            session.total_cache_read_tokens,
            session.total_cache_creation_tokens,
            session.total_hidden_tokens_est,
            session.distinct_model_variants,
            session.last_scan_offset,
            session.last_scan_at,
            session.scan_failure_count,
            SCANNER_VERSION,
            created_at,
            updated_at,
        ],
    )
    .context("upsert_session")?;
    Ok(())
}

/// Update incremental scan progress for a session.
/// `scan_failure_count = None` leaves the counter untouched.
pub fn update_scan_offset(
    conn: &Connection,
    session_id: &str,
    offset: u64,
    scan_failure_count: Option<u64>,
) -> Result<()> {
    let now = now_iso();
    if let Some(fc) = scan_failure_count {
        conn.execute(
            "UPDATE sessions SET last_scan_offset = ?1, last_scan_at = ?2, scan_failure_count = ?3, updated_at = ?2 WHERE session_id = ?4",
            params![offset as i64, now, fc as i64, session_id],
        )
        .context("update_scan_offset (with failure count)")?;
    } else {
        conn.execute(
            "UPDATE sessions SET last_scan_offset = ?1, last_scan_at = ?2, updated_at = ?2 WHERE session_id = ?3",
            params![offset as i64, now, session_id],
        )
        .context("update_scan_offset")?;
    }
    Ok(())
}

pub fn finalize_session(
    conn: &Connection,
    session_id: &str,
    aggregates: &SessionAggregates,
) -> Result<()> {
    let now = now_iso();
    conn.execute(
        r#"
        UPDATE sessions SET
            ended_at = COALESCE(?1, ended_at),
            end_reason = COALESCE(?2, end_reason),
            total_turns = ?3,
            total_input_tokens = ?4,
            total_output_tokens = ?5,
            total_cache_read_tokens = ?6,
            total_cache_creation_tokens = ?7,
            total_hidden_tokens_est = ?8,
            distinct_model_variants = COALESCE(?9, distinct_model_variants),
            updated_at = ?10
        WHERE session_id = ?11
        "#,
        params![
            aggregates.ended_at,
            aggregates.end_reason,
            aggregates.total_turns,
            aggregates.total_input_tokens,
            aggregates.total_output_tokens,
            aggregates.total_cache_read_tokens,
            aggregates.total_cache_creation_tokens,
            aggregates.total_hidden_tokens_est,
            aggregates.distinct_model_variants,
            now,
            session_id,
        ],
    )
    .context("finalize_session")?;
    Ok(())
}

fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRow> {
    Ok(SessionRow {
        session_id: row.get("session_id")?,
        source: row.get("source")?,
        started_at: row.get("started_at")?,
        ended_at: row.get("ended_at")?,
        end_reason: row.get("end_reason")?,
        transcript_path: row.get("transcript_path")?,
        cwd_initial: row.get("cwd_initial")?,
        git_branch_initial: row.get("git_branch_initial")?,
        git_commit_initial: row.get("git_commit_initial")?,
        tool_version: row.get("tool_version")?,
        os_info: row.get("os_info")?,
        permission_mode: row.get("permission_mode")?,
        model_initial: row.get("model_initial")?,
        loaded_instructions: row.get("loaded_instructions")?,
        env_snapshot: row.get("env_snapshot")?,
        total_turns: row.get("total_turns")?,
        total_input_tokens: row.get("total_input_tokens")?,
        total_output_tokens: row.get("total_output_tokens")?,
        total_cache_read_tokens: row.get("total_cache_read_tokens")?,
        total_cache_creation_tokens: row.get("total_cache_creation_tokens")?,
        total_hidden_tokens_est: row.get("total_hidden_tokens_est")?,
        distinct_model_variants: row.get("distinct_model_variants")?,
        last_scan_offset: row.get("last_scan_offset")?,
        last_scan_at: row.get("last_scan_at")?,
        scan_failure_count: row.get("scan_failure_count")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

pub fn get_session(conn: &Connection, session_id: &str) -> Result<Option<SessionRow>> {
    let mut stmt = conn.prepare("SELECT * FROM sessions WHERE session_id = ?1")?;
    let row = stmt
        .query_row(params![session_id], row_to_session)
        .optional()
        .context("get_session")?;
    Ok(row)
}

/// Paginated list. `cursor` is the `session_id` of the last row from the
/// previous page; rows whose `started_at` is strictly less than the cursor
/// row's `started_at` are returned, ordered DESC.
pub fn list_sessions(
    conn: &Connection,
    since: Option<&str>,
    model_variant: Option<&str>,
    source: Option<&str>,
    limit: usize,
    cursor: Option<&str>,
) -> Result<Vec<SessionRow>> {
    // Look up cursor's started_at (for keyset pagination).
    let cursor_started_at: Option<String> = if let Some(cur) = cursor {
        conn.query_row(
            "SELECT started_at FROM sessions WHERE session_id = ?1",
            params![cur],
            |r| r.get(0),
        )
        .optional()
        .context("looking up cursor started_at")?
    } else {
        None
    };

    let mut sql = String::from("SELECT * FROM sessions WHERE 1=1");
    let mut args: Vec<rusqlite::types::Value> = Vec::new();
    if let Some(s) = since {
        sql.push_str(" AND started_at >= ?");
        args.push(rusqlite::types::Value::Text(s.to_string()));
    }
    if let Some(mv) = model_variant {
        sql.push_str(" AND session_id IN (SELECT session_id FROM turns WHERE model_variant = ?)");
        args.push(rusqlite::types::Value::Text(mv.to_string()));
    }
    if let Some(src) = source {
        sql.push_str(" AND source = ?");
        args.push(rusqlite::types::Value::Text(src.to_string()));
    }
    if let Some(cs) = &cursor_started_at {
        sql.push_str(" AND started_at < ?");
        args.push(rusqlite::types::Value::Text(cs.clone()));
    }
    sql.push_str(" ORDER BY started_at DESC LIMIT ?");
    args.push(rusqlite::types::Value::Integer(limit as i64));

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(args.iter()), row_to_session)?
        .map(|r| r.map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;
    use rusqlite::Connection;

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();
        conn
    }

    fn make_session(id: &str, started_at: &str) -> SessionRow {
        SessionRow {
            session_id: id.to_string(),
            source: "claude-code".to_string(),
            started_at: started_at.to_string(),
            created_at: started_at.to_string(),
            updated_at: started_at.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn upsert_session_insert_then_update() {
        let conn = fresh_db();
        let mut s = make_session("s1", "2026-04-15T10:00:00Z");
        s.cwd_initial = Some("/tmp/a".into());
        upsert_session(&conn, &s).unwrap();

        let original_created_at = get_session(&conn, "s1").unwrap().unwrap().created_at;

        // Sleep so the second upsert's "now" differs (even though we don't
        // assert on updated_at directly, this guards against timestamp ties).
        std::thread::sleep(std::time::Duration::from_millis(5));

        s.cwd_initial = Some("/tmp/b".into());
        s.created_at = "2099-01-01T00:00:00Z".into(); // should be ignored
        upsert_session(&conn, &s).unwrap();

        let got = get_session(&conn, "s1").unwrap().unwrap();
        assert_eq!(got.cwd_initial.as_deref(), Some("/tmp/b"));
        assert_eq!(got.created_at, original_created_at, "created_at preserved");
    }

    #[test]
    fn list_sessions_filters_since() {
        let conn = fresh_db();
        upsert_session(&conn, &make_session("a", "2026-04-13T00:00:00Z")).unwrap();
        upsert_session(&conn, &make_session("b", "2026-04-14T00:00:00Z")).unwrap();
        upsert_session(&conn, &make_session("c", "2026-04-15T00:00:00Z")).unwrap();

        let later =
            list_sessions(&conn, Some("2026-04-14T00:00:00Z"), None, None, 100, None).unwrap();
        let ids: Vec<_> = later.iter().map(|r| r.session_id.clone()).collect();
        assert_eq!(ids, vec!["c", "b"]);
    }

    #[test]
    fn list_sessions_pagination() {
        let conn = fresh_db();
        upsert_session(&conn, &make_session("a", "2026-04-13T00:00:00Z")).unwrap();
        upsert_session(&conn, &make_session("b", "2026-04-14T00:00:00Z")).unwrap();
        upsert_session(&conn, &make_session("c", "2026-04-15T00:00:00Z")).unwrap();

        let page1 = list_sessions(&conn, None, None, None, 2, None).unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].session_id, "c");
        assert_eq!(page1[1].session_id, "b");

        let page2 = list_sessions(&conn, None, None, None, 2, Some(&page1[1].session_id)).unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0].session_id, "a");
    }

    #[test]
    fn finalize_session_updates_aggregates() {
        let conn = fresh_db();
        upsert_session(&conn, &make_session("z", "2026-04-15T00:00:00Z")).unwrap();
        finalize_session(
            &conn,
            "z",
            &SessionAggregates {
                ended_at: Some("2026-04-15T01:00:00Z".into()),
                end_reason: Some("clear".into()),
                total_turns: 7,
                total_output_tokens: 1234,
                ..Default::default()
            },
        )
        .unwrap();
        let got = get_session(&conn, "z").unwrap().unwrap();
        assert_eq!(got.total_turns, 7);
        assert_eq!(got.total_output_tokens, 1234);
        assert_eq!(got.ended_at.as_deref(), Some("2026-04-15T01:00:00Z"));
        assert_eq!(got.end_reason.as_deref(), Some("clear"));
    }
}
