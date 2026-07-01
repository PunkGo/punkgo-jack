//! `turn_content` table CRUD (v2 schema, v0.7.0).
//!
//! One row per content block of a turn. A Codex turn aggregates many blocks
//! (avg 17, max 534 measured), so per-block rows are required — a single
//! `turns.content_blob_hash` cannot represent a turn (AD3).
//!
//! **Privacy**: this table stores only a `content_hash` (a `sha256:<hex>` blob
//! reference) plus block metadata (kind, role, byte length, error flag). The
//! raw body lives in the content-addressed blob store, never in this table.
//! `content_hash` is `None` for blocks that are opaque (encrypted reasoning) or
//! whose source is under a metadata-only capture policy.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

/// One content block belonging to a turn.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TurnContentRow {
    pub turn_uuid: String,
    /// 0-based position of this block within its turn.
    pub block_index: i64,
    /// Block kind, source-neutral: `input_text` | `output_text` | `reasoning`
    /// | `tool_call` | `tool_result` | `image` | ... .
    pub kind: String,
    /// `user` | `assistant` | `developer` | `tool`, when applicable.
    pub role: Option<String>,
    /// `sha256:<hex>` blob reference, or `None` when the block body is opaque
    /// (encrypted reasoning) or not captured under the active policy.
    pub content_hash: Option<String>,
    pub is_error: bool,
    /// Byte length of the (pre-hash) block body.
    pub byte_len: i64,
    pub scanned_at: String,
}

/// Insert or replace a content block, keyed on `(turn_uuid, block_index)`.
/// Idempotent across re-scans of the same turn.
pub fn upsert_turn_content(conn: &Connection, row: &TurnContentRow) -> Result<()> {
    conn.execute(
        r#"
        INSERT OR REPLACE INTO turn_content (
            turn_uuid, block_index, kind, role, content_hash,
            is_error, byte_len, scanned_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#,
        params![
            row.turn_uuid,
            row.block_index,
            row.kind,
            row.role,
            row.content_hash,
            row.is_error as i64,
            row.byte_len,
            row.scanned_at,
        ],
    )
    .context("upsert_turn_content")?;
    Ok(())
}

fn row_to_content(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnContentRow> {
    Ok(TurnContentRow {
        turn_uuid: row.get("turn_uuid")?,
        block_index: row.get("block_index")?,
        kind: row.get("kind")?,
        role: row.get("role")?,
        content_hash: row.get("content_hash")?,
        is_error: row.get::<_, i64>("is_error")? != 0,
        byte_len: row.get("byte_len")?,
        scanned_at: row.get("scanned_at")?,
    })
}

/// List a turn's content blocks in `block_index` order.
pub fn list_content_for_turn(conn: &Connection, turn_uuid: &str) -> Result<Vec<TurnContentRow>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM turn_content WHERE turn_uuid = ?1 ORDER BY block_index ASC",
    )?;
    let rows = stmt
        .query_map(params![turn_uuid], row_to_content)?
        .map(|r| r.map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// Delete all content blocks for a turn. CRUD-complete; the reindex path uses
/// `delete_content_for_session`, so this is currently exercised only by tests
/// and reserved for a future single-turn purge (e.g. an MCP delete).
#[allow(dead_code)]
pub fn delete_content_for_turn(conn: &Connection, turn_uuid: &str) -> Result<usize> {
    let n = conn
        .execute(
            "DELETE FROM turn_content WHERE turn_uuid = ?1",
            params![turn_uuid],
        )
        .context("delete_content_for_turn")?;
    Ok(n)
}

/// Delete all content blocks belonging to a session's turns. Foreign keys are
/// not enforced (index/mod decision 8), so the reindex per-session DELETE must
/// clean `turn_content` explicitly via a join on `turns`.
pub fn delete_content_for_session(conn: &Connection, session_id: &str) -> Result<usize> {
    let n = conn
        .execute(
            "DELETE FROM turn_content WHERE turn_uuid IN \
             (SELECT turn_uuid FROM turns WHERE session_id = ?1)",
            params![session_id],
        )
        .context("delete_content_for_session")?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;
    use crate::index::turns::{upsert_turn, TurnRow};

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::init(&conn).unwrap();
        conn
    }

    fn block(turn: &str, idx: i64, kind: &str) -> TurnContentRow {
        TurnContentRow {
            turn_uuid: turn.into(),
            block_index: idx,
            kind: kind.into(),
            role: Some("assistant".into()),
            content_hash: Some(format!("sha256:{kind}{idx}")),
            is_error: false,
            byte_len: 100 + idx,
            scanned_at: "2026-07-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn upsert_and_list_ordered() {
        let conn = fresh_db();
        upsert_turn_content(&conn, &block("t1", 2, "output_text")).unwrap();
        upsert_turn_content(&conn, &block("t1", 0, "input_text")).unwrap();
        upsert_turn_content(&conn, &block("t1", 1, "reasoning")).unwrap();

        let listed = list_content_for_turn(&conn, "t1").unwrap();
        let kinds: Vec<_> = listed.iter().map(|b| b.kind.clone()).collect();
        assert_eq!(kinds, vec!["input_text", "reasoning", "output_text"]);
        assert_eq!(listed[0].content_hash.as_deref(), Some("sha256:input_text0"));
    }

    #[test]
    fn upsert_is_idempotent_on_pk() {
        let conn = fresh_db();
        upsert_turn_content(&conn, &block("t1", 0, "input_text")).unwrap();
        // Re-scan: same PK, different byte_len — REPLACE, not duplicate.
        let mut b = block("t1", 0, "input_text");
        b.byte_len = 999;
        upsert_turn_content(&conn, &b).unwrap();
        let listed = list_content_for_turn(&conn, "t1").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].byte_len, 999);
    }

    #[test]
    fn delete_for_turn_and_session() {
        let conn = fresh_db();
        // Two turns in one session; content under each.
        let mk_turn = |uuid: &str| TurnRow {
            turn_uuid: uuid.into(),
            session_id: "s1".into(),
            role: "assistant".into(),
            timestamp: "2026-07-01T00:00:00Z".into(),
            content_blocks_meta: "[]".into(),
            scanned_at: "2026-07-01T00:00:00Z".into(),
            source: Some("codex".into()),
            ..Default::default()
        };
        upsert_turn(&conn, &mk_turn("t1")).unwrap();
        upsert_turn(&conn, &mk_turn("t2")).unwrap();
        upsert_turn_content(&conn, &block("t1", 0, "input_text")).unwrap();
        upsert_turn_content(&conn, &block("t1", 1, "output_text")).unwrap();
        upsert_turn_content(&conn, &block("t2", 0, "input_text")).unwrap();

        // Delete one turn's content.
        assert_eq!(delete_content_for_turn(&conn, "t1").unwrap(), 2);
        assert!(list_content_for_turn(&conn, "t1").unwrap().is_empty());
        assert_eq!(list_content_for_turn(&conn, "t2").unwrap().len(), 1);

        // Session-wide delete via join on turns.
        upsert_turn_content(&conn, &block("t1", 0, "input_text")).unwrap();
        assert_eq!(delete_content_for_session(&conn, "s1").unwrap(), 2);
        assert!(list_content_for_turn(&conn, "t1").unwrap().is_empty());
        assert!(list_content_for_turn(&conn, "t2").unwrap().is_empty());
    }
}
