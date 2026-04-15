//! `turns` table CRUD.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use super::SCANNER_VERSION;

/// One row in the `turns` table. **Privacy**: every field here is metadata
/// only — no body text, no tool input/output, no thinking content. The
/// `content_blocks_meta` field is a JSON array built by the indexer with
/// only `idx`, `kind`, `byte_len`, `content_hash`, `signature_present`
/// fields per block.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TurnRow {
    pub turn_uuid: String,
    pub session_id: String,
    pub parent_turn_uuid: Option<String>,
    pub turn_order: i64,
    pub role: String,
    pub timestamp: String,

    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub is_sidechain: bool,
    pub slug: Option<String>,
    pub claude_code_version: Option<String>,

    pub request_id: Option<String>,
    pub message_id: Option<String>,
    pub model: Option<String>,
    pub model_variant: Option<String>,

    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_creation_tokens: Option<i64>,
    pub ephemeral_5m_tokens: Option<i64>,
    pub ephemeral_1h_tokens: Option<i64>,
    pub service_tier: Option<String>,
    pub stop_reason: Option<String>,

    /// JSON array string. Privacy contract enforced by the indexer:
    /// elements only carry `{idx, kind, byte_len, content_hash,
    /// signature_present}`. NEVER raw text.
    pub content_blocks_meta: String,
    pub visible_text_bytes: i64,
    pub visible_tool_use_bytes: i64,
    pub thinking_block_count: i64,
    pub estimated_hidden_tokens: i64,

    pub kernel_event_id: Option<String>,

    pub scanned_at: String,
}

/// PRIVACY: metadata only, no body text. The caller is responsible for
/// constructing `content_blocks_meta` without smuggling raw content.
pub fn upsert_turn(conn: &Connection, turn: &TurnRow) -> Result<()> {
    conn.execute(
        r#"
        INSERT OR REPLACE INTO turns (
            turn_uuid, session_id, parent_turn_uuid, turn_order, role, timestamp,
            cwd, git_branch, is_sidechain, slug, claude_code_version,
            request_id, message_id, model, model_variant,
            input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
            ephemeral_5m_tokens, ephemeral_1h_tokens, service_tier, stop_reason,
            content_blocks_meta, visible_text_bytes, visible_tool_use_bytes,
            thinking_block_count, estimated_hidden_tokens,
            kernel_event_id, scanned_at, scanner_version
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6,
            ?7, ?8, ?9, ?10, ?11,
            ?12, ?13, ?14, ?15,
            ?16, ?17, ?18, ?19,
            ?20, ?21, ?22, ?23,
            ?24, ?25, ?26,
            ?27, ?28,
            ?29, ?30, ?31
        )
        "#,
        params![
            turn.turn_uuid,
            turn.session_id,
            turn.parent_turn_uuid,
            turn.turn_order,
            turn.role,
            turn.timestamp,
            turn.cwd,
            turn.git_branch,
            turn.is_sidechain as i64,
            turn.slug,
            turn.claude_code_version,
            turn.request_id,
            turn.message_id,
            turn.model,
            turn.model_variant,
            turn.input_tokens,
            turn.output_tokens,
            turn.cache_read_tokens,
            turn.cache_creation_tokens,
            turn.ephemeral_5m_tokens,
            turn.ephemeral_1h_tokens,
            turn.service_tier,
            turn.stop_reason,
            turn.content_blocks_meta,
            turn.visible_text_bytes,
            turn.visible_tool_use_bytes,
            turn.thinking_block_count,
            turn.estimated_hidden_tokens,
            turn.kernel_event_id,
            turn.scanned_at,
            SCANNER_VERSION,
        ],
    )
    .context("upsert_turn")?;
    Ok(())
}

fn row_to_turn(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnRow> {
    Ok(TurnRow {
        turn_uuid: row.get("turn_uuid")?,
        session_id: row.get("session_id")?,
        parent_turn_uuid: row.get("parent_turn_uuid")?,
        turn_order: row.get("turn_order")?,
        role: row.get("role")?,
        timestamp: row.get("timestamp")?,
        cwd: row.get("cwd")?,
        git_branch: row.get("git_branch")?,
        is_sidechain: row.get::<_, i64>("is_sidechain")? != 0,
        slug: row.get("slug")?,
        claude_code_version: row.get("claude_code_version")?,
        request_id: row.get("request_id")?,
        message_id: row.get("message_id")?,
        model: row.get("model")?,
        model_variant: row.get("model_variant")?,
        input_tokens: row.get("input_tokens")?,
        output_tokens: row.get("output_tokens")?,
        cache_read_tokens: row.get("cache_read_tokens")?,
        cache_creation_tokens: row.get("cache_creation_tokens")?,
        ephemeral_5m_tokens: row.get("ephemeral_5m_tokens")?,
        ephemeral_1h_tokens: row.get("ephemeral_1h_tokens")?,
        service_tier: row.get("service_tier")?,
        stop_reason: row.get("stop_reason")?,
        content_blocks_meta: row.get("content_blocks_meta")?,
        visible_text_bytes: row.get("visible_text_bytes")?,
        visible_tool_use_bytes: row.get("visible_tool_use_bytes")?,
        thinking_block_count: row.get("thinking_block_count")?,
        estimated_hidden_tokens: row.get("estimated_hidden_tokens")?,
        kernel_event_id: row.get("kernel_event_id")?,
        scanned_at: row.get("scanned_at")?,
    })
}

pub fn get_turn(conn: &Connection, turn_uuid: &str) -> Result<Option<TurnRow>> {
    let mut stmt = conn.prepare("SELECT * FROM turns WHERE turn_uuid = ?1")?;
    let row = stmt
        .query_row(params![turn_uuid], row_to_turn)
        .optional()
        .context("get_turn")?;
    Ok(row)
}

/// List turns for a session, ordered by turn_order ASC. Bounded by
/// `limit` + `offset` to protect against memory blow-up on long sessions.
///
/// P2 review fix (2026-04-15): the previous unbounded variant was a DoS
/// surface — a MCP caller invoking `punkgo_session_detail` on a long
/// session could force the handler to materialize every turn in memory.
/// Callers that truly want all turns must loop with advancing offset.
pub fn list_turns_for_session(
    conn: &Connection,
    session_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<TurnRow>> {
    let mut stmt = conn.prepare(
        "SELECT * FROM turns WHERE session_id = ?1 \
         ORDER BY turn_order ASC LIMIT ?2 OFFSET ?3",
    )?;
    let rows = stmt
        .query_map(
            params![session_id, limit as i64, offset as i64],
            row_to_turn,
        )?
        .map(|r| r.map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn delete_turns_for_session(conn: &Connection, session_id: &str) -> Result<usize> {
    let n = conn
        .execute(
            "DELETE FROM turns WHERE session_id = ?1",
            params![session_id],
        )
        .context("delete_turns_for_session")?;
    Ok(n)
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

    fn make_turn(uuid: &str, session: &str, order: i64) -> TurnRow {
        TurnRow {
            turn_uuid: uuid.into(),
            session_id: session.into(),
            turn_order: order,
            role: "assistant".into(),
            timestamp: "2026-04-15T12:00:00Z".into(),
            content_blocks_meta: r#"[{"idx":0,"kind":"text","byte_len":42}]"#.into(),
            scanned_at: "2026-04-15T12:00:00Z".into(),
            ..Default::default()
        }
    }

    #[test]
    fn upsert_turn_preserves_content_blocks_meta() {
        let conn = fresh_db();
        let mut t = make_turn("u1", "s1", 0);
        // Use a JSON shape that could trip a naive serializer.
        t.content_blocks_meta = r#"[{"idx":0,"kind":"text","byte_len":1024,"content_hash":"abc","signature_present":false}]"#.into();
        upsert_turn(&conn, &t).unwrap();
        let got = get_turn(&conn, "u1").unwrap().unwrap();
        assert_eq!(got.content_blocks_meta, t.content_blocks_meta);
    }

    #[test]
    fn list_turns_for_session_ordered_by_turn_order() {
        let conn = fresh_db();
        // Insert out of order.
        upsert_turn(&conn, &make_turn("c", "s", 2)).unwrap();
        upsert_turn(&conn, &make_turn("a", "s", 0)).unwrap();
        upsert_turn(&conn, &make_turn("b", "s", 1)).unwrap();

        let listed = list_turns_for_session(&conn, "s", 100, 0).unwrap();
        let order: Vec<_> = listed.iter().map(|t| t.turn_uuid.clone()).collect();
        assert_eq!(order, vec!["a", "b", "c"]);

        // Verify limit + offset slicing.
        let page1 = list_turns_for_session(&conn, "s", 2, 0).unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].turn_uuid, "a");
        let page2 = list_turns_for_session(&conn, "s", 2, 2).unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0].turn_uuid, "c");
    }

    // NOTE (2026-04-15 fake-test review): the previous
    // `upsert_turn_no_text_leak` test lived here and was fake: it built
    // a pre-sanitized `content_blocks_meta` JSON in the test body,
    // passed it to `upsert_turn`, and asserted SQLite did not invent
    // the needle. Of course SQLite did not invent it — the needle was
    // never passed to the DB in the first place. The real privacy
    // invariant is "raw jsonl body text never reaches the DB via the
    // full scanner → indexer → upsert pipeline". That invariant lives
    // in `indexer::tests::pipeline_no_text_leak_from_user_prompt`
    // (and the dogfood test) which drive raw jsonl lines through
    // `run_reindex` end-to-end. This test module only covers the
    // per-function CRUD plumbing of `upsert_turn` itself; it does not
    // and cannot prove the pipeline invariant.

    #[test]
    fn delete_turns_for_session_clears_rows() {
        let conn = fresh_db();
        upsert_turn(&conn, &make_turn("a", "s", 0)).unwrap();
        upsert_turn(&conn, &make_turn("b", "s", 1)).unwrap();
        let n = delete_turns_for_session(&conn, "s").unwrap();
        assert_eq!(n, 2);
        assert!(list_turns_for_session(&conn, "s", 100, 0)
            .unwrap()
            .is_empty());
    }
}
