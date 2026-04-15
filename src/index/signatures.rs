//! `thinking_signatures` table CRUD.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use super::{PARSER_VERSION, SCANNER_VERSION};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SignatureRow {
    pub turn_uuid: String,
    pub block_index: i64,
    pub signature_b64: String,
    pub signature_bytes: i64,
    pub thinking_content_bytes: i64,
    pub extracted_model_variant: Option<String>,
    pub extracted_strings_json: Option<String>,
    pub scanned_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantStat {
    pub variant: String,
    pub count: u64,
    pub first_seen: String,
    pub last_seen: String,
}

pub fn upsert_signature(conn: &Connection, sig: &SignatureRow) -> Result<()> {
    // INSERT OR REPLACE on the (turn_uuid, block_index) UNIQUE constraint.
    conn.execute(
        r#"
        INSERT INTO thinking_signatures (
            turn_uuid, block_index, signature_b64, signature_bytes,
            thinking_content_bytes, extracted_model_variant, extracted_strings_json,
            parser_version, scanned_at, scanner_version
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(turn_uuid, block_index) DO UPDATE SET
            signature_b64           = excluded.signature_b64,
            signature_bytes         = excluded.signature_bytes,
            thinking_content_bytes  = excluded.thinking_content_bytes,
            extracted_model_variant = excluded.extracted_model_variant,
            extracted_strings_json  = excluded.extracted_strings_json,
            parser_version          = excluded.parser_version,
            scanned_at              = excluded.scanned_at,
            scanner_version         = excluded.scanner_version
        "#,
        params![
            sig.turn_uuid,
            sig.block_index,
            sig.signature_b64,
            sig.signature_bytes,
            sig.thinking_content_bytes,
            sig.extracted_model_variant,
            sig.extracted_strings_json,
            PARSER_VERSION,
            sig.scanned_at,
            SCANNER_VERSION,
        ],
    )
    .context("upsert_signature")?;
    Ok(())
}

pub fn list_signatures_for_turn(conn: &Connection, turn_uuid: &str) -> Result<Vec<SignatureRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT turn_uuid, block_index, signature_b64, signature_bytes,
               thinking_content_bytes, extracted_model_variant, extracted_strings_json,
               scanned_at
        FROM thinking_signatures
        WHERE turn_uuid = ?1
        ORDER BY block_index ASC
        "#,
    )?;
    let rows = stmt
        .query_map(params![turn_uuid], |r| {
            Ok(SignatureRow {
                turn_uuid: r.get(0)?,
                block_index: r.get(1)?,
                signature_b64: r.get(2)?,
                signature_bytes: r.get(3)?,
                thinking_content_bytes: r.get(4)?,
                extracted_model_variant: r.get(5)?,
                extracted_strings_json: r.get(6)?,
                scanned_at: r.get(7)?,
            })
        })?
        .map(|r| r.map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn delete_signatures_for_session(conn: &Connection, session_id: &str) -> Result<usize> {
    let n = conn
        .execute(
            r#"
            DELETE FROM thinking_signatures
            WHERE turn_uuid IN (SELECT turn_uuid FROM turns WHERE session_id = ?1)
            "#,
            params![session_id],
        )
        .context("delete_signatures_for_session")?;
    Ok(n)
}

pub fn list_distinct_variants(conn: &Connection) -> Result<Vec<VariantStat>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT extracted_model_variant,
               COUNT(*) AS c,
               MIN(scanned_at) AS first_seen,
               MAX(scanned_at) AS last_seen
        FROM thinking_signatures
        WHERE extracted_model_variant IS NOT NULL
        GROUP BY extracted_model_variant
        ORDER BY c DESC
        "#,
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(VariantStat {
                variant: r.get::<_, String>(0)?,
                count: r.get::<_, i64>(1)? as u64,
                first_seen: r.get::<_, String>(2)?,
                last_seen: r.get::<_, String>(3)?,
            })
        })?
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

    fn make_sig(turn: &str, idx: i64, variant: Option<&str>) -> SignatureRow {
        SignatureRow {
            turn_uuid: turn.into(),
            block_index: idx,
            signature_b64: "deadbeef".into(),
            signature_bytes: 4,
            thinking_content_bytes: 0,
            extracted_model_variant: variant.map(String::from),
            extracted_strings_json: Some("[]".into()),
            scanned_at: "2026-04-15T12:00:00Z".into(),
        }
    }

    #[test]
    fn upsert_signature_unique_turn_block() {
        let conn = fresh_db();
        upsert_signature(&conn, &make_sig("u1", 0, Some("numbat-v6"))).unwrap();
        // Second upsert on (u1, 0) overrides instead of duplicating.
        let mut s = make_sig("u1", 0, Some("claude-opus-4-6"));
        s.signature_b64 = "newsig".into();
        upsert_signature(&conn, &s).unwrap();

        let listed = list_signatures_for_turn(&conn, "u1").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].signature_b64, "newsig");
        assert_eq!(
            listed[0].extracted_model_variant.as_deref(),
            Some("claude-opus-4-6")
        );
    }

    #[test]
    fn list_distinct_variants_counts_correctly() {
        let conn = fresh_db();
        upsert_signature(&conn, &make_sig("a", 0, Some("numbat-v6"))).unwrap();
        upsert_signature(&conn, &make_sig("b", 0, Some("numbat-v6"))).unwrap();
        upsert_signature(&conn, &make_sig("c", 0, Some("claude-opus-4-6"))).unwrap();

        let stats = list_distinct_variants(&conn).unwrap();
        assert_eq!(stats.len(), 2);
        // numbat-v6 has 2 hits and should be first (ORDER BY count DESC).
        assert_eq!(stats[0].variant, "numbat-v6");
        assert_eq!(stats[0].count, 2);
        assert_eq!(stats[1].variant, "claude-opus-4-6");
        assert_eq!(stats[1].count, 1);
    }
}
