//! `CREATE TABLE` strings + idempotent `init` for jack.db.
//!
//! Schema verbatim from `docs/transcript-archaeologist-plan.md` §3.1 and
//! §4.7 (pending_scans queue). Foreign keys are declared as documentation
//! but not enforced (decision 8).

use anyhow::{Context, Result};
use rusqlite::Connection;

const SQL_SESSIONS: &str = r#"
CREATE TABLE IF NOT EXISTS sessions (
    session_id           TEXT PRIMARY KEY,
    source               TEXT NOT NULL,
    started_at           TEXT NOT NULL,
    ended_at             TEXT,
    end_reason           TEXT,
    transcript_path      TEXT,

    cwd_initial          TEXT,
    git_branch_initial   TEXT,
    git_commit_initial   TEXT,
    tool_version         TEXT,
    os_info              TEXT,
    permission_mode      TEXT,
    model_initial        TEXT,

    loaded_instructions  TEXT,
    env_snapshot         TEXT,

    total_turns               INTEGER DEFAULT 0,
    total_input_tokens        INTEGER DEFAULT 0,
    total_output_tokens       INTEGER DEFAULT 0,
    total_cache_read_tokens   INTEGER DEFAULT 0,
    total_cache_creation_tokens INTEGER DEFAULT 0,
    total_hidden_tokens_est   INTEGER DEFAULT 0,
    distinct_model_variants   TEXT,

    last_scan_offset     INTEGER NOT NULL DEFAULT 0,
    last_scan_at         TEXT,
    scan_failure_count   INTEGER NOT NULL DEFAULT 0,

    scanner_version      TEXT NOT NULL,

    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);
"#;

const SQL_SESSIONS_IDX_STARTED: &str =
    "CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at DESC);";
const SQL_SESSIONS_IDX_END_REASON: &str =
    "CREATE INDEX IF NOT EXISTS idx_sessions_end_reason ON sessions(end_reason);";

const SQL_TURNS: &str = r#"
CREATE TABLE IF NOT EXISTS turns (
    turn_uuid                 TEXT PRIMARY KEY,
    session_id                TEXT NOT NULL,
    parent_turn_uuid          TEXT,
    turn_order                INTEGER NOT NULL,
    role                      TEXT NOT NULL,
    timestamp                 TEXT NOT NULL,

    cwd                       TEXT,
    git_branch                TEXT,
    is_sidechain              INTEGER NOT NULL DEFAULT 0,
    slug                      TEXT,
    claude_code_version       TEXT,

    request_id                TEXT,
    message_id                TEXT,
    model                     TEXT,
    model_variant             TEXT,

    input_tokens              INTEGER,
    output_tokens             INTEGER,
    cache_read_tokens         INTEGER,
    cache_creation_tokens     INTEGER,
    ephemeral_5m_tokens       INTEGER,
    ephemeral_1h_tokens       INTEGER,
    service_tier              TEXT,
    stop_reason               TEXT,

    content_blocks_meta       TEXT NOT NULL,
    visible_text_bytes        INTEGER NOT NULL DEFAULT 0,
    visible_tool_use_bytes    INTEGER NOT NULL DEFAULT 0,
    thinking_block_count      INTEGER NOT NULL DEFAULT 0,
    estimated_hidden_tokens   INTEGER NOT NULL DEFAULT 0,

    kernel_event_id           TEXT,

    scanned_at                TEXT NOT NULL,
    scanner_version           TEXT NOT NULL,

    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
"#;

const SQL_TURNS_IDX_SESSION_ORDER: &str =
    "CREATE INDEX IF NOT EXISTS idx_turns_session_order ON turns(session_id, turn_order);";
const SQL_TURNS_IDX_PARENT: &str =
    "CREATE INDEX IF NOT EXISTS idx_turns_parent ON turns(parent_turn_uuid);";
const SQL_TURNS_IDX_MODEL_VARIANT: &str =
    "CREATE INDEX IF NOT EXISTS idx_turns_model_variant ON turns(model_variant);";
const SQL_TURNS_IDX_REQUEST_ID: &str =
    "CREATE INDEX IF NOT EXISTS idx_turns_request_id ON turns(request_id);";
const SQL_TURNS_IDX_KERNEL_EVENT: &str =
    "CREATE INDEX IF NOT EXISTS idx_turns_kernel_event ON turns(kernel_event_id);";

const SQL_SIGNATURES: &str = r#"
CREATE TABLE IF NOT EXISTS thinking_signatures (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    turn_uuid                   TEXT NOT NULL,
    block_index                 INTEGER NOT NULL,
    signature_b64               TEXT NOT NULL,
    signature_bytes             INTEGER NOT NULL,
    thinking_content_bytes      INTEGER NOT NULL DEFAULT 0,

    extracted_model_variant     TEXT,
    extracted_strings_json      TEXT,
    parser_version              TEXT NOT NULL,

    scanned_at                  TEXT NOT NULL,
    scanner_version             TEXT NOT NULL,

    UNIQUE (turn_uuid, block_index),
    FOREIGN KEY (turn_uuid) REFERENCES turns(turn_uuid)
);
"#;

const SQL_SIGNATURES_IDX_TURN: &str =
    "CREATE INDEX IF NOT EXISTS idx_sig_turn ON thinking_signatures(turn_uuid);";
const SQL_SIGNATURES_IDX_VARIANT: &str =
    "CREATE INDEX IF NOT EXISTS idx_sig_variant ON thinking_signatures(extracted_model_variant);";

const SQL_PENDING_SCANS: &str = r#"
CREATE TABLE IF NOT EXISTS pending_scans (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    transcript_path TEXT,
    enqueued_at     TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    last_attempt    TEXT
);
"#;

const SQL_PENDING_SCANS_IDX: &str =
    "CREATE INDEX IF NOT EXISTS idx_pending_session ON pending_scans(session_id);";

/// Run every CREATE statement. Idempotent: safe to call on every open.
///
/// Also explicitly disables foreign-key enforcement for this connection
/// (eng review decision 8). The kernel default in rusqlite is ON, so a
/// fresh `Connection::open_in_memory()` would otherwise reject orphan rows
/// — which we treat as legitimate data.
pub fn init(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "foreign_keys", "OFF")
        .context("init: setting foreign_keys = OFF")?;
    let stmts: &[&str] = &[
        SQL_SESSIONS,
        SQL_SESSIONS_IDX_STARTED,
        SQL_SESSIONS_IDX_END_REASON,
        SQL_TURNS,
        SQL_TURNS_IDX_SESSION_ORDER,
        SQL_TURNS_IDX_PARENT,
        SQL_TURNS_IDX_MODEL_VARIANT,
        SQL_TURNS_IDX_REQUEST_ID,
        SQL_TURNS_IDX_KERNEL_EVENT,
        SQL_SIGNATURES,
        SQL_SIGNATURES_IDX_TURN,
        SQL_SIGNATURES_IDX_VARIANT,
        SQL_PENDING_SCANS,
        SQL_PENDING_SCANS_IDX,
    ];
    for sql in stmts {
        conn.execute(sql, []).with_context(|| {
            format!(
                "executing schema stmt: {}",
                sql.lines().next().unwrap_or("")
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn fresh_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        // In-memory DBs need the same pragmas the on-disk path applies.
        let _: String = conn
            .query_row("PRAGMA journal_mode = WAL;", [], |r| r.get(0))
            .unwrap_or_default();
        conn
    }

    #[test]
    fn init_creates_all_tables() {
        let conn = fresh_conn();
        init(&conn).unwrap();
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .filter(|n| !n.starts_with("sqlite_"))
            .collect();
        assert!(names.contains(&"sessions".to_string()));
        assert!(names.contains(&"turns".to_string()));
        assert!(names.contains(&"thinking_signatures".to_string()));
        assert!(names.contains(&"pending_scans".to_string()));

        // Verify a few indexes exist.
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index'")
            .unwrap();
        let idx_names: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(idx_names.iter().any(|n| n == "idx_sessions_started"));
        assert!(idx_names.iter().any(|n| n == "idx_turns_session_order"));
        assert!(idx_names.iter().any(|n| n == "idx_turns_kernel_event"));
        assert!(idx_names.iter().any(|n| n == "idx_sig_turn"));
        assert!(idx_names.iter().any(|n| n == "idx_pending_session"));
    }

    #[test]
    fn init_is_idempotent() {
        let conn = fresh_conn();
        init(&conn).unwrap();
        init(&conn).unwrap();
        init(&conn).unwrap();
    }

    #[test]
    fn pragma_journal_mode_is_wal_on_disk() {
        // The in-memory PRAGMA journal_mode = WAL silently degrades to "memory"
        // on some sqlite builds, so verify on the real on-disk open path
        // instead. Use PUNKGO_DATA_DIR override for isolation.
        let _guard = crate::session::PUNKGO_DATA_DIR_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::TempDir::new().unwrap();
        let prev = std::env::var_os("PUNKGO_DATA_DIR");
        std::env::set_var("PUNKGO_DATA_DIR", tmp.path());

        let conn = crate::index::open_jack_db().unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode;", [], |r| r.get(0))
            .unwrap();
        // Restore env before assertion.
        if let Some(v) = prev {
            std::env::set_var("PUNKGO_DATA_DIR", v);
        } else {
            std::env::remove_var("PUNKGO_DATA_DIR");
        }
        assert_eq!(mode.to_lowercase(), "wal");
    }
}
