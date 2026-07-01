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

/// v2 (v0.7.0): per-content-block table. A single turn aggregates many blocks
/// (a Codex turn spans avg 17 / max 534 `response_item`s — measured), so a
/// single `turns.content_blob_hash` cannot represent it (AD3). Each block's
/// body, when captured, lives in the sha256 blob store; this table holds only
/// the hash reference + metadata — never raw text (privacy invariant intact).
const SQL_TURN_CONTENT: &str = r#"
CREATE TABLE IF NOT EXISTS turn_content (
    turn_uuid     TEXT NOT NULL,
    block_index   INTEGER NOT NULL,
    kind          TEXT NOT NULL,
    role          TEXT,
    content_hash  TEXT,
    is_error      INTEGER NOT NULL DEFAULT 0,
    byte_len      INTEGER NOT NULL DEFAULT 0,
    scanned_at    TEXT NOT NULL,

    PRIMARY KEY (turn_uuid, block_index),
    FOREIGN KEY (turn_uuid) REFERENCES turns(turn_uuid)
);
"#;

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
    migrate(conn)?;
    Ok(())
}

/// Current jack.db schema version. Bump when adding a migration step below.
const SCHEMA_VERSION: i64 = 2;

/// Apply forward-only, incremental migrations to an existing jack.db, tracked
/// via SQLite's `PRAGMA user_version`. Both a fresh DB (base tables just
/// created above) and a legacy DB (written by an older jack with fewer
/// columns) converge to the current shape.
///
/// Concurrency: `open_jack_db` runs on every process start, and Claude Code
/// fires parallel hooks — so several processes can hit the first-upgrade
/// window at once. The actual migration runs inside a `BEGIN IMMEDIATE`
/// transaction; combined with the 5s `busy_timeout` set in `open_jack_db`,
/// a losing writer blocks on the write lock, then re-reads `user_version`
/// inside the txn and no-ops instead of racing a duplicate `ALTER TABLE`.
fn migrate(conn: &Connection) -> Result<()> {
    let current: i64 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .context("reading user_version")?;
    if current == SCHEMA_VERSION {
        // Fast path: already current. Avoids taking a write lock on every open.
        return Ok(());
    }
    if current > SCHEMA_VERSION {
        // DB was written by a newer jack. Migrations are forward-only, so we
        // cannot downgrade it — and must not silently treat it as "migrated".
        // New columns are additive, so an older binary can still read/write the
        // rows it understands; proceed best-effort but make the mismatch loud.
        tracing::warn!(
            db_user_version = current,
            binary_schema_version = SCHEMA_VERSION,
            "jack.db schema is newer than this jack binary; running best-effort"
        );
        return Ok(());
    }

    // current < SCHEMA_VERSION: apply pending migrations atomically under an
    // IMMEDIATE write lock. A mid-batch failure rolls the whole txn back
    // (including the user_version bump), so the next open retries cleanly.
    conn.execute_batch("BEGIN IMMEDIATE")
        .context("begin migration transaction")?;
    match apply_migrations(conn) {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .context("commit migration transaction"),
        Err(e) => {
            // Best-effort rollback; surface the original error.
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Pending migration steps, run inside the `migrate` transaction. Re-reads
/// `user_version` first so a writer that blocked on the IMMEDIATE lock sees
/// the winner's result and no-ops. `user_version` is bumped to
/// `SCHEMA_VERSION` only after every pending block; because the batch is one
/// atomic transaction, a crash can never leave a half-applied schema.
fn apply_migrations(conn: &Connection) -> Result<()> {
    let current: i64 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .context("re-reading user_version inside migration txn")?;
    if current >= SCHEMA_VERSION {
        // Another process won the race and already migrated.
        return Ok(());
    }
    if current < 1 {
        // v1 — generalize the transcript index beyond Claude Code:
        //   turns.source            which agent produced the turn (e.g. "codex")
        //   turns.content_blob_hash SHA-256 of externalized turn content, set
        //                           when the capture policy stores full I/O.
        add_column_if_missing(conn, "turns", "source", "TEXT")?;
        add_column_if_missing(conn, "turns", "content_blob_hash", "TEXT")?;
    }
    if current < 2 {
        // v2 (v0.7.0) — per-block content table for multi-block turns (AD3).
        // Created via migration (not the frozen base schema) so fresh and
        // legacy DBs converge identically. IF NOT EXISTS keeps it idempotent.
        conn.execute(SQL_TURN_CONTENT, [])
            .context("v2 migration: create turn_content")?;
    }
    conn.pragma_update(None, "user_version", SCHEMA_VERSION)
        .context("bumping user_version")?;
    Ok(())
}

/// True if `table` already has a column named `column`.
fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    // `table`/`column` are internal constants, never user input — safe to
    // interpolate into the PRAGMA (which does not accept bound parameters).
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .any(|name| name == column);
    Ok(exists)
}

/// `ALTER TABLE ... ADD COLUMN`, guarded so re-running never errors on an
/// already-present column (belt-and-suspenders alongside `user_version`).
fn add_column_if_missing(conn: &Connection, table: &str, column: &str, decl: &str) -> Result<()> {
    if !column_exists(conn, table, column)? {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"),
            [],
        )
        .with_context(|| format!("adding column {table}.{column}"))?;
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
        assert!(names.contains(&"turn_content".to_string()));

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
    fn migrate_adds_v1_columns_and_sets_version() {
        let conn = fresh_conn();
        init(&conn).unwrap();
        assert!(column_exists(&conn, "turns", "source").unwrap());
        assert!(column_exists(&conn, "turns", "content_blob_hash").unwrap());
        let v: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
    }

    #[test]
    fn migrate_upgrades_legacy_turns_table() {
        // Simulate a jack.db written by an older jack: base tables only,
        // user_version still 0, missing the v1 columns.
        let conn = fresh_conn();
        conn.pragma_update(None, "foreign_keys", "OFF").unwrap();
        conn.execute(SQL_SESSIONS, []).unwrap();
        conn.execute(SQL_TURNS, []).unwrap();
        assert!(!column_exists(&conn, "turns", "source").unwrap());
        migrate(&conn).unwrap();
        assert!(column_exists(&conn, "turns", "source").unwrap());
        assert!(column_exists(&conn, "turns", "content_blob_hash").unwrap());
    }

    fn table_exists(conn: &Connection, table: &str) -> bool {
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |r| r.get(0),
            )
            .unwrap();
        n > 0
    }

    #[test]
    fn migrate_v1_db_gains_turn_content_at_v2() {
        // Simulate a jack.db already at v1 (has source/content_blob_hash but
        // no turn_content table). Migrating must add turn_content and reach v2.
        let conn = fresh_conn();
        conn.pragma_update(None, "foreign_keys", "OFF").unwrap();
        conn.execute(SQL_SESSIONS, []).unwrap();
        conn.execute(SQL_TURNS, []).unwrap();
        add_column_if_missing(&conn, "turns", "source", "TEXT").unwrap();
        add_column_if_missing(&conn, "turns", "content_blob_hash", "TEXT").unwrap();
        conn.pragma_update(None, "user_version", 1i64).unwrap();
        assert!(!table_exists(&conn, "turn_content"));

        migrate(&conn).unwrap();

        assert!(table_exists(&conn, "turn_content"));
        let v: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(v, 2);
    }

    #[test]
    fn turn_content_table_has_expected_columns() {
        let conn = fresh_conn();
        init(&conn).unwrap();
        for col in [
            "turn_uuid",
            "block_index",
            "kind",
            "role",
            "content_hash",
            "is_error",
            "byte_len",
            "scanned_at",
        ] {
            assert!(
                column_exists(&conn, "turn_content", col).unwrap(),
                "turn_content missing column {col}"
            );
        }
    }

    #[test]
    fn migrate_leaves_future_schema_version_intact() {
        // A DB written by a NEWER jack (user_version > SCHEMA_VERSION) must not
        // be errored on or silently "downgraded" — migrate() warns and no-ops,
        // leaving the version untouched so the newer schema is preserved.
        let conn = fresh_conn();
        init(&conn).unwrap();
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        migrate(&conn).unwrap();
        let v: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION + 1);
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
