//! Jack-side SQLite index for transcript-derived data.
//!
//! This module owns `~/.punkgo/state/jack/jack.db` — a database that is
//! **physically separate** from the kernel's `punkgo.db`. The kernel never
//! reads or writes this file. If the index is corrupted or out of date,
//! `punkgo-jack reindex --full` rebuilds it from the source-of-truth jsonl
//! transcripts under `~/.claude/projects/`.
//!
//! # Privacy invariant
//!
//! No table in this database stores raw prompt / response / thinking body
//! text. Only metadata (uuids, timestamps, byte lengths, hashes, model
//! identifiers, usage tokens, opaque thinking signatures). Bodies live in
//! the kernel blob store (sha256-addressed) and are referenced by hash.
//!
//! # Schema versioning
//!
//! Tables carry `scanner_version` / `parser_version` columns so future
//! parser upgrades can be diffed against historical rows and selectively
//! re-scanned by bumping the constants below.

use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

pub mod schema;
pub mod sessions;
pub mod signatures;
pub mod turns;

/// Stamped on every row written by the Lane D scanner. Bump when the
/// scanner emits a materially different shape.
pub const SCANNER_VERSION: &str = "jack-0.6.0-scanner-1";

/// Stamped on every `thinking_signatures` row. Bump when the signature
/// parser changes its extraction logic.
pub const PARSER_VERSION: &str = "jack-0.6.0-parser-1";

/// Resolve the on-disk path of the jack index database. Honors
/// `PUNKGO_DATA_DIR` the same way `crate::session::data_dir()` does, so
/// tests can isolate writes to a tempdir by setting that env var.
pub fn jack_db_path() -> Result<PathBuf> {
    let data_dir = crate::session::data_dir().context("resolving punkgo data dir")?;
    let dir = data_dir.join("state").join("jack");
    if !dir.exists() {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
    }
    Ok(dir.join("jack.db"))
}

/// Open (or create) the jack index database with the conventions the rest
/// of the indexer relies on:
///
/// - WAL journal (concurrent reads while indexer writes)
/// - `synchronous = NORMAL` (FULL is overkill for a derived index)
/// - 5-second busy timeout (avoid SQLITE_BUSY on brief contention)
/// - Foreign keys **off** by design (eng review decision 8 — orphan rows
///   are legitimate; FKs are documentation only)
///
/// Schema is initialized via `schema::init` on every open so a freshly
/// rolled-back DB self-heals on the next call.
pub fn open_jack_db() -> Result<Connection> {
    let path = jack_db_path()?;
    let conn = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )
    .with_context(|| format!("failed to open jack db at {}", path.display()))?;

    // Pragmas first — these affect every subsequent statement.
    // `journal_mode = WAL` returns a row; use query_row.
    let _: String = conn
        .query_row("PRAGMA journal_mode = WAL;", [], |r| r.get(0))
        .context("setting WAL journal mode")?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("setting synchronous = NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "OFF")
        .context("setting foreign_keys = OFF")?;
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .context("setting busy_timeout")?;

    schema::init(&conn).context("initializing jack.db schema")?;

    Ok(conn)
}

/// ISO 8601 UTC timestamp helper, matches the kernel convention.
pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}
