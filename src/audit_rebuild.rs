//! Rebuild the Merkle audit tree from events.
//!
//! After merging events from multiple databases, the audit_hashes and
//! audit_checkpoints tables are invalid. This module re-derives the complete
//! tlog Merkle tree from the event_hash column, producing bit-identical output
//! to what the kernel would have generated via `AuditLog::append_leaf`.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tracing::info;

/// CLI args for `punkgo-jack rebuild-audit`.
pub struct RebuildArgs {
    pub db_path: Option<String>,
}

pub fn parse_args(args: &mut impl Iterator<Item = String>) -> Result<RebuildArgs> {
    let db_path = args.next();
    Ok(RebuildArgs { db_path })
}

pub fn run_rebuild(args: RebuildArgs) -> Result<()> {
    let db_path = match args.db_path {
        Some(p) => PathBuf::from(p),
        None => {
            let data_dir = crate::session::data_dir()?;
            data_dir.join("state").join("punkgo.db")
        }
    };

    eprintln!("rebuilding audit tree from {}", db_path.display());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    runtime.block_on(rebuild_async(&db_path))
}

async fn rebuild_async(db_path: &std::path::Path) -> Result<()> {
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use sqlx::Row;

    let connect_options = SqliteConnectOptions::new()
        .filename(db_path)
        .journal_mode(SqliteJournalMode::Wal);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .context("failed to open database")?;

    // Clear existing audit data.
    sqlx::query("DELETE FROM audit_hashes")
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM audit_checkpoints")
        .execute(&pool)
        .await?;

    // Read all events ordered by log_index.
    let rows = sqlx::query("SELECT log_index, event_hash FROM events ORDER BY log_index ASC")
        .fetch_all(&pool)
        .await?;

    let total = rows.len();
    eprintln!("{total} events to process");

    // In-memory hash store (same as kernel's InMemHashReader).
    let mut hashes: HashMap<u64, [u8; 32]> = HashMap::new();

    for (i, row) in rows.iter().enumerate() {
        let log_index: i64 = row.get("log_index");
        let event_hash: String = row.get("event_hash");
        let n = log_index as u64;

        let leaf = hex_to_bytes(&event_hash)?;

        // Store leaf at level 0.
        let leaf_idx = stored_hash_index(0, n);
        hashes.insert(leaf_idx, leaf);

        // Walk up the tree: compute parent nodes.
        let mut current = leaf;
        let mut level: u8 = 0;

        while (n >> level) & 1 == 1 {
            let n_at_level = n >> level;
            let left_idx = stored_hash_index(level, n_at_level - 1);
            let left = *hashes
                .get(&left_idx)
                .with_context(|| format!("missing left sibling at idx {left_idx}"))?;
            let parent = node_hash(&left, &current);
            level += 1;
            let parent_idx = stored_hash_index(level, n_at_level >> 1);
            hashes.insert(parent_idx, parent);
            current = parent;
        }

        // Progress every 1000 events.
        if (i + 1) % 1000 == 0 || i + 1 == total {
            eprint!("\r  processed {}/{total}", i + 1);
        }
    }
    eprintln!();

    // Bulk insert all hashes.
    eprintln!("writing {} hash nodes...", hashes.len());
    let mut tx = pool.begin().await?;
    for (idx, hash) in &hashes {
        sqlx::query("INSERT INTO audit_hashes (hash_index, hash) VALUES (?1, ?2)")
            .bind(*idx as i64)
            .bind(hash.as_slice())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    // Create final checkpoint.
    let tree_size = total as u64;
    if tree_size > 0 {
        let root = tree_hash(tree_size, &hashes)?;
        let root_hex = bytes_to_hex(&root);

        // C2SP checkpoint format.
        let cp_text = format!("punkgo/kernel\n{tree_size}\n{root_hex}\n");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string();

        sqlx::query(
            "INSERT INTO audit_checkpoints (tree_size, root_hash, checkpoint_text, created_at)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(tree_size as i64)
        .bind(&root_hex)
        .bind(&cp_text)
        .bind(&now)
        .execute(&pool)
        .await?;

        eprintln!(
            "checkpoint: tree_size={tree_size}, root={}",
            &root_hex[..16]
        );
    }

    info!(
        events = total,
        hashes = hashes.len(),
        "audit rebuild complete"
    );
    eprintln!("done. Merkle tree rebuilt for {total} events.");
    Ok(())
}

// ---------------------------------------------------------------------------
// tlog algorithm (matching tlog_tiles crate exactly)
// ---------------------------------------------------------------------------

/// SHA-256(0x01 || left || right) — RFC 6962 internal node hash.
fn node_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([0x01]);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Maps (level, n) to a dense linear storage index.
/// Identical to `tlog_tiles::tlog::stored_hash_index`.
fn stored_hash_index(level: u8, n: u64) -> u64 {
    let mut n = n;
    for _ in 0..level {
        n = 2 * n + 1;
    }
    let mut i: u64 = 0;
    let mut s = n;
    loop {
        i += s;
        s >>= 1;
        if s == 0 {
            break;
        }
    }
    i + level as u64
}

/// Compute the tree root hash for a tree of size n.
/// Matches `tlog_tiles::tlog::tree_hash`.
fn tree_hash(n: u64, hashes: &HashMap<u64, [u8; 32]>) -> Result<[u8; 32]> {
    if n == 0 {
        // SHA-256("") — empty tree hash
        let h = Sha256::digest([]);
        return Ok(h.into());
    }

    // Collect the sub-tree root hashes and combine right-to-left.
    let indexes = sub_tree_index(0, n);
    let mut hash_values: Vec<[u8; 32]> = Vec::new();
    for idx in &indexes {
        let h = hashes
            .get(idx)
            .with_context(|| format!("missing hash at index {idx} for tree_hash"))?;
        hash_values.push(*h);
    }

    // Combine from right to left.
    let mut result = *hash_values.last().unwrap();
    for h in hash_values.iter().rev().skip(1) {
        result = node_hash(h, &result);
    }

    Ok(result)
}

/// Compute the stored hash indexes for the sub-tree roots of a tree of size n.
/// Matches `tlog_tiles::tlog::sub_tree_index`.
fn sub_tree_index(lo: u64, n: u64) -> Vec<u64> {
    let mut indexes = Vec::new();
    let mut lo = lo;
    let mut n = n;
    while n > 0 {
        // Find the largest power of 2 <= n.
        let k = 1u64 << (63 - n.leading_zeros() as u64);
        // The sub-tree root for the left half (lo..lo+k) is at the top level
        // covering k leaves.
        let level = (63 - k.leading_zeros()) as u8;
        let position = lo >> level;
        indexes.push(stored_hash_index(level, position));
        lo += k;
        n -= k;
    }
    indexes
}

// ---------------------------------------------------------------------------
// Hex helpers
// ---------------------------------------------------------------------------

fn hex_to_bytes(hex: &str) -> Result<[u8; 32]> {
    anyhow::ensure!(hex.len() == 64, "invalid hex hash length: {}", hex.len());
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .with_context(|| format!("invalid hex at position {i}"))?;
    }
    Ok(bytes)
}

fn bytes_to_hex(bytes: &[u8; 32]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for &b in bytes.iter() {
        out.push(LUT[(b >> 4) as usize] as char);
        out.push(LUT[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_hash_index_matches_tlog() {
        // Level 0: identity-like mapping
        assert_eq!(stored_hash_index(0, 0), 0);
        assert_eq!(stored_hash_index(0, 1), 1);
        assert_eq!(stored_hash_index(0, 2), 3);
        assert_eq!(stored_hash_index(0, 3), 4);
        assert_eq!(stored_hash_index(0, 4), 7);

        // Level 1
        assert_eq!(stored_hash_index(1, 0), 2);
        assert_eq!(stored_hash_index(1, 1), 5);
    }

    #[test]
    fn node_hash_is_rfc6962() {
        let left = [0u8; 32];
        let right = [1u8; 32];
        let result = node_hash(&left, &right);

        // Verify: SHA-256(0x01 || [0;32] || [1;32])
        let mut hasher = Sha256::new();
        hasher.update([0x01]);
        hasher.update([0u8; 32]);
        hasher.update([1u8; 32]);
        let expected: [u8; 32] = hasher.finalize().into();

        assert_eq!(result, expected);
    }

    #[test]
    fn hex_roundtrip() {
        let bytes = [
            0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x10, 0x20, 0x30, 0x40,
            0x50, 0x60, 0x70, 0x80,
        ];
        let hex = bytes_to_hex(&bytes);
        let back = hex_to_bytes(&hex).unwrap();
        assert_eq!(bytes, back);
    }
}
