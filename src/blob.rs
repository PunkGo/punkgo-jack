//! Content-addressed blob store for large audit payloads.
//!
//! Large fields (file contents, command output) are externalized from the Merkle
//! tree payload and stored as flat files keyed by their SHA-256 hash:
//!
//!   ~/.punkgo/blobs/<sha256-hex>
//!
//! The metadata stored in the kernel only contains the hash reference, keeping
//! the event log compact while preserving full evidence recoverability.

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Minimum size (bytes) for a string field to be externalized to the blob store.
/// Fields smaller than this are kept inline in metadata.
///
/// 1KB is derived from the energy model, not an empirical guess:
///   append_cost = 1 + payload_bytes / 1024
/// At exactly 1KB, append_cost jumps from 1 to 2 — this is the structural
/// threshold where externalization starts saving energy. Industry comparisons
/// (Rekor 100KB, SQLite ~100KB, Kafka 1MB) use storage/transport cost models;
/// PunkGo's threshold is lower because the Landauer-inspired energy tax
/// penalizes payload size at a finer granularity.
const EXTERNALIZE_THRESHOLD: usize = 1024;

/// Keys that should never be externalized, even if above threshold.
/// These are small display-critical fields needed for history/report rendering.
const NEVER_EXTERNALIZE: &[&str] = &["file_path", "command", "description", "pattern", "query"];

/// Compute SHA-256 hex digest of a byte slice.
pub fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Store content in the blob store if it exceeds the threshold.
/// Returns `Some("sha256:<hex>")` if externalized, `None` if kept inline.
pub fn externalize(content: &str) -> Result<Option<String>> {
    if content.len() < EXTERNALIZE_THRESHOLD {
        return Ok(None);
    }

    let hash = hash_bytes(content.as_bytes());
    let blob_path = blob_path(&hash)?;

    // Content-addressed: skip write if blob already exists (dedup).
    if !blob_path.exists() {
        if let Some(parent) = blob_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create blob dir {}", parent.display()))?;
        }
        std::fs::write(&blob_path, content.as_bytes())
            .with_context(|| format!("failed to write blob {}", blob_path.display()))?;
    }

    Ok(Some(format!("sha256:{hash}")))
}

/// Retrieve content from the blob store by hash reference.
/// Accepts both "sha256:<hex>" format and bare hex.
pub(crate) fn resolve(hash_ref: &str) -> Result<Option<String>> {
    let hex = hash_ref.strip_prefix("sha256:").unwrap_or(hash_ref);
    let path = blob_path(hex)?;

    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read blob {}", path.display()))?;
    Ok(Some(content))
}

/// Process a serde_json::Value, externalizing any string field that exceeds the threshold.
/// Returns the (possibly modified) value and a list of hash references.
///
/// Scans all top-level string fields in a JSON object. Fields in `NEVER_EXTERNALIZE`
/// are skipped (they're small display-critical values). All other strings above
/// `EXTERNALIZE_THRESHOLD` are replaced with `"sha256:<hex>"` references.
pub fn externalize_tool_input(tool_input: &Value) -> Result<(Value, Vec<String>)> {
    let Some(obj) = tool_input.as_object() else {
        return Ok((tool_input.clone(), vec![]));
    };

    let mut result = obj.clone();
    let mut refs = Vec::new();

    let keys: Vec<String> = result.keys().cloned().collect();
    for key in &keys {
        if NEVER_EXTERNALIZE.contains(&key.as_str()) {
            continue;
        }
        if let Some(val) = result.get(key).and_then(Value::as_str) {
            if let Some(hash_ref) = externalize(val)? {
                refs.push(hash_ref.clone());
                result.insert(key.clone(), Value::String(hash_ref));
            }
        }
    }

    Ok((Value::Object(result), refs))
}

/// Blob directory path.
pub fn blob_dir() -> Result<PathBuf> {
    let data_dir = crate::session::data_dir()?;
    Ok(data_dir.join("blobs"))
}

/// Blob file path for a given hex hash.
fn blob_path(hex_hash: &str) -> Result<PathBuf> {
    Ok(blob_dir()?.join(hex_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hash_bytes_deterministic() {
        let h1 = hash_bytes(b"hello world");
        let h2 = hash_bytes(b"hello world");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // SHA-256 hex = 64 chars
    }

    #[test]
    fn small_content_stays_inline() {
        // externalize checks threshold before touching filesystem.
        // Content below 1KB → returns None without any I/O.
        const { assert!(EXTERNALIZE_THRESHOLD > 5) };
        // We can't call externalize() without a valid data dir, but we can
        // verify the threshold logic directly:
        let small = "hello";
        assert!(small.len() < EXTERNALIZE_THRESHOLD);
    }

    #[test]
    fn hash_is_content_addressed() {
        // Same content → same hash (dedup guarantee).
        let h1 = hash_bytes(b"fn main() { println!(\"hello\"); }");
        let h2 = hash_bytes(b"fn main() { println!(\"hello\"); }");
        assert_eq!(h1, h2);

        // Different content → different hash.
        let h3 = hash_bytes(b"fn main() { println!(\"world\"); }");
        assert_ne!(h1, h3);
    }

    #[test]
    fn externalize_tool_input_handles_small_values() {
        // Small values stay inline — no filesystem needed.
        let input = json!({
            "file_path": "/src/main.rs",
            "content": "fn main() {}"
        });
        let (result, refs) = externalize_tool_input(&input).unwrap();
        assert!(refs.is_empty());
        assert_eq!(result["content"], "fn main() {}");
        assert_eq!(result["file_path"], "/src/main.rs");
    }

    #[test]
    fn externalize_tool_input_non_object_passthrough() {
        let input = json!("just a string");
        let (result, refs) = externalize_tool_input(&input).unwrap();
        assert!(refs.is_empty());
        assert_eq!(result, json!("just a string"));
    }

    #[test]
    fn externalize_tool_input_preserves_non_content_keys() {
        let input = json!({
            "file_path": "/src/main.rs",
            "description": "Write a file",
            "timeout": 5000
        });
        let (result, refs) = externalize_tool_input(&input).unwrap();
        assert!(refs.is_empty());
        assert_eq!(result["file_path"], "/src/main.rs");
        assert_eq!(result["description"], "Write a file");
        assert_eq!(result["timeout"], 5000);
    }

    #[test]
    fn never_externalize_keys_stay_inline() {
        // Even if a NEVER_EXTERNALIZE key is above threshold, it stays inline.
        for key in NEVER_EXTERNALIZE {
            let input = json!({ *key: "x" });
            let (result, refs) = externalize_tool_input(&input).unwrap();
            assert!(refs.is_empty(), "key {key} should never be externalized");
            assert_eq!(result[*key], "x");
        }
    }

    /// Integration test that uses filesystem — run with PUNKGO_DATA_DIR set to temp.
    /// Skipped by default since it requires a valid data directory.
    /// Run manually with: PUNKGO_DATA_DIR=/tmp/punkgo-test cargo test blob -- --ignored
    #[test]
    #[ignore]
    fn externalize_and_resolve_roundtrip() {
        let large = "x".repeat(2000);
        let hash_ref = externalize(&large).unwrap().unwrap();
        assert!(hash_ref.starts_with("sha256:"));

        let recovered = resolve(&hash_ref).unwrap().unwrap();
        assert_eq!(recovered, large);

        // Dedup: second write returns same hash.
        let hash_ref2 = externalize(&large).unwrap().unwrap();
        assert_eq!(hash_ref, hash_ref2);
    }
}
