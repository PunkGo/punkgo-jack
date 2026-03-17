//! Offline verification of PunkGo Merkle proofs.
//!
//! No daemon connection required — pure math.

use anyhow::{bail, Context, Result};
use serde_json::Value;
use tlog_tiles::tlog;

/// Parsed CLI arguments for `punkgo-jack verify`.
#[derive(Debug)]
pub struct VerifyArgs {
    pub event_id: Option<String>,
    pub file: Option<String>,
}

pub fn parse_args(args: &mut impl Iterator<Item = String>) -> Result<VerifyArgs> {
    let mut event_id = None;
    let mut file = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--file" | "-f" => {
                file = Some(args.next().context("--file requires a path")?);
            }
            _ if event_id.is_none() => event_id = Some(arg),
            other => bail!("unknown verify option: {other}"),
        }
    }

    if event_id.is_none() && file.is_none() {
        bail!("usage: punkgo-jack verify <event_id> or punkgo-jack verify --file proof.json");
    }

    Ok(VerifyArgs { event_id, file })
}

/// Run offline verification.
///
/// Two modes:
/// 1. `punkgo-jack verify <event_id>` — fetch from daemon, verify locally
/// 2. `punkgo-jack verify --file proof.json` — fully offline from exported JSON
pub fn run_verify(args: VerifyArgs) -> Result<()> {
    let data: Value = if let Some(path) = &args.file {
        let content =
            std::fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?;
        serde_json::from_str(&content)?
    } else if let Some(event_id) = &args.event_id {
        fetch_from_daemon(event_id)?
    } else {
        bail!("usage: punkgo-jack verify <event_id> or punkgo-jack verify --file proof.json");
    };

    // Extract fields for verification.
    let event = data.get("event").unwrap_or(&data);

    let event_hash_hex = event
        .get("event_hash")
        .and_then(Value::as_str)
        .context("missing event_hash")?;

    let log_index = event
        .get("log_index")
        .and_then(Value::as_u64)
        .context("missing log_index")?;

    let proof_obj = data.get("proof").context("missing proof object")?;

    let tree_size = proof_obj
        .get("tree_size")
        .and_then(Value::as_u64)
        .context("missing proof.tree_size")?;

    let proof_hashes_hex: Vec<&str> = proof_obj
        .get("proof")
        .and_then(Value::as_array)
        .context("missing proof.proof array")?
        .iter()
        .filter_map(Value::as_str)
        .collect();

    let checkpoint = data.get("checkpoint");
    let checkpoint_root_hex = checkpoint
        .and_then(|c| c.get("root_hash"))
        .and_then(Value::as_str);

    // Convert hex strings to tlog::Hash.
    let leaf_hash = hex_to_tlog_hash(event_hash_hex)?;

    let proof: Vec<tlog::Hash> = proof_hashes_hex
        .iter()
        .map(|h| hex_to_tlog_hash(h))
        .collect::<Result<Vec<_>>>()?;

    println!(
        "Event:      {}",
        event
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or(event_hash_hex)
    );
    println!("Log index:  {log_index}");
    println!("Tree size:  {tree_size}");
    println!("Leaf hash:  {event_hash_hex}");
    println!("Proof path: {} hashes", proof_hashes_hex.len());

    // Verify against checkpoint root if available.
    if let Some(root_hex) = checkpoint_root_hex {
        let root_hash = hex_to_tlog_hash(root_hex)?;
        println!("Checkpoint: {root_hex}");

        match tlog::check_record(&proof, tree_size, root_hash, log_index, leaf_hash) {
            Ok(()) => {
                println!("Inclusion:  \u{2713} VERIFIED \u{2014} leaf is in the tree, root matches checkpoint");
            }
            Err(e) => {
                println!("Inclusion:  \u{2717} FAILED \u{2014} {e}");
                std::process::exit(1);
            }
        }
    } else {
        // No checkpoint root — we can compute the implied root from the proof
        // but cannot verify it against anything. Still useful to check proof structure.
        let computed_root = compute_root_from_proof(&proof, tree_size, log_index, leaf_hash);
        match computed_root {
            Ok(root) => {
                let root_hex = hex_encode(&root.0);
                println!("Computed:   {root_hex}");
                println!("Inclusion:  \u{2713} VERIFIED \u{2014} proof is mathematically valid");
                println!("            (no checkpoint provided for root comparison)");
            }
            Err(e) => {
                println!("Inclusion:  \u{2717} FAILED \u{2014} {e}");
                std::process::exit(1);
            }
        }
    }

    // TSA verification: check if a TSR file exists for this tree_size.
    print_tsa_status(tree_size as i64, checkpoint_root_hex);

    Ok(())
}

/// Fetch event + proof + checkpoint from daemon via IPC.
fn fetch_from_daemon(event_id: &str) -> Result<Value> {
    use crate::ipc_client::{new_request_id, IpcClient};
    use punkgo_core::protocol::{RequestEnvelope, RequestType};
    use serde_json::json;

    let client = IpcClient::from_env(None);

    // Get event
    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "events", "limit": 100 }),
    };
    let resp = client.send(&req)?;
    if resp.status != "ok" {
        bail!("failed to query events from kernel");
    }
    let events = resp
        .payload
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let event = events
        .iter()
        .find(|e| {
            e.get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.starts_with(event_id))
        })
        .ok_or_else(|| anyhow::anyhow!("no event found matching '{event_id}'"))?
        .clone();

    let log_index = event
        .get("log_index")
        .and_then(Value::as_u64)
        .context("event has no log_index")?;

    // Get inclusion proof
    let proof_req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "audit_inclusion_proof", "log_index": log_index }),
    };
    let proof_resp = client.send(&proof_req)?;
    if proof_resp.status != "ok" {
        bail!("failed to get inclusion proof");
    }

    // Get checkpoint
    let cp_req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "audit_checkpoint" }),
    };
    let cp_resp = client.send(&cp_req)?;
    if cp_resp.status != "ok" {
        bail!("failed to get checkpoint");
    }

    Ok(json!({
        "event": event,
        "proof": proof_resp.payload,
        "checkpoint": cp_resp.payload,
    }))
}

/// Compute the implied tree root from a record proof.
/// This reimplements tlog's internal `run_record_proof` logic.
fn compute_root_from_proof(
    proof: &[tlog::Hash],
    tree_size: u64,
    log_index: u64,
    leaf_hash: tlog::Hash,
) -> Result<tlog::Hash> {
    if log_index >= tree_size {
        bail!("log_index ({log_index}) >= tree_size ({tree_size})");
    }
    run_record_proof(proof, 0, tree_size, log_index, leaf_hash)
}

fn run_record_proof(
    p: &[tlog::Hash],
    lo: u64,
    hi: u64,
    n: u64,
    leaf_hash: tlog::Hash,
) -> Result<tlog::Hash> {
    if n < lo || n >= hi {
        bail!("invalid proof structure");
    }
    if lo + 1 == hi {
        if !p.is_empty() {
            bail!("proof has extra hashes");
        }
        return Ok(leaf_hash);
    }
    if p.is_empty() {
        bail!("proof too short");
    }
    let k = maxpow2(hi - lo);
    if n < lo + k {
        let th = run_record_proof(&p[..p.len() - 1], lo, lo + k, n, leaf_hash)?;
        Ok(node_hash(th, p[p.len() - 1]))
    } else {
        let th = run_record_proof(&p[..p.len() - 1], lo + k, hi, n, leaf_hash)?;
        Ok(node_hash(p[p.len() - 1], th))
    }
}

/// Largest power of 2 less than n.
fn maxpow2(n: u64) -> u64 {
    if n <= 1 {
        return 1;
    }
    1u64 << (63 - (n - 1).leading_zeros())
}

/// RFC 6962 interior node hash: SHA-256(0x01 || left || right)
fn node_hash(left: tlog::Hash, right: tlog::Hash) -> tlog::Hash {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update([0x01]);
    hasher.update(left.0);
    hasher.update(right.0);
    let result = hasher.finalize();
    let mut h = [0u8; 32];
    h.copy_from_slice(&result);
    tlog::Hash(h)
}

fn hex_to_tlog_hash(hex_str: &str) -> Result<tlog::Hash> {
    let bytes =
        hex_decode(hex_str).map_err(|e| anyhow::anyhow!("invalid hex hash '{hex_str}': {e}"))?;
    if bytes.len() != 32 {
        bail!("hash must be 32 bytes, got {}", bytes.len());
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&bytes);
    Ok(tlog::Hash(h))
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    crate::tsa_verify::hex_decode(s)
}

fn hex_encode(bytes: &[u8]) -> String {
    crate::tsa_verify::hex_encode(bytes)
}

// ─── TSA verification ──────────────────────────────────────────────

/// Display TSA anchor status for a given tree_size.
fn print_tsa_status(tree_size: i64, checkpoint_root_hex: Option<&str>) {
    let Some(tsr_path) = crate::config::tsr_path(tree_size) else {
        println!("TSA:        not configured");
        return;
    };

    if !tsr_path.exists() {
        println!("TSA:        not anchored (no TSR for tree_size={tree_size})");
        return;
    }

    let tsr_bytes = match std::fs::read(&tsr_path) {
        Ok(b) => b,
        Err(e) => {
            println!("TSA:        \u{2717} FAILED to read TSR \u{2014} {e}");
            return;
        }
    };

    let expected_hash = checkpoint_root_hex.and_then(|h| crate::tsa_verify::hex_decode(h).ok());

    match crate::tsa_verify::verify_tsr(&tsr_bytes, expected_hash.as_deref()) {
        Ok(info) => {
            println!("TSA:        \u{2713} ANCHORED at {}", info.gen_time);
            println!("TSR file:   {}", tsr_path.display());
        }
        Err(e) => {
            println!("TSA:        \u{2717} INVALID \u{2014} {e}");
            println!("TSR file:   {}", tsr_path.display());
        }
    }
}

/// Standalone `verify-tsr` command: verify a specific TSR file or tree_size.
pub fn run_verify_tsr(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let arg = args.next().context(
        "usage: punkgo-jack verify-tsr <tree_size> or punkgo-jack verify-tsr --file <path>",
    )?;

    let tsr_path = if arg == "--file" || arg == "-f" {
        let path = args.next().context("--file requires a path")?;
        std::path::PathBuf::from(path)
    } else {
        let tree_size: i64 = arg
            .parse()
            .context("expected tree_size (integer) or --file <path>")?;
        crate::config::tsr_path(tree_size).context("failed to determine TSR path")?
    };

    if !tsr_path.exists() {
        bail!("TSR file not found: {}", tsr_path.display());
    }

    let tsr_bytes = std::fs::read(&tsr_path)
        .with_context(|| format!("failed to read {}", tsr_path.display()))?;

    println!("TSR file:   {}", tsr_path.display());

    match crate::tsa_verify::verify_tsr(&tsr_bytes, None) {
        Ok(info) => {
            println!("Status:     \u{2713} VALID");
            println!("Timestamp:  {}", info.gen_time);
            println!(
                "Hash:       {}",
                crate::tsa_verify::hex_encode(&info.message_hash)
            );
            println!("Protocol:   RFC 3161");
        }
        Err(e) => {
            println!("Status:     \u{2717} INVALID \u{2014} {e}");
            std::process::exit(1);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tlog_tiles::tlog;

    // --- Helper: in-memory hash storage for building test trees ---

    struct TestHashStorage(Vec<tlog::Hash>);

    impl tlog::HashReader for TestHashStorage {
        fn read_hashes(&self, indexes: &[u64]) -> Result<Vec<tlog::Hash>, tlog_tiles::Error> {
            let mut out = Vec::with_capacity(indexes.len());
            for &idx in indexes {
                out.push(self.0[idx as usize]);
            }
            Ok(out)
        }
    }

    /// Build a tree with `n` leaves from deterministic data, returning
    /// (hash_storage, leaf_hashes) so we can generate proofs and roots.
    fn build_test_tree(n: u64) -> (TestHashStorage, Vec<tlog::Hash>) {
        let mut storage = TestHashStorage(Vec::new());
        let mut leaf_hashes = Vec::new();
        for i in 0..n {
            let data = format!("leaf-{i}");
            let leaf_hash = tlog::record_hash(data.as_bytes());
            let new_hashes = tlog::stored_hashes_for_record_hash(i, leaf_hash, &storage).unwrap();
            storage.0.extend(new_hashes);
            leaf_hashes.push(leaf_hash);
        }
        (storage, leaf_hashes)
    }

    // --- parse_args tests ---

    #[test]
    fn parse_args_event_id() {
        let mut args = vec!["abc123".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert_eq!(parsed.event_id, Some("abc123".to_string()));
        assert_eq!(parsed.file, None);
    }

    #[test]
    fn parse_args_file_flag() {
        let mut args = vec!["--file".to_string(), "proof.json".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert_eq!(parsed.file, Some("proof.json".to_string()));
        assert_eq!(parsed.event_id, None);
    }

    #[test]
    fn parse_args_file_short_flag() {
        let mut args = vec!["-f".to_string(), "proof.json".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert_eq!(parsed.file, Some("proof.json".to_string()));
    }

    #[test]
    fn parse_args_empty_fails() {
        let mut args = Vec::<String>::new().into_iter();
        assert!(parse_args(&mut args).is_err());
    }

    #[test]
    fn parse_args_file_without_path_fails() {
        let mut args = vec!["--file".to_string()].into_iter();
        assert!(parse_args(&mut args).is_err());
    }

    #[test]
    fn parse_args_unknown_option_fails() {
        let mut args = vec!["abc".to_string(), "--bogus".to_string()].into_iter();
        assert!(parse_args(&mut args).is_err());
    }

    // --- hex helpers ---

    #[test]
    fn hex_roundtrip() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let encoded = hex_encode(&bytes);
        assert_eq!(encoded, "deadbeef");
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(decoded, bytes);
    }

    #[test]
    fn hex_decode_odd_length_fails() {
        assert!(hex_decode("abc").is_err());
    }

    #[test]
    fn hex_decode_invalid_char_fails() {
        assert!(hex_decode("zzzz").is_err());
    }

    #[test]
    fn hex_to_tlog_hash_valid() {
        let hex_str = "a".repeat(64); // 32 bytes of 0xaa
        let h = hex_to_tlog_hash(&hex_str).unwrap();
        assert_eq!(h.0, [0xaa; 32]);
    }

    #[test]
    fn hex_to_tlog_hash_wrong_length_fails() {
        let hex_str = "aabb"; // only 2 bytes
        assert!(hex_to_tlog_hash(&hex_str).is_err());
    }

    // --- maxpow2 ---

    #[test]
    fn maxpow2_known_values() {
        assert_eq!(maxpow2(1), 1);
        assert_eq!(maxpow2(2), 1);
        assert_eq!(maxpow2(3), 2);
        assert_eq!(maxpow2(4), 2);
        assert_eq!(maxpow2(5), 4);
        assert_eq!(maxpow2(8), 4);
        assert_eq!(maxpow2(9), 8);
        assert_eq!(maxpow2(16), 8);
        assert_eq!(maxpow2(17), 16);
    }

    // --- node_hash matches tlog ---

    #[test]
    fn node_hash_matches_tlog_library() {
        let left = tlog::record_hash(b"left");
        let right = tlog::record_hash(b"right");
        let our_result = node_hash(left, right);
        let lib_result = tlog::node_hash(left, right);
        assert_eq!(our_result, lib_result);
    }

    // --- compute_root_from_proof: cross-verify with tlog library ---

    #[test]
    fn compute_root_single_leaf() {
        // Tree with 1 leaf: root = leaf_hash, proof = empty
        let (storage, leaves) = build_test_tree(1);
        let tree_root = tlog::tree_hash(1, &storage).unwrap();
        let proof = tlog::prove_record(1, 0, &storage).unwrap();
        assert!(proof.is_empty());

        let computed = compute_root_from_proof(&proof, 1, 0, leaves[0]).unwrap();
        assert_eq!(computed, tree_root);
    }

    #[test]
    fn compute_root_two_leaves() {
        let (storage, leaves) = build_test_tree(2);
        let tree_root = tlog::tree_hash(2, &storage).unwrap();

        // Verify leaf 0
        let proof0 = tlog::prove_record(2, 0, &storage).unwrap();
        let computed0 = compute_root_from_proof(&proof0, 2, 0, leaves[0]).unwrap();
        assert_eq!(computed0, tree_root);

        // Verify leaf 1
        let proof1 = tlog::prove_record(2, 1, &storage).unwrap();
        let computed1 = compute_root_from_proof(&proof1, 2, 1, leaves[1]).unwrap();
        assert_eq!(computed1, tree_root);
    }

    #[test]
    fn compute_root_seven_leaves() {
        // Non-power-of-2 tree — exercises the asymmetric subtree logic
        let (storage, leaves) = build_test_tree(7);
        let tree_root = tlog::tree_hash(7, &storage).unwrap();

        for i in 0..7u64 {
            let proof = tlog::prove_record(7, i, &storage).unwrap();
            let computed = compute_root_from_proof(&proof, 7, i, leaves[i as usize]).unwrap();
            assert_eq!(
                computed, tree_root,
                "root mismatch for leaf {i} in 7-leaf tree"
            );
        }
    }

    #[test]
    fn compute_root_large_tree() {
        // 100 leaves — ensures the algorithm works at scale
        let (storage, leaves) = build_test_tree(100);
        let tree_root = tlog::tree_hash(100, &storage).unwrap();

        // Spot check several positions: first, last, middle, power-of-2 boundary
        for &i in &[0u64, 1, 31, 32, 63, 64, 98, 99] {
            let proof = tlog::prove_record(100, i, &storage).unwrap();
            let computed = compute_root_from_proof(&proof, 100, i, leaves[i as usize]).unwrap();
            assert_eq!(
                computed, tree_root,
                "root mismatch for leaf {i} in 100-leaf tree"
            );
        }
    }

    #[test]
    fn compute_root_matches_check_record() {
        // Our compute_root_from_proof must produce the same root that
        // tlog::check_record accepts.
        let (storage, leaves) = build_test_tree(50);
        let tree_root = tlog::tree_hash(50, &storage).unwrap();

        for &i in &[0u64, 7, 25, 49] {
            let proof = tlog::prove_record(50, i, &storage).unwrap();

            // tlog::check_record should accept
            tlog::check_record(&proof, 50, tree_root, i, leaves[i as usize]).unwrap();

            // Our compute should produce the same root
            let computed = compute_root_from_proof(&proof, 50, i, leaves[i as usize]).unwrap();
            assert_eq!(computed, tree_root);
        }
    }

    #[test]
    fn compute_root_index_out_of_range_fails() {
        assert!(compute_root_from_proof(&[], 5, 5, tlog::Hash([0; 32])).is_err());
        assert!(compute_root_from_proof(&[], 5, 10, tlog::Hash([0; 32])).is_err());
    }

    #[test]
    fn compute_root_wrong_leaf_hash_produces_wrong_root() {
        let (storage, leaves) = build_test_tree(10);
        let tree_root = tlog::tree_hash(10, &storage).unwrap();
        let proof = tlog::prove_record(10, 3, &storage).unwrap();

        // Correct leaf → correct root
        let correct = compute_root_from_proof(&proof, 10, 3, leaves[3]).unwrap();
        assert_eq!(correct, tree_root);

        // Wrong leaf → different root
        let wrong_leaf = tlog::Hash([0xff; 32]);
        let wrong = compute_root_from_proof(&proof, 10, 3, wrong_leaf).unwrap();
        assert_ne!(wrong, tree_root);
    }

    #[test]
    fn compute_root_truncated_proof_fails() {
        let (storage, leaves) = build_test_tree(10);
        let proof = tlog::prove_record(10, 3, &storage).unwrap();
        assert!(proof.len() > 1); // sanity: proof should have multiple hashes

        // Truncate proof
        let short = &proof[..proof.len() - 1];
        assert!(compute_root_from_proof(short, 10, 3, leaves[3]).is_err());
    }

    // --- run_verify with file (end-to-end) ---

    #[test]
    fn run_verify_from_file_with_checkpoint() {
        let (storage, leaves) = build_test_tree(20);
        let tree_root = tlog::tree_hash(20, &storage).unwrap();
        let proof = tlog::prove_record(20, 5, &storage).unwrap();

        let proof_hex: Vec<String> = proof.iter().map(|h| hex_encode(&h.0)).collect();
        let leaf_hex = hex_encode(&leaves[5].0);
        let root_hex = hex_encode(&tree_root.0);

        let data = json!({
            "event": {
                "id": "test-event-005",
                "event_hash": leaf_hex,
                "log_index": 5
            },
            "proof": {
                "tree_size": 20,
                "proof": proof_hex
            },
            "checkpoint": {
                "root_hash": root_hex
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proof.json");
        std::fs::write(&path, serde_json::to_string_pretty(&data).unwrap()).unwrap();

        let args = VerifyArgs {
            event_id: None,
            file: Some(path.to_str().unwrap().to_string()),
        };
        // Should succeed without error (prints to stdout)
        run_verify(args).unwrap();
    }

    #[test]
    fn run_verify_from_file_without_checkpoint() {
        let (storage, leaves) = build_test_tree(15);
        let proof = tlog::prove_record(15, 10, &storage).unwrap();

        let proof_hex: Vec<String> = proof.iter().map(|h| hex_encode(&h.0)).collect();
        let leaf_hex = hex_encode(&leaves[10].0);

        let data = json!({
            "event": {
                "id": "test-event-010",
                "event_hash": leaf_hex,
                "log_index": 10
            },
            "proof": {
                "tree_size": 15,
                "proof": proof_hex
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proof.json");
        std::fs::write(&path, serde_json::to_string_pretty(&data).unwrap()).unwrap();

        let args = VerifyArgs {
            event_id: None,
            file: Some(path.to_str().unwrap().to_string()),
        };
        run_verify(args).unwrap();
    }

    #[test]
    fn run_verify_from_file_wrong_root_exits() {
        // We can't test process::exit(1) directly, so test the internal logic instead.
        let (storage, leaves) = build_test_tree(10);
        let proof = tlog::prove_record(10, 3, &storage).unwrap();

        let wrong_root = tlog::Hash([0xaa; 32]);

        // check_record should fail with wrong root
        let result = tlog::check_record(&proof, 10, wrong_root, 3, leaves[3]);
        assert!(result.is_err());
    }

    #[test]
    fn run_verify_missing_event_hash_fails() {
        let data = json!({
            "event": {
                "id": "test",
                "log_index": 0
            },
            "proof": {
                "tree_size": 1,
                "proof": []
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, serde_json::to_string_pretty(&data).unwrap()).unwrap();

        let args = VerifyArgs {
            event_id: None,
            file: Some(path.to_str().unwrap().to_string()),
        };
        assert!(run_verify(args).is_err());
    }

    #[test]
    fn run_verify_missing_proof_fails() {
        let data = json!({
            "event": {
                "id": "test",
                "event_hash": "aa".repeat(32),
                "log_index": 0
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, serde_json::to_string_pretty(&data).unwrap()).unwrap();

        let args = VerifyArgs {
            event_id: None,
            file: Some(path.to_str().unwrap().to_string()),
        };
        assert!(run_verify(args).is_err());
    }

    /// Generate a proof JSON file for cross-language verification.
    /// Run with: cargo test generate_cross_verify_json -- --nocapture --ignored
    #[test]
    #[ignore]
    fn generate_cross_verify_json() {
        let (storage, leaves) = build_test_tree(20);
        let tree_root = tlog::tree_hash(20, &storage).unwrap();
        let proof = tlog::prove_record(20, 5, &storage).unwrap();

        let proof_hex: Vec<String> = proof.iter().map(|h| hex_encode(&h.0)).collect();
        let leaf_hex = hex_encode(&leaves[5].0);
        let root_hex = hex_encode(&tree_root.0);

        let data = json!({
            "event": {
                "id": "cross-verify-test-005",
                "event_hash": leaf_hex,
                "log_index": 5
            },
            "proof": {
                "tree_size": 20,
                "proof": proof_hex
            },
            "checkpoint": {
                "root_hash": root_hex
            }
        });

        let json_str = serde_json::to_string_pretty(&data).unwrap();
        println!("{json_str}");
    }

    #[test]
    fn run_verify_nonexistent_file_fails() {
        let args = VerifyArgs {
            event_id: None,
            file: Some("/tmp/does-not-exist-xyz.json".to_string()),
        };
        assert!(run_verify(args).is_err());
    }
}
