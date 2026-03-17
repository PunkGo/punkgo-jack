//! TSA anchoring — fetch checkpoint from kernel, submit to TSA, store TSR locally.
//!
//! This module handles the complete anchor lifecycle:
//! 1. Load config (enabled? which URL? rate limit?)
//! 2. Check rate limit (skip if too recent)
//! 3. Fetch latest checkpoint from kernel via IPC
//! 4. Check if already anchored (file exists)
//! 5. Build RFC 3161 TimeStampReq via x509-tsp
//! 6. HTTP POST to TSA
//! 7. Validate response via tsa_verify (PKIStatus, hash match, genTime)
//! 8. Store TSR to ~/.punkgo/state/tsa/<tree_size>.tsr
//!
//! The anchor command is a standalone CLI entry point, also called from setup hooks.
//! It is sync, short-lived, and best-effort — failure never blocks the user.

use std::fs;
use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{Context, Result};
use punkgo_core::protocol::{RequestEnvelope, RequestType};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::config::{self, Config};
use crate::ipc_client::{self, IpcClient};
use crate::tsa_verify;

/// CLI entry point: `punkgo-jack anchor [--quiet] [--stale-only]`
pub fn run_anchor(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let mut quiet = false;
    let mut stale_only = false;
    for arg in args {
        match arg.as_str() {
            "--quiet" | "-q" => quiet = true,
            "--stale-only" => stale_only = true,
            other => anyhow::bail!("unknown anchor option: {other}"),
        }
    }

    let config = config::load_config();
    if !config.tsa.enabled {
        if !quiet {
            eprintln!("[punkgo] TSA anchoring is disabled. Enable with: [tsa] enabled = true in ~/.punkgo/config.toml");
        }
        return Ok(());
    }

    if stale_only && !needs_anchor()? {
        debug!("no stale anchor needed");
        return Ok(());
    }

    match do_anchor(&config) {
        Ok(Some(receipt)) => {
            if !quiet {
                eprintln!(
                    "[punkgo] anchored tree_size={} root={} time={}",
                    receipt.tree_size,
                    receipt.root_hash_short(),
                    receipt.gen_time
                );
            }
            Ok(())
        }
        Ok(None) => {
            debug!("anchor skipped (already anchored or rate limited)");
            Ok(())
        }
        Err(e) => {
            warn!(error = %e, "TSA anchor failed");
            if !quiet {
                eprintln!("[punkgo] TSA anchor failed: {e}");
            }
            Ok(())
        }
    }
}

/// Anchor receipt — returned on successful anchoring.
pub struct AnchorReceipt {
    pub tree_size: i64,
    pub root_hash: String,
    pub gen_time: String,
}

impl AnchorReceipt {
    pub fn root_hash_short(&self) -> &str {
        self.root_hash.get(..8).unwrap_or(&self.root_hash)
    }
}

/// Core anchor logic. Returns Some(receipt) on success, None if skipped.
pub fn do_anchor(config: &Config) -> Result<Option<AnchorReceipt>> {
    let client = IpcClient::from_env(None);
    let (tree_size, root_hash) = fetch_checkpoint(&client)?;

    let tsr_path = config::tsr_path(tree_size).context("failed to determine TSR storage path")?;
    let tsr_exists = tsr_path.exists();
    if tsr_exists {
        debug!(tree_size, "already anchored");
        return Ok(None);
    }

    // Semantic rate limit: skip if tree hasn't grown since last anchor AND the
    // TSR file exists. If TSR is missing (deleted/lost), always re-anchor.
    if !should_anchor(tree_size, tsr_exists, config)? {
        debug!(tree_size, "no new events since last anchor, skipping");
        return Ok(None);
    }

    let hash_bytes =
        tsa_verify::hex_to_32bytes(&root_hash).context("invalid root_hash hex from checkpoint")?;
    let tsq = build_timestamp_req(&hash_bytes)?;

    info!(url = %config.tsa.url, tree_size, "submitting to TSA");
    let tsr_bytes = http_post_tsa(&config.tsa.url, &tsq, config.tsa.timeout_secs)?;

    // Validate: PKIStatus + hash match + extract genTime
    let tsr_info = tsa_verify::verify_tsr(&tsr_bytes, Some(&hash_bytes))?;

    // Atomic store: write tmp then rename
    if let Some(parent) = tsr_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = tsr_path.with_extension("tsr.tmp");
    {
        let mut f = fs::File::create(&tmp_path)?;
        f.write_all(&tsr_bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp_path, &tsr_path)?;

    save_last_anchored_tree_size(tree_size)?;

    info!(tree_size, root_hash = %root_hash, gen_time = %tsr_info.gen_time, "checkpoint anchored");
    Ok(Some(AnchorReceipt {
        tree_size,
        root_hash,
        gen_time: tsr_info.gen_time,
    }))
}

fn needs_anchor() -> Result<bool> {
    let client = IpcClient::from_env(None);
    let (tree_size, _) = match fetch_checkpoint(&client) {
        Ok(v) => v,
        Err(_) => return Ok(false),
    };
    let tsr_path = config::tsr_path(tree_size).context("failed to determine TSR path")?;
    Ok(!tsr_path.exists())
}

// ─── Checkpoint fetch ──────────────────────────────────────────────

fn fetch_checkpoint(client: &IpcClient) -> Result<(i64, String)> {
    let req = RequestEnvelope {
        request_id: ipc_client::new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "audit_checkpoint" }),
    };
    let resp = client.send(&req)?;
    if resp.status != "ok" {
        anyhow::bail!("kernel returned error: {}", resp.payload);
    }
    let tree_size = resp
        .payload
        .get("tree_size")
        .and_then(|v| v.as_i64())
        .context("missing tree_size in checkpoint response")?;
    let root_hash = resp
        .payload
        .get("root_hash")
        .and_then(|v| v.as_str())
        .context("missing root_hash in checkpoint response")?
        .to_string();
    Ok((tree_size, root_hash))
}

// ─── RFC 3161 TimeStampReq ─────────────────────────────────────────

fn build_timestamp_req(sha256_hash: &[u8; 32]) -> Result<Vec<u8>> {
    use const_oid::db::rfc5912::ID_SHA_256;
    use der::Encode;
    use spki::AlgorithmIdentifierOwned;
    use x509_tsp::{MessageImprint, TimeStampReq, TspVersion};

    let imprint = MessageImprint {
        hash_algorithm: AlgorithmIdentifierOwned {
            oid: ID_SHA_256,
            parameters: Some(der::asn1::AnyRef::NULL.into()),
        },
        hashed_message: der::asn1::OctetString::new(sha256_hash)?,
    };

    let req = TimeStampReq {
        version: TspVersion::V1,
        message_imprint: imprint,
        req_policy: None,
        nonce: None,
        cert_req: true,
        extensions: None,
    };

    req.to_der().context("failed to DER-encode TimeStampReq")
}

// ─── HTTP TSA ──────────────────────────────────────────────────────

fn http_post_tsa(url: &str, tsq: &[u8], timeout_secs: u64) -> Result<Vec<u8>> {
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(timeout_secs))
        .build();

    let resp = agent
        .post(url)
        .set("Content-Type", "application/timestamp-query")
        .send_bytes(tsq)
        .context("TSA HTTP request failed")?;

    let mut body = Vec::new();
    resp.into_reader()
        .read_to_end(&mut body)
        .context("failed to read TSA response body")?;
    Ok(body)
}

// ─── Semantic rate limiting ────────────────────────────────────────
//
// Primary: anchor only when tree has grown (new events since last anchor).
// Fallback: clock-based interval when tree_size tracking is unavailable.
// min_interval_secs=0 disables all rate limiting (CI burst mode).

/// Decide whether to anchor based on tree growth.
///
/// - burst mode (min_interval_secs=0): always anchor
/// - TSR file missing: always anchor (recovery)
/// - tree hasn't grown since last anchor: skip
/// - first run (no last_tree_size file): anchor
fn should_anchor(current_tree_size: i64, tsr_exists: bool, _config: &Config) -> Result<bool> {
    if _config.tsa.min_interval_secs == 0 {
        return Ok(true);
    }

    if !tsr_exists {
        return Ok(true);
    }

    match load_last_anchored_tree_size() {
        Some(last_size) => Ok(current_tree_size > last_size),
        None => Ok(true), // first run — anchor
    }
}

fn load_last_anchored_tree_size() -> Option<i64> {
    let path = config::tsa_state_dir()?.join("last_tree_size");
    let text = fs::read_to_string(path).ok()?;
    text.trim().parse().ok()
}

fn save_last_anchored_tree_size(tree_size: i64) -> Result<()> {
    let Some(dir) = config::tsa_state_dir() else {
        return Ok(());
    };
    fs::create_dir_all(&dir)?;
    fs::write(dir.join("last_tree_size"), tree_size.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use der::Decode;

    #[test]
    fn build_timestamp_req_produces_valid_der() {
        let hash = [0xab; 32];
        let tsq = build_timestamp_req(&hash).unwrap();
        assert_eq!(tsq[0], 0x30);
        let parsed = x509_tsp::TimeStampReq::from_der(&tsq).unwrap();
        assert_eq!(parsed.version, x509_tsp::TspVersion::V1);
        assert!(parsed.cert_req);
        assert_eq!(parsed.message_imprint.hashed_message.as_bytes(), &hash);
    }

    #[test]
    fn build_timestamp_req_different_hashes() {
        let r1 = build_timestamp_req(&[0x00; 32]).unwrap();
        let r2 = build_timestamp_req(&[0xff; 32]).unwrap();
        assert_ne!(r1, r2);
    }

    #[test]
    fn should_anchor_burst_mode_always_true() {
        let config = Config {
            tsa: config::TsaConfig {
                min_interval_secs: 0,
                ..Default::default()
            },
        };
        // burst mode (min_interval=0) always allows anchoring
        assert!(should_anchor(1, false, &config).unwrap());
        assert!(should_anchor(100, true, &config).unwrap());
    }
}
