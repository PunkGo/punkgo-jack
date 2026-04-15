//! Base64 + ASCII + regex pipeline for thinking signature parsing.
//!
//! See `super` module docs for rationale.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Structured metadata extracted from a single thinking signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignatureMeta {
    /// Decoded byte count. Zero if decode failed.
    pub bytes: usize,
    /// First model variant string matched in priority order
    /// (numbat → claude-opus → claude-haiku → claude-sonnet).
    #[serde(default)]
    pub model_variant: Option<String>,
    /// All deduplicated printable ASCII runs of length ≥ 4 extracted from the
    /// decoded bytes. Preserved for future parser upgrades / forensic analysis.
    #[serde(default)]
    pub extracted_strings: Vec<String>,
}

/// Minimum length of a printable ASCII run to keep as a candidate.
const MIN_RUN_LEN: usize = 4;

/// Priority-ordered regex patterns. The first pattern that matches any
/// extracted string (or the fallback scan) wins.
static PRIORITY_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        // Priority 1: numbat-v<digit>-... (e.g. numbat-v6-efforts-10-20-40-ab-prod).
        // The real numbat suffix always ends with letters (`-ab-prod`), so we
        // anchor the trailing character class to end at a letter. This lets us
        // cleanly strip the trailing protobuf field-tag byte (e.g. `0x38 = '8'`)
        // that commonly appears right after the string in the framing.
        Regex::new(r"numbat-v\d+(?:[a-z\d\-]*[a-z])?").unwrap(),
        // Priority 2–4: claude-<family>-<major>-<minor>(-<YYYYMMDD>)?
        // Canonical shapes observed in the wild:
        //   claude-opus-4-6          (major.minor)
        //   claude-haiku-4-5-20251001 (major.minor-8digitDate)
        // Dogfood scan (2026-04-15) showed a greedy pattern over-captures the
        // trailing protobuf field-tag byte (`0x38 = '8'`) producing bogus
        // `claude-opus-4-68` / `claude-haiku-4-5-202510018` variants.
        //
        // Fix: lock major/minor to exactly one digit each, then optionally
        // match exactly 8 digits for an ISO date suffix. Current Claude model
        // naming is single-digit for both major and minor, so this is exact.
        // If Anthropic ever ships e.g. `claude-opus-4-10`, this regex must be
        // updated — that's an explicit v0.6.0 trade-off favoring correctness
        // on the current data over forward-compat with speculative schemas.
        Regex::new(r"claude-opus-\d-\d(?:-\d{8})?").unwrap(),
        Regex::new(r"claude-haiku-\d-\d(?:-\d{8})?").unwrap(),
        Regex::new(r"claude-sonnet-\d-\d(?:-\d{8})?").unwrap(),
    ]
});

/// Parse a base64-encoded thinking signature. Never panics; invalid base64 or
/// empty input returns a `SignatureMeta` with `bytes = 0`.
pub fn parse_thinking_signature(sig_b64: &str) -> anyhow::Result<SignatureMeta> {
    if sig_b64.is_empty() {
        return Ok(SignatureMeta {
            bytes: 0,
            model_variant: None,
            extracted_strings: Vec::new(),
        });
    }

    // Try standard base64; if that fails, try with padding normalization.
    let decoded = match STANDARD.decode(sig_b64) {
        Ok(bytes) => bytes,
        Err(_) => {
            // Try trimming and re-padding. Signatures in Claude jsonl are
            // typically already well-padded but be defensive.
            let trimmed = sig_b64.trim().trim_end_matches('=');
            let rem = trimmed.len() % 4;
            let padded = if rem == 0 {
                trimmed.to_string()
            } else {
                let mut s = trimmed.to_string();
                for _ in 0..(4 - rem) {
                    s.push('=');
                }
                s
            };
            match STANDARD.decode(&padded) {
                Ok(bytes) => bytes,
                Err(_) => {
                    return Ok(SignatureMeta {
                        bytes: 0,
                        model_variant: None,
                        extracted_strings: Vec::new(),
                    });
                }
            }
        }
    };

    let bytes = decoded.len();

    // Primary extraction: contiguous printable ASCII runs ≥ MIN_RUN_LEN chars.
    let runs = extract_ascii_runs(&decoded, MIN_RUN_LEN);

    // Dedup preserving insertion order.
    let mut seen = std::collections::HashSet::new();
    let mut extracted_strings: Vec<String> = Vec::with_capacity(runs.len());
    for r in &runs {
        if seen.insert(r.clone()) {
            extracted_strings.push(r.clone());
        }
    }

    // Match: try each priority regex against each extracted run. Use
    // `.find()` so framing garbage attached to the run (e.g. `2"numbat-...8`)
    // still yields a clean match.
    //
    // Historical note (2026-04-15 fake-test review): an earlier version had
    // a "fallback" pass that replaced every non-printable byte with an ASCII
    // space and re-ran the regex on the mutated blob, claiming it recovered
    // variants that the primary run extractor missed. That pass was proven
    // dead for the current regex family: none of the priority patterns can
    // match a string containing an interior space, so the fallback could
    // never produce a match the primary path didn't already find.
    //
    // Two tests in this module (`test_parse_fallback_split_on_nonprintable`
    // and `test_parse_fallback_actually_helps`) self-documented the dead-
    // code status in long comment blocks and ended up only asserting "no
    // panic + correct byte count" — classic fake-test theater. Both the
    // fallback code and its fake tests were removed rather than kept as
    // "defensive dead code" per the project's no-debt rule.
    let mut model_variant: Option<String> = None;
    'outer: for pat in PRIORITY_PATTERNS.iter() {
        for s in &extracted_strings {
            if let Some(m) = pat.find(s) {
                model_variant = Some(m.as_str().to_string());
                break 'outer;
            }
        }
    }

    Ok(SignatureMeta {
        bytes,
        model_variant,
        extracted_strings,
    })
}

/// Extract all contiguous runs of printable ASCII bytes (0x20..=0x7e) of
/// length ≥ `min_len` from `data`.
fn extract_ascii_runs(data: &[u8], min_len: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    for &b in data {
        if (0x20..=0x7e).contains(&b) {
            current.push(b);
        } else if current.len() >= min_len {
            // SAFETY: all bytes in [0x20, 0x7e] are valid UTF-8.
            if let Ok(s) = std::str::from_utf8(&current) {
                out.push(s.to_string());
            }
            current.clear();
        } else {
            current.clear();
        }
    }
    if current.len() >= min_len {
        if let Ok(s) = std::str::from_utf8(&current) {
            out.push(s.to_string());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

    fn encode(bytes: &[u8]) -> String {
        STANDARD.encode(bytes)
    }

    #[test]
    fn test_parse_numbat_variant() {
        // Surround the numbat string with protobuf-style framing + binary junk.
        let mut payload: Vec<u8> = vec![0x01, 0x02, 0xff, 0xfe];
        payload.extend_from_slice(b"2\"numbat-v6-efforts-10-20-40-ab-prod8");
        payload.extend_from_slice(&[0x00, 0xab, 0xcd]);
        let b64 = encode(&payload);

        let meta = parse_thinking_signature(&b64).unwrap();
        assert!(meta.bytes > 0);
        assert_eq!(
            meta.model_variant.as_deref(),
            Some("numbat-v6-efforts-10-20-40-ab-prod")
        );
    }

    #[test]
    fn test_parse_claude_opus_variant() {
        let mut payload: Vec<u8> = vec![0x00, 0xff];
        payload.extend_from_slice(b"\x10claude-opus-4-68\x12something");
        payload.push(0x00);
        let b64 = encode(&payload);

        let meta = parse_thinking_signature(&b64).unwrap();
        // The payload embeds `claude-opus-4-68` where the trailing `8` is a
        // protobuf field-tag byte, not part of the real variant name. The
        // tightened regex `claude-opus-\d-\d(?:-\d{8})?` stops after the
        // single-digit minor version, producing the canonical `claude-opus-4-6`.
        // This is the post-dogfood behavior (2026-04-15): before the fix,
        // the regex over-captured and produced bogus `claude-opus-4-68`.
        let mv = meta.model_variant.expect("should match claude-opus");
        assert_eq!(
            mv, "claude-opus-4-6",
            "trailing framing byte must be stripped"
        );
    }

    #[test]
    fn test_parse_claude_haiku_variant() {
        // Fake-test review fix (2026-04-15): previous version used
        // `starts_with("claude-haiku-4-5-20251001")` which allowed any
        // trailing over-capture (e.g. `claude-haiku-4-5-202510012`) to
        // pass. Tightened to exact equality so an over-capturing regex
        // regression is caught immediately.
        let mut payload: Vec<u8> = vec![0xff, 0xfe, 0xfd];
        payload.extend_from_slice(b"2\"claude-haiku-4-5-20251001\x00end");
        let b64 = encode(&payload);

        let meta = parse_thinking_signature(&b64).unwrap();
        let mv = meta.model_variant.expect("should match claude-haiku");
        assert_eq!(
            mv, "claude-haiku-4-5-20251001",
            "trailing framing byte must be stripped"
        );
    }

    #[test]
    fn test_parse_priority_order() {
        // Contains both numbat and claude-opus — numbat (priority 1) wins.
        let mut payload: Vec<u8> = vec![0x01];
        payload.extend_from_slice(b"claude-opus-4-6\x00\x01\x02");
        payload.extend_from_slice(b"numbat-v6-efforts-10-20-40-ab-prod\x00");
        let b64 = encode(&payload);

        let meta = parse_thinking_signature(&b64).unwrap();
        assert_eq!(
            meta.model_variant.as_deref(),
            Some("numbat-v6-efforts-10-20-40-ab-prod")
        );
    }

    #[test]
    fn test_parse_no_match() {
        // Random binary with no recognizable model strings.
        let payload: Vec<u8> = vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0xff, 0xfe, 0xfd, 0xfc];
        let b64 = encode(&payload);
        let meta = parse_thinking_signature(&b64).unwrap();
        assert_eq!(meta.model_variant, None);
        // bytes should equal decoded length
        assert_eq!(meta.bytes, payload.len());
    }

    // NOTE (2026-04-15 fake-test review): two tests named
    // `test_parse_fallback_split_on_nonprintable` and
    // `test_parse_fallback_actually_helps` previously lived here. Both
    // self-documented in long comment blocks that they could not
    // actually exercise the fallback path against the current regex
    // family, and ended up only asserting "no panic + correct byte
    // count". They were deleted alongside the dead fallback code they
    // claimed to cover — the fallback replaced non-printable bytes
    // with ASCII space, and none of the priority patterns can match
    // a string with an interior space, so the fallback could never
    // produce a match that the primary run extractor did not already
    // find. Keeping fake tests around to "document" dead code is
    // worse than deleting both: the tests give false confidence and
    // the code gives false optionality.

    #[test]
    fn test_parse_invalid_base64() {
        // Garbage that can't be base64-decoded.
        let meta = parse_thinking_signature("!!!not-valid-base64!!!").unwrap();
        assert_eq!(meta.bytes, 0);
        assert_eq!(meta.model_variant, None);
        assert!(meta.extracted_strings.is_empty());
    }

    #[test]
    fn test_parse_empty_string() {
        let meta = parse_thinking_signature("").unwrap();
        assert_eq!(meta.bytes, 0);
        assert_eq!(meta.model_variant, None);
        assert!(meta.extracted_strings.is_empty());
    }

    #[test]
    fn test_extract_ascii_runs_min_len() {
        // Runs < 4 chars must be filtered out.
        let data = b"ab\x00cdef\x00xy\x00longer_run";
        let runs = extract_ascii_runs(data, 4);
        assert_eq!(runs, vec!["cdef".to_string(), "longer_run".to_string()]);
    }

    #[test]
    fn test_extracted_strings_deduped() {
        // Same string appears twice — should be deduped.
        let mut payload: Vec<u8> = vec![];
        payload.extend_from_slice(b"hello");
        payload.push(0x00);
        payload.extend_from_slice(b"hello");
        payload.push(0x00);
        payload.extend_from_slice(b"world");
        let b64 = encode(&payload);
        let meta = parse_thinking_signature(&b64).unwrap();
        assert_eq!(meta.extracted_strings.len(), 2);
        assert!(meta.extracted_strings.contains(&"hello".to_string()));
        assert!(meta.extracted_strings.contains(&"world".to_string()));
    }
}
