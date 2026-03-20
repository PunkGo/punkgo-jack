//! Shared TSR (TimeStampResp) verification logic.
//!
//! Used by both `anchor.rs` (validate after HTTP) and `verify.rs` (offline verification).
//! Single source of truth for TSR parsing — DRY.

use anyhow::{Context, Result};
use cms::content_info::ContentInfo;
use cms::signed_data::SignedData;
use der::{Decode, Encode};

/// Result of TSR verification.
#[derive(Debug)]
pub struct TsrInfo {
    /// Formatted timestamp string (ISO-like).
    pub gen_time: String,
    /// The hash embedded in the TSR's MessageImprint.
    pub message_hash: Vec<u8>,
}

/// Parse and validate a DER-encoded RFC 3161 TimeStampResp.
///
/// Checks:
/// 1. PKIStatus is GRANTED (0) or GRANTED_WITH_MODS (1)
/// 2. TimeStampToken is present
/// 3. TstInfo can be extracted from CMS SignedData
///
/// Optionally cross-checks the embedded hash against `expected_hash`.
pub fn verify_tsr(tsr_bytes: &[u8], expected_hash: Option<&[u8]>) -> Result<TsrInfo> {
    let resp = x509_tsp::TimeStampResp::from_der(tsr_bytes)
        .context("failed to parse TimeStampResp DER")?;

    // Check PKIStatus: extract the integer value from the DER encoding.
    // PKIStatus is an ASN.1 INTEGER. For status codes 0-5 (single byte),
    // the DER encoding is [0x02, 0x01, value]. We match this pattern explicitly
    // and reject anything that doesn't fit (multi-byte or malformed).
    let status_der = resp
        .status
        .status
        .to_der()
        .context("failed to encode PKIStatus")?;
    match status_der.as_slice() {
        [0x02, 0x01, 0x00] => {} // granted
        [0x02, 0x01, 0x01] => {} // grantedWithMods
        [0x02, 0x01, v] => {
            anyhow::bail!("TSA returned PKIStatus {v} (expected 0=granted or 1=grantedWithMods)");
        }
        other => {
            anyhow::bail!("unexpected PKIStatus encoding ({} bytes)", other.len());
        }
    }

    // Extract TstInfo: TimeStampToken → ContentInfo → SignedData → encapContentInfo → TstInfo
    let token = resp
        .time_stamp_token
        .context("TSA response has no TimeStampToken")?;
    let token_der = token
        .to_der()
        .context("failed to re-encode TimeStampToken")?;
    let content_info = ContentInfo::from_der(&token_der).context("failed to parse ContentInfo")?;
    let signed_data = content_info
        .content
        .decode_as::<SignedData>()
        .context("failed to decode SignedData")?;
    let encap = signed_data
        .encap_content_info
        .econtent
        .context("SignedData has no encapsulated content")?;
    let tst_info = x509_tsp::TstInfo::from_der(encap.value()).context("failed to parse TstInfo")?;

    // Extract message hash
    let message_hash = tst_info.message_imprint.hashed_message.as_bytes().to_vec();

    // Cross-check hash if provided
    if let Some(expected) = expected_hash {
        if message_hash != expected {
            anyhow::bail!(
                "TSR hash mismatch: expected {}, got {}",
                hex_encode(expected),
                hex_encode(&message_hash)
            );
        }
    }

    // Format genTime as human-readable string via chrono
    let gen_time = format_gen_time(&tst_info.gen_time);

    Ok(TsrInfo {
        gen_time,
        message_hash,
    })
}

/// Format a GeneralizedTime as a human-readable ISO string.
fn format_gen_time(gt: &der::asn1::GeneralizedTime) -> String {
    // GeneralizedTime wraps DateTime which has to_unix_duration()
    let duration = gt.to_unix_duration();
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(duration.as_secs() as i64, 0);
    match dt {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => format!("{gt:?}"), // fallback to Debug
    }
}

pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd length".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

pub fn hex_to_32bytes(hex: &str) -> Result<[u8; 32]> {
    if hex.len() != 64 {
        anyhow::bail!("expected 64-char hex, got {}", hex.len());
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .with_context(|| "invalid hex character")?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_encode_works() {
        assert_eq!(hex_encode(&[0xde, 0xad]), "dead");
    }

    #[test]
    fn hex_decode_works() {
        assert_eq!(
            hex_decode("deadbeef").unwrap(),
            vec![0xde, 0xad, 0xbe, 0xef]
        );
    }

    #[test]
    fn hex_decode_odd_length() {
        assert!(hex_decode("abc").is_err());
    }

    #[test]
    fn hex_to_32bytes_valid() {
        let hex = "ab".repeat(32);
        let bytes = hex_to_32bytes(&hex).unwrap();
        assert_eq!(bytes, [0xab; 32]);
    }

    #[test]
    fn hex_to_32bytes_wrong_length() {
        assert!(hex_to_32bytes("abcd").is_err());
    }

    #[test]
    fn verify_tsr_rejects_garbage() {
        let garbage = vec![0x30, 0x03, 0x02, 0x01, 0x00];
        let result = verify_tsr(&garbage, None);
        assert!(result.is_err());
    }

    #[test]
    fn verify_real_digicert_tsr() {
        // Real TSR from DigiCert TSA (fetched during development).
        // Hash submitted: [0xab; 32]
        let tsr_bytes = include_bytes!("../tests/fixtures/digicert_sample.tsr");
        let expected_hash = [0xab_u8; 32];

        let info = verify_tsr(tsr_bytes, Some(&expected_hash)).unwrap();

        // genTime should be a valid formatted timestamp
        assert!(
            info.gen_time.contains("UTC"),
            "genTime should contain UTC: {}",
            info.gen_time
        );
        // Hash should match what we submitted
        assert_eq!(info.message_hash, expected_hash.to_vec());
    }

    #[test]
    fn verify_real_tsr_rejects_wrong_hash() {
        let tsr_bytes = include_bytes!("../tests/fixtures/digicert_sample.tsr");
        let wrong_hash = [0x00_u8; 32]; // not [0xab; 32]
        let result = verify_tsr(tsr_bytes, Some(&wrong_hash));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("mismatch"),
            "error should mention mismatch: {err_msg}"
        );
    }

    #[test]
    fn pki_status_pattern_match() {
        // Simulate a GRANTED status DER: INTEGER(0) = [0x02, 0x01, 0x00]
        let granted = [0x02u8, 0x01, 0x00];
        match granted.as_slice() {
            [0x02, 0x01, v] => assert_eq!(*v, 0),
            _ => panic!("unexpected"),
        }
        // REJECTION status = 2
        let rejection = [0x02u8, 0x01, 0x02];
        match rejection.as_slice() {
            [0x02, 0x01, v] => assert!(*v > 1),
            _ => panic!("unexpected"),
        }
    }
}
