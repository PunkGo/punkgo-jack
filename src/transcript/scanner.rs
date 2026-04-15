//! Streaming jsonl transcript scanner.
//!
//! See `super` module docs for the privacy invariant. TL;DR: only metadata
//! flows through `TurnRecord`; never prompt/response/tool/thinking text.

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::signature::parse_thinking_signature;

/// A single assistant-or-user turn extracted from the transcript.
///
/// **Privacy**: No field in this struct ever contains raw user or assistant
/// text. `content_blocks` holds byte lengths and (for thinking blocks)
/// opaque base64 signatures only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnRecord {
    pub turn_uuid: String,
    #[serde(default)]
    pub parent_turn_uuid: Option<String>,
    pub session_id: String,
    /// `"user"` or `"assistant"`.
    pub role: String,
    /// ISO 8601 timestamp as-is from jsonl.
    pub timestamp: String,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub git_branch: Option<String>,
    #[serde(default)]
    pub is_sidechain: bool,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub claude_code_version: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    /// `message.model` (e.g. `claude-opus-4-6`).
    #[serde(default)]
    pub model: Option<String>,
    /// Model variant extracted from the first thinking signature, if any
    /// (e.g. `numbat-v6-efforts-10-20-40-ab-prod`).
    #[serde(default)]
    pub model_variant: Option<String>,
    #[serde(default)]
    pub usage: Option<UsageRecord>,
    #[serde(default)]
    pub content_blocks: Vec<ContentBlockRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UsageRecord {
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    pub cache_read_input_tokens: Option<u64>,
}

/// Per-block metadata. Never contains raw block content.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ContentBlockRecord {
    Text {
        byte_len: usize,
        #[serde(default)]
        content_hash: Option<String>,
    },
    ToolUse {
        name: String,
        byte_len: usize,
        #[serde(default)]
        content_hash: Option<String>,
    },
    ToolResult {
        byte_len: usize,
        #[serde(default)]
        content_hash: Option<String>,
        is_error: bool,
    },
    Thinking {
        thinking_byte_len: usize,
        /// Opaque base64 string from jsonl — does NOT contain user content.
        signature_b64: String,
        /// Length of base64-decoded signature in bytes.
        signature_bytes: usize,
    },
}

pub struct TranscriptScanner;

impl TranscriptScanner {
    /// Full scan of one .jsonl file. Streams line-by-line; memory-bounded to
    /// one line at a time. Returns records for assistant and user turns only
    /// (skips system entries, attachments, file-history-snapshots, etc.).
    ///
    /// If the first data line fails to parse, the entire file is skipped
    /// (heuristic: probably corrupted from an incomplete write) and a
    /// `tracing::warn!` is emitted.
    pub fn scan_file(path: &Path) -> Result<Vec<TurnRecord>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(scan_reader(reader, path, None))
    }

    /// Incremental scan starting at `from_byte_offset`. Returns (new turns,
    /// byte offset at which scanning stopped).
    ///
    /// If `from_byte_offset` is past EOF, returns an empty Vec and the
    /// current file length.
    ///
    /// The offset is advanced to a line boundary: if the requested offset
    /// lands mid-line, we drop that partial line and resume on the next
    /// newline. Callers should persist the returned offset for subsequent
    /// incremental scans.
    pub fn scan_incremental(path: &Path, from_byte_offset: u64) -> Result<(Vec<TurnRecord>, u64)> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        if from_byte_offset >= file_len {
            return Ok((Vec::new(), file_len));
        }

        // Determine whether the offset lands at a line boundary or mid-line.
        // If the byte immediately before `from_byte_offset` is a newline (or
        // offset == 0), the offset is at a clean line boundary and we must
        // NOT drop the first line. Otherwise, treat the first (partial) line
        // as torn-write residue and skip it.
        let skip_first_partial = if from_byte_offset == 0 {
            false
        } else {
            use std::io::Read;
            file.seek(SeekFrom::Start(from_byte_offset - 1))?;
            let mut prev = [0u8; 1];
            file.read_exact(&mut prev)?;
            prev[0] != b'\n'
        };

        file.seek(SeekFrom::Start(from_byte_offset))?;
        let reader = BufReader::new(file);

        let records = scan_reader(reader, path, Some(skip_first_partial));

        // Re-open to compute end offset (we consumed the reader).
        let end_offset = File::open(path)?.metadata()?.len();

        Ok((records, end_offset))
    }
}

/// Core streaming loop. Parses each line, skips malformed ones (with warning),
/// and collects `TurnRecord`s.
///
/// `skip_first_partial`: if `Some(true)`, the first line read is dropped
/// (used by incremental scan when the start offset may land mid-line).
fn scan_reader<R: BufRead>(
    reader: R,
    path: &Path,
    skip_first_partial: Option<bool>,
) -> Vec<TurnRecord> {
    let mut out: Vec<TurnRecord> = Vec::new();
    let mut first_data_line = true;
    let mut dropped_partial = skip_first_partial.unwrap_or(false);

    for (line_no, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    line = line_no + 1,
                    error = %e,
                    "transcript read error, skipping remainder"
                );
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        // Drop the first (possibly partial) line if this is an incremental scan.
        if dropped_partial {
            dropped_partial = false;
            first_data_line = false; // downgrade; corrupt-first-line heuristic N/A for incr
            continue;
        }

        let value: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                if first_data_line {
                    // Heuristic: corrupted first line → skip entire file.
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "first data line unparseable, skipping entire transcript file"
                    );
                    return Vec::new();
                }
                tracing::warn!(
                    path = %path.display(),
                    line = line_no + 1,
                    error = %e,
                    "malformed jsonl line, skipping"
                );
                continue;
            }
        };

        first_data_line = false;

        if let Some(record) = build_turn_record(&value) {
            out.push(record);
        }
    }

    out
}

/// Attempt to build a `TurnRecord` from a parsed line. Returns `None` for
/// non-turn entries (attachments, permission-mode, file-history-snapshot,
/// tool_result-only user entries, etc.).
fn build_turn_record(obj: &Value) -> Option<TurnRecord> {
    let entry_type = obj.get("type")?.as_str()?;
    if entry_type != "assistant" && entry_type != "user" {
        return None;
    }

    // Must have a message object with content array to be a real turn.
    let message = obj.get("message")?;
    if !message.is_object() {
        return None;
    }

    // Pre-filter: skip user entries whose content is only tool_result blocks.
    // These are tool results that the kernel already captures via hook events;
    // they carry no model variant data. Per plan §3 ambiguity resolution:
    // we skip them entirely.
    if entry_type == "user" {
        if let Some(content) = message.get("content").and_then(|c| c.as_array()) {
            let only_tool_results = !content.is_empty()
                && content
                    .iter()
                    .all(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_result"));
            if only_tool_results {
                return None;
            }
        }
    }

    let turn_uuid = obj.get("uuid")?.as_str()?.to_string();
    let session_id = obj.get("sessionId")?.as_str()?.to_string();
    let timestamp = obj.get("timestamp")?.as_str()?.to_string();

    let role = message
        .get("role")
        .and_then(|r| r.as_str())
        .unwrap_or(entry_type)
        .to_string();

    let parent_turn_uuid = obj
        .get("parentUuid")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let cwd = obj.get("cwd").and_then(|v| v.as_str()).map(String::from);
    let git_branch = obj
        .get("gitBranch")
        .and_then(|v| v.as_str())
        .map(String::from);
    let is_sidechain = obj
        .get("isSidechain")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let slug = obj.get("slug").and_then(|v| v.as_str()).map(String::from);
    let claude_code_version = obj
        .get("version")
        .and_then(|v| v.as_str())
        .map(String::from);
    let request_id = obj
        .get("requestId")
        .and_then(|v| v.as_str())
        .map(String::from);

    let message_id = message.get("id").and_then(|v| v.as_str()).map(String::from);
    let model = message
        .get("model")
        .and_then(|v| v.as_str())
        .map(String::from);

    let usage = message.get("usage").and_then(parse_usage);

    let (content_blocks, model_variant) = parse_content_blocks(message.get("content"));

    Some(TurnRecord {
        turn_uuid,
        parent_turn_uuid,
        session_id,
        role,
        timestamp,
        cwd,
        git_branch,
        is_sidechain,
        slug,
        claude_code_version,
        request_id,
        message_id,
        model,
        model_variant,
        usage,
        content_blocks,
    })
}

fn parse_usage(v: &Value) -> Option<UsageRecord> {
    let obj = v.as_object()?;
    let g = |k: &str| obj.get(k).and_then(|x| x.as_u64());
    Some(UsageRecord {
        input_tokens: g("input_tokens"),
        output_tokens: g("output_tokens"),
        cache_creation_input_tokens: g("cache_creation_input_tokens"),
        cache_read_input_tokens: g("cache_read_input_tokens"),
    })
}

/// Parse a `message.content` array into ContentBlockRecord list + extracted
/// model variant (from first thinking block).
///
/// Privacy: this function MUST NOT retain any raw block content in the
/// returned records. Only byte lengths and opaque signature b64 are stored.
fn parse_content_blocks(content: Option<&Value>) -> (Vec<ContentBlockRecord>, Option<String>) {
    let Some(arr) = content.and_then(|c| c.as_array()) else {
        return (Vec::new(), None);
    };

    let mut blocks = Vec::with_capacity(arr.len());
    let mut model_variant: Option<String> = None;

    for block in arr {
        let Some(btype) = block.get("type").and_then(|t| t.as_str()) else {
            continue;
        };

        match btype {
            "text" => {
                let byte_len = block
                    .get("text")
                    .and_then(|t| t.as_str())
                    .map(|s| s.len())
                    .unwrap_or(0);
                blocks.push(ContentBlockRecord::Text {
                    byte_len,
                    content_hash: None,
                });
            }
            "tool_use" => {
                let name = block
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_string();
                // Byte length = serialized length of the `input` object.
                let byte_len = block
                    .get("input")
                    .map(|v| serde_json::to_string(v).map(|s| s.len()).unwrap_or(0))
                    .unwrap_or(0);
                blocks.push(ContentBlockRecord::ToolUse {
                    name,
                    byte_len,
                    content_hash: None,
                });
            }
            "tool_result" => {
                // Rare in assistant entries but defend against it anyway.
                let byte_len = block
                    .get("content")
                    .map(|v| serde_json::to_string(v).map(|s| s.len()).unwrap_or(0))
                    .unwrap_or(0);
                let is_error = block
                    .get("is_error")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                blocks.push(ContentBlockRecord::ToolResult {
                    byte_len,
                    content_hash: None,
                    is_error,
                });
            }
            "thinking" => {
                let thinking_byte_len = block
                    .get("thinking")
                    .and_then(|t| t.as_str())
                    .map(|s| s.len())
                    .unwrap_or(0);
                let signature_b64 = block
                    .get("signature")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();

                let (sig_bytes, variant_from_sig) = if !signature_b64.is_empty() {
                    match parse_thinking_signature(&signature_b64) {
                        Ok(meta) => (meta.bytes, meta.model_variant),
                        Err(_) => (0, None),
                    }
                } else {
                    (0, None)
                };

                if model_variant.is_none() {
                    if let Some(v) = variant_from_sig {
                        model_variant = Some(v);
                    }
                }

                blocks.push(ContentBlockRecord::Thinking {
                    thinking_byte_len,
                    signature_b64,
                    signature_bytes: sig_bytes,
                });
            }
            _ => {
                // Unknown block type — skip silently.
            }
        }
    }

    (blocks, model_variant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use tempfile::NamedTempFile;

    fn b64_encode(bytes: &[u8]) -> String {
        STANDARD.encode(bytes)
    }

    /// Build a synthetic assistant-turn jsonl line with the given inner blocks.
    fn assistant_line(uuid: &str, blocks_json: Value) -> String {
        let obj = serde_json::json!({
            "type": "assistant",
            "uuid": uuid,
            "parentUuid": "parent-uuid-xyz",
            "sessionId": "test-session-abc",
            "timestamp": "2026-04-15T12:00:00.000Z",
            "cwd": "/tmp/work",
            "gitBranch": "main",
            "isSidechain": false,
            "slug": "test-slug",
            "version": "2.0.0",
            "requestId": "req_abc123",
            "message": {
                "id": "msg_xyz",
                "role": "assistant",
                "model": "claude-opus-4-6",
                "content": blocks_json,
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 200,
                    "cache_creation_input_tokens": 10,
                    "cache_read_input_tokens": 20
                }
            }
        });
        serde_json::to_string(&obj).unwrap()
    }

    fn user_line(uuid: &str, text: &str) -> String {
        let obj = serde_json::json!({
            "type": "user",
            "uuid": uuid,
            "parentUuid": null,
            "sessionId": "test-session-abc",
            "timestamp": "2026-04-15T12:00:01.000Z",
            "cwd": "/tmp/work",
            "gitBranch": "main",
            "isSidechain": false,
            "version": "2.0.0",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": text}]
            }
        });
        serde_json::to_string(&obj).unwrap()
    }

    fn write_tmp(lines: &[&str]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_scan_single_assistant_turn() {
        let sig_bytes = b"\x01\x02numbat-v6-efforts-10-20-40-ab-prod\x08\x00";
        let sig_b64 = b64_encode(sig_bytes);
        let line = assistant_line(
            "uuid-1",
            serde_json::json!([
                { "type": "text", "text": "Hello world response" },
                { "type": "thinking", "thinking": "", "signature": sig_b64 }
            ]),
        );
        let f = write_tmp(&[&line]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.turn_uuid, "uuid-1");
        assert_eq!(r.session_id, "test-session-abc");
        assert_eq!(r.role, "assistant");
        assert_eq!(r.model.as_deref(), Some("claude-opus-4-6"));
        assert_eq!(
            r.model_variant.as_deref(),
            Some("numbat-v6-efforts-10-20-40-ab-prod")
        );
        assert_eq!(r.content_blocks.len(), 2);

        // Privacy: text block has byte_len but no content
        match &r.content_blocks[0] {
            ContentBlockRecord::Text {
                byte_len,
                content_hash,
            } => {
                assert_eq!(*byte_len, "Hello world response".len());
                assert!(content_hash.is_none());
            }
            other => panic!("expected text block, got {:?}", other),
        }

        // Thinking block has signature (opaque) + bytes, no user content
        match &r.content_blocks[1] {
            ContentBlockRecord::Thinking {
                thinking_byte_len,
                signature_b64: sb,
                signature_bytes,
            } => {
                assert_eq!(*thinking_byte_len, 0);
                assert_eq!(sb, &sig_b64);
                assert!(*signature_bytes > 0);
            }
            other => panic!("expected thinking block, got {:?}", other),
        }

        // Usage populated
        let u = r.usage.as_ref().unwrap();
        assert_eq!(u.input_tokens, Some(100));
        assert_eq!(u.output_tokens, Some(200));
        assert_eq!(u.cache_creation_input_tokens, Some(10));
        assert_eq!(u.cache_read_input_tokens, Some(20));
    }

    #[test]
    fn test_scan_skip_malformed_lines() {
        let good1 = assistant_line(
            "uuid-1",
            serde_json::json!([{ "type": "text", "text": "hi" }]),
        );
        let bad = "{not valid json,,,";
        let good2 = assistant_line(
            "uuid-2",
            serde_json::json!([{ "type": "text", "text": "hey" }]),
        );
        let f = write_tmp(&[&good1, bad, &good2]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].turn_uuid, "uuid-1");
        assert_eq!(records[1].turn_uuid, "uuid-2");
    }

    #[test]
    fn test_scan_sidechain_flag() {
        let obj = serde_json::json!({
            "type": "assistant",
            "uuid": "uuid-sc",
            "parentUuid": "parent-uuid",
            "sessionId": "sess-1",
            "timestamp": "2026-04-15T12:00:00Z",
            "isSidechain": true,
            "message": {
                "role": "assistant",
                "model": "claude-opus-4-6",
                "content": [{ "type": "text", "text": "sub agent msg" }]
            }
        });
        let line = serde_json::to_string(&obj).unwrap();
        let f = write_tmp(&[&line]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].is_sidechain);
    }

    #[test]
    fn test_scan_incremental_from_offset() {
        // Write two lines, full scan, capture offset; append third line;
        // incremental scan from offset should return only the third.
        let line1 = assistant_line(
            "uuid-1",
            serde_json::json!([{ "type": "text", "text": "one" }]),
        );
        let line2 = assistant_line(
            "uuid-2",
            serde_json::json!([{ "type": "text", "text": "two" }]),
        );
        let f = write_tmp(&[&line1, &line2]);

        let first_scan = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(first_scan.len(), 2);

        // Capture current size as our offset.
        let offset_after_two = std::fs::metadata(f.path()).unwrap().len();

        // Append third line.
        let line3 = assistant_line(
            "uuid-3",
            serde_json::json!([{ "type": "text", "text": "three" }]),
        );
        {
            use std::fs::OpenOptions;
            let mut af = OpenOptions::new().append(true).open(f.path()).unwrap();
            writeln!(af, "{}", line3).unwrap();
            af.flush().unwrap();
        }

        let (new_records, new_offset) =
            TranscriptScanner::scan_incremental(f.path(), offset_after_two).unwrap();
        assert_eq!(new_records.len(), 1, "expected only the new record");
        assert_eq!(new_records[0].turn_uuid, "uuid-3");
        assert!(new_offset >= offset_after_two);

        // Scanning at EOF should return empty.
        let file_len = std::fs::metadata(f.path()).unwrap().len();
        let (empty, off) = TranscriptScanner::scan_incremental(f.path(), file_len).unwrap();
        assert!(empty.is_empty());
        assert_eq!(off, file_len);
    }

    #[test]
    fn test_scan_thinking_signature_passthrough() {
        // Construct a signature whose decoded bytes contain the numbat variant.
        let sig_bytes = b"junk\x01\x022\"numbat-v6-efforts-10-20-40-ab-prod8\x00more";
        let sig_b64 = b64_encode(sig_bytes);
        let line = assistant_line(
            "uuid-sig",
            serde_json::json!([
                { "type": "thinking", "thinking": "", "signature": sig_b64 }
            ]),
        );
        let f = write_tmp(&[&line]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].model_variant.as_deref(),
            Some("numbat-v6-efforts-10-20-40-ab-prod")
        );
    }

    /// Privacy guarantee test: no substring of the original prompt text must
    /// appear in the serialized `TurnRecord`. Mandatory per spec.
    #[test]
    fn test_scan_no_text_leak() {
        // Build a unique 10 KB text payload — must not appear in serialized record.
        let needle = "SECRET_PROMPT_TOKEN_d7f9c1e3";
        let mut big = String::with_capacity(10 * 1024);
        while big.len() < 10 * 1024 {
            big.push_str(needle);
            big.push(' ');
        }
        let line = user_line("uuid-priv", &big);
        let f = write_tmp(&[&line]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(records.len(), 1);

        // Serialize and assert the needle is not anywhere in the output.
        let json = serde_json::to_string(&records[0]).unwrap();
        assert!(
            !json.contains(needle),
            "PRIVACY VIOLATION: prompt text leaked into TurnRecord JSON"
        );

        // Positive check: byte_len was recorded.
        match &records[0].content_blocks[0] {
            ContentBlockRecord::Text { byte_len, .. } => {
                assert_eq!(*byte_len, big.len());
            }
            other => panic!("expected text block, got {:?}", other),
        }
    }

    #[test]
    fn test_scan_skips_non_turn_entries() {
        // Mix a real assistant turn with a permission-mode entry and an
        // attachment entry. Only the assistant turn should come back.
        let perm = r#"{"type":"permission-mode","permissionMode":"normal","sessionId":"s1"}"#;
        let attach = r#"{"type":"attachment","attachment":{},"cwd":"/t","sessionId":"s1","timestamp":"2026-04-15T00:00:00Z","uuid":"u-att","parentUuid":null,"isSidechain":false,"version":"2","userType":"external","entrypoint":"cli"}"#;
        let assistant = assistant_line(
            "u-real",
            serde_json::json!([{ "type": "text", "text": "real content" }]),
        );
        let f = write_tmp(&[perm, attach, &assistant]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].turn_uuid, "u-real");
    }

    #[test]
    fn test_scan_skips_tool_result_only_user_entries() {
        // A user entry whose content is purely tool_result should be skipped
        // (these are captured via hook events, not needed in transcript turns).
        let obj = serde_json::json!({
            "type": "user",
            "uuid": "u-tr",
            "parentUuid": "parent",
            "sessionId": "sess",
            "timestamp": "2026-04-15T00:00:00Z",
            "isSidechain": false,
            "message": {
                "role": "user",
                "content": [
                    { "type": "tool_result", "tool_use_id": "toolu_1", "content": "ok", "is_error": false }
                ]
            }
        });
        let line = serde_json::to_string(&obj).unwrap();
        let real_user = user_line("u-real", "real user prompt");
        let f = write_tmp(&[&line, &real_user]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].turn_uuid, "u-real");
    }

    #[test]
    fn test_scan_corrupted_first_line_skips_file() {
        // If the very first data line is unparseable, the whole file is skipped.
        let bad = "{broken";
        let good = assistant_line(
            "u-good",
            serde_json::json!([{ "type": "text", "text": "ok" }]),
        );
        let f = write_tmp(&[bad, &good]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert!(records.is_empty(), "corrupted first line should skip file");
    }

    #[test]
    fn test_scan_tool_use_block_byte_len() {
        let line = assistant_line(
            "uuid-tool",
            serde_json::json!([
                {
                    "type": "tool_use",
                    "id": "toolu_1",
                    "name": "Bash",
                    "input": { "command": "ls -la" }
                }
            ]),
        );
        let f = write_tmp(&[&line]);
        let records = TranscriptScanner::scan_file(f.path()).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0].content_blocks[0] {
            ContentBlockRecord::ToolUse { name, byte_len, .. } => {
                assert_eq!(name, "Bash");
                assert!(*byte_len > 0);
            }
            other => panic!("expected tool_use block, got {:?}", other),
        }
    }

    /// Dogfood test: walk the real ~/.claude/projects/ archive and print stats.
    /// Gated behind `#[ignore]` so it doesn't run in CI and doesn't touch real
    /// user data on `cargo test`. Run manually with:
    ///   cargo test dogfood_real_jsonl_archive -- --ignored --nocapture
    /// Privacy: relies on the TurnRecord never storing text bodies (audited by
    /// test_scan_no_text_leak). This test prints only counts and identifiers.
    #[test]
    #[ignore]
    fn dogfood_real_jsonl_archive() {
        let root = crate::session::home_dir()
            .expect("home dir")
            .join(".claude")
            .join("projects");
        if !root.exists() {
            eprintln!("SKIP: {} does not exist", root.display());
            return;
        }

        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                    out.push(path);
                }
            }
        }

        let mut files = Vec::new();
        walk(&root, &mut files);
        eprintln!("found {} jsonl files", files.len());

        let mut total_turns = 0usize;
        let mut files_with_thinking = 0usize;
        let mut total_thinking_blocks = 0usize;
        let mut total_text_blocks = 0usize;
        let mut total_tool_use_blocks = 0usize;
        let mut signatures_parsed = 0usize;
        let mut model_variant_hits = 0usize;
        let mut variant_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let mut scan_failures = 0usize;

        for (i, path) in files.iter().enumerate() {
            let records = match TranscriptScanner::scan_file(path) {
                Ok(r) => r,
                Err(_) => {
                    scan_failures += 1;
                    continue;
                }
            };
            let mut file_thinking = 0usize;
            for r in &records {
                total_turns += 1;
                for block in &r.content_blocks {
                    match block {
                        ContentBlockRecord::Thinking {
                            signature_b64,
                            signature_bytes: _,
                            ..
                        } => {
                            total_thinking_blocks += 1;
                            file_thinking += 1;
                            if let Ok(meta) =
                                crate::signature::parse_thinking_signature(signature_b64)
                            {
                                signatures_parsed += 1;
                                if let Some(v) = meta.model_variant {
                                    model_variant_hits += 1;
                                    *variant_counts.entry(v).or_insert(0) += 1;
                                }
                            }
                        }
                        ContentBlockRecord::Text { .. } => total_text_blocks += 1,
                        ContentBlockRecord::ToolUse { .. } => total_tool_use_blocks += 1,
                        ContentBlockRecord::ToolResult { .. } => {}
                    }
                }
            }
            if file_thinking > 0 {
                files_with_thinking += 1;
            }
            if (i + 1) % 50 == 0 {
                eprintln!("  progress: {}/{} files", i + 1, files.len());
            }
        }

        eprintln!("\n== DOGFOOD SCAN RESULTS ==");
        eprintln!("files scanned        : {}", files.len() - scan_failures);
        eprintln!("  scan failures      : {scan_failures}");
        eprintln!("files with thinking  : {files_with_thinking}");
        eprintln!("total turns          : {total_turns}");
        eprintln!("  text blocks        : {total_text_blocks}");
        eprintln!("  tool_use blocks    : {total_tool_use_blocks}");
        eprintln!("  thinking blocks    : {total_thinking_blocks}");
        eprintln!("signatures parsed ok : {signatures_parsed}");
        eprintln!(
            "model_variant hits   : {model_variant_hits}  ({:.1}% of thinking)",
            if total_thinking_blocks > 0 {
                100.0 * model_variant_hits as f64 / total_thinking_blocks as f64
            } else {
                0.0
            }
        );
        eprintln!("variant breakdown:");
        let mut sorted: Vec<_> = variant_counts.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        for (v, c) in sorted {
            eprintln!("  {c:6}  {v}");
        }

        assert!(
            !files.is_empty(),
            "no jsonl files found — sanity check failed"
        );
    }
}
