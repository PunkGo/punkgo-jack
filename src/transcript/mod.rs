//! Claude Code jsonl transcript scanner.
//!
//! Scans `~/.claude/projects/**/*.jsonl` files line-by-line and extracts
//! per-turn metadata (uuids, timestamps, model identifiers, usage counts,
//! content block byte lengths, and thinking signatures).
//!
//! # Privacy invariant
//!
//! The scanner NEVER stores or logs raw user prompt / assistant response
//! / tool input / tool result / thinking body text. Only the following
//! flows through `TurnRecord`:
//! - Identifiers (uuids, session id, request id, message id)
//! - Timestamps (as ISO 8601 strings, as-is from jsonl)
//! - Byte lengths of content block payloads
//! - Opaque thinking signature base64 strings (no user content)
//! - Model identifiers and usage token counts
//!
//! Content bodies themselves are expected to go to a separate blob store
//! keyed by content-addressed hash (wired up by Lane D); this module
//! computes byte lengths only and leaves `content_hash` as `None`.

pub mod scanner;

#[allow(unused_imports)]
pub use scanner::{ContentBlockRecord, TranscriptScanner, TurnRecord, UsageRecord};
