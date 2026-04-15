//! Thinking signature parser.
//!
//! Claude Code's jsonl transcripts contain `thinking` blocks whose `signature`
//! field is a base64-encoded protobuf blob. Since the Anthropic API redacted
//! the plaintext `thinking` field in early 2026, the signature is the only
//! remaining artifact that leaks Anthropic's internal model variant strings
//! (e.g. `numbat-v6-efforts-10-20-40-ab-prod`, `claude-opus-4-6`,
//! `claude-haiku-4-5-20251001`) via ASCII islands embedded in the protobuf
//! framing.
//!
//! This parser treats the decoded bytes as opaque binary with printable ASCII
//! runs and extracts model variant strings via regex matching. It does NOT
//! attempt real protobuf parsing.
//!
//! # Privacy
//!
//! The signature does not contain any user content — it is opaque metadata
//! produced by Anthropic's inference stack. Safe to store and log.

pub mod parser;

#[allow(unused_imports)]
pub use parser::{parse_thinking_signature, SignatureMeta};
