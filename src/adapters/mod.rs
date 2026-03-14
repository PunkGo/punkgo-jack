pub mod claude_code;
pub mod cursor;

use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::Value;

/// Intermediate event format produced by hook adapters,
/// ready for submission to the kernel via `log_observe`.
pub struct IngestEvent {
    /// Source identity, e.g. "claude-code".
    pub actor_id: String,
    /// Operation target, e.g. "file:/src/main.rs" or "bash:npm test".
    pub target: String,
    /// Semantic type: command_execution, file_write, file_read, etc.
    pub event_type: String,
    /// Human-readable description of what happened.
    pub content: String,
    /// Structured metadata extracted from the raw hook JSON.
    pub metadata: BTreeMap<String, Value>,
    /// Adapter source name: "claude-code" / "cursor" / "windsurf".
    pub source: String,
}

/// Trait for adapters that convert tool-specific hook JSON into `IngestEvent`.
pub trait HookAdapter: Send + Sync {
    /// Transform raw hook JSON into an `IngestEvent`.
    fn transform(&self, raw: &Value) -> Result<IngestEvent>;
}

/// Look up an adapter by source name.
pub fn adapter_for_source(source: &str) -> Option<Box<dyn HookAdapter>> {
    match source {
        "claude-code" => Some(Box::new(claude_code::ClaudeCodeAdapter)),
        "cursor" => Some(Box::new(cursor::CursorAdapter)),
        _ => None,
    }
}
