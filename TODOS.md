# TODOS

Deferred work, most recent first.

## Found in v0.7.0 real-machine dogfood (2026-07-02)

- **Claude Code blob externalization is un-redacted** — the pre-existing hook
  ingest path (`blob::externalize_tool_input`, since v0.6.x) writes large
  `tool_input`/`tool_response` fields to the local blob store verbatim; the
  v0.7.0 `Redactor` guards only the Codex capture path. A secret inside a CC
  tool-call body therefore lands in the local blob store in cleartext (blobs
  never leave the machine and kernel events carry only hashes, but secret-zero
  says scrub it). Fix for v0.7.1: route CC externalization through
  `redact::Redactor` before hashing/storing (hash then refers to the stored,
  redacted content; pre-existing blobs unaffected). Not a v0.7.0 regression.

## Deferred from v0.7.0 CEO review (2026-07-01)

- **E2 · Unified cross-agent session view** — `report`/MCP renders a session
  uniformly across `claude-code` / `cursor` / `codex` by `source`. Small,
  reuses existing report. Unblocked once Codex turns land (v0.7.0 P2).
- **E3 · Captured-content FTS search** — SQLite FTS index over blob content
  ("find where the agent touched auth"). New capability; scope-creep flag.
  Depends on content capture (v0.7.0 P2).

## Deferred from v0.7.0 P3 (2026-07-02)

- **Incremental Codex rollout scan** — `run_codex_reindex_session` (the P3 hook
  path) re-scans the whole rollout file on every `Stop`. For a long session
  whose file grows large, that is O(turns × filesize) over the session. Claude
  Code uses byte-offset incremental scan (`transcript/scanner.rs`
  scan_incremental + sessions.last_scan_offset); Codex should do the same:
  resume from the stored offset and append only new turns. Correct today (full
  re-scan is idempotent), just not optimal for very long sessions.

## Deferred from v0.7.0 Workstream B codex review (2026-07-02)

- **Exactly-once Codex receipts (kernel-side idempotency)** — `drain_codex_receipts`
  is at-least-once: a crash between `client.send()` (kernel committed) and
  `backfill_kernel_event_id` leaves the turn pending, so a later drain re-emits a
  duplicate receipt. Closing it needs the kernel to dedup on the deterministic
  target `codex/turn/{turn_uuid}` (reject/return-existing on a repeat). That is a
  kernel change (punkgo-kernel), out of scope for the jack-only v0.7.0. Failure
  mode today is a redundant append-only receipt, not data loss.
