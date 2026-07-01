# TODOS

Deferred work, most recent first.

## Deferred from v0.7.0 CEO review (2026-07-01)

- **E2 · Unified cross-agent session view** — `report`/MCP renders a session
  uniformly across `claude-code` / `cursor` / `codex` by `source`. Small,
  reuses existing report. Unblocked once Codex turns land (v0.7.0 P2).
- **E3 · Captured-content FTS search** — SQLite FTS index over blob content
  ("find where the agent touched auth"). New capability; scope-creep flag.
  Depends on content capture (v0.7.0 P2).

## Deferred from v0.7.0 Workstream B codex review (2026-07-02)

- **Exactly-once Codex receipts (kernel-side idempotency)** — `drain_codex_receipts`
  is at-least-once: a crash between `client.send()` (kernel committed) and
  `backfill_kernel_event_id` leaves the turn pending, so a later drain re-emits a
  duplicate receipt. Closing it needs the kernel to dedup on the deterministic
  target `codex/turn/{turn_uuid}` (reject/return-existing on a repeat). That is a
  kernel change (punkgo-kernel), out of scope for the jack-only v0.7.0. Failure
  mode today is a redundant append-only receipt, not data loss.
