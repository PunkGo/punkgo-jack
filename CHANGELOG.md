# Changelog

All notable changes to `punkgo-jack` will be documented in this file.

The format is loosely based on Keep a Changelog.

## [0.6.0] - 2026-04-16

**The Transcript Archaeologist.** Jack learns to read Claude Code's local transcript archive and build a searchable index of every turn, every thinking signature, every redacted token.

### Added

- **`punkgo-jack reindex` CLI** — backfill from `~/.claude/projects/*.jsonl`. Flags: `--full`, `--since`, `--session`, `--dry-run`. 389 files / 39 sessions / 44,949 turns indexed in 6.4 seconds on the dev machine.
- **jack.db** — new SQLite index at `~/.punkgo/state/jack/jack.db`, separate from kernel. Tables: `sessions`, `turns`, `thinking_signatures`, `pending_scans`. Rollback = delete the file.
- **6 new MCP tools** — `punkgo_session_list`, `punkgo_session_detail`, `punkgo_turn_detail`, `punkgo_hidden_tokens`, `punkgo_model_variants`, `punkgo_reindex`.
- **5 new Claude Code hooks** — InstructionsLoaded, PreCompact, PostCompact, StopFailure, PermissionDenied. Hook count 10 → 15. Cursor filters all 5 automatically.
- **Transcript scanner** (`src/transcript/`) — streaming jsonl reader, metadata-only output, stable byte-offset turn ordering.
- **Signature parser** (`src/signature/`) — extracts model variant strings from thinking block signatures. Detected variants: `numbat-v6-efforts-10-20-40-ab-prod`, `claude-opus-4-6`, `claude-haiku-4-5-20251001`. 99.4% hit rate on real data.
- **Three indexer recovery paths** — hook-triggered incremental, full backfill, startup reconciliation with durable `pending_scans` queue.
- **`externalize_or_inline` blob helper** — routes large adapter content to blob store (> 4 KB). Env override: `PUNKGO_MAX_INLINE_BYTES`.

### Fixed

- **Silent truncation removed** — `last_assistant_message` (500 bytes), user prompt (200 bytes), and Bash command display (200 bytes) no longer truncated. Full content preserved via blob store.
- **Subagent session merge** — subagent jsonl files now correctly fold into their parent session instead of creating orphan session rows.
- **Fork-safe turn upsert** — forked sessions no longer steal turns from the original session. First-write-wins on `turn_uuid` across sessions.

### Changed

- `meta["last_assistant_message"]` is now `{"inline": "..."}` or `{"blob_hash": ...}` instead of a bare string.
- `turns.turn_order` is now the canonical jsonl byte offset (stable across incremental and full reindex paths).
- `sessions_upserted` in reindex reports reflects unique parent sessions (39), not file count (389).

### Internal

- `rusqlite 0.32` (bundled), `regex 1`, `once_cell 1` added as dependencies.
- 298 unit tests, 20-session independent oracle cross-check on real data.
- Dead fallback code in signature parser removed (proven unreachable for current regex family).

## [0.5.4] - 2026-03-20

### Added

- **`punkgo-jack roast` command** — AI personality diagnosis based on your coding session data
- **Analysis engine** — personality typing, RPG stat generation (six meme-named axes: Yapping/Googling/Grinding/Shipping/Tunnel Vision/Plot Armor), worst-moment extraction
- **16 MBTI dog breed personalities** — config-driven via `roast_config.toml`, each with breed name, catchphrase, quips, and background color
- **103 quip templates** — randomized meme sentences per personality type
- **SVG card rendering** — Personality Card (400x520) and Vibe Card (400x320) with dog image embedding, radar chart, CSS animations
- **PNG export** — `--png` / `--svg` direct file output via `resvg` with inline styles for compatibility
- **JSON output** — `--json` flag for programmatic consumption
- **Time range filters** — `--today`, `--week`, `--month` session scoping
- **`roast help` subcommand** — usage guide
- **Shared `data_fetch` module** — extracted from `export.rs` for reuse across roast and export commands
- **16 dog images** — `assets/dogs/dog-{name}.png`, compiled via `include_bytes!`

### Fixed

- Strip BOM prefix in CTA text for natural English ("the" prefix removal)
- PNG rendering: inline styles for `resvg` compatibility (external CSS not supported)
- Session exit tips removed — `CONOUT$`/`/dev/tty` unreliable with async hooks

## [0.5.3] - 2026-03-17

### Fixed

- **Fix setup hang on macOS** — `punkgo-kerneld` has no `--version` flag; calling it started a full daemon that never exits, hanging `punkgo-jack setup` (macOS advisory flock doesn't prevent duplicate daemons). Removed `check_kernel_version()` and `parse_major_minor()` entirely. IPC protocol (`punkgo-core`) ensures wire compatibility.

## [0.5.2] - 2026-03-16

### Added

- **10 hook events** — added Stop, SubagentStart, SubagentStop, Notification (Claude Code 6→10, Cursor 6→9)
- Claude Code adapter: extract `last_assistant_message`, `agent_type`, `notification_type`
- Cursor adapter: extract `status`, `loop_count`, `modified_files`, `tool_call_count`
- `cursor_default_response`: allow `subagentStart` permission
- README: Dual-Tool Coexistence section (Third-party Skills handling)

### Fixed

- **Cursor BOM metadata loss** — Windows UTF-8 BOM (`\xEF\xBB\xBF`) in stdin caused all Cursor event metadata to be silently dropped since v0.4.1. Fixed with `strip_prefix('\u{FEFF}')` before JSON parse.

### Changed

- **Semantic TSA rate limit** — replaced clock-based rate limit with `tree_size` check. Anchor when tree grows, skip when unchanged, force re-anchor if TSR file lost. Removed dead code: `rate_limit_path`, `check_clock_rate_limit`, `now_epoch_secs`.

## [0.5.1] - 2026-03-16

### Changed

- **TSA anchoring on by default** — free DigiCert public service, rate-limited to once per 5 minutes
- Setup: print survey link (`punkgo.ai/why`) after successful setup
- Setup: check kernel version compatibility, warn if outdated
- README: "Why I built this" section with ICP positioning, simplified Quick Start to two commands

### Fixed

- `install.sh`: platform-aware install dir (`~/.punkgo/bin/` on Windows), PATH detection + guidance, skip `chmod +x` on Windows, `mkdir -p` before install

## [0.5.0] - 2026-03-16

### Added

- **RFC 3161 TSA anchoring** — `punkgo-jack anchor` command submits Merkle checkpoint root hash to a timestamp authority (default: DigiCert). Proves "this checkpoint existed before time T"
- **TSA response validation** — full RFC 3161 parsing via `x509-tsp`: PKIStatus check, hash cross-verification, genTime extraction
- **`verify-tsr` command** — standalone TSA token verification: `punkgo-jack verify-tsr <tree_size>`
- **TSA status in `verify`** — Merkle verification now automatically shows TSA anchor status when a TSR file exists
- **TSA status in `receipt`** — session receipts show anchor timestamp, searches for nearest covering checkpoint
- **`~/.punkgo/config.toml`** — first configuration file for jack. TSA is opt-in: `[tsa] enabled = true`
- **Configuration layer model** — env vars override config file override defaults (`PUNKGO_TSA_ENABLED`, `PUNKGO_TSA_URL`, etc.)
- **Rate limiting** — configurable minimum interval between TSA submissions (default: 5 min, set to 0 for CI)
- **Session crash recovery** — `anchor --stale-only` registered on SessionStart hook catches un-anchored checkpoints from crashed sessions
- **Hook multi-command support** — SessionEnd and SessionStart hooks now register both `ingest` and `anchor` commands
- **DigiCert TSR fixture test** — end-to-end validation with a real RFC 3161 response from DigiCert TSA

### Dependencies

- Added: `ureq` v2 (sync HTTP), `toml` v0.8, `x509-tsp` v0.1, `cms` v0.2, `der` v0.7, `spki` v0.7, `const-oid` v0.9
- Updated: `punkgo-core` 0.4.0 → 0.5.0

## [0.4.2] - 2026-03-15

### Changed
- All query commands (presence, history, export) default to showing all agents when `--actor` is not specified
- `try_seed_actor` now uses per-actor purpose field instead of hardcoded "claude-code-adapter"
- `setup cursor` messaging: informs user that duplicates are auto-detected (no manual action needed)

## [0.4.1] - 2026-03-14

### Added
- **Cursor IDE support**: `punkgo-jack setup cursor` / `unsetup cursor`
- Dedicated `CursorAdapter` with Cursor-specific tool name mapping (`Shell`, `Delete`), metadata enrichment (`cursor_version`, `model`, `user_email`, `duration_ms`, `workspace`), and `conversation_id` normalization
- Automatic source detection: `--actor cursor` vs `--actor claude-code` in presence/history/export
- Cursor-compatible hook responses per event type (`permission`, `continue`)
- `CURSOR_VERSION` env var detection for zero-overhead skip of claude-code hooks in Cursor environment
- Empty stdin resilience for Cursor session events

### Changed
- `hook_events()` now accepts source parameter for per-tool hook commands
- `--quiet` mode outputs typed JSON responses (Cursor requirement)
- `--summary` omitted for Cursor hooks (Cursor treats stderr as error)
- Presence footer simplified: removed "ask Claude" prompt
- Help text updated with cursor support

### Fixed
- Submit errors now spill to spillover before returning (no data loss)
- Error propagation from `run_inner` caught by wrapper (always exit 0 with valid JSON)

## [0.4.0] - 2026-03-13

### Changed
- IPC endpoint discovery: jack now reads `~/.punkgo/state/daemon.addr` instead of using a hardcoded address
- Removed `kill_stale_daemon()` — daemon lifecycle managed by kernel's flock-based locking
- Falls back to legacy endpoint for backward compatibility with older kernels
- After daemon auto-start, re-reads `daemon.addr` to pick up the new per-PID endpoint

### Fixed
- Stale socket/pipe issues after daemon crash on all platforms

## [0.3.1] - 2026-03-13

Energy model fix and daemon lifecycle improvements.

### Energy System Fix (P0)

- **Fixed energy starvation bug**: agents were starved by root's dominant `energy_share` (`floor()` rounded to 0)
- Energy distribution now targets agents only — humans (including root) get one-time initial balance
- Default actor seed: `energy_balance` 100,000 (was 10,000), `energy_share` 50.0 (was 0.1)
- Session summary energy display: fixed field name mismatch (`"balance"` → `"energy_balance"`), removed stale hardcoded initial balance

### Daemon Lifecycle (P1)

- `kill_stale_daemon()`: auto-kills leftover daemon processes before starting a new one
- Windows IPC endpoint changed to file-path pipe (`\\.\pipe\punkgo-kernel`) — fixes "Access Denied" with `GenericNamespaced`
- Session start energy check (`check_energy_level`): warns if actor energy is critically low

### Compatibility

- Zero migration: existing databases work automatically (SQL filters by `actor_type = 'agent'`)
- Requires punkgo-kernel ≥ 0.3.0

## [0.2.1] - 2026-03-08

Release readiness — cross-platform fixes, blob store, energy model, dependency diet.

### Breaking

- Rust edition changed from `"2024"` (invalid) to `"2021"` (stable)
- `rebuild-audit` command now requires `--features rebuild-audit` (removes sqlx from default build)

### Content-Addressed Blob Store (P0)

- Large tool_input fields (>1KB) externalized to `~/.punkgo/blobs/<sha256>`
- Metadata stores only hash reference (`sha256:...`), keeping event log compact
- Content-addressed dedup: same file content = one blob
- Graceful fallback to inline storage on blob store failure

### Energy Model Alignment (P0.5)

- Two-layer cost model: `total_cost = action_cost + append_cost`
- Action cost: observe=0, create=10, mutate=15, execute=25+output/256
- Append cost (universal): `1 + payload_bytes/1024` — reflects Landauer principle
- Blob store reduces append cost by shrinking payload (economic incentive alignment)

### Actor ID Generalization (P1)

- CLI commands (`history`, `presence`) accept `--actor <ID>` flag
- Default actor resolution: CLI flag > session state > query all actors
- `report`, `receipt`, MCP `session_receipt` use session actor_id automatically
- Removes 5 hardcoded `actor_id: "claude-code"` from read-side queries
- Adapter identity (`claude-code`) correctly preserved on write side

### Cross-Platform Fixes

- Statusline daemon detection: `pgrep` (Unix) with `tasklist` (Windows) fallback
- Previously Windows-only (`tasklist`), now works on macOS/Linux
- Deduplicated `home_dir()` — single implementation in `session.rs`

### Resilience

- Spillover file capped at 10MB — events silently dropped with warning above cap
- Error messages now include actionable guidance ("Is punkgo-kerneld running?")

### Packaging & Docs

- Added Cargo.toml metadata: `description`, `license`, `repository`, `homepage`, `keywords`, `categories`
- README: accurate tagline, "What Gets Recorded" table, energy model section
- `IMPROVEMENTS.md` moved to `docs/internal/` (not user-facing)
- `sqlx` moved to optional `rebuild-audit` feature (3.7MB binary without it)
- CLI help: `flush` and `rebuild-audit` listed in Commands section (was misplaced)

### Tests

- 81 tests with `--features rebuild-audit`, 78 without (audit_rebuild gated)

## [0.2.0] - 2026-03-03

End-to-end session receipt system — sessions, history CLI, spillover resilience.

### Session Management (NEW)

- Client-side session lifecycle: `start_session()`, `current_session()`, `increment_event_count()`, `end_session()`
- Session state persisted to `~/.punkgo/current_session.json` across process boundaries
- Session ID (UUID) attached to all tool event metadata
- Session receipt summary printed to stderr on SessionEnd (`--summary` flag)

### CLI Commands (NEW)

- `punkgo-jack history` — tabular event listing with `--limit`, `--today` filters
- `punkgo-jack show <EVENT_ID>` — full event detail with Merkle inclusion proof verification
- `punkgo-jack receipt [SESSION_ID]` — session receipt with consistency proof verification
- `punkgo-jack flush` — replay spillover events to daemon

### Hook Adapter Enhancements

- Added `PreToolUse` hook — observe-only, records intent before tool execution
- Added `UserPromptSubmit` hook — records user prompts as `user_prompt` events
- Now 6 hooks total (was 4): PreToolUse, PostToolUse, PostToolUseFailure, UserPromptSubmit, SessionStart, SessionEnd
- Configurable response capture via `PUNKGO_CAPTURE_RESPONSE` env var (`full` / `summary` / `none`)
- New ingest flags: `--receipt` (one-line receipt to stderr), `--summary` (session summary on end)

### MCP Server

- New tool: `punkgo_session_receipt` — session summary with event distribution and Merkle verification
- Now 7 MCP tools total (was 6)

### Spillover Resilience (NEW)

- Events saved to `~/.punkgo/spillover.jsonl` when daemon is unreachable
- `punkgo-jack flush` replays spillover events when daemon is back
- Ingest never blocks Claude Code — returns exit 0 even on daemon failure

### Setup

- Actor creation enhanced: `actor_type`, `purpose`, `energy_share` fields, energy bumped to 10000
- Hook registration expanded to 6 hooks (added PreToolUse, UserPromptSubmit)
- SessionEnd hook now includes `--summary` flag

### Tests

- 58 tests (was 34): +8 session, +4 history, +4 spillover, +5 adapter, +2 MCP tools, +1 ingest

## [0.1.0] - 2026-02-24

Initial release as `punkgo-jack` — local MCP adapter and hook ingest bridge for `punkgo-kernel`.

### MCP Server (`punkgo-jack serve`)

- MCP stdio server via `rmcp` (JSON-RPC 2.0, protocol version 2024-11-05)
- `DaemonBackend`: IPC-based backend connecting to `punkgo-kerneld` via Unix socket / Windows named pipe
- Endpoint discovery: `--endpoint` flag > `PUNKGO_DAEMON_ENDPOINT` env > platform default
- 6 MCP tools:
  - `punkgo_ping` — connectivity and backend health check
  - `punkgo_log` — record a human-friendly audit note (facade over kernel `submit_observe`)
  - `punkgo_query` — query recent events with local filtering (actor, action_type, keyword, time range)
  - `punkgo_verify` — Merkle inclusion proof (by `log_index` or `event_id`) and consistency proof
  - `punkgo_stats` — kernel total count + sampled derived stats (action_type / actor / day buckets)
  - `punkgo_checkpoint` — latest C2SP-format checkpoint from the kernel audit log

### Hook Adapter (`punkgo-jack ingest`)

- Reads hook JSON from stdin, transforms via adapter, submits to kernel via IPC
- Claude Code adapter: 11 tool type mappings (Bash, Read, Write, Edit, Glob, Grep, WebFetch, WebSearch, Task, mcp__*, fallback)
- Schema: `punkgo-jack-ingest-v1`
- Supports `--dry-run`, `--quiet`, `--event-type` override, `--endpoint` override
- Exit code strategy: 0 success, 1 failure (never 2, to avoid blocking Claude Code)

### Setup CLI (`punkgo-jack setup/unsetup`)

- `punkgo-jack setup claude-code` — auto-inject hooks into `~/.claude/settings.json`
  - PostToolUse, PostToolUseFailure, SessionStart, SessionEnd
  - Merge strategy preserves existing user hooks; idempotent
- `punkgo-jack unsetup claude-code` — remove punkgo hooks, preserve other config
- Auto-seeds actor on setup (best-effort, daemon may not be running)

### Dependencies

- `punkgo-core` v0.2.3 (protocol types with Serialize/Deserialize)
- `punkgo-runtime` v0.2.3 (dev-dependencies only, for `EmbeddedBackend` in tests)
- `rmcp` 0.16, `interprocess` 2.4, `uuid` 1, `chrono` 0.4
