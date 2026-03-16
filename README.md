# punkgo-jack

[![crates.io](https://img.shields.io/crates/v/punkgo-jack.svg)](https://crates.io/crates/punkgo-jack)

**Every AI action gets a receipt.**

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/presence.svg" alt="PunkGo presence heatmap — 7 days of AI agent activity" width="680">
</p>

Your AI agent just deleted your production database. Your project folder. Your `.env`. It happens every week on social media — and nobody can prove exactly what went wrong, because the session is already gone.

- **Post-incident forensics** — "which agent deleted that file at 3am?" Seconds, not hours.
- **Accountability that survives** — Terminal closed? Session compressed? The log doesn't care.
- **Trust but verify** — You approve actions. PunkGo proves what actually happened. Different things.
- **Energy awareness** — See what your agents cost. Per action, per session, per day.

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.sh | bash

# Windows (PowerShell)
irm https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.ps1 | iex

# or with cargo
cargo install punkgo-jack && cargo install punkgo-kernel
```

```bash
punkgo-jack setup claude-code   # Claude Code
punkgo-jack setup cursor        # Cursor IDE
# That's it. Your next session is already being recorded.
```

**Upgrade**: `punkgo-jack upgrade` — auto-detects cargo or install script, no re-setup needed.

## Record

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/history.svg" alt="PunkGo history — every tool call recorded with energy cost and Merkle receipt" width="680">
</p>

Every event gets a hash. Every hash gets appended to a Merkle tree. The tree is append-only — nobody can alter history without detection. Your statusline shows today's cumulative energy across all agents: `punkgo:⚡940`. Use `--actor` to filter by agent (e.g. `punkgo-jack presence --actor cursor`).

## Verify

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/verify-receipt.svg" alt="PunkGo verify — session events linked to Merkle tree with inclusion proof and append-only receipt" width="680">
</p>

`show` verifies any event's [RFC 6962](https://datatracker.ietf.org/doc/html/rfc6962) Merkle inclusion proof. `receipt` checks that the tree grew append-only during your session. No trust required, just math.

**Hand this to an auditor.** The checkpoint format follows [C2SP tlog-checkpoint](https://c2sp.org/tlog-checkpoint) — the same structure behind Go's `sum.golang.org` and Sigstore's Rekor. Export and verify with any RFC 6962 tool:

```bash
# C2SP checkpoint (portable, standard)
$ punkgo-jack show --checkpoint

# Raw proof hashes (feed to any RFC 6962 verifier)
$ punkgo-jack show a1b2c3 --json | jq '.proof'

# Offline verification — no daemon needed
$ punkgo-jack verify a1b2c3
$ punkgo-jack verify --file proof.json
```

**Cross-language verification.** PunkGo proofs are not proprietary — verify with Go's standard `sumdb/tlog` library:

```bash
$ punkgo-jack show a1b2c3 --json > proof.json
$ cd examples/verify-go && go run main.go proof.json
Inclusion:  VERIFIED -- leaf is in the tree, root matches checkpoint
```

See [`examples/verify-go/`](examples/verify-go/) for the full cross-verification example.

How the proof works under the hood → [punkgo-kernel audit trail](https://github.com/PunkGo/punkgo-kernel#audit-trail)

> **Trust model**: Checkpoints are Ed25519-signed (identity binding) and optionally timestamped via RFC 3161 TSA (time binding). Enable TSA with `[tsa] enabled = true` in `~/.punkgo/config.toml`. See [PIP-003](https://github.com/PunkGo/punkgo-kernel/blob/main/docs/PIP-003_EN.md) for the full trust layer architecture.

## Supported Agents

| Agent | Status | Integration |
|-------|--------|-------------|
| **Claude Code** (Terminal + VSCode) | Supported | `setup claude-code` — 6 hooks, fully automatic |
| **Cursor** | Supported | `setup cursor` — dedicated adapter, auto-detects source |
| **Custom agents** | Via MCP | Use `punkgo_log` tool directly |
| Windsurf, Cline, Aider | Planned | — |

## Requirements

- **Claude Code >= 1.0.85** — requires `SessionStart`, `SessionEnd`, and `UserPromptSubmit` hooks. Older versions partially work (tool call recording is fine, but session tracking and statusline won't function).
- **Cursor** — hooks support required (available in recent versions). Dual-tool users: Cursor may also trigger Claude Code hooks via Third-party skills — punkgo-jack detects this automatically and skips duplicates.
- **Rust toolchain** only needed if installing via `cargo install` instead of the one-line installer.

## How It Works

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/how-it-works.svg" alt="PunkGo architecture — hook fires, transform, kernel commit, receipt return" width="680">
</p>

- **Write path**: Hook → `punkgo-jack ingest` → IPC → kernel commit → Merkle tree update
- **Read path**: `punkgo-jack history/show/receipt` → IPC → kernel read
- **Resilience**: Daemon down? Auto-start it. Still down? Buffer to `spillover.jsonl`, replay later with `flush`.

## CLI

| Command | What it does |
|---------|-------------|
| `setup claude-code` | Install hooks + statusline + kernel detection |
| `setup cursor` | Install Cursor IDE hooks with dedicated adapter |
| `unsetup <tool> [--purge]` | Remove hooks. `--purge` also clears local state |
| `history [--actor ID]` | Recent events in a table |
| `show <EVENT_ID> [--json]` | Full event details + Merkle inclusion proof |
| `show --checkpoint` | Print C2SP tlog-checkpoint |
| `anchor [--quiet]` | Anchor latest checkpoint to RFC 3161 TSA |
| `verify <EVENT_ID>` | Offline Merkle proof verification + TSA status |
| `verify --file proof.json` | Fully offline verification from exported JSON |
| `verify-tsr <TREE_SIZE>` | Verify a stored TSA timestamp token |
| `receipt [SESSION]` | Session receipt with consistency proof |
| `report [SESSION]` | Turn-based session report |
| `presence [DAYS]` | Energy heatmap (default: 14 days) |
| `statusline on\|off` | Toggle energy statusline (Claude Code only) |
| `serve` | Start MCP server (7 tools for agent self-query) |
| `upgrade` | Check for updates and self-upgrade |
| `flush` | Replay buffered events to kernel |

## MCP Tools (Agent Self-Query)

When running as an MCP server, Claude Code can query its own audit trail:

| Tool | Description |
|------|-------------|
| `punkgo_ping` | Health check |
| `punkgo_log` | Record an audit note |
| `punkgo_query` | Query recent events |
| `punkgo_verify` | Merkle inclusion/consistency proofs |
| `punkgo_stats` | Event counts + distributions |
| `punkgo_checkpoint` | C2SP-format checkpoint |
| `punkgo_session_receipt` | Session receipt with Merkle verification |

## What Gets Recorded

| Recorded | Not Recorded |
|----------|-------------|
| Tool name + target + timestamp | Full tool output (default: exit code only) |
| File paths for read/write/edit | Agent reasoning / chain-of-thought |
| Shell commands | Images (metadata only: count + size) |
| User prompts | Streaming intermediate output |
| Merkle proof per event | |
| Energy cost per action | |

Want full output capture? Set `PUNKGO_CAPTURE_RESPONSE=full`.

## Energy Model

Every action has a cost:

```
total_cost = action_cost + append_cost
```

| Action | Cost |
|--------|------|
| observe (read) | 0 + append |
| create (new file) | 10 + append |
| mutate (edit) | 15 + append |
| execute (shell) | 25 + output_bytes/256 + append |
| append (universal) | 1 + payload_bytes/1024 |

The statusline shows `punkgo:⚡N` — your daily cumulative energy across all sessions.

## Environment Variables

| Variable | Default |
|----------|---------|
| `PUNKGO_DAEMON_ENDPOINT` | `\\.\pipe\punkgo-kernel` (Windows) / `punkgo-kernel` (Unix) |
| `PUNKGO_DATA_DIR` | `~/.punkgo` |
| `PUNKGO_STATE_DIR` | `~/.punkgo/state` |
| `PUNKGO_CAPTURE_RESPONSE` | `summary` (options: `full`, `summary`, `none`) |
| `PUNKGO_TSA_ENABLED` | `false` (set `true` to enable RFC 3161 anchoring) |
| `PUNKGO_TSA_URL` | `http://timestamp.digicert.com` |
| `PUNKGO_TSA_TIMEOUT_SECS` | `10` |
| `PUNKGO_TSA_MIN_INTERVAL_SECS` | `300` (set `0` for CI burst mode) |

## Build from Source

```bash
git clone https://github.com/PunkGo/punkgo-jack.git
cd punkgo-jack
cargo build --release
cargo test
```

Binary at `target/release/punkgo-jack`. You'll also need [punkgo-kernel](https://github.com/PunkGo/punkgo-kernel) for the daemon:

```bash
cargo install punkgo-kernel
```

## License

[MIT](LICENSE)
