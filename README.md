# punkgo-jack

[![CI](https://github.com/PunkGo/punkgo-jack/actions/workflows/ci.yml/badge.svg)](https://github.com/PunkGo/punkgo-jack/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/punkgo-jack.svg)](https://crates.io/crates/punkgo-jack)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

<h3 align="center">Every AI action gets a receipt.</h3>

<p align="center">
Cryptographic audit receipts for AI coding agents.<br>
Ed25519 signatures. Merkle trees. RFC 3161 timestamps.
</p>

```bash
curl -fsSL https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.sh | bash
```

---

**Contents:** [Quick Start](#quick-start) · [Why receipts?](#why-receipts) · [How It Works](#how-it-works) · [Verify](#verify) · [Trust Layers](#trust-layers) · [CLI](#cli) · [Config](#config) · [Supported Tools](#supported-tools) · [Evolution](#evolution) · [Roast](#punkgo-roast)

---

## Quick Start

```bash
curl -fsSL https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.sh | bash
punkgo-jack setup claude-code   # or: punkgo-jack setup cursor
```

Two commands. Your next AI session is recorded with Ed25519 signatures and RFC 3161 timestamps.

```bash
punkgo-jack receipt             # session summary + anchor time
punkgo-jack verify <ID>         # cryptographic proof
```

Upgrade: `punkgo-jack upgrade`. Uninstall: `punkgo-jack unsetup claude-code`.

<details>
<summary>Windows (PowerShell)</summary>

```powershell
irm https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.ps1 | iex
punkgo-jack setup claude-code
```

Or manually: `cargo install punkgo-jack && cargo install punkgo-kernel`. Requires [Rust toolchain](https://rustup.rs).
</details>

## Why receipts?

Your AI agent just deleted your production database. Your `.env`. It happens every week — and nobody can prove what went wrong, because the session is already gone.

PunkGo Jack is a dashcam for AI coding. Not a log file you can delete. Not a summary the AI writes about itself. A cryptographic receipt — append-only, Ed25519-signed, RFC 3161 timestamped. You can't backdate it, you can't delete it, you can't forge it.

**Who is this for?** If you use Claude Code or Cursor for client work, team projects, or anything where you need to show what your AI actually did.

## How It Works

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/overview.svg" alt="PunkGo overview — AI tools, hook adapter, kernel, three trust layers, verifiable receipts" width="680">
</p>

Hook fires &rarr; jack transforms &rarr; kernel commits to Merkle tree + Ed25519 signs &rarr; receipt sealed. Daemon down? Auto-started. Still down? Buffered to spillover, replayed later.

## Verify

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/verify-receipt.svg" alt="PunkGo verify — Merkle inclusion proof with Ed25519 signature and RFC 3161 timestamp" width="680">
</p>

```bash
punkgo-jack verify a1b2c3       # Merkle proof + TSA status
punkgo-jack verify-tsr 42       # verify stored TSA token
punkgo-jack receipt             # session summary + anchor timestamp
punkgo-jack show a1b2c3 --json  # raw proof for any RFC 6962 verifier
```

Cross-language verification: export proof JSON, verify with Go's `sumdb/tlog` — see [`examples/verify-go/`](examples/verify-go/).

## Trust Layers

> "Trust me bro" is not a cryptographic primitive.

| Layer | Proves | Mechanism |
|-------|--------|-----------|
| **Merkle** | "this event is in the tree, the tree is append-only" | RFC 6962 |
| **Ed25519** | "this kernel instance signed this checkpoint" | RFC 8032 |
| **TSA** | "this checkpoint existed before time T" | RFC 3161 |

A root operator with the signing key could rebuild the tree — this is the single-machine trust boundary. TSA adds time binding: you cannot backdate a timestamped checkpoint. See [PIP-003](https://github.com/PunkGo/punkgo-kernel/blob/main/docs/PIP-003_EN.md) for the full architecture.

## CLI

| Command | Description |
|---------|-------------|
| `setup <tool>` | Install hooks (claude-code, cursor) |
| `history` | Recent events table |
| `show <ID>` | Event details + Merkle proof + TSA status |
| `receipt` | Session receipt with anchor timestamp |
| `verify <ID>` | Offline Merkle + TSA verification |
| `verify-tsr <N>` | Verify stored TSA token |
| `anchor` | Anchor latest checkpoint to TSA |
| `presence` | Energy heatmap across agents |
| `export` | Export events as markdown or JSON |
| `serve` | MCP server (13 tools for agent self-query) |
| `reindex` | Backfill transcript index (`--full`, `--since`, `--session`, `--dry-run`) |
| `roast` | AI personality diagnosis from local data (`roast help`) |
| `upgrade` | Self-update (no re-setup needed) |

## Config

TSA anchoring is **on by default** (free DigiCert public service, rate-limited to once per 5 minutes). To customize, create `~/.punkgo/config.toml`:

```toml
[tsa]
# enabled = true                           # default: true
# url = "http://timestamp.digicert.com"    # default
# timeout_secs = 10                        # default
# min_interval_secs = 300                  # 0 for CI burst mode
```

Disable TSA: set `enabled = false` or `PUNKGO_TSA_ENABLED=false`. Other env vars: `PUNKGO_TSA_URL`, `PUNKGO_TSA_MIN_INTERVAL_SECS`.

## Supported Tools

| Tool | Status | Setup |
|------|--------|-------|
| **Claude Code** | Supported | `setup claude-code` — 15 hooks + statusline |
| **Cursor** | Supported | `setup cursor` — 9 hooks |
| **MCP** | Built-in | `serve` — 13 tools for agent self-query |
| Windsurf, Cline | Planned | — |

<details>
<summary>Dual-Tool Coexistence: Claude Code + Cursor</summary>

If you use both, each tool gets its own hooks. Cursor's Third-party Skills reads Claude Code's `settings.json` — PunkGo handles this automatically: `--source claude-code` hooks inside Cursor are silently skipped. No duplicate events.

**Setup:** `punkgo-jack setup claude-code && punkgo-jack setup cursor`

Leave Cursor's Third-party Skills **enabled** — PunkGo deduplicates automatically.
</details>

## Evolution

| Version | What changed |
|---------|-------------|
| **v0.6.0** | Transcript Archaeologist — jack.db index, 6 MCP tools, `reindex` CLI, 15 hooks, signature parser |
| v0.5.4 | Built-in `roast` command (local coding data analysis) |
| v0.5.3 | Fix setup hang on macOS |
| v0.5.2 | 10 hook events, Cursor BOM fix, semantic TSA rate limit |
| v0.5.1 | TSA on by default, Windows install fix |
| v0.5.0 | RFC 3161 TSA anchoring, verify-tsr, config system |
| v0.4.1 | Cursor IDE support, dual-tool coexistence |
| v0.4.0 | Verify, export, presence heatmap, MCP server |

## PunkGo Roast

Your AI has a personality. We built a test for it.

**16 dog breeds. One prompt. Zero registration.** &rarr; [roast.punkgo.ai](https://roast.punkgo.ai)

Jack also includes a built-in `punkgo-jack roast` command that analyzes your local coding data for personality signals. Run `punkgo-jack roast help` for details.

## License

[MIT](LICENSE)

---

<p align="center">
Every AI action gets a receipt.<br>
<a href="https://punkgo.ai">punkgo.ai</a>
</p>
