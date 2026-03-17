# punkgo-jack

[![crates.io](https://img.shields.io/crates/v/punkgo-jack.svg)](https://crates.io/crates/punkgo-jack)

> Every AI action gets a receipt.

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/presence.svg" alt="PunkGo presence heatmap — 7 days of AI agent activity across Claude Code and Cursor" width="680">
</p>

Your AI agent just deleted your production database. Your `.env`. It happens every week — and nobody can prove what went wrong, because the session is already gone. PunkGo records every tool call to an append-only Merkle tree with Ed25519 signatures and optional RFC 3161 timestamps.

---

**Contents:** [Why](#why-i-built-this) · [Quick Start](#quick-start) · [How It Works](#how-it-works) · [Verify](#verify) · [Trust Layers](#trust-layers) · [CLI](#cli) · [Config](#config) · [Supported Tools](#supported-tools) · [Dual-Tool](#dual-tool-coexistence-claude-code--cursor) · [Evolution](#evolution)

---

## Why I built this

I've been using Claude Code daily since January 2025. After 25,000+ AI actions, I realized I had zero proof of what actually happened. The AI's own logs? It can edit those. Git history? Doesn't capture the thinking process. I wanted something that works like a dashcam — always recording, can't be tampered with, and there when you need it.

PunkGo Jack is that dashcam. Not a log file you can delete. Not a summary the AI writes about itself. A cryptographic receipt — append-only, Ed25519-signed, RFC 3161 timestamped. You can't backdate it, you can't delete it, you can't forge it.

**Who is this for?** If you use Claude Code or Cursor for client work, team projects, or anything where you need to show what your AI actually did — this is for you.

---

## Quick Start

```bash
curl -fsSL https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.sh | bash
punkgo-jack setup claude-code   # or: punkgo-jack setup cursor
```

That's it — two commands. Your next AI session is recorded with Ed25519 signatures and RFC 3161 timestamps. Verify anytime:

```bash
punkgo-jack receipt             # session summary + anchor time
punkgo-jack verify <ID>         # cryptographic proof
```

Upgrade: `punkgo-jack upgrade`. Uninstall: `punkgo-jack unsetup claude-code`.

<details>
<summary>Windows / manual install</summary>

```powershell
cargo install punkgo-jack && cargo install punkgo-kernel
punkgo-jack setup claude-code
```

Requires [Rust toolchain](https://rustup.rs). The install script also works in Git Bash on Windows.
</details>

## How It Works

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/overview.svg" alt="PunkGo overview — AI tools, hook adapter, kernel, three trust layers, verifiable receipts" width="680">
</p>

Hook fires → jack transforms → kernel commits to Merkle tree + Ed25519 signs → receipt returned. Daemon down? Auto-started. Still down? Buffered to spillover, replayed later.

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

Each layer adds a guarantee the layer below cannot provide:

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
| `serve` | MCP server (7 tools for agent self-query) |
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
| **Claude Code** | Supported | `setup claude-code` — 10 hooks (tools, sessions, subagents, notifications) + statusline |
| **Cursor** | Supported | `setup cursor` — 9 hooks (tools, sessions, subagents) |
| **MCP** | Built-in | `serve` — 7 tools for agent self-query |
| Windsurf, Cline | Planned | — |

## Dual-Tool Coexistence: Claude Code + Cursor

If you use both Claude Code and Cursor on the same machine, each tool gets its own hooks (`setup claude-code` + `setup cursor`). However, Cursor's **Third-party Skills** feature reads Claude Code's `settings.json` hooks — this can cause Claude Code hooks to fire inside Cursor sessions.

PunkGo handles this automatically: when a `--source claude-code` hook runs inside Cursor (detected via `CURSOR_VERSION` env var), it is silently skipped. The dedicated `--source cursor` hook handles recording instead. No duplicate events, no manual config needed.

**Recommended setup for dual-tool users:**
1. Run both: `punkgo-jack setup claude-code && punkgo-jack setup cursor`
2. Leave Cursor's Third-party Skills **enabled** — PunkGo deduplicates automatically
3. If you see unexpected hook errors in Cursor, check that both tools are on the same PunkGo version (`punkgo-jack upgrade`)

<details>
<summary>Alternative: disable Third-party Skills in Cursor</summary>

If you prefer full isolation, disable Third-party Skills in Cursor settings. This prevents Cursor from reading Claude Code's hooks entirely. PunkGo's own Cursor hooks (`~/.cursor/hooks.json`) are unaffected.

Cursor Settings → Features → Third-party Skills → Off
</details>

## Evolution

| Version | What changed |
|---------|-------------|
| **v0.5.3** | Fix setup hang on macOS (remove kerneld --version check) |
| v0.5.2 | 10 hook events (Stop/Subagent/Notification), fix Cursor metadata loss (BOM), semantic TSA rate limit, dual-tool docs |
| v0.5.1 | TSA on by default, Windows install fix, kernel version check, setup survey |
| v0.5.0 | RFC 3161 TSA anchoring, verify-tsr, config system |
| v0.4.2 | Multi-agent default (--actor shows all) |
| v0.4.1 | Cursor IDE support, dual-tool coexistence |
| v0.4.0 | Verify, export, presence heatmap, MCP server |

## License

[MIT](LICENSE)
