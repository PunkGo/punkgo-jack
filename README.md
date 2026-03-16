# punkgo-jack

[![crates.io](https://img.shields.io/crates/v/punkgo-jack.svg)](https://crates.io/crates/punkgo-jack)

> Every AI action gets a receipt.

<p align="center">
  <img src="https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/assets/presence.svg" alt="PunkGo presence heatmap — 7 days of AI agent activity across Claude Code and Cursor" width="680">
</p>

Your AI agent just deleted your production database. Your `.env`. It happens every week — and nobody can prove what went wrong, because the session is already gone. PunkGo records every tool call to an append-only Merkle tree with Ed25519 signatures and optional RFC 3161 timestamps.

---

**Contents:** [Quick Start](#quick-start) · [How It Works](#how-it-works) · [Verify](#verify) · [Trust Layers](#trust-layers) · [CLI](#cli) · [Config](#config) · [Supported Tools](#supported-tools) · [Evolution](#evolution)

---

## Quick Start

```bash
# Install
curl -fsSL https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.sh | bash
# or: cargo install punkgo-jack && cargo install punkgo-kernel

# Setup (pick your tool)
punkgo-jack setup claude-code
punkgo-jack setup cursor

# Optional: enable RFC 3161 TSA time anchoring
echo -e '[tsa]\nenabled = true' >> ~/.punkgo/config.toml
```

That's it. Your next session is already being recorded. Upgrade anytime: `punkgo-jack upgrade`.

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

TSA anchoring is opt-in. Create `~/.punkgo/config.toml`:

```toml
[tsa]
enabled = true
# url = "http://timestamp.digicert.com"   # default
# timeout_secs = 10                        # default
# min_interval_secs = 300                  # 0 for CI burst mode
```

Env var overrides: `PUNKGO_TSA_ENABLED`, `PUNKGO_TSA_URL`, `PUNKGO_TSA_MIN_INTERVAL_SECS`.

## Supported Tools

| Tool | Status | Setup |
|------|--------|-------|
| **Claude Code** | Supported | `setup claude-code` — 6 hooks + statusline |
| **Cursor** | Supported | `setup cursor` — dedicated adapter |
| **MCP** | Built-in | `serve` — 7 tools for agent self-query |
| Windsurf, Cline | Planned | — |

## Evolution

| Version | What changed |
|---------|-------------|
| **v0.5.0** | RFC 3161 TSA anchoring, verify-tsr, config system |
| v0.4.2 | Multi-agent default (--actor shows all) |
| v0.4.1 | Cursor IDE support, dual-tool coexistence |
| v0.4.0 | Verify, export, presence heatmap, MCP server |

## License

[MIT](LICENSE)
