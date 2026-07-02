# Contributing

## Scope

`punkgo-jack` is the local adapter layer for `punkgo-kernel`, providing:

- **MCP server** (`punkgo-jack serve`) — tool surface for querying and proving AI agent history
- **Hook adapter** (`punkgo-jack ingest`) — automatic tool call recording via host tool hooks
- **Setup CLI** (`punkgo-jack setup/unsetup`) — auto-configure hooks in supported tools

Design principles:

- Keep `punkgo-kernel` as the only audit/commit engine
- Avoid adding a second append-only log implementation here
- Prefer thin facades and protocol adapters over duplicated kernel logic

## Requirements

- **Rust 1.88+** (MSRV declared in `Cargo.toml`)
- `punkgo-kerneld` running for integration testing (unit tests use `EmbeddedBackend` / `MockBackend` under `#[cfg(test)]`)

## Local Development

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

All three must pass before submitting a PR. CI runs these on three platforms (Ubuntu, Windows, macOS).

Feature flags:

```bash
cargo test --all-features        # includes rebuild-audit (requires SQLite)
```

(`default` is empty as of v0.7.0; `rebuild-audit` is the only optional feature.)

## Runtime Mode

- Production: `DaemonBackend` connects to `punkgo-kerneld` via IPC (Unix socket / Windows named pipe)
- Tests: `EmbeddedBackend` (in-process kernel) and `MockBackend` under `#[cfg(test)]`

## Contribution Guidelines

- Keep tool interfaces backward-compatible where possible
- Add tests for new tool behavior and error paths
- Document public-facing tool/API changes in `README.md` (and add tests)
- If changing kernel integration assumptions, explain why in the PR description

## Near-Term Priorities

- `read_events` pagination/cursor integration (kernel-level limitation, scan_limit max 100)
- Windsurf hook adapter (Cursor adapter completed in v0.4.1, Codex CLI in v0.7.0)
- See `TODOS.md` for deferred work (incremental Codex rollout scan, redacting the
  Claude Code blob externalization path, exactly-once kernel receipts)
