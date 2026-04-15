mod adapters;
mod anchor;
#[cfg(feature = "rebuild-audit")]
mod audit_rebuild;
mod backend;
mod blob;
mod config;
mod daemon;
mod data_fetch;
mod export;
mod history;
mod index;
mod indexer;
mod ingest;
mod ipc_client;
mod mcp;
mod presence;
mod report;
mod roast;
mod session;
mod setup;
mod spillover;
mod tools;
mod tsa_verify;
mod upgrade;
mod verify;
// Lane C v0.6.0: transcript + signature modules. Consumed by Lane D
// (indexer.rs / index/ / tools.rs).
mod signature;
mod transcript;

use std::env;

use anyhow::{Context, Result};
use tracing::{error, info};

fn init_tracing() {
    // Default to WARN for clean CLI output. Set RUST_LOG=info for debugging.
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| v.parse::<tracing::Level>().ok())
        .unwrap_or(tracing::Level::WARN);
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .init();
}

fn print_usage() {
    eprintln!(
        "Usage: punkgo-jack <command>\n\
         \n\
         Commands:\n\
         \x20 serve                   Start MCP server (default)\n\
         \x20 ingest [OPTIONS]        Ingest hook data from stdin into kernel\n\
         \x20 setup <TOOL>            Install punkgo hooks into a tool\n\
         \x20 unsetup <TOOL> [--purge] Remove punkgo hooks (--purge: also delete local state)\n\
         \x20 export [OPTIONS]        Export events as markdown or JSON\n\
         \x20 history [OPTIONS]       List recent recorded actions\n\
         \x20 show <EVENT_ID>         Show full details of a single event\n\
         \x20 receipt [SESSION_ID]    Generate session receipt\n\
         \x20 report [SESSION_ID]     Generate turn-based session report\n\
         \x20 roast [OPTIONS]        AI personality roast (try: roast help)\n\
         \x20 presence [DAYS]         Show collaboration heatmap (default: 14 days)\n\
         \x20 statusline on|off       Toggle energy statusline (Claude Code only)\n\
         \x20 anchor [OPTIONS]        Anchor latest checkpoint to TSA\n\
         \x20 verify <EVENT_ID>       Verify Merkle inclusion proof offline\n\
         \x20 verify-tsr <TREE_SIZE>  Verify a stored TSA timestamp token\n\
         \x20 upgrade                 Check for updates and upgrade\n\
         \x20 flush                   Replay spillover events to kernel\n\
         \x20 reindex [OPTIONS]      Backfill jack transcript index from ~/.claude/projects/\n\
         \x20 rebuild-audit           Rebuild Merkle tree from event hashes\n\
         \x20 help                    Show this message\n\
         \n\
         Export options:\n\
         \x20 --session <ID>          Filter by session ID\n\
         \x20 --last <N>              Export last N events\n\
         \x20 --format <FMT>          Output format: markdown (default), json\n\
         \x20 --output <FILE>         Write to file instead of stdout\n\
         \x20 --actor <ID>            Filter by actor\n\
         \n\
         Query options (history, presence, export):\n\
         \x20 --actor <ID>            Filter by actor (e.g. claude-code, cursor). Default: all\n\
         \n\
         Reindex options:\n\
         \x20 --full                  Re-scan every transcript file\n\
         \x20 --since <TS>            Only files with mtime >= ISO 8601 timestamp\n\
         \x20 --session <ID>          Only this session's transcript\n\
         \x20 --dry-run               Parse and report counts, do not write\n\
         \n\
         Ingest options:\n\
         \x20 --source <NAME>         Data source (claude-code, cursor)\n\
         \x20 --event-type <TYPE>     Override adapter-derived event type\n\
         \x20 --endpoint <ENDPOINT>   Override daemon endpoint\n\
         \x20 --dry-run               Parse and transform only, do not write\n\
         \x20 --quiet                 Suppress stdout JSON output\n\
         \x20 --receipt               Print receipt line to stderr (even in quiet mode)\n\
         \x20 --summary               Print session summary on session_end\n\
         \n\
         Supported tools:\n\
         \x20 claude-code             Claude Code hooks\n\
         \x20 cursor                  Cursor IDE hooks\n\
         \n\
         Environment:\n\
         \x20 PUNKGO_DAEMON_ENDPOINT  Override daemon IPC endpoint\n"
    );
}

fn main() {
    init_tracing();

    let mut args = env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "serve".to_string());

    if cmd == "--version" || cmd == "-V" || cmd == "version" {
        println!("punkgo-jack {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    let result = match cmd.as_str() {
        "serve" => run_serve(),
        "export" => run_export(&mut args),
        "ingest" => run_ingest(&mut args),
        "setup" => run_setup(&mut args),
        "unsetup" => run_unsetup(&mut args),
        "history" => run_history(&mut args),
        "show" => run_show(&mut args),
        "receipt" => run_receipt(&mut args),
        "report" => run_report(&mut args),
        "roast" => run_roast(&mut args),
        "presence" => run_presence(&mut args),
        "statusline" => run_statusline(&mut args),
        "anchor" => anchor::run_anchor(&mut args),
        "verify" => run_verify(&mut args),
        "verify-tsr" => verify::run_verify_tsr(&mut args),
        "upgrade" => upgrade::run_upgrade(),
        "flush" => spillover::flush(),
        "reindex" => run_reindex(&mut args),
        #[cfg(feature = "rebuild-audit")]
        "rebuild-audit" => run_rebuild_audit(&mut args),
        "help" | "-h" | "--help" => {
            print_usage();
            Ok(())
        }
        other => {
            error!(command = other, "unknown command");
            print_usage();
            std::process::exit(2);
        }
    };

    if let Err(e) = result {
        error!(error = %e, "command failed");
        // Exit 1 for errors. Never exit 2 (blocks Claude Code tool calls).
        std::process::exit(1);
    }
}

fn run_serve() -> Result<()> {
    let backend = backend::backend_from_env()?;
    info!("daemon backend initialized");

    // Path C: reconcile any leftover pending_scans from prior runs and
    // re-enqueue drifted sessions. Failures are non-fatal; the daemon
    // must come up even if the index is broken.
    if let Err(e) = indexer::reconcile_on_startup() {
        tracing::warn!(error = %e, "indexer reconcile_on_startup failed (non-fatal)");
    }
    maybe_print_first_run_hint();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime for punkgo-jack server")?;
    runtime.block_on(mcp::run_stdio(backend))
}

/// On first launch (sessions table empty), nudge the user to backfill.
/// Non-blocking — just one stderr line.
fn maybe_print_first_run_hint() {
    if let Ok(conn) = index::open_jack_db() {
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap_or(0);
        if count == 0 {
            eprintln!(
                "Tip: run 'punkgo-jack reindex --full' to backfill historical Claude Code sessions."
            );
        }
    }
}

fn run_reindex(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let mut opts = indexer::ReindexOptions::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--full" => opts.full = true,
            "--dry-run" => opts.dry_run = true,
            "--since" => {
                opts.since = Some(
                    args.next()
                        .context("--since requires an ISO 8601 timestamp")?,
                );
            }
            "--session" => {
                opts.session = Some(args.next().context("--session requires a session id")?);
            }
            other => anyhow::bail!("unknown reindex option: {other}"),
        }
    }

    if !opts.full && opts.since.is_none() && opts.session.is_none() {
        eprintln!("usage: punkgo-jack reindex --full | --since <TS> | --session <ID> [--dry-run]");
        std::process::exit(1);
    }

    let report = indexer::run_reindex(opts)?;
    println!("reindex complete:");
    println!("  files scanned    : {}", report.files_scanned);
    println!("  files failed     : {}", report.files_failed);
    println!("  sessions upserted: {}", report.sessions_upserted);
    println!("  turns upserted   : {}", report.turns_upserted);
    println!("  signatures       : {}", report.signatures_upserted);
    println!("  duration         : {:.1}s", report.duration_seconds);
    if !report.model_variant_breakdown.is_empty() {
        println!("  model variants:");
        let mut sorted: Vec<_> = report.model_variant_breakdown.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        for (v, c) in sorted {
            println!("    {c:6} {v}");
        }
    }
    Ok(())
}

fn run_export(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = export::parse_args(args)?;
    export::run_export(parsed)
}

fn run_ingest(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = ingest::parse_args(args)?;
    ingest::run(parsed)
}

fn run_setup(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let tool = args
        .next()
        .context("usage: punkgo-jack setup <TOOL> (e.g. claude-code)")?;
    setup::run_setup(&tool)
}

fn run_unsetup(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let mut tool = None;
    let mut purge = false;
    for arg in args {
        match arg.as_str() {
            "--purge" => purge = true,
            _ if tool.is_none() => tool = Some(arg),
            _ => anyhow::bail!("unexpected argument: {arg}"),
        }
    }
    let tool = tool.context("usage: punkgo-jack unsetup <TOOL> [--purge] (e.g. claude-code)")?;
    setup::run_unsetup(&tool, purge)
}

fn run_history(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = history::parse_history_args(args)?;
    history::run_history(parsed)
}

fn run_show(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = history::parse_show_args(args)?;
    history::run_show(parsed)
}

fn run_receipt(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = history::parse_receipt_args(args)?;
    history::run_receipt(parsed)
}

fn run_report(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = report::parse_args(args)?;
    report::run_report(parsed)
}

fn run_roast(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = roast::parse_args(args)?;
    roast::run_roast(parsed)
}

fn run_presence(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = presence::parse_args(args)?;
    presence::run_presence(parsed)
}

fn run_statusline(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let toggle = args
        .next()
        .context("usage: punkgo-jack statusline on|off")?;
    match toggle.as_str() {
        "on" => setup::toggle_statusline(true),
        "off" => setup::toggle_statusline(false),
        other => anyhow::bail!("unknown statusline option: {other} (expected on|off)"),
    }
}

fn run_verify(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = verify::parse_args(args)?;
    verify::run_verify(parsed)
}

#[cfg(feature = "rebuild-audit")]
fn run_rebuild_audit(args: &mut impl Iterator<Item = String>) -> Result<()> {
    let parsed = audit_rebuild::parse_args(args)?;
    audit_rebuild::run_rebuild(parsed)
}
