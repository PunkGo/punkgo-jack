use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use punkgo_core::protocol::{RequestEnvelope, RequestType};
use serde_json::{json, Map, Value};
use tracing::debug;

/// Detect punkgo hooks in existing settings.
/// Matches bare names, exe names, absolute paths (quoted or unquoted).
fn is_punkgo_hook(cmd: &str) -> bool {
    cmd.contains("punkgo-jack ingest")
        || cmd.contains("punkgo-jack.exe ingest")
        || cmd.contains("punkgo-jack\" ingest")
        || cmd.contains("punkgo-jack.exe\" ingest")
        || cmd.contains("punkgo-jack anchor")
        || cmd.contains("punkgo-jack.exe anchor")
        || cmd.contains("punkgo-jack\" anchor")
        || cmd.contains("punkgo-jack.exe\" anchor")
}

/// Build hook event definitions for the given executable path and source name.
/// The path is quoted to handle spaces in absolute paths (common on Windows).
/// Event names use Claude Code convention (PascalCase); callers convert if needed.
fn hook_events(exe: &str, source: &str) -> Vec<(&'static str, Vec<String>)> {
    // Quote the exe path if it contains spaces.
    let exe_cmd = if exe.contains(' ') {
        format!("\"{exe}\"")
    } else {
        exe.to_string()
    };
    vec![
        (
            "PreToolUse",
            vec![format!("{exe_cmd} ingest --source {source} --quiet")],
        ),
        (
            "PostToolUse",
            vec![format!("{exe_cmd} ingest --source {source} --quiet")],
        ),
        (
            "PostToolUseFailure",
            vec![format!("{exe_cmd} ingest --source {source} --quiet")],
        ),
        (
            "UserPromptSubmit",
            vec![format!("{exe_cmd} ingest --source {source} --quiet")],
        ),
        (
            "SessionStart",
            vec![
                format!("{exe_cmd} ingest --source {source} --event-type session_start --quiet"),
                format!("{exe_cmd} anchor --stale-only --quiet"),
            ],
        ),
        (
            "SessionEnd",
            vec![
                if source == "claude-code" {
                    format!(
                        "{exe_cmd} ingest --source {source} --event-type session_end --quiet --summary"
                    )
                } else {
                    format!("{exe_cmd} ingest --source {source} --event-type session_end --quiet")
                },
                format!("{exe_cmd} anchor --quiet"),
            ],
        ),
        // P0: agent completion — marks end of an action unit.
        (
            "Stop",
            vec![format!("{exe_cmd} ingest --source {source} --quiet")],
        ),
        // P0: subagent lifecycle — independent audit unit.
        (
            "SubagentStop",
            vec![format!("{exe_cmd} ingest --source {source} --quiet")],
        ),
        // P1: subagent start — pairs with SubagentStop.
        (
            "SubagentStart",
            vec![format!("{exe_cmd} ingest --source {source} --quiet")],
        ),
        // P0 (Claude Code only): async notifications (permission, idle, etc.)
        (
            "Notification",
            vec![format!("{exe_cmd} ingest --source {source} --quiet")],
        ),
    ]
}

// ---------------------------------------------------------------------------
// setup
// ---------------------------------------------------------------------------

/// Install punkgo hooks into a supported tool's settings.
pub fn run_setup(tool: &str) -> Result<()> {
    match tool {
        "claude-code" => setup_claude_code()?,
        "cursor" => setup_cursor()?,
        other => bail!("unsupported tool: '{other}'. Supported: claude-code, cursor"),
    }

    // Signal collection — help us understand who uses PunkGo and why.
    eprintln!();
    eprintln!("Setup complete for {tool}. Your next session is being recorded.");
    eprintln!();
    eprintln!("  Quick question — what made you try PunkGo?");
    eprintln!("  → 30 sec survey: https://punkgo.ai/why");
    eprintln!();

    Ok(())
}

fn setup_claude_code() -> Result<()> {
    // Resolve absolute path to the current executable so hooks don't depend on PATH.
    let exe_path =
        std::env::current_exe().context("failed to determine punkgo-jack executable path")?;
    // Claude Code runs hooks via /usr/bin/bash — backslashes are interpreted
    // as escape sequences. Convert to forward slashes on Windows.
    let exe_str = exe_path.to_string_lossy().replace('\\', "/");
    let events = hook_events(&exe_str, "claude-code");

    let settings_path = claude_code_settings_path()?;

    // Read existing settings (or start fresh).
    let mut settings = if settings_path.exists() {
        let content = std::fs::read_to_string(&settings_path)
            .with_context(|| format!("failed to read {}", settings_path.display()))?;
        serde_json::from_str::<Value>(&content)
            .with_context(|| format!("failed to parse {} as JSON", settings_path.display()))?
    } else {
        json!({})
    };

    let settings_obj = settings
        .as_object_mut()
        .context("settings.json root must be a JSON object")?;

    // Ensure "hooks" key exists.
    if !settings_obj.contains_key("hooks") {
        settings_obj.insert("hooks".into(), json!({}));
    }
    let hooks = settings_obj
        .get_mut("hooks")
        .and_then(Value::as_object_mut)
        .context("settings.json 'hooks' must be a JSON object")?;

    let mut installed = 0;
    let mut skipped = 0;

    for (event_name, commands) in &events {
        if merge_hook_entry(hooks, event_name, commands) {
            installed += 1;
        } else {
            skipped += 1;
        }
    }

    // Write back.
    if installed > 0 {
        // Ensure parent directory exists.
        if let Some(parent) = settings_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let content =
            serde_json::to_string_pretty(&settings).context("failed to serialize settings")?;
        std::fs::write(&settings_path, content.as_bytes())
            .with_context(|| format!("failed to write {}", settings_path.display()))?;
    }

    if installed > 0 {
        eprintln!(
            "hooks: {installed} installed into {}",
            settings_path.display()
        );
    } else {
        eprintln!("hooks: already installed ({skipped} skipped)");
    }

    // Detect and save punkgo-kerneld path for auto-start.
    detect_and_save_kerneld();

    // Inject punkgo status into Claude Code statusline.
    if let Err(e) = inject_statusline(&settings_path) {
        eprintln!("warning: failed to set up statusline: {e}");
    }

    // Try to seed the actor so ingest works immediately.
    try_seed_actor("claude-code");

    Ok(())
}

// ---------------------------------------------------------------------------
// Cursor setup
// ---------------------------------------------------------------------------

/// Map Claude Code PascalCase event names to Cursor camelCase equivalents.
fn cursor_event_name(claude_name: &str) -> Option<&str> {
    Some(match claude_name {
        "PreToolUse" => "preToolUse",
        "PostToolUse" => "postToolUse",
        "PostToolUseFailure" => "postToolUseFailure",
        "UserPromptSubmit" => "beforeSubmitPrompt",
        "SessionStart" => "sessionStart",
        "SessionEnd" => "sessionEnd",
        "Stop" => "stop",
        "SubagentStart" => "subagentStart",
        "SubagentStop" => "subagentStop",
        // Notification is Claude Code only — skip for Cursor.
        "Notification" => return None,
        // IMPORTANT: new Claude Code-only events must return None above.
        // Falling through passes PascalCase name to Cursor, which silently ignores it.
        other => other,
    })
}

fn setup_cursor() -> Result<()> {
    let exe_path =
        std::env::current_exe().context("failed to determine punkgo-jack executable path")?;
    // Cursor on Windows may execute hooks via WSL bash, which corrupts stdout.
    // Keep native backslash paths so Cursor uses cmd.exe / direct execution.
    let exe_str = exe_path.to_string_lossy().to_string();
    let events = hook_events(&exe_str, "cursor");

    // Warn if Claude Code hooks already exist — Cursor reads them automatically.
    let claude_path = claude_code_settings_path()?;
    if claude_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&claude_path) {
            if content.contains("punkgo-jack") {
                eprintln!("note: Cursor may also trigger Claude Code hooks (Third-party skills).");
                eprintln!(
                    "      This is handled automatically — duplicates are detected and skipped."
                );
            }
        }
    }

    let hooks_path = cursor_hooks_path()?;

    // Read existing hooks.json or start fresh.
    let mut root = if hooks_path.exists() {
        let content = std::fs::read_to_string(&hooks_path)
            .with_context(|| format!("failed to read {}", hooks_path.display()))?;
        serde_json::from_str::<Value>(&content)
            .with_context(|| format!("failed to parse {}", hooks_path.display()))?
    } else {
        json!({ "version": 1, "hooks": {} })
    };

    let root_obj = root
        .as_object_mut()
        .context("hooks.json root must be a JSON object")?;

    // Ensure version and hooks keys exist.
    root_obj.entry("version").or_insert_with(|| json!(1));
    root_obj.entry("hooks").or_insert_with(|| json!({}));

    let hooks = root_obj
        .get_mut("hooks")
        .and_then(Value::as_object_mut)
        .context("hooks.json 'hooks' must be a JSON object")?;

    let mut installed = 0;
    let mut skipped = 0;

    for (claude_event, commands) in &events {
        let Some(cursor_event) = cursor_event_name(claude_event) else {
            continue; // Event not applicable to Cursor (e.g. Notification).
        };

        // Get or create the array for this event.
        if !hooks.contains_key(cursor_event) {
            hooks.insert(cursor_event.into(), json!([]));
        }
        let entries = hooks
            .get_mut(cursor_event)
            .and_then(Value::as_array_mut)
            .expect("just ensured it's an array");

        // Check if punkgo hook already present.
        let already = entries.iter().any(|e| {
            e.get("command")
                .and_then(Value::as_str)
                .is_some_and(is_punkgo_hook)
        });

        if already {
            skipped += 1;
        } else {
            // Cursor hooks: each command is a separate entry (flat format).
            for cmd in commands {
                entries.push(json!({
                    "command": cmd,
                    "type": "command",
                    "timeout": 10
                }));
            }
            installed += 1;
        }
    }

    if installed > 0 {
        if let Some(parent) = hooks_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let content =
            serde_json::to_string_pretty(&root).context("failed to serialize hooks.json")?;
        std::fs::write(&hooks_path, content.as_bytes())
            .with_context(|| format!("failed to write {}", hooks_path.display()))?;
    }

    if installed > 0 {
        eprintln!("hooks: {installed} installed into {}", hooks_path.display());
    } else {
        eprintln!("hooks: already installed ({skipped} skipped)");
    }

    // Detect and save punkgo-kerneld path for auto-start.
    detect_and_save_kerneld();

    // Seed the actor.
    try_seed_actor("cursor");

    Ok(())
}

/// Merge hook commands into the hooks object for a given event.
/// Each event gets one entry with multiple commands in its `hooks` array.
/// Returns true if a new entry was added, false if already present.
fn merge_hook_entry(hooks: &mut Map<String, Value>, event_name: &str, commands: &[String]) -> bool {
    // Get or create the array for this event.
    if !hooks.contains_key(event_name) {
        hooks.insert(event_name.into(), json!([]));
    }
    let entries = hooks
        .get_mut(event_name)
        .and_then(Value::as_array_mut)
        .expect("just ensured it's an array");

    // Check if a punkgo hook already exists for this event.
    let already_present = entries.iter().any(|entry| {
        entry
            .get("hooks")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter().any(|h| {
                    h.get("command")
                        .and_then(Value::as_str)
                        .is_some_and(is_punkgo_hook)
                })
            })
            .unwrap_or(false)
    });

    if already_present {
        return false;
    }

    // Build hooks array with all commands for this event.
    let hook_items: Vec<Value> = commands
        .iter()
        .map(|cmd| {
            json!({
                "type": "command",
                "command": cmd,
                "timeout": 10,
                "async": true
            })
        })
        .collect();

    entries.push(json!({
        "matcher": ".*",
        "hooks": hook_items
    }));

    true
}

/// Detect punkgo-kerneld and save its path for auto-start on first session.
fn detect_and_save_kerneld() {
    // Check same directory as this executable first.
    let exe = std::env::current_exe().ok();
    let exe_dir = exe.as_ref().and_then(|p| p.parent());

    let kerneld_path = exe_dir
        .and_then(|dir| {
            ["punkgo-kerneld", "punkgo-kerneld.exe"]
                .iter()
                .map(|name| dir.join(name))
                .find(|p| p.exists())
        })
        .or_else(|| {
            // Try PATH — execute the binary directly (works on all platforms
            // including Git Bash on Windows).
            let check = std::process::Command::new("punkgo-kerneld")
                .arg("--version")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .output();
            if check.is_ok_and(|o| o.status.success()) {
                // Resolve full path via which/where if possible.
                let which_cmd = if cfg!(windows) { "where" } else { "which" };
                std::process::Command::new(which_cmd)
                    .arg("punkgo-kerneld")
                    .output()
                    .ok()
                    .filter(|o| o.status.success())
                    .and_then(|o| {
                        String::from_utf8_lossy(&o.stdout)
                            .trim()
                            .lines()
                            .next()
                            .filter(|s| !s.is_empty())
                            .map(PathBuf::from)
                    })
                    .or(Some(PathBuf::from("punkgo-kerneld")))
            } else {
                None
            }
        })
        .or_else(|| {
            // Check previously saved path (e.g. from manual config).
            crate::session::data_dir().ok().and_then(|dir| {
                std::fs::read_to_string(dir.join("kerneld_path"))
                    .ok()
                    .map(|s| std::path::PathBuf::from(s.trim()))
                    .filter(|p| p.exists())
            })
        });

    match kerneld_path {
        Some(path) => {
            if let Err(e) = crate::daemon::save_kerneld_path(&path) {
                eprintln!("warning: failed to save kerneld path: {e}");
            } else {
                eprintln!("kerneld: {} (auto-start enabled)", path.display());
            }
        }
        None => {
            eprintln!("kerneld: not found — attempting install...");
            match install_kerneld() {
                Ok(path) => {
                    if let Err(e) = crate::daemon::save_kerneld_path(&path) {
                        eprintln!("warning: failed to save kerneld path: {e}");
                    } else {
                        eprintln!("kerneld: {} (auto-start enabled)", path.display());
                    }
                }
                Err(e) => {
                    eprintln!("kerneld: install failed: {e}");
                    eprintln!("  hooks are installed but events will be buffered until the kernel is available.");
                    eprintln!("  manual install: cargo install punkgo-kernel");
                }
            }
        }
    }
}

/// Install punkgo-kerneld from crates.io via `cargo install`.
/// Returns the installed binary path on success.
fn install_kerneld() -> Result<PathBuf> {
    use std::process::{Command, Stdio};

    // Check that cargo is available.
    let cargo = Command::new("cargo")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if !cargo.is_ok_and(|s| s.success()) {
        bail!("cargo not found — install Rust from https://rustup.rs");
    }

    eprintln!("  running: cargo install punkgo-kernel");
    let status = Command::new("cargo")
        .args(["install", "punkgo-kernel"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("failed to run cargo install")?;

    if !status.success() {
        bail!("cargo install punkgo-kernel exited with {status}");
    }

    // Find the installed binary — should now be in cargo bin dir.
    let cargo_home = std::env::var("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|_| {
            crate::session::home_dir()
                .map(|h| h.join(".cargo"))
                .context("cannot determine home directory")
        })?;

    let bin_dir = cargo_home.join("bin");
    for name in &["punkgo-kerneld", "punkgo-kerneld.exe"] {
        let candidate = bin_dir.join(name);
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    // Fallback: bare name (cargo bin should be in PATH after install).
    Ok(PathBuf::from("punkgo-kerneld"))
}

/// Attempt to seed an actor via IPC. Failures are logged but do not abort setup,
/// because the daemon may not be running yet — hooks are the primary deliverable.
fn try_seed_actor(actor_id: &str) {
    use crate::ipc_client::{new_request_id, IpcClient};

    let client = IpcClient::from_env(None);
    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Submit,
        payload: json!({
            "actor_id": "root",
            "action_type": "create",
            "target": "ledger/actor",
            "payload": {
                "actor_id": actor_id,
                "actor_type": "agent",
                "purpose": format!("{actor_id}-adapter"),
                "energy_balance": 100_000,
                "energy_share": 50.0
            }
        }),
    };

    match client.send(&req) {
        Ok(resp) if resp.status == "ok" => {
            eprintln!("actor '{actor_id}' seeded (energy=100000, share=50.0)");
        }
        Ok(resp) => {
            let msg = resp
                .payload
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("");
            if msg.contains("already exists") {
                eprintln!("actor '{actor_id}' already registered");
            } else {
                eprintln!("actor seed: {msg} (will auto-retry on first session)");
            }
        }
        Err(_) => {
            // Daemon not running yet — this is normal. Auto-start will
            // handle it on the first Claude Code session (daemon.rs).
            eprintln!("actor: will be auto-seeded when kernel starts");
        }
    }
}

// ---------------------------------------------------------------------------
// statusline
// ---------------------------------------------------------------------------

const PUNKGO_SL_BEGIN: &str = "# BEGIN PUNKGO STATUSLINE";
const PUNKGO_SL_END: &str = "# END PUNKGO STATUSLINE";

/// The shell snippet injected into the statusline script.
/// Shows "go:N" with per-session event count when kernel is running, "punkgo:off" otherwise.
/// Expects `$input` to contain the raw JSON from Claude Code (including `session_id`).
fn punkgo_statusline_snippet() -> String {
    format!(
        r#"{begin}
# Auto-injected by punkgo-jack setup. Do not edit this block manually.
# Cross-platform daemon detection: pgrep (Unix) or tasklist (Windows/MSYS)
_pgo_running=0
if command -v pgrep >/dev/null 2>&1; then
  pgrep -x punkgo-kerneld >/dev/null 2>&1 && _pgo_running=1
elif command -v tasklist >/dev/null 2>&1; then
  tasklist 2>/dev/null | grep -qi punkgo-kerneld && _pgo_running=1
fi
if [ "$_pgo_running" = "1" ]; then
  _pgo_de=0
  _pgo_de_file="$HOME/.punkgo/daily_energy.json"
  if [ -f "$_pgo_de_file" ]; then
    _pgo_de_date=$(sed -n 's/.*"date"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$_pgo_de_file" 2>/dev/null)
    _pgo_today=$(date -u +%Y-%m-%d 2>/dev/null || date +%Y-%m-%d)
    if [ "$_pgo_de_date" = "$_pgo_today" ]; then
      _pgo_de=$(sed -n 's/.*"energy"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p' "$_pgo_de_file" 2>/dev/null)
      [ -z "$_pgo_de" ] && _pgo_de=0
    fi
  fi
  if [ "$_pgo_de" -ge 1000 ] 2>/dev/null; then
    _pgo_ef="$(( _pgo_de / 1000 )).$(( (_pgo_de % 1000) / 100 ))k"
  else
    _pgo_ef="$_pgo_de"
  fi
  out="${{out}}${{SEP}}$(printf '\033[32m%s\033[0m' "punkgo:⚡$_pgo_ef")"
else
  out="${{out}}${{SEP}}$(printf '\033[2m%s\033[0m' 'punkgo:off')"
fi
{end}"#,
        begin = PUNKGO_SL_BEGIN,
        end = PUNKGO_SL_END,
    )
}

/// A minimal statusline script created by punkgo when none exists.
fn punkgo_default_statusline() -> String {
    r#"#!/usr/bin/env bash
# PunkGo statusline — created by punkgo-jack setup.
# Feel free to customize. The block between BEGIN/END markers is managed by punkgo.

input=$(cat)

# Simple JSON value extractors (no jq dependency)
json_str() {
  echo "$input" | sed -n "s/.*\"${1}\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -1
}

cwd=$(json_str "cwd")
dir_name=$(basename "$cwd" 2>/dev/null)

RESET='\033[0m'
DIM='\033[2m'
CYAN='\033[36m'
GREEN='\033[32m'

SEP=$(printf "${DIM} | ${RESET}")
out=""

if [ -n "$dir_name" ]; then
  out=$(printf "${CYAN}%s${RESET}" "$dir_name")
fi

# BEGIN PUNKGO STATUSLINE
# Auto-injected by punkgo-jack setup. Do not edit this block manually.
# Cross-platform daemon detection: pgrep (Unix) or tasklist (Windows/MSYS)
_pgo_running=0
if command -v pgrep >/dev/null 2>&1; then
  pgrep -x punkgo-kerneld >/dev/null 2>&1 && _pgo_running=1
elif command -v tasklist >/dev/null 2>&1; then
  tasklist 2>/dev/null | grep -qi punkgo-kerneld && _pgo_running=1
fi
if [ "$_pgo_running" = "1" ]; then
  _pgo_de=0
  _pgo_de_file="$HOME/.punkgo/daily_energy.json"
  if [ -f "$_pgo_de_file" ]; then
    _pgo_de_date=$(sed -n 's/.*"date"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$_pgo_de_file" 2>/dev/null)
    _pgo_today=$(date -u +%Y-%m-%d 2>/dev/null || date +%Y-%m-%d)
    if [ "$_pgo_de_date" = "$_pgo_today" ]; then
      _pgo_de=$(sed -n 's/.*"energy"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p' "$_pgo_de_file" 2>/dev/null)
      [ -z "$_pgo_de" ] && _pgo_de=0
    fi
  fi
  if [ "$_pgo_de" -ge 1000 ] 2>/dev/null; then
    _pgo_ef="$(( _pgo_de / 1000 )).$(( (_pgo_de % 1000) / 100 ))k"
  else
    _pgo_ef="$_pgo_de"
  fi
  out="${out}${SEP}$(printf '\033[32m%s\033[0m' "punkgo:⚡$_pgo_ef")"
else
  out="${out}${SEP}$(printf '\033[2m%s\033[0m' 'punkgo:off')"
fi
# END PUNKGO STATUSLINE

now=$(date +%H:%M)
out="${out}${SEP}$(printf "${DIM}%s${RESET}" "$now")"

printf "%s" "$out"
"#
    .to_string()
}

/// Inject the punkgo statusline snippet into the user's statusline script.
/// If no statusline is configured, creates a minimal one with punkgo status.
fn inject_statusline(settings_path: &std::path::Path) -> Result<()> {
    // Read settings to find the statusline command.
    let content = std::fs::read_to_string(settings_path)
        .with_context(|| format!("failed to read {}", settings_path.display()))?;
    let mut settings: Value = serde_json::from_str(&content)?;

    let existing_script = settings
        .get("statusLine")
        .and_then(|sl| sl.get("command"))
        .and_then(Value::as_str)
        .and_then(|cmd| {
            // Extract script path from "bash /path/to/script.sh" or similar.
            let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
            if parts.len() == 2 {
                Some(parts[1].to_string())
            } else {
                None
            }
        });

    if let Some(ref path_str) = existing_script {
        let script_path = PathBuf::from(path_str);
        if script_path.exists() {
            return inject_into_existing_statusline(&script_path);
        }
    }

    // No statusline configured (or script file missing) — create a new one.
    let home = crate::session::home_dir().context("cannot determine home directory")?;
    let script_path = home.join(".claude").join("punkgo-statusline.sh");

    std::fs::write(&script_path, punkgo_default_statusline().as_bytes())
        .with_context(|| format!("failed to write {}", script_path.display()))?;

    // Update settings.json with the new statusLine config.
    let script_cmd = format!("bash {}", script_path.to_string_lossy().replace('\\', "/"));
    let settings_obj = settings
        .as_object_mut()
        .context("settings.json root must be a JSON object")?;
    settings_obj.insert(
        "statusLine".into(),
        json!({ "type": "command", "command": script_cmd }),
    );

    let updated = serde_json::to_string_pretty(&settings)?;
    std::fs::write(settings_path, updated.as_bytes())
        .with_context(|| format!("failed to write {}", settings_path.display()))?;

    eprintln!("statusline created: {}", script_path.display());
    eprintln!("punkgo status will appear in the Claude Code bottom bar");
    Ok(())
}

/// Inject the punkgo snippet into an existing statusline script.
fn inject_into_existing_statusline(script_path: &std::path::Path) -> Result<()> {
    let script = std::fs::read_to_string(script_path)
        .with_context(|| format!("failed to read {}", script_path.display()))?;

    let snippet = punkgo_statusline_snippet();

    // Already injected? Replace the existing block with the latest version.
    if script.contains(PUNKGO_SL_BEGIN) {
        let mut result = String::new();
        let mut inside_block = false;
        let mut replaced = false;
        for line in script.lines() {
            if line.trim() == PUNKGO_SL_BEGIN {
                inside_block = true;
                continue;
            }
            if line.trim() == PUNKGO_SL_END {
                inside_block = false;
                if !replaced {
                    result.push_str(&snippet);
                    result.push('\n');
                    replaced = true;
                }
                continue;
            }
            if !inside_block {
                result.push_str(line);
                result.push('\n');
            }
        }
        std::fs::write(script_path, result.as_bytes())
            .with_context(|| format!("failed to write {}", script_path.display()))?;
        eprintln!("statusline updated with punkgo indicator");
        return Ok(());
    }

    // Find the last `printf "%s" "$out"` or similar output line and inject before it.
    let lines: Vec<&str> = script.lines().collect();

    let mut insert_pos = None;
    for (i, line) in lines.iter().enumerate().rev() {
        let trimmed = line.trim();
        if (trimmed.contains("printf") || trimmed.contains("echo")) && trimmed.contains("$out") {
            insert_pos = Some(i);
            break;
        }
    }

    let new_script = if let Some(pos) = insert_pos {
        let mut result = lines[..pos].join("\n");
        result.push('\n');
        result.push_str(&snippet);
        result.push('\n');
        result.push_str(&lines[pos..].join("\n"));
        result.push('\n');
        result
    } else {
        // Fallback: append at the end.
        let mut result = script.clone();
        if !result.ends_with('\n') {
            result.push('\n');
        }
        result.push_str(&snippet);
        result.push('\n');
        result
    };

    std::fs::write(script_path, new_script.as_bytes())
        .with_context(|| format!("failed to write {}", script_path.display()))?;

    eprintln!("statusline updated with punkgo indicator");
    Ok(())
}

/// Remove the punkgo statusline snippet from the user's statusline script.
/// If the script was created by punkgo (`punkgo-statusline.sh`), remove it entirely
/// and clean up the statusLine config from settings.json.
fn remove_statusline(settings_path: &std::path::Path) -> Result<()> {
    let content = std::fs::read_to_string(settings_path)
        .with_context(|| format!("failed to read {}", settings_path.display()))?;
    let mut settings: Value = serde_json::from_str(&content)?;

    let script_path_str = settings
        .get("statusLine")
        .and_then(|sl| sl.get("command"))
        .and_then(Value::as_str)
        .and_then(|cmd| {
            let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
            if parts.len() == 2 {
                Some(parts[1].to_string())
            } else {
                None
            }
        });

    let Some(script_path_str) = script_path_str else {
        return Ok(());
    };

    let script_path = PathBuf::from(&script_path_str);
    if !script_path.exists() {
        return Ok(());
    }

    // If this is our own statusline file, remove it entirely + settings entry.
    let is_punkgo_owned = script_path
        .file_name()
        .and_then(|f| f.to_str())
        .is_some_and(|f| f == "punkgo-statusline.sh");

    if is_punkgo_owned {
        std::fs::remove_file(&script_path)
            .with_context(|| format!("failed to remove {}", script_path.display()))?;

        if let Some(obj) = settings.as_object_mut() {
            obj.remove("statusLine");
        }
        let updated = serde_json::to_string_pretty(&settings)?;
        std::fs::write(settings_path, updated.as_bytes())
            .with_context(|| format!("failed to write {}", settings_path.display()))?;

        eprintln!("statusline: punkgo-statusline.sh removed");
        return Ok(());
    }

    // User's own script — just remove the punkgo block.
    let script = std::fs::read_to_string(&script_path)
        .with_context(|| format!("failed to read {}", script_path.display()))?;

    if !script.contains(PUNKGO_SL_BEGIN) {
        return Ok(());
    }

    let mut result = String::new();
    let mut inside_block = false;
    for line in script.lines() {
        if line.trim() == PUNKGO_SL_BEGIN {
            inside_block = true;
            continue;
        }
        if line.trim() == PUNKGO_SL_END {
            inside_block = false;
            continue;
        }
        if !inside_block {
            result.push_str(line);
            result.push('\n');
        }
    }

    std::fs::write(&script_path, result.as_bytes())
        .with_context(|| format!("failed to write {}", script_path.display()))?;

    eprintln!("statusline: punkgo indicator removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// statusline toggle
// ---------------------------------------------------------------------------

/// Toggle the punkgo energy display in the statusline.
/// `on`: re-inject the punkgo snippet. `off`: remove the punkgo snippet.
/// Hooks and recording are unaffected — only the visual indicator changes.
pub fn toggle_statusline(on: bool) -> Result<()> {
    let settings_path =
        claude_code_settings_path().context("cannot find Claude Code settings path")?;
    if !settings_path.exists() {
        bail!(
            "Claude Code settings not found at {}. Run `punkgo-jack setup claude-code` first.",
            settings_path.display()
        );
    }
    if on {
        inject_statusline(&settings_path)?;
        eprintln!("statusline: punkgo energy display enabled");
    } else {
        remove_statusline(&settings_path)?;
        eprintln!("statusline: punkgo energy display disabled (recording continues)");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// unsetup
// ---------------------------------------------------------------------------

/// Remove punkgo hooks from a supported tool's settings.
/// If `purge` is true, also remove jack's local state files (sessions, daily energy, spillover, kerneld_path).
pub fn run_unsetup(tool: &str, purge: bool) -> Result<()> {
    match tool {
        "claude-code" => unsetup_claude_code(purge),
        "cursor" => unsetup_cursor(purge),
        other => bail!("unsupported tool: '{other}'. Supported: claude-code, cursor"),
    }
}

fn unsetup_claude_code(purge: bool) -> Result<()> {
    let settings_path = claude_code_settings_path()?;

    if !settings_path.exists() {
        debug!(path = %settings_path.display(), "no settings file found, nothing to remove");
        return Ok(());
    }

    let content = std::fs::read_to_string(&settings_path)
        .with_context(|| format!("failed to read {}", settings_path.display()))?;
    let mut settings: Value = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse {}", settings_path.display()))?;

    let Some(hooks) = settings
        .as_object_mut()
        .and_then(|obj| obj.get_mut("hooks"))
        .and_then(Value::as_object_mut)
    else {
        debug!("no hooks section found, nothing to remove");
        return Ok(());
    };

    let mut removed = 0;

    // For each hook event, remove entries containing punkgo commands.
    let event_keys: Vec<String> = hooks.keys().cloned().collect();
    for key in &event_keys {
        let Some(entries) = hooks.get_mut(key).and_then(Value::as_array_mut) else {
            continue;
        };

        let before = entries.len();
        entries.retain(|entry| {
            !entry
                .get("hooks")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter().any(|h| {
                        h.get("command")
                            .and_then(Value::as_str)
                            .is_some_and(is_punkgo_hook)
                    })
                })
                .unwrap_or(false)
        });
        removed += before - entries.len();
    }

    // Clean up empty arrays.
    let empty_keys: Vec<String> = hooks
        .iter()
        .filter(|(_, v)| v.as_array().is_some_and(|a| a.is_empty()))
        .map(|(k, _)| k.clone())
        .collect();
    for key in &empty_keys {
        hooks.remove(key);
    }

    // Remove "hooks" key entirely if empty.
    if hooks.is_empty() {
        if let Some(obj) = settings.as_object_mut() {
            obj.remove("hooks");
        }
    }

    // Write back.
    let content =
        serde_json::to_string_pretty(&settings).context("failed to serialize settings")?;
    std::fs::write(&settings_path, content.as_bytes())
        .with_context(|| format!("failed to write {}", settings_path.display()))?;

    debug!(
        removed,
        settings = %settings_path.display(),
        "unsetup claude-code complete"
    );

    // Remove statusline snippet.
    if let Err(e) = remove_statusline(&settings_path) {
        eprintln!("warning: failed to clean up statusline: {e}");
    }

    if purge {
        purge_jack_state()?;
    }

    Ok(())
}

fn unsetup_cursor(purge: bool) -> Result<()> {
    let hooks_path = cursor_hooks_path()?;

    if !hooks_path.exists() {
        debug!(path = %hooks_path.display(), "no hooks.json found, nothing to remove");
        return Ok(());
    }

    let content = std::fs::read_to_string(&hooks_path)
        .with_context(|| format!("failed to read {}", hooks_path.display()))?;
    let mut root: Value = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse {}", hooks_path.display()))?;

    let Some(hooks) = root
        .as_object_mut()
        .and_then(|obj| obj.get_mut("hooks"))
        .and_then(Value::as_object_mut)
    else {
        debug!("no hooks section found, nothing to remove");
        return Ok(());
    };

    let mut removed = 0;

    // Cursor format: hooks are flat (no nested "hooks" array).
    let event_keys: Vec<String> = hooks.keys().cloned().collect();
    for key in &event_keys {
        let Some(entries) = hooks.get_mut(key).and_then(Value::as_array_mut) else {
            continue;
        };
        let before = entries.len();
        entries.retain(|entry| {
            !entry
                .get("command")
                .and_then(Value::as_str)
                .is_some_and(is_punkgo_hook)
        });
        removed += before - entries.len();
    }

    // Clean up empty arrays.
    let empty_keys: Vec<String> = hooks
        .iter()
        .filter(|(_, v)| v.as_array().is_some_and(|a| a.is_empty()))
        .map(|(k, _)| k.clone())
        .collect();
    for key in &empty_keys {
        hooks.remove(key);
    }

    // Write back.
    let content = serde_json::to_string_pretty(&root).context("failed to serialize hooks.json")?;
    std::fs::write(&hooks_path, content.as_bytes())
        .with_context(|| format!("failed to write {}", hooks_path.display()))?;

    debug!(
        removed,
        path = %hooks_path.display(),
        "unsetup cursor complete"
    );

    if purge {
        purge_jack_state()?;
    }

    Ok(())
}

/// Remove jack's local state files (sessions, daily energy, spillover, kerneld_path).
/// Does NOT touch kernel data (~/.punkgo/state/, ~/.punkgo/blobs/).
fn purge_jack_state() -> Result<()> {
    let data_dir = crate::session::data_dir()?;
    let targets = [
        ("sessions", true), // directory
        ("daily_energy.json", false),
        ("spillover.jsonl", false),
        ("kerneld_path", false),
    ];
    let mut cleaned = 0;
    for (name, is_dir) in &targets {
        let path = data_dir.join(name);
        if !path.exists() {
            continue;
        }
        if *is_dir {
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        } else {
            std::fs::remove_file(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
        cleaned += 1;
    }
    if cleaned > 0 {
        eprintln!(
            "purge: removed {cleaned} jack state file(s) from {}",
            data_dir.display()
        );
    } else {
        eprintln!("purge: no jack state files to remove");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// path helpers
// ---------------------------------------------------------------------------

fn claude_code_settings_path() -> Result<PathBuf> {
    let home = crate::session::home_dir()
        .context("cannot determine home directory. Set HOME (Unix) or USERPROFILE (Windows).")?;
    Ok(home.join(".claude").join("settings.json"))
}

fn cursor_hooks_path() -> Result<PathBuf> {
    let home = crate::session::home_dir()
        .context("cannot determine home directory. Set HOME (Unix) or USERPROFILE (Windows).")?;
    Ok(home.join(".cursor").join("hooks.json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn merge_hook_entry_adds_new() {
        let mut hooks = Map::new();
        let cmds = vec!["punkgo-jack ingest --source claude-code --quiet".to_string()];
        let added = merge_hook_entry(&mut hooks, "PostToolUse", &cmds);
        assert!(added);
        assert!(hooks.contains_key("PostToolUse"));

        let entries = hooks["PostToolUse"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["matcher"], ".*");
        // Single command in hooks array
        assert_eq!(entries[0]["hooks"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn merge_hook_entry_skips_duplicate() {
        let mut hooks = Map::new();
        let cmds = vec!["punkgo-jack ingest --source claude-code --quiet".to_string()];
        merge_hook_entry(&mut hooks, "PostToolUse", &cmds);
        let added_again = merge_hook_entry(&mut hooks, "PostToolUse", &cmds);
        assert!(!added_again);

        let entries = hooks["PostToolUse"].as_array().unwrap();
        assert_eq!(entries.len(), 1); // Still only one entry.
    }

    #[test]
    fn merge_preserves_existing_hooks() {
        let mut hooks = Map::new();
        // Pre-existing user hook.
        hooks.insert(
            "PostToolUse".into(),
            json!([
                {
                    "matcher": "Bash",
                    "hooks": [{ "type": "command", "command": "echo 'user hook'" }]
                }
            ]),
        );

        let cmds = vec!["punkgo-jack ingest --source claude-code --quiet".to_string()];
        let added = merge_hook_entry(&mut hooks, "PostToolUse", &cmds);
        assert!(added);

        let entries = hooks["PostToolUse"].as_array().unwrap();
        assert_eq!(entries.len(), 2); // User hook + punkgo hook.
    }

    #[test]
    fn merge_multi_command_entry() {
        let mut hooks = Map::new();
        let cmds = vec![
            "punkgo-jack ingest --source claude-code --quiet".to_string(),
            "punkgo-jack anchor --quiet".to_string(),
        ];
        let added = merge_hook_entry(&mut hooks, "SessionEnd", &cmds);
        assert!(added);

        let entries = hooks["SessionEnd"].as_array().unwrap();
        assert_eq!(entries.len(), 1); // One entry
                                      // Two commands in the hooks array
        let inner = entries[0]["hooks"].as_array().unwrap();
        assert_eq!(inner.len(), 2);
        assert!(inner[0]["command"].as_str().unwrap().contains("ingest"));
        assert!(inner[1]["command"].as_str().unwrap().contains("anchor"));
    }

    #[test]
    fn setup_and_unsetup_round_trip() {
        let temp = TempDir::new().unwrap();
        let settings_path = temp.path().join("settings.json");

        // Write initial settings with some pre-existing config.
        std::fs::write(
            &settings_path,
            serde_json::to_string_pretty(&json!({
                "permissions": { "allow": ["Bash"] }
            }))
            .unwrap(),
        )
        .unwrap();

        // Simulate setup: read, merge, write.
        let content = std::fs::read_to_string(&settings_path).unwrap();
        let mut settings: Value = serde_json::from_str(&content).unwrap();
        let obj = settings.as_object_mut().unwrap();
        obj.insert("hooks".into(), json!({}));
        let hooks = obj["hooks"].as_object_mut().unwrap();

        let events = hook_events("punkgo-jack", "claude-code");
        for (event, cmds) in &events {
            merge_hook_entry(hooks, event, cmds);
        }
        std::fs::write(
            &settings_path,
            serde_json::to_string_pretty(&settings).unwrap(),
        )
        .unwrap();

        // Verify hooks are present.
        let content = std::fs::read_to_string(&settings_path).unwrap();
        let settings: Value = serde_json::from_str(&content).unwrap();
        let hooks = settings["hooks"].as_object().unwrap();
        assert_eq!(hooks.len(), 10); // 10 unique event names
                                     // SessionEnd has 1 entry with 2 inner hooks (ingest + anchor).
        let se = hooks["SessionEnd"].as_array().unwrap();
        assert_eq!(se.len(), 1);
        assert_eq!(se[0]["hooks"].as_array().unwrap().len(), 2);
        // SessionStart has 1 entry with 2 inner hooks (ingest + anchor).
        let ss = hooks["SessionStart"].as_array().unwrap();
        assert_eq!(ss.len(), 1);
        assert_eq!(ss[0]["hooks"].as_array().unwrap().len(), 2);
        // Pre-existing config preserved.
        assert!(settings["permissions"]["allow"].is_array());

        // Simulate unsetup: read, remove, write.
        let mut settings: Value = serde_json::from_str(&content).unwrap();
        let hooks = settings["hooks"].as_object_mut().unwrap();

        let event_keys: Vec<String> = hooks.keys().cloned().collect();
        for key in &event_keys {
            if let Some(entries) = hooks.get_mut(key).and_then(Value::as_array_mut) {
                entries.retain(|entry| {
                    !entry
                        .get("hooks")
                        .and_then(Value::as_array)
                        .map(|arr| {
                            arr.iter().any(|h| {
                                h.get("command")
                                    .and_then(Value::as_str)
                                    .is_some_and(is_punkgo_hook)
                            })
                        })
                        .unwrap_or(false)
                });
            }
        }

        // Clean up empty arrays and hooks key.
        let empty: Vec<String> = hooks
            .iter()
            .filter(|(_, v)| v.as_array().is_some_and(|a| a.is_empty()))
            .map(|(k, _)| k.clone())
            .collect();
        for k in &empty {
            hooks.remove(k);
        }
        if hooks.is_empty() {
            settings.as_object_mut().unwrap().remove("hooks");
        }

        std::fs::write(
            &settings_path,
            serde_json::to_string_pretty(&settings).unwrap(),
        )
        .unwrap();

        // Verify hooks are removed, other config preserved.
        let content = std::fs::read_to_string(&settings_path).unwrap();
        let settings: Value = serde_json::from_str(&content).unwrap();
        assert!(settings.get("hooks").is_none());
        assert!(settings["permissions"]["allow"].is_array());
    }

    #[test]
    fn is_punkgo_hook_detects_absolute_paths() {
        // Bare name
        assert!(is_punkgo_hook(
            "punkgo-jack ingest --source claude-code --quiet"
        ));
        // Windows exe
        assert!(is_punkgo_hook(
            "punkgo-jack.exe ingest --source claude-code"
        ));
        // Absolute path (Unix)
        assert!(is_punkgo_hook(
            "/home/user/.cargo/bin/punkgo-jack ingest --source claude-code"
        ));
        // Absolute path (Windows, quoted)
        assert!(is_punkgo_hook(
            "\"C:\\Users\\user\\.cargo\\bin\\punkgo-jack.exe\" ingest --source claude-code"
        ));
        // Quoted path with spaces
        assert!(is_punkgo_hook(
            "\"C:\\Program Files\\punkgo-jack\" ingest --source claude-code"
        ));
        // Not a punkgo hook
        assert!(!is_punkgo_hook("echo 'user hook'"));
    }

    #[test]
    fn hook_events_quotes_paths_with_spaces() {
        let events = hook_events("/path/with spaces/punkgo-jack", "claude-code");
        assert!(events[0].1[0].starts_with("\"/path/with spaces/punkgo-jack\""));

        let events = hook_events("/simple/path/punkgo-jack", "claude-code");
        assert!(events[0].1[0].starts_with("/simple/path/punkgo-jack "));
    }

    #[test]
    fn setup_creates_settings_from_scratch() {
        let temp = TempDir::new().unwrap();
        let settings_path = temp.path().join("settings.json");

        // No file exists initially.
        assert!(!settings_path.exists());

        let mut settings = json!({});
        let obj = settings.as_object_mut().unwrap();
        obj.insert("hooks".into(), json!({}));
        let hooks = obj["hooks"].as_object_mut().unwrap();

        let events = hook_events("punkgo-jack", "claude-code");
        for (event, cmds) in &events {
            merge_hook_entry(hooks, event, cmds);
        }

        std::fs::write(
            &settings_path,
            serde_json::to_string_pretty(&settings).unwrap(),
        )
        .unwrap();

        let content = std::fs::read_to_string(&settings_path).unwrap();
        let settings: Value = serde_json::from_str(&content).unwrap();
        let hooks = settings["hooks"].as_object().unwrap();
        assert_eq!(hooks.len(), 10); // 10 unique event names
        assert!(hooks.contains_key("PreToolUse"));
        assert!(hooks.contains_key("PostToolUse"));
        assert!(hooks.contains_key("PostToolUseFailure"));
        assert!(hooks.contains_key("UserPromptSubmit"));
        assert!(hooks.contains_key("SessionStart"));
        assert!(hooks.contains_key("SessionEnd"));
        // SessionEnd has 1 entry with 2 inner hooks (ingest + anchor).
        assert_eq!(
            hooks["SessionEnd"].as_array().unwrap()[0]["hooks"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            hooks["SessionStart"].as_array().unwrap()[0]["hooks"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }
}
