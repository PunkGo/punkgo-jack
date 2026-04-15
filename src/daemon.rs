use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use crate::ipc_client::{new_request_id, IpcClient};

/// Ensure the kernel daemon is running. If not reachable via IPC,
/// attempt to find and auto-start `punkgo-kerneld`.
///
/// Returns a status message if the daemon was auto-started, or `None` if
/// it was already running. Returns `Err` if the daemon could not be reached.
pub fn ensure_kernel_running(client: &IpcClient) -> Result<Option<String>, String> {
    // Quick ping — already running?
    if ping(client) {
        return Ok(None);
    }

    // Try to find kerneld binary.
    let Some(kerneld_path) = find_kerneld() else {
        warn!("punkgo-kerneld not found; cannot auto-start daemon");
        return Err("punkgo-kerneld not found".into());
    };

    // NOTE: No more kill_stale_daemon() — daemon lifecycle managed by kernel's
    // flock-based locking. Per-PID sockets/pipes avoid stale handle issues.

    info!(path = %kerneld_path.display(), "auto-starting punkgo-kerneld");

    if let Err(e) = spawn_kerneld(&kerneld_path) {
        warn!(error = %e, "failed to start punkgo-kerneld");
        return Err(format!("failed to start kernel: {e}"));
    }

    // Wait for daemon to become available (up to 5 seconds).
    // Re-read daemon.addr each iteration since the new daemon writes a new address.
    for attempt in 1..=10 {
        thread::sleep(Duration::from_millis(500));
        // Create a fresh client that re-reads daemon.addr
        let fresh_client = IpcClient::from_env(None);
        if ping(&fresh_client) {
            info!(attempts = attempt, "punkgo-kerneld is ready");
            try_seed_actor(&fresh_client);
            return Ok(Some(format!(
                "punkgo kernel started ({}ms) — AI actions are being recorded",
                attempt * 500
            )));
        }
    }

    warn!("punkgo-kerneld started but not responding after 5s");
    Err("kernel started but not responding after 5s".into())
}

/// Ping the kernel daemon with a lightweight stats query.
fn ping(client: &IpcClient) -> bool {
    use punkgo_core::protocol::{RequestEnvelope, RequestType};
    use serde_json::json;

    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Read,
        payload: json!({ "kind": "stats" }),
    };
    client.send(&req).is_ok()
}

/// Find the `punkgo-kerneld` binary.
///
/// Search order:
/// 1. Configured path in ~/.punkgo/kerneld_path (saved by `setup`)
/// 2. Same directory as the current executable (cargo install puts them together)
/// 3. PATH lookup via `where` (Windows) or `which` (Unix)
fn find_kerneld() -> Option<PathBuf> {
    // 1. Saved path from setup.
    if let Ok(data_dir) = crate::session::data_dir() {
        let saved = data_dir.join("kerneld_path");
        if let Ok(path_str) = std::fs::read_to_string(&saved) {
            let path = PathBuf::from(path_str.trim());
            if path.exists() {
                return Some(path);
            }
        }
    }

    // 2. Same directory as current executable.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            for name in &["punkgo-kerneld", "punkgo-kerneld.exe"] {
                let candidate = dir.join(name);
                if candidate.exists() {
                    return Some(candidate);
                }
            }
        }
    }

    // 3. PATH lookup — try running the binary directly (works on all platforms
    // including Git Bash on Windows, where `where` is unavailable).
    if let Ok(output) = Command::new("punkgo-kerneld")
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
    {
        if output.status.success() {
            // Binary found in PATH. Resolve its full path via the same
            // directory search, or fall back to the bare name.
            let which_cmd = if cfg!(windows) { "where" } else { "which" };
            if let Ok(which_out) = Command::new(which_cmd).arg("punkgo-kerneld").output() {
                if which_out.status.success() {
                    let stdout = String::from_utf8_lossy(&which_out.stdout);
                    if let Some(line) = stdout.trim().lines().next() {
                        if !line.is_empty() {
                            return Some(PathBuf::from(line));
                        }
                    }
                }
            }
            // which/where failed but the binary exists — use bare name.
            return Some(PathBuf::from("punkgo-kerneld"));
        }
    }

    None
}

/// Save the kerneld path to ~/.punkgo/kerneld_path for later auto-start.
pub fn save_kerneld_path(path: &Path) -> Result<()> {
    let data_dir = crate::session::data_dir()?;
    let target = data_dir.join("kerneld_path");
    std::fs::write(&target, path.to_string_lossy().as_bytes())
        .with_context(|| format!("failed to write {}", target.display()))?;
    Ok(())
}

/// Spawn `punkgo-kerneld` as a detached background process.
fn spawn_kerneld(path: &PathBuf) -> Result<()> {
    let state_dir = default_state_dir()?;
    std::fs::create_dir_all(&state_dir)
        .with_context(|| format!("failed to create {}", state_dir.display()))?;

    let mut cmd = Command::new(path);
    cmd.env("PUNKGO_STATE_DIR", &state_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // On Windows, create a new process group so the daemon survives after the
    // hook process exits. CREATE_NEW_PROCESS_GROUP (0x200) is sufficient —
    // DETACHED_PROCESS is not needed since stdio is already null.
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x00000200); // CREATE_NEW_PROCESS_GROUP
    }

    match cmd.spawn() {
        Ok(child) => {
            debug!(pid = child.id(), state_dir = %state_dir.display(), "punkgo-kerneld spawned");
        }
        Err(e) => {
            anyhow::bail!("failed to spawn {}: {e}", path.display());
        }
    }
    debug!(state_dir = %state_dir.display(), "punkgo-kerneld spawned");
    Ok(())
}

/// Default state directory: ~/.punkgo/state
fn default_state_dir() -> Result<PathBuf> {
    let data = crate::session::data_dir()?; // ~/.punkgo
    Ok(data.join("state"))
}

/// Seed the claude-code actor on a freshly started daemon. Non-fatal on failure.
fn try_seed_actor(client: &IpcClient) {
    use punkgo_core::protocol::{RequestEnvelope, RequestType};
    use serde_json::json;

    let req = RequestEnvelope {
        request_id: new_request_id(),
        request_type: RequestType::Submit,
        payload: json!({
            "actor_id": "root",
            "action_type": "create",
            "target": "ledger/actor",
            "payload": {
                "actor_id": "claude-code",
                "actor_type": "agent",
                "purpose": "claude-code-adapter",
                "energy_balance": 100_000,
                "energy_share": 50.0
            }
        }),
    };

    match client.send(&req) {
        Ok(resp) if resp.status == "ok" => {
            info!("actor 'claude-code' auto-seeded on fresh daemon");
        }
        Ok(resp) => {
            let msg = resp
                .payload
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !msg.contains("already exists") {
                debug!(error = msg, "actor seed returned non-ok");
            }
        }
        Err(e) => {
            debug!(error = %e, "failed to seed actor after auto-start");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_kerneld_returns_option() {
        // Just verify it doesn't panic. In CI, kerneld may or may not be present.
        let _ = find_kerneld();
    }

    #[test]
    fn default_state_dir_under_punkgo() {
        // Acquire the shared lock so we don't race with any test that mutates
        // PUNKGO_DATA_DIR (e.g. claude_code::tests::truncation::*).
        let _guard = crate::session::PUNKGO_DATA_DIR_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // If a prior test leaked the env var (shouldn't happen, but defensive),
        // clear it so this test sees the default behavior.
        let prev = std::env::var_os("PUNKGO_DATA_DIR");
        std::env::remove_var("PUNKGO_DATA_DIR");

        let dir = default_state_dir().unwrap();
        let dir_str = dir.to_string_lossy();

        // Restore before assertions so a failure doesn't leave env polluted.
        if let Some(v) = prev {
            std::env::set_var("PUNKGO_DATA_DIR", v);
        }

        assert!(
            dir_str.contains(".punkgo"),
            "expected .punkgo in path, got: {dir_str}"
        );
        assert!(
            dir_str.ends_with("state"),
            "expected path to end with 'state', got: {dir_str}"
        );
    }
}
