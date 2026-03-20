use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// Client-side session state. The kernel has no session concept —
/// jack manages sessions locally so each ingest invocation (separate process)
/// can correlate events to the same Claude Code session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    pub session_id: String,
    pub actor_id: String,
    pub started_at: String,
    pub event_count: u32,
    #[serde(default)]
    pub energy_consumed: u64,
}

/// Daily energy counter — tracks total energy consumed across all sessions today.
/// Stored at `~/.punkgo/daily_energy.json`, resets when the date changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DailyEnergy {
    date: String,
    energy: u64,
}

/// Start a new session. Writes state to `<data_dir>/sessions/<claude_session_id>.json`.
pub fn start_session(claude_session_id: &str, actor_id: &str) -> Result<SessionState> {
    start_session_in(&session_file_path(claude_session_id)?, actor_id)
}

/// Read the current session, if any.
pub fn current_session(claude_session_id: &str) -> Result<Option<SessionState>> {
    current_session_in(&session_file_path(claude_session_id)?)
}

/// Increment the event count and accumulate energy for the current session.
/// Also updates the global daily energy counter.
/// No-op if no session is active (graceful degradation).
pub fn record_event(claude_session_id: &str, settled_cost: u64) -> Result<()> {
    let path = session_file_path(claude_session_id)?;
    if !path.exists() {
        return Ok(());
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let mut state: SessionState = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    state.event_count += 1;
    state.energy_consumed += settled_cost;
    write_session(&path, &state)?;

    // Update global daily energy counter.
    let _ = add_daily_energy(settled_cost);
    Ok(())
}

/// End the current session. Returns the final state and removes the session file.
pub fn end_session(claude_session_id: &str) -> Result<SessionState> {
    end_session_in(&session_file_path(claude_session_id)?)
}

/// Return the resolved data directory path.
pub fn data_dir() -> Result<PathBuf> {
    punkgo_data_dir()
}

/// Find the most recently modified session (any session_id).
/// Useful when the caller doesn't know which Claude Code session it belongs to.
pub fn latest_session() -> Result<Option<SessionState>> {
    let sessions_dir = punkgo_data_dir()?.join("sessions");
    if !sessions_dir.exists() {
        return Ok(None);
    }

    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in std::fs::read_dir(&sessions_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if let Ok(meta) = entry.metadata() {
            if let Ok(modified) = meta.modified() {
                if newest.as_ref().is_none_or(|(t, _)| modified > *t) {
                    newest = Some((modified, path));
                }
            }
        }
    }

    match newest {
        Some((_, path)) => current_session_in(&path),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Internal implementations (path-parameterized for testability)
// ---------------------------------------------------------------------------

fn start_session_in(path: &Path, actor_id: &str) -> Result<SessionState> {
    if path.exists() {
        eprintln!("[punkgo] warning: overwriting stale session file");
    }

    let session_id = uuid::Uuid::new_v4().to_string();
    let started_at = chrono::Utc::now().to_rfc3339();
    let state = SessionState {
        session_id,
        actor_id: actor_id.to_string(),
        started_at,
        event_count: 0,
        energy_consumed: 0,
    };

    write_session(path, &state)?;
    Ok(state)
}

fn current_session_in(path: &Path) -> Result<Option<SessionState>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let state: SessionState = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(state))
}

fn end_session_in(path: &Path) -> Result<SessionState> {
    if !path.exists() {
        bail!("no active session to end");
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let state: SessionState = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    std::fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
    Ok(state)
}

// ---------------------------------------------------------------------------
// Path resolution
// ---------------------------------------------------------------------------

fn session_file_path(claude_session_id: &str) -> Result<PathBuf> {
    let dir = punkgo_data_dir()?.join("sessions");
    if !dir.exists() {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
    }
    Ok(dir.join(format!("{claude_session_id}.json")))
}

fn punkgo_data_dir() -> Result<PathBuf> {
    // Allow override via PUNKGO_DATA_DIR (useful for testing and custom deployments).
    if let Some(dir) = std::env::var_os("PUNKGO_DATA_DIR") {
        let dir = PathBuf::from(dir);
        if !dir.exists() {
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("failed to create {}", dir.display()))?;
        }
        return Ok(dir);
    }

    let home = home_dir()
        .context("cannot determine home directory. Set HOME (Unix) or USERPROFILE (Windows).")?;
    let dir = home.join(".punkgo");
    if !dir.exists() {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
    }
    Ok(dir)
}

pub fn home_dir() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("HOME") {
        return Some(PathBuf::from(home));
    }
    if let Some(profile) = std::env::var_os("USERPROFILE") {
        return Some(PathBuf::from(profile));
    }
    let drive = std::env::var_os("HOMEDRIVE")?;
    let path = std::env::var_os("HOMEPATH")?;
    let mut p = PathBuf::from(drive);
    p.push(path);
    Some(p)
}

fn daily_energy_path() -> Result<PathBuf> {
    Ok(punkgo_data_dir()?.join("daily_energy.json"))
}

fn today_str() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

fn add_daily_energy(cost: u64) -> Result<()> {
    let path = daily_energy_path()?;
    let today = today_str();
    let mut de = if path.exists() {
        let content = std::fs::read_to_string(&path)?;
        serde_json::from_str::<DailyEnergy>(&content).unwrap_or(DailyEnergy {
            date: today.clone(),
            energy: 0,
        })
    } else {
        DailyEnergy {
            date: today.clone(),
            energy: 0,
        }
    };
    // Reset on date change.
    if de.date != today {
        de = DailyEnergy {
            date: today,
            energy: 0,
        };
    }
    de.energy += cost;
    let content = serde_json::to_string(&de)?;
    std::fs::write(&path, content.as_bytes())?;
    Ok(())
}

fn write_session(path: &Path, state: &SessionState) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let content =
        serde_json::to_string_pretty(state).context("failed to serialize session state")?;
    std::fs::write(path, content.as_bytes())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_session_path() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("current_session.json");
        (dir, path)
    }

    #[test]
    fn start_and_read_session() {
        let (_dir, path) = temp_session_path();
        let state = start_session_in(&path, "claude-code").unwrap();
        assert_eq!(state.actor_id, "claude-code");
        assert_eq!(state.event_count, 0);
        assert!(!state.session_id.is_empty());

        let current = current_session_in(&path).unwrap().unwrap();
        assert_eq!(current.session_id, state.session_id);
    }

    /// Helper: simulate record_event by reading, incrementing, and writing back.
    fn do_record_event(path: &std::path::Path, cost: u64) {
        let content = std::fs::read_to_string(path).unwrap();
        let mut state: SessionState = serde_json::from_str(&content).unwrap();
        state.event_count += 1;
        state.energy_consumed += cost;
        let out = serde_json::to_string_pretty(&state).unwrap();
        std::fs::write(path, out.as_bytes()).unwrap();
    }

    #[test]
    fn increment_updates_count() {
        let (_dir, path) = temp_session_path();
        start_session_in(&path, "claude-code").unwrap();
        do_record_event(&path, 20);
        do_record_event(&path, 20);
        let current = current_session_in(&path).unwrap().unwrap();
        assert_eq!(current.event_count, 2);
    }

    #[test]
    fn end_session_removes_file() {
        let (_dir, path) = temp_session_path();
        start_session_in(&path, "claude-code").unwrap();
        do_record_event(&path, 20);
        let final_state = end_session_in(&path).unwrap();
        assert_eq!(final_state.event_count, 1);
        assert!(current_session_in(&path).unwrap().is_none());
    }

    #[test]
    fn end_session_without_start_fails() {
        let (_dir, path) = temp_session_path();
        let result = end_session_in(&path);
        assert!(result.is_err());
    }

    #[test]
    fn current_session_returns_none_without_start() {
        let (_dir, path) = temp_session_path();
        assert!(current_session_in(&path).unwrap().is_none());
    }

    #[test]
    fn start_overwrites_stale_session() {
        let (_dir, path) = temp_session_path();
        let first = start_session_in(&path, "claude-code").unwrap();
        let second = start_session_in(&path, "claude-code").unwrap();
        assert_ne!(first.session_id, second.session_id);
        let current = current_session_in(&path).unwrap().unwrap();
        assert_eq!(current.session_id, second.session_id);
    }

    #[test]
    fn session_state_serialization_roundtrip() {
        let state = SessionState {
            session_id: "test-uuid".into(),
            actor_id: "claude-code".into(),
            started_at: "2026-03-02T12:00:00Z".into(),
            event_count: 42,
            energy_consumed: 840,
        };
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: SessionState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.session_id, "test-uuid");
        assert_eq!(deserialized.event_count, 42);
    }
}
