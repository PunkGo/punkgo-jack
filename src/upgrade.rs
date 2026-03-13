//! Self-upgrade: check latest release and upgrade via cargo or install script.

use anyhow::{bail, Context, Result};
use std::process::Command;

const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const GITHUB_REPO: &str = "PunkGo/punkgo-jack";
const INSTALL_SH: &str = "https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.sh";
const INSTALL_PS1: &str = "https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.ps1";

pub fn run_upgrade() -> Result<()> {
    println!("Current: v{CURRENT_VERSION}");

    let latest =
        fetch_latest_tag().context("failed to check latest version (need internet + curl)")?;
    let latest_ver = latest.trim_start_matches('v');

    println!("Latest:  v{latest_ver}");

    if latest_ver == CURRENT_VERSION {
        println!("Already up to date.");
        return Ok(());
    }

    println!();

    if has_cargo() {
        println!("Upgrading via cargo install...");
        let status = Command::new("cargo")
            .args(["install", "punkgo-jack", "--force"])
            .status()
            .context("failed to run cargo install")?;
        if !status.success() {
            bail!("cargo install failed");
        }
        // Also upgrade kernel if installed
        println!();
        println!("Checking punkgo-kernel...");
        let _ = Command::new("cargo")
            .args(["install", "punkgo-kernel", "--force"])
            .status();
    } else if cfg!(windows) {
        println!("Upgrading via install script...");
        let status = Command::new("powershell")
            .args(["-Command", &format!("irm {INSTALL_PS1} | iex")])
            .status()
            .context("failed to run PowerShell install script")?;
        if !status.success() {
            bail!("install script failed");
        }
    } else {
        println!("Upgrading via install script...");
        let status = Command::new("bash")
            .args(["-c", &format!("curl -fsSL {INSTALL_SH} | bash")])
            .status()
            .context("failed to run install script")?;
        if !status.success() {
            bail!("install script failed");
        }
    }

    println!();
    println!("Upgraded to v{latest_ver}. No re-setup needed.");
    Ok(())
}

/// Fetch the latest release tag from GitHub API using curl.
fn fetch_latest_tag() -> Result<String> {
    let output = Command::new("curl")
        .args([
            "-fsSL",
            &format!("https://api.github.com/repos/{GITHUB_REPO}/releases/latest"),
        ])
        .output()
        .context("curl not found")?;

    if !output.status.success() {
        bail!("GitHub API request failed");
    }

    let body = String::from_utf8_lossy(&output.stdout);
    // Simple extraction — avoid adding a JSON dependency just for this.
    // Look for "tag_name": "v0.2.2"
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with("\"tag_name\"") {
            if let Some(start) = line.find(": \"") {
                let rest = &line[start + 3..];
                if let Some(end) = rest.find('"') {
                    return Ok(rest[..end].to_string());
                }
            }
        }
    }

    bail!("could not parse tag_name from GitHub API response")
}

fn has_cargo() -> bool {
    Command::new("cargo")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_version_is_set() {
        assert!(!CURRENT_VERSION.is_empty());
    }

    #[test]
    fn has_cargo_returns_bool() {
        // Just verify it doesn't panic
        let _ = has_cargo();
    }
}
