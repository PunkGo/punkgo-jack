//! Configuration loading for punkgo-jack.
//!
//! Layer model (highest priority wins):
//!   1. Environment variables   (PUNKGO_TSA_ENABLED, PUNKGO_TSA_URL, etc.)
//!   2. Global config           (~/.punkgo/config.toml)
//!   3. Built-in defaults

use std::path::PathBuf;

use serde::Deserialize;
use tracing::debug;

/// Top-level jack configuration.
#[derive(Debug, Deserialize, Default, PartialEq)]
pub struct Config {
    #[serde(default)]
    pub tsa: TsaConfig,
}

/// TSA anchoring configuration.
#[derive(Debug, Deserialize, PartialEq)]
pub struct TsaConfig {
    /// Master switch. Default: false (opt-in).
    #[serde(default)]
    pub enabled: bool,

    /// Primary TSA endpoint URL. RFC 3161 over HTTP(S).
    /// RFC 3161 responses are cryptographically signed, so HTTP is safe
    /// against content tampering (MITM can only block, not forge).
    #[serde(default = "default_tsa_url")]
    pub url: String,

    /// HTTP timeout in seconds. Default: 10.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// Minimum seconds between consecutive TSA submissions.
    /// Prevents rapid session cycling from spamming the TSA.
    /// Set to 0 for CI/burst mode. Default: 300 (5 minutes).
    #[serde(default = "default_min_interval")]
    pub min_interval_secs: u64,
}

impl Default for TsaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: default_tsa_url(),
            timeout_secs: default_timeout(),
            min_interval_secs: default_min_interval(),
        }
    }
}

fn default_tsa_url() -> String {
    "http://timestamp.digicert.com".to_string()
}

fn default_timeout() -> u64 {
    10
}

fn default_min_interval() -> u64 {
    300
}

/// Load configuration with env var overrides.
pub fn load_config() -> Config {
    let mut config = load_config_file().unwrap_or_default();
    apply_env_overrides(&mut config);
    config
}

fn load_config_file() -> Option<Config> {
    let path = config_file_path()?;
    if !path.exists() {
        return None;
    }
    let text = std::fs::read_to_string(&path).ok()?;
    let config: Config = toml::from_str(&text).ok()?;
    debug!(path = %path.display(), "loaded config");
    Some(config)
}

fn apply_env_overrides(config: &mut Config) {
    if let Ok(v) = std::env::var("PUNKGO_TSA_ENABLED") {
        config.tsa.enabled = v == "true" || v == "1";
    }
    if let Ok(v) = std::env::var("PUNKGO_TSA_URL") {
        config.tsa.url = v;
    }
    if let Ok(v) = std::env::var("PUNKGO_TSA_TIMEOUT_SECS") {
        if let Ok(n) = v.parse() {
            config.tsa.timeout_secs = n;
        }
    }
    if let Ok(v) = std::env::var("PUNKGO_TSA_MIN_INTERVAL_SECS") {
        if let Ok(n) = v.parse() {
            config.tsa.min_interval_secs = n;
        }
    }
}

fn config_file_path() -> Option<PathBuf> {
    crate::session::home_dir().map(|h| h.join(".punkgo").join("config.toml"))
}

/// TSA state directory: ~/.punkgo/state/tsa/
pub fn tsa_state_dir() -> Option<PathBuf> {
    crate::session::home_dir().map(|h| h.join(".punkgo").join("state").join("tsa"))
}

/// Path for a TSR file: <tsa_state_dir>/<tree_size>.tsr
pub fn tsr_path(tree_size: i64) -> Option<PathBuf> {
    tsa_state_dir().map(|d| d.join(format!("{tree_size}.tsr")))
}

/// Path for rate limit state file (plain text, epoch seconds).
pub fn rate_limit_path() -> Option<PathBuf> {
    tsa_state_dir().map(|d| d.join("last_anchor_ts"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = Config::default();
        assert!(!config.tsa.enabled);
        assert_eq!(config.tsa.url, "http://timestamp.digicert.com");
        assert_eq!(config.tsa.timeout_secs, 10);
        assert_eq!(config.tsa.min_interval_secs, 300);
    }

    #[test]
    fn parse_minimal_toml() {
        let toml_str = "[tsa]\nenabled = true\n";
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.tsa.enabled);
        // Defaults filled in
        assert_eq!(config.tsa.url, "http://timestamp.digicert.com");
        assert_eq!(config.tsa.timeout_secs, 10);
    }

    #[test]
    fn parse_full_toml() {
        let toml_str = r#"
[tsa]
enabled = true
url = "https://custom-tsa.example.com"
timeout_secs = 5
min_interval_secs = 0
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.tsa.enabled);
        assert_eq!(config.tsa.url, "https://custom-tsa.example.com");
        assert_eq!(config.tsa.timeout_secs, 5);
        assert_eq!(config.tsa.min_interval_secs, 0);
    }

    #[test]
    fn empty_toml_uses_defaults() {
        let config: Config = toml::from_str("").unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn env_override_enabled() {
        let mut config = Config::default();
        assert!(!config.tsa.enabled);

        // Simulate env var
        std::env::set_var("PUNKGO_TSA_ENABLED", "true");
        apply_env_overrides(&mut config);
        assert!(config.tsa.enabled);

        // Cleanup
        std::env::remove_var("PUNKGO_TSA_ENABLED");
    }

    #[test]
    fn env_override_url() {
        let mut config = Config::default();
        std::env::set_var("PUNKGO_TSA_URL", "https://test.example.com");
        apply_env_overrides(&mut config);
        assert_eq!(config.tsa.url, "https://test.example.com");
        std::env::remove_var("PUNKGO_TSA_URL");
    }
}
