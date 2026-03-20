//! Configuration-driven roast personality engine.
//!
//! All 16 personalities, their detection conditions, quips, traits, radar dimensions,
//! and visual properties are defined in `roast_config.toml`. This module:
//! 1. Deserializes the TOML into typed structs
//! 2. Evaluates simple condition expressions against computed metrics
//! 3. Selects personality + quip deterministically (first match wins)
//!
//! To add a new personality: edit the TOML file. No Rust changes needed.

use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Config structs (deserialized from TOML)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct RoastConfig {
    #[serde(rename = "radar_dimension")]
    pub radar_dimensions: Vec<RadarDimension>,

    #[serde(rename = "personality")]
    pub personalities: Vec<PersonalityConfig>,

    #[serde(rename = "trait")]
    pub traits: Vec<TraitConfig>,

    /// Max traits to assign. Default 2.
    #[serde(default = "default_max_traits")]
    pub max_traits: usize,
}

fn default_max_traits() -> usize {
    2
}

#[derive(Debug, Deserialize)]
pub struct RadarDimension {
    pub key: String,
    pub label: String,
    pub formula: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersonalityConfig {
    pub id: String,
    pub name: String,
    pub mbti: String,
    pub dog_breed: String,
    pub dog_image: String,
    pub emoji: String,
    pub card_color: String,
    pub catchphrase: String,
    /// Condition expression, e.g. "think_do_ratio > 2.5"
    pub condition: String,
    /// Optional radar chart bias overrides (0-100)
    #[serde(default)]
    pub radar_bias: HashMap<String, f64>,
    #[serde(default)]
    pub quips: Vec<QuipConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuipConfig {
    pub template: String,
    /// Condition expression, or "default" for fallback
    pub condition: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TraitConfig {
    pub id: String,
    pub label: String,
    pub emoji: String,
    pub condition: String,
}

// ---------------------------------------------------------------------------
// Metrics: the variable bag that conditions evaluate against
// ---------------------------------------------------------------------------

/// All computed metrics from event data. This is the "variable namespace"
/// that condition expressions can reference.
#[derive(Debug, Clone)]
pub struct RoastMetrics {
    pub vars: HashMap<String, f64>,
}

impl RoastMetrics {
    pub fn get(&self, name: &str) -> f64 {
        self.vars.get(name).copied().unwrap_or(0.0)
    }

    /// Build metrics from raw counters (called by analysis after counting events).
    #[allow(clippy::too_many_arguments)]
    pub fn from_counters(
        total_events: usize,
        file_read: usize,
        file_write: usize,
        file_edit: usize,
        content_search: usize,
        web_search: usize,
        file_search: usize,
        user_prompt: usize,
        subagent_start: usize,
        cmd_exec_failed: usize,
        session_starts: usize,
        session_ends: usize,
        max_file_read_count: usize,
        unique_files_read: usize,
        peak_hour: u8,
        night_ratio: f64,
        fail_count: usize,
    ) -> Self {
        let total = total_events as f64;
        let total_nz = if total > 0.0 { total } else { 1.0 };

        // Think:Do ratio
        let think = (file_read + content_search + web_search + file_search) as f64;
        let do_ = (file_write + file_edit) as f64;
        let think_do_ratio = if do_ > 0.0 {
            think / do_
        } else {
            f64::MAX.min(999.0)
        };

        // Edit:Write ratio
        let edit_write_ratio = if file_write > 0 {
            file_edit as f64 / file_write as f64
        } else {
            0.0
        };

        // Fail rate
        let fail_rate = fail_count as f64 / total_nz * 100.0;

        // Ratios as percentages
        let action_ratio = do_ / total_nz * 100.0;
        let search_ratio = web_search as f64 / total_nz * 100.0;
        let prompt_ratio = user_prompt as f64 / total_nz * 100.0;
        let subagent_ratio = if session_starts > 0 {
            subagent_start as f64 / session_starts as f64 * 100.0
        } else {
            0.0
        };

        // Ghost ratio
        let ghost_sessions = session_starts.saturating_sub(session_ends);
        let ghost_ratio = if session_starts > 0 {
            ghost_sessions as f64 / session_starts as f64 * 100.0
        } else {
            0.0
        };

        // File dispersion (0 = all reads on 1 file, 1 = perfectly spread)
        let file_dispersion = if max_file_read_count > 0 && file_read > 0 {
            1.0 - (max_file_read_count as f64 / file_read as f64)
        } else {
            0.5
        };

        // Actions per prompt
        let prompts_nz = user_prompt.max(1) as f64;
        let actions_per_prompt = total / prompts_nz;

        // Actions per session
        let sessions_nz = session_starts.max(1) as f64;
        let actions_per_session = total / sessions_nz;

        // Read/search ratio for radar
        let read_search_ratio = think / total_nz * 100.0;
        let web_search_ratio = web_search as f64 / total_nz * 100.0;
        let write_edit_ratio = do_ / total_nz * 100.0;

        // File concentration for radar
        let file_concentration = if file_read > 0 {
            max_file_read_count as f64 / file_read as f64 * 100.0
        } else {
            0.0
        };

        // Stability for radar
        let stability = (100.0 - fail_rate).max(0.0);

        // Session average length (actions per session, scaled to 0-100)
        let session_avg_length = (actions_per_session / 5.0).min(100.0);

        // Read:write ratio (raw)
        let read_write_ratio = if file_write > 0 {
            file_read as f64 / file_write as f64
        } else {
            0.0
        };

        let mut vars = HashMap::new();
        vars.insert("total_events".into(), total);
        vars.insert("file_read".into(), file_read as f64);
        vars.insert("file_write".into(), file_write as f64);
        vars.insert("file_edit".into(), file_edit as f64);
        vars.insert("content_search".into(), content_search as f64);
        vars.insert("web_search".into(), web_search as f64);
        vars.insert("file_search".into(), file_search as f64);
        vars.insert("user_prompt".into(), user_prompt as f64);
        vars.insert("subagent_start".into(), subagent_start as f64);
        vars.insert("cmd_exec_failed".into(), cmd_exec_failed as f64);
        vars.insert("session_starts".into(), session_starts as f64);
        vars.insert("session_ends".into(), session_ends as f64);
        vars.insert("max_file_read_count".into(), max_file_read_count as f64);
        vars.insert("unique_files_read".into(), unique_files_read as f64);
        vars.insert("peak_hour".into(), peak_hour as f64);
        vars.insert("night_ratio".into(), night_ratio);
        vars.insert("fail_count".into(), fail_count as f64);
        vars.insert("ghost_sessions".into(), ghost_sessions as f64);
        vars.insert("think_do_ratio".into(), think_do_ratio);
        vars.insert("edit_write_ratio".into(), edit_write_ratio);
        vars.insert("fail_rate".into(), fail_rate);
        vars.insert("action_ratio".into(), action_ratio);
        vars.insert("search_ratio".into(), search_ratio);
        vars.insert("prompt_ratio".into(), prompt_ratio);
        vars.insert("subagent_ratio".into(), subagent_ratio);
        vars.insert("ghost_ratio".into(), ghost_ratio);
        vars.insert("file_dispersion".into(), file_dispersion);
        vars.insert("actions_per_prompt".into(), actions_per_prompt);
        vars.insert("actions_per_session".into(), actions_per_session);
        vars.insert("read_write_ratio".into(), read_write_ratio);

        // Radar formula variables
        vars.insert("read_search_ratio".into(), read_search_ratio);
        vars.insert("web_search_ratio".into(), web_search_ratio);
        vars.insert("session_avg_length".into(), session_avg_length);
        vars.insert("write_edit_ratio".into(), write_edit_ratio);
        vars.insert("file_concentration".into(), file_concentration);
        vars.insert("stability".into(), stability);
        vars.insert("plot_armor".into(), stability); // alias

        // Convenience aliases for quip templates
        vars.insert("reads".into(), file_read as f64 / total_nz * 100.0);
        vars.insert("writes".into(), file_write as f64 / total_nz * 100.0);
        vars.insert("edits".into(), file_edit as f64);
        vars.insert("searches".into(), search_ratio);
        vars.insert("fails".into(), fail_rate);
        vars.insert("sessions".into(), session_starts as f64);
        vars.insert("top_file".into(), max_file_read_count as f64);
        vars.insert("actions".into(), total);
        vars.insert("night".into(), night_ratio);
        vars.insert("search_count".into(), web_search as f64);
        vars.insert("subagents".into(), subagent_start as f64);
        vars.insert("prompts".into(), user_prompt as f64);

        // Derived convenience
        let minutes_per_search = if web_search > 0 {
            // rough: assume period_days * 8h * 60min active time
            // This is a placeholder; real value would need period info
            (total / web_search as f64).max(1.0)
        } else {
            0.0
        };
        vars.insert("minutes_per_search".into(), minutes_per_search);

        // peak_rate placeholder (Phase 2: per-minute window)
        vars.insert("peak_rate".into(), actions_per_session / 10.0);

        RoastMetrics { vars }
    }
}

// ---------------------------------------------------------------------------
// Condition evaluator (simple expression parser)
// ---------------------------------------------------------------------------

/// Evaluate a condition string against metrics.
/// Supports: variable <op> number, combined with && and ||.
/// Examples: "think_do_ratio > 2.5", "fail_rate > 3.0 && total_events > 1000"
pub fn eval_condition(condition: &str, metrics: &RoastMetrics) -> bool {
    let condition = condition.trim();
    if condition == "default" || condition == "true" || condition.is_empty() {
        return true;
    }

    // Split by || first (lower precedence)
    if condition.contains("||") {
        return condition
            .split("||")
            .any(|part| eval_condition(part.trim(), metrics));
    }

    // Split by && (higher precedence)
    if condition.contains("&&") {
        return condition
            .split("&&")
            .all(|part| eval_condition(part.trim(), metrics));
    }

    // Single comparison: <var> <op> <number>
    eval_single_comparison(condition, metrics)
}

fn eval_single_comparison(expr: &str, metrics: &RoastMetrics) -> bool {
    // Try operators in order of length (>= before >, etc.)
    let ops = [">=", "<=", "==", "!=", ">", "<"];
    for op in &ops {
        if let Some(pos) = expr.find(op) {
            let var_name = expr[..pos].trim();
            let val_str = expr[pos + op.len()..].trim();

            let lhs = metrics.get(var_name);
            let rhs = match val_str.parse::<f64>() {
                Ok(v) => v,
                Err(_) => {
                    // rhs might be a variable name too
                    metrics.get(val_str)
                }
            };

            return match *op {
                ">=" => lhs >= rhs,
                "<=" => lhs <= rhs,
                "==" => (lhs - rhs).abs() < f64::EPSILON,
                "!=" => (lhs - rhs).abs() >= f64::EPSILON,
                ">" => lhs > rhs,
                "<" => lhs < rhs,
                _ => false,
            };
        }
    }

    // If no operator found, treat as boolean (nonzero = true)
    metrics.get(expr) != 0.0
}

// ---------------------------------------------------------------------------
// Config loading: built-in default + optional user override
// ---------------------------------------------------------------------------

/// The built-in config (compiled into the binary).
const BUILTIN_CONFIG: &str = include_str!("roast_config.toml");

/// Load roast config: user override (~/.punkgo/roast.toml) if it exists,
/// otherwise use the built-in default.
pub fn load_roast_config() -> Result<RoastConfig> {
    // Check for user override
    if let Some(user_path) = user_config_path() {
        if user_path.exists() {
            let text = std::fs::read_to_string(&user_path)
                .with_context(|| format!("failed to read {}", user_path.display()))?;
            let config: RoastConfig = toml::from_str(&text)
                .with_context(|| format!("failed to parse {}", user_path.display()))?;
            return Ok(config);
        }
    }

    // Fall back to built-in
    let config: RoastConfig =
        toml::from_str(BUILTIN_CONFIG).context("failed to parse built-in roast_config.toml")?;
    Ok(config)
}

fn user_config_path() -> Option<std::path::PathBuf> {
    crate::session::home_dir().map(|h| h.join(".punkgo").join("roast.toml"))
}

// ---------------------------------------------------------------------------
// Personality matching
// ---------------------------------------------------------------------------

/// Find the first matching personality from the config.
/// Returns the PersonalityConfig and its index.
pub fn match_personality<'a>(
    config: &'a RoastConfig,
    metrics: &RoastMetrics,
) -> &'a PersonalityConfig {
    // Personalities are already in priority order in the TOML array
    for p in &config.personalities {
        if eval_condition(&p.condition, metrics) {
            return p;
        }
    }
    // Fallback: first personality (Philosopher)
    &config.personalities[0]
}

/// Find matching traits (up to max_traits).
pub fn match_traits<'a>(config: &'a RoastConfig, metrics: &RoastMetrics) -> Vec<&'a TraitConfig> {
    let mut matched = Vec::new();
    for t in &config.traits {
        if eval_condition(&t.condition, metrics) {
            matched.push(t);
            if matched.len() >= config.max_traits {
                break;
            }
        }
    }
    matched
}

/// Select the best quip for a personality. First non-default match wins;
/// if no specific condition matches, use the "default" quip.
pub fn select_quip(personality: &PersonalityConfig, metrics: &RoastMetrics) -> String {
    let mut default_quip: Option<&str> = None;

    // First pass: find first non-default match
    for quip in &personality.quips {
        if quip.condition == "default" {
            default_quip = Some(&quip.template);
            continue;
        }
        if eval_condition(&quip.condition, metrics) {
            return expand_template(&quip.template, metrics);
        }
    }

    // Fallback to default
    if let Some(tmpl) = default_quip {
        return expand_template(tmpl, metrics);
    }

    // Absolute fallback
    personality.catchphrase.clone()
}

/// Expand {placeholder} in a template string using metrics.
pub fn expand_template(template: &str, metrics: &RoastMetrics) -> String {
    let mut result = template.to_string();
    // Find all {name} patterns and replace
    while let Some(start) = result.find('{') {
        let end = match result[start..].find('}') {
            Some(i) => start + i,
            None => break,
        };
        let var_name = &result[start + 1..end];
        let value = metrics.get(var_name);

        // Format: integers for counts, 1 decimal for percentages/ratios
        let formatted = if value == value.floor() && value.abs() < 1_000_000.0 {
            format_comma(value as i64)
        } else {
            format!("{:.1}", value)
        };

        result = format!("{}{}{}", &result[..start], formatted, &result[end + 1..]);
    }
    result
}

fn format_comma(n: i64) -> String {
    let s = n.abs().to_string();
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len + len / 3 + 1);
    if n < 0 {
        out.push('-');
    }
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

/// Compute radar chart values for a personality.
/// For each radar dimension, use the personality's radar_bias if present,
/// otherwise compute from the formula variable in metrics.
pub fn compute_radar(
    config: &RoastConfig,
    personality: &PersonalityConfig,
    metrics: &RoastMetrics,
) -> Vec<(String, f64)> {
    config
        .radar_dimensions
        .iter()
        .map(|dim| {
            let value = if let Some(&bias) = personality.radar_bias.get(&dim.key) {
                bias
            } else {
                // Use the formula variable from metrics, clamped to 0-100
                metrics.get(&dim.formula).clamp(0.0, 100.0)
            };
            (dim.label.clone(), value)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metrics() -> RoastMetrics {
        let mut vars = HashMap::new();
        vars.insert("think_do_ratio".into(), 3.0);
        vars.insert("fail_rate".into(), 1.5);
        vars.insert("total_events".into(), 5000.0);
        vars.insert("max_file_read_count".into(), 50.0);
        vars.insert("edit_write_ratio".into(), 1.2);
        vars.insert("night_ratio".into(), 30.0);
        vars.insert("peak_hour".into(), 22.0);
        vars.insert("search_ratio".into(), 5.0);
        vars.insert("action_ratio".into(), 15.0);
        vars.insert("prompt_ratio".into(), 8.0);
        vars.insert("subagent_ratio".into(), 10.0);
        vars.insert("ghost_ratio".into(), 5.0);
        vars.insert("cmd_exec_failed".into(), 3.0);
        vars.insert("file_dispersion".into(), 0.5);
        vars.insert("actions_per_prompt".into(), 20.0);
        vars.insert("subagent_start".into(), 5.0);
        vars.insert("sessions".into(), 10.0);
        vars.insert("top_file".into(), 50.0);
        vars.insert("reads".into(), 60.0);
        vars.insert("writes".into(), 10.0);
        vars.insert("actions".into(), 5000.0);
        vars.insert("fails".into(), 1.5);
        vars.insert("plot_armor".into(), 98.5);
        RoastMetrics { vars }
    }

    #[test]
    fn load_builtin_config() {
        let config = load_roast_config().unwrap();
        assert_eq!(config.personalities.len(), 16);
        assert_eq!(config.traits.len(), 6);
        assert_eq!(config.radar_dimensions.len(), 6);
    }

    #[test]
    fn eval_simple_gt() {
        let m = test_metrics();
        assert!(eval_condition("think_do_ratio > 2.5", &m));
        assert!(!eval_condition("think_do_ratio > 5.0", &m));
    }

    #[test]
    fn eval_and() {
        let m = test_metrics();
        assert!(eval_condition("fail_rate > 1.0 && total_events > 1000", &m));
        assert!(!eval_condition(
            "fail_rate > 1.0 && total_events > 10000",
            &m
        ));
    }

    #[test]
    fn eval_or() {
        let m = test_metrics();
        assert!(eval_condition(
            "fail_rate > 100.0 || think_do_ratio > 2.0",
            &m
        ));
    }

    #[test]
    fn eval_default() {
        let m = test_metrics();
        assert!(eval_condition("default", &m));
        assert!(eval_condition("", &m));
    }

    #[test]
    fn eval_gte_lte() {
        let m = test_metrics();
        assert!(eval_condition("peak_hour >= 22", &m));
        assert!(eval_condition("peak_hour <= 22", &m));
        assert!(!eval_condition("peak_hour >= 23", &m));
    }

    #[test]
    fn eval_eq() {
        let m = test_metrics();
        assert!(eval_condition("peak_hour == 22", &m));
        assert!(!eval_condition("peak_hour == 21", &m));
    }

    #[test]
    fn eval_var_vs_var() {
        let m = test_metrics();
        // subagent_start (5) < sessions (10)
        assert!(eval_condition("subagent_start < sessions", &m));
    }

    #[test]
    fn match_philosopher_first() {
        let config = load_roast_config().unwrap();
        let m = test_metrics(); // think_do_ratio = 3.0 > 2.5
        let p = match_personality(&config, &m);
        assert_eq!(p.id, "philosopher");
    }

    #[test]
    fn match_intern_when_high_fail() {
        let config = load_roast_config().unwrap();
        let mut vars = HashMap::new();
        vars.insert("think_do_ratio".into(), 1.0); // not philosopher
        vars.insert("fail_rate".into(), 5.0); // > 3.0
        vars.insert("total_events".into(), 2000.0); // > 1000
        vars.insert("action_ratio".into(), 20.0);
        vars.insert("search_ratio".into(), 3.0);
        vars.insert("max_file_read_count".into(), 5.0);
        vars.insert("edit_write_ratio".into(), 0.5);
        let m = RoastMetrics { vars };
        let p = match_personality(&config, &m);
        assert_eq!(p.id, "intern");
    }

    #[test]
    fn select_quip_default() {
        let config = load_roast_config().unwrap();
        let m = test_metrics();
        let p = match_personality(&config, &m);
        let quip = select_quip(p, &m);
        // Should get a non-empty quip
        assert!(!quip.is_empty());
    }

    #[test]
    fn select_quip_specific_condition() {
        let config = load_roast_config().unwrap();
        let mut m = test_metrics();
        m.vars.insert("top_file".into(), 55.0);
        let p = &config.personalities[0]; // philosopher
        let quip = select_quip(p, &m);
        // Should match "Read {top_file} files before writing one line." (top_file > 50)
        assert!(quip.contains("55"));
    }

    #[test]
    fn expand_template_basic() {
        let m = test_metrics();
        let result = expand_template("{reads}% reading. {writes}% writing.", &m);
        assert!(result.contains("60"));
        assert!(result.contains("10"));
    }

    #[test]
    fn expand_template_comma_format() {
        let mut vars = HashMap::new();
        vars.insert("actions".into(), 12345.0);
        let m = RoastMetrics { vars };
        let result = expand_template("{actions} actions total", &m);
        assert_eq!(result, "12,345 actions total");
    }

    #[test]
    fn trait_matching() {
        let config = load_roast_config().unwrap();
        let m = test_metrics(); // peak_hour = 22 -> Nocturnal, max_file_read_count = 50 -> Obsessive
        let traits = match_traits(&config, &m);
        assert!(traits.len() <= 2);
        assert_eq!(traits[0].id, "nocturnal");
        assert_eq!(traits[1].id, "obsessive");
    }

    #[test]
    fn radar_with_bias() {
        let config = load_roast_config().unwrap();
        let m = test_metrics();
        let p = match_personality(&config, &m); // philosopher
        let radar = compute_radar(&config, p, &m);
        assert_eq!(radar.len(), 6);
        // Philosopher has yapping bias = 95
        let yapping = radar.iter().find(|(l, _)| l == "Yapping").unwrap();
        assert_eq!(yapping.1, 95.0);
    }

    #[test]
    fn radar_without_bias_uses_formula() {
        let config = load_roast_config().unwrap();
        let m = test_metrics();
        let p = match_personality(&config, &m);
        let radar = compute_radar(&config, p, &m);
        // Googling has no bias for philosopher, should use web_search_ratio from metrics
        let googling = radar.iter().find(|(l, _)| l == "Googling").unwrap();
        assert_eq!(googling.1, m.get("web_search_ratio").clamp(0.0, 100.0));
    }

    #[test]
    fn format_comma_works() {
        assert_eq!(format_comma(1000), "1,000");
        assert_eq!(format_comma(42), "42");
        assert_eq!(format_comma(1234567), "1,234,567");
    }

    #[test]
    fn all_personalities_have_quips() {
        let config = load_roast_config().unwrap();
        for p in &config.personalities {
            assert!(!p.quips.is_empty(), "personality {} has no quips", p.id);
            // At least one "default" quip
            assert!(
                p.quips.iter().any(|q| q.condition == "default"),
                "personality {} has no default quip",
                p.id
            );
        }
    }

    #[test]
    fn all_personality_ids_unique() {
        let config = load_roast_config().unwrap();
        let mut seen = std::collections::HashSet::new();
        for p in &config.personalities {
            assert!(seen.insert(&p.id), "duplicate personality id: {}", p.id);
        }
    }

    #[test]
    fn all_personalities_have_valid_conditions() {
        let config = load_roast_config().unwrap();
        let m = test_metrics();
        for p in &config.personalities {
            // Should not panic
            let _ = eval_condition(&p.condition, &m);
            for q in &p.quips {
                let _ = eval_condition(&q.condition, &m);
            }
        }
        for t in &config.traits {
            let _ = eval_condition(&t.condition, &m);
        }
    }
}
