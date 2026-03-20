use anyhow::{Context, Result};
use serde_json::Value;

use crate::data_fetch;
use crate::ipc_client::IpcClient;
use crate::roast_analysis;
use crate::roast_config;
use crate::roast_render;

pub struct RoastArgs {
    pub format: RoastFormat,
    pub days: Option<u64>,
    pub actor: Option<String>,
    pub output: Option<String>,
    /// True when --today is used (renders Vibe Card instead of Personality Card).
    pub today: bool,
}

pub enum RoastFormat {
    Cli,
    Svg,
    Json,
}

/// Minimum events required to produce a meaningful roast.
const MIN_EVENTS: usize = 10;

pub fn parse_args(args: &mut impl Iterator<Item = String>) -> Result<RoastArgs> {
    let mut format = RoastFormat::Cli;
    let mut days = None;
    let mut actor = None;
    let mut output = None;
    let mut today = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--svg" => format = RoastFormat::Svg,
            "--json" => format = RoastFormat::Json,
            "--today" => {
                today = true;
                days = Some(1);
            }
            "--week" => {
                days = Some(7);
            }
            "--month" => {
                days = Some(30);
            }
            "--days" | "-d" => {
                days = Some(
                    args.next()
                        .context("--days requires a number")?
                        .parse::<u64>()
                        .context("--days must be a number")?,
                );
            }
            "--actor" => {
                actor = Some(args.next().context("--actor requires a value")?);
            }
            "--output" | "-o" => {
                output = Some(args.next().context("--output requires a path")?);
            }
            other => anyhow::bail!("unknown roast option: {other}"),
        }
    }
    Ok(RoastArgs {
        format,
        days,
        actor,
        output,
        today,
    })
}

pub fn run_roast(args: RoastArgs) -> Result<()> {
    let config = roast_config::load_roast_config()?;
    let client = IpcClient::from_env(None);
    let events = data_fetch::fetch_all_events(&client, args.actor.as_deref(), None)?;

    if events.is_empty() {
        eprintln!("No events found. Use Claude Code or Cursor for a while, then try again.");
        return Ok(());
    }

    // Filter by days if specified
    let events: Vec<Value> = if let Some(days) = args.days {
        let cutoff_ms = chrono::Utc::now().timestamp_millis() as u64 - (days * 86_400_000);
        events
            .into_iter()
            .filter(|e| data_fetch::parse_event_timestamp_ms(e).is_some_and(|ts| ts >= cutoff_ms))
            .collect()
    } else {
        events
    };

    if events.is_empty() {
        eprintln!("No events in the last {} days.", args.days.unwrap_or(0));
        return Ok(());
    }

    if events.len() < MIN_EVENTS {
        eprintln!(
            "Not enough data yet. Keep coding! ({} events, need at least {MIN_EVENTS})",
            events.len()
        );
        return Ok(());
    }

    let mut data = roast_analysis::analyze_events(&events, &config);

    // Try to get Merkle root (best-effort)
    if let Ok(checkpoint) = data_fetch::fetch_checkpoint(&client) {
        data.merkle_root = checkpoint
            .get("root_hash")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
    }

    let output_text = match args.format {
        RoastFormat::Cli => roast_render::render_cli(&data),
        RoastFormat::Json => roast_render::render_json(&data),
        RoastFormat::Svg => {
            if args.today {
                roast_render::render_vibe_svg(&data)
            } else {
                roast_render::render_personality_svg(&data)
            }
        }
    };

    if let Some(ref path) = args.output {
        std::fs::write(path, &output_text).with_context(|| format!("failed to write to {path}"))?;
        eprintln!("Roast saved to {path}");
    } else {
        print!("{output_text}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_defaults() {
        let mut args = Vec::<String>::new().into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(matches!(parsed.format, RoastFormat::Cli));
        assert!(parsed.days.is_none());
        assert!(parsed.actor.is_none());
        assert!(parsed.output.is_none());
        assert!(!parsed.today);
    }

    #[test]
    fn parse_args_svg() {
        let mut args = vec!["--svg".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(matches!(parsed.format, RoastFormat::Svg));
    }

    #[test]
    fn parse_args_json() {
        let mut args = vec!["--json".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(matches!(parsed.format, RoastFormat::Json));
    }

    #[test]
    fn parse_args_days() {
        let mut args = vec!["--days".to_string(), "7".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert_eq!(parsed.days, Some(7));
    }

    #[test]
    fn parse_args_today() {
        let mut args = vec!["--today".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(parsed.today);
        assert_eq!(parsed.days, Some(1));
    }

    #[test]
    fn parse_args_week() {
        let mut args = vec!["--week".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert_eq!(parsed.days, Some(7));
        assert!(!parsed.today);
    }

    #[test]
    fn parse_args_month() {
        let mut args = vec!["--month".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert_eq!(parsed.days, Some(30));
        assert!(!parsed.today);
    }

    #[test]
    fn parse_args_today_with_svg() {
        let mut args = vec!["--today".to_string(), "--svg".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(parsed.today);
        assert!(matches!(parsed.format, RoastFormat::Svg));
        assert_eq!(parsed.days, Some(1));
    }

    #[test]
    fn parse_args_unknown_flag_errors() {
        let mut args = vec!["--bogus".to_string()].into_iter();
        assert!(parse_args(&mut args).is_err());
    }
}
