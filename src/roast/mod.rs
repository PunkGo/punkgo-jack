mod analysis;
mod assets;
mod config;
mod render;

use anyhow::{Context, Result};
use serde_json::Value;
use std::path::PathBuf;

use crate::data_fetch;
use crate::ipc_client::IpcClient;

pub struct RoastArgs {
    pub format: RoastFormat,
    pub days: Option<u64>,
    pub actor: Option<String>,
    pub output: Option<String>,
    /// True when --today is used (renders Vibe Card instead of Personality Card).
    pub today: bool,
}

#[derive(PartialEq)]
pub enum RoastFormat {
    Cli,
    Svg,
    Png,
    Json,
}

/// Minimum events required to produce a meaningful roast.
const MIN_EVENTS: usize = 10;

const HELP_TEXT: &str = "\
punkgo-jack roast — AI personality diagnosis

Usage:
  punkgo-jack roast [OPTIONS]

Output formats:
  (default)     Terminal output with meme radar
  --svg         Save SVG card to current directory
  --png         Save PNG card to current directory
  --json        Print JSON to stdout

Time range:
  --today       Today only (Vibe Card)
  --week        Last 7 days
  --month       Last 30 days
  --days <N>    Last N days

Options:
  -o <PATH>     Override output path (file or directory)
  --actor <ID>  Filter by actor
  help, --help  Show this message

Examples:
  punkgo-jack roast              Terminal roast
  punkgo-jack roast --png        Save PNG to ./punkgo-roast.png
  punkgo-jack roast --svg        Save SVG to ./punkgo-roast.svg
  punkgo-jack roast --today --png  Today's vibe card as PNG
  punkgo-jack roast --png -o ~/cards/  Save PNG to ~/cards/punkgo-roast.png
";

pub fn parse_args(args: &mut impl Iterator<Item = String>) -> Result<RoastArgs> {
    let mut format = RoastFormat::Cli;
    let mut days = None;
    let mut actor = None;
    let mut output = None;
    let mut today = false;
    let mut show_help = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--svg" => format = RoastFormat::Svg,
            "--png" | "--export" => format = RoastFormat::Png,
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
                output = Some(args.next().context("-o requires a path")?);
            }
            "help" | "--help" | "-h" => {
                show_help = true;
            }
            other => {
                eprintln!("Unknown option: {other}\n");
                print!("{HELP_TEXT}");
                anyhow::bail!("unknown roast option: {other}");
            }
        }
    }

    if show_help {
        print!("{HELP_TEXT}");
        std::process::exit(0);
    }

    Ok(RoastArgs {
        format,
        days,
        actor,
        output,
        today,
    })
}

/// Build default filename like `punkgo-roast-today.png` or `punkgo-roast.png`.
fn default_filename(period: &str, ext: &str) -> String {
    if period.is_empty() {
        format!("punkgo-roast.{ext}")
    } else {
        format!("punkgo-roast-{period}.{ext}")
    }
}

/// Map args to a period slug for the filename.
fn period_slug(args: &RoastArgs) -> &'static str {
    if args.today {
        return "today";
    }
    match args.days {
        Some(7) => "week",
        Some(30) => "month",
        Some(1) => "today",
        Some(_) => "custom",
        None => "",
    }
}

/// Resolve output path: handle directory vs file, auto-append filename.
fn resolve_output_path(output: Option<&str>, period: &str, ext: &str) -> PathBuf {
    let name = default_filename(period, ext);

    match output {
        Some(p) => {
            let path = PathBuf::from(p);
            if path.is_dir() {
                path.join(&name)
            } else if p.ends_with('/') || p.ends_with('\\') {
                std::fs::create_dir_all(&path).ok();
                path.join(&name)
            } else if path.extension().is_none() && !p.contains('.') {
                // No extension and no dot — treat as directory intent
                std::fs::create_dir_all(&path).ok();
                path.join(&name)
            } else {
                path
            }
        }
        None => PathBuf::from(&name),
    }
}

pub fn run_roast(args: RoastArgs) -> Result<()> {
    let cfg = config::load_roast_config()?;
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

    let mut data = analysis::analyze_events(&events, &cfg);

    // Try to get Merkle root (best-effort)
    if let Ok(checkpoint) = data_fetch::fetch_checkpoint(&client) {
        data.merkle_root = checkpoint
            .get("root_hash")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
    }

    let period = period_slug(&args);

    match args.format {
        RoastFormat::Cli => {
            let text = render::render_cli(&data);
            if let Some(ref p) = args.output {
                let path = resolve_output_path(Some(p), period, "txt");
                std::fs::write(&path, &text)
                    .with_context(|| format!("failed to write to {}", path.display()))?;
                eprintln!("Saved to {}", path.display());
            } else {
                print!("{text}");
            }
        }
        RoastFormat::Json => {
            let text = render::render_json(&data);
            if let Some(ref p) = args.output {
                let path = resolve_output_path(Some(p), period, "json");
                std::fs::write(&path, &text)
                    .with_context(|| format!("failed to write to {}", path.display()))?;
                eprintln!("Saved to {}", path.display());
            } else {
                print!("{text}");
            }
        }
        RoastFormat::Svg => {
            let svg = if args.today {
                render::render_vibe_svg(&data)
            } else {
                render::render_personality_svg(&data)
            };
            let path = resolve_output_path(args.output.as_deref(), period, "svg");
            std::fs::write(&path, &svg)
                .with_context(|| format!("failed to write to {}", path.display()))?;
            eprintln!("Saved to {}", path.display());
        }
        RoastFormat::Png => {
            return export_png(&data, args.today, args.output.as_deref(), period);
        }
    }

    Ok(())
}

#[cfg(feature = "roast-png")]
fn export_png(
    data: &analysis::RoastData,
    today: bool,
    output: Option<&str>,
    period: &str,
) -> Result<()> {
    let scale = 2;
    let png_data = render::render_png(data, today, scale)?;
    let path = resolve_output_path(output, period, "png");

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }

    std::fs::write(&path, &png_data)
        .with_context(|| format!("failed to write to {}", path.display()))?;
    eprintln!("Saved to {}", path.display());
    Ok(())
}

#[cfg(not(feature = "roast-png"))]
fn export_png(
    _data: &analysis::RoastData,
    _today: bool,
    _output: Option<&str>,
    _period: &str,
) -> Result<()> {
    anyhow::bail!(
        "--png requires the 'roast-png' feature. Rebuild with: cargo build --features roast-png"
    );
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
    fn parse_args_png() {
        let mut args = vec!["--png".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(matches!(parsed.format, RoastFormat::Png));
    }

    #[test]
    fn parse_args_export_compat() {
        // --export still works as alias for --png
        let mut args = vec!["--export".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(matches!(parsed.format, RoastFormat::Png));
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
    fn parse_args_today_with_png() {
        let mut args = vec!["--today".to_string(), "--png".to_string()].into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(parsed.today);
        assert!(matches!(parsed.format, RoastFormat::Png));
        assert_eq!(parsed.days, Some(1));
    }

    #[test]
    fn parse_args_png_with_output() {
        let mut args = vec![
            "--png".to_string(),
            "-o".to_string(),
            "my-card.png".to_string(),
        ]
        .into_iter();
        let parsed = parse_args(&mut args).unwrap();
        assert!(matches!(parsed.format, RoastFormat::Png));
        assert_eq!(parsed.output, Some("my-card.png".to_string()));
    }

    #[test]
    fn parse_args_unknown_flag_errors() {
        let mut args = vec!["--bogus".to_string()].into_iter();
        assert!(parse_args(&mut args).is_err());
    }

    // --- resolve_output_path tests ---

    #[test]
    fn resolve_path_none_default_period() {
        let path = resolve_output_path(None, "", "png");
        assert_eq!(path, PathBuf::from("punkgo-roast.png"));
    }

    #[test]
    fn resolve_path_none_today() {
        let path = resolve_output_path(None, "today", "png");
        assert_eq!(path, PathBuf::from("punkgo-roast-today.png"));
    }

    #[test]
    fn resolve_path_none_week() {
        let path = resolve_output_path(None, "week", "svg");
        assert_eq!(path, PathBuf::from("punkgo-roast-week.svg"));
    }

    #[test]
    fn resolve_path_file_overrides() {
        let path = resolve_output_path(Some("my-card.png"), "today", "png");
        assert_eq!(path, PathBuf::from("my-card.png"));
    }

    #[test]
    fn resolve_path_trailing_slash() {
        let path = resolve_output_path(Some("cards/"), "week", "svg");
        assert_eq!(path, PathBuf::from("cards/punkgo-roast-week.svg"));
    }

    #[test]
    fn resolve_path_trailing_backslash() {
        let path = resolve_output_path(Some("cards\\"), "month", "png");
        assert_eq!(path, PathBuf::from("cards\\/punkgo-roast-month.png"));
    }

    // --- period_slug tests ---

    #[test]
    fn period_slug_default() {
        let args = RoastArgs {
            format: RoastFormat::Cli,
            days: None,
            actor: None,
            output: None,
            today: false,
        };
        assert_eq!(period_slug(&args), "");
    }

    #[test]
    fn period_slug_today() {
        let args = RoastArgs {
            format: RoastFormat::Png,
            days: Some(1),
            actor: None,
            output: None,
            today: true,
        };
        assert_eq!(period_slug(&args), "today");
    }

    #[test]
    fn period_slug_week() {
        let args = RoastArgs {
            format: RoastFormat::Png,
            days: Some(7),
            actor: None,
            output: None,
            today: false,
        };
        assert_eq!(period_slug(&args), "week");
    }

    #[test]
    fn period_slug_custom_days() {
        let args = RoastArgs {
            format: RoastFormat::Png,
            days: Some(5),
            actor: None,
            output: None,
            today: false,
        };
        assert_eq!(period_slug(&args), "custom");
    }
}
