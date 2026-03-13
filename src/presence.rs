use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use punkgo_core::protocol::{RequestEnvelope, RequestType};
use serde_json::{json, Value};

use crate::ipc_client::{new_request_id, IpcClient};

/// CLI args for `punkgo-jack presence`.
pub struct PresenceArgs {
    pub days: usize,
    pub actor: Option<String>,
}

pub fn parse_args(args: &mut impl Iterator<Item = String>) -> Result<PresenceArgs> {
    let mut days: usize = 14;
    let mut actor: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--actor" => {
                actor = Some(
                    args.next()
                        .context("--actor requires a value (e.g. claude-code)")?,
                );
            }
            s if s.parse::<usize>().is_ok() => {
                days = s.parse().unwrap();
            }
            other => bail!("unknown presence option: {other}"),
        }
    }

    Ok(PresenceArgs { days, actor })
}

pub fn run_presence(args: PresenceArgs) -> Result<()> {
    let client = IpcClient::from_env(None);
    let actor_id = args.actor.or_else(|| {
        crate::session::latest_session()
            .ok()
            .flatten()
            .map(|s| s.actor_id)
    });
    let events = fetch_all_events(&client, actor_id.as_deref())?;

    if events.is_empty() {
        eprintln!("No events recorded yet.");
        return Ok(());
    }

    let (heatmap, stats) = build_heatmap(&events, args.days);
    render(&heatmap, &stats);
    Ok(())
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

struct DayRow {
    label: String,
    hours: [u32; 24],
    total: u32,
}

struct Stats {
    total_events: usize,
    total_energy: u64,
    total_decisions: usize,
    total_sessions: usize,
    total_days: usize,
    pre_count: usize,
    post_count: usize,
    failed_count: usize,
    peak_hour_label: String,
}

fn build_heatmap(events: &[Value], max_days: usize) -> (Vec<DayRow>, Stats) {
    // day -> hour -> energy, also day -> decisions
    let mut day_hour: BTreeMap<String, [u32; 24]> = BTreeMap::new();
    let mut day_decisions: BTreeMap<String, u32> = BTreeMap::new();
    let mut sessions = std::collections::BTreeSet::new();
    let mut pre_count = 0usize;
    let mut post_count = 0usize;
    let mut failed_count = 0usize;
    let mut total_energy = 0u64;

    for event in events {
        let ts_ms = event
            .pointer("/payload/client_timestamp")
            .and_then(|v| v.as_u64())
            .or_else(|| {
                event.get("timestamp").and_then(|v| {
                    v.as_u64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
            });

        let Some(ts_ms) = ts_ms else { continue };

        let dt = chrono::DateTime::from_timestamp_millis(ts_ms as i64);
        let Some(dt) = dt else { continue };
        let local = dt.with_timezone(&chrono::Local);
        let day_key = local.format("%Y-%m-%d").to_string();
        let hour = local.format("%H").to_string().parse::<usize>().unwrap_or(0);

        // Accumulate energy (settled_energy) per hour bucket
        let energy = event
            .get("settled_energy")
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32;
        let entry = day_hour.entry(day_key.clone()).or_insert([0; 24]);
        entry[hour] += energy;
        total_energy += energy as u64;

        let etype = event
            .pointer("/payload/event_type")
            .and_then(Value::as_str)
            .unwrap_or("");

        if etype == "user_prompt" {
            *day_decisions.entry(day_key.clone()).or_insert(0) += 1;
        }

        if etype.ends_with("_pre") {
            pre_count += 1;
        } else if etype.contains("failed") {
            failed_count += 1;
        } else if !matches!(etype, "user_prompt" | "session_start" | "session_end" | "") {
            post_count += 1;
        }

        if let Some(sid) = event
            .pointer("/payload/metadata/punkgo_session_id")
            .and_then(Value::as_str)
        {
            sessions.insert(sid.to_string());
        }
    }

    // Build rows for the last `max_days` calendar days (always show full range,
    // like GitHub's contribution graph — empty days get all-zero rows).
    let today = chrono::Local::now().date_naive();
    let selected_days: Vec<String> = (0..max_days)
        .rev()
        .map(|i| {
            (today - chrono::Duration::days(i as i64))
                .format("%Y-%m-%d")
                .to_string()
        })
        .collect();

    // Peak hour across all days
    let mut hour_totals = [0u32; 24];
    for hours in day_hour.values() {
        for (i, c) in hours.iter().enumerate() {
            hour_totals[i] += c;
        }
    }
    let peak_hour_idx = hour_totals
        .iter()
        .enumerate()
        .max_by_key(|(_, c)| *c)
        .map(|(i, _)| i)
        .unwrap_or(0);

    let rows: Vec<DayRow> = selected_days
        .iter()
        .map(|day| {
            let hours = day_hour.get(day).copied().unwrap_or([0; 24]);
            let total: u32 = hours.iter().sum();

            // Format label: "3/6 Thu"
            let label = if let Ok(d) = chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d") {
                format!("{}/{} {}", d.format("%-m"), d.format("%-d"), d.format("%a"))
            } else {
                day.clone()
            };

            DayRow {
                label,
                hours,
                total,
            }
        })
        .collect();

    let stats = Stats {
        total_events: events.len(),
        total_energy,
        total_decisions: day_decisions.values().sum::<u32>() as usize,
        total_sessions: sessions.len(),
        total_days: selected_days.len(),
        pre_count,
        post_count,
        failed_count,
        peak_hour_label: format!("{:02}:00", peak_hour_idx),
    };

    (rows, stats)
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

fn render(rows: &[DayRow], stats: &Stats) {
    // ANSI 256 color: purple gradient
    // Levels: empty, l1 (dim), l2, l3, l4, l5 (bright)
    let global_max: u32 = rows
        .iter()
        .flat_map(|r| r.hours.iter())
        .copied()
        .max()
        .unwrap_or(1);

    fn level(count: u32, max: u32) -> usize {
        if count == 0 {
            return 0;
        }
        let ratio = count as f64 / max as f64;
        if ratio < 0.1 {
            1
        } else if ratio < 0.25 {
            2
        } else if ratio < 0.50 {
            3
        } else if ratio < 0.75 {
            4
        } else {
            5
        }
    }

    // Use background-colored spaces for a tight GitHub-style grid.
    // ANSI 256 bg colors: purple gradient
    let bg_colors = [0, 22, 28, 34, 40, 46]; // black, dark→bright green

    // Header
    println!();
    println!("  \x1b[1;38;5;40m⚡ punkgo-jack\x1b[0m");

    // Rows — each cell is 2 chars wide (bg-colored "  ")
    for row in rows {
        print!("  \x1b[38;5;245m{:>7}\x1b[0m ", row.label);

        for h in 0..24 {
            let lvl = level(row.hours[h], global_max);
            let bg = bg_colors[lvl];
            print!("\x1b[48;5;{bg}m  \x1b[0m");
        }

        // Day total energy (dim, right of the row)
        print!(
            " \x1b[38;5;240m{:>6}\x1b[0m",
            format_number(row.total as usize)
        );
        println!();
    }

    // Stats
    println!();
    let accounted = stats.post_count + stats.failed_count;
    let completeness = if stats.pre_count > 0 {
        accounted as f64 / stats.pre_count as f64 * 100.0
    } else {
        100.0
    };

    println!(
        "  \x1b[1;38;5;40m⚡{}\x1b[0m energy \x1b[38;5;245m·\x1b[0m \x1b[1m{}\x1b[0m actions \x1b[38;5;245m·\x1b[0m \x1b[1m{}\x1b[0m decisions \x1b[38;5;245m·\x1b[0m \x1b[38;5;40m{:.1}%\x1b[0m receipted",
        format_number(stats.total_energy as usize),
        format_number(stats.total_events),
        format_number(stats.total_decisions),
        completeness,
    );
    println!(
        "  \x1b[38;5;245m{} sessions · peak {} · {} days\x1b[0m",
        stats.total_sessions, stats.peak_hour_label, stats.total_days,
    );
    println!();
    println!("  \x1b[38;5;240mRun \x1b[0mpunkgo-jack presence\x1b[38;5;240m or ask Claude \x1b[0m\"show my punkgo\"\x1b[38;5;240m to see this again.\x1b[0m");
    println!();
}

fn format_number(n: usize) -> String {
    if n >= 1000 {
        format!("{},{:03}", n / 1000, n % 1000)
    } else {
        n.to_string()
    }
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

fn fetch_all_events(client: &IpcClient, actor_id: Option<&str>) -> Result<Vec<Value>> {
    let mut all: Vec<Value> = Vec::new();
    let mut before_index: Option<i64> = None;

    loop {
        let mut payload = json!({
            "kind": "events",
            "limit": 500
        });
        if let Some(actor) = actor_id {
            payload["actor_id"] = json!(actor);
        }
        if let Some(bi) = before_index {
            payload["before_index"] = json!(bi);
        }

        let req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Read,
            payload,
        };

        let resp = client.send(&req).context("failed to query events")?;
        if resp.status != "ok" {
            let msg = resp
                .payload
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            bail!("failed to query events from kernel: {msg}. Is punkgo-kerneld running?");
        }

        let events = resp
            .payload
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        if events.is_empty() {
            break;
        }

        let has_more = resp
            .payload
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let next_cursor = resp.payload.get("next_cursor").and_then(Value::as_i64);

        all.extend(events);

        if !has_more || next_cursor.is_none() {
            break;
        }
        before_index = next_cursor;
    }

    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_number_with_comma() {
        assert_eq!(format_number(8572), "8,572");
        assert_eq!(format_number(315), "315");
        assert_eq!(format_number(12345), "12,345");
    }

    #[test]
    fn parse_args_default_14_days() {
        let args = Vec::<String>::new();
        let parsed = parse_args(&mut args.into_iter()).unwrap();
        assert_eq!(parsed.days, 14);
        assert!(parsed.actor.is_none());
    }

    #[test]
    fn parse_args_custom_days() {
        let args = vec!["7".to_string()];
        let parsed = parse_args(&mut args.into_iter()).unwrap();
        assert_eq!(parsed.days, 7);
    }

    #[test]
    fn parse_args_with_actor() {
        let args = vec![
            "--actor".to_string(),
            "openclaw".to_string(),
            "7".to_string(),
        ];
        let parsed = parse_args(&mut args.into_iter()).unwrap();
        assert_eq!(parsed.days, 7);
        assert_eq!(parsed.actor.as_deref(), Some("openclaw"));
    }
}
