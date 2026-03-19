#![allow(dead_code)]

use std::collections::BTreeMap;

use chrono::Timelike;
use serde::Serialize;
use serde_json::Value;

use crate::data_fetch;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum Personality {
    Philosopher,
    Intern,
    Rereader,
    Perfectionist,
    Vampire,
    Goldfish,
    Brute,
    Ghost,
    Speedrunner,
    Googler,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum Trait {
    Nocturnal,
    Obsessive,
    Chatty,
    LoneWolf,
    Delegator,
    Overachiever,
}

#[derive(Debug, Clone, Serialize)]
pub struct RpgStats {
    pub str_val: u8,
    pub int_val: u8,
    pub dex_val: u8,
    pub luk_val: u8,
    pub cha_val: u8,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorstMoment {
    pub description: String,
    pub detail: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoastData {
    pub total_events: usize,
    pub period_days: u64,
    pub personality: Personality,
    pub traits: Vec<Trait>,
    pub rpg: RpgStats,
    pub type_counts: BTreeMap<String, usize>,
    pub fail_count: usize,
    pub fail_rate: f64,
    pub session_starts: usize,
    pub session_ends: usize,
    pub hour_distribution: [u32; 24],
    pub worst_moments: Vec<WorstMoment>,
    pub most_read_file: Option<(String, usize)>,
    pub think_do_ratio: f64,
    pub edit_write_ratio: f64,
    pub peak_hour: u8,
    pub merkle_root: Option<String>,
}

// ---------------------------------------------------------------------------
// Core analysis
// ---------------------------------------------------------------------------

pub fn analyze_events(events: &[Value]) -> RoastData {
    let total_events = events.len();

    let mut type_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut hour_distribution = [0u32; 24];
    let mut file_read_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut session_starts: usize = 0;
    let mut session_ends: usize = 0;
    let mut first_ts: Option<u64> = None;
    let mut last_ts: Option<u64> = None;
    let mut fail_count: usize = 0;

    for event in events {
        let etype = data_fetch::event_type(event).to_string();
        let target = data_fetch::event_target(event).to_string();

        // type_counts
        *type_counts.entry(etype.clone()).or_insert(0) += 1;

        // hour distribution
        if let Some(ts_ms) = data_fetch::parse_event_timestamp_ms(event) {
            if let Some(dt) = chrono::DateTime::from_timestamp_millis(ts_ms as i64) {
                let local = dt.with_timezone(&chrono::Local);
                hour_distribution[local.hour() as usize] += 1;
            }
            // track period
            first_ts = Some(match first_ts {
                None => ts_ms,
                Some(prev) => prev.min(ts_ms),
            });
            last_ts = Some(match last_ts {
                None => ts_ms,
                Some(prev) => prev.max(ts_ms),
            });
        }

        // file read counts — strip "hook/file:" prefix
        if etype == "file_read" {
            let path = target
                .strip_prefix("hook/file:")
                .unwrap_or(&target)
                .to_string();
            *file_read_counts.entry(path).or_insert(0) += 1;
        }

        // session tracking
        if etype == "session_start" {
            session_starts += 1;
        } else if etype == "session_end" {
            session_ends += 1;
        }

        // fail count
        if etype.contains("failed") {
            fail_count += 1;
        }
    }

    // period_days
    let period_days = match (first_ts, last_ts) {
        (Some(a), Some(b)) if b > a => (b - a) / 86_400_000 + 1,
        (Some(_), Some(_)) => 1,
        _ => 0,
    };

    // fail_rate
    let fail_rate = if total_events > 0 {
        fail_count as f64 / total_events as f64 * 100.0
    } else {
        0.0
    };

    // event-type helpers (read from type_counts for zero-copy)
    let tc = &type_counts;
    let count = |k: &str| -> usize { *tc.get(k).unwrap_or(&0) };

    let file_read = count("file_read");
    let content_search = count("content_search");
    let web_search = count("web_search");
    let file_search = count("file_search");
    let file_write = count("file_write");
    let file_edit = count("file_edit");

    // think_do_ratio
    let think = (file_read + content_search + web_search + file_search) as f64;
    let do_ = (file_write + file_edit) as f64;
    let think_do_ratio = if do_ > 0.0 { think / do_ } else { f64::MAX };

    // edit_write_ratio
    let edit_write_ratio = if file_write > 0 {
        file_edit as f64 / file_write as f64
    } else {
        0.0
    };

    // peak_hour
    let peak_hour = hour_distribution
        .iter()
        .enumerate()
        .max_by_key(|(_, &v)| v)
        .map(|(i, _)| i as u8)
        .unwrap_or(0);

    // most_read_file
    let most_read_file = file_read_counts
        .iter()
        .max_by_key(|(_, &cnt)| cnt)
        .map(|(path, &cnt)| (path.clone(), cnt));

    // max read count for personality checks
    let max_file_read_count = most_read_file.as_ref().map(|(_, c)| *c).unwrap_or(0);

    // personality / traits / rpg
    let personality = classify_personality(
        think_do_ratio,
        fail_rate,
        total_events,
        max_file_read_count,
        edit_write_ratio,
        &hour_distribution,
        session_starts,
        session_ends,
        web_search,
        count("command_execution_failed"),
    );

    let user_prompt = count("user_prompt");
    let subagent_start = count("subagent_start");
    let traits = classify_traits(
        peak_hour,
        max_file_read_count,
        user_prompt,
        total_events,
        subagent_start,
        session_starts,
    );

    let rpg = compute_rpg(
        file_write,
        file_edit,
        file_read,
        content_search,
        web_search,
        file_search,
        total_events,
        fail_rate,
        user_prompt,
        session_starts,
    );

    let worst_moments =
        detect_worst_moments(&most_read_file, session_starts, session_ends, fail_count);

    RoastData {
        total_events,
        period_days,
        personality,
        traits,
        rpg,
        type_counts,
        fail_count,
        fail_rate,
        session_starts,
        session_ends,
        hour_distribution,
        worst_moments,
        most_read_file,
        think_do_ratio,
        edit_write_ratio,
        peak_hour,
        merkle_root: None,
    }
}

// ---------------------------------------------------------------------------
// Personality classification (priority order, first match wins)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn classify_personality(
    think_do_ratio: f64,
    fail_rate: f64,
    total: usize,
    max_file_read_count: usize,
    edit_write_ratio: f64,
    hour_distribution: &[u32; 24],
    session_starts: usize,
    session_ends: usize,
    web_search: usize,
    cmd_exec_failed: usize,
) -> Personality {
    // 1. Philosopher
    if think_do_ratio > 2.5 {
        return Personality::Philosopher;
    }

    // 2. Intern
    if fail_rate > 3.0 && total > 1000 {
        return Personality::Intern;
    }

    // 3. Rereader
    if max_file_read_count >= 40 {
        return Personality::Rereader;
    }

    // 4. Perfectionist
    if edit_write_ratio > 2.0 {
        return Personality::Perfectionist;
    }

    // 5. Vampire: hours 20-23 + 0-4 events > 50% of total
    {
        let night_events: u32 = hour_distribution[20..=23]
            .iter()
            .chain(hour_distribution[0..=4].iter())
            .sum();
        let total_hour_events: u32 = hour_distribution.iter().sum();
        if total_hour_events > 0 && (night_events as f64 / total_hour_events as f64) > 0.5 {
            return Personality::Vampire;
        }
    }

    // 6. Goldfish: Phase 2: sliding window
    // (skipped)

    // 7. Brute: command_execution_failed >= 10
    // Phase 2: consecutive detection
    if cmd_exec_failed >= 10 {
        return Personality::Brute;
    }

    // 8. Ghost
    if session_starts > 0 {
        let ghost_ratio = (session_starts as f64 - session_ends as f64) / session_starts as f64;
        if ghost_ratio > 0.3 {
            return Personality::Ghost;
        }
    }

    // 9. Speedrunner: Phase 2: per-minute peak
    // (skipped)

    // 10. Googler
    if total > 0 && (web_search as f64 / total as f64) > 0.15 {
        return Personality::Googler;
    }

    // Fallback
    Personality::Philosopher
}

// ---------------------------------------------------------------------------
// Trait classification (max 2)
// ---------------------------------------------------------------------------

fn classify_traits(
    peak_hour: u8,
    max_file_read_count: usize,
    user_prompt: usize,
    total: usize,
    subagent_start: usize,
    session_starts: usize,
) -> Vec<Trait> {
    let mut candidates: Vec<Trait> = Vec::new();

    // Nocturnal
    if peak_hour >= 20 || peak_hour <= 3 {
        candidates.push(Trait::Nocturnal);
    }

    // Obsessive
    if max_file_read_count >= 30 {
        candidates.push(Trait::Obsessive);
    }

    // Chatty
    if total > 0 && (user_prompt as f64 / total as f64) > 0.10 {
        candidates.push(Trait::Chatty);
    }

    // Overachiever: total / prompts > 30
    let prompts = user_prompt.max(1);
    if total as f64 / prompts as f64 > 30.0 {
        candidates.push(Trait::Overachiever);
    }

    // LoneWolf
    if total > 100 && subagent_start == 0 {
        candidates.push(Trait::LoneWolf);
    }

    // Delegator
    if session_starts > 0 && (subagent_start as f64 / session_starts as f64) > 0.5 {
        candidates.push(Trait::Delegator);
    }

    candidates.truncate(2);
    candidates
}

// ---------------------------------------------------------------------------
// RPG stats
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn compute_rpg(
    file_write: usize,
    file_edit: usize,
    file_read: usize,
    content_search: usize,
    web_search: usize,
    file_search: usize,
    total: usize,
    fail_rate: f64,
    user_prompt: usize,
    session_starts: usize,
) -> RpgStats {
    let total_f = total as f64;

    // STR: (writes + edits) / total * 300, capped at 100
    let str_val = if total_f > 0.0 {
        ((file_write + file_edit) as f64 / total_f * 300.0).min(100.0) as u8
    } else {
        0
    };

    // INT: (reads + searches) / total * 200, capped at 100
    let int_val = if total_f > 0.0 {
        ((file_read + content_search + web_search + file_search) as f64 / total_f * 200.0)
            .min(100.0) as u8
    } else {
        0
    };

    // DEX: 50 (placeholder, Phase 2)
    let dex_val: u8 = 50;

    // LUK: (100 - fail_rate * 10), clamped 0-100
    let luk_val = (100.0 - fail_rate * 10.0).clamp(0.0, 100.0) as u8;

    // CHA: (prompts / sessions * 5), capped at 100
    let cha_val = if session_starts > 0 {
        (user_prompt as f64 / session_starts as f64 * 5.0).min(100.0) as u8
    } else {
        0
    };

    RpgStats {
        str_val,
        int_val,
        dex_val,
        luk_val,
        cha_val,
    }
}

// ---------------------------------------------------------------------------
// Worst moments
// ---------------------------------------------------------------------------

fn detect_worst_moments(
    most_read_file: &Option<(String, usize)>,
    session_starts: usize,
    session_ends: usize,
    fail_count: usize,
) -> Vec<WorstMoment> {
    let mut moments: Vec<WorstMoment> = Vec::new();

    // Most-read file if count >= 10
    if let Some((path, count)) = most_read_file {
        if *count >= 10 {
            let filename = std::path::Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(path.as_str());
            moments.push(WorstMoment {
                description: format!("Read {filename} {count} times"),
                detail: "(obsession level: concerning)".to_string(),
                count: *count,
            });
        }
    }

    // Ghost sessions if > 2
    let ghost_count = session_starts.saturating_sub(session_ends);
    if ghost_count > 2 {
        moments.push(WorstMoment {
            description: format!("{ghost_count} ghost sessions"),
            detail: "(started, never finished)".to_string(),
            count: ghost_count,
        });
    }

    // Total failures if > 5
    if fail_count > 5 {
        moments.push(WorstMoment {
            description: format!("{fail_count} failed operations"),
            detail: "(confidence > competence)".to_string(),
            count: fail_count,
        });
    }

    // Sort descending by count
    moments.sort_by(|a, b| b.count.cmp(&a.count));
    moments.truncate(3);
    moments
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_event(event_type: &str, ts_ms: u64, target: &str) -> Value {
        json!({
            "payload": {
                "event_type": event_type,
                "client_timestamp": ts_ms,
                "metadata": {}
            },
            "target": format!("hook/file:{}", target),
            "settled_energy": 1
        })
    }

    fn make_events(specs: &[(&str, usize)]) -> Vec<Value> {
        let mut events = Vec::new();
        let mut ts = 1_710_000_000_000u64;
        for (etype, count) in specs {
            for _ in 0..*count {
                events.push(make_event(etype, ts, "src/main.rs"));
                ts += 1000;
            }
        }
        events
    }

    #[test]
    fn philosopher_when_think_do_high() {
        let events = make_events(&[
            ("file_read", 100),
            ("web_search", 50),
            ("file_write", 20),
            ("file_edit", 10),
            ("command_execution", 50),
        ]);
        let data = analyze_events(&events);
        assert_eq!(data.personality, Personality::Philosopher);
    }

    #[test]
    fn intern_when_high_fail_rate() {
        let events = make_events(&[
            ("file_read", 500),
            ("file_write", 200),
            ("command_execution", 300),
            ("command_execution_failed", 100),
        ]);
        let data = analyze_events(&events);
        assert_eq!(data.personality, Personality::Intern);
    }

    #[test]
    fn rpg_luk_inversely_related_to_fail_rate() {
        let events = make_events(&[("file_read", 100)]);
        let data = analyze_events(&events);
        assert!(data.rpg.luk_val > 90);
    }

    #[test]
    fn worst_moments_sorted_by_count() {
        let data = analyze_events(&make_events(&[
            ("file_read", 200),
            ("command_execution_failed", 30),
            ("session_start", 10),
            ("session_end", 3),
        ]));
        assert!(!data.worst_moments.is_empty());
        for w in data.worst_moments.windows(2) {
            assert!(w[0].count >= w[1].count);
        }
    }

    #[test]
    fn personality_priority_philosopher_over_rereader() {
        // Both should trigger, but Philosopher has higher priority
        let mut events = make_events(&[
            ("file_read", 150),
            ("web_search", 50),
            ("file_write", 20),
            ("file_edit", 10),
        ]);
        // Make 50 of the reads target the same file to trigger Rereader
        for e in events.iter_mut().take(50) {
            e["target"] = json!("hook/file:src/setup.rs");
        }
        let data = analyze_events(&events);
        assert_eq!(data.personality, Personality::Philosopher);
    }

    #[test]
    fn empty_events_returns_defaults() {
        let data = analyze_events(&[]);
        assert_eq!(data.total_events, 0);
        assert_eq!(data.personality, Personality::Philosopher); // fallback
    }

    #[test]
    fn traits_max_two() {
        let events = make_events(&[
            ("file_read", 100),
            ("user_prompt", 50),
            ("file_write", 10),
            ("subagent_start", 0),
        ]);
        let data = analyze_events(&events);
        assert!(data.traits.len() <= 2);
    }
}
