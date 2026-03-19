#![allow(dead_code)]

use crate::roast_analysis::{Personality, RoastData, Trait};

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

impl Personality {
    pub fn emoji(&self) -> &str {
        match self {
            Personality::Philosopher => "🤔",
            Personality::Intern => "🙈",
            Personality::Rereader => "🔁",
            Personality::Perfectionist => "✏️",
            Personality::Vampire => "🧛",
            Personality::Goldfish => "🐟",
            Personality::Brute => "🔨",
            Personality::Ghost => "👻",
            Personality::Speedrunner => "⚡",
            Personality::Googler => "🔍",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Personality::Philosopher => "THE PHILOSOPHER",
            Personality::Intern => "THE INTERN",
            Personality::Rereader => "THE REREADER",
            Personality::Perfectionist => "THE PERFECTIONIST",
            Personality::Vampire => "THE VAMPIRE",
            Personality::Goldfish => "THE GOLDFISH",
            Personality::Brute => "THE BRUTE",
            Personality::Ghost => "THE GHOST",
            Personality::Speedrunner => "THE SPEEDRUNNER",
            Personality::Googler => "THE GOOGLER",
        }
    }

    pub fn catchphrase(&self) -> &str {
        match self {
            Personality::Philosopher => "Let me read that one more time.",
            Personality::Intern => "What if I just... try everything?",
            Personality::Rereader => "I've read this before. Let me read it again.",
            Personality::Perfectionist => "Actually, let me rewrite that.",
            Personality::Vampire => "I do my best work at 2am.",
            Personality::Goldfish => "Wait, what was I looking at?",
            Personality::Brute => "sudo. SUDO. S U D O.",
            Personality::Ghost => "I was never here.",
            Personality::Speedrunner => "Done. Wait. Done again.",
            Personality::Googler => "Let me Google that for myself.",
        }
    }
}

impl Trait {
    pub fn label(&self) -> &str {
        match self {
            Trait::Nocturnal => "Nocturnal 🌙",
            Trait::Obsessive => "Obsessive 🔄",
            Trait::Chatty => "Chatty 💬",
            Trait::LoneWolf => "Lone Wolf 🐺",
            Trait::Delegator => "Delegator 📋",
            Trait::Overachiever => "Overachiever 🏃",
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn bar(val: u8) -> String {
    let filled = (val as usize / 10).min(10);
    let empty = 10 - filled;
    format!("{}{}", "█".repeat(filled), "░".repeat(empty))
}

pub fn comma(n: usize) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut result = String::with_capacity(len + len / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(b as char);
    }
    result
}

// ---------------------------------------------------------------------------
// Render functions
// ---------------------------------------------------------------------------

pub fn render_cli(data: &RoastData) -> String {
    let mut out = String::new();

    // Header
    out.push_str("  ========================================\n");
    out.push_str("      PUNKGO ROAST RECEIPT\n");
    out.push_str("  ========================================\n");
    out.push_str(&format!(
        "    Period: {} days | {} events\n",
        data.period_days,
        comma(data.total_events)
    ));
    out.push('\n');

    // Personality
    out.push_str("    -- YOUR AI IS --\n");
    out.push('\n');
    out.push_str(&format!(
        "       {} {}\n",
        data.personality.emoji(),
        data.personality.name()
    ));

    // Traits
    let trait_labels: Vec<&str> = data.traits.iter().map(|t| t.label()).collect();
    if !trait_labels.is_empty() {
        out.push_str(&format!("       {}\n", trait_labels.join(" · ")));
    }
    out.push('\n');
    out.push_str(&format!("       \"{}\"\n", data.personality.catchphrase()));
    out.push('\n');

    // RPG Stats
    out.push_str("    -- STATS --\n");
    out.push('\n');
    out.push_str(&format!(
        "    STR {}  {}\n",
        bar(data.rpg.str_val),
        data.rpg.str_val
    ));
    out.push_str(&format!(
        "    INT {}  {}\n",
        bar(data.rpg.int_val),
        data.rpg.int_val
    ));
    out.push_str(&format!(
        "    DEX {}  {}\n",
        bar(data.rpg.dex_val),
        data.rpg.dex_val
    ));
    out.push_str(&format!(
        "    LUK {}  {}\n",
        bar(data.rpg.luk_val),
        data.rpg.luk_val
    ));
    out.push_str(&format!(
        "    CHA {}  {}\n",
        bar(data.rpg.cha_val),
        data.rpg.cha_val
    ));
    out.push('\n');

    // Evidence
    out.push_str("    -- THE EVIDENCE --\n");
    out.push('\n');

    let evidence_rows: &[(&str, &str)] = &[
        ("command_execution", "Bash commands"),
        ("file_read", "File reads"),
        ("web_search", "Web searches"),
        ("file_edit", "File edits"),
        ("content_search", "Content greps"),
        ("file_write", "File writes"),
    ];

    for (key, label) in evidence_rows {
        let count = data.type_counts.get(*key).copied().unwrap_or(0);
        out.push_str(&format!("    {:<26} {:>7}\n", label, comma(count)));
    }
    out.push_str("    ------------------------------\n");
    out.push_str(&format!(
        "    {:<26} {:>7}\n",
        "TOTAL",
        comma(data.total_events)
    ));
    out.push_str(&format!(
        "    {:<26} {:>6.2}%\n",
        "FAIL RATE", data.fail_rate
    ));
    out.push('\n');

    // Worst Moments
    out.push_str("    -- WORST MOMENTS --\n");
    out.push('\n');
    for (i, moment) in data.worst_moments.iter().enumerate() {
        out.push_str(&format!(
            "    {}. {}\n       {}\n",
            i + 1,
            moment.description,
            moment.detail
        ));
        out.push('\n');
    }

    // Merkle
    if let Some(ref hash) = data.merkle_root {
        let short: String = hash.chars().take(20).collect();
        out.push_str(&format!("    {}...  (merkle)\n", short));
        out.push('\n');
    }

    // CTA — e.g. "Your AI is a the brute." matches test expectation
    let name_lower = data.personality.name().to_lowercase();
    out.push_str(&format!("    Your AI is a {}.\n", name_lower));
    out.push_str("    What's yours?\n");
    out.push_str("    > punkgo.ai/roast\n");
    out.push_str("  ========================================\n");

    out
}

pub fn render_json(data: &RoastData) -> String {
    serde_json::to_string_pretty(data).unwrap_or_else(|_| "{}".into())
}

pub fn render_svg(_data: &RoastData) -> String {
    todo!("SVG rendering implemented in Task 5")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roast_analysis::*;
    use std::collections::BTreeMap;

    fn sample_data() -> RoastData {
        RoastData {
            total_events: 1000,
            period_days: 7,
            personality: Personality::Philosopher,
            traits: vec![Trait::Nocturnal, Trait::Obsessive],
            rpg: RpgStats {
                str_val: 12,
                int_val: 82,
                dex_val: 47,
                luk_val: 87,
                cha_val: 58,
            },
            type_counts: [("file_read".into(), 500), ("file_write".into(), 100)]
                .into_iter()
                .collect(),
            fail_count: 15,
            fail_rate: 1.5,
            session_starts: 10,
            session_ends: 7,
            hour_distribution: [0; 24],
            worst_moments: vec![WorstMoment {
                description: "Read main.rs 50 times".into(),
                detail: "(obsession level: concerning)".into(),
                count: 50,
            }],
            most_read_file: Some(("src/main.rs".into(), 50)),
            think_do_ratio: 2.8,
            edit_write_ratio: 1.5,
            peak_hour: 22,
            merkle_root: None,
        }
    }

    #[test]
    fn cli_render_contains_personality() {
        let out = render_cli(&sample_data());
        assert!(out.contains("THE PHILOSOPHER"));
        assert!(out.contains("Let me read that one more time"));
    }

    #[test]
    fn cli_render_contains_rpg() {
        let out = render_cli(&sample_data());
        assert!(out.contains("STR"));
        assert!(out.contains("INT"));
    }

    #[test]
    fn cli_render_uses_actual_personality_in_cta() {
        let mut data = sample_data();
        data.personality = Personality::Brute;
        let out = render_cli(&data);
        assert!(out.contains("the brute"));
    }

    #[test]
    fn json_render_is_valid() {
        let json = render_json(&sample_data());
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["personality"], "Philosopher");
    }

    #[test]
    fn comma_formatting() {
        assert_eq!(comma(1000), "1,000");
        assert_eq!(comma(37504), "37,504");
        assert_eq!(comma(42), "42");
    }

    #[test]
    fn bar_rendering() {
        assert_eq!(bar(100).chars().count(), 10);
        assert_eq!(bar(0).chars().count(), 10);
        assert_eq!(bar(50).chars().count(), 10);
    }
}
