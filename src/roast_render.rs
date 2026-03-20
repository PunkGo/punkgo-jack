#![allow(dead_code)]

use crate::roast_analysis::RoastData;

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
        data.personality.emoji, data.personality.name
    ));

    // Traits
    let trait_labels: Vec<String> = data.traits.iter().map(|t| t.display_label()).collect();
    if !trait_labels.is_empty() {
        out.push_str(&format!("       {}\n", trait_labels.join(" · ")));
    }
    out.push('\n');
    out.push_str(&format!("       \"{}\"\n", data.quip));
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

    // CTA — strip "THE " prefix for natural English
    let name_lower = data.personality.name.to_lowercase();
    let name_short = name_lower.strip_prefix("the ").unwrap_or(&name_lower);
    out.push_str(&format!("    Your AI is a {}.\n", name_short));
    out.push_str("    What's yours?\n");
    out.push_str("    > punkgo.ai/roast\n");
    out.push_str("  ========================================\n");

    out
}

pub fn render_json(data: &RoastData) -> String {
    serde_json::to_string_pretty(data).unwrap_or_else(|_| "{}".into())
}

/// Tiny helper: build a single SVG element string.
fn svg_el(tag: &str, attrs: &str, content: &str) -> String {
    if content.is_empty() {
        format!("<{tag} {attrs}/>", tag = tag, attrs = attrs)
    } else {
        format!(
            "<{tag} {attrs}>{content}</{tag}>",
            tag = tag,
            attrs = attrs,
            content = content,
        )
    }
}

pub fn render_svg(data: &RoastData) -> String {
    // ---- dynamic fragments ----

    // RPG bar rows
    let stat_defs = [
        ("STR", data.rpg.str_val, "#ff6b6b"),
        ("INT", data.rpg.int_val, "#4ecdc4"),
        ("DEX", data.rpg.dex_val, "#45b7d1"),
        ("LUK", data.rpg.luk_val, "#96ceb4"),
        ("CHA", data.rpg.cha_val, "#feca57"),
    ];
    let mut rpg_rows = String::new();
    for (i, (stat_name, val, color)) in stat_defs.iter().enumerate() {
        let y = 370 + i * 28;
        let bar_bg_y = y - 9;
        let bar_w = ((*val as f32 / 100.0) * 180.0) as u32;
        let delay_ms = i * 150;

        rpg_rows.push_str(&svg_el(
            "text",
            &format!(
                "x=\"30\" y=\"{y}\" class=\"stat\" \
                 style=\"fill:#8b949e;font-size:11px;font-family:monospace;\
                 animation-delay:{delay_ms}ms\"",
                y = y,
                delay_ms = delay_ms,
            ),
            stat_name,
        ));
        rpg_rows.push('\n');

        rpg_rows.push_str(&svg_el(
            "rect",
            &format!(
                "x=\"65\" y=\"{y}\" width=\"180\" height=\"10\" rx=\"3\" fill=\"#21262d\"",
                y = bar_bg_y,
            ),
            "",
        ));
        rpg_rows.push('\n');

        rpg_rows.push_str(&svg_el(
            "rect",
            &format!(
                "x=\"65\" y=\"{y}\" width=\"{w}\" height=\"10\" rx=\"3\" \
                 fill=\"{color}\" class=\"bar\" \
                 style=\"animation-delay:{delay_ms}ms\"",
                y = bar_bg_y,
                w = bar_w,
                color = color,
                delay_ms = delay_ms,
            ),
            "",
        ));
        rpg_rows.push('\n');

        rpg_rows.push_str(&svg_el(
            "text",
            &format!(
                "x=\"255\" y=\"{y}\" class=\"stat\" \
                 style=\"fill:#e6edf3;font-size:11px;font-family:monospace;\
                 animation-delay:{delay_ms}ms\"",
                y = y,
                delay_ms = delay_ms,
            ),
            &val.to_string(),
        ));
        rpg_rows.push('\n');
    }

    // Evidence rows
    let evidence_defs: &[(&str, &str)] = &[
        ("command_execution", "Bash commands"),
        ("file_read", "File reads"),
        ("web_search", "Web searches"),
        ("file_edit", "File edits"),
        ("file_write", "File writes"),
    ];
    let mut ev_rows = String::new();
    for (i, (key, label)) in evidence_defs.iter().enumerate() {
        let count = data.type_counts.get(*key).copied().unwrap_or(0);
        let y = 528 + i * 18;
        ev_rows.push_str(&svg_el(
            "text",
            &format!(
                "x=\"30\" y=\"{y}\" style=\"fill:#8b949e;font-size:11px;font-family:monospace\"",
                y = y,
            ),
            label,
        ));
        ev_rows.push('\n');
        ev_rows.push_str(&svg_el(
            "text",
            &format!(
                "x=\"410\" y=\"{y}\" \
                 style=\"fill:#e6edf3;font-size:11px;font-family:monospace;text-anchor:end\"",
                y = y,
            ),
            &comma(count),
        ));
        ev_rows.push('\n');
    }

    // Optional Merkle row
    let merkle_row = match &data.merkle_root {
        Some(hash) => {
            let short: String = hash.chars().take(20).collect();
            svg_el(
                "text",
                "x=\"220\" y=\"642\" \
                 style=\"fill:#484f58;font-size:9px;font-family:monospace;text-anchor:middle\"",
                &format!("{short}...", short = short),
            ) + "\n"
        }
        None => String::new(),
    };

    let trait_str: String = data
        .traits
        .iter()
        .map(|t| t.display_label())
        .collect::<Vec<_>>()
        .join(" . ");

    let name_lower = data.personality.name.to_lowercase();
    let personality_name = &data.personality.name;
    let emoji = &data.personality.emoji;
    let catchphrase = &data.quip;
    let period_days = data.period_days;
    let total_events_str = comma(data.total_events);
    let fail_rate = data.fail_rate;
    let total_label = comma(data.total_events);

    // ---- assemble SVG ----
    let css_block = "<style>\n    \
        @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }\n    \
        @keyframes fillBar { from { width: 0; } }\n    \
        .stat { opacity: 0; animation: fadeIn 0.4s ease forwards; }\n    \
        .bar { animation: fillBar 0.8s ease forwards; }\n  \
        </style>";

    let mut out = String::with_capacity(4096);

    out.push_str("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"440\" height=\"680\" viewBox=\"0 0 440 680\">\n");
    out.push_str("<defs>\n  ");
    out.push_str(css_block);
    out.push_str("\n</defs>\n");

    out.push_str(
        "<rect width=\"440\" height=\"680\" rx=\"12\" ry=\"12\" \
        fill=\"#0d1117\" stroke=\"#30363d\" stroke-width=\"1.5\"/>\n",
    );

    out.push_str(
        "<text x=\"220\" y=\"36\" \
        style=\"fill:#58a6ff;font-size:13px;font-family:monospace;\
        text-anchor:middle;font-weight:bold;letter-spacing:2px\">PUNKGO ROAST RECEIPT</text>\n",
    );
    out.push_str(
        "<line x1=\"20\" y1=\"48\" x2=\"420\" y2=\"48\" stroke=\"#30363d\" stroke-width=\"1\"/>\n",
    );

    out.push_str(&format!(
        "<text x=\"220\" y=\"68\" \
         style=\"fill:#8b949e;font-size:11px;font-family:monospace;text-anchor:middle\">\
         Period: {days} days | {total} events</text>\n",
        days = period_days,
        total = total_events_str,
    ));

    out.push_str(
        "<text x=\"220\" y=\"100\" \
        style=\"fill:#8b949e;font-size:11px;font-family:monospace;\
        text-anchor:middle;letter-spacing:1px\">YOUR AI IS</text>\n",
    );

    out.push_str(&format!(
        "<text x=\"220\" y=\"136\" \
         style=\"fill:#e6edf3;font-size:22px;font-family:monospace;\
         text-anchor:middle;font-weight:bold\">{emoji} {name}</text>\n",
        emoji = emoji,
        name = personality_name,
    ));

    out.push_str(&format!(
        "<text x=\"220\" y=\"158\" \
         style=\"fill:#8b949e;font-size:11px;font-family:monospace;text-anchor:middle\">{traits}</text>\n",
        traits = trait_str,
    ));

    out.push_str(&format!(
        "<text x=\"220\" y=\"182\" \
         style=\"fill:#7ee787;font-size:11px;font-family:monospace;\
         text-anchor:middle;font-style:italic\">&quot;{phrase}&quot;</text>\n",
        phrase = catchphrase,
    ));

    out.push_str("<line x1=\"20\" y1=\"198\" x2=\"420\" y2=\"198\" stroke=\"#30363d\" stroke-width=\"1\"/>\n");

    out.push_str(
        "<text x=\"220\" y=\"222\" \
        style=\"fill:#58a6ff;font-size:11px;font-family:monospace;\
        text-anchor:middle;letter-spacing:2px\">STATS</text>\n",
    );
    out.push_str(
        "<text x=\"30\" y=\"248\" \
        style=\"fill:#8b949e;font-size:10px;font-family:monospace\">stat</text>\n",
    );
    out.push_str(
        "<text x=\"255\" y=\"248\" \
        style=\"fill:#8b949e;font-size:10px;font-family:monospace\">val</text>\n",
    );
    out.push_str("<line x1=\"20\" y1=\"254\" x2=\"420\" y2=\"254\" stroke=\"#21262d\" stroke-width=\"1\"/>\n");

    out.push_str(&rpg_rows);

    out.push_str("<line x1=\"20\" y1=\"518\" x2=\"420\" y2=\"518\" stroke=\"#30363d\" stroke-width=\"1\"/>\n");

    out.push_str(
        "<text x=\"220\" y=\"516\" \
        style=\"fill:#58a6ff;font-size:11px;font-family:monospace;\
        text-anchor:middle;letter-spacing:2px\">THE EVIDENCE</text>\n",
    );

    out.push_str(&ev_rows);

    out.push_str("<line x1=\"20\" y1=\"612\" x2=\"420\" y2=\"612\" stroke=\"#30363d\" stroke-width=\"1\"/>\n");
    out.push_str("<text x=\"30\" y=\"628\" \
        style=\"fill:#e6edf3;font-size:11px;font-family:monospace;font-weight:bold\">TOTAL</text>\n");
    out.push_str(&format!(
        "<text x=\"410\" y=\"628\" \
         style=\"fill:#e6edf3;font-size:11px;font-family:monospace;\
         text-anchor:end;font-weight:bold\">{total}</text>\n",
        total = total_label,
    ));
    out.push_str(
        "<text x=\"30\" y=\"644\" \
        style=\"fill:#f85149;font-size:11px;font-family:monospace\">FAIL RATE</text>\n",
    );
    out.push_str(&format!(
        "<text x=\"410\" y=\"644\" \
         style=\"fill:#f85149;font-size:11px;font-family:monospace;text-anchor:end\">{rate:.2}%</text>\n",
        rate = fail_rate,
    ));

    out.push_str(&merkle_row);

    out.push_str("<line x1=\"20\" y1=\"654\" x2=\"420\" y2=\"654\" stroke=\"#30363d\" stroke-width=\"1\"/>\n");
    out.push_str(&format!(
        "<text x=\"220\" y=\"668\" \
         style=\"fill:#8b949e;font-size:10px;font-family:monospace;text-anchor:middle\">\
         Your AI is a {name}. What&apos;s yours?</text>\n",
        name = name_lower.strip_prefix("the ").unwrap_or(&name_lower),
    ));
    out.push_str(
        "<text x=\"220\" y=\"678\" \
        style=\"fill:#58a6ff;font-size:10px;font-family:monospace;text-anchor:middle\">\
        punkgo.ai/roast</text>\n",
    );

    out.push_str("</svg>");
    out
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
            personality: MatchedPersonality {
                id: "philosopher".into(),
                name: "THE PHILOSOPHER".into(),
                mbti: "INTP".into(),
                emoji: "\u{1F914}".into(),
                catchphrase: "This needs more research.".into(),
                card_color: "#E0EFDA".into(),
                dog_breed: "Border Collie".into(),
                dog_image: "dog-philosopher.png".into(),
            },
            traits: vec![
                MatchedTrait {
                    id: "nocturnal".into(),
                    label: "Nocturnal".into(),
                    emoji: "\u{1F319}".into(),
                },
                MatchedTrait {
                    id: "obsessive".into(),
                    label: "Obsessive".into(),
                    emoji: "\u{1F504}".into(),
                },
            ],
            quip: "60% reading. 10% writing. The rest? Existential crisis.".into(),
            radar: vec![
                ("Yapping".into(), 95.0),
                ("Googling".into(), 5.0),
                ("Grinding".into(), 40.0),
                ("Shipping".into(), 10.0),
                ("Tunnel Vision".into(), 50.0),
                ("Plot Armor".into(), 98.5),
            ],
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
        assert!(out.contains("Existential crisis"));
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
        data.personality.id = "brute".into();
        data.personality.name = "THE BRUTE".into();
        let out = render_cli(&data);
        assert!(out.contains("a brute"));
    }

    #[test]
    fn json_render_is_valid() {
        let json = render_json(&sample_data());
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["personality"]["id"], "philosopher");
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

    #[test]
    fn svg_render_is_valid_xml() {
        let svg = render_svg(&sample_data());
        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("THE PHILOSOPHER"));
        assert!(svg.contains("@keyframes"));
        assert!(svg.ends_with("</svg>"));
    }

    #[test]
    fn svg_contains_rpg_bars() {
        let svg = render_svg(&sample_data());
        assert!(svg.contains("STR"));
        assert!(svg.contains("INT"));
        assert!(svg.contains("fillBar"));
    }
}
