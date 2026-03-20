#![allow(dead_code)]

use crate::roast_analysis::RoastData;
use crate::roast_assets;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn bar(val: u8) -> String {
    let filled = (val as usize / 10).min(10);
    let empty = 10 - filled;
    format!("{}{}", "\u{2588}".repeat(filled), "\u{2591}".repeat(empty))
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

/// Escape text for safe XML embedding.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\'', "&#39;")
        .replace('"', "&quot;")
}

// ---------------------------------------------------------------------------
// Radar chart geometry (shared by personality card)
// ---------------------------------------------------------------------------

const RADAR_CX: f64 = 200.0;
const RADAR_CY: f64 = 170.0;
const RADAR_R: f64 = 75.0;
const RADAR_ANGLES: [f64; 6] = [-90.0, -30.0, 30.0, 90.0, 150.0, 210.0];
const RADAR_LABELS: [&str; 6] = [
    "Yapping",
    "Googling",
    "Grinding",
    "Shipping",
    "Tunnel Vision",
    "Plot Armor",
];

fn polar(val: f64, angle_deg: f64) -> (f64, f64) {
    let rad = angle_deg.to_radians();
    (
        RADAR_CX + val / 100.0 * RADAR_R * rad.cos(),
        RADAR_CY + val / 100.0 * RADAR_R * rad.sin(),
    )
}

/// Label positions around the radar chart (x, y, text-anchor).
fn radar_label_positions() -> [(f64, f64, &'static str); 6] {
    let cx = RADAR_CX;
    let cy = RADAR_CY;
    let r = RADAR_R;
    [
        (cx, cy - r - 12.0, "middle"),
        (cx + r + 8.0, cy - r * 0.5 + 4.0, "start"),
        (cx + r + 8.0, cy + r * 0.5 + 4.0, "start"),
        (cx, cy + r + 16.0, "middle"),
        (cx - r - 8.0, cy + r * 0.5 + 4.0, "end"),
        (cx - r - 8.0, cy - r * 0.5 + 4.0, "end"),
    ]
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
        out.push_str(&format!("       {}\n", trait_labels.join(" . ")));
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

    // CTA
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

// ---------------------------------------------------------------------------
// Personality Card SVG (400x520) — full card with radar chart
// ---------------------------------------------------------------------------

pub fn render_personality_svg(data: &RoastData) -> String {
    let accent = roast_assets::accent_color(&data.personality.id);
    let bg = &data.personality.card_color;
    let name = roast_assets::short_name(&data.personality.name);
    let mbti = &data.personality.mbti;
    let quip_safe = xml_escape(&data.quip);
    let catch_safe = xml_escape(&data.personality.catchphrase);

    // Dog image
    let dog_b64 = roast_assets::dog_image_base64(&data.personality.dog_image).unwrap_or_default();

    // Radar polygon
    let radar_values: Vec<f64> = data
        .radar
        .iter()
        .map(|(_, v)| *v)
        .chain(std::iter::repeat(50.0))
        .take(6)
        .collect();

    let points: Vec<(f64, f64)> = radar_values
        .iter()
        .zip(RADAR_ANGLES.iter())
        .map(|(&v, &a)| polar(v, a))
        .collect();

    let poly_str: String = points
        .iter()
        .map(|(x, y)| format!("{:.0},{:.0}", x, y))
        .collect::<Vec<_>>()
        .join(" ");

    // Grid circles
    let mut grid = String::new();
    for gr in [75, 60, 45, 30] {
        grid.push_str(&format!(
            "      <circle cx=\"{cx}\" cy=\"{cy}\" r=\"{gr}\" fill=\"none\" stroke=\"#00000010\" stroke-width=\"0.5\"/>\n",
            cx = RADAR_CX as i32,
            cy = RADAR_CY as i32,
            gr = gr,
        ));
    }

    // Radar labels
    let label_pos = radar_label_positions();
    let mut lbls = String::new();
    for (i, (lx, ly, anchor)) in label_pos.iter().enumerate() {
        lbls.push_str(&format!(
            "    <text class=\"lb\" x=\"{lx:.0}\" y=\"{ly:.0}\" text-anchor=\"{anchor}\" \
             fill=\"#7A7A7A\" font-family=\"DM Sans, sans-serif\" font-size=\"10\" \
             font-weight=\"600\">{label}</text>\n",
            lx = lx,
            ly = ly,
            anchor = anchor,
            label = RADAR_LABELS[i],
        ));
    }

    // Radar dots
    let mut dots = String::new();
    for (px, py) in &points {
        dots.push_str(&format!(
            "      <circle class=\"dt\" cx=\"{px:.0}\" cy=\"{py:.0}\" r=\"3\"/>\n",
            px = px,
            py = py,
        ));
    }

    // Stats line
    let stats_line = format!(
        "Since {period} days -- {total} events",
        period = data.period_days,
        total = comma(data.total_events),
    );

    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="400" height="520" viewBox="0 0 400 520">
  <defs><style type="text/css">
    @import url('https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:wght@800&amp;family=DM+Sans:ital,wght@0,400;0,600;0,700;1,400&amp;display=swap');
    @keyframes fadeUp {{ from {{ opacity:0; transform:translateY(8px) }} to {{ opacity:1; transform:translateY(0) }} }}
    @keyframes popIn {{ from {{ opacity:0; transform:scale(0.8) }} to {{ opacity:1; transform:scale(1) }} }}
    @keyframes drawPoly {{ from {{ stroke-dashoffset:800 }} to {{ stroke-dashoffset:0 }} }}
    @keyframes fillFade {{ from {{ fill-opacity:0 }} to {{ fill-opacity:0.2 }} }}
    @keyframes dotPop {{ 0% {{ r:0; opacity:0 }} 60% {{ r:5 }} 100% {{ r:3; opacity:1 }} }}
    .tp {{ opacity:0; animation: fadeUp 0.3s ease forwards; animation-delay:0.05s }}
    .t {{ opacity:0; animation: fadeUp 0.5s ease forwards; animation-delay:0.1s }}
    .m {{ opacity:0; animation: fadeUp 0.4s ease forwards; animation-delay:0.2s }}
    .d {{ opacity:0; animation: popIn 0.5s cubic-bezier(0.34,1.56,0.64,1) forwards; animation-delay:0.3s; transform-origin:center }}
    .g {{ opacity:0; animation: fadeUp 0.3s ease forwards; animation-delay:0.5s }}
    .po {{ fill:none; stroke:{accent}; stroke-width:2; stroke-linejoin:round; stroke-dasharray:800; stroke-dashoffset:800; animation: drawPoly 1s cubic-bezier(0.22,1,0.36,1) forwards; animation-delay:0.7s }}
    .pf {{ fill:{accent}; fill-opacity:0; stroke:none; animation: fillFade 0.4s ease forwards; animation-delay:1.5s }}
    .dt {{ fill:{accent}; r:0; opacity:0; animation: dotPop 0.25s cubic-bezier(0.34,1.56,0.64,1) forwards }}
    .dt:nth-child(1){{animation-delay:0.9s}} .dt:nth-child(2){{animation-delay:1.0s}}
    .dt:nth-child(3){{animation-delay:1.1s}} .dt:nth-child(4){{animation-delay:1.2s}}
    .dt:nth-child(5){{animation-delay:1.3s}} .dt:nth-child(6){{animation-delay:1.4s}}
    .lb {{ opacity:0; animation: fadeUp 0.3s ease forwards; animation-delay:0.6s }}
    .q {{ opacity:0; animation: fadeUp 0.4s ease forwards; animation-delay:1.6s }}
    .st {{ opacity:0; animation: fadeUp 0.3s ease forwards; animation-delay:1.7s }}
    .b {{ opacity:0; animation: fadeUp 0.3s ease forwards; animation-delay:1.8s }}
  </style></defs>

  <rect width="400" height="520" rx="20" fill="{bg}"/>
  <text class="tp" x="200" y="18" text-anchor="middle" fill="#9A9A92" font-family="DM Sans, sans-serif" font-size="11" font-weight="600" letter-spacing="3">TRUE PERSONALITY</text>
  <text class="t" x="200" y="46" text-anchor="middle" fill="#1A1A1A" font-family="Bricolage Grotesque, sans-serif" font-size="30" font-weight="800">{name}</text>
  <text class="m" x="200" y="64" text-anchor="middle" fill="{accent}" font-family="DM Sans, sans-serif" font-size="13" font-weight="600" letter-spacing="4">{mbti}</text>
  <image class="d" x="135" y="70" width="130" height="130" preserveAspectRatio="xMidYMid meet" href="data:image/png;base64,{dog_b64}"/>

  <g transform="translate(0, 132)">
    <g class="g">
{grid}    </g>
{lbls}
    <polygon class="pf" points="{poly_str}"/>
    <polygon class="po" points="{poly_str}"/>
    <g>
{dots}    </g>
  </g>

  <text class="q" x="200" y="412" text-anchor="middle" fill="#1A1A1A" font-family="DM Sans, sans-serif" font-size="15" font-weight="700">{quip}</text>
  <text class="q" x="200" y="434" text-anchor="middle" fill="{accent}" font-family="DM Sans, sans-serif" font-size="12" font-style="italic">&quot;{catch}&quot;</text>
  <text class="st" x="200" y="458" text-anchor="middle" fill="#B0B0A8" font-family="DM Sans, sans-serif" font-size="9">{stats}</text>
  <text class="b" x="200" y="480" text-anchor="middle" fill="#9A9A92" font-family="DM Sans, sans-serif" font-size="12" font-weight="600">What kind of dog is your AI?</text>
  <text class="b" x="200" y="498" text-anchor="middle" fill="{accent}" font-family="DM Sans, sans-serif" font-size="12">Find out at punkgo.ai/roast</text>
</svg>"##,
        accent = accent,
        bg = bg,
        name = name,
        mbti = mbti,
        dog_b64 = dog_b64,
        grid = grid,
        lbls = lbls,
        poly_str = poly_str,
        dots = dots,
        quip = quip_safe,
        catch = catch_safe,
        stats = stats_line,
    )
}

// ---------------------------------------------------------------------------
// Vibe Card SVG (400x320) — compact card for --today
// ---------------------------------------------------------------------------

pub fn render_vibe_svg(data: &RoastData) -> String {
    let accent = roast_assets::accent_color(&data.personality.id);
    let bg = &data.personality.card_color;
    let name = roast_assets::short_name(&data.personality.name);
    let mbti = &data.personality.mbti;
    let quip_safe = xml_escape(&data.quip);
    let catch_safe = xml_escape(&data.personality.catchphrase);

    let dog_b64 = roast_assets::dog_image_base64(&data.personality.dog_image).unwrap_or_default();

    let footer = format!(
        "{total} events - punkgo.ai/roast",
        total = comma(data.total_events),
    );

    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="400" height="320" viewBox="0 0 400 320">
  <defs><style type="text/css">
    @import url('https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:wght@800&amp;family=DM+Sans:ital,wght@0,400;0,600;0,700;1,400&amp;display=swap');
    @keyframes fadeUp {{ from {{ opacity:0; transform:translateY(8px) }} to {{ opacity:1; transform:translateY(0) }} }}
    @keyframes popIn {{ from {{ opacity:0; transform:scale(0.8) }} to {{ opacity:1; transform:scale(1) }} }}
    .label {{ opacity:0; animation: fadeUp 0.4s ease forwards; animation-delay:0.1s }}
    .name  {{ opacity:0; animation: fadeUp 0.4s ease forwards; animation-delay:0.2s }}
    .mbti  {{ opacity:0; animation: fadeUp 0.3s ease forwards; animation-delay:0.3s }}
    .dog   {{ opacity:0; animation: popIn 0.5s cubic-bezier(0.34,1.56,0.64,1) forwards; animation-delay:0.4s; transform-origin:200px 165px }}
    .quote {{ opacity:0; animation: fadeUp 0.4s ease forwards; animation-delay:0.7s }}
    .catch {{ opacity:0; animation: fadeUp 0.3s ease forwards; animation-delay:0.9s }}
    .foot  {{ opacity:0; animation: fadeUp 0.3s ease forwards; animation-delay:1.0s }}
  </style></defs>

  <rect width="400" height="320" rx="20" fill="{bg}"/>

  <text class="label" x="200" y="28" text-anchor="middle" fill="#9A9A92"
        font-family="DM Sans, sans-serif" font-size="11" font-weight="600" letter-spacing="3">TODAY&#39;S VIBE</text>

  <text class="name" x="200" y="56" text-anchor="middle" fill="#1A1A1A"
        font-family="Bricolage Grotesque, sans-serif" font-size="28" font-weight="800">{name}</text>

  <text class="mbti" x="200" y="72" text-anchor="middle" fill="{accent}"
        font-family="DM Sans, sans-serif" font-size="12" font-weight="600" letter-spacing="4">{mbti}</text>

  <image class="dog" x="145" y="80" width="110" height="110" preserveAspectRatio="xMidYMid meet"
         href="data:image/png;base64,{dog_b64}"/>

  <text class="quote" x="200" y="216" text-anchor="middle" fill="#1A1A1A"
        font-family="DM Sans, sans-serif" font-size="14" font-weight="700">{quip}</text>

  <text class="catch" x="200" y="240" text-anchor="middle" fill="{accent}"
        font-family="DM Sans, sans-serif" font-size="11" font-style="italic">&quot;{catch}&quot;</text>

  <text class="foot" x="200" y="296" text-anchor="middle" fill="#B0B0A8"
        font-family="DM Sans, sans-serif" font-size="10">{footer}</text>
</svg>"##,
        bg = bg,
        accent = accent,
        name = name,
        mbti = mbti,
        dog_b64 = dog_b64,
        quip = quip_safe,
        catch = catch_safe,
        footer = footer,
    )
}

// ---------------------------------------------------------------------------
// Unified render_svg: dispatches based on is_today flag
// ---------------------------------------------------------------------------

/// Render SVG card. If `today` is true, renders a compact Vibe Card (400x320).
/// Otherwise renders a full Personality Card (400x520) with radar chart.
pub fn render_svg(data: &RoastData) -> String {
    render_personality_svg(data)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roast_analysis::*;

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
        assert!(svg.contains("PHILOSOPHER"));
        assert!(svg.contains("@keyframes"));
        assert!(svg.ends_with("</svg>"));
    }

    #[test]
    fn svg_contains_radar_chart() {
        let svg = render_svg(&sample_data());
        assert!(svg.contains("polygon"));
        assert!(svg.contains("Yapping"));
        assert!(svg.contains("Googling"));
        assert!(svg.contains("drawPoly"));
    }

    #[test]
    fn svg_contains_dog_image() {
        let svg = render_svg(&sample_data());
        assert!(svg.contains("data:image/png;base64,"));
    }

    #[test]
    fn svg_contains_personality_info() {
        let svg = render_svg(&sample_data());
        assert!(svg.contains("PHILOSOPHER"));
        assert!(svg.contains("INTP"));
        assert!(svg.contains("#5A8C6A")); // accent
        assert!(svg.contains("#E0EFDA")); // card_color
    }

    #[test]
    fn personality_svg_dimensions() {
        let svg = render_personality_svg(&sample_data());
        assert!(svg.contains("width=\"400\""));
        assert!(svg.contains("height=\"520\""));
    }

    #[test]
    fn vibe_svg_is_valid() {
        let svg = render_vibe_svg(&sample_data());
        assert!(svg.starts_with("<svg"));
        assert!(svg.ends_with("</svg>"));
        assert!(svg.contains("width=\"400\""));
        assert!(svg.contains("height=\"320\""));
        assert!(svg.contains("PHILOSOPHER"));
        assert!(svg.contains("INTP"));
        assert!(svg.contains("TODAY&#39;S VIBE"));
    }

    #[test]
    fn vibe_svg_contains_dog() {
        let svg = render_vibe_svg(&sample_data());
        assert!(svg.contains("data:image/png;base64,"));
    }

    #[test]
    fn vibe_svg_no_radar() {
        let svg = render_vibe_svg(&sample_data());
        assert!(!svg.contains("polygon"));
        assert!(!svg.contains("Yapping"));
    }

    #[test]
    fn xml_escape_works() {
        assert_eq!(xml_escape("a & b"), "a &amp; b");
        assert_eq!(xml_escape("a < b"), "a &lt; b");
        assert_eq!(xml_escape("it's"), "it&#39;s");
    }

    #[test]
    fn svg_all_16_personalities_render() {
        let ids = [
            (
                "philosopher",
                "THE PHILOSOPHER",
                "#E0EFDA",
                "dog-philosopher.png",
            ),
            ("architect", "THE ARCHITECT", "#D8E0CC", "dog-architect.png"),
            ("intern", "THE INTERN", "#FFE0EC", "dog-intern.png"),
            ("commander", "THE COMMANDER", "#E8D0D8", "dog-commander.png"),
            ("rereader", "THE REREADER", "#FFE8D0", "dog-rereader.png"),
            ("caretaker", "THE CARETAKER", "#F5E6D8", "dog-caretaker.png"),
            (
                "perfectionist",
                "THE PERFECTIONIST",
                "#E8D8F0",
                "dog-perfectionist.png",
            ),
            ("mentor", "THE MENTOR", "#D8D0E8", "dog-mentor.png"),
            ("vampire", "THE VAMPIRE", "#D0D4DC", "dog-vampire.png"),
            ("drifter", "THE DRIFTER", "#F0E8F8", "dog-drifter.png"),
            ("goldfish", "THE GOLDFISH", "#D8F0F4", "dog-goldfish.png"),
            ("helper", "THE HELPER", "#DCF0DC", "dog-helper.png"),
            ("brute", "THE BRUTE", "#F4D0C8", "dog-brute.png"),
            ("ghost", "THE GHOST", "#E8E8E8", "dog-ghost.png"),
            (
                "speedrunner",
                "THE SPEEDRUNNER",
                "#FFF0C8",
                "dog-speedrunner.png",
            ),
            ("googler", "THE GOOGLER", "#D0E0F4", "dog-googler.png"),
        ];
        for (id, name, color, dog) in &ids {
            let mut data = sample_data();
            data.personality.id = id.to_string();
            data.personality.name = name.to_string();
            data.personality.card_color = color.to_string();
            data.personality.dog_image = dog.to_string();

            let svg = render_personality_svg(&data);
            assert!(svg.starts_with("<svg"), "personality {id} SVG broken");
            assert!(svg.ends_with("</svg>"), "personality {id} SVG broken");
            assert!(
                svg.contains("data:image/png;base64,"),
                "personality {id} missing dog image"
            );

            let vibe = render_vibe_svg(&data);
            assert!(vibe.starts_with("<svg"), "vibe {id} SVG broken");
            assert!(vibe.ends_with("</svg>"), "vibe {id} SVG broken");
        }
    }

    #[test]
    fn radar_polar_math() {
        // At angle -90 (top), full value should be (200, 95)
        let (x, y) = polar(100.0, -90.0);
        assert!((x - 200.0).abs() < 1.0);
        assert!((y - 95.0).abs() < 1.0);

        // At angle 90 (bottom), full value should be (200, 245)
        let (x, y) = polar(100.0, 90.0);
        assert!((x - 200.0).abs() < 1.0);
        assert!((y - 245.0).abs() < 1.0);
    }
}
