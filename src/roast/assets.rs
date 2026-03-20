//! Embedded dog images and per-personality visual metadata for roast cards.

use base64::Engine;

/// Return the base64-encoded PNG for a given dog image filename.
pub fn dog_image_base64(dog_filename: &str) -> Option<String> {
    let bytes: &[u8] = match dog_filename {
        "dog-philosopher.png" => include_bytes!("../../assets/dogs/dog-philosopher.png").as_slice(),
        "dog-architect.png" => include_bytes!("../../assets/dogs/dog-architect.png").as_slice(),
        "dog-intern.png" => include_bytes!("../../assets/dogs/dog-intern.png").as_slice(),
        "dog-commander.png" => include_bytes!("../../assets/dogs/dog-commander.png").as_slice(),
        "dog-rereader.png" => include_bytes!("../../assets/dogs/dog-rereader.png").as_slice(),
        "dog-caretaker.png" => include_bytes!("../../assets/dogs/dog-caretaker.png").as_slice(),
        "dog-perfectionist.png" => {
            include_bytes!("../../assets/dogs/dog-perfectionist.png").as_slice()
        }
        "dog-mentor.png" => include_bytes!("../../assets/dogs/dog-mentor.png").as_slice(),
        "dog-vampire.png" => include_bytes!("../../assets/dogs/dog-vampire.png").as_slice(),
        "dog-drifter.png" => include_bytes!("../../assets/dogs/dog-drifter.png").as_slice(),
        "dog-goldfish.png" => include_bytes!("../../assets/dogs/dog-goldfish.png").as_slice(),
        "dog-helper.png" => include_bytes!("../../assets/dogs/dog-helper.png").as_slice(),
        "dog-brute.png" => include_bytes!("../../assets/dogs/dog-brute.png").as_slice(),
        "dog-ghost.png" => include_bytes!("../../assets/dogs/dog-ghost.png").as_slice(),
        "dog-speedrunner.png" => include_bytes!("../../assets/dogs/dog-speedrunner.png").as_slice(),
        "dog-googler.png" => include_bytes!("../../assets/dogs/dog-googler.png").as_slice(),
        _ => return None,
    };
    Some(base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// Accent color for each personality id (darker shade of card_color).
/// These are hand-picked to complement each personality's card_color.
pub fn accent_color(personality_id: &str) -> &'static str {
    match personality_id {
        "philosopher" => "#5A8C6A",
        "architect" => "#5A6B50",
        "intern" => "#C75050",
        "commander" => "#6B5060",
        "rereader" => "#C08040",
        "caretaker" => "#8B7060",
        "perfectionist" => "#8060A0",
        "mentor" => "#605080",
        "vampire" => "#505868",
        "drifter" => "#7060A0",
        "goldfish" => "#408090",
        "helper" => "#508050",
        "brute" => "#B04040",
        "ghost" => "#808080",
        "speedrunner" => "#B09030",
        "googler" => "#4070B0",
        _ => "#808080",
    }
}

/// Short display name (strips "THE " prefix from personality name).
pub fn short_name(name: &str) -> &str {
    name.strip_prefix("THE ").unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_16_dogs_loadable() {
        let dogs = [
            "dog-philosopher.png",
            "dog-architect.png",
            "dog-intern.png",
            "dog-commander.png",
            "dog-rereader.png",
            "dog-caretaker.png",
            "dog-perfectionist.png",
            "dog-mentor.png",
            "dog-vampire.png",
            "dog-drifter.png",
            "dog-goldfish.png",
            "dog-helper.png",
            "dog-brute.png",
            "dog-ghost.png",
            "dog-speedrunner.png",
            "dog-googler.png",
        ];
        for dog in &dogs {
            let b64 = dog_image_base64(dog);
            assert!(b64.is_some(), "failed to load {}", dog);
            assert!(!b64.unwrap().is_empty(), "empty b64 for {}", dog);
        }
    }

    #[test]
    fn unknown_dog_returns_none() {
        assert!(dog_image_base64("dog-unicorn.png").is_none());
    }

    #[test]
    fn accent_colors_are_hex() {
        let ids = [
            "philosopher",
            "architect",
            "intern",
            "commander",
            "rereader",
            "caretaker",
            "perfectionist",
            "mentor",
            "vampire",
            "drifter",
            "goldfish",
            "helper",
            "brute",
            "ghost",
            "speedrunner",
            "googler",
        ];
        for id in &ids {
            let c = accent_color(id);
            assert!(c.starts_with('#'), "accent for {} doesn't start with #", id);
            assert_eq!(c.len(), 7, "accent for {} wrong length", id);
        }
    }

    #[test]
    fn short_name_strips_prefix() {
        assert_eq!(short_name("THE PHILOSOPHER"), "PHILOSOPHER");
        assert_eq!(short_name("BRUTE"), "BRUTE");
    }
}
