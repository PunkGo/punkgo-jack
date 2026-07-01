//! Secret redaction for captured content (AD6, secret-zero iron law).
//!
//! When a source captures full I/O (`codex = full`), tool arguments and tool
//! output can carry secrets — an `env | grep` in a shell command, an API key
//! echoed into a log, a token pasted into a prompt. Before any body is written
//! to the blob store, it passes through [`Redactor::redact`], which replaces
//! secret-looking spans with `[REDACTED:<kind>]`.
//!
//! Two complementary strategies:
//! 1. **Known token shapes** — high-precision regexes for provider key formats
//!    (OpenAI, Anthropic, GitHub, AWS, Slack, Google, JWTs) and
//!    `KEY=value` / `KEY: value` assignments whose key name looks sensitive.
//! 2. **Live environment values** — the literal values of this process's
//!    environment variables whose *name* matches a secret keyword. This catches
//!    a real secret leaked verbatim even if its shape is unknown.
//!
//! Redaction is deliberately conservative about generic high-entropy strings
//! (which cause false positives on hashes / base64 blobs); it fires on known
//! shapes, sensitive assignments, and known env values rather than "any long
//! random-looking string".

use once_cell::sync::Lazy;
use regex::Regex;

/// Minimum length for an environment variable's value to be treated as a
/// redactable secret (avoids scrubbing short values like `HOME=/x` or a
/// one-char flag that happens to sit in a secret-named var).
const MIN_ENV_SECRET_LEN: usize = 8;

/// Environment variable name fragments that mark a value as sensitive.
const SECRET_ENV_MARKERS: &[&str] = &[
    "KEY", "TOKEN", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL", "AUTH", "APIKEY",
];

/// (regex, kind) pairs for known secret shapes. Compiled once.
static TOKEN_PATTERNS: Lazy<Vec<(Regex, &'static str)>> = Lazy::new(|| {
    let p = |re: &str| Regex::new(re).expect("valid redaction regex");
    vec![
        // OpenAI keys: sk-..., sk-proj-..., etc.
        (p(r"sk-[A-Za-z0-9_-]{16,}"), "openai-key"),
        // Anthropic keys.
        (p(r"sk-ant-[A-Za-z0-9_-]{16,}"), "anthropic-key"),
        // GitHub tokens: ghp_, gho_, ghu_, ghs_, ghr_.
        (p(r"gh[pousr]_[A-Za-z0-9]{20,}"), "github-token"),
        // AWS access key id.
        (p(r"AKIA[0-9A-Z]{16}"), "aws-access-key"),
        // Google API key.
        (p(r"AIza[0-9A-Za-z_-]{35}"), "google-key"),
        // Slack tokens.
        (p(r"xox[baprs]-[A-Za-z0-9-]{10,}"), "slack-token"),
        // JWT (header.payload.signature, base64url).
        (
            p(r"eyJ[A-Za-z0-9_-]{6,}\.eyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}"),
            "jwt",
        ),
        // KEY=value / KEY: value where KEY looks sensitive. The value (quoted
        // or an unspaced run) is captured in group 2 and redacted; the key is
        // preserved so the shape stays legible.
        (
            p(r#"(?i)([A-Za-z0-9_]*(?:key|token|secret|password|passwd|credential|auth)[A-Za-z0-9_]*)\s*[:=]\s*["']?([^\s"']{6,})["']?"#),
            "assignment",
        ),
    ]
});

/// A configured redactor. Cheap to build; regexes are shared statics.
pub struct Redactor {
    /// Literal env-var values to scrub verbatim (longest first so a value that
    /// is a prefix of another does not leave a tail behind).
    env_values: Vec<String>,
}

impl Default for Redactor {
    fn default() -> Self {
        Self::from_env()
    }
}

impl Redactor {
    /// Build a redactor seeded from the current process environment.
    pub fn from_env() -> Self {
        let mut env_values: Vec<String> = std::env::vars()
            .filter(|(k, v)| is_secret_env_name(k) && v.len() >= MIN_ENV_SECRET_LEN)
            .map(|(_, v)| v)
            .collect();
        // Longest first: redact the most specific value before its substrings.
        env_values.sort_by_key(|v| std::cmp::Reverse(v.len()));
        env_values.dedup();
        Self { env_values }
    }

    /// Build a redactor with an explicit set of secret values (for tests).
    #[cfg(test)]
    pub fn with_env_values(mut env_values: Vec<String>) -> Self {
        env_values.retain(|v| v.len() >= MIN_ENV_SECRET_LEN);
        env_values.sort_by_key(|v| std::cmp::Reverse(v.len()));
        env_values.dedup();
        Self { env_values }
    }

    /// Redact secrets from `text`. Returns the scrubbed string and the number
    /// of spans redacted.
    pub fn redact(&self, text: &str) -> (String, usize) {
        let mut out = text.to_string();
        let mut hits = 0usize;

        // 1. Literal env-var values first (highest confidence).
        for value in &self.env_values {
            if out.contains(value.as_str()) {
                hits += out.matches(value.as_str()).count();
                out = out.replace(value.as_str(), "[REDACTED:env]");
            }
        }

        // 2. Known token shapes + sensitive assignments.
        for (re, kind) in TOKEN_PATTERNS.iter() {
            if *kind == "assignment" {
                // Preserve the key, redact the value (group 2) — but only when
                // the value actually looks like a secret. A sensitive-looking
                // key name alone (`pubkey: 0x1`, `token: ERC20`, `key: value`)
                // is NOT enough; otherwise ordinary content full of `key:` /
                // `token:` (code, configs, blockchain data) gets shredded.
                out = re
                    .replace_all(&out, |caps: &regex::Captures| {
                        if looks_like_secret_value(&caps[2]) {
                            hits += 1;
                            format!("{}=[REDACTED:secret]", &caps[1])
                        } else {
                            caps[0].to_string()
                        }
                    })
                    .into_owned();
            } else {
                let replacement = format!("[REDACTED:{kind}]");
                let count = re.find_iter(&out).count();
                if count > 0 {
                    hits += count;
                    out = re.replace_all(&out, replacement.as_str()).into_owned();
                }
            }
        }

        (out, hits)
    }
}

/// Heuristic: does a value assigned to a sensitive-named key actually look
/// like a secret (vs. an ordinary short word / identifier)? Requires a
/// reasonably long, mixed alphanumeric run — real API keys / tokens are long
/// and high-entropy. This deliberately errs toward redaction for long hex
/// (a raw private key is indistinguishable from a long hex id by shape, so
/// secret-zero wins), while sparing `key: value` / `token: ERC20` / short ids.
fn looks_like_secret_value(v: &str) -> bool {
    const MIN_SECRET_VALUE_LEN: usize = 16;
    if v.len() < MIN_SECRET_VALUE_LEN {
        return false;
    }
    let has_digit = v.chars().any(|c| c.is_ascii_digit());
    let has_alpha = v.chars().any(|c| c.is_ascii_alphabetic());
    has_digit && has_alpha
}

/// True if an env var *name* marks its value as a likely secret.
fn is_secret_env_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    SECRET_ENV_MARKERS.iter().any(|m| upper.contains(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_known_token_shapes() {
        let r = Redactor::with_env_values(vec![]);
        let (out, hits) = r.redact("run with sk-abcdefghijklmnopqrstuvwx and done");
        assert!(out.contains("[REDACTED:openai-key]"), "got: {out}");
        assert!(!out.contains("sk-abcdefghij"));
        assert_eq!(hits, 1);
    }

    #[test]
    fn redacts_github_and_aws_and_jwt() {
        let r = Redactor::with_env_values(vec![]);
        let (out, _) = r.redact("ghp_0123456789abcdef0123456789abcdef0123 AKIAIOSFODNN7EXAMPLE");
        assert!(out.contains("[REDACTED:github-token]"));
        assert!(out.contains("[REDACTED:aws-access-key]"));

        let jwt = "eyJhbGciOi.eyJzdWIiOi.SflKxwRJSM";
        let (out2, _) = r.redact(jwt);
        assert!(out2.contains("[REDACTED:jwt]"), "got: {out2}");
    }

    #[test]
    fn redacts_sensitive_assignments_preserving_key() {
        let r = Redactor::with_env_values(vec![]);
        let (out, _) = r.redact(r#"export API_KEY="supersecretvalue123" OTHER=fine"#);
        assert!(out.contains("API_KEY=[REDACTED:secret]"), "got: {out}");
        assert!(!out.contains("supersecretvalue123"));
        // Non-secret assignment left intact.
        assert!(out.contains("OTHER=fine"));
    }

    #[test]
    fn redacts_literal_env_value() {
        // Acceptance (AD6): a known secret in tool output is scrubbed in the
        // stored blob, even with an unknown shape.
        let secret = "gk_notaknownshape_9f8e7d6c5b4a";
        let r = Redactor::with_env_values(vec![secret.to_string()]);
        let (out, hits) = r.redact(&format!("the tool printed {secret} to stdout"));
        assert!(out.contains("[REDACTED:env]"));
        assert!(!out.contains(secret));
        assert_eq!(hits, 1);
    }

    #[test]
    fn spares_nonsecret_sensitive_keys() {
        let r = Redactor::with_env_values(vec![]);
        // Sensitive-looking key names with ordinary short values: NOT secrets.
        for c in ["pubkey: 0xAbC", "token: ERC20", "key: value", "authToken: refresh"] {
            let (out, hits) = r.redact(c);
            assert_eq!(hits, 0, "false-positive redaction on: {c} -> {out}");
            assert_eq!(out, c);
        }
        // A long high-entropy value assigned to a sensitive key IS redacted.
        let (out, hits) = r.redact("private_key = 0123456789abcdef0123456789abcdef");
        assert_eq!(hits, 1);
        assert!(out.contains("private_key=[REDACTED:secret]"), "got {out}");
    }

    #[test]
    fn leaves_ordinary_text_untouched() {
        let r = Redactor::with_env_values(vec![]);
        let input = "fn main() { println!(\"hello world\"); } // a normal comment";
        let (out, hits) = r.redact(input);
        assert_eq!(out, input);
        assert_eq!(hits, 0);
    }

    #[test]
    fn short_env_values_are_not_treated_as_secrets() {
        // A short value (< MIN_ENV_SECRET_LEN) must not be scrubbed — avoids
        // nuking every occurrence of e.g. a 3-char token in normal prose.
        let r = Redactor::with_env_values(vec!["abc".to_string()]);
        let (out, hits) = r.redact("abc appears in abcdef and abc again");
        assert_eq!(hits, 0);
        assert!(out.contains("abc"));
    }

    #[test]
    fn is_secret_env_name_matches_markers() {
        assert!(is_secret_env_name("OPENAI_API_KEY"));
        assert!(is_secret_env_name("aws_secret_access_key"));
        assert!(is_secret_env_name("MY_TOKEN"));
        assert!(!is_secret_env_name("HOME"));
        assert!(!is_secret_env_name("PATH"));
    }
}
