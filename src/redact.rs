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
///
/// Order matters: whole-blob and capture-group patterns (PEM, connection
/// strings, `Bearer` headers) run BEFORE the generic key=value assignment
/// catch-all, so a token embedded in a header/URL is redacted by its precise
/// pattern rather than slipping past the assignment value gate.
static TOKEN_PATTERNS: Lazy<Vec<(Regex, &'static str)>> = Lazy::new(|| {
    let p = |re: &str| Regex::new(re).expect("valid redaction regex");
    vec![
        // PEM private-key blocks (any label; multi-line). Unambiguous — always
        // redact the whole block. `(?s)` makes `.` span newlines.
        (
            p(r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----"),
            "private-key",
        ),
        // Connection-string credentials: scheme://user:PASSWORD@host. Redacts
        // the password (capture group 1), keeps scheme/user/host legible.
        (
            p(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s:/@]+:([^\s@/]{3,})@"),
            "conn-cred",
        ),
        // Authorization headers / bearer tokens: `Bearer <token>` /
        // `Basic <token>`. Redacts the token (group 1) after the scheme word —
        // catches opaque/custom tokens that match no provider shape.
        (
            p(r"(?i)\b(?:bearer|basic)\s+([A-Za-z0-9._~+/=-]{8,})"),
            "bearer",
        ),
        // OpenAI keys: sk-..., sk-proj-..., etc.
        (p(r"sk-[A-Za-z0-9_-]{16,}"), "openai-key"),
        // Anthropic keys.
        (p(r"sk-ant-[A-Za-z0-9_-]{16,}"), "anthropic-key"),
        // GitHub tokens: ghp_, gho_, ghu_, ghs_, ghr_, and fine-grained PATs.
        (p(r"gh[pousr]_[A-Za-z0-9]{20,}"), "github-token"),
        (p(r"github_pat_[0-9A-Za-z_]{20,}"), "github-token"),
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
            match *kind {
                "assignment" => {
                    // Preserve the key, redact the value (group 2). A
                    // sensitive-looking key name alone (`pubkey: 0x1`,
                    // `token: ERC20`, `key: value`) is NOT enough for the
                    // generic markers, or ordinary code/config/blockchain
                    // content gets shredded. But `password`/`passwd`/
                    // `credential` keys are high-precision: redact any value
                    // >= 8 chars (real passwords are often letters-only, so
                    // the digit+alpha entropy gate would miss them).
                    out = re
                        .replace_all(&out, |caps: &regex::Captures| {
                            if secret_assignment_value_should_redact(&caps[1], &caps[2]) {
                                hits += 1;
                                format!("{}=[REDACTED:secret]", &caps[1])
                            } else {
                                caps[0].to_string()
                            }
                        })
                        .into_owned();
                }
                // Capture-group patterns: redact only group 1 (the secret),
                // keeping the surrounding shape (scheme://user:…@, `Bearer …`).
                "conn-cred" | "bearer" => {
                    let marker = format!("[REDACTED:{kind}]");
                    out = re
                        .replace_all(&out, |caps: &regex::Captures| {
                            hits += 1;
                            caps[0].replacen(&caps[1], &marker, 1)
                        })
                        .into_owned();
                }
                // Whole-match shapes (provider keys, PEM blocks, JWT).
                _ => {
                    let replacement = format!("[REDACTED:{kind}]");
                    let count = re.find_iter(&out).count();
                    if count > 0 {
                        hits += count;
                        out = re.replace_all(&out, replacement.as_str()).into_owned();
                    }
                }
            }
        }

        (out, hits)
    }
}

/// Decide whether a `KEY[:=]value` value should be redacted, given the key
/// name and the value. Secret-zero (iron law) is the priority; the only reason
/// not to redact every value on a sensitive-named key is that some key names
/// (`pubkey`, a React `key`, `authToken: refresh`) are frequently innocent, and
/// blindly shredding them would gut the recorded content.
///
/// - **Unambiguous secret keys** (`password`/`passwd`/`credential`/`secret`/
///   `apikey`): redact any value >= 8 chars. These names are never innocent, so
///   even a letters-only value is a secret (cross-model review finding: an
///   entropy gate here misses `password: SuperSecretPassphrase`).
/// - **Ambiguous keys** (`key`/`token`/`auth`): redact any value >= 16 chars,
///   letters-only included (catches opaque API tokens like
///   `AUTH_TOKEN=abcdefghijklmnopqrstuvwxyz`) while sparing short identifiers
///   (`key: value`, `token: ERC20`, `pubkey: 0xAbC`).
fn secret_assignment_value_should_redact(key: &str, value: &str) -> bool {
    // Never re-redact a marker a prior pattern already inserted (e.g. a PEM
    // block redacted to `[REDACTED:private-key]` sitting after a `key:`).
    if value.starts_with("[REDACTED:") {
        return false;
    }
    let kl = key.to_ascii_lowercase();
    let unambiguous = kl.contains("password")
        || kl.contains("passwd")
        || kl.contains("credential")
        || kl.contains("secret")
        || kl.contains("apikey");
    if unambiguous {
        value.len() >= 8
    } else {
        value.len() >= 16
    }
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
    fn redacts_pem_private_key_block() {
        let r = Redactor::with_env_values(vec![]);
        let pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA1234\nabcd/EFGH+ijkl\n-----END RSA PRIVATE KEY-----";
        let (out, hits) = r.redact(&format!("here is the key:\n{pem}\ndone"));
        assert_eq!(hits, 1);
        assert!(out.contains("[REDACTED:private-key]"), "got: {out}");
        assert!(!out.contains("MIIEowIBAA"), "PEM body leaked: {out}");
    }

    #[test]
    fn redacts_connection_string_password() {
        let r = Redactor::with_env_values(vec![]);
        let (out, hits) = r.redact("DATABASE_URL=postgres://appuser:Tr0ub4dor3@db.internal:5432/prod");
        assert!(hits >= 1);
        assert!(!out.contains("Tr0ub4dor3"), "db password leaked: {out}");
        assert!(out.contains("postgres://appuser:"), "shape not preserved: {out}");
        assert!(out.contains("@db.internal"), "host stripped: {out}");
    }

    #[test]
    fn redacts_bearer_and_basic_tokens() {
        let r = Redactor::with_env_values(vec![]);
        // Opaque bearer token matching no provider shape.
        let (out, _) = r.redact("curl -H \"Authorization: Bearer 4f9e8d7c6b5a49392817065f4e3d2c1b\"");
        assert!(!out.contains("4f9e8d7c6b5a49392817065f4e3d2c1b"), "bearer token leaked: {out}");
        assert!(out.contains("Bearer [REDACTED:bearer]"), "got: {out}");

        let (out2, _) = r.redact("Authorization: Basic dXNlcjpwYXNzd29yZA==");
        assert!(!out2.contains("dXNlcjpwYXNzd29yZA=="), "basic cred leaked: {out2}");
    }

    #[test]
    fn redacts_letters_only_password() {
        let r = Redactor::with_env_values(vec![]);
        // No digit — the generic entropy gate would miss it, but a
        // password-named key is high-precision.
        let (out, hits) = r.redact("password: SuperSecretPassphrase");
        assert_eq!(hits, 1);
        assert!(!out.contains("SuperSecretPassphrase"), "password leaked: {out}");
        assert!(out.contains("password=[REDACTED:secret]"), "got: {out}");
    }

    #[test]
    fn redacts_letters_only_token_on_sensitive_key() {
        // Cross-model review finding: a letters-only value on a key/token/auth
        // key (no digit) previously slipped the entropy gate.
        let r = Redactor::with_env_values(vec![]);
        for c in [
            "API_KEY=SuperSecretPassphraseLong",
            "AUTH_TOKEN=abcdefghijklmnopqrstuvwxyz",
        ] {
            let (out, hits) = r.redact(c);
            assert_eq!(hits, 1, "letters-only secret leaked: {c} -> {out}");
            assert!(out.contains("[REDACTED:secret]"), "got: {out}");
        }
        // github fine-grained PAT shape.
        let (out2, _) = r.redact("token github_pat_11ABCDEFG0aBcDeFgHiJkLmNoPqRsTuVwXyZ");
        assert!(out2.contains("[REDACTED:github-token]"), "got: {out2}");
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
