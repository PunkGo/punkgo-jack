use std::io::{BufRead, BufReader, Write};

use anyhow::{bail, Context, Result};
use punkgo_core::protocol::{RequestEnvelope, ResponseEnvelope};
use tracing::{debug, trace};
use uuid::Uuid;

/// Synchronous IPC client for communicating with `punkgo-kerneld`.
///
/// Supports Unix domain sockets (Linux/macOS) and Windows named pipes.
/// Each `send()` call opens a new connection, sends a line-delimited JSON
/// request, reads one line back, and closes — matching the punkgo-cli protocol.
pub struct IpcClient {
    endpoint: String,
}

impl IpcClient {
    /// Create an IPC client with endpoint discovery.
    ///
    /// Priority:
    /// 1. `endpoint_override` parameter (e.g. from `--endpoint` CLI flag)
    /// 2. `PUNKGO_DAEMON_ENDPOINT` environment variable
    /// 3. Platform default (Unix: `state/punkgo.sock`, Windows: `\\.\pipe\punkgo-kernel`)
    pub fn from_env(endpoint_override: Option<&str>) -> Self {
        let endpoint = endpoint_override
            .map(String::from)
            .or_else(|| std::env::var("PUNKGO_DAEMON_ENDPOINT").ok())
            .unwrap_or_else(default_endpoint);
        debug!(endpoint = %endpoint, "IPC client created");
        Self { endpoint }
    }

    /// Return the resolved endpoint string.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Send a `RequestEnvelope` and receive a `ResponseEnvelope`.
    ///
    /// Protocol: line-delimited JSON — one JSON object per line, newline-terminated.
    pub fn send(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope> {
        let request_line =
            serde_json::to_string(request).context("failed to serialize IPC request")?;
        trace!(endpoint = %self.endpoint, request_id = %request.request_id, "IPC send");
        let resp_line = send_raw(&self.endpoint, &request_line).with_context(|| {
            format!(
                "failed to communicate with punkgo-kerneld at {}. \
                     Is the daemon running? Start it with: punkgo-kerneld start",
                self.endpoint
            )
        })?;
        let resp: ResponseEnvelope = serde_json::from_str(&resp_line)
            .with_context(|| format!("failed to parse daemon response: {resp_line}"))?;
        trace!(request_id = %resp.request_id, status = %resp.status, "IPC response");
        Ok(resp)
    }
}

/// Generate a unique request ID for IPC calls.
pub fn new_request_id() -> String {
    Uuid::new_v4().to_string()
}

fn default_endpoint() -> String {
    // 1. Try daemon.addr (written by kernel with per-PID endpoint)
    if let Some(addr) = read_daemon_addr() {
        return addr;
    }
    // 2. Fallback to old hardcoded address (backward compat with old kernel)
    if cfg!(windows) {
        r"\\.\pipe\punkgo-kernel".to_string()
    } else {
        "punkgo-kernel".to_string()
    }
}

/// Read the daemon address from `~/.punkgo/state/daemon.addr`.
///
/// The kernel daemon writes this file with a per-PID endpoint address.
/// Format: `addr=<endpoint>` (one per line, first match wins).
fn read_daemon_addr() -> Option<String> {
    let state_dir = std::env::var("PUNKGO_STATE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .unwrap_or_else(|_| ".".into());
            std::path::PathBuf::from(home).join(".punkgo").join("state")
        });
    let addr_path = state_dir.join("daemon.addr");
    let content = std::fs::read_to_string(&addr_path).ok()?;
    parse_addr(&content)
}

fn parse_addr(content: &str) -> Option<String> {
    for line in content.lines() {
        if let Some(addr) = line.strip_prefix("addr=") {
            let addr = addr.trim();
            if !addr.is_empty() {
                return Some(addr.to_string());
            }
        }
    }
    None
}

fn send_raw(endpoint: &str, request_line: &str) -> Result<String> {
    use interprocess::local_socket::{
        traits::Stream as _, GenericFilePath, GenericNamespaced, Name, Stream, ToFsName, ToNsName,
    };

    fn endpoint_to_name(endpoint: &str) -> std::io::Result<Name<'_>> {
        if endpoint.contains('/') || endpoint.contains('\\') {
            endpoint.to_fs_name::<GenericFilePath>()
        } else {
            endpoint.to_ns_name::<GenericNamespaced>()
        }
    }

    let name =
        endpoint_to_name(endpoint).with_context(|| format!("invalid endpoint name: {endpoint}"))?;
    let mut stream = Stream::connect(name).with_context(|| format!("connect to {endpoint}"))?;
    stream.write_all(request_line.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let mut resp = String::new();
    let n = reader.read_line(&mut resp)?;
    if n == 0 {
        bail!("empty response from daemon");
    }
    Ok(resp.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_addr_extracts_endpoint() {
        let content = "addr=//./pipe/punkgo-kernel-1234\n";
        assert_eq!(
            parse_addr(content),
            Some("//./pipe/punkgo-kernel-1234".to_string())
        );
    }

    #[test]
    fn parse_addr_unix_socket() {
        let content = "pid=42\naddr=/home/user/.punkgo/state/daemon-42.sock\n";
        assert_eq!(
            parse_addr(content),
            Some("/home/user/.punkgo/state/daemon-42.sock".to_string())
        );
    }

    #[test]
    fn parse_addr_empty_value() {
        assert_eq!(parse_addr("addr=\n"), None);
        assert_eq!(parse_addr("addr=  \n"), None);
    }

    #[test]
    fn parse_addr_no_addr_line() {
        assert_eq!(parse_addr("pid=42\nversion=1\n"), None);
    }

    #[test]
    fn parse_addr_empty_file() {
        assert_eq!(parse_addr(""), None);
    }

    #[test]
    fn parse_addr_trims_whitespace() {
        let content = "addr=  /tmp/test.sock  \n";
        assert_eq!(parse_addr(content), Some("/tmp/test.sock".to_string()));
    }
}
