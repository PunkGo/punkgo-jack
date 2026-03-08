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
    "punkgo-kernel".to_string()
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
