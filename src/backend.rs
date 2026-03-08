use anyhow::{bail, Result};
use punkgo_core::protocol::{RequestEnvelope, RequestType, ResponseEnvelope};
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::ipc_client::{new_request_id, IpcClient};

pub trait KernelBackend: Send + Sync {
    fn ping(&self) -> Result<Value>;
    fn query(&self, actor_id: Option<String>, limit: Option<u64>) -> Result<Value>;
    fn verify(&self, log_index: u64, tree_size: Option<u64>) -> Result<Value>;
    fn verify_consistency(&self, old_size: u64, tree_size: u64) -> Result<Value>;
    fn checkpoint(&self) -> Result<Value>;
    fn log_observe(&self, actor_id: String, target: String, payload: Value) -> Result<Value>;
    fn stats(&self) -> Result<Value>;
}

/// Create a `DaemonBackend` connected to `punkgo-kerneld` via IPC.
pub fn backend_from_env() -> Result<Box<dyn KernelBackend>> {
    Ok(Box::new(DaemonBackend::from_env()?))
}

fn unwrap_kernel_response(resp: ResponseEnvelope) -> Result<Value> {
    match resp.status.as_str() {
        "ok" => Ok(resp.payload),
        "error" => {
            let msg = resp
                .payload
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("kernel returned error");
            bail!("kernel rejected request: {msg}. Is punkgo-kerneld running?");
        }
        other => {
            warn!(status = other, request_id = %resp.request_id, "unexpected kernel response status");
            Ok(json!({
                "warning": format!("unexpected kernel response status: {other}"),
                "raw": {
                    "request_id": resp.request_id,
                    "status": resp.status,
                    "payload": resp.payload
                }
            }))
        }
    }
}

// ---------------------------------------------------------------------------
// DaemonBackend — connects to punkgo-kerneld via IPC
// ---------------------------------------------------------------------------

pub struct DaemonBackend {
    client: IpcClient,
}

impl DaemonBackend {
    /// Create a backend connected to `punkgo-kerneld`.
    /// Endpoint resolution: --endpoint arg > PUNKGO_DAEMON_ENDPOINT env > platform default.
    pub fn from_env() -> Result<Self> {
        let client = IpcClient::from_env(None);
        debug!(endpoint = %client.endpoint(), "DaemonBackend created");
        Ok(Self { client })
    }

    fn read_query(&self, payload: Value) -> Result<Value> {
        let req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Read,
            payload,
        };
        let resp = self.client.send(&req)?;
        unwrap_kernel_response(resp)
    }

    fn submit_action(&self, payload: Value) -> Result<Value> {
        let req = RequestEnvelope {
            request_id: new_request_id(),
            request_type: RequestType::Submit,
            payload,
        };
        let resp = self.client.send(&req)?;
        unwrap_kernel_response(resp)
    }
}

impl KernelBackend for DaemonBackend {
    fn ping(&self) -> Result<Value> {
        self.read_query(json!({ "kind": "health" }))
    }

    fn query(&self, actor_id: Option<String>, limit: Option<u64>) -> Result<Value> {
        let mut payload = json!({ "kind": "events" });
        if let Some(actor_id) = actor_id {
            payload["actor_id"] = Value::String(actor_id);
        }
        if let Some(limit) = limit {
            payload["limit"] = json!(limit.min(100));
        }
        self.read_query(payload)
    }

    fn verify(&self, log_index: u64, tree_size: Option<u64>) -> Result<Value> {
        let mut payload = json!({
            "kind": "audit_inclusion_proof",
            "log_index": log_index
        });
        if let Some(tree_size) = tree_size {
            payload["tree_size"] = json!(tree_size);
        }
        self.read_query(payload)
    }

    fn verify_consistency(&self, old_size: u64, tree_size: u64) -> Result<Value> {
        self.read_query(json!({
            "kind": "audit_consistency_proof",
            "old_size": old_size,
            "tree_size": tree_size
        }))
    }

    fn checkpoint(&self) -> Result<Value> {
        self.read_query(json!({ "kind": "audit_checkpoint" }))
    }

    fn log_observe(&self, actor_id: String, target: String, payload: Value) -> Result<Value> {
        self.submit_action(json!({
            "actor_id": actor_id,
            "action_type": "observe",
            "target": target,
            "payload": payload
        }))
    }

    fn stats(&self) -> Result<Value> {
        self.read_query(json!({ "kind": "stats" }))
    }
}

// ---------------------------------------------------------------------------
// EmbeddedBackend — only used in tests (requires punkgo-runtime)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod embedded {
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};

    use anyhow::{anyhow, Context, Result};
    use punkgo_core::protocol::{RequestEnvelope, RequestType};
    use punkgo_kernel::{Kernel, KernelConfig};
    use serde_json::{json, Value};

    use super::{unwrap_kernel_response, KernelBackend};

    pub struct EmbeddedBackend {
        runtime: Option<tokio::runtime::Runtime>,
        kernel: Kernel,
        next_request_id: AtomicU64,
    }

    impl EmbeddedBackend {
        pub fn bootstrap(config: KernelConfig) -> Result<Self> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .context("failed to build tokio runtime for embedded kernel backend")?;

            let kernel = if tokio::runtime::Handle::try_current().is_ok() {
                tokio::task::block_in_place(|| runtime.block_on(Kernel::bootstrap(&config)))
            } else {
                runtime.block_on(Kernel::bootstrap(&config))
            }
            .map_err(|e| anyhow!("failed to bootstrap embedded PunkGo kernel: {e}"))?;

            Ok(Self {
                runtime: Some(runtime),
                kernel,
                next_request_id: AtomicU64::new(1),
            })
        }

        fn runtime(&self) -> &tokio::runtime::Runtime {
            self.runtime
                .as_ref()
                .expect("embedded backend runtime should be present")
        }

        fn next_request_id(&self) -> String {
            let n = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            format!("mcp-embedded-{n}")
        }

        fn read_query(&self, payload: Value) -> Result<Value> {
            let req = RequestEnvelope {
                request_id: self.next_request_id(),
                request_type: RequestType::Read,
                payload,
            };
            self.dispatch_request(req)
        }

        fn submit_action(&self, payload: Value) -> Result<Value> {
            let req = RequestEnvelope {
                request_id: self.next_request_id(),
                request_type: RequestType::Submit,
                payload,
            };
            self.dispatch_request(req)
        }

        fn dispatch_request(&self, req: RequestEnvelope) -> Result<Value> {
            let resp = self.runtime_block_on(self.kernel.handle_request(req));
            unwrap_kernel_response(resp)
        }

        fn runtime_block_on<F>(&self, fut: F) -> F::Output
        where
            F: Future,
        {
            if tokio::runtime::Handle::try_current().is_ok() {
                tokio::task::block_in_place(|| self.runtime().block_on(fut))
            } else {
                self.runtime().block_on(fut)
            }
        }
    }

    impl Drop for EmbeddedBackend {
        fn drop(&mut self) {
            let Some(runtime) = self.runtime.take() else {
                return;
            };

            if tokio::runtime::Handle::try_current().is_ok() {
                tokio::task::block_in_place(|| drop(runtime));
            } else {
                drop(runtime);
            }
        }
    }

    impl KernelBackend for EmbeddedBackend {
        fn ping(&self) -> Result<Value> {
            self.read_query(json!({ "kind": "health" }))
        }

        fn query(&self, actor_id: Option<String>, limit: Option<u64>) -> Result<Value> {
            let mut payload = json!({ "kind": "events" });
            if let Some(actor_id) = actor_id {
                payload["actor_id"] = Value::String(actor_id);
            }
            if let Some(limit) = limit {
                payload["limit"] = json!(limit.min(100));
            }
            self.read_query(payload)
        }

        fn verify(&self, log_index: u64, tree_size: Option<u64>) -> Result<Value> {
            let mut payload = json!({
                "kind": "audit_inclusion_proof",
                "log_index": log_index
            });
            if let Some(tree_size) = tree_size {
                payload["tree_size"] = json!(tree_size);
            }
            self.read_query(payload)
        }

        fn verify_consistency(&self, old_size: u64, tree_size: u64) -> Result<Value> {
            self.read_query(json!({
                "kind": "audit_consistency_proof",
                "old_size": old_size,
                "tree_size": tree_size
            }))
        }

        fn checkpoint(&self) -> Result<Value> {
            self.read_query(json!({ "kind": "audit_checkpoint" }))
        }

        fn log_observe(&self, actor_id: String, target: String, payload: Value) -> Result<Value> {
            self.submit_action(json!({
                "actor_id": actor_id,
                "action_type": "observe",
                "target": target,
                "payload": payload
            }))
        }

        fn stats(&self) -> Result<Value> {
            self.read_query(json!({ "kind": "stats" }))
        }
    }

    pub struct MockBackend;

    impl KernelBackend for MockBackend {
        fn ping(&self) -> Result<Value> {
            Ok(json!({
                "status": "ok",
                "note": "mock backend"
            }))
        }

        fn query(&self, actor_id: Option<String>, _limit: Option<u64>) -> Result<Value> {
            Ok(json!({
                "events": [
                    {
                        "id": "evt_mock_0001",
                        "log_index": 0,
                        "event_hash": "mockhash0001",
                        "actor_id": actor_id.unwrap_or_else(|| "root".to_string()),
                        "action_type": "observe",
                        "target": "mcp/punkgo/log",
                        "payload": { "message": "mock event from punkgo-jack" },
                        "payload_hash": "mockpayloadhash0001",
                        "artifact_hash": null,
                        "reserved_energy": 0,
                        "settled_energy": 0,
                        "timestamp": "1700000000000"
                    }
                ]
            }))
        }

        fn verify(&self, log_index: u64, tree_size: Option<u64>) -> Result<Value> {
            Ok(json!({
                "log_index": log_index,
                "tree_size": tree_size.unwrap_or(1),
                "proof": ["00".repeat(32)]
            }))
        }

        fn verify_consistency(&self, old_size: u64, tree_size: u64) -> Result<Value> {
            Ok(json!({
                "old_size": old_size,
                "new_size": tree_size,
                "proof": ["11".repeat(32)]
            }))
        }

        fn checkpoint(&self) -> Result<Value> {
            Ok(json!({
                "origin": "punkgo/kernel",
                "tree_size": 1,
                "root_hash": "aa".repeat(32),
                "signature": "mock-signature"
            }))
        }

        fn log_observe(&self, actor_id: String, target: String, payload: Value) -> Result<Value> {
            Ok(json!({
                "event_id": "evt_mock_log_0001",
                "log_index": 1,
                "event_hash": "mockloghash0001",
                "reserved_cost": 0,
                "settled_cost": 0,
                "artifact_hash": null,
                "actor_id": actor_id,
                "action_type": "observe",
                "target": target,
                "payload": payload
            }))
        }

        fn stats(&self) -> Result<Value> {
            Ok(json!({ "event_count": 1 }))
        }
    }
}

#[cfg(test)]
pub use embedded::MockBackend;

#[cfg(test)]
mod tests {
    use super::*;
    use embedded::EmbeddedBackend;
    use punkgo_kernel::KernelConfig;
    use tempfile::TempDir;

    #[test]
    fn embedded_backend_smoke_flow() {
        let temp = TempDir::new().expect("temp dir");
        let state_dir = temp.path().join("state");
        let backend = EmbeddedBackend::bootstrap(KernelConfig {
            state_dir,
            ipc_endpoint: "embedded://test".to_string(),
        })
        .expect("embedded backend bootstraps");

        let ping = backend.ping().expect("ping");
        assert_eq!(ping.get("status").and_then(Value::as_str), Some("ok"));

        let receipt = backend
            .log_observe(
                "root".to_string(),
                "mcp/punkgo/log".to_string(),
                json!({
                    "schema": "punkgo-jack-log-v1",
                    "event_type": "test",
                    "content": "embedded smoke test"
                }),
            )
            .expect("submit observe");
        let log_index = receipt
            .get("log_index")
            .and_then(Value::as_i64)
            .expect("log_index") as u64;

        let queried = backend
            .query(Some("root".to_string()), Some(10))
            .expect("query");
        let events = queried
            .get("events")
            .and_then(Value::as_array)
            .expect("events array");
        assert!(!events.is_empty());

        let proof = backend.verify(log_index, None).expect("inclusion proof");
        assert_eq!(
            proof.get("log_index").and_then(Value::as_i64),
            Some(log_index as i64)
        );

        let checkpoint = backend.checkpoint().expect("checkpoint");
        assert!(checkpoint.get("tree_size").is_some());

        let stats = backend.stats().expect("stats");
        let event_count = stats
            .get("event_count")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        assert!(event_count >= 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn embedded_backend_bootstrap_and_drop_inside_tokio_runtime() {
        let temp = TempDir::new().expect("temp dir");
        let state_dir = temp.path().join("state-async");

        let backend = EmbeddedBackend::bootstrap(KernelConfig {
            state_dir,
            ipc_endpoint: "embedded://tokio-test".to_string(),
        })
        .expect("embedded backend bootstraps inside tokio runtime");

        let ping = backend.ping().expect("ping inside tokio runtime");
        assert_eq!(ping.get("status").and_then(Value::as_str), Some("ok"));

        drop(backend);
    }
}
