use anyhow::{anyhow, Context, Result};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router,
    transport::stdio,
    ErrorData as McpError, ServerHandler, ServiceExt,
};

use crate::backend::KernelBackend;
use crate::tools::{
    self, PunkgoLogParams, PunkgoQueryParams, PunkgoSessionReceiptParams, PunkgoStatsParams,
    PunkgoVerifyParams,
};

pub async fn run_stdio(backend: Box<dyn KernelBackend>) -> Result<()> {
    let service = PunkgoMcpServer::new(backend)
        .serve(stdio())
        .await
        .map_err(|e| anyhow!("failed to start rmcp stdio server: {e}"))?;

    let cancellation = service.cancellation_token();
    let ctrl_c_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancellation.cancel();
        }
    });

    let wait_result = service
        .waiting()
        .await
        .context("rmcp service task join failure");

    ctrl_c_task.abort();
    let _ = ctrl_c_task.await;

    wait_result?;
    Ok(())
}

struct PunkgoMcpServer {
    backend: Box<dyn KernelBackend>,
    tool_router: ToolRouter<Self>,
}

impl PunkgoMcpServer {
    fn new(backend: Box<dyn KernelBackend>) -> Self {
        Self {
            backend,
            tool_router: Self::tool_router(),
        }
    }

    fn map_tool_error(&self, tool: &str, err: anyhow::Error) -> McpError {
        McpError::internal_error(
            "tool dispatch failed",
            Some(serde_json::json!({ "tool": tool, "error": err.to_string() })),
        )
    }
}

#[tool_router(router = tool_router)]
impl PunkgoMcpServer {
    #[tool(
        name = "punkgo_ping",
        description = "Check punkgo-jack connectivity and backend health"
    )]
    async fn punkgo_ping(&self) -> Result<CallToolResult, McpError> {
        tools::punkgo_ping(self.backend.as_ref()).map_err(|e| self.map_tool_error("punkgo_ping", e))
    }

    #[tool(
        name = "punkgo_log",
        description = "Record a human-friendly audit note by submitting an observe action (facade over kernel submit_observe). Recommended event_type values: tool_call, user_note, decision, milestone, error, security_event, custom."
    )]
    async fn punkgo_log(
        &self,
        Parameters(params): Parameters<PunkgoLogParams>,
    ) -> Result<CallToolResult, McpError> {
        tools::punkgo_log(self.backend.as_ref(), params)
            .map_err(|e| self.map_tool_error("punkgo_log", e))
    }

    #[tool(
        name = "punkgo_query",
        description = "Query recent PunkGo kernel events with optional local filtering (facade over read_events)"
    )]
    async fn punkgo_query(
        &self,
        Parameters(params): Parameters<PunkgoQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        tools::punkgo_query(self.backend.as_ref(), params)
            .map_err(|e| self.map_tool_error("punkgo_query", e))
    }

    #[tool(
        name = "punkgo_verify",
        description = "Get a Merkle inclusion or consistency proof (facade over kernel audit proof reads)"
    )]
    async fn punkgo_verify(
        &self,
        Parameters(params): Parameters<PunkgoVerifyParams>,
    ) -> Result<CallToolResult, McpError> {
        tools::punkgo_verify(self.backend.as_ref(), params)
            .map_err(|e| self.map_tool_error("punkgo_verify", e))
    }

    #[tool(
        name = "punkgo_stats",
        description = "Read kernel event_count and derive sample distributions/timeline from recent events"
    )]
    async fn punkgo_stats(
        &self,
        Parameters(params): Parameters<PunkgoStatsParams>,
    ) -> Result<CallToolResult, McpError> {
        tools::punkgo_stats(self.backend.as_ref(), params)
            .map_err(|e| self.map_tool_error("punkgo_stats", e))
    }

    #[tool(
        name = "punkgo_checkpoint",
        description = "Return the latest C2SP-format checkpoint from the kernel audit log"
    )]
    async fn punkgo_checkpoint(&self) -> Result<CallToolResult, McpError> {
        tools::punkgo_checkpoint(self.backend.as_ref())
            .map_err(|e| self.map_tool_error("punkgo_checkpoint", e))
    }

    #[tool(
        name = "punkgo_session_receipt",
        description = "Get a session receipt with event count, energy consumed, and Merkle root for the current or specified session"
    )]
    async fn punkgo_session_receipt(
        &self,
        Parameters(params): Parameters<PunkgoSessionReceiptParams>,
    ) -> Result<CallToolResult, McpError> {
        tools::punkgo_session_receipt(self.backend.as_ref(), params)
            .map_err(|e| self.map_tool_error("punkgo_session_receipt", e))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for PunkgoMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "PunkGo MCP adapter for punkgo-kernel. Use punkgo_log/query/verify/stats/checkpoint to work with the local audit trail.".to_string(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MockBackend;

    #[test]
    fn tool_router_lists_punkgo_tools() {
        let server = PunkgoMcpServer::new(Box::new(MockBackend));
        let tools = server.tool_router.list_all();
        assert!(tools.iter().any(|t| t.name == "punkgo_log"));
        assert!(tools.iter().any(|t| t.name == "punkgo_verify"));
        assert!(tools.iter().any(|t| t.name == "punkgo_session_receipt"));
        assert_eq!(tools.len(), 7);
    }
}
