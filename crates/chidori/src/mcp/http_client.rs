//! Remote MCP client over **Streamable HTTP** (the MCP spec's HTTP transport,
//! protocol 2025-03-26). Mirrors the stdio [`super::client::McpClient`] surface
//! — `tools()` + `call_tool()` — but speaks JSON-RPC 2.0 over HTTP POST instead
//! of a child's stdin/stdout. Only the subset the framework uses is
//! implemented: `initialize` / `notifications/initialized` / `tools/list` /
//! `tools/call`.
//!
//! Two cross-repo contracts make this safe (mcp-http-transport-chidori.md §3):
//!
//! - **The bearer is a placeholder, not a token.** `auth_token` is a
//!   `__CHIDORI_SECRET__<id>__` value; this client substitutes the real token
//!   through the host secret broker ([`crate::runtime::secret_env`]), locked to
//!   the server's own host, so a token is never sent to any other host and the
//!   guest never sees it. (Highest-risk integration point per the design doc —
//!   substitution happens in [`McpHttpClient::connect`].)
//! - **Scope challenges surface as structured errors.** A `401` / `403` /
//!   `WWW-Authenticate` becomes a `{ mcpError: { kind, server, … } }` JSON error
//!   (not an opaque string) so agent-builder's broker can trigger step-up
//!   re-auth or report the missing scope during generation.

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

use super::client::RemoteTool;
use super::config::McpServerConfig;

/// Pinned protocol version for the HTTP transport. Newer than the stdio
/// client's `2024-11-05`; 2025-03-26 is the revision that introduced Streamable
/// HTTP. (Open question 4 in the design doc — negotiation can come later.)
const PROTOCOL_VERSION: &str = "2025-03-26";

pub struct McpHttpClient {
    client: reqwest::Client,
    url: String,
    timeout: std::time::Duration,
    /// The server id, used only to label structured errors.
    server_id: String,
    /// Realized `Authorization` header value (`Bearer <real-token>`), already
    /// run through the secret broker. `None` when the server needs no auth.
    auth_header: Option<String>,
    /// Streamable-HTTP session id returned by `initialize`, echoed on every
    /// later request via `Mcp-Session-Id`.
    session_id: tokio::sync::Mutex<Option<String>>,
    /// Only one caller may replace an expired session. Other callers reuse
    /// its new id after the handshake finishes.
    reconnect: tokio::sync::Mutex<()>,
    tools: Vec<RemoteTool>,
}

enum RpcReply {
    Response(Value),
    SessionNotFound,
}

fn initialize_params() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": { "name": "chidori", "version": env!("CARGO_PKG_VERSION") }
    })
}

impl McpHttpClient {
    /// Connect to a remote HTTP MCP server: substitute the bearer placeholder
    /// host-side, run the JSON-RPC handshake, and discover its tools.
    pub async fn connect(server_id: &str, cfg: McpServerConfig) -> Result<Self> {
        let url = cfg
            .url
            .clone()
            .filter(|u| !u.is_empty())
            .ok_or_else(|| anyhow!("http MCP server `{server_id}` has no url"))?;

        // Resolve the bearer placeholder → real token through the host secret
        // broker, audience-locked to the server's own host. A placeholder bound
        // to a different host fails closed here (never sent), satisfying the
        // spec's "MUST NOT send tokens to any server other than the one that
        // issued them."
        let auth_header = match cfg.auth_token.as_deref().filter(|t| !t.is_empty()) {
            Some(token) => {
                let host = url::Url::parse(&url)
                    .ok()
                    .and_then(|u| u.host_str().map(str::to_owned))
                    .ok_or_else(|| {
                        anyhow!("http MCP server `{server_id}` has an unparseable url")
                    })?;
                let store = crate::runtime::secret_env::SecretStore::global();
                let header = store
                    .substitute_str(&format!("Bearer {token}"), &host)
                    .map_err(|err| anyhow!("MCP `{server_id}` auth: {err}"))?;
                Some(header)
            }
            None => None,
        };

        // Bound the complete response, including a streaming body that stalls
        // after headers. The send-only timeout below cannot cover resp.text().
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(cfg.timeout_ms))
            .build()
            .context("building MCP http client")?;

        let mut this = Self {
            client,
            url,
            timeout: std::time::Duration::from_millis(cfg.timeout_ms),
            server_id: server_id.to_string(),
            auth_header,
            session_id: tokio::sync::Mutex::new(None),
            reconnect: tokio::sync::Mutex::new(()),
            tools: Vec::new(),
        };

        // Handshake.
        this.post_rpc("initialize", initialize_params()).await?;
        this.post_notification("notifications/initialized", Value::Null)
            .await?;

        // Discover tools.
        let list = this.post_rpc("tools/list", json!({})).await?;
        this.tools = list
            .get("result")
            .and_then(|r| r.get("tools"))
            .and_then(|t| serde_json::from_value(t.clone()).ok())
            .unwrap_or_default();

        Ok(this)
    }

    pub fn tools(&self) -> &[RemoteTool] {
        &self.tools
    }

    pub async fn call_tool(&self, name: &str, args: &Value) -> Result<Value> {
        let resp = self
            .post_rpc("tools/call", json!({ "name": name, "arguments": args }))
            .await?;
        // Collapse `{ content: [{type, text}, ...] }` to a single value for
        // agent ergonomics — identical to the stdio client.
        let content = resp
            .get("result")
            .and_then(|r| r.get("content"))
            .cloned()
            .unwrap_or(Value::Null);
        if let Value::Array(ref items) = content {
            if items.len() == 1 {
                if let Some(text) = items[0].get("text").and_then(|v| v.as_str()) {
                    return Ok(Value::String(text.to_string()));
                }
            }
        }
        Ok(content)
    }

    /// Apply the standard headers (auth, accept, session) to a request builder.
    fn apply_headers(
        &self,
        mut req: reqwest::RequestBuilder,
        session: &Option<String>,
    ) -> reqwest::RequestBuilder {
        // Streamable HTTP requires the client to accept both a single JSON
        // response and an SSE stream.
        req = req.header(
            reqwest::header::ACCEPT,
            "application/json, text/event-stream",
        );
        if let Some(auth) = &self.auth_header {
            req = req.header(reqwest::header::AUTHORIZATION, auth);
        }
        if let Some(sid) = session {
            req = req.header("Mcp-Session-Id", sid);
        }
        req
    }

    /// Retry exactly once after the server definitively rejects an expired
    /// session before dispatch. Ambiguous transport failures are never replayed.
    async fn post_rpc(&self, method: &str, params: Value) -> Result<Value> {
        let session = self.session_id.lock().await.clone();
        match self.post_rpc_once(method, params.clone(), &session).await? {
            RpcReply::Response(response) => Ok(response),
            RpcReply::SessionNotFound => {
                let expired = session
                    .as_deref()
                    .expect("session rejection requires an id");
                self.reinitialize_if_expired(expired).await?;
                let refreshed = self.session_id.lock().await.clone();
                match self.post_rpc_once(method, params, &refreshed).await? {
                    RpcReply::Response(response) => Ok(response),
                    RpcReply::SessionNotFound => Err(anyhow!(
                        "MCP `{method}` session was rejected again after one reconnect"
                    )),
                }
            }
        }
    }

    async fn reinitialize_if_expired(&self, expired: &str) -> Result<()> {
        let _guard = self.reconnect.lock().await;
        if self.session_id.lock().await.as_deref() != Some(expired) {
            return Ok(());
        }
        tracing::warn!(server = %self.server_id, "MCP session expired; reinitializing");
        let no_session = None;
        match self
            .post_rpc_once("initialize", initialize_params(), &no_session)
            .await?
        {
            RpcReply::Response(_) => {}
            RpcReply::SessionNotFound => {
                return Err(anyhow!(
                    "MCP `{}` rejected reinitialization",
                    self.server_id
                ));
            }
        }
        self.post_notification("notifications/initialized", Value::Null)
            .await?;
        let session = self.session_id.lock().await.clone();
        match self
            .post_rpc_once("tools/list", json!({}), &session)
            .await?
        {
            RpcReply::Response(_) => Ok(()),
            RpcReply::SessionNotFound => Err(anyhow!(
                "MCP `{}` rejected the new session during tools/list",
                self.server_id
            )),
        }
    }

    /// POST a single JSON-RPC request with a fixed session snapshot. A 404
    /// session-layer rejection is distinct from a tool error or a lost reply.
    async fn post_rpc_once(
        &self,
        method: &str,
        params: Value,
        session: &Option<String>,
    ) -> Result<RpcReply> {
        let id = 1;
        let payload = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let req = self.apply_headers(self.client.post(&self.url).json(&payload), session);

        let resp = tokio::time::timeout(self.timeout, req.send())
            .await
            .map_err(|_| anyhow!("MCP `{}` to `{}` timed out", method, self.server_id))?
            .map_err(|err| {
                if err.is_timeout() {
                    anyhow!("MCP `{}` to `{}` timed out", method, self.server_id)
                } else {
                    self.transport_error(method, &err.to_string())
                }
            })?;

        let status = resp.status();
        let response_session = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);

        if status.as_u16() == 401 || status.as_u16() == 403 {
            let www = resp
                .headers()
                .get(reqwest::header::WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            return Err(self.scope_error(status.as_u16(), www));
        }

        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let text = resp.text().await.map_err(|err| {
            if err.is_timeout() {
                self.transport_error(method, "response body timed out")
            } else {
                self.transport_error(method, &err.to_string())
            }
        })?;

        let response = if content_type.contains("text/event-stream") {
            parse_sse_response(&text, id)
                .ok_or_else(|| self.transport_error(method, "no JSON-RPC response in SSE stream"))?
        } else if text.trim().is_empty() {
            // A notification-only POST can legitimately return 202 with no body.
            json!({ "jsonrpc": "2.0", "id": id, "result": {} })
        } else {
            serde_json::from_str::<Value>(&text).map_err(|err| {
                self.transport_error(method, &format!("invalid JSON-RPC body: {err}"))
            })?
        };

        if is_session_not_found(status, session, &response) {
            return Ok(RpcReply::SessionNotFound);
        }
        if let Some(err) = response.get("error") {
            return Err(anyhow!("MCP `{}` error: {}", method, err));
        }
        if method == "initialize" {
            *self.session_id.lock().await = response_session;
        } else if let Some(sid) = response_session {
            let mut current = self.session_id.lock().await;
            // A late response from an old session must not replace a newer id.
            if *current == *session {
                *current = Some(sid);
            }
        }
        Ok(RpcReply::Response(response))
    }

    /// Fire-and-forget notification (no response correlation needed).
    async fn post_notification(&self, method: &str, params: Value) -> Result<()> {
        let payload = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        let session = self.session_id.lock().await.clone();
        let req = self.apply_headers(self.client.post(&self.url).json(&payload), &session);
        // Best-effort: a notification failure shouldn't abort the handshake.
        let _ = req.send().await;
        Ok(())
    }

    /// Structured `401`/`403` scope challenge (mcp-http-transport-chidori.md
    /// §3.3), serialized as the error message so it rides the existing
    /// tool-error channel and agent-builder can parse it.
    fn scope_error(&self, status: u16, www_authenticate: Option<String>) -> anyhow::Error {
        let kind = if status == 403 {
            "insufficient_scope"
        } else {
            "unauthorized"
        };
        let required_scopes = www_authenticate
            .as_deref()
            .and_then(parse_required_scopes)
            .unwrap_or_default();
        let value = json!({ "mcpError": {
            "kind": kind,
            "server": self.server_id,
            "wwwAuthenticate": www_authenticate,
            "requiredScopes": required_scopes,
        }});
        anyhow!(value.to_string())
    }

    fn transport_error(&self, method: &str, message: &str) -> anyhow::Error {
        let value = json!({ "mcpError": {
            "kind": "transport",
            "server": self.server_id,
            "message": format!("`{method}`: {message}"),
        }});
        anyhow!(value.to_string())
    }
}

fn is_session_not_found(
    status: reqwest::StatusCode,
    session: &Option<String>,
    response: &Value,
) -> bool {
    status == reqwest::StatusCode::NOT_FOUND
        && session.is_some()
        && response.get("id").is_some_and(Value::is_null)
        && response["error"]["code"].as_i64() == Some(-32000)
        && response["error"]["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("Session not found"))
}

/// Pull `scope="a b c"` out of a `WWW-Authenticate` challenge, if present.
fn parse_required_scopes(www_authenticate: &str) -> Option<Vec<String>> {
    let idx = www_authenticate.find("scope=")?;
    let rest = &www_authenticate[idx + "scope=".len()..];
    let rest = rest.trim_start_matches('"');
    let end = rest.find('"').unwrap_or(rest.len());
    let scopes: Vec<String> = rest[..end].split_whitespace().map(str::to_string).collect();
    if scopes.is_empty() {
        None
    } else {
        Some(scopes)
    }
}

/// Find the JSON-RPC response with `id == want_id` in an SSE stream body. Each
/// event's `data:` payload is a JSON-RPC message; we pick the one correlating
/// to our request (servers may interleave notifications before the response).
fn parse_sse_response(body: &str, want_id: i64) -> Option<Value> {
    let mut last_with_result: Option<Value> = None;
    for line in body.lines() {
        let line = line.trim_start();
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(data.trim()) else {
            continue;
        };
        if value.get("id").and_then(Value::as_i64) == Some(want_id) {
            return Some(value);
        }
        if value.get("result").is_some() || value.get("error").is_some() {
            last_with_result = Some(value);
        }
    }
    last_with_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::State,
        http::{HeaderMap, HeaderValue, StatusCode},
        response::{IntoResponse, Response},
        routing::post,
        Json, Router,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RestartState {
        session: Option<String>,
        initializes: usize,
        stale_rejections: usize,
        executions: usize,
        initialize_headers: Vec<Option<String>>,
        ambiguous_next: bool,
    }

    fn mock_reply(status: StatusCode, body: Value, session: Option<&str>) -> Response {
        let mut response = (status, Json(body)).into_response();
        response
            .headers_mut()
            .insert("connection", HeaderValue::from_static("close"));
        if let Some(session) = session {
            response
                .headers_mut()
                .insert("mcp-session-id", HeaderValue::from_str(session).unwrap());
        }
        response
    }

    fn read_mock_request_body(stream: &mut std::net::TcpStream) -> Value {
        use std::io::Read;
        let mut request = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = stream.read(&mut chunk).unwrap_or(0);
            if n == 0 {
                return Value::Null;
            }
            request.extend_from_slice(&chunk[..n]);
            let Some(headers_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..headers_end]);
            let content_length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            let body_start = headers_end + 4;
            if request.len() >= body_start + content_length {
                return serde_json::from_slice(&request[body_start..body_start + content_length])
                    .unwrap_or(Value::Null);
            }
        }
    }

    async fn restartable_rpc(
        State(state): State<Arc<Mutex<RestartState>>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        let method = body["method"].as_str().unwrap_or("");
        let sent_session = headers
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut state = state.lock().unwrap();
        if method == "initialize" {
            state.initialize_headers.push(sent_session);
            state.initializes += 1;
            let session = format!("sess-{}", state.initializes);
            state.session = Some(session.clone());
            return mock_reply(
                StatusCode::OK,
                json!({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": PROTOCOL_VERSION}}),
                Some(&session),
            );
        }
        if sent_session != state.session {
            state.stale_rejections += 1;
            return mock_reply(
                StatusCode::NOT_FOUND,
                json!({"jsonrpc": "2.0", "id": null, "error": {
                    "code": -32000,
                    "message": "Session not found. Reinitialize the MCP connection without an MCP-Session-Id header."
                }}),
                None,
            );
        }
        match method {
            "notifications/initialized" => {
                let mut response = (StatusCode::ACCEPTED, "").into_response();
                response
                    .headers_mut()
                    .insert("connection", HeaderValue::from_static("close"));
                response
            }
            "tools/list" => mock_reply(
                StatusCode::OK,
                json!({"jsonrpc": "2.0", "id": 1, "result": {"tools": [{
                    "name": "seller_inquire",
                    "description": "test write",
                    "inputSchema": {"type": "object", "properties": {}}
                }]}}),
                None,
            ),
            "tools/call" => {
                state.executions += 1;
                if std::mem::take(&mut state.ambiguous_next) {
                    // The server has executed the write, but the reply is not
                    // a definitive pre-dispatch session rejection.
                    return mock_reply(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({"jsonrpc": "2.0", "id": null, "error": {
                            "code": -32000,
                            "message": "Session not found"
                        }}),
                        None,
                    );
                }
                mock_reply(
                    StatusCode::OK,
                    json!({"jsonrpc": "2.0", "id": 1, "result": {
                        "content": [{"type": "text", "text": "sent-once"}]
                    }}),
                    None,
                )
            }
            _ => mock_reply(StatusCode::NOT_FOUND, json!({"error": "unknown"}), None),
        }
    }

    async fn spawn_restartable_server(
        state: Arc<Mutex<RestartState>>,
        bind: std::net::SocketAddr,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/mcp", post(restartable_rpc))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, task)
    }

    async fn restart_mock_server(
        task: tokio::task::JoinHandle<()>,
        addr: std::net::SocketAddr,
        state: Arc<Mutex<RestartState>>,
    ) -> tokio::task::JoinHandle<()> {
        task.abort();
        let _ = task.await;
        state.lock().unwrap().session = None;
        spawn_restartable_server(state, addr).await.1
    }

    fn restartable_config(addr: std::net::SocketAddr) -> McpServerConfig {
        McpServerConfig {
            command: String::new(),
            args: vec![],
            env: Default::default(),
            enabled: true,
            transport: super::super::config::McpTransport::Http,
            url: Some(format!("http://{addr}/mcp")),
            auth_token: None,
            timeout_ms: 30_000,
        }
    }

    #[test]
    fn parses_scopes_from_www_authenticate() {
        let www = r#"Bearer error="insufficient_scope", scope="tasks:read tasks:write""#;
        assert_eq!(
            parse_required_scopes(www),
            Some(vec!["tasks:read".to_string(), "tasks:write".to_string()])
        );
    }

    #[test]
    fn no_scope_clause_yields_none() {
        assert_eq!(parse_required_scopes(r#"Bearer realm="x""#), None);
    }

    #[test]
    fn sse_picks_matching_id() {
        let body = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"x\"}\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n\n";
        let v = parse_sse_response(body, 1).unwrap();
        assert_eq!(v["result"]["ok"], true);
    }

    /// A minimal JSON-RPC-over-HTTP MCP server: one request per connection
    /// (`Connection: close`, so reqwest reconnects), routed by JSON-RPC method.
    /// Replies to initialize / tools/list / tools/call; ignores notifications.
    fn spawn_mock_mcp_server() -> std::net::SocketAddr {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let parsed = read_mock_request_body(&mut stream);
                let method = parsed.get("method").and_then(Value::as_str).unwrap_or("");
                let result = match method {
                    "initialize" => {
                        json!({"protocolVersion": PROTOCOL_VERSION, "serverInfo": {"name": "mock"}})
                    }
                    "tools/list" => json!({"tools": [{
                        "name": "echo",
                        "description": "echoes",
                        "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}
                    }]}),
                    "tools/call" => json!({"content": [{"type": "text", "text": "echoed!"}]}),
                    _ => {
                        // Notification (no id): 202 with no body.
                        let resp = "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        let _ = stream.write_all(resp.as_bytes());
                        continue;
                    }
                };
                let payload = json!({"jsonrpc": "2.0", "id": 1, "result": result}).to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nMcp-Session-Id: sess-1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    payload.len(), payload,
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        addr
    }

    /// Like `spawn_mock_mcp_server`, but deliberately sleeps before every
    /// response so a per-server timeout can be observed during `initialize`.
    fn spawn_delayed_mock_mcp_server(delay: std::time::Duration) -> std::net::SocketAddr {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let parsed = read_mock_request_body(&mut stream);
                let method = parsed.get("method").and_then(Value::as_str).unwrap_or("");
                std::thread::sleep(delay);
                let response = match method {
                    "initialize" => {
                        json!({"protocolVersion": PROTOCOL_VERSION, "serverInfo": {"name": "slow"}})
                    }
                    "tools/list" => json!({"tools": []}),
                    _ => json!({}),
                };
                let payload = json!({"jsonrpc": "2.0", "id": 1, "result": response}).to_string();
                let http = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    payload.len(), payload,
                );
                let _ = stream.write_all(http.as_bytes());
            }
        });
        addr
    }

    /// Sends headers promptly, then stalls before completing the JSON body.
    fn spawn_stalled_body_mcp_server(delay: std::time::Duration) -> std::net::SocketAddr {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = read_mock_request_body(&mut stream);
                let payload = json!({"jsonrpc": "2.0", "id": 1, "result": {}}).to_string();
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    payload.len(),
                );
                let _ = stream.write_all(headers.as_bytes());
                std::thread::sleep(delay);
                let _ = stream.write_all(payload.as_bytes());
            }
        });
        addr
    }

    #[test]
    fn connect_discovers_tools_and_calls_them() {
        let addr = spawn_mock_mcp_server();
        let cfg = McpServerConfig {
            command: String::new(),
            args: vec![],
            env: Default::default(),
            enabled: true,
            transport: super::super::config::McpTransport::Http,
            url: Some(format!("http://{addr}/mcp")),
            auth_token: None,
            timeout_ms: 30_000,
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let client = McpHttpClient::connect("mock", cfg).await.unwrap();
            assert_eq!(client.tools().len(), 1);
            assert_eq!(client.tools()[0].name, "echo");
            let out = client
                .call_tool("echo", &json!({"text": "hi"}))
                .await
                .unwrap();
            // Single text content block collapses to a string.
            assert_eq!(out, Value::String("echoed!".to_string()));
        });
    }

    #[test]
    fn connect_honors_the_http_servers_configured_timeout() {
        let addr = spawn_delayed_mock_mcp_server(std::time::Duration::from_millis(100));
        let cfg: McpServerConfig = serde_json::from_str(&format!(
            r#"{{"transport":"http","url":"http://{addr}/mcp","timeoutMs":25}}"#
        ))
        .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let err = match rt.block_on(McpHttpClient::connect("slow", cfg)) {
            Ok(_) => panic!("a 25ms server timeout must abort a 100ms HTTP response"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("timed out"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn connect_times_out_when_http_body_stalls_after_headers() {
        let addr = spawn_stalled_body_mcp_server(std::time::Duration::from_millis(150));
        let cfg: McpServerConfig = serde_json::from_str(&format!(
            r#"{{"transport":"http","url":"http://{addr}/mcp","timeoutMs":30}}"#
        ))
        .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let err = match rt.block_on(McpHttpClient::connect("stalled-body", cfg)) {
            Ok(_) => panic!("a 30ms timeout must abort a stalled response body"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("timed out"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn reconnects_after_server_restart_and_executes_a_write_once() {
        let state = Arc::new(Mutex::new(RestartState::default()));
        let (addr, server) = spawn_restartable_server(
            state.clone(),
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        )
        .await;
        let client = McpHttpClient::connect("mock", restartable_config(addr))
            .await
            .unwrap();

        // The listener is replaced at the same URL; the client remains alive
        // with the old session id, as in a remote MCP service restart.
        let server = restart_mock_server(server, addr, state.clone()).await;
        let out = client
            .call_tool("seller_inquire", &json!({"message": "test"}))
            .await
            .unwrap();
        assert_eq!(out, Value::String("sent-once".into()));
        let state = state.lock().unwrap();
        assert_eq!(state.initializes, 2);
        assert_eq!(state.stale_rejections, 1);
        assert_eq!(state.executions, 1);
        assert_eq!(state.initialize_headers, vec![None, None]);
        server.abort();
    }

    #[tokio::test]
    async fn does_not_retry_a_write_after_an_ambiguous_server_failure() {
        let state = Arc::new(Mutex::new(RestartState::default()));
        let (addr, server) = spawn_restartable_server(
            state.clone(),
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        )
        .await;
        let client = McpHttpClient::connect("mock", restartable_config(addr))
            .await
            .unwrap();
        state.lock().unwrap().ambiguous_next = true;

        let err = client
            .call_tool("seller_inquire", &json!({"message": "test"}))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Session not found"));
        let state = state.lock().unwrap();
        assert_eq!(state.initializes, 1);
        assert_eq!(state.executions, 1);
        server.abort();
    }

    #[tokio::test]
    async fn concurrent_stale_calls_share_one_reinitialization() {
        let state = Arc::new(Mutex::new(RestartState::default()));
        let (addr, server) = spawn_restartable_server(
            state.clone(),
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        )
        .await;
        let client = McpHttpClient::connect("mock", restartable_config(addr))
            .await
            .unwrap();
        let server = restart_mock_server(server, addr, state.clone()).await;

        let first_args = json!({"message": "one"});
        let second_args = json!({"message": "two"});
        let (first, second) = tokio::join!(
            client.call_tool("seller_inquire", &first_args),
            client.call_tool("seller_inquire", &second_args)
        );
        assert!(first.is_ok(), "first call: {first:?}");
        assert!(second.is_ok(), "second call: {second:?}");
        let state = state.lock().unwrap();
        assert_eq!(state.initializes, 2);
        assert_eq!(state.executions, 2);
        assert_eq!(state.initialize_headers, vec![None, None]);
        server.abort();
    }
}
