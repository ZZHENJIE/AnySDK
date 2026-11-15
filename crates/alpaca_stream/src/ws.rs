//! 底层 WebSocket 传输原语:类型别名、TLS 初始化、带超时的连接、握手等待。

use std::time::Duration;

use anyhow::Context as _;
use futures::StreamExt as _;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

/// Concrete WebSocket (TLS via the crate's rustls feature).
pub(crate) type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
/// Write half of a split socket.
pub(crate) type WsWriter = futures::stream::SplitSink<WsStream, Message>;
/// Read half of a split socket.
pub(crate) type WsReader = futures::stream::SplitStream<WsStream>;

/// 单次连接尝试(TCP/TLS/升级)的硬超时;防火墙黑洞必须表现为普通错误,绝不挂死。
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// 主动保活:writer 每隔该间隔发送一个 WS ping 帧,防止 NAT / 代理静默断链。
pub const PING_INTERVAL: Duration = Duration::from_secs(25);

/// 下行静默判定:超过该时长没有收到任何帧(数据 / 心跳 / pong)即视为死链,强制重连。
pub const STALE_AFTER: Duration = Duration::from_secs(90);

/// 握手阶段的永久性错误:服务端 `T: "error"` 帧(认证失败、订阅不含该
/// feed、连接数超限等)。重连不可能成功 —— supervisor 收到后停止重连
/// ([`StreamStatus::Failed`]),`connect()` 则直接返回 `Err`。
#[derive(Debug)]
pub(crate) struct Fatal(pub String);

impl std::fmt::Display for Fatal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Fatal {}

/// tokio-tungstenite pulls in rustls without default features, i.e. without
/// any crypto provider. Install the ring-based one once per process before
/// the first `wss://` handshake.
pub(crate) fn ensure_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// 连接 `url`(可附加升级请求头),整体受 [`CONNECT_TIMEOUT`] 约束。
pub(crate) async fn connect_ws(
    url: &str,
    headers: &[(String, String)],
) -> anyhow::Result<(WsWriter, WsReader)> {
    ensure_crypto_provider();
    let connect = async {
        let mut request = url
            .into_client_request()
            .with_context(|| format!("build upgrade request for {url}"))?;
        for (key, value) in headers {
            let name: HeaderName = key
                .parse()
                .with_context(|| format!("bad header name {key:?}"))?;
            let value: HeaderValue = value
                .parse()
                .with_context(|| format!("bad header value for {key:?}"))?;
            request.headers_mut().insert(name, value);
        }
        let (ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .with_context(|| format!("connect failed: {url}"))?;
        let (write, read) = ws.split();
        Ok((write, read))
    };
    match tokio::time::timeout(CONNECT_TIMEOUT, connect).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!("connect timed out after {CONNECT_TIMEOUT:?}"),
    }
}

/// Read frames until the server reports the given `success` message (or fail
/// on error). Used for the connect handshakes.
pub(crate) async fn wait_control(read: &mut WsReader, want: &str) -> anyhow::Result<()> {
    loop {
        let frame = read
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("stream closed during handshake"))??;
        let Message::Text(text) = frame else { continue };
        let array: Vec<serde_json::Value> = serde_json::from_str(text.as_str())
            .with_context(|| format!("handshake frame: {}", text.as_str()))?;
        for value in array {
            match value.get("T").and_then(|t| t.as_str()) {
                Some("success") => {
                    if value.get("msg").and_then(|m| m.as_str()) == Some(want) {
                        return Ok(());
                    }
                }
                // 永久性错误:标记为 Fatal,supervisor 不再重连。
                Some("error") => {
                    return Err(anyhow::Error::new(Fatal(format!(
                        "stream handshake error: {value}"
                    ))))
                }
                _ => {}
            }
        }
    }
}
