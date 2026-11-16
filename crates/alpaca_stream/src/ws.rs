//! 底层 WebSocket 传输原语:类型别名、TLS 初始化、带超时的连接、握手等待。
//!
//! TCP 建链支持可选 SOCKS5 代理:代理只介入 TCP 阶段(域名解析发生在代理端),
//! 其上的 TLS 与 WS 升级仍由 tungstenite 完成,协议层无感知。

use std::time::Duration;

use anyhow::Context as _;
use futures::StreamExt as _;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};

/// Concrete WebSocket (TLS via the crate's rustls feature).
pub(crate) type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<Transport>>;
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

/// TLS 升级前的底层字节流:直连 TCP,或已完成 SOCKS5 握手的代理流。
///
/// tungstenite 要求统一的 `AsyncRead + AsyncWrite` 流类型才能做 TLS + WS
/// 升级,两条建链路径(直连 / 代理)由此枚举汇合。
#[derive(Debug)]
pub(crate) enum Transport {
    Direct(tokio::net::TcpStream),
    Socks5(tokio_socks::tcp::Socks5Stream<tokio::net::TcpStream>),
}

impl tokio::io::AsyncRead for Transport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Direct(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            Self::Socks5(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for Transport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Direct(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            Self::Socks5(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Direct(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            Self::Socks5(stream) => std::pin::Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Direct(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            Self::Socks5(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// 解析 `socks5://host:port`(或 `socks5h://`,语义与本实现一致:域名在代理端
/// 解析)。仅支持无认证代理;解析失败直接报错(fail-fast)。
pub(crate) fn parse_proxy(value: &str) -> anyhow::Result<(String, u16)> {
    let rest = value
        .strip_prefix("socks5h://")
        .or_else(|| value.strip_prefix("socks5://"))
        .with_context(|| {
            format!("unsupported proxy scheme {value:?} (only socks5:// is supported)")
        })?;
    anyhow::ensure!(
        !rest.contains('@'),
        "proxy authentication is not supported: {value:?}"
    );
    let (host, port) = rest
        .rsplit_once(':')
        .with_context(|| format!("proxy address must be socks5://host:port, got {value:?}"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("proxy port in {value:?}"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    anyhow::ensure!(!host.is_empty(), "proxy address has empty host: {value:?}");
    Ok((host.to_owned(), port))
}

/// 代理查找结果:显式参数优先;未提供时读 `ALL_PROXY` / `all_proxy` 兜底。
///
/// 显式参数的 scheme 错误会直接报错(调用方明确想走代理);环境变量仅在值
/// 以 `socks5://` / `socks5h://` 开头时生效——其他 scheme(如常见的
/// `http://…`)静默忽略,保持直连,不让为其他工具设置的变量弄断流。
fn resolve_proxy_with(
    explicit: Option<&str>,
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Option<(String, u16)>> {
    match explicit {
        Some(value) => parse_proxy(value).map(Some),
        None => ["ALL_PROXY", "all_proxy"]
            .iter()
            .find_map(|key| env(key).filter(|v| is_socks5_scheme(v)))
            .map(|v| parse_proxy(v.trim()))
            .transpose(),
    }
}

fn is_socks5_scheme(value: &str) -> bool {
    let value = value.trim();
    value.starts_with("socks5://") || value.starts_with("socks5h://")
}

fn resolve_proxy(explicit: Option<&str>) -> anyhow::Result<Option<(String, u16)>> {
    resolve_proxy_with(explicit, |key| std::env::var(key).ok())
}

/// 升级请求的目标 `(host, port)`(`wss` 默认 443,`ws` 默认 80)。
fn request_target(request: &Request) -> anyhow::Result<(String, u16)> {
    let uri = request.uri();
    let host = uri.host().context("ws url has no host")?.to_owned();
    let port = uri
        .port_u16()
        .or_else(|| match uri.scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .context("ws url has no port and scheme has no default")?;
    Ok((host, port))
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
                    ))));
                }
                _ => {}
            }
        }
    }
}

/// 连接 `url`(可附加升级请求头,可选 SOCKS5 代理),整体受 [`CONNECT_TIMEOUT`]
/// 约束。代理只在 TCP 建链阶段介入:目标域名交给代理端解析,其上的 TLS 与
/// WS 升级不变;重连由 supervisor 走同一入口,自然同样经过代理。
pub(crate) async fn connect_ws(
    url: &str,
    headers: &[(String, String)],
    proxy: Option<&str>,
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
        let proxy = resolve_proxy(proxy)?;

        // 直连分支与 tokio_tungstenite::connect_async 同路径(TCP → TLS/升级),
        // 仅把底层流包进 Transport 以便与代理分支汇合到同一 WsStream 类型。
        let (host, port) = request_target(&request)?;
        let transport = match proxy {
            None => {
                let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
                    .await
                    .with_context(|| format!("connect failed: {url}"))?;
                Transport::Direct(tcp)
            }
            Some((proxy_host, proxy_port)) => {
                // 目标传 (域名, 端口),解析交给代理端,顺带绕开本地 DNS 污染。
                let stream = tokio_socks::tcp::Socks5Stream::connect(
                    (proxy_host.as_str(), proxy_port),
                    (host.as_str(), port),
                )
                .await
                .with_context(|| format!("connect via proxy failed: {url}"))?;
                Transport::Socks5(stream)
            }
        };
        let (ws, _) =
            tokio_tungstenite::client_async_tls_with_config(request, transport, None, None)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_proxy_accepts_socks5_and_socks5h() {
        assert_eq!(
            parse_proxy("socks5://127.0.0.1:7890").unwrap(),
            ("127.0.0.1".into(), 7890)
        );
        // socks5h 与本实现语义一致(域名始终由代理端解析),同样接受。
        assert_eq!(
            parse_proxy("socks5h://proxy.example.com:1080").unwrap(),
            ("proxy.example.com".into(), 1080)
        );
        assert_eq!(
            parse_proxy("socks5://[::1]:7890").unwrap(),
            ("::1".into(), 7890)
        );
    }

    #[test]
    fn parse_proxy_rejects_other_schemes_and_garbage() {
        // 其他 scheme 明确报错,说明不支持。
        assert!(parse_proxy("http://127.0.0.1:7890").is_err());
        assert!(parse_proxy("https://127.0.0.1:7890").is_err());
        assert!(parse_proxy("socks4://127.0.0.1:1080").is_err());
        // 缺端口 / 坏端口 / 缺 host / 认证信息。
        assert!(parse_proxy("socks5://127.0.0.1").is_err());
        assert!(parse_proxy("socks5://127.0.0.1:notaport").is_err());
        assert!(parse_proxy("socks5://:7890").is_err());
        assert!(parse_proxy("socks5://user:pass@127.0.0.1:7890").is_err());
        assert!(parse_proxy("").is_err());
    }

    #[test]
    fn resolve_proxy_explicit_wins_over_env() {
        let env = |key: &str| match key {
            "ALL_PROXY" => Some("socks5://10.0.0.1:1080".into()),
            _ => None,
        };
        let resolved = resolve_proxy_with(Some("socks5://127.0.0.1:7890"), env).unwrap();
        assert_eq!(resolved, Some(("127.0.0.1".into(), 7890)));
    }

    #[test]
    fn resolve_proxy_falls_back_to_env() {
        let env = |key: &str| match key {
            "all_proxy" => Some(" socks5h://127.0.0.1:7890 ".into()),
            _ => None,
        };
        let resolved = resolve_proxy_with(None, env).unwrap();
        assert_eq!(resolved, Some(("127.0.0.1".into(), 7890)));
    }

    #[test]
    fn resolve_proxy_ignores_non_socks5_env() {
        // 常见的 ALL_PROXY=http://… 不弄断流:静默忽略,保持直连。
        let env = |key: &str| match key {
            "ALL_PROXY" => Some("http://127.0.0.1:7890".into()),
            _ => None,
        };
        assert_eq!(resolve_proxy_with(None, env).unwrap(), None);
        assert_eq!(resolve_proxy_with(None, |_| None).unwrap(), None);
    }

    #[test]
    fn resolve_proxy_explicit_bad_scheme_fails_fast() {
        assert!(resolve_proxy_with(Some("http://127.0.0.1:7890"), |_| None).is_err());
    }
}
