//! 新闻实时流(spec: Alpaca `WebSocket-News.md`)。
//!
//! 认证通过 WS 升级请求头完成;连接建立后由 `supervisor`
//! 监督:断线自动重连(指数退避),重连后自动重放订阅。

use std::sync::{Arc, Mutex};

use anyhow::Context as _;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use simd_json::prelude::*;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::status::StreamStatus;
use crate::supervisor::{self, Protocol};
use crate::ws::wait_control;

/// 新闻流默认 URL。
pub const NEWS_STREAM_URL: &str = "wss://stream.data.alpaca.markets/v1beta1/news";

/// One news article.
#[derive(Debug, Clone, Deserialize)]
pub struct NewsArticle {
    pub id: i64,
    pub headline: String,
    /// Summary (may be the first sentence of [`NewsArticle::content`]).
    pub summary: String,
    pub author: String,
    #[serde(rename = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "updated_at")]
    pub updated_at: DateTime<Utc>,
    /// Full content; may contain HTML.
    pub content: String,
    /// Article URL, when applicable.
    pub url: Option<String>,
    /// Related / mentioned symbols.
    pub symbols: Vec<String>,
    /// Originating source (e.g. `benzinga`).
    pub source: Option<String>,
}

/// Tagged news-stream message (`T` discriminator).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "T")]
pub enum NewsMessage {
    #[serde(rename = "n")]
    News(NewsArticle),
    #[serde(rename = "subscription")]
    Subscription { news: Option<Vec<String>> },
    #[serde(rename = "success")]
    Success { msg: String },
    #[serde(rename = "error")]
    ServerError { code: String, msg: String },
}

// ---------- supervisor 接入 ----------

#[derive(Debug, Clone)]
enum NewsAction {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

/// 期望订阅状态:符号集合,`"*"` 表示全部;空集合 = 不订阅任何内容。
type NewsDesired = Vec<String>;

struct NewsProtocol {
    url: String,
    key_id: String,
    secret: String,
    proxy: Option<String>,
}

fn action_json(action: &str, symbols: &[String]) -> String {
    simd_json::json!({ "action": action, "news": symbols.to_vec() }).encode()
}

impl Protocol for NewsProtocol {
    type Message = NewsMessage;
    type Action = NewsAction;
    type Desired = NewsDesired;

    fn url(&self) -> String {
        self.url.clone()
    }

    fn proxy(&self) -> Option<String> {
        self.proxy.clone()
    }

    /// 新闻流的认证走升级请求头。
    fn upgrade_headers(&self) -> Vec<(String, String)> {
        vec![
            ("APCA-API-KEY-ID".into(), self.key_id.clone()),
            ("APCA-API-SECRET-KEY".into(), self.secret.clone()),
        ]
    }

    async fn handshake(&self, _write: &mut crate::ws::WsWriter, read: &mut crate::ws::WsReader) -> anyhow::Result<()> {
        wait_control(read, "authenticated")
            .await
            .context("news stream: handshake")
    }

    fn replay(desired: &NewsDesired) -> Vec<String> {
        if desired.is_empty() {
            Vec::new()
        } else {
            vec![action_json("subscribe", desired)]
        }
    }

    fn apply(desired: &mut NewsDesired, action: &NewsAction) -> Vec<String> {
        let (wire_action, symbols) = match action {
            NewsAction::Subscribe(symbols) => {
                for symbol in symbols {
                    if !desired.contains(symbol) {
                        desired.push(symbol.clone());
                    }
                }
                ("subscribe", symbols)
            }
            NewsAction::Unsubscribe(symbols) => {
                desired.retain(|s| !symbols.contains(s));
                ("unsubscribe", symbols)
            }
        };
        vec![action_json(wire_action, symbols)]
    }

    /// 新闻流消息量低:直接走 serde 兜底路径。
    fn parse_frame(buf: &mut [u8]) -> Vec<NewsMessage> {
        crate::hotpath::parse_frame_serde(buf)
    }
}

// ---------- handle ----------

#[derive(Debug)]
struct Inner {
    actions: mpsc::Sender<NewsAction>,
    messages: broadcast::Sender<NewsMessage>,
    status: broadcast::Sender<StreamStatus>,
    close: CancellationToken,
}

/// Live news stream, supervised for auto-reconnect.
///
/// Clone 廉价(共享同一连接);**最后一个句柄 drop 时流自动关闭**。
#[derive(Clone, Debug)]
pub struct NewsStream {
    inner: Arc<Inner>,
}

impl NewsStream {
    /// Connect to the default news URL. The first attempt is synchronous and
    /// bounded by [`crate::ws::CONNECT_TIMEOUT`] — a failure returns `Err`
    /// (fail-fast); afterwards the supervisor keeps the connection alive:
    /// auto-reconnect with exponential backoff + subscription replay until
    /// [`close`](Self::close) or the last handle is dropped.
    pub async fn connect(key_id: impl Into<String>, secret: impl Into<String>) -> anyhow::Result<Self> {
        Self::connect_to(NEWS_STREAM_URL, key_id, secret).await
    }

    /// [`connect`](Self::connect) 的自定义端点版本(自建代理 / 沙箱 / 测试服务)。
    pub async fn connect_to(
        url: &str,
        key_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> anyhow::Result<Self> {
        Self::connect_to_with_proxy(url, key_id, secret, None).await
    }

    /// [`connect_to`](Self::connect_to) 的代理版本。`proxy` 为 `socks5://host:port`
    /// (SOCKS5,无认证;域名解析发生在代理端);`None` = 直连。显式传入时
    /// 优先于 `ALL_PROXY` / `all_proxy` 环境变量(后者仅在 socks5 scheme 时生效)。
    pub async fn connect_to_with_proxy(
        url: &str,
        key_id: impl Into<String>,
        secret: impl Into<String>,
        proxy: Option<String>,
    ) -> anyhow::Result<Self> {
        let protocol = Arc::new(NewsProtocol {
            url: url.to_string(),
            key_id: key_id.into(),
            secret: secret.into(),
            proxy,
        });

        let first = supervisor::attempt_connect(protocol.as_ref())
            .await
            .context("news stream: initial connect failed")?;

        let (messages, _) = broadcast::channel(4096);
        let (status, _) = broadcast::channel(16);
        let (actions, action_rx) = mpsc::channel(16);
        let desired = Arc::new(Mutex::new(Vec::new()));
        let close = CancellationToken::new();

        tokio::spawn(supervisor::run(
            protocol,
            desired,
            close.clone(),
            action_rx,
            messages.clone(),
            status.clone(),
            Some(first),
        ));

        Ok(Self {
            inner: Arc::new(Inner {
                actions,
                messages,
                status,
                close,
            }),
        })
    }

    /// Subscribe to specific symbols, or `["*"]` for everything. While
    /// disconnected the request is merged into the desired state and
    /// replayed after reconnect.
    pub async fn subscribe(&self, symbols: &[&str]) -> anyhow::Result<()> {
        self.send(NewsAction::Subscribe(symbols.iter().map(|s| s.to_string()).collect()))
            .await
    }

    /// Subscribe to every article.
    pub async fn subscribe_all(&self) -> anyhow::Result<()> {
        self.subscribe(&["*"]).await
    }

    /// Unsubscribe from specific symbols, or `"*"` for everything; the
    /// socket stays open. Mirrors [`subscribe`](Self::subscribe) while
    /// disconnected.
    pub async fn unsubscribe(&self, symbols: &[&str]) -> anyhow::Result<()> {
        self.send(NewsAction::Unsubscribe(symbols.iter().map(|s| s.to_string()).collect()))
            .await
    }

    /// Unsubscribe from every article.
    pub async fn unsubscribe_all(&self) -> anyhow::Result<()> {
        self.unsubscribe(&["*"]).await
    }

    async fn send(&self, action: NewsAction) -> anyhow::Result<()> {
        self.inner
            .actions
            .send(action)
            .await
            .context("news stream: closed")
    }

    /// Subscribe to every message of the stream.
    pub fn messages(&self) -> broadcast::Receiver<NewsMessage> {
        self.inner.messages.subscribe()
    }

    /// 连接状态事件([`StreamStatus`]:Connected / Reconnecting /
    /// Failed{reason,message} / Closed)。
    pub fn status(&self) -> broadcast::Receiver<StreamStatus> {
        self.inner.status.subscribe()
    }

    /// 显式关闭流(supervisor 停止重连并清理连接任务)。幂等。
    pub fn close(&self) {
        self.inner.close.cancel();
    }
}

impl Drop for NewsStream {
    fn drop(&mut self) {
        // 最后一个句柄 drop → 关闭流。
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.close.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// simd-json 等价解析:wire 帧字符串 → DOM 值。
    fn parse_dom(text: &str) -> simd_json::OwnedValue {
        let mut bytes = text.as_bytes().to_vec();
        simd_json::to_owned_value(&mut bytes).unwrap()
    }

    #[test]
    fn apply_unions_symbols_and_replays_desired_state() {
        let mut desired: NewsDesired = Vec::new();
        let frames = NewsProtocol::apply(&mut desired, &NewsAction::Subscribe(vec!["AAPL".into(), "MSFT".into()]));
        assert_eq!(desired, ["AAPL", "MSFT"]);
        assert_eq!(frames.len(), 1);
        let json = parse_dom(&frames[0]);
        assert_eq!(json.get("action").and_then(|v| v.as_str()), Some("subscribe"));
        assert_eq!(json.get("news"), Some(&simd_json::json!(["AAPL", "MSFT"])));

        // 重复订阅不产生重复符号。
        NewsProtocol::apply(&mut desired, &NewsAction::Subscribe(vec!["AAPL".into(), "TSLA".into()]));
        assert_eq!(desired, ["AAPL", "MSFT", "TSLA"]);

        // 退订移除符号,星号与普通符号共存于同一集合。
        NewsProtocol::apply(&mut desired, &NewsAction::Subscribe(vec!["*".into()]));
        NewsProtocol::apply(&mut desired, &NewsAction::Unsubscribe(vec!["AAPL".into()]));
        assert_eq!(desired, ["MSFT", "TSLA", "*"]);
        NewsProtocol::apply(&mut desired, &NewsAction::Unsubscribe(vec!["*".into(), "MSFT".into(), "TSLA".into()]));
        assert!(desired.is_empty());

        // 空期望状态不重放任何帧。
        assert!(NewsProtocol::replay(&desired).is_empty());
        NewsProtocol::apply(&mut desired, &NewsAction::Subscribe(vec!["*".into()]));
        let replay = NewsProtocol::replay(&desired);
        assert_eq!(replay.len(), 1);
        let json = parse_dom(&replay[0]);
        assert_eq!(json.get("news"), Some(&simd_json::json!(["*"])));
    }

    /// News articles decode with their wire field names.
    #[test]
    fn news_message_deserializes() {
        let mut bytes = br#"{
            "T": "n",
            "id": 24843171,
            "headline": "Apple Leader in Phone Sales",
            "summary": "summary",
            "author": "Charles Gross",
            "created_at": "2021-12-31T11:08:42Z",
            "updated_at": "2021-12-31T11:08:43Z",
            "content": "<p>...</p>",
            "url": null,
            "symbols": ["AAPL"],
            "source": "benzinga"
        }"#
        .to_vec();
        let msg: NewsMessage = simd_json::from_slice(&mut bytes).unwrap();
        let NewsMessage::News(article) = msg else {
            panic!("expected news, got {msg:?}");
        };
        assert_eq!(article.id, 24843171);
        assert!(article.url.is_none());
        assert_eq!(article.symbols, ["AAPL"]);
    }
}
