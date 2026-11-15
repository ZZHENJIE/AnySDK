//! 股票实时数据流(spec: Alpaca `WebSocket-Stock.md`)。
//!
//! 频道:trades、quotes、minute/daily/updated bars、trade corrections、
//! trade cancels/errors、LULD bands、trading statuses、order imbalances。
//!
//! 连接由 [`supervisor`](crate::supervisor) 监督:断线自动重连(指数退避),
//! 重连后自动重放订阅,期间 [`StockStream::status`] 广播 [`StreamStatus`] 事件。

use std::sync::{Arc, Mutex};

use alpaca_sdk::Feed;
use anyhow::Context as _;
use chrono::{DateTime, Utc};
use futures::SinkExt as _;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::status::StreamStatus;
use crate::supervisor::{self, Protocol};
use crate::ws::wait_control;

/// 股票流默认 base URL。
pub const STOCK_STREAM_URL: &str = "wss://stream.data.alpaca.markets";

/// Which stock channels to subscribe to. Symbols are plain (`"AAPL"`) or
/// `"*"` for everything. Subscribing to `trades` automatically enables
/// corrections and cancel/errors.
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StockSubscription {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub trades: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quotes: Vec<String>,
    /// Minute bars.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub bars: Vec<String>,
    /// Bars updated by late trades.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub updated_bars: Vec<String>,
    /// Running daily bars.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub daily_bars: Vec<String>,
    /// Trading halt/resume statuses.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<String>,
    /// Limit up / limit down bands.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub lulds: Vec<String>,
    /// Order imbalances (spec: subscribe with e.g. `["INAQU"]`).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub imbalances: Vec<String>,
}

impl StockSubscription {
    fn is_empty(&self) -> bool {
        self.trades.is_empty()
            && self.quotes.is_empty()
            && self.bars.is_empty()
            && self.updated_bars.is_empty()
            && self.daily_bars.is_empty()
            && self.statuses.is_empty()
            && self.lulds.is_empty()
            && self.imbalances.is_empty()
    }

    /// 合并另一个订阅的符号(去重,保持首次出现的顺序)。
    fn merge(&mut self, other: &Self) {
        for_channels(self, other, merge_channel);
    }

    /// 移除另一个订阅中出现的符号。
    fn unmerge(&mut self, other: &Self) {
        for_channels(self, other, |target, symbols| {
            target.retain(|s| !symbols.contains(s));
        });
    }
}

fn for_channels(
    a: &mut StockSubscription,
    b: &StockSubscription,
    mut f: impl FnMut(&mut Vec<String>, &[String]),
) {
    f(&mut a.trades, &b.trades);
    f(&mut a.quotes, &b.quotes);
    f(&mut a.bars, &b.bars);
    f(&mut a.updated_bars, &b.updated_bars);
    f(&mut a.daily_bars, &b.daily_bars);
    f(&mut a.statuses, &b.statuses);
    f(&mut a.lulds, &b.lulds);
    f(&mut a.imbalances, &b.imbalances);
}

fn merge_channel(target: &mut Vec<String>, symbols: &[String]) {
    for symbol in symbols {
        if !target.contains(symbol) {
            target.push(symbol.clone());
        }
    }
}

/// A trade print.
#[derive(Debug, Clone, Deserialize)]
pub struct TradeMessage {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub id: i64,
    /// Exchange code where the trade occurred.
    #[serde(rename = "x")]
    pub exchange: String,
    #[serde(rename = "p")]
    pub price: f64,
    #[serde(rename = "s")]
    pub size: i64,
    /// Trade conditions.
    #[serde(rename = "c", default)]
    pub conditions: Vec<String>,
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    /// Tape.
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// A top-of-book quote.
#[derive(Debug, Clone, Deserialize)]
pub struct QuoteMessage {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "ax")]
    pub ask_exchange: String,
    #[serde(rename = "ap")]
    pub ask_price: f64,
    /// Ask size in round lots.
    #[serde(rename = "as")]
    pub ask_size: i64,
    #[serde(rename = "bx")]
    pub bid_exchange: String,
    #[serde(rename = "bp")]
    pub bid_price: f64,
    /// Bid size in round lots.
    #[serde(rename = "bs")]
    pub bid_size: i64,
    #[serde(rename = "c", default)]
    pub conditions: Vec<String>,
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// Bar payload shared by the minute / daily / updated bars channels.
#[derive(Debug, Clone, Deserialize)]
pub struct BarMessage {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "o")]
    pub open: f64,
    #[serde(rename = "h")]
    pub high: f64,
    #[serde(rename = "l")]
    pub low: f64,
    #[serde(rename = "c")]
    pub close: f64,
    #[serde(rename = "v")]
    pub volume: i64,
    /// Volume-weighted average price.
    #[serde(rename = "vw")]
    pub vwap: f64,
    /// Number of trades.
    #[serde(rename = "n")]
    pub trade_count: i64,
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
}

/// Correction of a previously sent trade.
#[derive(Debug, Clone, Deserialize)]
pub struct CorrectionMessage {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "x")]
    pub exchange: String,
    #[serde(rename = "oi")]
    pub original_id: i64,
    #[serde(rename = "op")]
    pub original_price: f64,
    #[serde(rename = "os")]
    pub original_size: i64,
    #[serde(rename = "oc", default)]
    pub original_conditions: Vec<String>,
    #[serde(rename = "ci")]
    pub corrected_id: i64,
    #[serde(rename = "cp")]
    pub corrected_price: f64,
    #[serde(rename = "cs")]
    pub corrected_size: i64,
    #[serde(rename = "cc", default)]
    pub corrected_conditions: Vec<String>,
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// Cancellation (`a = "C"`) or error (`a = "E"`) of a previously sent trade.
#[derive(Debug, Clone, Deserialize)]
pub struct CancelErrorMessage {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub id: i64,
    #[serde(rename = "x")]
    pub exchange: String,
    #[serde(rename = "p")]
    pub price: f64,
    #[serde(rename = "s")]
    pub size: i64,
    /// `"C"` for cancel, `"E"` for error.
    #[serde(rename = "a")]
    pub action: String,
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// Limit up / limit down price band.
#[derive(Debug, Clone, Deserialize)]
pub struct LuldMessage {
    #[serde(rename = "S")]
    pub symbol: String,
    /// Limit up price.
    #[serde(rename = "u")]
    pub limit_up: f64,
    /// Limit down price.
    #[serde(rename = "d")]
    pub limit_down: f64,
    /// Indicator.
    #[serde(rename = "i")]
    pub indicator: String,
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// Trading status (halts, resumes, pauses).
#[derive(Debug, Clone, Deserialize)]
pub struct TradingStatusMessage {
    #[serde(rename = "S")]
    pub symbol: String,
    /// Status code.
    #[serde(rename = "sc")]
    pub status_code: String,
    /// Status message.
    #[serde(rename = "sm")]
    pub status_message: String,
    /// Reason code.
    #[serde(rename = "rc")]
    pub reason_code: String,
    /// Reason message.
    #[serde(rename = "rm")]
    pub reason_message: String,
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// Order imbalance print.
#[derive(Debug, Clone, Deserialize)]
pub struct ImbalanceMessage {
    #[serde(rename = "S")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price: f64,
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// Echo of the channels currently active after a subscribe.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionState {
    #[serde(default)]
    pub trades: Vec<String>,
    #[serde(default)]
    pub quotes: Vec<String>,
    #[serde(default)]
    pub bars: Vec<String>,
    #[serde(default)]
    pub updated_bars: Vec<String>,
    #[serde(default)]
    pub daily_bars: Vec<String>,
    #[serde(default)]
    pub statuses: Vec<String>,
    #[serde(default)]
    pub lulds: Vec<String>,
    #[serde(default)]
    pub corrections: Vec<String>,
    #[serde(default)]
    pub cancel_errors: Vec<String>,
}

/// Tagged stock-stream message (`T` discriminator, spec table).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "T")]
pub enum StockMessage {
    #[serde(rename = "t")]
    Trade(TradeMessage),
    #[serde(rename = "q")]
    Quote(QuoteMessage),
    /// Minute bars channel.
    #[serde(rename = "b")]
    MinuteBar(BarMessage),
    /// Daily bars channel.
    #[serde(rename = "d")]
    DailyBar(BarMessage),
    /// Updated bars channel.
    #[serde(rename = "u")]
    UpdatedBar(BarMessage),
    #[serde(rename = "c")]
    TradeCorrection(CorrectionMessage),
    #[serde(rename = "x")]
    TradeCancelError(CancelErrorMessage),
    #[serde(rename = "l")]
    Luld(LuldMessage),
    #[serde(rename = "s")]
    TradingStatus(TradingStatusMessage),
    #[serde(rename = "i")]
    Imbalance(ImbalanceMessage),
    #[serde(rename = "subscription")]
    Subscription(SubscriptionState),
    #[serde(rename = "success")]
    Success { msg: String },
    #[serde(rename = "error")]
    ServerError { code: String, msg: String },
    /// Server keep-alive (`{"T":"ping"}`). Carries nothing we must answer
    /// here; decoded so it never trips the unknown-message warning.
    #[serde(rename = "ping")]
    Ping {},
}

// ---------- supervisor 接入 ----------

#[derive(Debug, Clone)]
enum StockAction {
    Subscribe(StockSubscription),
    Unsubscribe(StockSubscription),
}

struct StockProtocol {
    url: String,
    key_id: String,
    secret: String,
}

fn action_json(action: &str, subscription: &StockSubscription) -> String {
    let mut body = serde_json::to_value(subscription).expect("subscription serializes");
    if let Some(obj) = body.as_object_mut() {
        obj.insert("action".into(), serde_json::Value::from(action));
    }
    body.to_string()
}

impl Protocol for StockProtocol {
    type Message = StockMessage;
    type Action = StockAction;
    type Desired = StockSubscription;

    fn url(&self) -> String {
        self.url.clone()
    }

    async fn handshake(&self, write: &mut crate::ws::WsWriter, read: &mut crate::ws::WsReader) -> anyhow::Result<()> {
        let auth = serde_json::json!({"action": "auth", "key": self.key_id, "secret": self.secret});
        write
            .send(Message::text(auth.to_string()))
            .await
            .context("stock stream: send auth")?;
        wait_control(read, "authenticated")
            .await
            .context("stock stream: handshake")
    }

    fn replay(desired: &StockSubscription) -> Vec<String> {
        if desired.is_empty() {
            Vec::new()
        } else {
            vec![action_json("subscribe", desired)]
        }
    }

    fn apply(desired: &mut StockSubscription, action: &StockAction) -> Vec<String> {
        let (wire_action, sub) = match action {
            StockAction::Subscribe(sub) => {
                desired.merge(sub);
                ("subscribe", sub)
            }
            StockAction::Unsubscribe(sub) => {
                desired.unmerge(sub);
                ("unsubscribe", sub)
            }
        };
        vec![action_json(wire_action, sub)]
    }
}

// ---------- handle ----------

#[derive(Debug)]
struct Inner {
    actions: mpsc::Sender<StockAction>,
    messages: broadcast::Sender<StockMessage>,
    status: broadcast::Sender<StreamStatus>,
    close: CancellationToken,
}

/// Live stock data stream for one feed, supervised for auto-reconnect.
///
/// Clone 廉价(共享同一连接);**最后一个句柄 drop 时流自动关闭**。
#[derive(Clone, Debug)]
pub struct StockStream {
    inner: Arc<Inner>,
}

impl StockStream {
    /// Connect to the default stock stream URL for `feed` and run the `auth`
    /// handshake. The first attempt is synchronous and bounded by
    /// [`crate::ws::CONNECT_TIMEOUT`] — a failure returns `Err` (fail-fast);
    /// afterwards the supervisor keeps the connection alive: auto-reconnect
    /// with exponential backoff + subscription replay until
    /// [`close`](Self::close) or the last handle is dropped.
    pub async fn connect(
        feed: Feed,
        key_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> anyhow::Result<Self> {
        Self::connect_with_url(STOCK_STREAM_URL, feed, key_id, secret).await
    }

    /// [`connect`] 的自定义端点版本(自建代理 / 沙箱 / 测试服务)。
    pub async fn connect_with_url(
        url_base: &str,
        feed: Feed,
        key_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let path = feed
            .path()
            .context("stock stream: feed not available on the WebSocket")?;
        let protocol = Arc::new(StockProtocol {
            url: format!("{url_base}/{path}"),
            key_id: key_id.into(),
            secret: secret.into(),
        });

        let first = supervisor::attempt_connect(protocol.as_ref())
            .await
            .context("stock stream: initial connect failed")?;

        let (messages, _) = broadcast::channel(4096);
        let (status, _) = broadcast::channel(16);
        let (actions, action_rx) = mpsc::channel(16);
        let desired = Arc::new(Mutex::new(StockSubscription::default()));
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

    /// Send the `subscribe` action; the server echoes the active channels as
    /// a [`StockMessage::Subscription`]. While disconnected the request is
    /// merged into the desired state and replayed after reconnect.
    pub async fn subscribe(&self, subscription: &StockSubscription) -> anyhow::Result<()> {
        self.send(StockAction::Subscribe(subscription.clone())).await
    }

    /// Send the `unsubscribe` action; the socket stays open. Mirrors
    /// [`subscribe`](Self::subscribe) while disconnected.
    pub async fn unsubscribe(&self, subscription: &StockSubscription) -> anyhow::Result<()> {
        self.send(StockAction::Unsubscribe(subscription.clone())).await
    }

    async fn send(&self, action: StockAction) -> anyhow::Result<()> {
        self.inner
            .actions
            .send(action)
            .await
            .context("stock stream: closed")
    }

    /// Subscribe to every message of the stream.
    pub fn messages(&self) -> broadcast::Receiver<StockMessage> {
        self.inner.messages.subscribe()
    }

    /// 连接状态事件([`StreamStatus`]:Connected / Reconnecting / Closed)。
    pub fn status(&self) -> broadcast::Receiver<StreamStatus> {
        self.inner.status.subscribe()
    }

    /// 显式关闭流(supervisor 停止重连并清理连接任务)。幂等。
    pub fn close(&self) {
        self.inner.close.cancel();
    }
}

impl Drop for StockStream {
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

    fn symbols(sub: &StockSubscription) -> (&[String], &[String]) {
        (&sub.trades, &sub.quotes)
    }

    #[test]
    fn merge_dedups_and_preserves_order() {
        let mut desired = StockSubscription::default();
        desired.merge(&StockSubscription {
            trades: vec!["AAPL".into(), "MSFT".into()],
            quotes: vec!["AAPL".into()],
            ..Default::default()
        });
        desired.merge(&StockSubscription {
            trades: vec!["MSFT".into(), "TSLA".into()],
            ..Default::default()
        });
        assert_eq!(symbols(&desired).0, ["AAPL", "MSFT", "TSLA"]);
        assert_eq!(symbols(&desired).1, ["AAPL"]);
    }

    #[test]
    fn unmerge_removes_and_replay_reflects_desired_state() {
        let mut desired = StockSubscription {
            trades: vec!["AAPL".into(), "MSFT".into()],
            ..Default::default()
        };
        desired.unmerge(&StockSubscription {
            trades: vec!["AAPL".into()],
            ..Default::default()
        });
        assert_eq!(symbols(&desired).0, ["MSFT"]);

        let replay = StockProtocol::replay(&desired);
        assert_eq!(replay.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&replay[0]).unwrap();
        assert_eq!(json["action"], "subscribe");
        assert_eq!(json["trades"], serde_json::json!(["MSFT"]));
        // 未订阅的频道不出现(camelCase + skip_serializing_if)。
        assert!(json.get("quotes").is_none());
    }

    #[test]
    fn replay_is_empty_without_subscriptions() {
        let frames = StockProtocol::replay(&StockSubscription::default());
        assert!(frames.is_empty());
    }

    #[test]
    fn apply_merges_desired_and_emits_wire_action() {
        let mut desired = StockSubscription::default();
        let frames = StockProtocol::apply(
            &mut desired,
            &StockAction::Subscribe(StockSubscription {
                trades: vec!["AAPL".into()],
                ..Default::default()
            }),
        );
        assert_eq!(symbols(&desired).0, ["AAPL"]);
        assert_eq!(frames.len(), 1);
        let json: serde_json::Value = serde_json::from_str(&frames[0]).unwrap();
        assert_eq!(json["action"], "subscribe");

        let frames = StockProtocol::apply(
            &mut desired,
            &StockAction::Unsubscribe(StockSubscription {
                trades: vec!["AAPL".into()],
                ..Default::default()
            }),
        );
        assert!(symbols(&desired).0.is_empty());
        let json: serde_json::Value = serde_json::from_str(&frames[0]).unwrap();
        assert_eq!(json["action"], "unsubscribe");
    }

    /// Server keep-alive frames decode into a dedicated variant instead of
    /// tripping the unknown-message warning — with or without extra fields.
    #[test]
    fn ping_decodes_silently() {
        for text in [r#"[{"T":"ping"}]"#, r#"[{"T":"ping","msg":"hello"}]"#] {
            let msgs: Vec<StockMessage> = serde_json::from_str::<Vec<serde_json::Value>>(text)
                .unwrap()
                .into_iter()
                .map(serde_json::from_value)
                .collect::<Result<_, _>>()
                .unwrap_or_else(|err| panic!("{text} should decode: {err}"));
            assert!(matches!(msgs.as_slice(), [StockMessage::Ping {}]), "{text}");
        }
    }

    /// Trade prints decode with their wire field names.
    #[test]
    fn trade_message_deserializes() {
        let msg: StockMessage = serde_json::from_value(serde_json::json!({
            "T": "t", "S": "AAPL", "i": 6913, "x": "D", "p": 182.13, "s": 10,
            "c": ["@", "F"], "t": "2022-01-03T14:30:00.123Z", "z": "C"
        }))
        .unwrap();
        let StockMessage::Trade(trade) = msg else {
            panic!("expected trade, got {msg:?}");
        };
        assert_eq!(trade.price, 182.13);
        assert_eq!(trade.size, 10);
        assert_eq!(trade.tape.as_deref(), Some("C"));
    }
}
