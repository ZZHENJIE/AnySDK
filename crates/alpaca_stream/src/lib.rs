//! Alpaca 实时 WebSocket 流:自愈式长连接。
//!
//! ## 架构
//!
//! ```text
//! StreamHandle(Clone 廉价,最后一个 drop 自动关闭)
//! ├── actions_tx   → mpsc   → supervisor(不直连 socket)
//! ├── messages()   → broadcast(4096)   → StockMessage / NewsMessage
//! ├── status()     → broadcast(16)     → StreamStatus 事件
//! └── close()      → CancellationToken(显式关闭,幂等)
//! ```
//!
//! 每条流一个 supervisor 任务:连接(TCP/TLS/升级 + 握手整体 ≤
//! [`ws::CONNECT_TIMEOUT`])→ 重放期望订阅 → `Connected`;断线或下行静默
//! ([`ws::STALE_AFTER`])后按指数退避([`ws::PING_INTERVAL`] 主动 ping 保活,
//! 1s 起步封顶 60s)无限重连,重连成功自动重放订阅并清零退避。
//!
//! ## 行为约定
//!
//! - **首连同步失败即 `Err`**(fail-fast);之后断线由 supervisor 在后台自愈。
//! - **断线期间的 subscribe/unsubscribe 只更新期望状态**,重连后自动重放。
//! - [`StreamStatus::Reconnecting`] 携带第几次重试与本次退避时长,GUI 可展示。
//! - [`StockStream`] / [`NewsStream`] 的 `Clone` 共享同一连接;最后一个句柄
//!   drop 等价于 [`close`](StockStream::close)。

pub mod news;
pub mod stocks;
mod status;
mod supervisor;
mod ws;

pub use alpaca_sdk::Feed;
pub use news::{NewsMessage, NewsStream};
pub use status::StreamStatus;
pub use stocks::{StockMessage, StockStream, StockSubscription};
pub use ws::{CONNECT_TIMEOUT, PING_INTERVAL, STALE_AFTER};

/// 股票流默认 base URL(`StockStream::connect` 使用)。
pub use stocks::STOCK_STREAM_URL;
/// 新闻流默认 URL(`NewsStream::connect` 使用)。
pub use news::NEWS_STREAM_URL;
