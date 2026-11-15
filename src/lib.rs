//! AnySDK 统一入口 crate。
//!
//! 默认引入核心架构 [`utils`]:`HttpExecutor` / `Method` / `HttpResponse` /
//! `Client` —— 你为自己的 HTTP 客户端实现一次 `HttpExecutor`,即可驱动全部 SDK。
//! 各数据 SDK 通过 feature 按需启用:
//!
//! | feature | 引入的模块 | 说明 |
//! |---|---|---|
//! | (默认) | `anysdk::utils` | 仅核心架构,不拉入任何 SDK |
//! | `alpaca` | `anysdk::alpaca_sdk` | Alpaca 市场数据 HTTP/REST(历史 / 快照 / 新闻) |
//! | `alpaca-stream` | `anysdk::alpaca_stream` | Alpaca WebSocket 实时流(自动重连 + 保活;隐含 `alpaca`) |
//! | `finviz` | `anysdk::finviz_sdk` | Finviz Elite CSV 导出(筛选器 / 日历) |
//! | `full` | 两者 | 全部启用 |
//!
//! ```toml
//! anysdk = { git = "https://github.com/ZZHENJIE/AnySDK", features = ["alpaca-stream"] }
//! ```

// 核心架构:始终可用(默认引入)。
pub use utils;

#[cfg(feature = "alpaca")]
pub use alpaca_sdk;

#[cfg(feature = "alpaca-stream")]
pub use alpaca_stream;

#[cfg(feature = "finviz")]
pub use finviz_sdk;
