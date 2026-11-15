//! Alpaca 市场数据 SDK(data-only;无交易)—— 纯 HTTP/REST。
//!
//! 实时 WebSocket 流在独立的 `alpaca_stream` crate(`Feed` 枚举为本 crate
//! 与其共享)。
//!
//! 不绑定任何 HTTP 客户端:请求只被描述为 [`utils::Method`],由使用方为
//! 自己的客户端类型实现的 [`utils::HttpExecutor`] 执行。
//!
//! 接口范围(以 Alpaca market data 规范为准,全覆盖):
//! bars / quotes / trades / auctions(历史 + latest,多标的 / 单标的)、
//! snapshots、meta(condition / exchange codes)、news(REST)。
//!
//! 本 SDK 只解析数据:响应 JSON 按字段名反序列化为类型化结构体,不做任何
//! 语义转换;时间字段保持 API 原始值(`DateTime<Utc>` 为 serde 反序列化的
//! 直接产物)。
//!
//! 命名约定:`<域>` = multi 单页;`<域>_all` = 自动翻页拉完;`<域>_single(symbol, …)`
//! = 单标的;`latest_<域>` = 多标的最新值;`latest_<域去掉 s>(symbol)` = 单标的最新值。

pub mod auctions;
pub mod bars;
pub mod meta;
pub mod news;
pub mod quotes;
pub mod snapshots;
pub mod trades;

use std::sync::Arc;

use serde::de::DeserializeOwned;
use utils::{HttpExecutor, Method};

/// Default market-data base URL (HTTP).
pub const DATA_URL: &str = "https://data.alpaca.markets";

/// Data feed selector, shared by every HTTP `feed` parameter and the stock
/// WebSocket (the wire value is the same string everywhere:
/// `sip`/`iex`/`otc`/`delayed_sip`/`boats`/`overnight`).
///
/// Note: individual endpoints only accept a subset (e.g. historical bars
/// have no `delayed_sip`/`overnight`); unsupported values are rejected by
/// the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Feed {
    /// SIP (Securities Information Processor) — all US exchanges.
    Sip,
    /// Investors Exchange.
    Iex,
    /// Over-the-counter exchanges.
    Otc,
    /// 15-minute delayed SIP.
    DelayedSip,
    /// Blue Ocean ATS, overnight US trading.
    Boats,
    /// Derived Alpaca overnight feed.
    Overnight,
}

impl Feed {
    /// Wire value used in HTTP query parameters.
    pub fn as_str(self) -> &'static str {
        match self {
            Feed::Sip => "sip",
            Feed::Iex => "iex",
            Feed::Otc => "otc",
            Feed::DelayedSip => "delayed_sip",
            Feed::Boats => "boats",
            Feed::Overnight => "overnight",
        }
    }

    /// WebSocket URL path for the stock stream (news is a fixed URL).
    ///
    /// Returns `None` for feeds the WebSocket does not offer (currently only
    /// [`Feed::Otc`] exists on the historical HTTP endpoints).
    pub fn path(self) -> Option<&'static str> {
        match self {
            Feed::Sip => Some("v2/sip"),
            Feed::Iex => Some("v2/iex"),
            Feed::Otc => None,
            Feed::DelayedSip => Some("v2/delayed_sip"),
            Feed::Boats => Some("v1beta1/boats"),
            Feed::Overnight => Some("v1beta1/overnight"),
        }
    }
}

impl From<Feed> for &'static str {
    fn from(value: Feed) -> Self {
        value.as_str()
    }
}

/// Per-client platform state held by the generic [`utils::Client`].
#[derive(Clone)]
pub struct State {
    pub(crate) data_url: Arc<str>,
    pub(crate) key_id: Arc<str>,
    pub(crate) secret: Arc<str>,
}

/// Authenticated market-data client over any HTTP client + executor.
///
/// `T` is the user's HTTP client type (e.g. `reqwest::Client`),
/// `E` the [`HttpExecutor`] implementation that drives it.
pub struct Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    inner: utils::Client<T, State, E>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Client against the default market-data endpoint.
    pub fn new(
        http_client: T,
        key_id: impl Into<String>,
        secret: impl Into<String>,
        executor: E,
    ) -> Self {
        Self {
            inner: utils::Client::new(
                http_client,
                State {
                    data_url: Arc::from(DATA_URL),
                    key_id: Arc::from(key_id.into().as_str()),
                    secret: Arc::from(secret.into().as_str()),
                },
                executor,
            ),
        }
    }

    /// Set the API key ID.
    pub fn set_key_id(&mut self, key_id: &str) {
        self.inner.state.key_id = Arc::from(key_id);
    }

    /// Set the API secret key.
    pub fn set_secret(&mut self, secret: &str) {
        self.inner.state.secret = Arc::from(secret);
    }

    /// Override the market-data base URL (e.g. the sandbox host).
    pub fn with_data_url(mut self, url: impl Into<String>) -> Self {
        self.inner.state.data_url = Arc::from(url.into().as_str());
        self
    }

    /// Build an authenticated GET request against the data API.
    pub(crate) fn get(
        &self,
        path: &str,
        query: &[(&'static str, String)],
    ) -> anyhow::Result<Method> {
        let url = utils::build_url(&self.inner.state.data_url, path, query);
        Ok(Method::get(url)
            .with_header("APCA-API-KEY-ID", &*self.inner.state.key_id)
            .with_header("APCA-API-SECRET-KEY", &*self.inner.state.secret))
    }

    /// Send a request, require a 2xx and decode the JSON body (non-2xx
    /// becomes an error carrying the API's raw message).
    pub(crate) async fn send_json<R: DeserializeOwned>(&self, request: Method) -> anyhow::Result<R> {
        let response = self.inner.send(request).await?;
        let bytes = utils::ensure_success(response)?;
        utils::parse_json(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_wire_values_match_spec() {
        assert_eq!(Feed::Sip.as_str(), "sip");
        assert_eq!(Feed::Iex.as_str(), "iex");
        assert_eq!(Feed::Otc.as_str(), "otc");
        assert_eq!(Feed::DelayedSip.as_str(), "delayed_sip");
        assert_eq!(Feed::Boats.as_str(), "boats");
        assert_eq!(Feed::Overnight.as_str(), "overnight");
        let wire: &str = Feed::Boats.into();
        assert_eq!(wire, "boats");
    }

    #[test]
    fn feed_websocket_paths() {
        assert_eq!(Feed::Sip.path(), Some("v2/sip"));
        assert_eq!(Feed::Boats.path(), Some("v1beta1/boats"));
        // OTC exists only on the historical HTTP endpoints.
        assert_eq!(Feed::Otc.path(), None);
    }
}
