//! Historical stock trades — `GET /v2/stocks/trades` (multi-symbol),
//! `GET /v2/stocks/{symbol}/trades` (single) plus the `/latest` variants
//! (spec: `HTTP-Historical-Trades.md`,
//! `HTTP-Historical trades (single symbol).md`,
//! `HTTP-Latest-Trades.md`, `HTTP-Latest trade (single symbol).md`).
//!
//! All endpoints are methods on [`Client`]: `trades`, `trades_all`,
//! `trades_single`, `trades_single_all`, `latest_trades`, `latest_trade`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{Client, Feed};
use utils::{push_opt, HttpExecutor};

/// A trade print.
#[derive(Debug, Clone, Deserialize)]
pub struct Trade {
    /// Trade timestamp (RFC-3339, nanosecond precision).
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    /// Exchange code where the trade occurred.
    #[serde(rename = "x")]
    pub exchange: String,
    #[serde(rename = "p")]
    pub price: f64,
    #[serde(rename = "s")]
    pub size: i64,
    /// Trade ID.
    #[serde(rename = "i")]
    pub id: i64,
    /// Trade conditions.
    #[serde(rename = "c", default)]
    pub conditions: Vec<String>,
    /// Tape (`A`/`B`/`C`/`N`/`O`).
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// Parameters for [`Client::trades`] / [`Client::trades_all`] (multi-symbol).
#[derive(Debug, Default, Clone)]
pub struct Params {
    /// Symbols to query (joined with `,`).
    pub symbols: Vec<String>,
    /// Inclusive interval start (RFC-3339 or `YYYY-MM-DD`).
    pub start: Option<String>,
    /// Inclusive interval end.
    pub end: Option<String>,
    /// Max data points per page (default 1000, max 10000).
    pub limit: Option<u32>,
    /// Pagination token from a previous page.
    pub page_token: Option<String>,
    /// `"asc"` (default) or `"desc"`.
    pub sort: Option<String>,
    /// Data feed.
    pub feed: Option<Feed>,
    /// ISO-4217 currency (default USD).
    pub currency: Option<String>,
    /// Restrict to one tape (`A`/`B`/`C`/`N`/`O`).
    pub tape: Option<String>,
}

impl Params {
    fn query(&self) -> anyhow::Result<Vec<(&'static str, String)>> {
        if self.symbols.is_empty() {
            anyhow::bail!("trades: `symbols` is required");
        }
        let mut q: Vec<(&'static str, String)> = vec![("symbols", self.symbols.join(","))];
        for (key, value) in [
            ("start", &self.start),
            ("end", &self.end),
            ("limit", &self.limit.as_ref().map(ToString::to_string)),
            ("page_token", &self.page_token),
            ("sort", &self.sort),
            ("feed", &self.feed.as_ref().map(|f| f.as_str().to_string())),
            ("currency", &self.currency),
            ("tape", &self.tape),
        ] {
            push_opt(&mut q, key, value);
        }
        Ok(q)
    }
}

/// Parameters for [`Client::trades_single`] / [`Client::trades_single_all`]
/// (symbol goes in the URL; no `symbols`/`tape` on this endpoint).
#[derive(Debug, Default, Clone)]
pub struct SingleParams {
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<u32>,
    pub page_token: Option<String>,
    pub sort: Option<String>,
    pub feed: Option<Feed>,
    pub currency: Option<String>,
}

impl SingleParams {
    fn query(&self) -> Vec<(&'static str, String)> {
        let mut q = Vec::new();
        for (key, value) in [
            ("start", &self.start),
            ("end", &self.end),
            ("limit", &self.limit.as_ref().map(ToString::to_string)),
            ("page_token", &self.page_token),
            ("sort", &self.sort),
            ("feed", &self.feed.as_ref().map(|f| f.as_str().to_string())),
            ("currency", &self.currency),
        ] {
            push_opt(&mut q, key, value);
        }
        q
    }
}

/// Parameters for [`Client::latest_trades`] (multi-symbol).
#[derive(Debug, Default, Clone)]
pub struct LatestParams {
    /// Symbols to query.
    pub symbols: Vec<String>,
    /// Data feed.
    pub feed: Option<Feed>,
    /// ISO-4217 currency (default USD).
    pub currency: Option<String>,
}

impl LatestParams {
    fn query(&self) -> anyhow::Result<Vec<(&'static str, String)>> {
        if self.symbols.is_empty() {
            anyhow::bail!("latest trades: `symbols` is required");
        }
        let mut q: Vec<(&'static str, String)> = vec![("symbols", self.symbols.join(","))];
        for (key, value) in [
            ("feed", &self.feed.as_ref().map(|f| f.as_str().to_string())),
            ("currency", &self.currency),
        ] {
            push_opt(&mut q, key, value);
        }
        Ok(q)
    }
}

/// Multi-symbol response page, keyed by symbol.
#[derive(Debug, Clone, Deserialize)]
pub struct TradesPage {
    #[serde(default)]
    pub trades: HashMap<String, Vec<Trade>>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    pub currency: Option<String>,
}

/// Single-symbol response page.
#[derive(Debug, Clone, Deserialize)]
pub struct SingleTradesPage {
    pub symbol: String,
    #[serde(default)]
    pub trades: Vec<Trade>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    pub currency: Option<String>,
}

/// Latest trades for several symbols (`GET /v2/stocks/trades/latest`).
#[derive(Debug, Clone, Deserialize)]
pub struct LatestTrades {
    #[serde(default)]
    pub trades: HashMap<String, Trade>,
    pub currency: Option<String>,
}

/// Latest trade for one symbol (`GET /v2/stocks/{symbol}/trades/latest`).
#[derive(Debug, Clone, Deserialize)]
pub struct LatestTrade {
    pub symbol: String,
    #[serde(rename = "trade")]
    pub trade: Trade,
    pub currency: Option<String>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Fetch a single page of historical trades (multi-symbol).
    pub async fn trades(&self, params: &Params) -> anyhow::Result<TradesPage> {
        self.send_json(self.get("/v2/stocks/trades", &params.query()?)?).await
    }

    /// Fetch trades for every symbol, following `next_page_token` until
    /// exhausted.
    pub async fn trades_all(&self, params: &Params) -> anyhow::Result<HashMap<String, Vec<Trade>>> {
        let mut all: HashMap<String, Vec<Trade>> = HashMap::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.trades(&p).await?;
            for (symbol, trades) in page.trades {
                all.entry(symbol).or_default().extend(trades);
            }
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(all),
            }
        }
    }

    /// Fetch a single page of historical trades for one symbol.
    pub async fn trades_single(
        &self,
        symbol: &str,
        params: &SingleParams,
    ) -> anyhow::Result<SingleTradesPage> {
        self.send_json(self.get(&format!("/v2/stocks/{symbol}/trades"), &params.query())?)
            .await
    }

    /// Fetch all historical trades for one symbol, following pages.
    pub async fn trades_single_all(
        &self,
        symbol: &str,
        params: &SingleParams,
    ) -> anyhow::Result<Vec<Trade>> {
        let mut all = Vec::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.trades_single(symbol, &p).await?;
            all.extend(page.trades);
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(all),
            }
        }
    }

    /// Fetch the latest trade for several symbols.
    pub async fn latest_trades(&self, params: &LatestParams) -> anyhow::Result<LatestTrades> {
        self.send_json(self.get("/v2/stocks/trades/latest", &params.query()?)?).await
    }

    /// Fetch the latest trade for one symbol.
    pub async fn latest_trade(&self, symbol: &str) -> anyhow::Result<LatestTrade> {
        self.send_json(self.get(&format!("/v2/stocks/{symbol}/trades/latest"), &[])?).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// simd-json 等价解析:样本字符串 → 类型化结构体。
    fn parse_raw<T: serde::de::DeserializeOwned>(raw: &str) -> T {
        let mut bytes = raw.as_bytes().to_vec();
        simd_json::from_slice(&mut bytes).unwrap()
    }

    #[test]
    fn trades_page_deserializes() {
        let raw = r#"{
            "trades": {
                "AAPL": [
                    {"t": "2022-01-03T14:30:00.123Z", "x": "D", "p": 182.13,
                     "s": 10, "i": 6913, "c": ["@", "F", "T", "I"], "z": "C"}
                ]
            },
            "next_page_token": "TkVYVA=="
        }"#;
        let page: TradesPage = parse_raw(raw);
        let trade = &page.trades["AAPL"][0];
        assert_eq!(trade.price, 182.13);
        assert_eq!(trade.id, 6913);
        assert_eq!(trade.exchange, "D");
        assert_eq!(page.next_page_token.as_deref(), Some("TkVYVA=="));
    }

    #[test]
    fn latest_trade_deserializes() {
        let raw = r#"{
            "symbol": "AAPL",
            "trade": {"t": "2022-01-03T14:30:00.123Z", "x": "D", "p": 182.13,
                      "s": 10, "i": 6913, "c": ["@"], "z": "C"},
            "currency": "USD"
        }"#;
        let latest: LatestTrade = parse_raw(raw);
        assert_eq!(latest.symbol, "AAPL");
        assert_eq!(latest.trade.size, 10);
    }
}
