//! Point-in-time market snapshots — `GET /v2/stocks/snapshots` (multi-symbol)
//! and `GET /v2/stocks/{symbol}/snapshot` (single)
//! (spec: `HTTP-Snapshots.md`,
//! `HTTP-Snapshot (single symbol).md`).
//!
//! A snapshot bundles the latest trade, latest quote, latest minute bar,
//! current daily bar and previous daily bar in one response.
//!
//! Endpoints are methods on [`Client`]: `snapshots` (multi) and
//! `snapshot` (single).

use std::collections::HashMap;

use serde::Deserialize;

use crate::{Client, Feed};
use utils::{push_opt, HttpExecutor};

/// Latest quote/trade/bars bundle for one symbol.
#[derive(Debug, Clone, Deserialize)]
pub struct Snapshot {
    /// Current daily bar.
    #[serde(rename = "dailyBar")]
    pub daily_bar: Option<crate::bars::Bar>,
    /// Latest quote.
    #[serde(rename = "latestQuote")]
    pub latest_quote: Option<crate::quotes::Quote>,
    /// Latest trade.
    #[serde(rename = "latestTrade")]
    pub latest_trade: Option<crate::trades::Trade>,
    /// Latest minute bar.
    #[serde(rename = "minuteBar")]
    pub minute_bar: Option<crate::bars::Bar>,
    /// Previous day's daily bar.
    #[serde(rename = "prevDailyBar")]
    pub prev_daily_bar: Option<crate::bars::Bar>,
}

/// Parameters for [`Client::snapshots`] (multi-symbol).
#[derive(Debug, Default, Clone)]
pub struct Params {
    /// Symbols to query (joined with `,`).
    pub symbols: Vec<String>,
    /// Data feed.
    pub feed: Option<Feed>,
    /// ISO-4217 currency (default USD).
    pub currency: Option<String>,
}

impl Params {
    fn query(&self) -> anyhow::Result<Vec<(&'static str, String)>> {
        if self.symbols.is_empty() {
            anyhow::bail!("snapshots: `symbols` is required");
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

/// Parameters for [`Client::snapshot`].
#[derive(Debug, Default, Clone)]
pub struct SingleParams {
    /// Data feed.
    pub feed: Option<Feed>,
    /// ISO-4217 currency (default USD).
    pub currency: Option<String>,
}

impl SingleParams {
    fn query(&self) -> Vec<(&'static str, String)> {
        let mut q = Vec::new();
        for (key, value) in [
            ("feed", &self.feed.as_ref().map(|f| f.as_str().to_string())),
            ("currency", &self.currency),
        ] {
            push_opt(&mut q, key, value);
        }
        q
    }
}

/// Snapshot for one symbol (`GET /v2/stocks/{symbol}/snapshot`).
#[derive(Debug, Clone, Deserialize)]
pub struct SingleSnapshot {
    pub symbol: String,
    pub currency: Option<String>,
    #[serde(flatten)]
    pub snapshot: Snapshot,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Fetch snapshots for several symbols, keyed by symbol
    /// (`GET /v2/stocks/snapshots`; the response body is a bare map).
    pub async fn snapshots(&self, params: &Params) -> anyhow::Result<HashMap<String, Snapshot>> {
        self.send_json(self.get("/v2/stocks/snapshots", &params.query()?)?).await
    }

    /// Fetch the snapshot for one symbol
    /// (`GET /v2/stocks/{symbol}/snapshot`).
    pub async fn snapshot(&self, symbol: &str, params: &SingleParams) -> anyhow::Result<SingleSnapshot> {
        self.send_json(self.get(&format!("/v2/stocks/{symbol}/snapshot"), &params.query())?)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_deserializes_camel_case() {
        let raw = r#"{
            "dailyBar": {"t": "2022-01-03T05:00:00Z", "o": 177.77, "h": 182.88,
                         "l": 177.26, "c": 182.19, "v": 160418504, "n": 449385, "vw": 181.517},
            "latestQuote": {"t": "2022-01-04T15:19:00.1024Z", "ax": "Q", "ap": 182.13, "as": 4,
                            "bx": "P", "bp": 182.12, "bs": 3, "c": ["R"], "z": "C"},
            "latestTrade": {"t": "2022-01-04T15:19:00.1024Z", "x": "D", "p": 182.1,
                            "s": 225, "i": 6913, "c": ["@"], "z": "C"},
            "minuteBar": {"t": "2022-01-04T15:19:00Z", "o": 182.1, "h": 182.1,
                          "l": 182.05, "c": 182.05, "v": 1372, "n": 18, "vw": 182.0777},
            "prevDailyBar": {"t": "2022-01-03T05:00:00Z", "o": 177.77, "h": 182.88,
                             "l": 177.26, "c": 182.19, "v": 160418504, "n": 449385, "vw": 181.517}
        }"#;
        let snapshot: Snapshot = serde_json::from_str(raw).unwrap();
        assert_eq!(snapshot.latest_quote.unwrap().ask_price, 182.13);
        assert_eq!(snapshot.latest_trade.unwrap().price, 182.1);
        assert_eq!(snapshot.minute_bar.unwrap().volume, 1372);
    }

    #[test]
    fn single_snapshot_flattens() {
        let raw = r#"{
            "symbol": "AAPL",
            "currency": "USD",
            "latestTrade": {"t": "2022-01-04T15:19:00.1024Z", "x": "D", "p": 182.1,
                            "s": 225, "i": 6913, "c": ["@"], "z": "C"}
        }"#;
        let single: SingleSnapshot = serde_json::from_str(raw).unwrap();
        assert_eq!(single.symbol, "AAPL");
        assert!(single.snapshot.prev_daily_bar.is_none());
        assert_eq!(single.snapshot.latest_trade.unwrap().price, 182.1);
    }
}
