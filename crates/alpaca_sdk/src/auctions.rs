//! Historical stock auctions — `GET /v2/stocks/auctions` (multi-symbol),
//! `GET /v2/stocks/{symbol}/auctions` (single)
//! (spec: `HTTP-Historical-Auctions.md`,
//! `HTTP-Historical-Auctions(single).md`).
//!
//! Auctions are the opening/closing auction prints (price + exchange +
//! condition) for each trading day, keyed by symbol. Paginated the same way
//! as [`crate::bars`].
//!
//! All endpoints are methods on [`Client`]: `auctions`, `auctions_all`,
//! `auctions_single`, `auctions_single_all`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{Client, Feed};
use utils::{push_opt, HttpExecutor};

/// Parameters for [`Client::auctions`] / [`Client::auctions_all`]
/// (multi-symbol).
#[derive(Debug, Default, Clone)]
pub struct Params {
    /// Symbols to query (joined with `,`).
    pub symbols: Vec<String>,
    /// Inclusive interval start (RFC-3339 or `YYYY-MM-DD`).
    pub start: Option<String>,
    /// Inclusive interval end.
    pub end: Option<String>,
    /// Max data points per page (spec: 1..=10000, default 1000).
    pub limit: Option<u32>,
    /// Pagination token from a previous page.
    pub page_token: Option<String>,
    /// `"asc"` (default) or `"desc"`.
    pub sort: Option<String>,
    /// Symbol as-of date (`YYYY-MM-DD`) or `-` to skip symbol mapping.
    pub asof: Option<String>,
    /// Data feed.
    pub feed: Option<Feed>,
    /// ISO-4217 currency (default USD).
    pub currency: Option<String>,
}

impl Params {
    fn query(&self) -> anyhow::Result<Vec<(&'static str, String)>> {
        if self.symbols.is_empty() {
            anyhow::bail!("auctions: `symbols` is required");
        }
        let mut q: Vec<(&'static str, String)> = vec![("symbols", self.symbols.join(","))];
        for (key, value) in [
            ("start", &self.start),
            ("end", &self.end),
            ("limit", &self.limit.as_ref().map(ToString::to_string)),
            ("page_token", &self.page_token),
            ("sort", &self.sort),
            ("asof", &self.asof),
            ("feed", &self.feed.as_ref().map(|f| f.as_str().to_string())),
            ("currency", &self.currency),
        ] {
            push_opt(&mut q, key, value);
        }
        Ok(q)
    }
}

/// Parameters for [`Client::auctions_single`] / [`Client::auctions_single_all`]
/// (symbol goes in the URL; no `symbols`).
#[derive(Debug, Default, Clone)]
pub struct SingleParams {
    /// Inclusive interval start.
    pub start: Option<String>,
    /// Inclusive interval end.
    pub end: Option<String>,
    /// Max data points per page.
    pub limit: Option<u32>,
    /// Pagination token from a previous page.
    pub page_token: Option<String>,
    /// `"asc"` (default) or `"desc"`.
    pub sort: Option<String>,
    /// Symbol as-of date or `-` to skip symbol mapping.
    pub asof: Option<String>,
    /// Data feed.
    pub feed: Option<Feed>,
    /// ISO-4217 currency (default USD).
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
            ("asof", &self.asof),
            ("feed", &self.feed.as_ref().map(|f| f.as_str().to_string())),
            ("currency", &self.currency),
        ] {
            push_opt(&mut q, key, value);
        }
        q
    }
}

/// One auction print.
#[derive(Debug, Clone, Deserialize)]
pub struct Auction {
    /// Auction timestamp.
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    /// Exchange code (see `v2/stocks/meta/exchanges`).
    #[serde(rename = "x")]
    pub exchange: String,
    /// Auction price.
    #[serde(rename = "p")]
    pub price: f64,
    /// Condition flag marking this print as an auction.
    #[serde(rename = "c")]
    pub condition: String,
}

/// Opening and closing auction prints for one trading day.
#[derive(Debug, Clone, Deserialize)]
pub struct DailyAuctions {
    /// Day the auctions belong to (`YYYY-MM-DD`).
    #[serde(rename = "d")]
    pub date: String,
    /// Opening auctions (every price/exchange/condition triplet shown once,
    /// with its earliest timestamp).
    #[serde(rename = "o", default)]
    pub open: Vec<Auction>,
    /// Closing auctions (same de-duplication rule).
    #[serde(rename = "c", default)]
    pub close: Vec<Auction>,
}

/// Multi-symbol response page, keyed by symbol.
#[derive(Debug, Clone, Deserialize)]
pub struct AuctionsPage {
    #[serde(default)]
    pub auctions: HashMap<String, Vec<DailyAuctions>>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    pub currency: Option<String>,
}

/// Single-symbol response page (auctions as an array, not a map).
#[derive(Debug, Clone, Deserialize)]
pub struct SinglePage {
    pub symbol: String,
    #[serde(default)]
    pub auctions: Vec<DailyAuctions>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    pub currency: Option<String>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Fetch a single page of historical auctions (multi-symbol).
    pub async fn auctions(&self, params: &Params) -> anyhow::Result<AuctionsPage> {
        self.send_json(self.get("/v2/stocks/auctions", &params.query()?)?).await
    }

    /// Fetch auctions for every symbol, following `next_page_token` until
    /// exhausted.
    pub async fn auctions_all(
        &self,
        params: &Params,
    ) -> anyhow::Result<HashMap<String, Vec<DailyAuctions>>> {
        let mut all: HashMap<String, Vec<DailyAuctions>> = HashMap::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.auctions(&p).await?;
            for (symbol, days) in page.auctions {
                all.entry(symbol).or_default().extend(days);
            }
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(all),
            }
        }
    }

    /// Fetch a single page of historical auctions for one symbol
    /// (`GET /v2/stocks/{symbol}/auctions`).
    pub async fn auctions_single(
        &self,
        symbol: &str,
        params: &SingleParams,
    ) -> anyhow::Result<SinglePage> {
        self.send_json(self.get(&format!("/v2/stocks/{symbol}/auctions"), &params.query())?)
            .await
    }

    /// Fetch all historical auctions for one symbol, following pages.
    pub async fn auctions_single_all(
        &self,
        symbol: &str,
        params: &SingleParams,
    ) -> anyhow::Result<Vec<DailyAuctions>> {
        let mut all = Vec::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.auctions_single(symbol, &p).await?;
            all.extend(page.auctions);
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(all),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_deserializes() {
        let raw = r#"{
            "auctions": {
                "AAPL": [
                    {
                        "d": "2022-10-12",
                        "o": [
                            {"t": "2022-10-12T13:30:00.120649216Z", "x": "P", "p": 138.36, "c": "6"}
                        ],
                        "c": [
                            {"t": "2022-10-12T20:00:00.120649216Z", "x": "P", "p": 138.36, "c": "6"}
                        ]
                    }
                ]
            },
            "next_page_token": null,
            "currency": "USD"
        }"#;
        let page: AuctionsPage = serde_json::from_str(raw).unwrap();
        let day = &page.auctions["AAPL"][0];
        assert_eq!(day.date, "2022-10-12");
        assert_eq!(day.close[0].price, 138.36);
        assert_eq!(day.close[0].condition, "6");
    }
}
