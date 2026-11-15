//! Historical stock quotes — `GET /v2/stocks/quotes` (multi-symbol),
//! `GET /v2/stocks/{symbol}/quotes` (single) plus the `/latest` variants
//! (spec: `HTTP-Historical-Quotes.md`,
//! `HTTP-Historical quotes (single symbol).md`,
//! `HTTP-Latest-Quotes.md`, `HTTP-Latest quote (single symbol).md`).
//!
//! All endpoints are methods on [`Client`]: `quotes`, `quotes_all`,
//! `quotes_single`, `quotes_single_all`, `latest_quotes`, `latest_quote`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{Client, Feed};
use utils::{push_opt, HttpExecutor};

/// A top-of-book quote: best bid and ask.
#[derive(Debug, Clone, Deserialize)]
pub struct Quote {
    /// Quote timestamp (RFC-3339, nanosecond precision).
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    /// Ask exchange code.
    #[serde(rename = "ax")]
    pub ask_exchange: String,
    #[serde(rename = "ap")]
    pub ask_price: f64,
    /// Ask size in round lots.
    #[serde(rename = "as")]
    pub ask_size: i64,
    /// Bid exchange code.
    #[serde(rename = "bx")]
    pub bid_exchange: String,
    #[serde(rename = "bp")]
    pub bid_price: f64,
    /// Bid size in round lots.
    #[serde(rename = "bs")]
    pub bid_size: i64,
    /// Quote conditions.
    #[serde(rename = "c", default)]
    pub conditions: Vec<String>,
    /// Tape (`A`/`B`/`C`/`N`/`O`).
    #[serde(rename = "z")]
    pub tape: Option<String>,
}

/// Parameters for [`Client::quotes`] / [`Client::quotes_all`] (multi-symbol).
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
            anyhow::bail!("quotes: `symbols` is required");
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

/// Parameters for [`Client::quotes_single`] / [`Client::quotes_single_all`]
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

/// Parameters for [`Client::latest_quotes`] (multi-symbol).
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
            anyhow::bail!("latest quotes: `symbols` is required");
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
pub struct QuotesPage {
    #[serde(default)]
    pub quotes: HashMap<String, Vec<Quote>>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    pub currency: Option<String>,
}

/// Single-symbol response page.
#[derive(Debug, Clone, Deserialize)]
pub struct SingleQuotesPage {
    pub symbol: String,
    #[serde(default)]
    pub quotes: Vec<Quote>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    pub currency: Option<String>,
}

/// Latest quotes for several symbols (`GET /v2/stocks/quotes/latest`).
#[derive(Debug, Clone, Deserialize)]
pub struct LatestQuotes {
    #[serde(default)]
    pub quotes: HashMap<String, Quote>,
    pub currency: Option<String>,
}

/// Latest quote for one symbol (`GET /v2/stocks/{symbol}/quotes/latest`).
#[derive(Debug, Clone, Deserialize)]
pub struct LatestQuote {
    pub symbol: String,
    #[serde(rename = "quote")]
    pub quote: Quote,
    pub currency: Option<String>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Fetch a single page of historical quotes (multi-symbol).
    pub async fn quotes(&self, params: &Params) -> anyhow::Result<QuotesPage> {
        self.send_json(self.get("/v2/stocks/quotes", &params.query()?)?).await
    }

    /// Fetch quotes for every symbol, following `next_page_token` until
    /// exhausted.
    pub async fn quotes_all(&self, params: &Params) -> anyhow::Result<HashMap<String, Vec<Quote>>> {
        let mut all: HashMap<String, Vec<Quote>> = HashMap::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.quotes(&p).await?;
            for (symbol, quotes) in page.quotes {
                all.entry(symbol).or_default().extend(quotes);
            }
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(all),
            }
        }
    }

    /// Fetch a single page of historical quotes for one symbol.
    pub async fn quotes_single(
        &self,
        symbol: &str,
        params: &SingleParams,
    ) -> anyhow::Result<SingleQuotesPage> {
        self.send_json(self.get(&format!("/v2/stocks/{symbol}/quotes"), &params.query())?)
            .await
    }

    /// Fetch all historical quotes for one symbol, following pages.
    pub async fn quotes_single_all(
        &self,
        symbol: &str,
        params: &SingleParams,
    ) -> anyhow::Result<Vec<Quote>> {
        let mut all = Vec::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.quotes_single(symbol, &p).await?;
            all.extend(page.quotes);
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(all),
            }
        }
    }

    /// Fetch the latest quote for several symbols.
    pub async fn latest_quotes(&self, params: &LatestParams) -> anyhow::Result<LatestQuotes> {
        self.send_json(self.get("/v2/stocks/quotes/latest", &params.query()?)?).await
    }

    /// Fetch the latest quote for one symbol.
    pub async fn latest_quote(&self, symbol: &str) -> anyhow::Result<LatestQuote> {
        self.send_json(self.get(&format!("/v2/stocks/{symbol}/quotes/latest"), &[])?).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_page_deserializes() {
        let raw = r#"{
            "quotes": {
                "AAPL": [
                    {"t": "2022-01-03T14:30:00.123Z", "ax": "Q", "ap": 182.13, "as": 4,
                     "bx": "P", "bp": 182.12, "bs": 3, "c": ["R"], "z": "C"}
                ]
            },
            "next_page_token": null,
            "currency": "USD"
        }"#;
        let page: QuotesPage = serde_json::from_str(raw).unwrap();
        let quote = &page.quotes["AAPL"][0];
        assert_eq!(quote.ask_price, 182.13);
        assert_eq!(quote.bid_size, 3);
        assert_eq!(quote.conditions, vec!["R"]);
    }

    #[test]
    fn latest_quote_deserializes() {
        let raw = r#"{
            "symbol": "AAPL",
            "quote": {"t": "2022-01-03T14:30:00.123Z", "ax": "Q", "ap": 182.13, "as": 4,
                      "bx": "P", "bp": 182.12, "bs": 3, "c": ["R"], "z": "C"},
            "currency": "USD"
        }"#;
        let latest: LatestQuote = serde_json::from_str(raw).unwrap();
        assert_eq!(latest.symbol, "AAPL");
        assert_eq!(latest.quote.bid_price, 182.12);
    }
}
