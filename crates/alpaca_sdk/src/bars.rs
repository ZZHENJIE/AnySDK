//! Historical stock bars — `GET /v2/stocks/bars` (multi-symbol),
//! `GET /v2/stocks/{symbol}/bars` (single) plus the `/latest` variants
//! (spec: `HTTP-Historical-Bars.md`,
//! `HTTP-Historical-Bars(single symbol).md`,
//! `HTTP-Latest-Bars.md`, `HTTP-Latest bar (single symbol).md`).
//!
//! All endpoints are methods on [`Client`]: `bars` (multi page),
//! `bars_all` (auto-pagination, with the `bars_all_with_progress`
//! per-page progress variant), `bars_single` / `bars_single_all`
//! (single-symbol), `latest_bars` / `latest_bar`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{Client, Feed};
use utils::{push_opt, HttpExecutor};

/// Bar aggregation sizes accepted by the API (spec: `[1-59]Min`, `[1-23]Hour`,
/// `1Day`, `1Week`, `[1,2,3,4,6,12]Month` and their short forms).
pub mod timeframe {
    pub const MIN_1: &str = "1Min";
    pub const MIN_5: &str = "5Min";
    pub const MIN_15: &str = "15Min";
    pub const MIN_30: &str = "30Min";
    pub const HOUR_1: &str = "1Hour";
    pub const DAY_1: &str = "1Day";
    pub const WEEK_1: &str = "1Week";
    pub const MONTH_1: &str = "1Month";
}

/// Price adjustments: `raw`, `split`, `dividend`, `spin-off`, `all`
/// (comma-combinable, e.g. `split,spin-off`).
pub mod adjustment {
    pub const RAW: &str = "raw";
    pub const SPLIT: &str = "split";
    pub const DIVIDEND: &str = "dividend";
    pub const SPIN_OFF: &str = "spin-off";
    pub const ALL: &str = "all";
}

/// Parameters for [`Client::bars`] / [`Client::bars_all`] (multi-symbol).
#[derive(Debug, Default, Clone)]
pub struct Params {
    /// Symbols to query (joined with `,`).
    pub symbols: Vec<String>,
    /// Bar aggregation size (see [`timeframe`]); required by the API.
    pub timeframe: String,
    /// Inclusive interval start (RFC-3339 or `YYYY-MM-DD`).
    pub start: Option<String>,
    /// Inclusive interval end.
    pub end: Option<String>,
    /// Max data points per page (spec: 1..=10000, default 1000; applies to
    /// the whole response, not per symbol).
    pub limit: Option<u32>,
    /// Pagination token from a previous page.
    pub page_token: Option<String>,
    /// `"asc"` (default) or `"desc"`.
    pub sort: Option<String>,
    /// Price/volume adjustments (see [`adjustment`]).
    pub adjustment: Option<String>,
    /// Symbol as-of date (`YYYY-MM-DD`) or `-` to skip symbol mapping.
    pub asof: Option<String>,
    /// Data feed.
    pub feed: Option<Feed>,
    /// ISO-4217 currency of all prices (default USD).
    pub currency: Option<String>,
}

impl Params {
    fn query(&self) -> anyhow::Result<Vec<(&'static str, String)>> {
        if self.symbols.is_empty() {
            anyhow::bail!("bars: `symbols` is required");
        }
        if self.timeframe.is_empty() {
            anyhow::bail!("bars: `timeframe` is required");
        }
        let mut q: Vec<(&'static str, String)> =
            vec![("symbols", self.symbols.join(",")), ("timeframe", self.timeframe.clone())];
        for (key, value) in [
            ("start", &self.start),
            ("end", &self.end),
            ("limit", &self.limit.as_ref().map(ToString::to_string)),
            ("page_token", &self.page_token),
            ("sort", &self.sort),
            ("adjustment", &self.adjustment),
            ("asof", &self.asof),
            ("feed", &self.feed.as_ref().map(|f| f.as_str().to_string())),
            ("currency", &self.currency),
        ] {
            push_opt(&mut q, key, value);
        }
        Ok(q)
    }
}

/// Parameters for [`Client::bars_single`] / [`Client::bars_single_all`]
/// (symbol goes in the URL; `timeframe` still required).
#[derive(Debug, Default, Clone)]
pub struct SingleParams {
    /// Bar aggregation size (see [`timeframe`]); required by the API.
    pub timeframe: String,
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
    /// Price/volume adjustments (see [`adjustment`]).
    pub adjustment: Option<String>,
    /// Symbol as-of date or `-` to skip symbol mapping.
    pub asof: Option<String>,
    /// Data feed.
    pub feed: Option<Feed>,
    /// ISO-4217 currency (default USD).
    pub currency: Option<String>,
}

impl SingleParams {
    fn query(&self) -> anyhow::Result<Vec<(&'static str, String)>> {
        if self.timeframe.is_empty() {
            anyhow::bail!("bars: `timeframe` is required");
        }
        let mut q: Vec<(&'static str, String)> = vec![("timeframe", self.timeframe.clone())];
        for (key, value) in [
            ("start", &self.start),
            ("end", &self.end),
            ("limit", &self.limit.as_ref().map(ToString::to_string)),
            ("page_token", &self.page_token),
            ("sort", &self.sort),
            ("adjustment", &self.adjustment),
            ("asof", &self.asof),
            ("feed", &self.feed.as_ref().map(|f| f.as_str().to_string())),
            ("currency", &self.currency),
        ] {
            push_opt(&mut q, key, value);
        }
        Ok(q)
    }
}

/// Parameters for [`Client::latest_bars`] (multi-symbol).
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
            anyhow::bail!("latest bars: `symbols` is required");
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

/// One OHLCV aggregate.
#[derive(Debug, Clone, Deserialize)]
pub struct Bar {
    /// Bar start timestamp (RFC-3339, nanosecond precision).
    #[serde(rename = "t")]
    pub time: DateTime<Utc>,
    #[serde(rename = "o")]
    pub open: f64,
    #[serde(rename = "h")]
    pub high: f64,
    #[serde(rename = "l")]
    pub low: f64,
    #[serde(rename = "c")]
    pub close: f64,
    /// Bar volume.
    #[serde(rename = "v")]
    pub volume: i64,
    /// Trade count in the bar.
    #[serde(rename = "n")]
    pub trade_count: i64,
    /// Volume-weighted average price.
    #[serde(rename = "vw")]
    pub vwap: f64,
}

/// Multi-symbol response page, keyed by symbol. Results are sorted by symbol
/// first, then by bar timestamp — a page may therefore contain only the first
/// symbol(s) when the limit was hit.
#[derive(Debug, Clone, Deserialize)]
pub struct BarsPage {
    #[serde(default)]
    pub bars: HashMap<String, Vec<Bar>>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    pub currency: Option<String>,
}

/// Single-symbol response page.
#[derive(Debug, Clone, Deserialize)]
pub struct SingleBarsPage {
    pub symbol: String,
    #[serde(default)]
    pub bars: Vec<Bar>,
    #[serde(default)]
    pub next_page_token: Option<String>,
    pub currency: Option<String>,
}

/// Latest bar for several symbols, one bar per symbol
/// (`GET /v2/stocks/bars/latest`).
#[derive(Debug, Clone, Deserialize)]
pub struct LatestBars {
    #[serde(default)]
    pub bars: HashMap<String, Bar>,
    pub currency: Option<String>,
}

/// Latest bar for one symbol (`GET /v2/stocks/{symbol}/bars/latest`).
#[derive(Debug, Clone, Deserialize)]
pub struct LatestBar {
    pub symbol: String,
    #[serde(rename = "bar")]
    pub bar: Bar,
    pub currency: Option<String>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Fetch a single page of historical bars (multi-symbol).
    pub async fn bars(&self, params: &Params) -> anyhow::Result<BarsPage> {
        self.send_json(self.get("/v2/stocks/bars", &params.query()?)?).await
    }

    /// Fetch bars for every symbol, following `next_page_token` until
    /// exhausted.
    pub async fn bars_all(&self, params: &Params) -> anyhow::Result<HashMap<String, Vec<Bar>>> {
        self.bars_all_with_progress(params, |_| {}).await
    }

    /// [`bars_all`](Self::bars_all) 的带进度版本:每拉完一页,回调累计已拉取的 bar 条数
    /// (relay 用它给客户端报翻页进度)。
    pub async fn bars_all_with_progress(
        &self,
        params: &Params,
        mut on_page: impl FnMut(usize),
    ) -> anyhow::Result<HashMap<String, Vec<Bar>>> {
        let mut all: HashMap<String, Vec<Bar>> = HashMap::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.bars(&p).await?;
            let mut fetched = all.values().map(Vec::len).sum::<usize>();
            for (symbol, bars) in page.bars {
                fetched += bars.len();
                all.entry(symbol).or_default().extend(bars);
            }
            on_page(fetched);
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(all),
            }
        }
    }

    /// Fetch a single page of historical bars for one symbol
    /// (`GET /v2/stocks/{symbol}/bars`).
    pub async fn bars_single(
        &self,
        symbol: &str,
        params: &SingleParams,
    ) -> anyhow::Result<SingleBarsPage> {
        self.send_json(self.get(&format!("/v2/stocks/{symbol}/bars"), &params.query()?)?)
            .await
    }

    /// Fetch all historical bars for one symbol, following pages.
    pub async fn bars_single_all(
        &self,
        symbol: &str,
        params: &SingleParams,
    ) -> anyhow::Result<Vec<Bar>> {
        let mut all = Vec::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.bars_single(symbol, &p).await?;
            all.extend(page.bars);
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => return Ok(all),
            }
        }
    }

    /// Fetch the latest bar for several symbols, one bar per symbol
    /// (`GET /v2/stocks/bars/latest`).
    pub async fn latest_bars(&self, params: &LatestParams) -> anyhow::Result<LatestBars> {
        self.send_json(self.get("/v2/stocks/bars/latest", &params.query()?)?).await
    }

    /// Fetch the latest bar for one symbol
    /// (`GET /v2/stocks/{symbol}/bars/latest`).
    pub async fn latest_bar(&self, symbol: &str) -> anyhow::Result<LatestBar> {
        self.send_json(self.get(&format!("/v2/stocks/{symbol}/bars/latest"), &[])?).await
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
    fn page_deserializes() {
        let raw = r#"{
            "bars": {
                "AAPL": [
                    {"t": "2022-01-03T09:00:00Z", "o": 178.26, "h": 178.34,
                     "l": 177.76, "c": 178.08, "v": 60937, "n": 1727, "vw": 177.954244}
                ]
            },
            "next_page_token": "APAPLAA*AMDMAA==",
            "currency": "USD"
        }"#;
        let page: BarsPage = parse_raw(raw);
        let bar = &page.bars["AAPL"][0];
        assert_eq!(bar.open, 178.26);
        assert_eq!(bar.close, 178.08);
        assert_eq!(bar.volume, 60937);
        assert_eq!(page.next_page_token.as_deref(), Some("APAPLAA*AMDMAA=="));
    }

    #[test]
    fn latest_bar_deserializes() {
        let raw = r#"{
            "symbol": "AAPL",
            "bar": {"t": "2022-01-03T09:00:00Z", "o": 178.26, "h": 178.34,
                    "l": 177.76, "c": 178.08, "v": 60937, "n": 1727, "vw": 177.954244},
            "currency": "USD"
        }"#;
        let latest: LatestBar = parse_raw(raw);
        assert_eq!(latest.symbol, "AAPL");
        assert_eq!(latest.bar.high, 178.34);
    }

    /// 翻页进度:mock 执行器按序吐两页,进度回调应报「每页拉完后的累计条数」。
    #[test]
    fn bars_all_with_progress_reports_cumulative_counts() {
        use std::sync::{Arc, Mutex};

        use utils::HttpResponse;

        struct MockExecutor;

        /// 依次返回预置页的假服务端(作为"HTTP 客户端"类型注入)。
        struct PageServer {
            pages: Mutex<Vec<&'static str>>,
        }

        impl HttpExecutor<Arc<PageServer>> for MockExecutor {
            #[allow(clippy::unused_async)]
            async fn execute(
                &self,
                client: &Arc<PageServer>,
                _request: &utils::Method,
            ) -> anyhow::Result<HttpResponse> {
                let page = client.pages.lock().unwrap().remove(0);
                Ok(HttpResponse {
                    status: 200,
                    body: page.as_bytes().to_vec(),
                })
            }
        }

        let server = Arc::new(PageServer {
            pages: Mutex::new(vec![
                r#"{"bars":{"AAPL":[{"t":"2022-01-03T09:00:00Z","o":178.26,"h":178.34,"l":177.76,"c":178.08,"v":60937,"n":1727,"vw":177.95}]},"next_page_token":"P2"}"#,
                r#"{"bars":{"AAPL":[{"t":"2022-01-04T09:00:00Z","o":178.0,"h":178.1,"l":177.9,"c":178.05,"v":100,"n":2,"vw":178.0}],"MSFT":[{"t":"2022-01-04T09:00:00Z","o":335.0,"h":335.2,"l":334.8,"c":335.1,"v":50,"n":1,"vw":335.0}]}}"#,
            ]),
        });
        let client = Client::new(server, "key", "secret", MockExecutor);

        let progress = Mutex::new(Vec::new());
        let all = futures::executor::block_on(client.bars_all_with_progress(
            &Params {
                symbols: vec!["AAPL".into(), "MSFT".into()],
                timeframe: timeframe::DAY_1.into(),
                ..Default::default()
            },
            |fetched| progress.lock().unwrap().push(fetched),
        ))
        .unwrap();

        assert_eq!(progress.into_inner().unwrap(), [1, 3]);
        assert_eq!(all["AAPL"].len(), 2);
        assert_eq!(all["MSFT"].len(), 1);
    }
}
