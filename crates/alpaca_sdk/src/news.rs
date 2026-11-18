//! News articles — `GET /v1beta1/news`
//! (spec: `News.md`).
//!
//! Latest news across stocks and crypto, newest first by default. Paginated
//! with `next_page_token` like every list endpoint.
//!
//! All endpoints are methods on [`Client`]: `news`, `news_all`.

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::Client;
use utils::{push_opt, HttpExecutor};

/// Parameters for [`Client::news`] / [`Client::news_all`]. Every field is
/// optional; the server defaults to the latest 10 articles, newest first.
#[derive(Debug, Default, Clone)]
pub struct Params {
    /// Inclusive interval start (RFC-3339 or `YYYY-MM-DD`).
    pub start: Option<String>,
    /// Inclusive interval end.
    pub end: Option<String>,
    /// `"desc"` (default) or `"asc"`.
    pub sort: Option<String>,
    /// Symbols to query (joined with `,`; empty = everything).
    pub symbols: Vec<String>,
    /// News items per page (spec: 1..=50, default 10).
    pub limit: Option<u32>,
    /// Include article content when available.
    pub include_content: Option<bool>,
    /// Exclude articles without content.
    pub exclude_contentless: Option<bool>,
    /// Pagination token from a previous page.
    pub page_token: Option<String>,
}

impl Params {
    fn query(&self) -> Vec<(&'static str, String)> {
        let mut q: Vec<(&'static str, String)> = Vec::new();
        if !self.symbols.is_empty() {
            q.push(("symbols", self.symbols.join(",")));
        }
        for (key, value) in [
            ("start", &self.start),
            ("end", &self.end),
            ("sort", &self.sort),
            ("limit", &self.limit.as_ref().map(ToString::to_string)),
            (
                "include_content",
                &self.include_content.map(|v| v.to_string()),
            ),
            (
                "exclude_contentless",
                &self.exclude_contentless.map(|v| v.to_string()),
            ),
            ("page_token", &self.page_token),
        ] {
            push_opt(&mut q, key, value);
        }
        q
    }
}

/// One image attached to a news article.
#[derive(Debug, Clone, Deserialize)]
pub struct NewsImage {
    /// `thumb` / `small` / `large`.
    pub size: String,
    pub url: String,
}

/// One news article.
#[derive(Debug, Clone, Deserialize)]
pub struct NewsArticle {
    pub id: i64,
    pub headline: String,
    pub author: String,
    /// Article creation time.
    #[serde(rename = "created_at")]
    pub created_at: DateTime<Utc>,
    /// Article last-update time.
    #[serde(rename = "updated_at")]
    pub updated_at: DateTime<Utc>,
    /// Summary text (may be the first sentence of [`NewsArticle::content`]).
    pub summary: String,
    /// Full content (may contain HTML).
    pub content: String,
    /// Related images (may be empty).
    #[serde(default)]
    pub images: Vec<NewsImage>,
    /// Related or mentioned symbols.
    #[serde(default)]
    pub symbols: Vec<String>,
    /// Originating source (e.g. `benzinga`).
    pub source: String,
    /// Article URL, when applicable.
    pub url: Option<String>,
}

/// One page of news.
#[derive(Debug, Clone, Deserialize)]
pub struct NewsPage {
    pub news: Vec<NewsArticle>,
    /// Pagination token for the next page.
    #[serde(rename = "next_page_token")]
    pub next_page_token: Option<String>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Fetch a single page of news (`GET /v1beta1/news`).
    pub async fn news(&self, params: &Params) -> anyhow::Result<NewsPage> {
        self.send_json(self.get("/v1beta1/news", &params.query())?)
            .await
    }

    /// Fetch all news matching the parameters, following pages.
    pub async fn news_all(&self, params: &Params) -> anyhow::Result<Vec<NewsArticle>> {
        let mut all = Vec::new();
        let mut page_token = None;
        loop {
            let mut p = params.clone();
            p.page_token.clone_from(&page_token);
            let page = self.news(&p).await?;
            all.extend(page.news);
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

    /// simd-json 等价解析:样本字符串 → 类型化结构体。
    fn parse_raw<T: serde::de::DeserializeOwned>(raw: &str) -> T {
        let mut bytes = raw.as_bytes().to_vec();
        simd_json::from_slice(&mut bytes).unwrap()
    }

    #[test]
    fn page_deserializes() {
        let raw = r#"{
            "news": [
                {
                    "author": "Charles Gross",
                    "content": "<p>This headline-only article is meant to show you why a stock is moving, the most difficult aspect of stock trading....</p>",
                    "created_at": "2021-12-31T11:08:42Z",
                    "headline": "Apple Leader in Phone Sales in China for Second Straight Month in November With 23.6% Share, According to Market Research Data",
                    "id": 24843171,
                    "images": [
                        {"size": "thumb", "url": "https://example.com/thumb.jpg"}
                    ],
                    "source": "benzinga",
                    "summary": "This headline-only article is meant to show you why a stock is moving, the most difficult aspect of stock trading",
                    "symbols": ["AAPL"],
                    "updated_at": "2021-12-31T11:08:43Z",
                    "url": "https://www.benzinga.com/news/21/12/24843171/apple-leader-in-phone-sales-in-china-for-second-straight-month-in-november-with-23-6-share-according"
                }
            ],
            "next_page_token": "MTY0MDk0ODkyMzAwMDAwMDAwMHwyNDg0MzE3MQ=="
        }"#;
        let page: NewsPage = parse_raw(raw);
        assert_eq!(page.news.len(), 1);
        let article = &page.news[0];
        assert_eq!(article.id, 24843171);
        assert_eq!(article.source, "benzinga");
        assert_eq!(article.symbols, vec!["AAPL"]);
        assert_eq!(article.images.len(), 1);
        assert_eq!(article.images[0].size, "thumb");
        assert!(article.url.is_some());
        assert_eq!(
            page.next_page_token.as_deref(),
            Some("MTY0MDk0ODkyMzAwMDAwMDAwMHwyNDg0MzE3MQ==")
        );
    }

    #[test]
    fn page_deserializes_null_url_and_empty_images() {
        let raw = r#"{
            "news": [
                {
                    "author": "a",
                    "content": "c",
                    "created_at": "2021-12-31T11:08:42Z",
                    "headline": "h",
                    "id": 1,
                    "images": [],
                    "source": "s",
                    "summary": "s",
                    "symbols": [],
                    "updated_at": "2021-12-31T11:08:43Z",
                    "url": null
                }
            ],
            "next_page_token": null
        }"#;
        let page: NewsPage = parse_raw(raw);
        let article = &page.news[0];
        assert!(article.url.is_none());
        assert!(article.images.is_empty());
        assert!(page.next_page_token.is_none());
    }

    #[test]
    fn query_encodes_parameters() {
        let params = Params {
            symbols: vec!["AAPL".into(), "TSLA".into()],
            limit: Some(50),
            include_content: Some(true),
            ..Default::default()
        };
        let q = params.query();
        assert!(q.contains(&("symbols", "AAPL,TSLA".to_string())));
        assert!(q.contains(&("limit", "50".to_string())));
        assert!(q.contains(&("include_content", "true".to_string())));
        // Unset options stay absent.
        assert!(!q.iter().any(|(key, _)| *key == "start"));
        assert!(!q.iter().any(|(key, _)| *key == "sort"));
    }
}
