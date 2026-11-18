//! Assets — `GET /v2/assets` (spec: `Assets.md`).
//!
//! The tradeable instrument list (`us_equity` / `crypto`). The server returns
//! the full array in one response — no pagination. Query filters: `status` and
//! `asset_class` (e.g. `status=active&asset_class=us_equity`).
//!
//! All endpoints are methods on [`Client`]: `assets`.

use serde::Deserialize;

use crate::Client;
use utils::{push_opt, HttpExecutor};

/// Parameters for [`Client::assets`]. Both fields are optional; unset means
/// the server default (`status=active`, all asset classes).
#[derive(Debug, Default, Clone)]
pub struct Params {
    /// `active` / `inactive`.
    pub status: Option<String>,
    /// `us_equity` / `crypto`.
    pub asset_class: Option<String>,
}

impl Params {
    fn query(&self) -> Vec<(&'static str, String)> {
        let mut q: Vec<(&'static str, String)> = Vec::new();
        for (key, value) in [("status", &self.status), ("asset_class", &self.asset_class)] {
            push_opt(&mut q, key, value);
        }
        q
    }
}

/// One tradeable asset.
#[derive(Debug, Clone, Deserialize)]
pub struct Asset {
    /// Stable UUID (`904837e3-3b76-47ec-b432-046b6d147448`).
    pub id: String,
    /// `us_equity` / `crypto`.
    pub class: String,
    /// Primary exchange (e.g. `NASDAQ`).
    pub exchange: String,
    pub symbol: String,
    /// Full name (e.g. `Apple Inc. Common Stock`).
    pub name: String,
    /// `active` / `inactive`.
    pub status: String,
    pub tradable: bool,
    #[serde(default)]
    pub marginable: Option<bool>,
    #[serde(default)]
    pub shortable: Option<bool>,
    #[serde(default)]
    pub easy_to_borrow: Option<bool>,
    #[serde(default)]
    pub fractionable: Option<bool>,
    /// Crypto only; the API encodes decimals as JSON strings.
    #[serde(default)]
    pub min_order_size: Option<String>,
    /// Crypto only; the API encodes decimals as JSON strings.
    #[serde(default)]
    pub min_trade_increment: Option<String>,
    /// Crypto only; the API encodes decimals as JSON strings.
    #[serde(default)]
    pub price_increment: Option<String>,
    /// Extra tags (e.g. `pdt_no`).
    #[serde(default)]
    pub attributes: Vec<String>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Fetch the asset list (`GET /v2/assets`). The server returns the full
    /// array in a single response — no pagination.
    pub async fn assets(&self, params: &Params) -> anyhow::Result<Vec<Asset>> {
        self.send_json(self.get("/v2/assets", &params.query())?).await
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
    fn assets_deserializes() {
        let raw = r#"[
            {
                "id": "904837e3-3b76-47ec-b432-046b6d147448",
                "class": "us_equity",
                "exchange": "NASDAQ",
                "symbol": "AAPL",
                "name": "Apple Inc. Common Stock",
                "status": "active",
                "tradable": true,
                "marginable": true,
                "shortable": true,
                "easy_to_borrow": true,
                "fractionable": true,
                "attributes": ["pdt_no"]
            },
            {
                "id": "63f22d59-7F2D-4E7c-A0E4-02CBEE3094B8",
                "class": "crypto",
                "exchange": "CRYPTO",
                "symbol": "BTCUSD",
                "name": "Bitcoin / U.S. Dollar",
                "status": "active",
                "tradable": true,
                "min_order_size": "0.0001",
                "min_trade_increment": "0.00000001",
                "price_increment": "0.01"
            }
        ]"#;
        let assets: Vec<Asset> = parse_raw(raw);
        assert_eq!(assets.len(), 2);

        let aapl = &assets[0];
        assert_eq!(aapl.id, "904837e3-3b76-47ec-b432-046b6d147448");
        assert_eq!(aapl.symbol, "AAPL");
        assert_eq!(aapl.name, "Apple Inc. Common Stock");
        assert_eq!(aapl.class, "us_equity");
        assert_eq!(aapl.exchange, "NASDAQ");
        assert_eq!(aapl.status, "active");
        assert!(aapl.tradable);
        assert_eq!(aapl.marginable, Some(true));
        assert_eq!(aapl.attributes, vec!["pdt_no"]);

        // crypto 资产:小数字段为字符串;us_equity 专有字段缺省为 None。
        let btc = &assets[1];
        assert_eq!(btc.class, "crypto");
        assert_eq!(btc.price_increment.as_deref(), Some("0.01"));
        assert!(btc.marginable.is_none());
        assert!(btc.attributes.is_empty());
    }

    #[test]
    fn query_encodes_parameters() {
        let q = Params {
            status: Some("active".into()),
            asset_class: Some("us_equity".into()),
        }
        .query();
        assert!(q.contains(&("status", "active".to_string())));
        assert!(q.contains(&("asset_class", "us_equity".to_string())));

        // 未设置的参数不出现。
        assert!(Params::default().query().is_empty());
    }

    /// mock 执行器返回样例 JSON 数组 → 经 `Client::assets` 全链路断言关键字段
    /// (URL 与 query 拼装 + 2xx 判定 + simd-json 反序列化)。
    #[test]
    fn assets_via_mock_executor() {
        use std::sync::{Arc, Mutex};

        use utils::HttpResponse;

        const SAMPLE: &str = r#"[
            {
                "id": "904837e3-3b76-47ec-b432-046b6d147448",
                "class": "us_equity",
                "exchange": "NASDAQ",
                "symbol": "AAPL",
                "name": "Apple Inc. Common Stock",
                "status": "active",
                "tradable": true
            }
        ]"#;

        struct MockExecutor;

        /// 假服务端(作为"HTTP 客户端"类型注入),记录收到的请求 URL。
        struct FakeServer {
            urls: Mutex<Vec<String>>,
        }

        impl HttpExecutor<Arc<FakeServer>> for MockExecutor {
            #[allow(clippy::unused_async)]
            async fn execute(
                &self,
                client: &Arc<FakeServer>,
                request: &utils::Method,
            ) -> anyhow::Result<HttpResponse> {
                let utils::Method::Get(request) = request else {
                    panic!("assets: expected a GET request");
                };
                client.urls.lock().unwrap().push(request.url.clone());
                Ok(HttpResponse {
                    status: 200,
                    body: SAMPLE.as_bytes().to_vec(),
                })
            }
        }

        let server = Arc::new(FakeServer {
            urls: Mutex::new(Vec::new()),
        });
        let client = Client::new(server.clone(), "key", "secret", MockExecutor);

        let assets = futures::executor::block_on(client.assets(&Params {
            status: Some("active".into()),
            asset_class: Some("us_equity".into()),
        }))
        .unwrap();

        // URL:请求打到 /v2/assets,query 带 status + asset_class。
        assert_eq!(
            server.urls.lock().unwrap().as_slice(),
            ["https://data.alpaca.markets/v2/assets?status=active&asset_class=us_equity"]
        );
        // 响应:样例数组解码为 Vec<Asset>,关键字段就位。
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].symbol, "AAPL");
        assert_eq!(assets[0].class, "us_equity");
        assert_eq!(assets[0].status, "active");
        assert!(assets[0].tradable);
    }
}
