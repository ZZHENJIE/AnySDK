//! Reference metadata: condition codes and exchange codes
//! (`GET /v2/stocks/meta/conditions/{ticktype}`,
//! `GET /v2/stocks/meta/exchanges`; spec: `HTTP-Condition-Codes.md`,
//! `HTTP-Exchange-Codes.md`).
//!
//! Both responses are plain `code -> description` maps; endpoints are methods
//! on [`Client`]: `condition_codes` and `exchange_codes`.

use std::collections::HashMap;

use crate::Client;
use utils::HttpExecutor;

/// Which condition-code set to fetch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TickType {
    /// Condition codes attached to trades.
    Trades,
    /// Condition codes attached to quotes.
    Quotes,
}

impl TickType {
    fn path_segment(self) -> &'static str {
        match self {
            TickType::Trades => "trades",
            TickType::Quotes => "quotes",
        }
    }
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Condition codes for the given tick type, keyed by code
    /// (e.g. `"@"` → `"Regular Sale"`).
    pub async fn condition_codes(
        &self,
        tick_type: TickType,
    ) -> anyhow::Result<HashMap<String, String>> {
        let path = format!("/v2/stocks/meta/conditions/{}", tick_type.path_segment());
        self.send_json(self.get(&path, &[])?).await
    }

    /// Exchange codes, keyed by code
    /// (e.g. `"N"` → `"New York Stock Exchange"`).
    pub async fn exchange_codes(&self) -> anyhow::Result<HashMap<String, String>> {
        self.send_json(self.get("/v2/stocks/meta/exchanges", &[])?).await
    }
}
