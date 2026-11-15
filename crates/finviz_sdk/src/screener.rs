use crate::Client;
use utils::HttpExecutor;
use serde::{Deserialize, Serialize};

/// The Finviz Elite export columns, by the index their `c=` parameter
/// takes: `(index, name)`. The UI's column picker is built from this.
pub const EXPORT_COLUMNS: &[(u16, &str)] = &[
    (0, "No."),
    (1, "Ticker"),
    (2, "Company"),
    (3, "Sector"),
    (4, "Industry"),
    (5, "Country"),
    (6, "Market Cap"),
    (7, "P/E"),
    (8, "Forward P/E"),
    (9, "PEG"),
    (10, "P/S"),
    (11, "P/B"),
    (12, "P/Cash"),
    (13, "P/Free Cash Flow"),
    (14, "Dividend Yield"),
    (15, "Payout Ratio"),
    (16, "EPS (ttm)"),
    (17, "EPS Growth This Year"),
    (18, "EPS Growth Next Year"),
    (19, "EPS Growth Past 5 Years"),
    (20, "EPS Growth Next 5 Years"),
    (21, "Sales Growth Past 5 Years"),
    (22, "EPS Growth Quarter Over Quarter"),
    (23, "Sales Growth Quarter Over Quarter"),
    (24, "Shares Outstanding"),
    (25, "Shares Float"),
    (26, "Insider Ownership"),
    (27, "Insider Transactions"),
    (28, "Institutional Ownership"),
    (29, "Institutional Transactions"),
    (30, "Short Float"),
    (31, "Short Ratio"),
    (32, "Return on Assets"),
    (33, "Return on Equity"),
    (34, "Return on Invested Capital"),
    (35, "Current Ratio"),
    (36, "Quick Ratio"),
    (37, "LT Debt/Equity"),
    (38, "Total Debt/Equity"),
    (39, "Gross Margin"),
    (40, "Operating Margin"),
    (41, "Profit Margin"),
    (42, "Performance (Week)"),
    (43, "Performance (Month)"),
    (44, "Performance (Quarter)"),
    (45, "Performance (Half Year)"),
    (46, "Performance (Year)"),
    (47, "Performance (YTD)"),
    (48, "Beta"),
    (49, "Average True Range"),
    (50, "Volatility (Week)"),
    (51, "Volatility (Month)"),
    (52, "20-Day Simple Moving Average"),
    (53, "50-Day Simple Moving Average"),
    (54, "200-Day Simple Moving Average"),
    (55, "50-Day High"),
    (56, "50-Day Low"),
    (57, "52-Week High"),
    (58, "52-Week Low"),
    (59, "Relative Strength Index (14)"),
    (60, "Change from Open"),
    (61, "Gap"),
    (62, "Analyst Recom"),
    (63, "Average Volume"),
    (64, "Relative Volume"),
    (65, "Price"),
    (66, "Change"),
    (67, "Volume"),
    (68, "Earnings Date"),
    (69, "Target Price"),
    (70, "IPO Date"),
    (71, "After-Hours Close"),
    (72, "After-Hours Change"),
    (73, "Book/sh"),
    (74, "Cash/sh"),
    (75, "Dividend"),
    (76, "Employees"),
    (77, "EPS Next Q"),
    (78, "Income"),
    (79, "Index"),
    (80, "Optionable"),
    (81, "Prev Close"),
    (82, "Sales"),
    (83, "Shortable"),
    (84, "Short Interest"),
    (85, "Float %"),
    (86, "Open"),
    (87, "High"),
    (88, "Low"),
    (89, "Trades"),
    (90, "Performance (1 Minute)"),
    (91, "Performance (2 Minutes)"),
    (92, "Performance (3 Minutes)"),
    (93, "Performance (5 Minutes)"),
    (94, "Performance (10 Minutes)"),
    (95, "Performance (15 Minutes)"),
    (96, "Performance (30 Minutes)"),
    (97, "Performance (1 Hour)"),
    (98, "Performance (2 Hours)"),
    (99, "Performance (4 Hours)"),
    (100, "Asset Type"),
    (101, "ETF Type"),
    (102, "Region"),
    (103, "Single Category"),
    (104, "Sector/Theme"),
    (105, "Tags"),
    (106, "Active/Passive"),
    (107, "Net Expense Ratio"),
    (108, "Total Holdings"),
    (109, "Assets Under Management"),
    (110, "Net Asset Value"),
    (111, "Net Asset Value %"),
    (112, "Net Flows (1 Month)"),
    (113, "Net Flows % (1 Month)"),
    (114, "Net Flows (3 Month)"),
    (115, "Net Flows % (3 Month)"),
    (116, "Net Flows (YTD)"),
    (117, "Net Flows % (YTD)"),
    (118, "Net Flows (1 Year)"),
    (119, "Net Flows % (1 Year)"),
    (120, "Return 1 Year"),
    (121, "Return 3 Year"),
    (122, "Return 5 Year"),
    (123, "Return 10 Year"),
    (124, "Return Since Inception"),
    (125, "All-Time High"),
    (126, "All-Time Low"),
    (127, "EPS Surprise"),
    (128, "Revenue Surprise"),
    (129, "Exchange"),
    (130, "Dividend TTM"),
    (131, "Dividend Ex Date"),
    (132, "EPS Year Over Year TTM"),
    (133, "Sales Year Over Year TTM"),
    (134, "52-Week Range"),
    (135, "News Time"),
    (136, "News URL"),
    (137, "News Title"),
    (138, "Performance (3 Years)"),
    (139, "Performance (5 Years)"),
    (140, "Performance (10 Years)"),
    (141, "After-Hours Volume"),
    (142, "EPS Growth Past 3 Years"),
    (143, "Sales Growth Past 3 Years"),
    (144, "Enterprise Value"),
    (145, "EV/EBITDA"),
    (146, "EV/Sales"),
    (147, "Dividend Growth 1 Year"),
    (148, "Dividend Growth 3 Years"),
    (149, "Dividend Growth 5 Years"),
    (150, "Daily Digest"),
    (151, "Security Type"),
    (152, "Tracked Index"),
    (153, "Candlestick"),
    (154, "Dilution Risk"),
    (155, "Performance (MTD)"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ScreenerType {
    /// Settings persisted by the previous data layer stored the bare
    /// variant names; keep loading them.
    #[serde(alias = "Stocks")]
    #[serde(alias = "Stock")]
    Stocks,
    #[serde(alias = "ETF")]
    ETF,
}

impl ScreenerType {
    pub fn as_str(self) -> &'static str {
        match self {
            ScreenerType::Stocks => "stocks",
            ScreenerType::ETF => "etf",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Parameter {
    pub screener_type: ScreenerType,
    pub order_by: String,
    pub signal: Option<String>,
    /// Settings persisted by the previous data layer named this field
    /// `parameter`; keep loading those.
    #[serde(alias = "parameter")]
    pub filters: Option<String>,
    pub columns: Option<String>,
}

impl Default for Parameter {
    fn default() -> Self {
        Self {
            screener_type: ScreenerType::Stocks,
            order_by: "-volume".to_string(),
            signal: None,
            filters: None,
            columns: None,
        }
    }
}

impl Parameter {
    pub fn to_query(&self) -> String {
        let mut result = format!(
            "type={}&o={}",
            self.screener_type.as_str(),
            urlencoding::encode(&self.order_by)
        );
        if let Some(signal) = &self.signal {
            result.push_str(&format!("&s={}", urlencoding::encode(signal)));
        }
        if let Some(filters) = &self.filters {
            result.push_str(&format!("&f={}", urlencoding::encode(filters)));
        }
        if let Some(cols) = &self.columns {
            result.push_str(&format!("&c={}", urlencoding::encode(cols)));
        }
        result
    }

    // Market screener query with default columns (No. and Ticker)
    pub fn market_to_query(&self) -> String {
        let mut result = format!(
            "type={}&o={}&c=0,1",
            self.screener_type.as_str(),
            urlencoding::encode(&self.order_by)
        );
        if let Some(signal) = &self.signal {
            result.push_str(&format!("&s={}", urlencoding::encode(signal)));
        }
        if let Some(filters) = &self.filters {
            result.push_str(&format!("&f={}", urlencoding::encode(filters)));
        }
        result
    }
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    pub async fn screener(&self, parameter: Parameter) -> anyhow::Result<crate::CSVResult> {
        let bytes = self
            .get_bytes("screener", &format!("v=151&{}", parameter.to_query()))
            .await?;
        crate::parse_csv_bytes(&bytes)
    }

    /// Market screener query with default columns (No. and Ticker)
    pub async fn market_screener(&self, parameter: Parameter) -> anyhow::Result<crate::CSVResult> {
        let bytes = self
            .get_bytes(
                "screener",
                &format!("v=151&{}", parameter.market_to_query()),
            )
            .await?;
        crate::parse_csv_bytes(&bytes)
    }
}
