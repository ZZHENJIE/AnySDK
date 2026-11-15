use crate::Client;
use serde::{Deserialize, Serialize};
use utils::HttpExecutor;

/// The named columns of the earnings export.
const COLUMNS: &[&str] = &[
    "Date",
    "Ticker",
    "Company",
    "Market Cap",
    "EPS Estimate",
    "EPS Actual",
    "EPS Surprise",
    "EPS GAAP Estimate",
    "EPS GAAP Actual",
    "EPS GAAP Surprise",
    "Revenue Estimate",
    "Revenue Actual",
    "Revenue Surprise",
    "1-Day Price Reaction",
];

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Item {
    /// Raw export date cell, e.g. `2026-11-10 16:05`; the time part is
    /// sometimes absent. Kept verbatim — the SDK performs no timezone or
    /// "All Day" conversion.
    pub date: String,
    pub symbol: String,
    pub company: String,
    /// The numeric columns are in millions (money) or percents (surprises,
    /// the one-day reaction); an empty or unparsable cell is `None`.
    pub market_cap: Option<f64>,
    pub eps_estimate: Option<f64>,
    pub eps_actual: Option<f64>,
    pub eps_surprise: Option<f64>,
    pub eps_gaap_estimate: Option<f64>,
    pub eps_gaap_actual: Option<f64>,
    pub eps_gaap_surprise: Option<f64>,
    pub revenue_estimate: Option<f64>,
    pub revenue_actual: Option<f64>,
    pub revenue_surprise: Option<f64>,
    pub one_day_price_reaction: Option<f64>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    /// Fetch the earnings calendar from `from` (`YYYY-MM-DD`, passed
    /// through to the export query unchanged).
    pub async fn earnings_calendar(&self, from: &str) -> anyhow::Result<Vec<Item>> {
        let bytes = self
            .get_bytes("calendar/earnings", &format!("dateFrom={from}"))
            .await?;
        parse_csv(&bytes)
    }
}

fn parse_csv(bytes: &[u8]) -> anyhow::Result<Vec<Item>> {
    crate::parse_rows(bytes, COLUMNS, |row| {
        Ok(Item {
            date: row.cell("Date").to_string(),
            symbol: row.cell("Ticker").to_string(),
            company: row.cell("Company").to_string(),
            market_cap: parse_number(row.cell("Market Cap")),
            eps_estimate: parse_number(row.cell("EPS Estimate")),
            eps_actual: parse_number(row.cell("EPS Actual")),
            eps_surprise: parse_number(row.cell("EPS Surprise")),
            eps_gaap_estimate: parse_number(row.cell("EPS GAAP Estimate")),
            eps_gaap_actual: parse_number(row.cell("EPS GAAP Actual")),
            eps_gaap_surprise: parse_number(row.cell("EPS GAAP Surprise")),
            revenue_estimate: parse_number(row.cell("Revenue Estimate")),
            revenue_actual: parse_number(row.cell("Revenue Actual")),
            revenue_surprise: parse_number(row.cell("Revenue Surprise")),
            one_day_price_reaction: parse_number(row.cell("1-Day Price Reaction")),
        })
    })
}

/// Parse a bare numeric cell; an empty or unparsable cell is `None`.
fn parse_number(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_number_accepts_only_bare_numbers() {
        assert_eq!(parse_number("1234.5"), Some(1234.5));
        assert_eq!(parse_number("+2.5"), Some(2.5));
        assert_eq!(parse_number("-3.2"), Some(-3.2));
        assert_eq!(parse_number(""), None);
        assert_eq!(parse_number("$1.5"), None);
        assert_eq!(parse_number("2.5%"), None);
        assert_eq!(parse_number("1,234.5"), None);
        assert_eq!(parse_number("N/A"), None);
    }

    #[test]
    fn parses_rows_and_keeps_the_raw_date_cell() {
        let csv = "Date,Ticker,Company,Market Cap,EPS Estimate,EPS Actual,EPS Surprise,\
                   EPS GAAP Estimate,EPS GAAP Actual,EPS GAAP Surprise,Revenue Estimate,\
                   Revenue Actual,Revenue Surprise,1-Day Price Reaction\r\n\
                   2026-11-10 16:05,AAPL,Apple Inc.,3500000,1.5,1.7,13.3,,,,,,,\r\n\
                   2026-11-12,LMN,Lemon Co.,,,,,,,,1,2,3,\r\n";
        let items = parse_csv(csv.as_bytes()).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].symbol, "AAPL");
        assert_eq!(items[0].market_cap, Some(3500000.0));
        assert_eq!(items[0].eps_actual, Some(1.7));
        assert_eq!(items[0].eps_surprise, Some(13.3));
        assert_eq!(items[0].eps_gaap_estimate, None);
        // The date cell is passed through verbatim — with or without a time
        // part, no timezone conversion.
        assert_eq!(items[0].date, "2026-11-10 16:05");
        assert_eq!(items[1].date, "2026-11-12");
        assert_eq!(items[1].revenue_estimate, Some(1.0));
        assert_eq!(items[1].revenue_actual, Some(2.0));
        assert_eq!(items[1].one_day_price_reaction, None);
    }
}
