use crate::Client;
use serde::{Deserialize, Serialize};
use utils::HttpExecutor;

/// The named columns of the economic export.
const COLUMNS: &[&str] = &[
    "Event", "Date", "Time", "Impact", "For", "Actual", "Expected", "Prior",
];

/// The export query window: `dateFrom` / `dateTo` (`YYYY-MM-DD`), passed
/// through to the export query unchanged — the SDK applies no week
/// alignment or other business rules.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Parameter {
    pub from: String,
    pub to: String,
}

impl Parameter {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
        }
    }

    pub fn to_query(&self) -> String {
        format!(
            "dateFrom={}&dateTo={}",
            urlencoding::encode(&self.from),
            urlencoding::encode(&self.to)
        )
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Item {
    pub event: String,
    /// Raw export date cell (`YYYY-MM-DD`), verbatim.
    pub date: String,
    /// Raw export time cell (`HH:MM`, `HH:MM:SS`, `All Day`, or empty) —
    /// no timezone conversion is performed by the SDK.
    pub time: String,
    pub impact: u8,
    pub r#for: Option<String>,
    pub actual: Option<String>,
    pub expected: Option<String>,
    pub prior: Option<String>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    pub async fn economic_calendar(&self, parameter: Parameter) -> anyhow::Result<Vec<Item>> {
        let bytes = self
            .get_bytes("calendar/economic", &parameter.to_query())
            .await?;
        parse_csv(&bytes)
    }
}

fn parse_csv(bytes: &[u8]) -> anyhow::Result<Vec<Item>> {
    crate::parse_rows(bytes, COLUMNS, |row| {
        Ok(Item {
            event: row.cell("Event").to_string(),
            date: row.cell("Date").to_string(),
            time: row.cell("Time").to_string(),
            impact: row.cell("Impact").parse().unwrap_or_default(),
            r#for: row.optional("For"),
            actual: row.optional("Actual"),
            expected: row.optional("Expected"),
            prior: row.optional("Prior"),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_named_columns_and_normalizes_blanks() {
        let csv = "Event,Date,Time,Impact,For,Actual,Expected,Prior\r\n\
                   CPI m/m,2026-01-14,08:30,3,December,0.2%,0.3%,0.1%\r\n\
                   Fed Interest Rate Decision,2026-01-28,All Day,3,,,\r\n";
        let items = parse_csv(csv.as_bytes()).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].event, "CPI m/m");
        assert_eq!(items[0].impact, 3);
        assert_eq!(items[0].r#for.as_deref(), Some("December"));
        assert_eq!(items[0].actual.as_deref(), Some("0.2%"));
        // Date and time cells pass through verbatim — including "All Day".
        assert_eq!(items[0].date, "2026-01-14");
        assert_eq!(items[0].time, "08:30");
        assert_eq!(items[1].time, "All Day");
        // The second row carries no For/Actual/Expected/Prior cells: blank
        // means absent, not an empty string.
        assert_eq!(items[1].r#for, None);
        assert_eq!(items[1].actual, None);
        assert_eq!(items[1].expected, None);
        assert_eq!(items[1].prior, None);
    }

    #[test]
    fn rejects_missing_columns() {
        assert!(parse_csv(b"Event,Date\r\nx,2026-01-14\r\n").is_err());
    }

    #[test]
    fn parameter_builds_an_encoded_query() {
        let parameter = Parameter::new("2026-01-12", "2026-01-16");
        assert_eq!(parameter.to_query(), "dateFrom=2026-01-12&dateTo=2026-01-16");
    }
}
