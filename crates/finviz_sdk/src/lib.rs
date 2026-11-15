//! Client for Finviz Elite's CSV export endpoints: the screener and the
//! economic / earnings calendars.
//!
//! HTTP 部分不绑定任何 HTTP 客户端:请求由使用方为 `T` 实现的
//! [`utils::HttpExecutor`] 执行。本 SDK 只解析数据 —— CSV 按表头名反序列化为
//! 类型化结构体;时间字段保持导出的原始字符串,不做任何时区换算或语义转换。

pub mod calendar;
pub mod screener;

use std::sync::Arc;

use utils::HttpExecutor;

pub struct Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    inner: utils::Client<T, Arc<str>, E>,
}

impl<T, E> Client<T, E>
where
    T: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    pub fn new(http_client: T, api_token: &str, executor: E) -> Self {
        Self {
            inner: utils::Client::new(http_client, Arc::from(api_token), executor),
        }
    }

    pub fn set_token(&mut self, api_token: &str) {
        self.inner.state = Arc::from(api_token);
    }

    /// Fetch an authenticated Elite export endpoint and return its raw
    /// bytes. A non-2xx status is an error carrying the response body,
    /// instead of surfacing later as a misleading CSV "missing column".
    pub(crate) async fn get_bytes(&self, path: &str, query: &str) -> anyhow::Result<Vec<u8>> {
        let url = format!(
            "https://elite.finviz.com/export/{path}?auth={}&{query}",
            self.inner.state,
        );
        let response = self.inner.get(url).await?;
        utils::ensure_success(response)
    }
}

#[derive(Debug)]
pub struct CSVResult {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

use std::io::Cursor;

/// Parse a full CSV export: strip the BOM, normalize short/long rows to the
/// header count.
pub(crate) fn parse_csv_bytes(bytes: &[u8]) -> anyhow::Result<CSVResult> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(Cursor::new(bytes));

    let headers: Vec<String> = rdr
        .headers()?
        .iter()
        .map(|s| s.trim_start_matches('\u{feff}').to_string())
        .collect();

    let col_count = headers.len();

    let mut rows: Vec<Vec<String>> = Vec::new();
    for record in rdr.records() {
        let record = record?;
        let mut row: Vec<String> = record.iter().map(|s| s.to_string()).collect();

        if row.len() < col_count {
            row.resize(col_count, String::new());
        } else if row.len() > col_count {
            row.truncate(col_count);
        }

        rows.push(row);
    }

    Ok(CSVResult { headers, rows })
}

pub(crate) fn column_pos(headers: &csv::StringRecord, name: &str) -> anyhow::Result<usize> {
    headers
        .iter()
        .position(|header| header.trim() == name)
        .ok_or_else(|| anyhow::anyhow!("export is missing the `{name}` column"))
}

/// Parse a CSV export whose columns are identified by header name: every
/// name in `columns` is resolved against the header row once, then each
/// record is converted by `build` through a [`Row`] view.
pub(crate) fn parse_rows<T>(
    bytes: &[u8],
    columns: &[&str],
    mut build: impl FnMut(&Row<'_>) -> anyhow::Result<T>,
) -> anyhow::Result<Vec<T>> {
    let text = std::str::from_utf8(bytes)?;
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(text.as_bytes());
    let headers = reader.headers()?.clone();

    let positions = columns
        .iter()
        .map(|name| column_pos(&headers, name))
        .collect::<anyhow::Result<Vec<usize>>>()?;

    let mut items = Vec::new();
    for record in reader.records() {
        let record = record?;
        if record.is_empty() {
            continue;
        }
        let row = Row {
            record: &record,
            columns,
            positions: &positions,
        };
        items.push(build(&row)?);
    }
    Ok(items)
}

/// A resolved view of one CSV record: `columns` and `positions` are the
/// requested column names and their header-row indexes, in the same order.
pub(crate) struct Row<'a> {
    record: &'a csv::StringRecord,
    columns: &'a [&'a str],
    positions: &'a [usize],
}

impl Row<'_> {
    /// The trimmed cell text under `column`; a short record reads as "".
    pub(crate) fn cell(&self, column: &str) -> &str {
        self.columns
            .iter()
            .zip(self.positions)
            .find(|(name, _)| **name == column)
            .and_then(|(_, &index)| self.record.get(index))
            .unwrap_or_default()
            .trim()
    }

    /// [`Row::cell`] as `Option<String>`: a blank cell is `None`.
    pub(crate) fn optional(&self, column: &str) -> Option<String> {
        match self.cell(column) {
            "" => None,
            value => Some(value.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_csv_bytes_strips_bom_and_normalizes_rows() {
        let csv = "\u{feff}A,B\r\n1,2\r\n3\r\n4,5,6\r\n";
        let result = parse_csv_bytes(csv.as_bytes()).unwrap();
        assert_eq!(result.headers, ["A", "B"]);
        assert_eq!(result.rows, [["1", "2"], ["3", ""], ["4", "5"]]);
    }

    #[test]
    fn column_pos_reports_missing_column() {
        let headers = csv::StringRecord::from(vec!["A", "B"]);
        assert_eq!(column_pos(&headers, "B").unwrap(), 1);
        assert!(column_pos(&headers, "C").is_err());
    }

    #[test]
    fn parse_rows_resolves_named_columns() {
        let csv = "Event,Date,Time\r\nCPI,2026-01-14,08:30\r\n\r\nFed,2026-01-28,All Day\r\n";
        let pairs = parse_rows(csv.as_bytes(), &["Event", "Date"], |row| {
            Ok((row.cell("Event").to_string(), row.cell("Date").to_string()))
        })
        .unwrap();
        assert_eq!(
            pairs,
            [
                ("CPI".to_string(), "2026-01-14".to_string()),
                ("Fed".to_string(), "2026-01-28".to_string())
            ]
        );
        assert!(
            parse_rows(
                csv.as_bytes(),
                &["Event", "Missing"],
                |_| -> anyhow::Result<()> { Ok(()) }
            )
            .is_err()
        );
    }
}
