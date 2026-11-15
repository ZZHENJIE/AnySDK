# Finviz SDK

`finviz_sdk` 封装 [Finviz Elite](https://elite.finviz.com) 的 CSV 导出接口,支持筛选器(Screener)与财报日历(Earnings)、经济日历(Economic)数据。

> 注意:这些接口需要 **Finviz Elite 付费账号**。登录后浏览器地址栏中 URL 携带的 `auth` 参数即为 API 密钥。
>
> 客户端不绑定任何 HTTP 实现:构造时传入你的 HTTP 客户端与 `HttpExecutor` 执行器(接入方式见[首页](/))。

## 依赖

```toml
[dependencies]
# 启用 `finviz` feature 即引入 finviz_sdk(utils 核心默认自带)
anysdk = { git = "https://github.com/ZZHENJIE/AnySDK", features = ["finviz"] }
tokio = { version = "1", features = ["full"] }
```

## 创建客户端

```rust
let client = anysdk::finviz_sdk::Client::new(
    reqwest::Client::new(),
    "YOUR_FINVIZ_ELITE_AUTH_TOKEN",
    ReqwestExecutor,
);

// 可选:随时替换 token
// client.set_token("...");
```

## 「只解析」说明

SDK 把导出的 CSV 按表头名反序列化为类型化结构体:保留 BOM 剥离、短行补齐、空白单元格 → `None`、数字字符串 → `f64`;**不做**任何时区换算、`"All Day"` 处理或日期窗口的业务对齐 —— 日历条目的 `date` / `time` 字段是导出的原始字符串,时间戳换算由调用方自行处理。

## 筛选器(Screener)

```rust
use anysdk::finviz_sdk::screener::{Parameter, ScreenerType};

// 列选择器的完整索引:EXPORT_COLUMNS(156 列,"c=" 参数的 index → 列名)
use anysdk::finviz_sdk::screener::EXPORT_COLUMNS;

let csv = client.screener(Parameter {
    screener_type: ScreenerType::Stocks,        // Stocks / ETF
    order_by: "-marketcap".into(),              // "o=" 排序
    filters: Some("exch_nyse".into()),          // "f=" 过滤器,逗号组合
    columns: Some("0,1,6,65,66".into()),        // "c=" 导出列
    signal: None,                               // "s=" 信号
}).await?;

println!("{:?}", csv.headers);                  // Vec<String>
for row in &csv.rows {
    println!("{:?}", row);                      // Vec<String>,与 headers 对齐
}

// 市场总览:只取 No. + Ticker 两列
let overview = client.market_screener(Parameter {
    order_by: "-volume".into(),
    ..Default::default()
}).await?;
```

返回值为原始行列(`CSVResult { headers, rows }`),字段语义由列名决定,SDK 不做数值清洗。

## 财报日历(Earnings Calendar)

```rust
// from:`YYYY-MM-DD`,直传 dateFrom 查询参数
let items = client.earnings_calendar("2026-01-14").await?;
for item in &items {
    println!("{} {} EPS est {:?}", item.date, item.symbol, item.eps_estimate);
}
```

`Item` 字段:

| 字段 | 说明 |
| --- | --- |
| `date` | 导出原始日期单元格,如 `2026-11-10 16:05`(时间部分可能缺失),**原样保留** |
| `symbol` / `company` | 标的与公司名 |
| `market_cap` | 市值(百万),空/不可解析为 `None` |
| `eps_estimate` / `eps_actual` / `eps_surprise` | EPS 预期 / 实际 / 超预期(%) |
| `eps_gaap_estimate` / `eps_gaap_actual` / `eps_gaap_surprise` | GAAP 口径同上 |
| `revenue_estimate` / `revenue_actual` / `revenue_surprise` | 营收预期 / 实际 / 超预期(%) |
| `one_day_price_reaction` | 单日股价反应(%) |

## 经济日历(Economic Calendar)

```rust
use anysdk::finviz_sdk::calendar::economic::Parameter;

// 时间窗口直传,SDK 不做周一对齐等业务规则
let events = client.economic_calendar(Parameter::new("2026-01-12", "2026-01-16")).await?;
for event in &events {
    println!("{} {} {} impact {}", event.date, event.time, event.event, event.impact);
}
```

`Item` 字段:`event`、`date`(`YYYY-MM-DD` 原样)、`time`(`HH:MM` / `HH:MM:SS` / `All Day` / 空,原样)、`impact`(1–3,不可解析为 0)、`r#for`、`actual`、`expected`、`prior`(空白单元格为 `None`)。

## 错误行为

非 2xx 响应返回 `Err`,错误信息携带 HTTP 状态码与 API 的原始响应体 —— 不会把失败误报成 CSV "缺列":

```text
Finviz export `screener` failed (HTTP 401): ...
```

## 测试

```bash
cargo test -p finviz_sdk   # 离线 CSV 解析测试
cargo run -p test -- list  # 联网端点回归(需密钥)
```
