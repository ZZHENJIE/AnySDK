# Alpaca SDK

`alpaca_sdk` 封装 Alpaca Market Data API(**data-only**,无交易)—— 纯 HTTP/REST:历史 bars / quotes / trades / auctions、最新值、快照、元数据、新闻、标的列表(assets)。

> 实时 WebSocket 流在独立的 [Alpaca Stream](alpaca_stream.md) 页面。客户端不绑定任何 HTTP 实现:构造时传入你的 HTTP 客户端与 `HttpExecutor` 执行器(接入方式见[首页](/))。

## 依赖

```toml
[dependencies]
# 启用 `alpaca` feature 即引入 alpaca_sdk(utils 核心默认自带)
anysdk = { git = "https://github.com/ZZHENJIE/AnySDK", features = ["alpaca"] }
tokio = { version = "1", features = ["full"] }
```

## 创建客户端

```rust
let client = anysdk::alpaca_sdk::Client::new(
    // 超时 / 重试 / 代理等传输策略由使用方在自己的 HTTP 客户端上配置
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?,
    "YOUR_API_KEY",
    "YOUR_API_SECRET",
    ReqwestExecutor,
);

// 可选:覆盖 data API base URL(如沙箱环境)
let client = client.with_data_url("https://paper-api.alpaca.markets");
// 可选:随时替换密钥
// client.set_key_id("..."); client.set_secret("...");
```

认证:HTTP 走 `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` 请求头(由 SDK 注入);股票 WS 走 `auth` action,新闻 WS 走升级请求头。

## 数据 Feed

`Feed` 枚举贯穿 HTTP `feed` 参数与 WS 路径:`Sip` / `Iex` / `Otc` / `DelayedSip` / `Boats` / `Overnight`。个别端点只接受子集(如历史 bars 无 `delayed_sip`/`overnight`),不支持的值由服务端拒绝。

## 命名约定

| 方法形式 | 含义 |
| --- | --- |
| `<域>(&params)` | 多标的,单页(含 `next_page_token`) |
| `<域>_all(&params)` | 多标的,自动跟踪翻页拉完 |
| `<域>_all_with_progress(&params, on_page)` | 同上,每拉完一页回调累计条数(仅 `bars_all` 提供带进度变体) |
| `<域>_single(symbol, &params)` | 单标的,单页 |
| `<域>_single_all(symbol, &params)` | 单标的,自动翻页拉完 |
| `latest_<域>(&params)` | 多标的最新值 |
| `latest_<域去掉 s>(symbol)` | 单标的最新值 |

域:`bars` / `quotes` / `trades` / `auctions`(无 latest 变体)、`snapshots`(`snapshots` / `snapshot`)、`news`(`news` / `news_all`)、`assets`(`assets`,单方法:GET /v2/assets 全量数组、无翻页)。

## 历史数据示例

```rust
use anysdk::alpaca_sdk::{bars, quotes, trades, auctions};

// 多标的单页
let page = client.bars(&bars::Params {
    symbols: vec!["AAPL".into(), "MSFT".into()],
    timeframe: bars::timeframe::DAY_1.into(),   // 必填,见 bars::timeframe 常量
    start: Some("2026-01-02".into()),
    end: Some("2026-01-10".into()),
    limit: Some(1000),
    ..Default::default()                        // adjustment / feed / currency / sort / asof
}).await?;
println!("{:?}", page.next_page_token);

// 自动翻页拉完
let all: std::collections::HashMap<String, Vec<bars::Bar>> = client.bars_all(&params).await?;

// 带翻页进度:每拉完一页,回调累计已拉取的 bar 条数(原 bars_all 内部委托此方法)
let all = client.bars_all_with_progress(&params, |fetched| {
    println!("已拉取 {fetched} 条");
}).await?;

// 单标的
let single = client.bars_single_all("AAPL", &bars::SingleParams {
    timeframe: bars::timeframe::MIN_5.into(),
    ..Default::default()
}).await?;

// 最新值
let latest = client.latest_quotes(&quotes::LatestParams {
    symbols: vec!["AAPL".into()],
    ..Default::default()
}).await?;
let one = client.latest_trade("AAPL").await?;

// 拍卖(开/收盘竞价,按日分组)
let auctions = client.auctions(&auctions::Params {
    symbols: vec!["AAPL".into()],
    ..Default::default()
}).await?;
```

时间字段(如 `Bar::time`)为 `DateTime<Utc>`,是 serde 对 API 原始 RFC-3339 字符串的直接反序列化产物,SDK 不做额外换算。

## 快照

```rust
use anysdk::alpaca_sdk::snapshots;

let snapshots = client.snapshots(&snapshots::Params {
    symbols: vec!["AAPL".into(), "MSFT".into()],
    ..Default::default()
}).await?;
// 一次打包:latestTrade / latestQuote / minuteBar / dailyBar / prevDailyBar
let close = snapshots["AAPL"].daily_bar.as_ref().map(|b| b.close);

let single = client.snapshot("AAPL", &Default::default()).await?;
```

## 元数据与新闻

```rust
use anysdk::alpaca_sdk::{meta, news};

// 交易所代码("N" → "New York Stock Exchange")
let exchanges = client.exchange_codes().await?;
// 成交/报价条件代码
let conditions = client.condition_codes(meta::TickType::Trades).await?;

// 新闻(最新在前;news_all 自动翻页)
let page = client.news(&news::Params {
    symbols: vec!["AAPL".into()],
    limit: Some(50),
    ..Default::default()
}).await?;
let all = client.news_all(&news::Params::default()).await?;

// 标的列表(GET /v2/assets:单次返回全量数组,无翻页)
let assets = client.assets(&anysdk::alpaca_sdk::assets::Params {
    status: Some("active".into()),
    asset_class: Some("us_equity".into()),
}).await?;
```

## 错误行为

非 2xx 响应返回 `Err`,错误信息携带 HTTP 状态码与 API 的原始响应体,例如:

```text
HTTP 403: {"message":"invalid credentials"}
```

## 测试

```bash
cargo test -p alpaca_sdk   # 离线反序列化测试 + mock 执行器驱动的翻页进度测试
cargo run -p test -- list  # 联网端点回归(需密钥)
```
