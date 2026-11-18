# AnySDK

基于 Rust 的通用**数据** SDK 工作区(workspace),通过统一的 `HttpExecutor` 核心封装各类数据源 —— 内置 **Alpaca** 与 **Finviz Elite**,提供类型安全的异步 API。

引入只需一个入口 crate:根目录的 `anysdk`,`utils` 核心默认自带,各 SDK 通过 feature 按需启用。

## 特性

- **不绑定任何 HTTP 客户端**:SDK 只把请求描述为 `utils::Method`(URL + headers),由你为自己的客户端(`reqwest` / 自研 / GUI 内嵌 client 等)实现一次 `HttpExecutor` 即可驱动全部端点;超时 / 重试 / 代理等传输策略同样归你自己的客户端配置。
- **只解析,不转换**:SDK 仅把 JSON / CSV 反序列化为类型化结构体;时间等字段保持数据源的原始值,不做时区换算、默认值填充或任何语义转换。JSON 层为 simd-json(REST 低频路径走 serde,流热路径走 tape 快解析,见下)。
- **自愈式实时流**:`alpaca_stream` 提供股票 / 新闻 WebSocket 流 —— 断线自动重连(指数退避)、主动 ping + 静默检测保活、订阅自动重放、独立连接状态通道;股票流消息循环用 tape 快路径解析(`q`/`t` 零 DOM 构建),对拍测试保证与 serde 路径逐字段一致。
- **类型安全 / 异步 / 零 unsafe**:全部 crate `deny(unsafe_code)`,`edition 2024`。

## 工作区结构

```
AnySDK/
├── src/lib.rs        # 入口 crate「anysdk」:utils 默认引入,feature 选择 SDK
├── crates/
│   ├── utils/         # 核心架构:Method / HttpResponse / HttpExecutor<T> / Client<T, S, E>
│   ├── alpaca_sdk/    # Alpaca 市场数据(HTTP/REST)
│   ├── alpaca_stream/ # Alpaca WebSocket 实时流(自动重连 + 保活 + 订阅重放)
│   ├── finviz_sdk/    # Finviz Elite CSV 导出(screener / 财报日历 / 经济日历)
│   └── test/          # 内部集成测试 / 示例程序(reqwest 执行器示范)
├── docs/             # 文档站点(docsify)
├── Cargo.toml        # workspace 配置 + anysdk 入口包
└── README.md
```

## 架构

```
你的 HTTP 客户端 T ──┐
                     ├─→ HttpExecutor<T>(你实现一次)──→ utils::Client<T, S, E>
平台 state S(密钥等)─┤                                       │
执行器 E ────────────┘                            SDK 侧 Client<T, E> 只负责:
                                                  拼 URL → 描述 Method → 执行 →
                                                  状态码判定 → JSON/CSV 反序列化
```

- `HttpExecutor<T>` 只做传输:发请求、带回 `HttpResponse { status, body }`。任何非 2xx 都由 SDK 统一转为携带 API 原始错误体的 `Err`。
- SDK 不知道 `T` 是什么 —— 换 HTTP 客户端时 SDK 代码零改动。

### 接入示例:让 `reqwest::Client` 驱动全部 SDK

```rust
use anysdk::utils::{HttpExecutor, HttpResponse, Method};

pub struct ReqwestExecutor;

impl HttpExecutor<reqwest::Client> for ReqwestExecutor {
    async fn execute(
        &self,
        client: &reqwest::Client,
        request: &Method,
    ) -> anyhow::Result<HttpResponse> {
        let response = match request {
            Method::Get(request) => {
                let mut builder = client.get(&request.url);
                for (key, value) in &request.headers {
                    builder = builder.header(key.as_str(), value.as_str());
                }
                builder.send().await?
            }
            Method::Post(request) => {
                let mut builder = client.post(&request.url).body(request.body.clone());
                for (key, value) in &request.headers {
                    builder = builder.header(key.as_str(), value.as_str());
                }
                builder.send().await?
            }
        };
        Ok(HttpResponse {
            status: response.status().as_u16(),
            body: response.bytes().await?.to_vec(),
        })
    }
}
```

## 快速开始

### 环境要求

- Rust 1.85+(`edition = "2024"`)

### 添加依赖

```toml
[dependencies]
# 只引一个入口 crate;utils 核心默认自带,feature 决定启用哪些 SDK
anysdk = { git = "https://github.com/ZZHENJIE/AnySDK", features = ["alpaca-stream", "finviz"] }
tokio = { version = "1", features = ["full"] }
```

| feature | 引入的模块 |
| --- | --- |
| (默认,不加 feature) | `anysdk::utils` —— 仅核心架构,不拉入任何 SDK |
| `alpaca` | `anysdk::alpaca_sdk`(HTTP/REST) |
| `alpaca-stream` | `anysdk::alpaca_stream`(WS 实时流,隐含 `alpaca`) |
| `finviz` | `anysdk::finviz_sdk` |
| `full` | 全部启用 |

### Alpaca:历史数据 / 最新值 / 实时流

```rust
use anysdk::alpaca_sdk::{bars, quotes, snapshots, news, Feed};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 本 SDK 不绑定 HTTP 客户端:超时、重试、代理等传输策略都由使用方在自己的
    // reqwest::Client 上配置(如 .timeout(Duration::from_secs(10))),再交给执行器。
    let mut client = anysdk::alpaca_sdk::Client::new(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?,
        "YOUR_API_KEY",
        "YOUR_API_SECRET",
        ReqwestExecutor,
    );

    // 历史数据:<域> 单页 / <域>_all 自动翻页 / <域>_single 单标的 / latest_<域> 最新值
    let page = client.bars(&bars::Params {
        symbols: vec!["AAPL".into(), "MSFT".into()],
        timeframe: bars::timeframe::DAY_1.into(),
        ..Default::default()
    }).await?;
    let all = client.bars_all(&page_params).await?;        // 跟踪 next_page_token 拉完
    let single = client.bars_single_all("AAPL", &params).await?;

    // 最新值
    let latest = client.latest_quotes(&quotes::LatestParams {
        symbols: vec!["AAPL".into()], ..Default::default()
    }).await?;

    // 快照(最新 trade + quote + 分钟 bar + 当日/前日日 bar)
    let snapshots = client.snapshots(&snapshots::Params {
        symbols: vec!["AAPL".into()], ..Default::default()
    }).await?;

    // 新闻(REST)
    let page = client.news(&news::Params::default()).await?;

    // 实时流(WS):自愈式长连接 —— 断线自动重连 + 订阅自动重放 + 保活
    let stream = anysdk::alpaca_stream::StockStream::connect(Feed::Iex, "KEY", "SECRET").await?;
    // 连接状态事件(Connected / Reconnecting{attempt,delay} /
    // Failed{reason: AuthFailed|Network|Protocol, message} / Closed;
    // AuthFailed 时 SDK 已停止重连,上层不应再重试)
    let mut status = stream.status();
    tokio::spawn(async move { while let Ok(ev) = status.recv().await { println!("{ev:?}"); } });
    stream.subscribe(&anysdk::alpaca_stream::StockSubscription {
        trades: vec!["AAPL".into()],
        ..Default::default()
    }).await?;
    // 退订:socket 保持打开;断线期间退订同样记入期望状态,重连后不会复活
    stream.unsubscribe(&anysdk::alpaca_stream::StockSubscription {
        trades: vec!["AAPL".into()],
        ..Default::default()
    }).await?;
    let mut rx = stream.messages();
    while let Ok(msg) = rx.recv().await {
        println!("{msg:?}");
    }
    stream.close(); // 显式关闭;不调用则自动重连直到最后一个句柄 drop
    Ok(())
}
```

### Finviz Elite:筛选器 / 日历

```rust
use anysdk::finviz_sdk::calendar::economic::Parameter as EconomicParameter;
use anysdk::finviz_sdk::screener::{Parameter, ScreenerType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = anysdk::finviz_sdk::Client::new(
        reqwest::Client::new(),
        "YOUR_FINVIZ_ELITE_AUTH_TOKEN",   // 浏览器 URL 中的 auth 参数
        ReqwestExecutor,
    );

    // 筛选器导出(列选择见 anysdk::finviz_sdk::screener::EXPORT_COLUMNS)
    let csv = client.screener(Parameter {
        screener_type: ScreenerType::Stocks,
        order_by: "-marketcap".into(),
        filters: Some("exch_nyse".into()),
        columns: Some("0,1,65,66".into()),
        signal: None,
    }).await?;
    println!("{:?}", csv.headers);

    // 财报日历(date 字段保持导出原始字符串,如 "2026-11-10 16:05")
    let earnings = client.earnings_calendar("2026-01-14").await?;

    // 经济日历(时间窗口直传,SDK 不做周对齐等业务规则)
    let events = client
        .economic_calendar(EconomicParameter::new("2026-01-12", "2026-01-16"))
        .await?;
    Ok(())
}
```

## 「只解析」原则

SDK 把数据源的原始载荷(JSON / CSV)反序列化为类型化结构体,仅此而已:

- **保留**:字段名映射(serde rename)、CSV 按表头名取列、BOM 剥离、空白单元格 → `None`、数字字符串 → `f64`。
- **不做**:时区换算(美东 → UTC 时间戳)、`"All Day"` 处理、日历窗口的业务对齐、字段派生。

因此日历类条目的时间字段是原始字符串(`date` / `time`),换算成时间戳由调用方自行决定。

## 环境变量

建议将密钥通过环境变量注入,避免硬编码(`test` crate 已支持项目根 `.env`):

```bash
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
export FINVIZ_ELITE_AUTH=...
```

## 端点回归

```bash
cargo run -p test -- list            # 列出全部端点
cargo run -p test -- alpaca:bars     # 跑单个端点
cargo run -p test -- all             # 跑全部端点
```

## 文档

- 在线文档:[docs](https://zzhenjie.github.io/AnySDK)(docsify 构建)
- 本地文档:`cargo doc --no-deps` 生成 rustdoc,位于 `target/doc/`

## License

本项目采用 [GPL-3.0-only](LICENSE) 许可证。

## 声明

本 SDK 仅用于数据访问,不构成任何投资建议。使用前请遵守各数据源的服务条款。
