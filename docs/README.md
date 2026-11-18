# AnySDK 文档

AnySDK 是一个基于 Rust 的通用**数据** SDK 工作区,通过统一的 `HttpExecutor` 核心封装各类数据源 —— 内置 **Alpaca** 与 **Finviz Elite**,提供类型安全的异步接口。

引入只需一个入口 crate:根目录的 `anysdk`,`utils` 核心默认自带,各 SDK 通过 feature 按需启用(`alpaca` / `finviz` / `full`)。

## 架构

- **不绑定任何 HTTP 客户端**:SDK 只把请求描述为 `utils::Method`(URL + headers),由你为自己的客户端类型实现一次 `HttpExecutor` 即可驱动全部端点;非 2xx 响应由 SDK 统一转为携带 API 原始错误体的错误。超时、重试、代理等传输策略同样由你自己的 HTTP 客户端配置(如 `reqwest::Client::builder().timeout(...)`)。
- **只解析,不转换**:SDK 仅把 JSON / CSV 反序列化为类型化结构体;时间等字段保持数据源的原始值,不做时区换算或语义转换。JSON 解析层为 simd-json(REST 低频路径走 serde 接口)。
- **自愈式实时流**:`alpaca_stream` 提供股票 / 新闻 WebSocket 流 —— 断线自动重连(指数退避)、主动 ping + 静默检测保活、订阅自动重放、独立连接状态通道(`Failed{reason, message}` 区分认证类永久失败);股票流消息循环用 tape 快路径解析(`q`/`t` 零 DOM 构建,对拍测试保证与 serde 路径一致)。

## 内容

- [Alpaca SDK](alpaca_sdk.md):历史 bars / quotes / trades / auctions、快照、元数据、新闻(REST)
- [Alpaca Stream](alpaca_stream.md):股票流 / 新闻流(WebSocket,自动重连 + 保活 + 订阅重放)
- [Finviz SDK](finviz_sdk.md):筛选器导出 / 财报日历 / 经济日历(CSV)

## 工作区结构

```
AnySDK/
├── src/lib.rs        # 入口 crate「anysdk」:utils 默认引入,feature 选择 SDK
├── crates/
│   ├── utils/         # 核心架构:Method / HttpResponse / HttpExecutor<T> / Client<T, S, E>
│   ├── alpaca_sdk/    # Alpaca 市场数据 SDK(HTTP/REST)
│   ├── alpaca_stream/ # Alpaca WebSocket 实时流(自动重连 + 保活)
│   ├── finviz_sdk/    # Finviz Elite 数据 SDK
│   └── test/          # 内部集成测试 / 示例程序(reqwest 执行器示范)
├── docs/             # 本文档站点(docsify)
├── Cargo.toml        # workspace 配置 + anysdk 入口包
└── README.md
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
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }
```

| feature | 引入的模块 |
| --- | --- |
| (默认,不加 feature) | `anysdk::utils` —— 仅核心架构 |
| `alpaca` | `anysdk::alpaca_sdk`(HTTP/REST) |
| `alpaca-stream` | `anysdk::alpaca_stream`(WS 实时流,隐含 `alpaca`) |
| `finviz` | `anysdk::finviz_sdk` |
| `full` | 全部启用 |

### 接入执行器(一次性)

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

### 示例:Alpaca 快照

```rust
use anysdk::alpaca_sdk::{snapshots, Feed};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 超时等传输策略由使用方在自己的 HTTP 客户端上配置
    let client = anysdk::alpaca_sdk::Client::new(
        reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build()?,
        "YOUR_API_KEY",
        "YOUR_API_SECRET",
        ReqwestExecutor,
    );

    let snapshots = client.snapshots(&snapshots::Params {
        symbols: vec!["AAPL".into()],
        ..Default::default()
    }).await?;

    if let Some(bar) = snapshots["AAPL"].daily_bar.as_ref() {
        println!("AAPL 收盘价: {}", bar.close);
    }
    Ok(())
}
```

### 示例:Finviz 筛选器

```rust
use anysdk::finviz_sdk::screener::{Parameter, ScreenerType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = anysdk::finviz_sdk::Client::new(
        reqwest::Client::new(),
        "YOUR_FINVIZ_ELITE_AUTH_TOKEN",
        ReqwestExecutor,
    );

    let csv = client.screener(Parameter {
        screener_type: ScreenerType::Stocks,
        order_by: "-marketcap".into(),
        filters: Some("exch_nyse".into()),
        columns: Some("0,1,65,66".into()),
        signal: None,
    }).await?;
    println!("{:?}", csv.headers);
    Ok(())
}
```

## 环境变量

建议将密钥通过环境变量注入,避免硬编码:`ALPACA_API_KEY` / `ALPACA_API_SECRET` / `FINVIZ_ELITE_AUTH`。

## License

本项目采用 [GPL-3.0-only](../LICENSE) 许可证。

> 本 SDK 仅用于数据访问,不构成任何投资建议。使用前请遵守各数据源的服务条款。
