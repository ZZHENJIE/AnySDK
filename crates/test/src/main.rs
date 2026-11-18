//! 内部集成测试 / 示例程序(需要真实网络与密钥)。
//!
//! 演示新架构的接入方式:为自己的 HTTP 客户端(`reqwest::Client`)实现一次
//! [`HttpExecutor`],即可驱动全部 SDK;WebSocket 流由 SDK 内置模块提供。
//!
//! 用法:
//! - `test list`           列出所有端点
//! - `test <endpoint>`     运行单个端点
//! - `test all`            运行全部端点
//!
//! 密钥通过环境变量或项目根 `.env` 注入(已设置的环境变量优先):
//! `ALPACA_API_KEY` / `ALPACA_API_SECRET` / `FINVIZ_ELITE_AUTH`。

use std::collections::BTreeMap;
use std::time::Duration;

use alpaca_sdk::{Feed, bars, news, quotes, snapshots};
use finviz_sdk::calendar::economic::Parameter as EconomicParameter;
use finviz_sdk::screener::{Parameter, ScreenerType};
use utils::{HttpExecutor, HttpResponse, Method};

// ---------- 执行器:唯一的 reqwest 落点 ----------

/// 把 `reqwest::Client` 适配成 SDK 可用的执行器:只负责传输(状态码 + 响应体)。
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
        let status = response.status().as_u16();
        let body = response.bytes().await?;
        Ok(HttpResponse {
            status,
            body: body.to_vec(),
        })
    }
}

// ---------- 密钥与客户端 ----------

/// Environment variable takes precedence; error out when unset.
fn secret(env_var: &str) -> anyhow::Result<String> {
    std::env::var(env_var).map_err(|_| anyhow::anyhow!("missing environment variable {env_var}"))
}

fn alpaca_api_key() -> anyhow::Result<String> {
    secret("ALPACA_API_KEY")
}

fn alpaca_api_secret() -> anyhow::Result<String> {
    secret("ALPACA_API_SECRET")
}

fn finviz_auth() -> anyhow::Result<String> {
    secret("FINVIZ_ELITE_AUTH")
}

fn alpaca_client() -> anyhow::Result<alpaca_sdk::Client<reqwest::Client, ReqwestExecutor>> {
    Ok(alpaca_sdk::Client::new(
        reqwest::Client::new(),
        alpaca_api_key()?,
        alpaca_api_secret()?,
        ReqwestExecutor,
    ))
}

fn finviz_client() -> anyhow::Result<finviz_sdk::Client<reqwest::Client, ReqwestExecutor>> {
    Ok(finviz_sdk::Client::new(
        reqwest::Client::new(),
        &finviz_auth()?,
        ReqwestExecutor,
    ))
}

// ---------- Alpaca 端点 ----------

async fn run_alpaca_snapshot() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let snapshots = client
        .snapshots(&snapshots::Params {
            symbols: vec!["AAPL".into(), "MSFT".into()],
            ..Default::default()
        })
        .await?;
    for (symbol, snapshot) in &snapshots {
        let close = snapshot.daily_bar.as_ref().map(|bar| bar.close);
        println!("{symbol}: daily close = {close:?}");
    }
    Ok(())
}

async fn run_alpaca_snapshot_single() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let snapshot = client.snapshot("AAPL", &Default::default()).await?;
    println!("{snapshot:#?}");
    Ok(())
}

async fn run_alpaca_bars() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let page = client
        .bars(&bars::Params {
            symbols: vec!["AAPL".into(), "MSFT".into()],
            timeframe: bars::timeframe::DAY_1.into(),
            limit: Some(5),
            ..Default::default()
        })
        .await?;
    for (symbol, bars) in &page.bars {
        println!(
            "{symbol}: {} bars, first close = {:?}",
            bars.len(),
            bars.first().map(|b| b.close)
        );
    }
    println!("next page: {:?}", page.next_page_token);
    Ok(())
}

async fn run_alpaca_bars_single_all() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let all = client
        .bars_single_all(
            "AAPL",
            &bars::SingleParams {
                timeframe: bars::timeframe::DAY_1.into(),
                start: Some("2026-01-02".into()),
                end: Some("2026-01-10".into()),
                ..Default::default()
            },
        )
        .await?;
    println!("{} bars total", all.len());
    Ok(())
}

async fn run_alpaca_latest_quotes() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let latest = client
        .latest_quotes(&quotes::LatestParams {
            symbols: vec!["AAPL".into()],
            ..Default::default()
        })
        .await?;
    for (symbol, quote) in &latest.quotes {
        println!(
            "{symbol}: bid {} x ask {}",
            quote.bid_price, quote.ask_price
        );
    }
    Ok(())
}

async fn run_alpaca_trades_all() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let all = client
        .trades_all(&alpaca_sdk::trades::Params {
            symbols: vec!["AAPL".into()],
            limit: Some(100),
            ..Default::default()
        })
        .await?;
    for (symbol, trades) in &all {
        println!("{symbol}: {} trades", trades.len());
    }
    Ok(())
}

async fn run_alpaca_exchange_codes() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let codes = client.exchange_codes().await?;
    for (code, name) in codes.iter().take(10) {
        println!("{code} = {name}");
    }
    println!("({} exchanges total)", codes.len());
    Ok(())
}

async fn run_alpaca_condition_codes() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let codes = client
        .condition_codes(alpaca_sdk::meta::TickType::Trades)
        .await?;
    for (code, name) in codes.iter().take(10) {
        println!("{code} = {name}");
    }
    Ok(())
}

async fn run_alpaca_news() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let page = client
        .news(&news::Params {
            symbols: vec!["AAPL".into()],
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    for article in &page.news {
        println!("[{}] {}", article.created_at, article.headline);
    }
    Ok(())
}

async fn run_alpaca_news_all() -> anyhow::Result<()> {
    let client = alpaca_client()?;
    let all = client.news_all(&news::Params::default()).await?;
    println!("{} articles total", all.len());
    Ok(())
}

/// 本地设备时间,用于对照行情时间戳观察链路停顿。
fn local_now() -> String {
    chrono::Local::now().format("%H:%M:%S%.3f").to_string()
}

async fn run_alpaca_stock_stream() -> anyhow::Result<()> {
    let stream =
        alpaca_stream::StockStream::connect(Feed::Sip, alpaca_api_key()?, alpaca_api_secret()?)
            .await?;

    // 连接性观察:状态事件带本地时间打印,便于和重连时刻对照。
    let mut status_rx = stream.status();
    tokio::spawn(async move {
        while let Ok(event) = status_rx.recv().await {
            println!("[{}] [status] {event:?}", local_now());
        }
    });

    // quotes 盘前也持续有流,适合任意时段冒烟(trade 盘中才有)。
    stream
        .subscribe(&alpaca_stream::StockSubscription {
            quotes: vec!["SPY".into()],
            ..Default::default()
        })
        .await?;

    // 持续打印行情,不自动结束(Ctrl+C 退出);方括号内是本地设备时间。
    let mut receiver = stream.messages();
    loop {
        match receiver.recv().await {
            Ok(alpaca_stream::StockMessage::Quote(q)) => println!(
                "[{}] SPY bid {:.2}({})x{} / ask {:.2}({})x{} | feed {}",
                local_now(),
                q.bid_price,
                q.bid_exchange,
                q.bid_size,
                q.ask_price,
                q.ask_exchange,
                q.ask_size,
                q.time.format("%H:%M:%S%.6fZ")
            ),
            Ok(alpaca_stream::StockMessage::Subscription(_)) => {}
            // broadcast 落后(打印太慢被丢帧)只提示,不断流。
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                println!("[{}] [warn] receiver lagged, dropped {n} messages", local_now());
            }
            Ok(msg) => println!("[{}] {msg:?}", local_now()),
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                anyhow::bail!("stream closed")
            }
        }
    }
}

async fn run_alpaca_news_stream() -> anyhow::Result<()> {
    let stream =
        alpaca_stream::NewsStream::connect(alpaca_api_key()?, alpaca_api_secret()?).await?;

    let mut status_rx = stream.status();
    tokio::spawn(async move {
        while let Ok(event) = status_rx.recv().await {
            println!("[status] {event:?}");
        }
    });

    stream.subscribe_all().await?;
    let mut receiver = stream.messages();
    for _ in 0..2 {
        let message = tokio::time::timeout(Duration::from_secs(60), receiver.recv()).await;
        match message {
            Ok(Ok(msg)) => println!("{msg:?}"),
            Ok(Err(err)) => anyhow::bail!("stream closed: {err}"),
            Err(_) => anyhow::bail!("timed out waiting for stream messages"),
        }
    }
    stream.close();
    Ok(())
}

// ---------- WS 链路延迟测量 ----------

/// 滚动延迟统计:min / avg / max,单位毫秒。
#[derive(Default)]
struct LagStats {
    count: u64,
    sum_ms: f64,
    min_ms: f64,
    max_ms: f64,
    /// 超过 1s 的尖峰次数(链路卡顿的直观信号)。
    spikes: u64,
}

impl LagStats {
    fn record(&mut self, lag_ms: f64) {
        self.count += 1;
        self.sum_ms += lag_ms;
        self.min_ms = if self.count == 1 { lag_ms } else { self.min_ms.min(lag_ms) };
        self.max_ms = self.max_ms.max(lag_ms);
        if lag_ms > 1000.0 {
            self.spikes += 1;
            println!(
                "[{}] [spike] lag {:.0} ms (>1s)",
                local_now(),
                lag_ms
            );
        }
    }

    fn summary(&self) -> String {
        if self.count == 0 {
            return "no messages".into();
        }
        format!(
            "n={} avg={:.0}ms min={:.0}ms max={:.0}ms spikes>1s={}",
            self.count,
            self.sum_ms / self.count as f64,
            self.min_ms,
            self.max_ms,
            self.spikes
        )
    }
}

/// 行情 WS 链路端到端延迟测量:订阅 SPY quotes,逐条计算
/// `本地接收时刻 − 交易所时间戳`,每 5 秒打印滚动统计,Ctrl+C 退出。
///
/// 路由 A/B 对比:
/// - 设置环境变量 `LAG_PROXY=socks5://host:port` → 经 SOCKS5 代理;
/// - 不设置 → 直连。
/// `LAG_SECS`(默认 60)控制运行时长,到时打印总结后退出。
async fn run_alpaca_lag_test() -> anyhow::Result<()> {
    let secs: u64 = std::env::var("LAG_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let proxy = std::env::var("LAG_PROXY").ok();
    let route = match &proxy {
        Some(p) => format!("proxy {p}"),
        None => "direct".to_string(),
    };

    let stream = alpaca_stream::StockStream::connect_with_url_and_proxy(
        alpaca_stream::STOCK_STREAM_URL,
        Feed::Sip,
        alpaca_api_key()?,
        alpaca_api_secret()?,
        proxy,
    )
    .await?;
    println!("[{}] connected via {route}", local_now());

    let mut status_rx = stream.status();
    tokio::spawn(async move {
        while let Ok(event) = status_rx.recv().await {
            println!("[{}] [status] {event:?}", local_now());
        }
    });

    stream
        .subscribe(&alpaca_stream::StockSubscription {
            quotes: vec!["SPY".into()],
            ..Default::default()
        })
        .await?;

    let mut stats = LagStats::default();
    let mut receiver = stream.messages();
    let mut report = tokio::time::interval(Duration::from_secs(5));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                println!("[{}] === {secs}s summary ({route}): {} ===", local_now(), stats.summary());
                stream.close();
                return Ok(());
            }
            _ = report.tick() => {
                println!("[{}] rolling: {}", local_now(), stats.summary());
            }
            message = receiver.recv() => match message {
                Ok(alpaca_stream::StockMessage::Quote(q)) => {
                    let lag_ms = (chrono::Utc::now() - q.time).num_milliseconds() as f64;
                    // 负值 = 本机时钟快于交易所钟(NTP 误差),按 0 计。
                    stats.record(lag_ms.max(0.0));
                }
                Ok(alpaca_stream::StockMessage::Subscription(_)) => {}
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    println!("[{}] [warn] receiver lagged, dropped {n}", local_now());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    anyhow::bail!("stream closed")
                }
            },
        }
    }
}

// ---------- Finviz 端点 ----------

async fn run_finviz_screener() -> anyhow::Result<()> {
    let client = finviz_client()?;
    let result = client
        .screener(Parameter {
            screener_type: ScreenerType::Stocks,
            order_by: "-marketcap".into(),
            filters: Some("exch_nyse".into()),
            columns: Some("0,1,6,65,66".into()),
            signal: None,
        })
        .await?;
    println!("columns: {:?}", result.headers);
    for row in result.rows.iter().take(5) {
        println!("{row:?}");
    }
    Ok(())
}

async fn run_finviz_market_screener() -> anyhow::Result<()> {
    let client = finviz_client()?;
    let result = client
        .market_screener(Parameter {
            order_by: "-volume".into(),
            ..Default::default()
        })
        .await?;
    for row in result.rows.iter().take(10) {
        println!("{row:?}");
    }
    Ok(())
}

async fn run_finviz_earnings_calendar() -> anyhow::Result<()> {
    let client = finviz_client()?;
    let items = client.earnings_calendar("2026-01-14").await?;
    for item in items.iter().take(10) {
        println!(
            "{} {}: EPS est {:?}",
            item.date, item.symbol, item.eps_estimate
        );
    }
    Ok(())
}

async fn run_finviz_economic_calendar() -> anyhow::Result<()> {
    let client = finviz_client()?;
    let items = client
        .economic_calendar(EconomicParameter::new("2026-01-12", "2026-01-16"))
        .await?;
    for item in items.iter().take(10) {
        println!(
            "{} {} {} (impact {}) actual {:?}",
            item.date, item.time, item.event, item.impact, item.actual
        );
    }
    Ok(())
}

// ---------- Registry ----------

#[tokio::main]
async fn main() {
    // Load .env from the project root; already-set env vars take precedence.
    dotenvy::dotenv().ok();

    let args: Vec<String> = std::env::args().skip(1).collect();

    let registry = registry();

    if args.is_empty() || args[0] == "--help" || args[0] == "-h" || args[0] == "list" {
        println!("Usage: test <endpoint-name>\n\nAvailable endpoints:");
        for name in registry.keys() {
            println!("  {name}");
        }
        println!("\n  all  (run every endpoint)");
        return;
    }

    if args[0] == "all" {
        let mut failed = Vec::new();
        for (name, runner) in &registry {
            println!("=== {name} ===");
            if let Err(e) = runner().await {
                eprintln!("FAILED: {e:#}");
                failed.push(*name);
            }
        }
        if !failed.is_empty() {
            eprintln!("{} endpoint(s) failed: {}", failed.len(), failed.join(", "));
            std::process::exit(1);
        }
        return;
    }

    let Some(runner) = registry.get(args[0].as_str()) else {
        eprintln!(
            "Unknown endpoint: {}\nUse `test list` to see available endpoints",
            args[0]
        );
        std::process::exit(1);
    };

    match runner().await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Test failed: {e:#}");
            std::process::exit(1);
        }
    }
}

type Runner = fn() -> futures::future::BoxFuture<'static, anyhow::Result<()>>;

fn registry() -> BTreeMap<&'static str, Runner> {
    BTreeMap::from([
        (
            "alpaca:snapshot",
            (|| Box::pin(run_alpaca_snapshot()) as _) as Runner,
        ),
        (
            "alpaca:snapshot_single",
            (|| Box::pin(run_alpaca_snapshot_single()) as _) as Runner,
        ),
        (
            "alpaca:bars",
            (|| Box::pin(run_alpaca_bars()) as _) as Runner,
        ),
        (
            "alpaca:bars_single_all",
            (|| Box::pin(run_alpaca_bars_single_all()) as _) as Runner,
        ),
        (
            "alpaca:latest_quotes",
            (|| Box::pin(run_alpaca_latest_quotes()) as _) as Runner,
        ),
        (
            "alpaca:trades_all",
            (|| Box::pin(run_alpaca_trades_all()) as _) as Runner,
        ),
        (
            "alpaca:exchange_codes",
            (|| Box::pin(run_alpaca_exchange_codes()) as _) as Runner,
        ),
        (
            "alpaca:condition_codes",
            (|| Box::pin(run_alpaca_condition_codes()) as _) as Runner,
        ),
        (
            "alpaca:news",
            (|| Box::pin(run_alpaca_news()) as _) as Runner,
        ),
        (
            "alpaca:news_all",
            (|| Box::pin(run_alpaca_news_all()) as _) as Runner,
        ),
        (
            "alpaca:stock_stream",
            (|| Box::pin(run_alpaca_stock_stream()) as _) as Runner,
        ),
        (
            "alpaca:lag_test",
            (|| Box::pin(run_alpaca_lag_test()) as _) as Runner,
        ),
        (
            "alpaca:news_stream",
            (|| Box::pin(run_alpaca_news_stream()) as _) as Runner,
        ),
        (
            "finviz:screener",
            (|| Box::pin(run_finviz_screener()) as _) as Runner,
        ),
        (
            "finviz:market_screener",
            (|| Box::pin(run_finviz_market_screener()) as _) as Runner,
        ),
        (
            "finviz:earnings_calendar",
            (|| Box::pin(run_finviz_earnings_calendar()) as _) as Runner,
        ),
        (
            "finviz:economic_calendar",
            (|| Box::pin(run_finviz_economic_calendar()) as _) as Runner,
        ),
    ])
}
