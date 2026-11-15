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

use alpaca_sdk::{bars, news, quotes, snapshots, Feed};
use finviz_sdk::screener::{Parameter, ScreenerType};
use finviz_sdk::calendar::economic::Parameter as EconomicParameter;
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
                let mut builder = client
                    .post(&request.url)
                    .body(request.body.clone());
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
        println!("{symbol}: {} bars, first close = {:?}", bars.len(), bars.first().map(|b| b.close));
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
        println!("{symbol}: bid {} x ask {}", quote.bid_price, quote.ask_price);
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

async fn run_alpaca_stock_stream() -> anyhow::Result<()> {
    let stream = alpaca_stream::StockStream::connect(
        Feed::Iex,
        alpaca_api_key()?,
        alpaca_api_secret()?,
    )
    .await?;

    // 状态事件后台打印;消息收 5 条后显式关闭。
    let mut status_rx = stream.status();
    tokio::spawn(async move {
        while let Ok(event) = status_rx.recv().await {
            println!("[status] {event:?}");
        }
    });

    stream
        .subscribe(&alpaca_stream::StockSubscription {
            trades: vec!["AAPL".into()],
            ..Default::default()
        })
        .await?;
    let mut receiver = stream.messages();
    for _ in 0..5 {
        let message = tokio::time::timeout(Duration::from_secs(30), receiver.recv()).await;
        match message {
            Ok(Ok(msg)) => println!("{msg:?}"),
            Ok(Err(err)) => anyhow::bail!("stream closed: {err}"),
            Err(_) => anyhow::bail!("timed out waiting for stream messages"),
        }
    }
    stream.close();
    Ok(())
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
        println!("{} {}: EPS est {:?}", item.date, item.symbol, item.eps_estimate);
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
        ("alpaca:bars", (|| Box::pin(run_alpaca_bars()) as _) as Runner),
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
        ("alpaca:news", (|| Box::pin(run_alpaca_news()) as _) as Runner),
        (
            "alpaca:news_all",
            (|| Box::pin(run_alpaca_news_all()) as _) as Runner,
        ),
        (
            "alpaca:stock_stream",
            (|| Box::pin(run_alpaca_stock_stream()) as _) as Runner,
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
