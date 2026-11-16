# Alpaca Stream(实时流)

`alpaca_stream` 提供 Alpaca 的实时 WebSocket 流:**自愈式长连接** —— 断线自动重连(指数退避)、主动保活、订阅自动重放、独立连接状态通道。

> 依赖入口:`anysdk = { git = "...", features = ["alpaca-stream"] }`(`alpaca-stream` 隐含 `alpaca`);或直接引 `alpaca_stream` crate。接入 `HttpExecutor` 见[首页](/)。

## 两条流

| 流 | 认证方式 | 频道 |
| --- | --- | --- |
| [`StockStream`](#股票流-stockstream) | 连接后发 `auth` action | trades / quotes / minute·daily·updated bars / corrections / cancel-errors / LULD / trading status / imbalances |
| [`NewsStream`](#新闻流-newsstream) | 升级请求头(APCA key) | 新闻文章 |

## 连接模型

Alpaca 流式 API 的模型是**一条连接承载多个订阅**:

- 同一 feed 只需**一条连接**:一个 `StockStream` 实例,用 `subscribe` / `unsubscribe` 在这条连接上随时增删频道与符号;
- 多个消费者(如多个 GUI 面板)用 `Clone` 共享同一条连接,而不是各自 `connect`;
- **不要对同一 feed 重复 `connect()`** —— 每条连接都计入平台的并发连接限制;
- 股票流与新闻流是两个不同端点,各自一条连接(`StockStream` + `NewsStream`)。

## 架构

```
StockStream / NewsStream 句柄(Clone 廉价,最后一个 drop 自动关闭)
├── actions     → mpsc    → supervisor(订阅变更不直连 socket)
├── messages()  → broadcast(4096) → 类型化消息
├── status()    → broadcast(16)   → StreamStatus 事件
└── close()     → CancellationToken(幂等)
```

每条流一个 **supervisor 任务**,持有连接生命周期与**期望订阅状态**:

1. 连接 + 握手(整体 ≤ `CONNECT_TIMEOUT` = 15s,防火墙黑洞表现为普通错误)
2. 重放期望订阅 → 广播 `Connected`
3. 会话:writer 每 `PING_INTERVAL`(25s)发 WS ping 保活;reader 解码消息并维护最近收帧时间
4. 连接死亡或下行静默(`STALE_AFTER` = 90s 无任何帧)→ 广播 `Reconnecting{attempt, delay}`
5. 指数退避重连:1s → 2s → 4s → … → 60s 封顶;重连成功后自动重放订阅并清零退避
6. 握手阶段收到服务端 **error 帧**(认证失败、订阅不含该 feed、连接数超限等)→ 视为**永久性错误**:广播 `Failed{message}` 并停止重连(重试不可能成功)

## 股票流(StockStream)

```rust
use anysdk::alpaca_sdk::Feed;
use anysdk::alpaca_stream::{StockMessage, StockStream, StockSubscription};

let stream = StockStream::connect(Feed::Iex, "YOUR_API_KEY", "YOUR_API_SECRET").await?;

// 连接状态事件(GUI 可展示)
let mut status = stream.status();
tokio::spawn(async move {
    while let Ok(event) = status.recv().await {
        println!("[status] {event:?}");   // Connected / Reconnecting{attempt,delay} / Closed
    }
});

stream.subscribe(&StockSubscription {
    trades: vec!["AAPL".into()],      // "*" 订阅全部
    quotes: vec![],
    bars: vec![],                     // 分钟 bar
    updated_bars: vec![],
    daily_bars: vec![],
    statuses: vec![],
    lulds: vec![],
    imbalances: vec![],
}).await?;

let mut rx = stream.messages();
while let Ok(msg) = rx.recv().await {
    match msg {
        StockMessage::Trade(t) => println!("{} {} x {}", t.symbol, t.size, t.price),
        StockMessage::Quote(q) => println!("{} bid {} / ask {}", q.symbol, q.bid_price, q.ask_price),
        _ => {}
    }
}

// 增量退订(socket 保持);自定义端点用 connect_with_url
stream.unsubscribe(&sub).await?;
stream.close(); // 显式关闭(幂等);不调用则重连直到最后一个句柄 drop
```

`Feed`:`Sip` / `Iex` / `Otc`(WS 不可用)/ `DelayedSip` / `Boats` / `Overnight`。

## 代理(SOCKS5)

WS 的 TCP 链路可以经过 **SOCKS5 代理**建立(直连不稳 / 本地 DNS 被污染时有用):
代理只介入 TCP 建连阶段,目标域名交给**代理端**解析,其上的 TLS 与 WS 升级、
握手、订阅重放完全不变;断线重连自动走同一代理配置。

```rust
use anysdk::alpaca_stream::{NEWS_STREAM_URL, STOCK_STREAM_URL};

// 显式指定代理;None = 直连
let stream = StockStream::connect_with_url_and_proxy(
    STOCK_STREAM_URL, Feed::Iex, "KEY", "SECRET",
    Some("socks5://127.0.0.1:7890".into()),
).await?;

let news = NewsStream::connect_to_with_proxy(
    NEWS_STREAM_URL, "KEY", "SECRET",
    Some("socks5://127.0.0.1:7890".into()),
).await?;
```

规则:

- 仅支持 `socks5://host:port`(接受 `socks5h://`,语义相同),**不支持认证**;
  scheme 或地址非法时首连直接报错(fail-fast)。
- 显式参数**优先**于环境变量;未提供时读 `ALL_PROXY` / `all_proxy` 兜底——
  仅当值为 `socks5://` / `socks5h://` 时生效,其他 scheme(如 `http://…`)
  **静默忽略保持直连**,不会因其他工具设置的变量弄断流。
- 代理握手耗时计入单次连接的 `CONNECT_TIMEOUT`(15s),不单独放宽。

## 新闻流(NewsStream)

```rust
use anysdk::alpaca_stream::{NewsMessage, NewsStream};

let stream = NewsStream::connect("YOUR_API_KEY", "YOUR_API_SECRET").await?;
stream.subscribe_all().await?;               // ["*"];也可 subscribe(&["AAPL", ...])

let mut rx = stream.messages();
while let Ok(msg) = rx.recv().await {
    if let NewsMessage::News(article) = msg {
        println!("[{}] {}", article.created_at, article.headline);
    }
}
```

## 行为细则

- **状态事件**(`status()`):`Connected` / `Reconnecting{attempt, delay}` / `Failed{message}`(永久性错误,已停止重连)/ `Closed`(正常关闭)。
- **首连同步失败即 `Err`**(fail-fast)—— 包括权限类 error 帧;之后断线由 supervisor 在后台自愈,调用方无感,但**永久性错误不再重试**。
- **断线期间的 subscribe / unsubscribe 只更新期望状态**,重连成功后自动重放(股票流:auth + 全量 subscribe;新闻流:subscribe 当前符号集)。
- `Clone` 共享同一连接;**最后一个句柄 drop 等价于 `close()`**。`death_token` / `clone_handle`(旧 API)已移除,由 `status()` 与 `Clone` 取代。
- 订阅重放前收到的订阅动作会进入期望状态,不会丢失。
- 消息以 JSON 数组到达,解码为 `T` 标签强类型枚举;未知消息跳过并告警,不会杀死流。
- tungstenite 依赖 WS 协议层,SDK 另在应用层显式回应服务端 ping。

## 常量

| 常量 | 值 | 说明 |
| --- | --- | --- |
| `CONNECT_TIMEOUT` | 15s | 单次连接尝试(TCP/TLS/升级 + 握手)硬超时 |
| `PING_INTERVAL` | 25s | 主动 WS ping 保活间隔 |
| `STALE_AFTER` | 90s | 下行静默判定阈值,超过即强制重连 |

## 测试

```bash
cargo test -p alpaca_stream   # 单测(退避曲线 / 订阅合并 / 消息解码)
                              # + 本机 WS 集成测试(断线→重连→重放→关闭,无需外网)
cargo run -p test -- alpaca:stock_stream   # 联网冒烟(需密钥)
```
