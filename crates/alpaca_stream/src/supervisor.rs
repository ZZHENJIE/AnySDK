//! 共享监督循环:每条流一个 supervisor 任务,拥有连接生命周期与期望订阅状态。
//!
//! ```text
//! connect(≤CONNECT_TIMEOUT) → 握手 → 重放期望订阅 → Connected
//!   → writer(转发 JSON / PING_INTERVAL 发 Ping)+ reader(解码→broadcast,
//!     维护 last_seen,Ping→Pong)
//!   → select! { 连接死亡 | 显式 close | 看门狗(下行静默 > STALE_AFTER) }
//!   → 死亡 → Reconnecting{attempt, delay} → 退避(可被 close / action 打断)
//!   → 重连;close → abort 连接任务 → Closed → supervisor 退出
//! ```
//!
//! 退避期间仍会消费 action 通道:订阅变更只更新期望状态,不影响退避时长。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{SinkExt as _, StreamExt as _};
use serde::de::DeserializeOwned;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::status::StreamStatus;
use crate::ws::{self, Fatal, WsReader, WsWriter};

/// 重连退避:首次 1s,指数翻倍,封顶 [`BACKOFF_MAX`]。
pub(crate) const BACKOFF_INITIAL: u64 = 1;
pub(crate) const BACKOFF_MAX: u64 = 60;

/// 看门狗巡检间隔(`STALE_AFTER` 的粗粒度检查点)。
const WATCHDOG_TICK: Duration = Duration::from_secs(5);

/// writer 任务消费的上行帧。
pub(crate) enum FrameAction {
    /// 发送一条文本帧(协议 JSON)。
    Send(String),
    /// 回应服务端的 WS ping 帧。
    Pong,
}

/// 每条流的协议胶水:URL / 握手 / 期望状态的变更与重放。
pub(crate) trait Protocol: Send + Sync + 'static {
    /// 线上消息类型(broadcast 分发)。
    type Message: DeserializeOwned + Clone + Send + Sync + 'static;
    /// 调用方经 handle 请求的动作(如 subscribe / unsubscribe)。
    type Action: Send + 'static;
    /// supervisor 持有的期望状态快照,重连成功后整体重放。
    type Desired: Send + Sync + 'static;

    fn url(&self) -> String;

    /// SOCKS5 代理地址(`socks5://host:port`);`None` = 直连。代理是
    /// [`ws::connect_ws`] 内部的建链细节,重连自动走同一配置。
    fn proxy(&self) -> Option<String> {
        None
    }

    /// 升级请求附加头(新闻流放 APCA key;股票流为空)。
    fn upgrade_headers(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    /// 连接建立后的握手(股票流:auth action;新闻流:等服务端确认)。
    async fn handshake(&self, write: &mut WsWriter, read: &mut WsReader) -> anyhow::Result<()>;

    /// (重)连接成功后,按期望状态生成需要下发的 JSON 帧。
    fn replay(desired: &Self::Desired) -> Vec<String>;

    /// 把一个动作合并进期望状态;返回需要**立即**下发的 JSON 帧
    /// (断线期间返回值会被丢弃,靠重放恢复)。
    fn apply(desired: &mut Self::Desired, action: &Self::Action) -> Vec<String>;
}

/// 重连退避曲线:`BACKOFF_INITIAL << (attempt-1)`,封顶 `BACKOFF_MAX`(秒)。
fn backoff_delay(attempt: u32) -> Duration {
    let secs = BACKOFF_INITIAL
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(BACKOFF_MAX)
        .clamp(BACKOFF_INITIAL, BACKOFF_MAX);
    Duration::from_secs(secs)
}

/// 单次连接尝试(TCP/TLS/升级 + 握手)整体受 [`ws::CONNECT_TIMEOUT`] 约束。
pub(crate) async fn attempt_connect<P: Protocol>(
    protocol: &P,
) -> anyhow::Result<(WsWriter, WsReader)> {
    match tokio::time::timeout(
        ws::CONNECT_TIMEOUT,
        async {
            let (mut write, mut read) = ws::connect_ws(
                &protocol.url(),
                &protocol.upgrade_headers(),
                protocol.proxy().as_deref(),
            )
            .await?;
            protocol.handshake(&mut write, &mut read).await?;
            Ok((write, read))
        },
    )
    .await
    {
        Ok(result) => result,
        Err(_) => anyhow::bail!("connect timed out after {:?}", ws::CONNECT_TIMEOUT),
    }
}

/// 监督主循环。`initial` 是 [`crate::stocks::StockStream::connect`] 等入口
/// 同步建立的首连(supervisor 直接接管,不再重连一次)。
pub(crate) async fn run<P: Protocol>(
    protocol: Arc<P>,
    desired: Arc<Mutex<P::Desired>>,
    close: CancellationToken,
    mut actions: mpsc::Receiver<P::Action>,
    messages: broadcast::Sender<P::Message>,
    status: broadcast::Sender<StreamStatus>,
    mut initial: Option<(WsWriter, WsReader)>,
) {
    let mut attempt: u32 = 0;
    'supervisor: loop {
        // 取一条连接:首连直接接管;否则先退避(期间继续更新期望状态)再连。
        let connection = match initial.take() {
            Some(pair) => Ok(pair),
            None => {
                attempt += 1;
                let delay = backoff_delay(attempt);
                let _ = status.send(StreamStatus::Reconnecting { attempt, delay });
                let deadline = tokio::time::Instant::now() + delay;
                let mut cancelled = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = close.cancelled() => { cancelled = true; break; }
                        Some(action) = actions.recv() => {
                            let _ = P::apply(&mut desired.lock().unwrap(), &action);
                        }
                        _ = tokio::time::sleep_until(deadline) => break,
                    }
                }
                if cancelled {
                    break 'supervisor;
                }
                attempt_connect(protocol.as_ref()).await
            }
        };

        match connection {
            Ok((write, read)) => {
                attempt = 0;
                run_session::<P>(
                    write,
                    read,
                    &mut actions,
                    &desired,
                    &close,
                    &messages,
                    &status,
                )
                .await;
                if close.is_cancelled() {
                    break 'supervisor;
                }
            }
            Err(err) => {
                if let Some(fatal) = err.downcast_ref::<Fatal>() {
                    // 永久性错误(认证 / 权限 / 连接数限制):重连不可能成功。
                    warn!("stream fatal error, giving up: {}", fatal.0);
                    let _ = status.send(StreamStatus::Failed {
                        message: fatal.0.clone(),
                    });
                    return;
                }
                warn!("stream connect failed (attempt {attempt}): {err:#}");
            }
        }
    }
    let _ = status.send(StreamStatus::Closed);
}

/// 一条已握手的连接:双任务 + 重放 + 会话循环;退出时清理所有连接任务。
/// (协议操作全部是 `P::` 关联函数,无需实例。)
async fn run_session<P: Protocol>(
    write: WsWriter,
    read: WsReader,
    actions: &mut mpsc::Receiver<P::Action>,
    desired: &Arc<Mutex<P::Desired>>,
    close: &CancellationToken,
    messages: &broadcast::Sender<P::Message>,
    status: &broadcast::Sender<StreamStatus>,
) {
    let conn_alive = CancellationToken::new();
    let (frame_tx, frame_rx) = mpsc::channel::<FrameAction>(32);
    let last_seen = Arc::new(Mutex::new(Instant::now()));

    let writer = tokio::spawn(writer_task(write, frame_rx, conn_alive.clone()));
    let reader = tokio::spawn(reader_task(
        read,
        frame_tx.clone(),
        messages.clone(),
        last_seen.clone(),
        conn_alive.clone(),
    ));

    // 重放期望订阅(断线期间的变更已合入 desired)。
    let replay_frames = P::replay(&desired.lock().unwrap());
    for frame in replay_frames {
        let _ = frame_tx.send(FrameAction::Send(frame)).await;
    }
    let _ = status.send(StreamStatus::Connected);

    // 会话循环:连接死亡 / 显式关闭 / 下行静默三者退出;平时消化动作。
    let mut watchdog = tokio::time::interval(WATCHDOG_TICK);
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = close.cancelled() => break,
            _ = conn_alive.cancelled() => break,
            Some(action) = actions.recv() => {
                let frames = P::apply(&mut desired.lock().unwrap(), &action);
                for frame in frames {
                    let _ = frame_tx.send(FrameAction::Send(frame)).await;
                }
            }
            _ = watchdog.tick() => {
                let silent_for = last_seen.lock().unwrap().elapsed();
                if silent_for > ws::STALE_AFTER {
                    warn!("stream silent for {silent_for:?}; forcing reconnect");
                    break;
                }
            }
        }
    }

    // 清理本条连接的任务(writer/reader 也会自发取消,这里兜底)。
    conn_alive.cancel();
    writer.abort();
    reader.abort();
    let _ = writer.await;
    let _ = reader.await;
}

async fn writer_task(
    mut write: WsWriter,
    mut frames: mpsc::Receiver<FrameAction>,
    conn_alive: CancellationToken,
) {
    let mut ping = tokio::time::interval(ws::PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = conn_alive.cancelled() => break,
            frame = frames.recv() => match frame {
                Some(FrameAction::Send(text)) => {
                    if write.send(Message::text(text)).await.is_err() {
                        break;
                    }
                }
                Some(FrameAction::Pong) => {
                    if write.send(Message::Pong(Vec::new().into())).await.is_err() {
                        break;
                    }
                }
                // 上游关闭:连接拆除。
                None => break,
            },
            _ = ping.tick() => {
                if write.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
        }
    }
    conn_alive.cancel();
}

async fn reader_task<M: DeserializeOwned + Clone + Send + Sync + 'static>(
    mut read: WsReader,
    frames: mpsc::Sender<FrameAction>,
    messages: broadcast::Sender<M>,
    last_seen: Arc<Mutex<Instant>>,
    conn_alive: CancellationToken,
) {
    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                mark_seen(&last_seen);
                for msg in parse_messages::<M>(text.as_str()) {
                    let _ = messages.send(msg);
                }
            }
            // 显式回 Pong,不假设底层库的自动回应行为。
            Some(Ok(Message::Ping(_))) => {
                mark_seen(&last_seen);
                let _ = frames.send(FrameAction::Pong).await;
            }
            Some(Ok(Message::Pong(_))) => mark_seen(&last_seen),
            // 非 ping/pong/文本帧不承载协议内容。
            Some(Ok(_)) => {}
            Some(Err(err)) => {
                warn!("stream read failed: {err}");
                break;
            }
            None => break,
        }
    }
    conn_alive.cancel();
}

fn mark_seen(last_seen: &Mutex<Instant>) {
    *last_seen.lock().unwrap() = Instant::now();
}

/// Decode a frame's message array; unknown entries are skipped with a warning
/// so one bad message never kills the stream.
fn parse_messages<T: DeserializeOwned>(text: &str) -> Vec<T> {
    let Ok(array) = serde_json::from_str::<Vec<serde_json::Value>>(text) else {
        warn!("unrecognized stream frame: {text}");
        return Vec::new();
    };
    array
        .into_iter()
        .filter_map(|value| match serde_json::from_value(value) {
            Ok(msg) => Some(msg),
            Err(err) => {
                warn!("skipping stream message: {err}");
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_curve_doubles_and_caps() {
        let secs = |attempt: u32| backoff_delay(attempt).as_secs();
        assert_eq!(secs(1), 1);
        assert_eq!(secs(2), 2);
        assert_eq!(secs(3), 4);
        assert_eq!(secs(4), 8);
        assert_eq!(secs(5), 16);
        assert_eq!(secs(6), 32);
        // 64 封顶为 60,之后保持。
        assert_eq!(secs(7), 60);
        assert_eq!(secs(8), 60);
        assert_eq!(secs(1_000), 60);
    }

    #[test]
    fn backoff_never_below_initial() {
        assert_eq!(backoff_delay(0), Duration::from_secs(BACKOFF_INITIAL));
    }
}
