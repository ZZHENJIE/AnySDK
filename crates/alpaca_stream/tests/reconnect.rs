//! 本机 ws:// 服务端集成测试,验证 supervisor 的全部关键行为:
//! 1. 首连 → 服务端断开 → 自动重连 → 订阅重放 → 显式关闭
//! 2. 握手 error 帧(认证 / 权限类永久错误)→ fail-fast,直接 `Err`
//! 3. 重连阶段遇到 error 帧 → `Failed` 事件,停止重连(不再无限重试)
//!
//! 不依赖外网与密钥。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{SinkExt as _, StreamExt as _};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use alpaca_stream::{NewsStream, StreamStatus};

/// 极简新闻流服务端:
/// - `error_mode` 为真:发 error 帧后断开(模拟认证失败 / 无 feed 权限);
/// - 否则发握手成功帧,读首帧(期望 subscribe)记录到 `seen`,然后保持
///   打开,直到 `kill` 置真或对端断开;
/// - 外层持续 accept(连接断开后客户端重连即可"重启")。
async fn spawn_server(
    listener: TcpListener,
    seen: Arc<Mutex<Vec<String>>>,
    kill: watch::Receiver<bool>,
    error_mode: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => return,
            };
            let seen = seen.clone();
            let mut kill = kill.clone();
            let error_mode = error_mode.clone();
            tokio::spawn(async move {
                // 权限类永久错误窗口:发 error 帧后断开(优先于宕机窗口)。
                if *error_mode.borrow() {
                    let Ok(ws) = accept_async(stream).await else { return };
                    let (mut write, _) = ws.split();
                    let _ = write
                        .send(Message::text(
                            r#"[{"T":"error","code":"402","msg":"insufficient subscription"}]"#,
                        ))
                        .await;
                    return;
                }
                if *kill.borrow() {
                    return; // 宕机窗口内的连接:直接掐断
                }
                let Ok(ws) = accept_async(stream).await else { return };
                let (mut write, mut read) = ws.split();

                // 握手:新闻流等服务端确认。
                if write
                    .send(Message::text(r#"[{"T":"success","msg":"authenticated"}]"#))
                    .await
                    .is_err()
                {
                    return;
                }

                // 首帧 = 客户端的订阅动作(或重连后的重放),记录。
                match read.next().await {
                    Some(Ok(Message::Text(text))) => seen.lock().unwrap().push(text.to_string()),
                    _ => return,
                }

                // 保持连接,直到 kill 置真或对端断开。
                loop {
                    tokio::select! {
                        _ = kill.changed() => { if *kill.borrow() { return; } }
                        frame = read.next() => {
                            if matches!(frame, None | Some(Err(_))) { return; }
                        }
                    }
                }
            });
        }
    });
}

async fn wait_until(
    seen: &Arc<Mutex<Vec<String>>>,
    cond: impl Fn(&[String]) -> bool,
    what: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if cond(&seen.lock().unwrap()) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_status(
    status: &mut broadcast::Receiver<StreamStatus>,
    cond: impl Fn(&StreamStatus) -> bool,
    what: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        match tokio::time::timeout(Duration::from_millis(500), status.recv()).await {
            Ok(Ok(event)) if cond(&event) => return,
            // 其他事件 / 接收方落后:跳过继续等。
            Ok(Ok(_)) => {}
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(_)) => panic!("status channel closed while waiting for {what}"),
            Err(_) => {}
        }
    }
}

#[tokio::test]
async fn reconnects_and_replays_subscription() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (kill_tx, kill_rx) = watch::channel(false);
    let (_error_tx, error_rx) = watch::channel(false);
    spawn_server(listener, seen.clone(), kill_rx, error_rx).await;

    // 首连(supervisor 接管),订阅 AAPL。
    let stream = NewsStream::connect_to(&format!("ws://{addr}"), "key", "secret")
        .await
        .unwrap();
    let mut status = stream.status();
    stream.subscribe(&["AAPL"]).await.unwrap();

    wait_until(&seen, |seen| seen.len() >= 1, "first subscribe").await;
    assert_eq!(
        seen.lock().unwrap()[0],
        r#"{"action":"subscribe","news":["AAPL"]}"#
    );

    // 服务端断开 → 客户端进入重连(指数退避)。
    kill_tx.send(true).unwrap();
    wait_status(&mut status, |s| {
        matches!(
            s,
            StreamStatus::Reconnecting { attempt: 1, delay } if *delay == Duration::from_secs(1)
        )
    }, "Reconnecting with 1s backoff")
    .await;

    // 服务端恢复 → 重连成功,订阅被自动重放。
    tokio::time::sleep(Duration::from_millis(100)).await;
    kill_tx.send(false).unwrap();
    wait_until(&seen, |seen| seen.len() >= 2, "replayed subscribe").await;
    assert_eq!(
        seen.lock().unwrap()[1],
        r#"{"action":"subscribe","news":["AAPL"]}"#
    );
    wait_status(&mut status, |s| matches!(s, StreamStatus::Connected), "Connected after reconnect").await;

    // 显式关闭 → Closed 事件,supervisor 退出。
    stream.close();
    wait_status(&mut status, |s| matches!(s, StreamStatus::Closed), "Closed").await;
}

#[tokio::test]
async fn fatal_handshake_error_fails_fast() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (_kill_tx, kill_rx) = watch::channel(false);
    let (_error_tx, error_rx) = watch::channel(true);
    spawn_server(listener, seen, kill_rx, error_rx).await;

    // 首连即遇权限类 error 帧:fail-fast,直接 Err,不进入重连循环。
    let started = Instant::now();
    let err = NewsStream::connect_to(&format!("ws://{addr}"), "key", "secret")
        .await
        .unwrap_err();

    assert!(started.elapsed() < Duration::from_secs(5), "fail-fast, not retry loop");
    // `{:#}` 展开整个错误链(context + 底层 Fatal 原因)。
    let message = format!("{err:#}");
    assert!(message.contains("initial connect failed"), "{message}");
    assert!(message.contains("insufficient subscription"), "{message}");
}

#[tokio::test]
async fn fatal_handshake_error_stops_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (kill_tx, kill_rx) = watch::channel(false);
    let (error_tx, error_rx) = watch::channel(false);
    spawn_server(listener, seen.clone(), kill_rx, error_rx).await;

    // 正常首连 + 订阅。
    let stream = NewsStream::connect_to(&format!("ws://{addr}"), "key", "secret")
        .await
        .unwrap();
    let mut status = stream.status();
    stream.subscribe(&["AAPL"]).await.unwrap();
    wait_until(&seen, |seen| seen.len() >= 1, "first subscribe").await;

    // 服务端断开,且重启后进入"无权限"状态 → 重连握手会收到 error 帧。
    kill_tx.send(true).unwrap();
    error_tx.send(true).unwrap();
    wait_status(&mut status, |s| {
        matches!(s, StreamStatus::Reconnecting { attempt: 1, .. })
    }, "Reconnecting once")
    .await;

    // 永久性错误 → Failed 事件,supervisor 退出,不再无限重试。
    wait_status(&mut status, |s| {
        matches!(
            s,
            StreamStatus::Failed { message } if message.contains("insufficient subscription")
        )
    }, "Failed with the handshake error")
    .await;

    // 确认 supervisor 已停止:一段时间后既无新连接,也无新状态事件。
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(seen.lock().unwrap().len(), 1, "no reconnect after Failed");

    // close() 幂等:对已退出的 supervisor 无副作用。
    stream.close();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(seen.lock().unwrap().len(), 1, "still no reconnect after close");
}
