//! 连接状态事件:supervisor 在生命周期关键节点广播。

use std::time::Duration;

/// 永久性失败的细分原因。消费方(relay)按 reason 决定后续动作:
/// `AuthFailed` 不重试;`Network` 交给 SDK 的指数退避重连;`Protocol`
/// 可能需要降级处理。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusReason {
    /// 认证/鉴权类永久失败(握手 error 帧:认证失败、无该 feed 权限、
    /// 连接数超限)。重试无意义 —— SDK 内部已停止重连,直接进 `Failed` 终态。
    AuthFailed,
    /// 网络断开类错误。SDK 内部按指数退避无限重连(以 [`StreamStatus::Reconnecting`]
    /// 事件表达),不会以 `Failed{Network}` 终结;保留该变体供上层自行归类。
    Network,
    /// 协议错误(如服务端返回无法理解的帧)。当前实现按未知消息跳过、不断流;
    /// 保留该变体供上层自行归类。
    Protocol,
}

/// 连接状态:经 [`super::StreamStatus`] 的 broadcast 通道分发(容量 16)。
/// 消费方落后过多会跳过中间事件(`RecvError::Lagged`),以最新状态为准。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamStatus {
    /// 连接建立且握手、订阅重放完成,可以正常收发。
    Connected,
    /// 连接死亡,将在 `delay` 后发起第 `attempt` 次重连(指数退避,重连成功清零)。
    Reconnecting { attempt: u32, delay: Duration },
    /// 永久性错误(见 [`StatusReason::AuthFailed`]):重连无意义,supervisor
    /// 已停止重连并退出。`message` 是服务端 error 帧的原始文本。与
    /// [`StreamStatus::Closed`](Self::Closed)(正常关闭)区分。
    Failed {
        reason: StatusReason,
        message: String,
    },
    /// 流已显式关闭(`close()` 或最后一个句柄 drop),supervisor 退出。
    Closed,
}
