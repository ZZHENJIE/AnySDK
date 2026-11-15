//! 连接状态事件:supervisor 在生命周期关键节点广播。

use std::time::Duration;

/// 连接状态:经 [`super::StreamStatus`] 的 broadcast 通道分发(容量 16)。
/// 消费方落后过多会跳过中间事件(`RecvError::Lagged`),以最新状态为准。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamStatus {
    /// 连接建立且握手、订阅重放完成,可以正常收发。
    Connected,
    /// 连接死亡,将在 `delay` 后发起第 `attempt` 次重连(指数退避,重连成功清零)。
    Reconnecting { attempt: u32, delay: Duration },
    /// 永久性错误(认证失败 / 无该 feed 权限 / 连接数超限等握手 error 帧):
    /// 重连无意义,supervisor 已停止重连并退出。与 [`StreamStatus::Closed`]
    /// (正常关闭)区分。
    Failed { message: String },
    /// 流已显式关闭([`close`](…) 或最后一个句柄 drop),supervisor 退出。
    Closed,
}
