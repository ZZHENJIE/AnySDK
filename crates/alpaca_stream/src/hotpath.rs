//! 流消息热路径解析。
//!
//! 股票流帧是「单层 JSON 对象的数组」,峰值可达每秒数万条 —— 走 tape 快路径:
//! [`simd_json::to_tape`] 一次拿到扁平 token 序列,跳过 DOM 树构建,按 `T` 分发:
//! - `t`(Trade)/ `q`(Quote):顺序扫一遍字段直接构造结构体 —— 零 DOM 分配,
//!   这是行消息体量最大的两个频道;
//! - 其余类型(bar/LULD/状态/订阅回执等,低频):把该消息的 tape 片段还原成
//!   `OwnedValue` 交 serde 兜底,与旧实现语义完全一致。
//!
//! 两条路径共享错误约定:帧整体无效 → 整帧丢弃并告警;单条消息无效 → 只跳过
//! 该条并告警,绝不中断流。
//!
//! 对拍测试:`tests/tape_parity.rs` 保证 tape 路径与 serde 路径逐字段一致。

use serde::de::DeserializeOwned;
use simd_json::value::owned::Object as JsonObject;
use simd_json::{Node, OwnedValue, StaticNode};
use tracing::warn;

use crate::stocks::{QuoteMessage, StockMessage, TradeMessage};

// ---------- serde 兜底路径 ----------

/// serde 路径:整帧进 DOM,逐条转换;单条失败只跳过自己。
/// (语义与旧 serde_json 实现一致;新闻流与对拍基准都走这里。)
pub fn parse_frame_serde<M: DeserializeOwned>(buf: &mut [u8]) -> Vec<M> {
    let OwnedValue::Array(array) = to_owned(buf) else {
        warn!("unrecognized stream frame: {}", String::from_utf8_lossy(buf));
        return Vec::new();
    };
    array
        .into_iter()
        .filter_map(|value| match simd_json::serde::from_owned_value(value) {
            Ok(msg) => Some(msg),
            Err(err) => {
                warn!("skipping stream message: {err}");
                None
            }
        })
        .collect()
}

/// `simd_json::to_owned_value` 的包装:失败时告警并返回 Null(调用方按形状分发)。
fn to_owned(buf: &mut [u8]) -> OwnedValue {
    match simd_json::to_owned_value(buf) {
        Ok(value) => value,
        Err(err) => {
            warn!("unrecognized stream frame: {err}");
            OwnedValue::Static(StaticNode::Null)
        }
    }
}

// ---------- tape 快路径 ----------

/// 股票流帧解析:tape 快路径(`q`/`t`)+ serde 兜底(其余类型)。
pub fn parse_stock_frame(buf: &mut [u8]) -> Vec<StockMessage> {
    let tape = match simd_json::to_tape(buf) {
        Ok(tape) => tape,
        Err(err) => {
            warn!("unrecognized stream frame: {err}");
            return Vec::new();
        }
    };
    let nodes = tape.0.as_slice();
    // 顶层必须是消息数组;tape 无显式 ArrayEnd,元素边界由头部 count 描述。
    if !matches!(nodes.first(), Some(Node::Array { .. })) {
        warn!("unrecognized stream frame: {}", String::from_utf8_lossy(buf));
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut idx = 1;
    while idx < nodes.len() {
        match nodes[idx] {
            Node::Object { count, .. } => {
                let Some(span) = nodes.get(idx..idx + count + 1) else { break };
                match message_from_span(span) {
                    Ok(msg) => out.push(msg),
                    Err(err) => warn!("skipping stream message: {err}"),
                }
                idx += count + 1;
            }
            // 非对象的数组元素:两条路径都会拒绝,保持「逐条告警后跳过」。
            other => {
                warn!("skipping stream message: unexpected {}", kind_of(&other));
                idx += node_count(&other);
            }
        }
    }
    out
}

/// 单条消息:tape 片段 → [`StockMessage`]。
///
/// 先从 span 中找 `T` 定类型:`q`/`t` 走手工构建;其余(含缺 `T`/`T` 非字符串)
/// 交给 serde 兜底 —— 与旧路径一样以「解析失败 → 告警跳过」收场。
fn message_from_span(span: &[Node]) -> Result<StockMessage, String> {
    match find_tag(span) {
        Some("t") => trade_from_span(span).map(StockMessage::Trade),
        Some("q") => quote_from_span(span).map(StockMessage::Quote),
        _ => simd_json::serde::from_owned_value(span_to_owned(span)?)
            .map_err(|err| err.to_string()),
    }
}

/// 顺序扫描 span 里的键值对,取 `T` 的字符串值;没有则 `None`。
fn find_tag<'a>(span: &'a [Node<'a>]) -> Option<&'a str> {
    for (key, value_idx) in pairs(span) {
        if key == "T" {
            return str_of(&span[value_idx]).ok();
        }
    }
    None
}

/// 顺序扫描 span 里的键值对,产出 `(键, 值节点下标)`:`span[0]` 是对象头,
/// 其后键、值交替(值可占多个节点)。
fn pairs<'a>(span: &'a [Node<'a>]) -> impl Iterator<Item = (&'a str, usize)> + 'a {
    let mut idx = 1;
    std::iter::from_fn(move || {
        let Node::String(key) = span.get(idx)? else {
            return None;
        };
        let value_idx = idx + 1;
        let value = span.get(value_idx)?;
        idx = value_idx + node_count(value);
        Some((*key, value_idx))
    })
}

/// 一个节点在 tape 中占据的节点数(容器头 + 子元素)。
fn node_count(node: &Node) -> usize {
    match node {
        Node::Object { count, .. } | Node::Array { count, .. } => count + 1,
        _ => 1,
    }
}

fn kind_of(node: &Node) -> &'static str {
    match node {
        Node::String(_) => "string",
        Node::Object { .. } => "object",
        Node::Array { .. } => "array",
        Node::Static(_) => "scalar",
    }
}

// ---------- tape → 字段提取(与 serde_json 的类型判定保持一致) ----------

fn str_of<'a>(node: &'a Node<'a>) -> Result<&'a str, String> {
    match node {
        Node::String(s) => Ok(s),
        _ => Err(format!("expected string, got {}", kind_of(node))),
    }
}

/// f64 字段:接受 F64/I64/U64(serde 数值访客的默认行为),其余拒绝。
fn f64_of(node: &Node) -> Result<f64, String> {
    match node {
        Node::Static(StaticNode::F64(v)) => Ok(*v),
        Node::Static(StaticNode::I64(v)) => Ok(*v as f64),
        Node::Static(StaticNode::U64(v)) => Ok(*v as f64),
        _ => Err(format!("expected number, got {}", kind_of(node))),
    }
}

/// i64 字段:接受 I64/可容纳的 U64(超出范围与 serde 一样报错),其余拒绝。
fn i64_of(node: &Node) -> Result<i64, String> {
    match node {
        Node::Static(StaticNode::I64(v)) => Ok(*v),
        Node::Static(StaticNode::U64(v)) => {
            i64::try_from(*v).map_err(|_| "number out of i64 range".to_string())
        }
        _ => Err(format!("expected integer, got {}", kind_of(node))),
    }
}

/// `Option<String>` 字段:缺失/`null` → `None`,字符串 → `Some`,其余拒绝。
fn opt_str_of(node: &Node) -> Result<Option<String>, String> {
    match node {
        Node::Static(StaticNode::Null) => Ok(None),
        other => str_of(other).map(str::to_owned).map(Some),
    }
}

/// `Vec<String>` 字段(字符串数组;`null` 与 serde 一样视为类型错误)。
fn str_vec_of(span: &[Node]) -> Result<Vec<String>, String> {
    let Node::Array { len, .. } = span[0] else {
        return Err(format!("expected array, got {}", kind_of(&span[0])));
    };
    let mut out = Vec::with_capacity(len);
    let mut idx = 1;
    for _ in 0..len {
        let end = idx + node_count(&span[idx]);
        out.push(str_of(&span[idx])?.to_owned());
        idx = end;
    }
    Ok(out)
}

/// RFC-3339 时间字符串 → `DateTime<Utc>`(与 chrono serde 反序列化同一条路)。
fn time_of(node: &Node) -> Result<chrono::DateTime<chrono::Utc>, String> {
    let raw = str_of(node)?;
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|t| t.with_timezone(&chrono::Utc))
        .map_err(|err| format!("bad timestamp {raw:?}: {err}"))
}

// ---------- tape → 消息结构体(单层对象,按字段名顺序扫描) ----------

/// Trade 消息:`{"T":"t","S":..,"i":..,"x":..,"p":..,"s":..,"c":..,"t":..,"z":..}`。
fn trade_from_span(span: &[Node]) -> Result<TradeMessage, String> {
    let mut symbol = None;
    let mut id = None;
    let mut exchange = None;
    let mut price = None;
    let mut size = None;
    let mut conditions = Vec::new(); // `c` 带 serde default:缺失即空
    let mut time = None;
    let mut tape = None; // `z` 是 Option:缺失即 None
    for (key, value_idx) in pairs(span) {
        match key {
            "S" => symbol = Some(str_of(&span[value_idx])?.to_owned()),
            "i" => id = Some(i64_of(&span[value_idx])?),
            "x" => exchange = Some(str_of(&span[value_idx])?.to_owned()),
            "p" => price = Some(f64_of(&span[value_idx])?),
            "s" => size = Some(i64_of(&span[value_idx])?),
            "c" => conditions = str_vec_of(&span[value_idx..])?,
            "t" => time = Some(time_of(&span[value_idx])?),
            "z" => tape = opt_str_of(&span[value_idx])?,
            _ => {}
        }
    }
    Ok(TradeMessage {
        symbol: require(symbol, "S")?,
        id: require(id, "i")?,
        exchange: require(exchange, "x")?,
        price: require(price, "p")?,
        size: require(size, "s")?,
        conditions,
        time: require(time, "t")?,
        tape,
    })
}

/// Quote 消息:`{"T":"q","S":..,"ax":..,"ap":..,"as":..,"bx":..,"bp":..,"bs":..,"c":..,"t":..,"z":..}`。
fn quote_from_span(span: &[Node]) -> Result<QuoteMessage, String> {
    let mut symbol = None;
    let mut ask_exchange = None;
    let mut ask_price = None;
    let mut ask_size = None;
    let mut bid_exchange = None;
    let mut bid_price = None;
    let mut bid_size = None;
    let mut conditions = Vec::new();
    let mut time = None;
    let mut tape = None;
    for (key, value_idx) in pairs(span) {
        match key {
            "S" => symbol = Some(str_of(&span[value_idx])?.to_owned()),
            "ax" => ask_exchange = Some(str_of(&span[value_idx])?.to_owned()),
            "ap" => ask_price = Some(f64_of(&span[value_idx])?),
            "as" => ask_size = Some(i64_of(&span[value_idx])?),
            "bx" => bid_exchange = Some(str_of(&span[value_idx])?.to_owned()),
            "bp" => bid_price = Some(f64_of(&span[value_idx])?),
            "bs" => bid_size = Some(i64_of(&span[value_idx])?),
            "c" => conditions = str_vec_of(&span[value_idx..])?,
            "t" => time = Some(time_of(&span[value_idx])?),
            "z" => tape = opt_str_of(&span[value_idx])?,
            _ => {}
        }
    }
    Ok(QuoteMessage {
        symbol: require(symbol, "S")?,
        ask_exchange: require(ask_exchange, "ax")?,
        ask_price: require(ask_price, "ap")?,
        ask_size: require(ask_size, "as")?,
        bid_exchange: require(bid_exchange, "bx")?,
        bid_price: require(bid_price, "bp")?,
        bid_size: require(bid_size, "bs")?,
        conditions,
        time: require(time, "t")?,
        tape,
    })
}

/// 必填字段兜底:缺失时报出字段名(与 serde 的 missing field 错误等价)。
fn require<T>(value: Option<T>, field: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("missing field {field}"))
}

// ---------- tape 片段 → OwnedValue(serde 兜底的桥) ----------

/// 把一条消息的 tape 片段还原成 owned DOM,交给 serde 反序列化。
/// (低频类型才走这里;所有权字符串拷贝的开销可以忽略。)
fn span_to_owned(span: &[Node]) -> Result<OwnedValue, String> {
    match span[0] {
        Node::String(s) => Ok(OwnedValue::from(s)),
        Node::Static(n) => Ok(OwnedValue::Static(n)),
        Node::Object { len, .. } => {
            let mut obj = JsonObject::new();
            let mut idx = 1;
            for _ in 0..len {
                let key = str_of(&span[idx])?;
                let end = idx + 1 + node_count(&span[idx + 1]);
                let value = span_to_owned(span.get(idx + 1..end).ok_or("truncated object")?)?;
                obj.insert(key.to_owned(), value);
                idx = end;
            }
            Ok(OwnedValue::from(obj))
        }
        Node::Array { len, .. } => {
            let mut array = Vec::with_capacity(len);
            let mut idx = 1;
            for _ in 0..len {
                let end = idx + node_count(&span[idx]);
                let value = span_to_owned(span.get(idx..end).ok_or("truncated array")?)?;
                array.push(value);
                idx = end;
            }
            Ok(OwnedValue::from(array))
        }
    }
}
