//! tape 快路径与 serde 兜底路径的对拍测试。
//!
//! 同一批真实 Alpaca 消息样本(quote/trade/bar/LULD/交易状态/失衡/纠正/
//! 取消、订阅回执、success/error/ping、未知类型、坏数据)分别走两条路径,
//! 断言消息序列**逐字段完全一致**(含「该跳过的都跳过」的行为一致)。
//! 这是 tape 重写的安全网:对拍不过不许删旧路径。

use alpaca_stream::hotpath::{parse_frame_serde, parse_stock_frame};
use alpaca_stream::stocks::{QuoteMessage, TradeMessage};
use alpaca_stream::StockMessage;

/// 对拍:同一帧分别走 tape 路径与 serde 路径,结果必须一致。
fn assert_parity(frame: &str) {
    let mut tape_buf = frame.as_bytes().to_vec();
    let mut serde_buf = frame.as_bytes().to_vec();
    let via_tape = parse_stock_frame(&mut tape_buf);
    let via_serde = parse_frame_serde::<StockMessage>(&mut serde_buf);
    assert_eq!(
        via_tape, via_serde,
        "tape/serde paths disagree on frame: {frame}\nvia tape:  {via_tape:?}\nvia serde: {via_serde:?}"
    );
}

#[test]
fn quote_parity_full_fields() {
    assert_parity(
        r#"[{"T":"q","S":"NVDA","ax":"Q","ap":189.52,"as":7,"bx":"P","bp":189.51,"bs":8,"c":["R"],"t":"2024-06-18T15:30:00.123456789Z","z":"C"}]"#,
    );
}

#[test]
fn trade_parity_full_fields() {
    assert_parity(
        r#"[{"T":"t","S":"AAPL","i":6913,"x":"D","p":182.13,"s":10,"c":["@","F"],"t":"2022-01-03T14:30:00.123Z","z":"C"}]"#,
    );
}

/// `T` 不在首个键位,`c`/`z` 缺失(c 走 default,z 走 Option),整数形式价格。
#[test]
fn quote_parity_reordered_and_sparse() {
    assert_parity(
        r#"[{"t":"2024-06-18T15:30:00Z","bs":3,"bp":182.12,"bx":"P","as":4,"ap":182.13,"ax":"Q","S":"AAPL","T":"q","c":[],"z":null}]"#,
    );
    assert_parity(
        r#"[{"T":"q","S":"SPY","ax":"Q","ap":500,"as":1,"bx":"P","bp":499.95,"bs":2,"t":"2024-06-18T20:59:59Z"}]"#,
    );
}

/// tape 路径独立断言:字段值精确(不依赖两条路径「一起错」)。
#[test]
fn quote_fields_exact_via_tape() {
    let mut buf = br#"[{"T":"q","S":"NVDA","ax":"Q","ap":189.52,"as":7,"bx":"P","bp":189.51,"bs":8,"c":["R","T"],"t":"2024-06-18T15:30:00.123456789Z","z":"C"}]"#.to_vec();
    let msgs = parse_stock_frame(&mut buf);
    let [StockMessage::Quote(quote)] = &msgs[..] else {
        panic!("expected single quote, got {msgs:?}");
    };
    assert_eq!(quote.symbol, "NVDA");
    assert_eq!(quote.ask_exchange, "Q");
    assert_eq!(quote.ask_price, 189.52);
    assert_eq!(quote.ask_size, 7);
    assert_eq!(quote.bid_exchange, "P");
    assert_eq!(quote.bid_price, 189.51);
    assert_eq!(quote.bid_size, 8);
    assert_eq!(quote.conditions, ["R", "T"]);
    assert_eq!(
        quote.time.to_string(),
        "2024-06-18 15:30:00.123456789 UTC"
    );
    assert_eq!(quote.tape.as_deref(), Some("C"));
}

#[test]
fn trade_fields_exact_via_tape() {
    let mut buf = br#"[{"T":"t","S":"AAPL","i":6913,"x":"D","p":182.13,"s":10,"c":["@","F"],"t":"2022-01-03T14:30:00.123Z","z":"C"}]"#.to_vec();
    let msgs = parse_stock_frame(&mut buf);
    let [StockMessage::Trade(trade)] = &msgs[..] else {
        panic!("expected single trade, got {msgs:?}");
    };
    let expected = TradeMessage {
        symbol: "AAPL".into(),
        id: 6913,
        exchange: "D".into(),
        price: 182.13,
        size: 10,
        conditions: vec!["@".into(), "F".into()],
        time: chrono::DateTime::parse_from_rfc3339("2022-01-03T14:30:00.123Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        tape: Some("C".into()),
    };
    assert_eq!(*trade, expected);
}

/// 低频类型(serde 兜底):bar / LULD / 交易状态 / 失衡 / 纠正 / 取消。
#[test]
fn fallback_channel_parity() {
    assert_parity(
        r#"[{"T":"b","S":"AAPL","o":178.26,"h":178.34,"l":177.76,"c":178.08,"v":60937,"n":1727,"vw":177.954244,"t":"2022-01-03T14:30:00Z"}]"#,
    );
    assert_parity(
        r#"[{"T":"d","S":"AAPL","o":177.77,"h":182.88,"l":177.26,"c":182.19,"v":160418504,"n":449385,"vw":181.517,"t":"2022-01-03T05:00:00Z"}]"#,
    );
    assert_parity(
        r#"[{"T":"u","S":"AAPL","o":182.1,"h":182.1,"l":182.05,"c":182.05,"v":1372,"n":18,"vw":182.0777,"t":"2022-01-04T15:19:00Z"}]"#,
    );
    assert_parity(
        r#"[{"T":"l","S":"AAPL","u":182.5,"d":177.5,"i":"S","t":"2022-01-03T14:30:00Z","z":"C"}]"#,
    );
    assert_parity(
        r#"[{"T":"s","S":"AAPL","sc":"T","sm":"LULD halt \"straddle\"","rc":"T1","rm":"News","t":"2022-01-03T14:30:00Z","z":"C"}]"#,
    );
    assert_parity(
        r#"[{"T":"i","S":"AAPL","p":182.0,"t":"2022-01-03T14:30:00Z","z":"C"}]"#,
    );
    assert_parity(
        r#"[{"T":"c","S":"AAPL","x":"D","oi":6913,"op":182.13,"os":10,"oc":["@","F"],"ci":6914,"cp":182.14,"cs":11,"cc":["@"],"t":"2022-01-03T14:30:01Z","z":"C"}]"#,
    );
    assert_parity(
        r#"[{"T":"x","S":"AAPL","i":6913,"x":"D","p":182.13,"s":10,"a":"C","t":"2022-01-03T14:30:01Z","z":"C"}]"#,
    );
}

/// 订阅回执(camelCase,含空数组与星号)、success / error / ping(带额外字段)。
#[test]
fn control_frame_parity() {
    assert_parity(
        r#"[{"T":"subscription","trades":["AAPL"],"quotes":["MSFT","NVDA"],"bars":[],"dailyBars":["TSLA"],"corrections":["AAPL"],"cancelErrors":[]}]"#,
    );
    assert_parity(r#"[{"T":"success","msg":"authenticated"}]"#);
    assert_parity(r#"[{"T":"error","code":"401","msg":"unauthorized"}]"#);
    assert_parity(r#"[{"T":"ping"}]"#);
    assert_parity(r#"[{"T":"ping","msg":"keepalive"}]"#);
}

/// 一帧多条混合消息(顺序保持)。
#[test]
fn mixed_frame_parity() {
    assert_parity(
        r#"[
            {"T":"q","S":"NVDA","ax":"Q","ap":189.52,"as":7,"bx":"P","bp":189.51,"bs":8,"c":[],"t":"2024-06-18T15:30:00Z","z":"C"},
            {"T":"t","S":"AAPL","i":6913,"x":"D","p":182.13,"s":10,"c":["@"],"t":"2022-01-03T14:30:00.123Z","z":"C"},
            {"T":"ping"},
            {"T":"b","S":"MSFT","o":315.1,"h":315.4,"l":315.0,"c":315.2,"v":100,"n":3,"vw":315.2,"t":"2022-01-03T14:30:00Z"}
        ]"#,
    );
}

/// 未知类型、未知字段、数组里的标量元素:两条路径都必须同样跳过。
#[test]
fn unknown_and_extra_parity() {
    assert_parity(r#"[{"T":"zzz","S":"AAPL"}]"#);
    assert_parity(
        r#"[{"T":"t","S":"AAPL","i":1,"x":"D","p":10.0,"s":1,"t":"2022-01-03T14:30:00Z","future_field":{"nested":[1,2]}}]"#,
    );
    assert_parity(r#"[1,{"T":"t","S":"AAPL","i":1,"x":"D","p":10.0,"s":1,"t":"2022-01-03T14:30:00Z"},"x"]"#);
}

/// 坏数据:字段类型错、必填缺失、数值越界 —— 两条路径都应拒绝该条(整帧其余照常)。
#[test]
fn bad_message_parity() {
    // 价格是字符串、id 是浮点:类型错误。
    assert_parity(r#"[{"T":"t","S":"AAPL","i":6913,"x":"D","p":"182.13","s":10,"t":"2022-01-03T14:30:00Z"}]"#);
    assert_parity(r#"[{"T":"t","S":"AAPL","i":1.5,"x":"D","p":1.0,"s":10,"t":"2022-01-03T14:30:00Z"}]"#);
    // 缺必填字段 S。
    assert_parity(r#"[{"T":"t","i":6913,"x":"D","p":1.0,"s":10,"t":"2022-01-03T14:30:00Z"}]"#);
    // 时间串非法。
    assert_parity(r#"[{"T":"t","S":"AAPL","i":1,"x":"D","p":1.0,"s":10,"t":"not-a-time"}]"#);
    // size 超出 i64(serde 与 tape 都应拒绝)。
    assert_parity(r#"[{"T":"t","S":"AAPL","i":1,"x":"D","p":1.0,"s":18446744073709551615,"t":"2022-01-03T14:30:00Z"}]"#);
    // c 为 null(serde 视为类型错误,tape 同样拒绝)。
    assert_parity(r#"[{"T":"q","S":"AAPL","ax":"Q","ap":1.0,"as":1,"bx":"P","bp":1.0,"bs":1,"c":null,"t":"2022-01-03T14:30:00Z"}]"#);
    // 好消息与坏消息同帧:坏的那条被跳过,好的保留。
    assert_parity(
        r#"[
            {"T":"t","S":"AAPL","i":1,"x":"D","p":"bad","s":10,"t":"2022-01-03T14:30:00Z"},
            {"T":"q","S":"NVDA","ax":"Q","ap":189.52,"as":7,"bx":"P","bp":189.51,"bs":8,"c":[],"t":"2024-06-18T15:30:00Z","z":"C"}
        ]"#,
    );
}

/// 整帧级失败:非数组、坏 JSON —— 两条路径都返回空。
#[test]
fn bad_frame_parity() {
    assert_parity(r#"{"T":"ping"}"#);
    assert_parity(r#"not json at all"#);
    assert_parity(r#"[{"T":"q","S":"AAPL""#);
    assert_parity(r#""#);
}

/// 指数记数/大整数价格的等价性(serde 数值访客与 tape 提取一致)。
#[test]
fn number_form_parity() {
    assert_parity(
        r#"[{"T":"q","S":"BRK.A","ax":"Q","ap":6.024e2,"as":1,"bx":"P","bp":602.3,"bs":1,"c":[],"t":"2024-06-18T15:30:00Z"}]"#,
    );
    assert_parity(
        r#"[{"T":"t","S":"AAPL","i":9223372036854775807,"x":"D","p":182.13,"s":2147483647,"t":"2022-01-03T14:30:00Z"}]"#,
    );
}

/// reader 复用同一缓冲:同一 buffer 清空重灌后再解析,结果必须稳定
/// (simd-json 会原地改写 buffer,复用语义不能被破坏)。
#[test]
fn reused_buffer_is_stable() {
    let frame = r#"[{"T":"q","S":"NVDA","ax":"Q","ap":189.52,"as":7,"bx":"P","bp":189.51,"bs":8,"c":["R"],"t":"2024-06-18T15:30:00Z","z":"C"}]"#;
    let mut buf = Vec::new();
    buf.extend_from_slice(frame.as_bytes());
    let first = parse_stock_frame(&mut buf);
    buf.clear();
    buf.extend_from_slice(frame.as_bytes());
    let second = parse_stock_frame(&mut buf);
    assert_eq!(first, second);
    let mut serde_buf = Vec::new();
    serde_buf.extend_from_slice(frame.as_bytes());
    assert_eq!(parse_frame_serde::<StockMessage>(&mut serde_buf), first);
}

/// 一条手工构造的 QuoteMessage 与 tape 结果比对(字段级防呆)。
#[test]
fn quote_message_struct_level_check() {
    let mut buf = br#"[{"T":"q","S":"AAPL","ax":"Q","ap":182.13,"as":4,"bx":"P","bp":182.12,"bs":3,"c":["R"],"t":"2022-01-03T14:30:00.123Z","z":"C"}]"#.to_vec();
    let [StockMessage::Quote(quote)] = &parse_stock_frame(&mut buf)[..] else {
        panic!("expected single quote");
    };
    let expected = QuoteMessage {
        symbol: "AAPL".into(),
        ask_exchange: "Q".into(),
        ask_price: 182.13,
        ask_size: 4,
        bid_exchange: "P".into(),
        bid_price: 182.12,
        bid_size: 3,
        conditions: vec!["R".into()],
        time: chrono::DateTime::parse_from_rfc3339("2022-01-03T14:30:00.123Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        tape: Some("C".into()),
    };
    assert_eq!(*quote, expected);
}
