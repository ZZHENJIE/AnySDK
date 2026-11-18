//! AnySDK 核心架构:与任何 HTTP 客户端解耦。
//!
//! - [`Method`] 描述一个请求(URL、headers、body),不承载任何执行逻辑;
//! - 使用方为自己的 HTTP 客户端类型 `T` 实现 [`HttpExecutor<T>`];
//! - SDK 侧的 [`Client`] 组合「任意 HTTP 客户端 `T` + 平台状态 `S` + 执行器 `E`」。
//!
//! 本 crate 只做请求描述、状态码判定与解析(JSON/URL 拼接),
//! 不依赖任何具体 HTTP 客户端,也不做任何业务数据转换。

use std::future::Future;

// ---------- 请求模型 ----------

#[derive(Clone, Debug)]
pub enum Method {
    Get(GetRequest),
    Post(PostRequest),
}

#[derive(Clone, Debug)]
pub struct GetRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct PostRequest {
    pub url: String,
    pub body: String,
    pub headers: Vec<(String, String)>,
}

impl Method {
    pub fn get(url: impl Into<String>) -> Self {
        Method::Get(GetRequest {
            url: url.into(),
            headers: vec![],
        })
    }
    pub fn post(url: impl Into<String>, body: impl Into<String>) -> Self {
        Method::Post(PostRequest {
            url: url.into(),
            body: body.into(),
            headers: vec![],
        })
    }

    /// 给请求加一个 header(SDK 注入认证等 state 用)。
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let pair = (key.into(), value.into());
        match &mut self {
            Method::Get(g) => g.headers.push(pair),
            Method::Post(p) => p.headers.push(pair),
        }
        self
    }
}

// ---------- 响应模型 ----------

/// 一次 HTTP 交换的传输事实:状态码 + 完整响应体。状态码如何产生由执行器负责。
#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

// ---------- 执行器 trait(不接收 state) ----------

/// 用户为自己的 HTTP 客户端实现这个 trait。
/// 只负责「用 client 发 request,带回状态码与完整响应体」,
/// 不知道任何平台 state,也不做状态码判定。
pub trait HttpExecutor<T>: Send + Sync + 'static
where
    T: Send + Sync + 'static,
{
    fn execute(
        &self,
        client: &T,
        request: &Method,
    ) -> impl Future<Output = anyhow::Result<HttpResponse>> + Send;
}

// ---------- SDK Client ----------

pub struct Client<T, S, E>
where
    T: Send + Sync + 'static,
    S: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    pub http_client: T,
    pub state: S,
    executor: E,
}

impl<T, S, E> Client<T, S, E>
where
    T: Send + Sync + 'static,
    S: Send + Sync + 'static,
    E: HttpExecutor<T>,
{
    pub fn new(http_client: T, state: S, executor: E) -> Self {
        Self {
            http_client,
            state,
            executor,
        }
    }

    pub async fn send(&self, request: Method) -> anyhow::Result<HttpResponse> {
        self.executor.execute(&self.http_client, &request).await
    }

    pub async fn get(&self, url: impl Into<String>) -> anyhow::Result<HttpResponse> {
        self.send(Method::get(url)).await
    }

    pub async fn post(
        &self,
        url: impl Into<String>,
        body: impl Into<String>,
    ) -> anyhow::Result<HttpResponse> {
        self.send(Method::post(url, body)).await
    }
}

// ---------- 共享辅助 ----------

/// 非 2xx → 错误携带 API 的原始响应体;2xx → 返回响应体字节。
/// 各 SDK 在拿到 [`HttpResponse`] 后统一调用。
pub fn ensure_success(response: HttpResponse) -> anyhow::Result<Vec<u8>> {
    if response.is_success() {
        Ok(response.body)
    } else {
        anyhow::bail!(
            "HTTP {}: {}",
            response.status,
            String::from_utf8_lossy(&response.body).trim()
        )
    }
}

/// `base + path + query` 拼 URL,query 的键与值均做 percent 编码。
pub fn build_url(base: &str, path: &str, query: &[(&str, String)]) -> String {
    let mut url = format!("{base}{path}");
    if !query.is_empty() {
        url.push('?');
        for (index, (key, value)) in query.iter().enumerate() {
            if index > 0 {
                url.push('&');
            }
            url.push_str(&format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            ));
        }
    }
    url
}

/// 把响应体字节按 JSON 解析为 `T`(各 SDK 的 `send_json` 落点)。
///
/// simd-json 会原地改写 buffer(SIMD 对齐 + 字符串 unescape),因此内部先拷贝;
/// 持有 `Vec<u8>` 的调用方请用零拷贝的 [`parse_json_owned`]。
pub fn parse_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    parse_json_owned(bytes.to_vec())
}

/// [`parse_json`] 的零拷贝版本:直接消费响应体字节。
pub fn parse_json_owned<T: serde::de::DeserializeOwned>(mut bytes: Vec<u8>) -> anyhow::Result<T> {
    Ok(simd_json::from_slice(&mut bytes)?)
}

/// Append `key: value` to a query builder when the option is set.
pub fn push_opt(
    query: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: &Option<String>,
) {
    if let Some(value) = value {
        query.push((key, value.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_url_encodes_query() {
        let url = build_url(
            "https://data.alpaca.markets",
            "/v2/stocks/bars",
            &[
                ("symbols", "AAPL,MSFT".to_string()),
                ("timeframe", "1Day".to_string()),
            ],
        );
        assert_eq!(
            url,
            "https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL%2CMSFT&timeframe=1Day"
        );
    }

    #[test]
    fn build_url_without_query_has_no_question_mark() {
        let url = build_url("https://example.com", "/path", &[]);
        assert_eq!(url, "https://example.com/path");
    }

    #[test]
    fn ensure_success_passes_body_and_reports_raw_error() {
        let ok = HttpResponse {
            status: 200,
            body: b"{}".to_vec(),
        };
        assert_eq!(ensure_success(ok).unwrap(), b"{}".to_vec());

        let err = HttpResponse {
            status: 403,
            body: b"\n{\"message\":\"auth failed\"}\n".to_vec(),
        };
        let message = ensure_success(err).unwrap_err().to_string();
        assert!(message.contains("403"), "{message}");
        assert!(message.contains("auth failed"), "{message}");
    }

    #[test]
    fn with_header_applies_to_both_variants() {
        let get = Method::get("u").with_header("a", "1");
        let Method::Get(g) = get else { unreachable!() };
        assert_eq!(g.headers, vec![("a".to_string(), "1".to_string())]);

        let post = Method::post("u", "b").with_header("a", "1");
        let Method::Post(p) = post else {
            unreachable!()
        };
        assert_eq!(p.headers, vec![("a".to_string(), "1".to_string())]);
    }
}
