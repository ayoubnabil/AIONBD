use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8080";
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

pub type Result<T> = std::result::Result<T, AionBDError>;

#[derive(Debug)]
pub enum AionBDError {
    InvalidOption(String),
    InvalidArgument(String),
    Transport {
        method: String,
        path: String,
        source: std::io::Error,
    },
    Http {
        status: u16,
        method: String,
        path: String,
        body: String,
    },
    InvalidJson {
        method: String,
        path: String,
        body: String,
        source: serde_json::Error,
    },
}

impl Display for AionBDError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidOption(message) | Self::InvalidArgument(message) => write!(f, "{message}"),
            Self::Transport {
                method,
                path,
                source,
            } => write!(f, "request failed for {method} {path}: {source}"),
            Self::Http {
                status,
                method,
                path,
                body,
            } => write!(f, "HTTP {status} on {method} {path}: {body}"),
            Self::InvalidJson {
                method,
                path,
                source,
                ..
            } => write!(f, "invalid JSON response on {method} {path}: {source}"),
        }
    }
}

impl std::error::Error for AionBDError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport { source, .. } => Some(source),
            Self::InvalidJson { source, .. } => Some(source),
            Self::InvalidOption(_) | Self::InvalidArgument(_) | Self::Http { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub timeout: Duration,
    pub api_key: Option<String>,
    pub bearer_token: Option<String>,
    pub headers: HashMap<String, String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            api_key: None,
            bearer_token: None,
            headers: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Metric {
    Dot,
    L2,
    Cosine,
}

impl Default for Metric {
    fn default() -> Self {
        Self::Dot
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    Exact,
    Ivf,
    Auto,
}

impl Default for SearchMode {
    fn default() -> Self {
        Self::Auto
    }
}

pub type MetricsResponse = Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiveResponse {
    pub status: String,
    pub uptime_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadyChecks {
    pub engine_loaded: bool,
    pub storage_available: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadyResponse {
    pub status: String,
    pub uptime_ms: u64,
    pub checks: ReadyChecks,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistanceResponse {
    pub metric: Metric,
    pub value: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionResponse {
    pub name: String,
    pub dimension: u32,
    pub strict_finite: bool,
    pub point_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ListCollectionsResponse {
    pub collections: Vec<CollectionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchResponse {
    pub id: u64,
    pub metric: Metric,
    pub value: f32,
    pub mode: SearchMode,
    pub recall_at_k: Option<f32>,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchHit {
    pub id: u64,
    pub value: f32,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchTopKResponse {
    pub metric: Metric,
    pub mode: SearchMode,
    pub recall_at_k: Option<f32>,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchTopKBatchItem {
    pub mode: SearchMode,
    pub recall_at_k: Option<f32>,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchTopKBatchResponse {
    pub metric: Metric,
    pub results: Vec<SearchTopKBatchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpsertPointResponse {
    pub id: u64,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpsertPointsBatchItem {
    pub id: u64,
    pub values: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpsertPointsBatchResponse {
    pub created: u64,
    pub updated: u64,
    pub results: Vec<UpsertPointResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PointResponse {
    pub id: u64,
    pub values: Vec<f32>,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PointIdResponse {
    pub id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ListPointsResponse {
    pub points: Vec<PointIdResponse>,
    pub total: u64,
    pub next_offset: Option<u64>,
    pub next_after_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeletePointResponse {
    pub id: u64,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeleteCollectionResponse {
    pub name: String,
    pub deleted: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    pub metric: Option<Metric>,
    pub mode: Option<SearchMode>,
    pub target_recall: Option<f32>,
    pub filter: Option<Value>,
    pub include_payload: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct SearchTopKOptions {
    pub search: SearchOptions,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct ListPointsOptions {
    pub offset: i64,
    pub limit: Option<u32>,
    pub after_id: Option<u64>,
}

#[derive(Debug, Clone)]
struct ParsedBaseUrl {
    host: String,
    port: u16,
    path_prefix: String,
}

#[derive(Debug, Clone)]
pub struct AionBDClient {
    base_url: ParsedBaseUrl,
    timeout: Duration,
    api_key: Option<String>,
    bearer_token: Option<String>,
    default_headers: HashMap<String, String>,
}

impl AionBDClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        Self::with_options(base_url, ClientOptions::default())
    }

    pub fn with_options(base_url: impl AsRef<str>, options: ClientOptions) -> Result<Self> {
        Ok(Self {
            base_url: parse_base_url(base_url.as_ref())?,
            timeout: options.timeout,
            api_key: options.api_key,
            bearer_token: options.bearer_token,
            default_headers: options.headers,
        })
    }

    pub fn live(&self) -> Result<LiveResponse> {
        self.request_json("GET", "/live", None)
    }

    pub fn ready(&self) -> Result<ReadyResponse> {
        self.request_json("GET", "/ready", None)
    }

    pub fn health(&self) -> Result<ReadyResponse> {
        self.ready()
    }

    pub fn metrics(&self) -> Result<MetricsResponse> {
        self.request_json("GET", "/metrics", None)
    }

    pub fn metrics_prometheus(&self) -> Result<String> {
        self.request_raw("GET", "/metrics/prometheus", None)
    }

    pub fn distance(
        &self,
        left: &[f32],
        right: &[f32],
        metric: Metric,
    ) -> Result<DistanceResponse> {
        let body = json!({
            "left": left,
            "right": right,
            "metric": metric,
        });
        self.request_json("POST", "/distance", Some(body))
    }

    pub fn create_collection(
        &self,
        name: &str,
        dimension: u32,
        strict_finite: bool,
    ) -> Result<CollectionResponse> {
        let body = json!({
            "name": name,
            "dimension": dimension,
            "strict_finite": strict_finite,
        });
        self.request_json("POST", "/collections", Some(body))
    }

    pub fn list_collections(&self) -> Result<ListCollectionsResponse> {
        self.request_json("GET", "/collections", None)
    }

    pub fn get_collection(&self, name: &str) -> Result<CollectionResponse> {
        let path = collection_item_path(name);
        self.request_json("GET", &path, None)
    }

    pub fn search_collection(
        &self,
        collection: &str,
        query: &[f32],
        options: Option<SearchOptions>,
    ) -> Result<SearchResponse> {
        let options = options.unwrap_or_default();
        let mut body = Map::new();
        body.insert("query".into(), json!(query));
        apply_search_options(&mut body, &options);

        let path = collection_child_path(collection, "search");
        self.request_json("POST", &path, Some(Value::Object(body)))
    }

    pub fn search_collection_top_k(
        &self,
        collection: &str,
        query: &[f32],
        options: Option<SearchTopKOptions>,
    ) -> Result<SearchTopKResponse> {
        let body = self.search_top_k_body(query, options)?;
        let path = collection_child_path(collection, "search/topk");
        self.request_json("POST", &path, Some(Value::Object(body)))
    }

    pub fn search_collection_top_k_batch(
        &self,
        collection: &str,
        queries: &[Vec<f32>],
        options: Option<SearchTopKOptions>,
    ) -> Result<SearchTopKBatchResponse> {
        let mut body = self.search_top_k_body(&[], options)?;
        body.remove("query");
        body.insert("queries".into(), json!(queries));

        let path = collection_child_path(collection, "search/topk/batch");
        self.request_json("POST", &path, Some(Value::Object(body)))
    }

    pub fn upsert_point(
        &self,
        collection: &str,
        point_id: u64,
        values: &[f32],
        payload: Option<Value>,
    ) -> Result<UpsertPointResponse> {
        let mut body = Map::new();
        body.insert("values".into(), json!(values));
        if let Some(payload) = payload {
            body.insert("payload".into(), payload);
        }

        let path = point_item_path(collection, point_id);
        self.request_json("PUT", &path, Some(Value::Object(body)))
    }

    pub fn upsert_points_batch(
        &self,
        collection: &str,
        points: &[UpsertPointsBatchItem],
    ) -> Result<UpsertPointsBatchResponse> {
        let body = json!({ "points": points });
        let path = collection_child_path(collection, "points");
        self.request_json("POST", &path, Some(body))
    }

    pub fn get_point(&self, collection: &str, point_id: u64) -> Result<PointResponse> {
        let path = point_item_path(collection, point_id);
        self.request_json("GET", &path, None)
    }

    pub fn list_points(
        &self,
        collection: &str,
        options: Option<ListPointsOptions>,
    ) -> Result<ListPointsResponse> {
        let path = build_list_points_path(collection, options)?;
        self.request_json("GET", &path, None)
    }

    pub fn delete_point(&self, collection: &str, point_id: u64) -> Result<DeletePointResponse> {
        let path = point_item_path(collection, point_id);
        self.request_json("DELETE", &path, None)
    }

    pub fn delete_collection(&self, name: &str) -> Result<DeleteCollectionResponse> {
        let path = collection_item_path(name);
        self.request_json("DELETE", &path, None)
    }

    fn search_top_k_body(
        &self,
        query: &[f32],
        options: Option<SearchTopKOptions>,
    ) -> Result<Map<String, Value>> {
        let (search, limit, include_limit) = match options {
            None => (SearchOptions::default(), 10_u32, true),
            Some(opts) => (
                opts.search,
                opts.limit.unwrap_or_default(),
                opts.limit.is_some(),
            ),
        };

        if include_limit && limit == 0 {
            return Err(AionBDError::InvalidArgument(
                "limit must be a positive integer".to_string(),
            ));
        }

        let mut body = Map::new();
        body.insert("query".into(), json!(query));
        apply_search_options(&mut body, &search);
        if include_limit {
            body.insert("limit".into(), json!(limit));
        }
        Ok(body)
    }

    fn request_json<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<T> {
        let payload = self.do_request(method, path, body, false)?;
        self.deserialize_json(method, path, payload)
    }

    fn deserialize_json<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        payload: String,
    ) -> Result<T> {
        serde_json::from_str::<T>(&payload).map_err(|source| AionBDError::InvalidJson {
            method: method.to_string(),
            path: path.to_string(),
            body: payload,
            source,
        })
    }

    fn request_raw(&self, method: &str, path: &str, body: Option<Value>) -> Result<String> {
        self.do_request(method, path, body, true)
    }

    fn do_request(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        raw: bool,
    ) -> Result<String> {
        let target = format!("{}{}", self.base_url.path_prefix, path);
        let mut stream = TcpStream::connect((self.base_url.host.as_str(), self.base_url.port))
            .map_err(|source| transport_error(method, path, source))?;
        stream
            .set_read_timeout(Some(self.timeout))
            .and_then(|_| stream.set_write_timeout(Some(self.timeout)))
            .map_err(|source| transport_error(method, path, source))?;

        let body_payload = body
            .map(|value| serde_json::to_string(&value))
            .transpose()
            .map_err(|source| AionBDError::InvalidJson {
                method: method.to_string(),
                path: path.to_string(),
                body: String::new(),
                source,
            })?;

        let request = self.build_http_request(method, &target, body_payload.as_deref(), raw);

        stream
            .write_all(request.as_bytes())
            .and_then(|_| stream.flush())
            .map_err(|source| transport_error(method, path, source))?;

        let mut raw_response = Vec::new();
        stream
            .read_to_end(&mut raw_response)
            .map_err(|source| transport_error(method, path, source))?;

        let parsed = parse_http_response(&raw_response)
            .map_err(|source| transport_error(method, path, source))?;
        let payload = String::from_utf8_lossy(&parsed.body).into_owned();

        if !(200..=299).contains(&parsed.status) {
            return Err(AionBDError::Http {
                status: parsed.status,
                method: method.to_string(),
                path: path.to_string(),
                body: payload,
            });
        }

        Ok(payload)
    }

    fn build_http_request(
        &self,
        method: &str,
        path: &str,
        body_payload: Option<&str>,
        raw: bool,
    ) -> String {
        let mut request = String::new();
        request.push_str(&format!("{method} {path} HTTP/1.1\r\n"));
        request.push_str(&format!("Host: {}\r\n", self.base_url.host));
        request.push_str("Connection: close\r\n");
        request.push_str(&format!(
            "Accept: {}\r\n",
            if raw {
                "text/plain"
            } else {
                "application/json"
            }
        ));

        for (name, value) in &self.default_headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        if let Some(api_key) = &self.api_key {
            request.push_str(&format!("x-api-key: {api_key}\r\n"));
        }
        if let Some(token) = &self.bearer_token {
            request.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }

        if let Some(payload) = body_payload {
            request.push_str("Content-Type: application/json\r\n");
            request.push_str(&format!("Content-Length: {}\r\n", payload.len()));
            request.push_str("\r\n");
            request.push_str(payload);
        } else {
            request.push_str("\r\n");
        }

        request
    }
}

fn build_list_points_path(collection: &str, options: Option<ListPointsOptions>) -> Result<String> {
    let (offset, limit, include_limit, after_id) = match options {
        None => (0_i64, 100_u32, true, None),
        Some(opts) => (
            opts.offset,
            opts.limit.unwrap_or_default(),
            opts.limit.is_some(),
            opts.after_id,
        ),
    };

    if offset < 0 {
        return Err(AionBDError::InvalidArgument(
            "offset must be a non-negative integer".to_string(),
        ));
    }
    if after_id.is_some() && offset != 0 {
        return Err(AionBDError::InvalidArgument(
            "offset must be 0 when after_id is provided".to_string(),
        ));
    }
    if include_limit && limit == 0 {
        return Err(AionBDError::InvalidArgument(
            "limit must be a positive integer".to_string(),
        ));
    }

    let mut query = Vec::new();
    if include_limit {
        query.push(format!("limit={limit}"));
    }
    if let Some(after_id) = after_id {
        query.push(format!("after_id={after_id}"));
    } else {
        query.push(format!("offset={offset}"));
    }

    let base = collection_child_path(collection, "points");
    Ok(format!("{base}?{}", query.join("&")))
}

#[derive(Debug)]
struct ParsedHttpResponse {
    status: u16,
    body: Vec<u8>,
}

fn parse_http_response(raw: &[u8]) -> std::io::Result<ParsedHttpResponse> {
    let split_index = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing response headers")
        })?;

    let headers_block = &raw[..split_index];
    let body_block = &raw[split_index + 4..];
    let headers_text = String::from_utf8_lossy(headers_block);

    let mut lines = headers_text.split("\r\n");
    let status_line = lines.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing status line")
    })?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing status code"))?
        .parse::<u16>()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid status code"))?;

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    let body = if headers
        .get("transfer-encoding")
        .map(|value| value.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false)
    {
        decode_chunked_body(body_block)?
    } else {
        body_block.to_vec()
    };

    Ok(ParsedHttpResponse { status, body })
}

fn decode_chunked_body(body: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut result = Vec::new();
    let mut cursor = 0_usize;

    while cursor < body.len() {
        let line_end = find_crlf(body, cursor).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid chunk header")
        })?;
        let line = std::str::from_utf8(&body[cursor..line_end]).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid chunk size line")
        })?;
        let hex_size = line.split(';').next().unwrap_or("0").trim();
        let size = usize::from_str_radix(hex_size, 16).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid chunk size")
        })?;
        cursor = line_end + 2;

        if size == 0 {
            break;
        }
        let end = cursor + size;
        if end > body.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk exceeds payload size",
            ));
        }
        result.extend_from_slice(&body[cursor..end]);
        cursor = end + 2;
    }

    Ok(result)
}

fn find_crlf(buffer: &[u8], start: usize) -> Option<usize> {
    buffer[start..]
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|index| start + index)
}

fn parse_base_url(base_url: &str) -> Result<ParsedBaseUrl> {
    let trimmed = base_url.trim();
    let normalized = if trimmed.is_empty() {
        DEFAULT_BASE_URL
    } else {
        trimmed
    };

    let remainder = normalized.strip_prefix("http://").ok_or_else(|| {
        AionBDError::InvalidOption("base URL must start with http://".to_string())
    })?;

    let (authority, path_prefix) = match remainder.split_once('/') {
        Some((authority, path)) => {
            let prefix = format!("/{}", path.trim_matches('/'));
            let prefix = if prefix == "/" { String::new() } else { prefix };
            (authority, prefix)
        }
        None => (remainder, String::new()),
    };

    if authority.is_empty() {
        return Err(AionBDError::InvalidOption(
            "base URL authority cannot be empty".to_string(),
        ));
    }

    let (host, port) = if let Some((host, port)) = authority.rsplit_once(':') {
        let parsed_port = port
            .parse::<u16>()
            .map_err(|_| AionBDError::InvalidOption("invalid base URL port".to_string()))?;
        (host.to_string(), parsed_port)
    } else {
        (authority.to_string(), 80_u16)
    };

    if host.is_empty() {
        return Err(AionBDError::InvalidOption(
            "base URL host cannot be empty".to_string(),
        ));
    }

    Ok(ParsedBaseUrl {
        host,
        port,
        path_prefix,
    })
}

fn collection_item_path(name: &str) -> String {
    format!("/collections/{}", escape_segment(name))
}

fn collection_child_path(collection: &str, child: &str) -> String {
    format!(
        "{}/{}",
        collection_item_path(collection),
        child.trim_start_matches('/')
    )
}

fn point_item_path(collection: &str, point_id: u64) -> String {
    format!("{}/points/{point_id}", collection_item_path(collection))
}

fn transport_error(method: &str, path: &str, source: std::io::Error) -> AionBDError {
    AionBDError::Transport {
        method: method.to_string(),
        path: path.to_string(),
        source,
    }
}

fn escape_segment(value: impl AsRef<str>) -> String {
    let mut output = String::new();
    for byte in value.as_ref().as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.' | b'~') {
            output.push(char::from(*byte));
        } else {
            output.push('%');
            output.push_str(&format!("{:02X}", byte));
        }
    }
    output
}

fn apply_search_options(body: &mut Map<String, Value>, options: &SearchOptions) {
    let metric = options.metric.unwrap_or_default();
    let mode = options.mode.unwrap_or_default();
    body.insert("metric".into(), json!(metric));
    body.insert("mode".into(), json!(mode));
    if let Some(target_recall) = options.target_recall {
        body.insert("target_recall".into(), json!(target_recall));
    }
    if let Some(filter) = &options.filter {
        body.insert("filter".into(), filter.clone());
    }
    if let Some(include_payload) = options.include_payload {
        body.insert("include_payload".into(), json!(include_payload));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_headers(request: &str) -> HashMap<String, String> {
        let head = request.split("\r\n\r\n").next().expect("headers split");
        let mut lines = head.split("\r\n");
        let _request_line = lines.next().expect("request line");

        let mut headers = HashMap::new();
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            }
        }
        headers
    }

    #[test]
    fn search_collection_top_k_omits_limit_when_none() {
        let client = AionBDClient::new("http://127.0.0.1:8080").expect("client");
        let body = client
            .search_top_k_body(
                &[1.0, 2.0],
                Some(SearchTopKOptions {
                    limit: None,
                    ..SearchTopKOptions::default()
                }),
            )
            .expect("body");

        assert_eq!(body.get("limit"), None);
        assert_eq!(body.get("metric"), Some(&json!("dot")));
        assert_eq!(body.get("mode"), Some(&json!("auto")));
    }

    #[test]
    fn list_points_omits_limit_and_supports_cursor_mode() {
        let path = build_list_points_path(
            "demo",
            Some(ListPointsOptions {
                offset: 0,
                limit: None,
                after_id: Some(7),
            }),
        )
        .expect("path");

        assert_eq!(path, "/collections/demo/points?after_id=7");
    }

    #[test]
    fn list_points_rejects_mixed_offset_and_after_id() {
        let error = build_list_points_path(
            "demo",
            Some(ListPointsOptions {
                offset: 1,
                limit: Some(100),
                after_id: Some(2),
            }),
        )
        .expect_err("mixed mode should fail");

        match error {
            AionBDError::InvalidArgument(message) => {
                assert!(message.contains("offset must be 0"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn client_sends_auth_headers() {
        let client = AionBDClient::with_options(
            "http://127.0.0.1:8080",
            ClientOptions {
                api_key: Some("key-a".to_string()),
                bearer_token: Some("token-a".to_string()),
                ..ClientOptions::default()
            },
        )
        .expect("client");

        let request = client.build_http_request("GET", "/live", None, false);
        let headers = request_headers(&request);

        assert_eq!(headers.get("x-api-key"), Some(&"key-a".to_string()));
        assert_eq!(
            headers.get("authorization"),
            Some(&"Bearer token-a".to_string())
        );
    }

    #[test]
    fn metrics_prometheus_uses_text_plain_accept() {
        let client = AionBDClient::new("http://127.0.0.1:8080").expect("client");
        let request = client.build_http_request("GET", "/metrics/prometheus", None, true);
        let headers = request_headers(&request);

        assert_eq!(headers.get("accept"), Some(&"text/plain".to_string()));
    }

    #[test]
    fn http_error_status_and_body_are_parsed() {
        let raw = b"HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{\"error\":\"boom\"}";
        let parsed = parse_http_response(raw).expect("parsed");

        assert_eq!(parsed.status, 400);
        assert_eq!(
            String::from_utf8_lossy(&parsed.body),
            "{\"error\":\"boom\"}"
        );
    }

    #[test]
    fn invalid_json_response_is_exposed() {
        let client = AionBDClient::new("http://127.0.0.1:8080").expect("client");
        let error = client
            .deserialize_json::<LiveResponse>("GET", "/live", "not-json".to_string())
            .expect_err("should fail");

        match error {
            AionBDError::InvalidJson { body, .. } => assert_eq!(body, "not-json"),
            other => panic!("unexpected error: {other}"),
        }
    }
}
