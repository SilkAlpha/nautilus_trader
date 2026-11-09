// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Integration tests for Bitget HTTP client using a mock server.

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use jiff::Timestamp;
use nautilus_bitget::{
    common::{
        consts::BITGET_VENUE,
        enums::{
            BitgetAccountType, BitgetBboSideType, BitgetEnvironment, BitgetMarginMode,
            BitgetOrderSmpType, BitgetOrderType, BitgetPositionIdx, BitgetProductType,
            BitgetRepayStatus, BitgetTriggerType, BitgetUnifiedMarginStatus,
        },
        parse::parse_linear_instrument,
        urls::bitget_http_base_url,
    },
    http::{
        client::{BitgetHttpClient, BitgetRawHttpClient},
        models::BitgetInstrumentLinearResponse,
        query::{
            BitgetFeeRateParams, BitgetInstrumentsInfoParamsBuilder, BitgetNativeTpSlParams,
            BitgetPositionListParamsBuilder, BitgetWalletBalanceParams,
        },
    },
};
use nautilus_common::{cache::InstrumentLookupError, testing::wait_until_async};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    data::BarType,
    enums::{
        AvgPxReconciliation, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce,
        TriggerType,
    },
    identifiers::{AccountId, ClientOrderId, InstrumentId, Symbol},
    instruments::{CurrencyPair, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_network::http::HttpClient;
use rstest::rstest;
use serde_json::{Value, json};

// (endpoint, query)
type EndpointQueries = Arc<tokio::sync::Mutex<Vec<(String, HashMap<String, String>)>>>;

// Bitget signs every private request with these four headers
const AUTH_HEADERS: [&str; 4] = [
    "ACCESS-KEY",
    "ACCESS-SIGN",
    "ACCESS-TIMESTAMP",
    "ACCESS-PASSPHRASE",
];

// Bitget rejects requests whose ACCESS-TIMESTAMP is more than 30 s off the server time
const BITGET_TIMESTAMP_WINDOW_MS: i64 = 30_000;

#[derive(Debug, Clone, Copy)]
enum RequiredInstrumentCachePath {
    Trades,
    FundingRates,
    OrderbookSnapshot,
    Bars,
}

/// Captured order submission for validation in tests.
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
struct CapturedOrder {
    path: String,
    category: String,
    symbol: String,
    side: String,
    order_type: String,
    qty: String,
    price: Option<String>,
    trigger_price: Option<String>,
    trigger_order_type: Option<String>,
    trigger_order_price: Option<String>,
    time_in_force: Option<String>,
    reduce_only: Option<String>,
    order_link_id: Option<String>,
    pos_side: Option<String>,
    bbo_level: Option<String>,
    raw_body: Value,
}

#[allow(dead_code)]
#[derive(Clone)]
struct TestServerState {
    request_count: Arc<tokio::sync::Mutex<usize>>,
    wallet_balance_requests: Arc<tokio::sync::Mutex<usize>>,
    endpoint_queries: EndpointQueries,
    realtime_requests: Arc<tokio::sync::Mutex<usize>>,
    history_requests: Arc<tokio::sync::Mutex<usize>>,
    position_queries: Arc<tokio::sync::Mutex<Vec<HashMap<String, String>>>>,
    order_submissions: Arc<tokio::sync::Mutex<Vec<CapturedOrder>>>,
    batch_cancel_requests: Arc<tokio::sync::Mutex<Vec<Value>>>,
    request_headers: Arc<tokio::sync::Mutex<Vec<axum::http::HeaderMap>>>,
}

impl Default for TestServerState {
    fn default() -> Self {
        Self {
            request_count: Arc::new(tokio::sync::Mutex::new(0)),
            wallet_balance_requests: Arc::new(tokio::sync::Mutex::new(0)),
            endpoint_queries: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            realtime_requests: Arc::new(tokio::sync::Mutex::new(0)),
            history_requests: Arc::new(tokio::sync::Mutex::new(0)),
            position_queries: Arc::default(),
            order_submissions: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            batch_cancel_requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            request_headers: Arc::default(),
        }
    }
}

/// Wait for the test server to be ready by polling a health endpoint.
async fn wait_for_server(addr: SocketAddr, path: &str) {
    let health_url = format!("http://{addr}{path}");
    let http_client = HttpClient::builder().build().unwrap();
    wait_until_async(
        || {
            let url = health_url.clone();
            let client = http_client.clone();
            async move { client.get(url, None, None, Some(1), None).await.is_ok() }
        },
        Duration::from_secs(5),
    )
    .await;
}

// Load test data from existing files
#[allow(dead_code)]
fn load_test_data(filename: &str) -> Value {
    let path = format!("test_data/{filename}");
    let content = std::fs::read_to_string(path).unwrap();
    serde_json::from_str(&content).unwrap()
}

/// Wraps `data` in the Bitget v3 success envelope.
fn bitget_ok(data: &Value) -> Value {
    json!({
        "code": "00000",
        "msg": "success",
        "requestTime": 1794193082020i64,
        "data": data
    })
}

/// Returns a Bitget v3 error envelope with the given HTTP status.
fn bitget_error(status: StatusCode, code: &str, msg: &str) -> Response {
    (
        status,
        Json(json!({
            "code": code,
            "msg": msg,
            "requestTime": 1794193082020i64,
            "data": null
        })),
    )
        .into_response()
}

/// Rejects requests lacking Bitget signed auth headers with 401.
fn require_bitget_auth(headers: &axum::http::HeaderMap) -> Option<Response> {
    if AUTH_HEADERS.iter().any(|name| !headers.contains_key(*name)) {
        return Some(bitget_error(
            StatusCode::UNAUTHORIZED,
            "40006",
            "Invalid ACCESS_KEY",
        ));
    }
    None
}

/// Bitget returns the oldest id of a page as `cursor`; the request after the last page yields
/// `{"list": null, "cursor": null}`.
fn empty_page() -> Value {
    bitget_ok(&json!({ "list": null, "cursor": null }))
}

fn has_cursor(query: &HashMap<String, String>) -> bool {
    query.get("cursor").is_some_and(|cursor| !cursor.is_empty())
}

/// Two resting TSLAUSDT post-only orders (`open-order-1`, `open-order-2`), taken from the demo
/// order history and reset to `new`.
fn open_orders() -> Vec<Value> {
    let history = load_test_data("http_get_orders_history.json");
    let rows = history["data"]["list"].as_array().unwrap();
    rows[2..4]
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let mut order = row.clone();
            order["orderId"] = json!(format!("open-order-{}", index + 1));
            order["clientOid"] = json!(format!("client-open-{}", index + 1));
            order["orderStatus"] = json!("new");
            order["cancelReason"] = json!("");
            order
        })
        .collect()
}

/// The demo order history (filled and cancelled TSLAUSDT orders) followed by a copy of
/// `open-order-1`, which history and the open list both return while an order is live.
fn history_with_duplicate() -> Vec<Value> {
    let history = load_test_data("http_get_orders_history.json");
    let mut rows = history["data"]["list"].as_array().unwrap()[..2].to_vec();
    rows.insert(0, open_orders()[0].clone());
    rows
}

// Mock endpoint handlers
#[allow(dead_code)]
async fn handle_get_server_time() -> impl IntoResponse {
    Json(load_test_data("http_get_server_time.json"))
}

#[allow(dead_code)]
async fn handle_get_instruments(query: Query<HashMap<String, String>>) -> impl IntoResponse {
    let category = query.get("category").map(String::as_str);

    let filename = match category {
        Some("USDT-FUTURES") => "http_get_instruments_linear.json",
        Some("SPOT") => "http_get_instruments_spot.json",
        Some("COIN-FUTURES") => "http_get_instruments_inverse.json",
        _ => {
            return bitget_error(
                StatusCode::BAD_REQUEST,
                "400172",
                "Parameter verification failed",
            );
        }
    };

    let instruments = load_test_data(filename);
    Json(instruments).into_response()
}

#[allow(dead_code)]
async fn handle_get_klines(query: Query<HashMap<String, String>>) -> impl IntoResponse {
    if !query.contains_key("category")
        || !query.contains_key("symbol")
        || !query.contains_key("interval")
    {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    }

    let klines = load_test_data("http_get_klines_linear.json");
    Json(klines).into_response()
}

#[allow(dead_code)]
async fn handle_get_trades(query: Query<HashMap<String, String>>) -> impl IntoResponse {
    if !query.contains_key("category") || !query.contains_key("symbol") {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    }

    let trades = load_test_data("http_get_trades_recent.json");
    Json(trades).into_response()
}

#[allow(dead_code)]
async fn handle_get_executions(Query(query): Query<HashMap<String, String>>) -> impl IntoResponse {
    // `/api/v3/trade/fills` takes no symbol filter: page 1 carries an ETHUSDT row the client
    // must drop, page 2 the remaining TSLAUSDT fills
    let mut executions = load_test_data("http_get_executions.json");
    let rows = executions["data"]["list"].as_array().unwrap().clone();
    let (list, next_cursor) = if has_cursor(&query) {
        (rows[1..].to_vec(), Value::Null)
    } else {
        let mut other_symbol = rows[1].clone();
        other_symbol["symbol"] = json!("ETHUSDT");
        other_symbol["execId"] = json!("eth-fill-1");
        (vec![rows[0].clone(), other_symbol], json!("trade-page"))
    };
    executions["data"]["list"] = json!(list);
    executions["data"]["cursor"] = next_cursor;

    Json(executions)
}

#[allow(dead_code)]
async fn handle_get_orders(
    Query(query): Query<HashMap<String, String>>,
    State(state): State<TestServerState>,
    headers: axum::http::HeaderMap,
) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    let mut count = state.request_count.lock().await;
    *count += 1;

    // Simulate rate limiting after 5 requests
    if *count > 5 {
        return bitget_error(StatusCode::TOO_MANY_REQUESTS, "429", "Too Many Requests");
    }

    if has_cursor(&query) {
        return Json(empty_page()).into_response();
    }

    let orders = load_test_data("http_get_orders_history.json");
    Json(orders).into_response()
}

/// Parses a place-order body, or `None` when it is not JSON or lacks a required field.
fn parse_order_body(body: &axum::body::Bytes) -> Option<Value> {
    let order_req: Value = serde_json::from_slice(body).ok()?;

    // Validate required fields
    let complete = order_req.get("category").is_some()
        && order_req.get("symbol").is_some()
        && order_req.get("side").is_some()
        && (order_req.get("orderType").is_some() || order_req.get("type").is_some())
        && order_req.get("qty").is_some();

    complete.then_some(order_req)
}

#[allow(dead_code)]
async fn handle_post_order(headers: axum::http::HeaderMap, body: axum::body::Bytes) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    let Some(order_req) = parse_order_body(&body) else {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    };

    // Return the demo place-order acknowledgement for this client order id
    let mut response = load_test_data("http_post_place_order.json");
    response["data"]["clientOid"] = order_req.get("clientOid").cloned().unwrap_or(json!(""));
    Json(response).into_response()
}

/// Stateful order handler that captures order details for test validation.
///
/// Serves both `place-order` and `place-strategy-order` (conditional orders).
#[allow(dead_code)]
async fn handle_post_order_with_capture(
    State(state): State<TestServerState>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    let Some(order_req) = parse_order_body(&body) else {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    };

    let field = |name: &str| {
        order_req
            .get(name)
            .and_then(|v| v.as_str())
            .map(String::from)
    };

    let captured = CapturedOrder {
        path: uri.path().to_string(),
        category: field("category").unwrap_or_default(),
        symbol: field("symbol").unwrap_or_default(),
        side: field("side").unwrap_or_default(),
        order_type: field("orderType").unwrap_or_default(),
        qty: field("qty").unwrap_or_default(),
        price: field("price"),
        trigger_price: field("triggerPrice"),
        trigger_order_type: field("triggerOrderType"),
        trigger_order_price: field("triggerOrderPrice"),
        time_in_force: field("timeInForce"),
        reduce_only: field("reduceOnly"),
        order_link_id: field("clientOid"),
        pos_side: field("posSide"),
        bbo_level: field("bboLevel"),
        raw_body: order_req.clone(),
    };

    {
        let mut orders = state.order_submissions.lock().await;
        orders.push(captured);
    }

    let fixture = if uri.path().ends_with("place-strategy-order") {
        "http_post_place_strategy_order.json"
    } else {
        "http_post_place_order.json"
    };
    Json(load_test_data(fixture)).into_response()
}

/// Serves `order-info` with the demo filled IOC order (the post-submit lookup).
#[allow(dead_code)]
async fn handle_get_order_info(headers: axum::http::HeaderMap) -> Response {
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    Json(load_test_data("http_get_order_info.json")).into_response()
}

/// Serves the conditional (strategy) order list with the demo trigger order.
#[allow(dead_code)]
async fn handle_get_strategy_orders(
    Query(query): Query<HashMap<String, String>>,
    State(state): State<TestServerState>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    state
        .endpoint_queries
        .lock()
        .await
        .push((uri.path().to_string(), query.clone()));

    if has_cursor(&query) {
        return Json(empty_page()).into_response();
    }

    Json(load_test_data("http_get_strategy_orders_history.json")).into_response()
}

#[allow(dead_code)]
async fn handle_get_wallet_balance(
    State(state): State<TestServerState>,
    headers: axum::http::HeaderMap,
) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    *state.wallet_balance_requests.lock().await += 1;
    let mut wallet = load_test_data("http_get_wallet_balance.json");

    // The demo account holds no borrowed coin: add an ETH row whose debt makes a SPOT short
    let assets = wallet["data"]["assets"].as_array_mut().unwrap();
    let mut eth = assets[1].clone();
    eth["coin"] = json!("ETH");
    eth["equity"] = json!("-0.06142");
    eth["balance"] = json!("0");
    eth["available"] = json!("0");
    eth["usdValue"] = json!("-189.31");
    eth["balanceOriginal"] = json!("0");
    eth["debt"] = json!("0.06142");
    assets.push(eth);

    Json(wallet).into_response()
}

#[allow(dead_code)]
async fn handle_get_account_info(headers: axum::http::HeaderMap) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    let timestamp = headers["ACCESS-TIMESTAMP"]
        .to_str()
        .expect("timestamp header is valid UTF-8")
        .parse::<i64>()
        .expect("timestamp header is milliseconds");
    let server_time_ms = Timestamp::now().as_millisecond();

    if (server_time_ms - timestamp).abs() > BITGET_TIMESTAMP_WINDOW_MS {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "40008",
            "Request timestamp expired",
        );
    }

    let account_info = load_test_data("http_get_account_info.json");
    Json(account_info).into_response()
}

#[allow(dead_code)]
async fn handle_cancel_order(headers: axum::http::HeaderMap, body: axum::body::Bytes) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    // Parse JSON body
    let Ok(cancel_req): Result<Value, _> = serde_json::from_slice(&body) else {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    };

    // Bitget needs `orderId` or `clientOid`
    if cancel_req.get("orderId").is_none() && cancel_req.get("clientOid").is_none() {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    }

    // Return the demo cancel acknowledgement
    Json(load_test_data("http_post_cancel_order.json")).into_response()
}

#[allow(dead_code)]
async fn handle_batch_cancel_orders(
    State(state): State<TestServerState>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    let results: Vec<Value> = request
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    json!({
                        "orderId": entry.get("orderId").cloned().unwrap_or(Value::Null),
                        "clientOid": entry.get("clientOid").cloned().unwrap_or(Value::Null),
                        "code": "0",
                        "msg": "success"
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    state.batch_cancel_requests.lock().await.push(request);

    Json(bitget_ok(&json!(results))).into_response()
}

#[allow(dead_code)]
async fn handle_get_positions(headers: axum::http::HeaderMap) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    let positions = load_test_data("http_get_positions.json");
    Json(positions).into_response()
}

#[allow(dead_code)]
async fn handle_get_fee_rate(headers: axum::http::HeaderMap) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    let fee_rate = load_test_data("http_get_fee_rate.json");
    Json(fee_rate).into_response()
}

#[allow(dead_code)]
async fn handle_repay(headers: axum::http::HeaderMap, body: axum::body::Bytes) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    // Parse JSON body
    let Ok(repay_req): Result<Value, _> = serde_json::from_slice(&body) else {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    };

    // Validate required fields
    if repay_req.get("coin").is_none() {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    }

    // Bitget answers a repay with `"data": "success"` or a business error code
    if repay_req["coin"] == "FAIL" {
        return bitget_error(StatusCode::BAD_REQUEST, "25202", "Insufficient balance");
    }

    Json(bitget_ok(&json!("success"))).into_response()
}

#[allow(dead_code)]
async fn handle_get_orders_realtime(
    query: Query<HashMap<String, String>>,
    State(state): State<TestServerState>,
    headers: axum::http::HeaderMap,
) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    // Check required parameters
    if !query.contains_key("category") {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    }

    // Track queries (Bitget takes no settle coin)
    {
        let mut queries = state.endpoint_queries.lock().await;
        queries.push(("realtime".to_string(), query.0.clone()));
    }

    {
        let mut count = state.realtime_requests.lock().await;
        *count += 1;
    }

    if has_cursor(&query) {
        return Json(empty_page()).into_response();
    }

    let mut orders = open_orders();

    if let Some(limit_str) = query.get("limit")
        && let Ok(limit) = limit_str.parse::<usize>()
    {
        orders.truncate(limit);
    }

    Json(bitget_ok(
        &json!({ "list": orders, "cursor": "open-order-2" }),
    ))
    .into_response()
}

#[allow(dead_code)]
async fn handle_get_orders_history_reconciliation(
    query: Query<HashMap<String, String>>,
    State(state): State<TestServerState>,
    headers: axum::http::HeaderMap,
) -> Response {
    // Check for authentication headers
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    // Check required parameters
    if !query.contains_key("category") {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    }

    // Track queries (Bitget takes no settle coin)
    {
        let mut queries = state.endpoint_queries.lock().await;
        queries.push(("history".to_string(), query.0.clone()));
    }

    {
        let mut count = state.history_requests.lock().await;
        *count += 1;
    }

    if has_cursor(&query) {
        return Json(empty_page()).into_response();
    }

    let mut orders = history_with_duplicate();

    if let Some(limit_str) = query.get("limit")
        && let Ok(limit) = limit_str.parse::<usize>()
    {
        orders.truncate(limit);
    }

    Json(bitget_ok(
        &json!({ "list": orders, "cursor": "history-page-2" }),
    ))
    .into_response()
}

#[allow(dead_code)]
fn create_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route("/api/v3/market/candles", get(handle_get_klines))
        .route("/api/v3/market/fills", get(handle_get_trades))
        .route("/api/v3/trade/fills", get(handle_get_executions))
        .route("/api/v3/trade/history-orders", get(handle_get_orders))
        .route("/api/v3/trade/unfilled-orders", get(handle_get_orders))
        .route("/api/v3/trade/place-order", post(handle_post_order))
        .route("/api/v3/trade/cancel-order", post(handle_cancel_order))
        .route(
            "/api/v3/trade/cancel-batch",
            post(handle_batch_cancel_orders),
        )
        .route("/api/v3/account/assets", get(handle_get_wallet_balance))
        .route("/api/v3/account/settings", get(handle_get_account_info))
        .route(
            "/api/v3/position/current-position",
            get(handle_get_positions),
        )
        .route("/api/v3/account/all-fee-rate", get(handle_get_fee_rate))
        .route("/api/v3/account/repay", post(handle_repay))
        .with_state(state)
}

#[allow(dead_code)]
async fn start_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    // Bind to port 0 to let the OS assign an available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = TestServerState::default();
    let router = create_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // Give server time to start
    wait_for_server(addr, "/api/v3/market/time").await;
    Ok((addr, state))
}

fn credentialed_client(base_url: String) -> BitgetHttpClient {
    BitgetHttpClient::with_credentials(
        "test_api_key".to_string(),
        "test_api_secret".to_string(),
        "test_api_passphrase".to_string(),
        Some(base_url),
        60,
        3,
        1000,
        10_000,
        5_000,
        None,
    )
    .unwrap()
}

#[rstest]
#[case::trades(RequiredInstrumentCachePath::Trades)]
#[case::funding_rates(RequiredInstrumentCachePath::FundingRates)]
#[case::orderbook_snapshot(RequiredInstrumentCachePath::OrderbookSnapshot)]
#[case::bars(RequiredInstrumentCachePath::Bars)]
#[tokio::test]
async fn test_public_market_data_request_missing_cached_instrument_returns_lookup_error(
    #[case] path: RequiredInstrumentCachePath,
) {
    let client = BitgetHttpClient::new(
        Some("http://127.0.0.1:9".to_string()),
        1,
        0,
        1,
        1,
        5_000,
        None,
    )
    .unwrap();
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE);

    let result = match path {
        RequiredInstrumentCachePath::Trades => client
            .request_trades(BitgetProductType::Linear, instrument_id, None)
            .await
            .map(|_| ()),
        RequiredInstrumentCachePath::FundingRates => client
            .request_funding_rates(BitgetProductType::Linear, instrument_id, None, None, None)
            .await
            .map(|_| ()),
        RequiredInstrumentCachePath::OrderbookSnapshot => client
            .request_orderbook_snapshot(BitgetProductType::Linear, instrument_id, None)
            .await
            .map(|_| ()),
        RequiredInstrumentCachePath::Bars => {
            let bar_type = BarType::from("BTCUSDT-PERP.BITGET-1-MINUTE-LAST-EXTERNAL");
            client
                .request_bars(BitgetProductType::Linear, bar_type, None, None, None, true)
                .await
                .map(|_| ())
        }
    };

    assert_eq!(
        result.unwrap_err().to_string(),
        InstrumentLookupError::not_found(instrument_id).to_string()
    );
}

#[rstest]
#[tokio::test]
async fn test_client_creation() {
    let client = BitgetHttpClient::new(None, 60, 3, 1000, 10_000, 5_000, None).unwrap();

    assert!(client.base_url().contains("bitget.com"));
    assert!(client.credential().is_none());
}

#[rstest]
#[tokio::test]
async fn test_client_with_credentials() {
    let client = credentialed_client("https://api.bitget.com".to_string());

    assert!(client.credential().is_some());
}

#[rstest]
#[case(true, false, BitgetEnvironment::Demo)]
#[case(false, true, BitgetEnvironment::Testnet)]
#[case(false, false, BitgetEnvironment::Mainnet)]
fn test_clients_new_with_env_resolve_default_url(
    #[case] demo: bool,
    #[case] testnet: bool,
    #[case] environment: BitgetEnvironment,
) {
    let raw_client = BitgetRawHttpClient::new_with_env(
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        None,
        demo,
        testnet,
        60,
        3,
        1000,
        10_000,
        5_000,
        None,
    )
    .unwrap();
    let client = BitgetHttpClient::new_with_env(
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        None,
        demo,
        testnet,
        60,
        3,
        1000,
        10_000,
        5_000,
        None,
    )
    .unwrap();
    let expected = bitget_http_base_url(environment);

    assert_eq!(raw_client.base_url(), expected);
    assert_eq!(client.base_url(), expected);
    // Demo trading (and Testnet, which Bitget lacks) routes through `paptrading: 1`
    assert_eq!(raw_client.is_demo_trading(), demo || testnet);
}

#[rstest]
#[tokio::test]
async fn test_testnet_urls() {
    // Bitget has no testnet host: Testnet resolves to the shared REST host plus demo trading
    assert_eq!(
        bitget_http_base_url(BitgetEnvironment::Testnet),
        bitget_http_base_url(BitgetEnvironment::Mainnet)
    );

    let client = BitgetHttpClient::new(
        Some(bitget_http_base_url(BitgetEnvironment::Testnet).to_string()),
        60,
        3,
        1000,
        10_000,
        5_000,
        None,
    )
    .unwrap();

    assert_eq!(client.base_url(), "https://api.bitget.com");
}

#[rstest]
#[tokio::test]
async fn test_custom_base_url() {
    let custom_url = "https://custom.bitget.com";
    let client = BitgetHttpClient::new(
        Some(custom_url.to_string()),
        60,
        3,
        1000,
        10_000,
        5_000,
        None,
    )
    .unwrap();

    assert_eq!(client.base_url(), custom_url);
}

/// Records the request headers and serves the account settings.
async fn handle_get_account_info_capture(
    State(state): State<TestServerState>,
    headers: axum::http::HeaderMap,
) -> Response {
    state.request_headers.lock().await.push(headers.clone());
    handle_get_account_info(headers).await
}

#[rstest]
#[case::demo(true)]
#[case::mainnet(false)]
#[tokio::test]
async fn test_demo_trading_sends_paptrading_header(#[case] demo: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = TestServerState::default();
    let router = Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route(
            "/api/v3/account/settings",
            get(handle_get_account_info_capture),
        )
        .with_state(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    wait_for_server(addr, "/api/v3/market/time").await;

    let client = BitgetHttpClient::new_with_env(
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(format!("http://{addr}")),
        demo,
        false,
        60,
        3,
        1000,
        10_000,
        5_000,
        None,
    )
    .unwrap();

    client.get_account_info().await.unwrap();

    let headers = state.request_headers.lock().await;
    assert_eq!(headers.len(), 1);
    assert_eq!(
        headers[0].get("paptrading").map(|v| v.to_str().unwrap()),
        demo.then_some("1")
    );
    assert_eq!(headers[0]["ACCESS-PASSPHRASE"], "test_api_passphrase");
    assert_eq!(headers[0]["locale"], "en-US");
}

#[rstest]
#[tokio::test]
async fn test_request_fill_reports_walks_cursor_and_filters_symbol() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");
    let client = credentialed_client(base_url);
    let instruments = client
        .request_instruments(BitgetProductType::Linear, None, None)
        .await
        .unwrap();

    for instrument in instruments {
        client.cache_instrument(instrument);
    }

    let reports = client
        .request_fill_reports(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            Some(InstrumentId::from("TSLAUSDT-PERP.BITGET")),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let trade_ids: Vec<String> = reports
        .iter()
        .map(|report| report.trade_id.to_string())
        .collect();

    assert_eq!(
        trade_ids,
        vec![
            "1503520820379758592",
            "1503520806874099712",
            "1503520362420482048",
            "1503520348428283904",
        ]
    );
}

#[rstest]
#[tokio::test]
async fn test_get_server_time() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let response = client.get_server_time().await.unwrap();
    assert_eq!(response.result.time_second_str(), "1794192837");
    assert_eq!(response.result.time_nano_str(), "1794192837141960000");
}

#[rstest]
#[tokio::test]
async fn test_get_instruments_linear() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let params = BitgetInstrumentsInfoParamsBuilder::default()
        .category(BitgetProductType::Linear)
        .build()
        .unwrap();

    let response = client.get_instruments_linear(&params).await.unwrap();
    assert!(!response.result.list.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_get_instruments_spot() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let params = BitgetInstrumentsInfoParamsBuilder::default()
        .category(BitgetProductType::Spot)
        .build()
        .unwrap();

    let response = client.get_instruments_spot(&params).await.unwrap();
    assert!(!response.result.list.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_get_instruments_inverse() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let params = BitgetInstrumentsInfoParamsBuilder::default()
        .category(BitgetProductType::Inverse)
        .build()
        .unwrap();

    let response = client.get_instruments_inverse(&params).await.unwrap();
    assert!(!response.result.list.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_get_instruments_option() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let params = BitgetInstrumentsInfoParamsBuilder::default()
        .category(BitgetProductType::Option)
        .build()
        .unwrap();

    // Bitget lists no options: the venue rejects the OPTION category
    let error = client.get_instruments_option(&params).await.unwrap_err();
    assert!(
        error.to_string().contains("400172"),
        "unexpected error: {error}"
    );
}

#[rstest]
#[tokio::test]
async fn test_place_order() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let order_request = serde_json::json!({
        "category": "USDT-FUTURES",
        "symbol": "TSLAUSDT",
        "side": "buy",
        "orderType": "limit",
        "qty": "0.02",
        "price": "340",
        "timeInForce": "post_only",
        "clientOid": "test-order-123"
    });

    let response = client.place_order(&order_request).await.unwrap();
    assert_eq!(response.ret_code, 0);
    assert_eq!(
        response.result.order_id.as_deref(),
        Some("1503520305156874240")
    );
}

#[rstest]
#[tokio::test]
async fn test_authenticated_endpoint_requires_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    // Create client without credentials
    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    // Should fail when trying to call authenticated endpoint without credentials
    let result = client
        .get_open_orders(
            BitgetProductType::Linear,
            Some("TSLAUSDT".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
    result.unwrap_err();
}

#[rstest]
#[tokio::test]
async fn test_rate_limiting_returns_error() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::with_credentials(
        "test_api_key".to_string(),
        "test_api_secret".to_string(),
        "test_api_passphrase".to_string(),
        Some(base_url),
        60,
        0,
        1,
        1,
        5_000,
        None,
    )
    .unwrap();

    // Make multiple requests to trigger rate limit (mock server limits after 5)
    let mut last_error = None;

    for _ in 0..10 {
        match client
            .get_open_orders(
                BitgetProductType::Linear,
                Some("TSLAUSDT".to_owned()),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                last_error = Some(e);
                break;
            }
        }
    }

    // Verify rate limit was triggered
    assert!(last_error.is_some());
    let error = last_error.unwrap();
    assert!(error.to_string().contains("429") || error.to_string().contains("Too Many"));
}

/// Rejects the first two requests with a retryable error, then serves the order history.
///
/// Bitget answers an exceeded quota with HTTP 429 and an IP-wide penalty, which the client
/// honours as a cooldown rather than retrying; `40015` is the code Bitget asks to retry.
#[allow(dead_code)]
async fn handle_get_orders_realtime_retry(State(state): State<TestServerState>) -> Response {
    let mut count = state.request_count.lock().await;
    *count += 1;

    if *count <= 2 {
        return bitget_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "40015",
            "System is abnormal, please try again later",
        );
    }

    let mut orders = load_test_data("http_get_orders_history.json");
    orders["data"]["list"].as_array_mut().unwrap().truncate(1);
    orders["data"]["cursor"] = Value::Null;
    Json(orders).into_response()
}

#[allow(dead_code)]
fn create_retry_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route(
            "/api/v3/trade/unfilled-orders",
            get(handle_get_orders_realtime_retry),
        )
        .with_state(state)
}

#[allow(dead_code)]
async fn start_retry_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = TestServerState::default();
    let router = create_retry_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    wait_for_server(addr, "/api/v3/market/time").await;
    Ok((addr, state))
}

#[rstest]
#[tokio::test]
async fn test_retryable_error_retries_then_succeeds() {
    let (addr, state) = start_retry_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    // The 1ms delay bounds cap the jittered backoff, so both retries land immediately
    let client = BitgetHttpClient::with_credentials(
        "test_api_key".to_string(),
        "test_api_secret".to_string(),
        "test_api_passphrase".to_string(),
        Some(base_url),
        60,
        2,
        1,
        1,
        5_000,
        None,
    )
    .unwrap();

    let response = client
        .get_open_orders(
            BitgetProductType::Linear,
            Some("TSLAUSDT".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let request_count = *state.request_count.lock().await;

    assert_eq!(request_count, 3);
    assert_eq!(response.ret_code, 0);
    assert_eq!(response.ret_msg, "success");
    assert_eq!(response.result.list.len(), 1);
    assert_eq!(response.result.list[0].order_id, "1503520820376788992");
    assert_eq!(response.result.list[0].order_link_id, "tdcapflp93218");
}

#[rstest]
#[tokio::test]
async fn test_rate_limited_requests_use_fresh_auth_timestamps() {
    const RECV_WINDOW_MS: u64 = 250;
    const REQUEST_COUNT: usize = 51;

    let (addr, _state) = start_test_server().await.unwrap();
    let client = BitgetHttpClient::with_credentials(
        "test_api_key".to_string(),
        "test_api_secret".to_string(),
        "test_api_passphrase".to_string(),
        Some(format!("http://{addr}")),
        60,
        0,
        1,
        1,
        RECV_WINDOW_MS,
        None,
    )
    .unwrap();

    let requests = (0..REQUEST_COUNT).map(|_| {
        let client = client.clone();
        async move { client.get_account_info().await }
    });
    let results = futures_util::future::join_all(requests).await;

    assert_eq!(results.len(), REQUEST_COUNT);
    for result in results {
        assert!(result.is_ok(), "request failed: {result:?}");
    }
}

#[rstest]
#[tokio::test]
async fn test_get_open_orders_with_symbol() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let response = client
        .get_open_orders(
            BitgetProductType::Linear,
            Some("TSLAUSDT".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(response.ret_code, 0);
    assert!(response.result.list.is_empty() || !response.result.list.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_get_open_orders_without_symbol() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let response = client
        .get_open_orders(
            BitgetProductType::Linear,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(response.ret_code, 0);
}

#[rstest]
#[tokio::test]
async fn test_get_wallet_balance_requires_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    // Create client without credentials
    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let params = BitgetWalletBalanceParams {
        account_type: BitgetAccountType::Unified,
        coin: None,
    };

    // Should fail when trying to call authenticated endpoint without credentials
    let result = client.get_wallet_balance(&params).await;
    result.unwrap_err();
}

#[rstest]
#[tokio::test]
async fn test_get_wallet_balance_with_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let params = BitgetWalletBalanceParams {
        account_type: BitgetAccountType::Unified,
        coin: None,
    };

    let response = client.get_wallet_balance(&params).await.unwrap();

    // `/api/v3/account/assets` returns one unified account object
    assert_eq!(response.ret_code, 0);
    assert!(!response.result.coin.is_empty());
    assert_eq!(response.result.account_type, BitgetAccountType::Unified);
}

#[rstest]
#[tokio::test]
async fn test_get_positions_requires_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let params = BitgetPositionListParamsBuilder::default()
        .category(BitgetProductType::Linear)
        .build()
        .unwrap();

    let result = client.get_positions(&params).await;
    result.unwrap_err();
}

#[rstest]
#[tokio::test]
async fn test_get_positions_with_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let params = BitgetPositionListParamsBuilder::default()
        .category(BitgetProductType::Linear)
        .build()
        .unwrap();

    let response = client.get_positions(&params).await.unwrap();

    assert_eq!(response.ret_code, 0);
}

#[rstest]
#[tokio::test]
async fn test_get_fee_rate_requires_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let params = BitgetFeeRateParams {
        category: BitgetProductType::Linear,
        symbol: Some("TSLAUSDT".to_string()),
        base_coin: None,
    };

    let result = client.get_fee_rate(&params).await;
    result.unwrap_err();
}

#[rstest]
#[tokio::test]
async fn test_get_fee_rate_with_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let params = BitgetFeeRateParams {
        category: BitgetProductType::Linear,
        symbol: Some("TSLAUSDT".to_string()),
        base_coin: None,
    };

    let response = client.get_fee_rate(&params).await.unwrap();

    assert_eq!(response.ret_code, 0);
    assert!(!response.result.list.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_get_account_info_requires_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let result = client.get_account_info().await;
    result.unwrap_err();
}

#[rstest]
#[tokio::test]
async fn test_get_account_info_with_credentials() {
    use nautilus_bitget::http::models::BitgetAccountInfoResponse;

    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let response: BitgetAccountInfoResponse = client.get_account_info().await.unwrap();

    // Demo account settings: `accountLevel: basic`, `accountMode: hybrid`, `deltaSwitch: no`
    assert_eq!(response.ret_code, 0);
    assert_eq!(response.result.margin_mode, BitgetMarginMode::RegularMargin);
    assert_eq!(
        response.result.unified_margin_status,
        BitgetUnifiedMarginStatus::Hybrid
    );
    assert!(!response.result.is_master_trader);
    assert!(!response.result.spot_hedging_status);
}

#[rstest]
#[tokio::test]
async fn test_batch_cancel_linear_respects_venue_limit() {
    let (addr, state) = start_test_server().await.unwrap();
    let client = credentialed_client(format!("http://{addr}"));
    let instrument_ids = (0..20)
        .map(|_| InstrumentId::new(Symbol::from("TSLAUSDT-PERP"), *BITGET_VENUE))
        .collect::<Vec<_>>();
    let client_order_ids = (0..20)
        .map(|index| Some(ClientOrderId::from(format!("linear-cancel-{index}"))))
        .collect::<Vec<_>>();

    let reports = client
        .batch_cancel_orders(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            instrument_ids,
            client_order_ids,
            vec![None; 20],
        )
        .await
        .unwrap();

    // `cancel-batch` takes a bare array of up to 20 entries, each carrying `category`
    let requests = state.batch_cancel_requests.lock().await;
    assert!(reports.is_empty());
    assert_eq!(requests.len(), 1);
    let entries = requests[0].as_array().unwrap();
    assert_eq!(entries.len(), 20);
    assert!(
        entries
            .iter()
            .all(|entry| entry["category"] == "USDT-FUTURES" && entry["symbol"] == "TSLAUSDT")
    );
    assert_eq!(
        entries
            .iter()
            .map(|order| order["clientOid"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        (0..20)
            .map(|index| format!("linear-cancel-{index}"))
            .collect::<Vec<_>>()
    );
    drop(requests);

    let error = client
        .batch_cancel_orders(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            (0..21)
                .map(|_| InstrumentId::new(Symbol::from("TSLAUSDT-PERP"), *BITGET_VENUE))
                .collect(),
            (0..21)
                .map(|index| Some(ClientOrderId::from(format!("linear-overflow-{index}"))))
                .collect(),
            vec![None; 21],
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Batch cancel limit is 20 orders for linear"
    );
    assert_eq!(state.batch_cancel_requests.lock().await.len(), 1);
}

// Create router with separate handlers for reconciliation testing
#[allow(dead_code)]
fn create_reconciliation_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route("/api/v3/account/all-fee-rate", get(handle_get_fee_rate))
        .route(
            "/api/v3/trade/unfilled-orders",
            get(handle_get_orders_realtime),
        )
        .route(
            "/api/v3/trade/history-orders",
            get(handle_get_orders_history_reconciliation),
        )
        .route(
            "/api/v3/trade/unfilled-strategy-orders",
            get(handle_get_strategy_orders),
        )
        .route(
            "/api/v3/trade/history-strategy-orders",
            get(handle_get_strategy_orders),
        )
        .with_state(state)
}

#[allow(dead_code)]
async fn start_reconciliation_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = TestServerState::default();
    let router = create_reconciliation_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    wait_for_server(addr, "/api/v3/market/time").await;
    Ok((addr, state))
}

async fn reconciliation_client(addr: SocketAddr) -> BitgetHttpClient {
    let client = credentialed_client(format!("http://{addr}"));

    // Required for parsing order reports
    let instruments = client
        .request_instruments(BitgetProductType::Linear, None, None)
        .await
        .unwrap();

    for instrument in instruments {
        client.cache_instrument(instrument);
    }

    client
}

/// Returns the endpoints queried, in request order.
async fn queried_endpoints(state: &TestServerState) -> Vec<String> {
    state
        .endpoint_queries
        .lock()
        .await
        .iter()
        .map(|(endpoint, _)| endpoint.clone())
        .collect()
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_calls_both_endpoints() {
    let (addr, _state) = start_reconciliation_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");

    // Should call BOTH unfilled-orders and history-orders
    // Use a limit to restrict to 3 orders for this test
    let reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            None,    // No specific instrument
            false,   // open_only=false triggers dual endpoint call
            None,    // start
            None,    // end
            Some(3), // Limit to 3 to keep test focused on deduplication logic
        )
        .await
        .unwrap();

    // Should get 3 orders:
    // - 2 from unfilled-orders (open-order-1, open-order-2)
    // - 1 more from history due to the limit
    // - open-order-1 appears in both unfilled and history but should be deduplicated
    let order_ids: Vec<String> = reports
        .iter()
        .map(|r| r.venue_order_id.to_string())
        .collect();

    assert_eq!(
        reports.len(),
        3,
        "Should have 3 orders total (respecting limit)"
    );
    assert!(order_ids.contains(&"open-order-1".to_string()));
    assert!(order_ids.contains(&"open-order-2".to_string()));
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_linear_needs_no_settle_coin() {
    let (addr, state) = start_reconciliation_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");

    // Bitget lists open orders per category, so no settle coin is needed without a symbol
    let result = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            None, // No symbol
            true, // open_only=true
            None, // start
            None, // end
            None, // limit
        )
        .await;

    assert!(result.is_ok(), "Should succeed without a settle coin");

    let queries = state.endpoint_queries.lock().await;
    assert!(!queries.is_empty());
    for (endpoint, query) in queries.iter() {
        assert_eq!(
            query.get("category").map(String::as_str),
            Some("USDT-FUTURES"),
            "{endpoint}: {query:?}"
        );
        assert!(!query.contains_key("settleCoin"), "{endpoint}: {query:?}");
    }
}

#[rstest]
#[tokio::test]
async fn test_order_deduplication_by_order_id() {
    let (addr, _state) = start_reconciliation_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");

    // Test deduplication by querying both realtime and history for a specific instrument
    let instrument_id = InstrumentId::new(Symbol::from("TSLAUSDT-PERP"), *BITGET_VENUE);

    let reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            Some(instrument_id),
            false, // This will query both unfilled and history endpoints
            None,  // start
            None,  // end
            None,  // limit
        )
        .await
        .unwrap();

    // Count occurrences of open-order-1
    // It should appear once despite being in both unfilled and history responses
    let open_order_1_count = reports
        .iter()
        .filter(|r| r.venue_order_id.to_string() == "open-order-1")
        .count();

    assert_eq!(
        open_order_1_count, 1,
        "open-order-1 should appear exactly once (deduplicated across unfilled/history)"
    );
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_linear_queries_each_order_endpoint() {
    let (addr, state) = start_reconciliation_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");

    let _reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            None,
            true,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Open pass: regular then conditional orders; closed pass: the same from history.
    // Conditional (strategy) orders live on their own Bitget endpoints.
    assert_eq!(
        queried_endpoints(&state).await,
        vec![
            "realtime",
            "realtime",
            "/api/v3/trade/unfilled-strategy-orders",
            "/api/v3/trade/unfilled-strategy-orders",
            "history",
            "/api/v3/trade/history-strategy-orders",
        ],
        "Should query each order endpoint, following each open-list cursor once"
    );
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_respects_limit_across_endpoints() {
    let (addr, state) = start_reconciliation_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");

    // unfilled-orders returns 2 orders, the strategy endpoint 1, history 3
    let reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            None,
            true,
            None,
            None,
            Some(3),
        )
        .await
        .unwrap();

    assert!(
        reports.len() <= 3,
        "Should return at most 3 reports, was {}",
        reports.len()
    );

    // Both the regular and the conditional order lists should be queried
    let endpoints = queried_endpoints(&state).await;
    assert!(
        endpoints.len() >= 2,
        "Should query more than one endpoint, was {endpoints:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_stops_before_next_endpoint() {
    let (addr, state) = start_reconciliation_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");

    // unfilled-orders returns 2 orders, limit=1 should stop after the first request
    let reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            None,
            true,
            None,
            None,
            Some(1),
        )
        .await
        .unwrap();

    assert_eq!(reports.len(), 1, "Should return exactly 1 report");

    // Early termination: the other order endpoints should be skipped
    assert_eq!(
        queried_endpoints(&state).await,
        vec!["realtime"],
        "Should only query unfilled-orders when limit reached"
    );
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_combines_orders_from_each_endpoint() {
    let (addr, state) = start_reconciliation_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");

    let reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            None,
            true,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(queried_endpoints(&state).await.len(), 6);

    let order_ids: Vec<String> = reports
        .iter()
        .map(|r| r.venue_order_id.to_string())
        .collect();

    // unfilled-orders: open-order-1, open-order-2; unfilled-strategy-orders: the trigger
    // order; history-orders: the filled and the partially filled IOC (open-order-1 again is
    // deduplicated); history-strategy-orders: the trigger order again (deduplicated)
    assert_eq!(
        order_ids,
        vec![
            "open-order-1",
            "open-order-2",
            "1503521064568303616",
            "1503520820376788992",
            "1503520806866935808",
        ],
        "Should combine the orders of each endpoint, was {order_ids:?}"
    );
}

#[allow(dead_code)]
async fn handle_get_orders_realtime_open_only(
    query: Query<HashMap<String, String>>,
    State(state): State<TestServerState>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    {
        let mut count = state.realtime_requests.lock().await;
        *count += 1;
    }
    state
        .endpoint_queries
        .lock()
        .await
        .push((uri.path().to_string(), query.0.clone()));

    if uri.path().contains("strategy") || has_cursor(&query) {
        return Json(empty_page()).into_response();
    }

    let mut list = open_orders();
    list.truncate(1);

    // Bitget serves closed orders from history-orders, not from unfilled-orders
    if uri.path() == "/api/v3/trade/history-orders" {
        // Keep the open order so the two passes overlap and exercise dedup
        let mut cancelled = list[0].clone();
        let fields = cancelled
            .as_object_mut()
            .expect("Fixture order should be an object");
        fields.insert("orderId".to_string(), json!("cancelled-order-1"));
        fields.insert("clientOid".to_string(), json!("client-cancelled-1"));
        fields.insert("orderStatus".to_string(), json!("cancelled"));
        fields.insert("cancelReason".to_string(), json!("normal_cancel"));
        list.push(cancelled);
        return Json(bitget_ok(
            &json!({ "list": list, "cursor": "closed-page-2" }),
        ))
        .into_response();
    }

    Json(bitget_ok(&json!({ "list": list, "cursor": null }))).into_response()
}

#[allow(dead_code)]
fn create_open_only_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route("/api/v3/account/all-fee-rate", get(handle_get_fee_rate))
        .route(
            "/api/v3/trade/unfilled-orders",
            get(handle_get_orders_realtime_open_only),
        )
        .route(
            "/api/v3/trade/history-orders",
            get(handle_get_orders_realtime_open_only),
        )
        .route(
            "/api/v3/trade/unfilled-strategy-orders",
            get(handle_get_orders_realtime_open_only),
        )
        .route(
            "/api/v3/trade/history-strategy-orders",
            get(handle_get_orders_realtime_open_only),
        )
        .with_state(state)
}

#[allow(dead_code)]
async fn start_open_only_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = TestServerState::default();
    let router = create_open_only_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    wait_for_server(addr, "/api/v3/market/time").await;
    Ok((addr, state))
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_open_only_includes_recently_closed() {
    let (addr, state) = start_open_only_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("TSLAUSDT-PERP"), *BITGET_VENUE);

    let reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            Some(instrument_id),
            true,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let realtime_requests = *state.realtime_requests.lock().await;
    assert_eq!(
        realtime_requests, 4,
        "Should query the open and closed lists for both regular and conditional orders"
    );

    let cancelled = reports
        .iter()
        .find(|r| r.venue_order_id.as_str() == "cancelled-order-1")
        .expect("Terminal report for the closed order should be returned");
    assert_eq!(cancelled.order_status, OrderStatus::Canceled);

    let open_count = reports
        .iter()
        .filter(|r| r.venue_order_id.as_str() == "open-order-1")
        .count();
    assert_eq!(
        open_count, 1,
        "Open order returned by both passes should be deduplicated"
    );
    assert_eq!(
        reports.len(),
        2,
        "Should have the open and the closed order"
    );
}

#[rstest]
#[tokio::test]
async fn test_repay_spot_borrow_with_amount() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let amount = Quantity::new_checked(0.5, 8).unwrap();
    let response = client.repay_spot_borrow("ETH", Some(amount)).await.unwrap();

    assert_eq!(response.ret_code, 0);
    assert_eq!(response.ret_msg, "success");
    assert_eq!(response.result.result_status, BitgetRepayStatus::Success);
}

#[rstest]
#[tokio::test]
async fn test_repay_spot_borrow_without_amount() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    // Test repaying all outstanding borrows by passing None for amount
    let response = client.repay_spot_borrow("ETH", None).await.unwrap();

    assert_eq!(response.ret_code, 0);
    assert_eq!(response.ret_msg, "success");
    assert_eq!(response.result.result_status, BitgetRepayStatus::Success);
}

#[rstest]
#[tokio::test]
async fn test_repay_spot_borrow_requires_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let amount = Quantity::new_checked(0.5, 8).unwrap();
    let result = client.repay_spot_borrow("ETH", Some(amount)).await;
    assert!(result.is_err(), "Should fail without credentials");
}

#[rstest]
#[tokio::test]
async fn test_repay_spot_borrow_with_conversion_with_amount() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let amount = Quantity::new_checked(0.5, 8).unwrap();
    let response = client
        .repay_spot_borrow_with_conversion("ETH", Some(amount))
        .await
        .unwrap();

    assert_eq!(response.ret_code, 0);
    assert_eq!(response.ret_msg, "success");
    assert_eq!(response.result.result_status, BitgetRepayStatus::Success);
}

#[rstest]
#[tokio::test]
async fn test_repay_spot_borrow_with_conversion_without_amount() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    // Test repaying all outstanding borrows by passing None for amount
    let response = client
        .repay_spot_borrow_with_conversion("ETH", None)
        .await
        .unwrap();

    assert_eq!(response.ret_code, 0);
    assert_eq!(response.ret_msg, "success");
    assert_eq!(response.result.result_status, BitgetRepayStatus::Success);
}

#[rstest]
#[tokio::test]
async fn test_repay_spot_borrow_with_conversion_requires_credentials() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let amount = Quantity::new_checked(0.5, 8).unwrap();
    let result = client
        .repay_spot_borrow_with_conversion("ETH", Some(amount))
        .await;
    assert!(result.is_err(), "Should fail without credentials");
}

#[rstest]
#[case::without_conversion(false)]
#[case::with_conversion(true)]
#[tokio::test]
async fn test_repay_spot_borrow_rejects_failed_result_status(#[case] with_conversion: bool) {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");
    let client = credentialed_client(base_url);
    let amount = Some(Quantity::new_checked(0.5, 8).unwrap());

    let result = if with_conversion {
        client
            .repay_spot_borrow_with_conversion("FAIL", amount)
            .await
            .map(|_| ())
    } else {
        client.repay_spot_borrow("FAIL", amount).await.map(|_| ())
    };

    // Bitget reports a failed repay as a business error code, not a result status
    let error = result.expect_err("failed repay should return an error");
    assert!(
        error.to_string().contains("25202"),
        "Unexpected error: {error}"
    );
}

#[rstest]
#[tokio::test]
async fn test_get_spot_borrow_amount_returns_zero_when_no_borrow() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let borrow_amount = client.get_spot_borrow_amount("USDT").await.unwrap();

    // USDT has zero debt in the test data
    assert_eq!(borrow_amount, rust_decimal::Decimal::ZERO);
}

#[rstest]
#[tokio::test]
async fn test_get_spot_borrow_amount_returns_zero_when_coin_not_found() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    let borrow_amount = client.get_spot_borrow_amount("UNKNOWN").await.unwrap();

    // Should return zero when coin not found
    assert_eq!(borrow_amount, rust_decimal::Decimal::ZERO);
}

#[rstest]
#[tokio::test]
async fn test_spot_position_report_short_from_borrowed_balance() {
    let (addr, _state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    client.set_use_spot_position_reports(true);

    let eth = Currency::from("ETH");
    let usdt = Currency::from("USDT");
    let ethusdt = CurrencyPair::builder()
        .instrument_id("ETHUSDT-SPOT.BITGET".into())
        .raw_symbol("ETHUSDT".into())
        .base_currency(eth)
        .quote_currency(usdt)
        .price_precision(2)
        .size_precision(5)
        .price_increment(Price::from("0.01"))
        .size_increment(Quantity::from("0.00001"))
        .ts_event(0.into())
        .ts_init(0.into())
        .build()
        .unwrap();
    client.cache_instrument(InstrumentAny::CurrencyPair(ethusdt));

    let account_id = AccountId::new("BITGET-UNIFIED");
    let reports = client
        .request_position_status_reports(
            account_id,
            BitgetProductType::Spot,
            Some(InstrumentId::from("ETHUSDT-SPOT.BITGET")),
        )
        .await
        .unwrap();

    let eth_report = reports
        .iter()
        .find(|r| r.instrument_id.symbol.as_str() == "ETHUSDT-SPOT")
        .expect("ETH SPOT position report not found");

    assert_eq!(eth_report.position_side, PositionSide::Short);
    assert_eq!(eth_report.quantity, Quantity::new(0.06142, 5));
}

#[rstest]
#[tokio::test]
async fn test_unscoped_spot_position_reports_fail_without_wallet_request() {
    let (addr, state) = start_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = credentialed_client(base_url);

    client.set_use_spot_position_reports(true);

    let eth = Currency::from("ETH");

    for (symbol, quote) in [("ETHUSDT", "USDT"), ("ETHUSDC", "USDC")] {
        let instrument = CurrencyPair::builder()
            .instrument_id(format!("{symbol}-SPOT.BITGET").into())
            .raw_symbol(symbol.into())
            .base_currency(eth)
            .quote_currency(Currency::from(quote))
            .price_precision(2)
            .size_precision(5)
            .price_increment(Price::from("0.01"))
            .size_increment(Quantity::from("0.00001"))
            .ts_event(0.into())
            .ts_init(0.into())
            .build()
            .unwrap();
        client.cache_instrument(InstrumentAny::CurrencyPair(instrument));
    }

    let error = client
        .request_position_status_reports(
            AccountId::new("BITGET-UNIFIED"),
            BitgetProductType::Spot,
            None,
        )
        .await
        .expect_err("unscoped SPOT reports cannot be attributed to a pair");

    assert!(error.to_string().contains("pair identity"));
    assert_eq!(*state.wallet_balance_requests.lock().await, 0);
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_with_time_filtering() {
    let (addr, state) = start_reconciliation_test_server().await.unwrap();
    let client = reconciliation_client(addr).await;
    let account_id = AccountId::from("BITGET-UNIFIED");
    let start_time = Timestamp::now() - jiff::SignedDuration::from_hours(24 * (7));
    let end_time = Timestamp::now();

    let _reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            None,
            false, // Query history
            Some(start_time),
            Some(end_time),
            Some(10),
        )
        .await
        .unwrap();

    // The history endpoint should have been called with startTime and endTime
    let queries = state.endpoint_queries.lock().await;
    let history: Vec<&HashMap<String, String>> = queries
        .iter()
        .filter(|(endpoint, _)| endpoint == "history")
        .map(|(_, query)| query)
        .collect();

    assert!(!history.is_empty(), "Should have called history endpoint");
    for query in history {
        assert_eq!(
            query.get("startTime"),
            Some(&start_time.as_millisecond().to_string())
        );
        assert_eq!(
            query.get("endTime"),
            Some(&end_time.as_millisecond().to_string())
        );
    }
}

/// Handler that returns only a partial bar on first page, then closed bars on second page.
async fn handle_get_klines_partial_first_page(
    query: Query<HashMap<String, String>>,
    State(state): State<TestServerState>,
) -> impl IntoResponse {
    if !query.contains_key("category") || !query.contains_key("symbol") {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "400172",
            "Parameter verification failed",
        );
    }

    let mut count = state.request_count.lock().await;
    *count += 1;
    let page = *count;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let bar_duration_ms = 60_000i64;
    let partial_bar_start = (now_ms / bar_duration_ms) * bar_duration_ms;

    // `/api/v3/market/candles` rows: [ts, open, high, low, close, baseVolume, quoteTurnover]
    if page == 1 {
        Json(bitget_ok(&json!([[
            partial_bar_start.to_string(),
            "100000",
            "100100",
            "99900",
            "100050",
            "1000",
            "100000000"
        ]])))
        .into_response()
    } else {
        let closed_bar_2_start = partial_bar_start - 2 * bar_duration_ms;
        let closed_bar_1_start = partial_bar_start - 3 * bar_duration_ms;

        Json(bitget_ok(&json!([
            [
                closed_bar_1_start.to_string(),
                "99700",
                "99800",
                "99600",
                "99750",
                "500",
                "50000000"
            ],
            [
                closed_bar_2_start.to_string(),
                "99800",
                "99900",
                "99700",
                "99850",
                "600",
                "60000000"
            ]
        ])))
        .into_response()
    }
}

fn create_partial_first_page_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route(
            "/api/v3/market/candles",
            get(handle_get_klines_partial_first_page),
        )
        .with_state(state)
}

async fn start_partial_first_page_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = TestServerState::default();
    let router = create_partial_first_page_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    wait_for_server(addr, "/api/v3/market/time").await;

    Ok((addr, state))
}

/// Tests that pagination continues when first page only has partial bars (P1 bug fix).
#[rstest]
#[tokio::test]
async fn test_request_bars_continues_pagination_when_first_page_only_partial() {
    let (addr, state) = start_partial_first_page_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();

    let instruments = client
        .request_instruments(BitgetProductType::Linear, None, None)
        .await
        .unwrap();

    for instrument in instruments {
        client.cache_instrument(instrument);
    }

    let bar_type = BarType::from("BTCUSDT-PERP.BITGET-1-MINUTE-LAST-EXTERNAL");
    let bars = client
        .request_bars(BitgetProductType::Linear, bar_type, None, None, None, true)
        .await
        .unwrap();

    assert_eq!(
        bars.len(),
        2,
        "Should continue pagination and return closed bars from second page"
    );
    let request_count = *state.request_count.lock().await;
    assert!(
        request_count >= 2,
        "Should have made at least 2 requests to paginate past partial bars"
    );
}

#[allow(dead_code)]
fn create_order_capture_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route(
            "/api/v3/trade/place-order",
            post(handle_post_order_with_capture),
        )
        .route(
            "/api/v3/trade/place-strategy-order",
            post(handle_post_order_with_capture),
        )
        .route("/api/v3/trade/order-info", get(handle_get_order_info))
        .route(
            "/api/v3/trade/unfilled-strategy-orders",
            get(handle_get_strategy_orders),
        )
        .route("/api/v3/account/all-fee-rate", get(handle_get_fee_rate))
        .with_state(state)
}

async fn start_order_capture_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = TestServerState::default();
    let router = create_order_capture_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    wait_for_server(addr, "/api/v3/market/time").await;

    Ok((addr, state))
}

async fn order_capture_client(
    addr: SocketAddr,
    product_type: BitgetProductType,
) -> BitgetHttpClient {
    let client = credentialed_client(format!("http://{addr}"));

    let instruments = client
        .request_instruments(product_type, None, None)
        .await
        .unwrap();

    for instrument in instruments {
        client.cache_instrument(instrument);
    }

    client
}

const PLACE_ORDER_PATH: &str = "/api/v3/trade/place-order";
const PLACE_STRATEGY_ORDER_PATH: &str = "/api/v3/trade/place-strategy-order";

#[rstest]
#[tokio::test]
async fn test_submit_order_stop_market_with_trigger_price() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("stop-market-test-1");
    let quantity = Quantity::new(0.001, 3);
    let trigger_price = Price::new(100_000.0, 2);

    let result = client
        .submit_order(
            account_id,
            BitgetProductType::Linear,
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            OrderType::StopMarket,
            quantity,
            None, // time_in_force
            None, // price (not used for market orders)
            Some(trigger_price),
            None,  // post_only
            false, // reduce_only
            false, // is_quote_quantity
            false, // is_leverage
            None,  // position_idx
            None,  // bbo_side_type
            None,  // bbo_level
            None,  // smp_type
            None,  // native_tp_sl
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1, "Should have captured one order");

    // Bitget places conditional orders as `trigger` strategy orders
    let order = &orders[0];
    assert_eq!(order.path, PLACE_STRATEGY_ORDER_PATH);
    assert_eq!(order.category, "USDT-FUTURES");
    assert_eq!(order.symbol, "BTCUSDT");
    assert_eq!(order.side, "buy");
    assert_eq!(order.raw_body["type"], "trigger");
    assert_eq!(order.trigger_order_type.as_deref(), Some("market"));
    assert_eq!(
        order.trigger_price.as_deref(),
        Some("100000.00"),
        "Should have trigger price"
    );

    // The venue derives the trigger direction from the trigger price
    assert!(order.raw_body.get("triggerDirection").is_none());

    // Strategy orders carry no timeInForce
    assert!(
        order.time_in_force.is_none(),
        "Market orders should not have timeInForce"
    );
}

#[rstest]
#[tokio::test]
async fn test_submit_order_stop_limit_with_trigger_price_and_limit_price() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("stop-limit-test-1");
    let quantity = Quantity::new(0.001, 3);
    let trigger_price = Price::new(99_000.0, 2);
    let limit_price = Price::new(98_500.0, 2);

    let result = client
        .submit_order(
            account_id,
            BitgetProductType::Linear,
            instrument_id,
            client_order_id,
            OrderSide::Sell,
            OrderType::StopLimit,
            quantity,
            Some(TimeInForce::Gtc),
            Some(limit_price),
            Some(trigger_price),
            None,  // post_only
            true,  // reduce_only
            false, // is_quote_quantity
            false, // is_leverage
            None,  // position_idx
            None,  // bbo_side_type
            None,  // bbo_level
            None,  // smp_type
            None,  // native_tp_sl
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1, "Should have captured one order");

    let order = &orders[0];
    assert_eq!(order.path, PLACE_STRATEGY_ORDER_PATH);
    assert_eq!(order.category, "USDT-FUTURES");
    assert_eq!(order.symbol, "BTCUSDT");
    assert_eq!(order.side, "sell");
    assert_eq!(order.trigger_order_type.as_deref(), Some("limit"));
    assert_eq!(
        order.trigger_order_price.as_deref(),
        Some("98500.00"),
        "Should have limit price"
    );
    assert_eq!(
        order.trigger_price.as_deref(),
        Some("99000.00"),
        "Should have trigger price"
    );
    assert!(order.raw_body.get("triggerDirection").is_none());
    assert_eq!(order.reduce_only.as_deref(), Some("YES"));
}

#[rstest]
#[tokio::test]
async fn test_submit_order_market_if_touched_trigger_direction() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("mit-test-1");
    let quantity = Quantity::new(0.001, 3);
    let trigger_price = Price::new(95_000.0, 2);

    // MarketIfTouched Buy triggers on FALL (opposite of stop)
    let result = client
        .submit_order(
            account_id,
            BitgetProductType::Linear,
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            OrderType::MarketIfTouched,
            quantity,
            None,
            None,
            Some(trigger_price),
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);

    // Bitget takes no trigger direction: a trigger fires when the price crosses triggerPrice
    // from either side, so a Buy MIT is the same trigger order as a Buy Stop
    let order = &orders[0];
    assert_eq!(order.path, PLACE_STRATEGY_ORDER_PATH);
    assert_eq!(order.trigger_price.as_deref(), Some("95000.00"));
    assert_eq!(order.trigger_order_type.as_deref(), Some("market"));
    assert!(
        order.raw_body.get("triggerDirection").is_none(),
        "Bitget derives the direction"
    );
}

#[rstest]
#[tokio::test]
async fn test_submit_order_post_only() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("post-only-test-1");
    let quantity = Quantity::new(0.001, 3);
    let price = Price::new(100_000.0, 2);

    let result = client
        .submit_order(
            account_id,
            BitgetProductType::Linear,
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            OrderType::Limit,
            quantity,
            Some(TimeInForce::Gtc), // This will be overridden by post_only
            Some(price),
            None,
            Some(true), // post_only
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);

    let order = &orders[0];
    assert_eq!(order.path, PLACE_ORDER_PATH);
    assert_eq!(order.order_type, "limit");
    assert_eq!(
        order.time_in_force.as_deref(),
        Some("post_only"),
        "Post-only orders should have timeInForce=post_only"
    );
}

#[rstest]
#[tokio::test]
async fn test_submit_order_with_bbo_sends_bbo_and_omits_price() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let result = client
        .submit_order(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE),
            ClientOrderId::from("bbo-test-1"),
            OrderSide::Buy,
            OrderType::Limit,
            Quantity::new(0.001, 3),
            Some(TimeInForce::Gtc),
            Some(Price::new(50_000.0, 2)),
            None,
            None,
            false,
            false,
            false,
            None,
            Some(BitgetBboSideType::Queue),
            Some("1".to_string()),
            None,
            None,
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);

    // Bitget combines the BBO side type and level into one `bboLevel` value
    let order = &orders[0];
    assert_eq!(order.order_type, "limit");
    assert_eq!(order.price, None);
    assert!(order.raw_body.get("bboSideType").is_none());
    assert_eq!(order.bbo_level.as_deref(), Some("queue1"));
}

#[rstest]
#[case(Some(BitgetOrderSmpType::None), Some("none"))]
#[case(Some(BitgetOrderSmpType::CancelMaker), Some("cancel_maker"))]
#[case(Some(BitgetOrderSmpType::CancelTaker), Some("cancel_taker"))]
#[case(Some(BitgetOrderSmpType::CancelBoth), Some("cancel_both"))]
#[case(None, None)]
#[tokio::test]
async fn test_submit_order_serializes_smp_type(
    #[case] smp_type: Option<BitgetOrderSmpType>,
    #[case] expected: Option<&str>,
) {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let result = client
        .submit_order(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE),
            ClientOrderId::from("smp-test-1"),
            OrderSide::Buy,
            OrderType::Limit,
            Quantity::from("0.001"),
            Some(TimeInForce::Gtc),
            Some(Price::from("50000.00")),
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            smp_type,
            None,
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);

    let body = &orders[0].raw_body;

    // Bitget calls self-trade prevention `stpMode`
    assert_eq!(body.get("stpMode").and_then(|v| v.as_str()), expected);
}

#[rstest]
#[tokio::test]
async fn test_submit_order_with_native_tp_sl_serializes_fields() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let native_tp_sl = BitgetNativeTpSlParams {
        take_profit: Some("55000".to_string()),
        stop_loss: Some("47000".to_string()),
        tp_trigger_by: Some(BitgetTriggerType::LastPrice),
        sl_trigger_by: Some(BitgetTriggerType::MarkPrice),
        tp_order_type: Some(BitgetOrderType::Limit),
        sl_order_type: Some(BitgetOrderType::Market),
        tp_limit_price: Some("55100".to_string()),
        sl_limit_price: None,
        tpsl_mode: None,
        close_on_trigger: Some(true),
        order_iv: None,
        mmp: None,
    };

    let result = client
        .submit_order(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE),
            ClientOrderId::from("native-tpsl-test-1"),
            OrderSide::Buy,
            OrderType::Limit,
            Quantity::new(0.001, 3),
            Some(TimeInForce::Gtc),
            Some(Price::new(50_000.0, 2)),
            None,
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            Some(&native_tp_sl),
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);

    let body = &orders[0].raw_body;
    assert_eq!(
        body.get("takeProfit").and_then(|v| v.as_str()),
        Some("55000")
    );
    assert_eq!(body.get("stopLoss").and_then(|v| v.as_str()), Some("47000"));
    assert_eq!(
        body.get("tpTriggerBy").and_then(|v| v.as_str()),
        Some("market")
    );
    assert_eq!(
        body.get("slTriggerBy").and_then(|v| v.as_str()),
        Some("mark")
    );
    assert_eq!(
        body.get("tpOrderType").and_then(|v| v.as_str()),
        Some("limit")
    );
    assert_eq!(
        body.get("slOrderType").and_then(|v| v.as_str()),
        Some("market")
    );
    assert_eq!(
        body.get("tpLimitPrice").and_then(|v| v.as_str()),
        Some("55100")
    );
    // place-order has no closeOnTrigger or tpslMode
    assert!(body.get("closeOnTrigger").is_none());
    assert!(body.get("tpslMode").is_none());
}

#[rstest]
#[tokio::test]
async fn test_submit_order_spot_market_base_quantity() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Spot).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-SPOT"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("spot-base-qty-test-1");
    let quantity = Quantity::new(0.001, 3);

    let result = client
        .submit_order(
            account_id,
            BitgetProductType::Spot,
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            OrderType::Market,
            quantity,
            None,
            None,
            None,
            None,
            false,
            false, // is_quote_quantity=false -> base coin
            true,  // is_leverage
            None,  // position_idx
            None,  // bbo_side_type
            None,  // bbo_level
            None,  // smp_type
            None,  // native_tp_sl
        )
        .await;

    // Bitget sizes spot market buys in the quote coin only (no `marketUnit`)
    let error = result.expect_err("base-quantity spot market buy should be refused");
    assert!(
        error.to_string().contains("require a quote quantity"),
        "unexpected error: {error}"
    );
    assert!(state.order_submissions.lock().await.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_submit_order_spot_market_quote_quantity() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Spot).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-SPOT"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("spot-quote-qty-test-1");
    let quantity = Quantity::new(100.0, 2); // 100 USDT worth

    let result = client
        .submit_order(
            account_id,
            BitgetProductType::Spot,
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            OrderType::Market,
            quantity,
            None,
            None,
            None,
            None,
            false,
            true,  // is_quote_quantity=true -> quote coin
            false, // is_leverage
            None,  // position_idx
            None,  // bbo_side_type
            None,  // bbo_level
            None,  // smp_type
            None,  // native_tp_sl
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);

    let order = &orders[0];
    assert_eq!(order.category, "SPOT");
    assert_eq!(order.order_type, "market");
    assert_eq!(order.qty, "100.00");
    assert!(
        order.raw_body.get("marketUnit").is_none(),
        "Bitget has no marketUnit"
    );
    assert!(
        order.raw_body.get("isLeverage").is_none(),
        "Bitget has no isLeverage"
    );
}

#[rstest]
#[tokio::test]
async fn test_submit_order_linear_does_not_send_market_unit() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("linear-market-test-1");
    let quantity = Quantity::new(0.001, 3);

    let result = client
        .submit_order(
            account_id,
            BitgetProductType::Linear,
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            OrderType::Market,
            quantity,
            None,
            None,
            None,
            None,
            false,
            true,  // is_quote_quantity - should be ignored for LINEAR
            false, // is_leverage - only for SPOT
            None,  // position_idx
            None,  // bbo_side_type
            None,  // bbo_level
            None,  // smp_type
            None,  // native_tp_sl
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);

    let order = &orders[0];
    assert_eq!(order.category, "USDT-FUTURES");
    assert!(
        order.raw_body.get("marketUnit").is_none(),
        "LINEAR market orders should not have marketUnit"
    );
    assert!(
        order.raw_body.get("isLeverage").is_none(),
        "LINEAR orders should not have isLeverage"
    );
}

#[rstest]
#[tokio::test]
async fn test_submit_order_limit_if_touched_trigger_direction() {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("lit-test-1");
    let quantity = Quantity::new(0.001, 3);
    let trigger_price = Price::new(105_000.0, 2);
    let limit_price = Price::new(105_500.0, 2);

    // LimitIfTouched Sell triggers on RISE (opposite of stop)
    let result = client
        .submit_order(
            account_id,
            BitgetProductType::Linear,
            instrument_id,
            client_order_id,
            OrderSide::Sell,
            OrderType::LimitIfTouched,
            quantity,
            Some(TimeInForce::Gtc),
            Some(limit_price),
            Some(trigger_price),
            None,
            false,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);

    let order = &orders[0];
    assert_eq!(order.path, PLACE_STRATEGY_ORDER_PATH);
    assert_eq!(order.trigger_order_type.as_deref(), Some("limit"));
    assert_eq!(
        order.trigger_price.as_deref(),
        Some("105000.00"),
        "Should have trigger price"
    );
    assert_eq!(
        order.trigger_order_price.as_deref(),
        Some("105500.00"),
        "Should have limit price"
    );

    // Bitget takes no trigger direction (see the MIT test)
    assert!(
        order.raw_body.get("triggerDirection").is_none(),
        "Bitget derives the direction"
    );
}

#[rstest]
#[case::omitted(None, None)]
#[case::one_way(Some(BitgetPositionIdx::OneWay), None)]
#[case::buy_hedge(Some(BitgetPositionIdx::BuyHedge), Some("long"))]
#[case::sell_hedge(Some(BitgetPositionIdx::SellHedge), Some("short"))]
#[tokio::test]
async fn test_submit_order_serializes_position_idx(
    #[case] position_idx: Option<BitgetPositionIdx>,
    #[case] expected: Option<&str>,
) {
    let (addr, state) = start_order_capture_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let result = client
        .submit_order(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            InstrumentId::new(Symbol::from("BTCUSDT-PERP"), *BITGET_VENUE),
            ClientOrderId::from("posidx-test"),
            OrderSide::Buy,
            OrderType::Limit,
            Quantity::new(0.001, 3),
            Some(TimeInForce::Gtc),
            Some(Price::new(50_000.0, 2)),
            None,
            None,
            false,
            false,
            false,
            position_idx,
            None,
            None,
            None,
            None,
        )
        .await;

    assert!(result.is_ok(), "Order submission should succeed");

    // Bitget sends `posSide` in hedge mode only; one-way orders omit it
    let orders = state.order_submissions.lock().await;
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].pos_side.as_deref(), expected);
}

async fn handle_empty_orders(headers: axum::http::HeaderMap) -> Response {
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    Json(empty_page()).into_response()
}

/// `order-info` answers an unknown order with `25204 Order does not exist`.
async fn handle_unknown_order_info(headers: axum::http::HeaderMap) -> Response {
    if let Some(response) = require_bitget_auth(&headers) {
        return response;
    }

    bitget_error(StatusCode::BAD_REQUEST, "25204", "Order does not exist")
}

fn create_empty_orders_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route("/api/v3/account/all-fee-rate", get(handle_get_fee_rate))
        .route("/api/v3/trade/order-info", get(handle_unknown_order_info))
        .route("/api/v3/trade/unfilled-orders", get(handle_empty_orders))
        .route("/api/v3/trade/history-orders", get(handle_empty_orders))
        .route(
            "/api/v3/trade/unfilled-strategy-orders",
            get(handle_empty_orders),
        )
        .route(
            "/api/v3/trade/history-strategy-orders",
            get(handle_empty_orders),
        )
        .with_state(state)
}

async fn start_empty_orders_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = TestServerState::default();
    let router = create_empty_orders_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    wait_for_server(addr, "/api/v3/market/time").await;
    Ok((addr, state))
}

#[rstest]
#[tokio::test]
async fn test_query_order_not_found_returns_none() {
    let (addr, _state) = start_empty_orders_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("TSLAUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("query-test-1");

    let result = client
        .query_order(
            account_id,
            BitgetProductType::Linear,
            instrument_id,
            Some(client_order_id),
            None,
        )
        .await;

    assert!(
        result.is_ok(),
        "query_order should not error for an unknown order"
    );
    assert!(
        result.unwrap().is_none(),
        "query_order should return None when the order is not found"
    );
}

// Serves the trigger order on the conditional (strategy) list, nothing on the regular lists
#[allow(dead_code)]
fn create_tp_sl_test_router() -> Router {
    Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route("/api/v3/account/all-fee-rate", get(handle_get_fee_rate))
        .route("/api/v3/trade/unfilled-orders", get(handle_empty_orders))
        .route("/api/v3/trade/history-orders", get(handle_empty_orders))
        .route(
            "/api/v3/trade/unfilled-strategy-orders",
            get(handle_get_strategy_orders),
        )
        .route(
            "/api/v3/trade/history-strategy-orders",
            get(handle_empty_orders),
        )
        .with_state(TestServerState::default())
}

#[allow(dead_code)]
async fn start_tp_sl_test_server() -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = create_tp_sl_test_router();

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    wait_for_server(addr, "/api/v3/market/time").await;
    Ok(addr)
}

#[rstest]
#[tokio::test]
async fn test_request_order_status_reports_tp_sl_orders() {
    let addr = start_tp_sl_test_server().await.unwrap();
    let client = order_capture_client(addr, BitgetProductType::Linear).await;

    let account_id = AccountId::from("BITGET-UNIFIED");
    let instrument_id = InstrumentId::new(Symbol::from("TSLAUSDT-PERP"), *BITGET_VENUE);

    let reports = client
        .request_order_status_reports(
            account_id,
            BitgetProductType::Linear,
            Some(instrument_id),
            true,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // The demo trigger buy-market order, mark-price triggered at 396.89
    assert_eq!(reports.len(), 1, "Should have 1 conditional order");

    let report = &reports[0];
    assert_eq!(report.venue_order_id.as_str(), "1503521064568303616");
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(report.order_side, Some(OrderSide::Buy));
    assert_eq!(report.trigger_price, Some(Price::from("396.89")));
    assert_eq!(report.trigger_type, Some(TriggerType::MarkPrice));
    assert_eq!(report.order_status, OrderStatus::Canceled);
    assert!(!report.reduce_only);
}

// -------------------------------------------------------------------------------------------------
// Sub-account endpoints (/api/v3/user/*)
//
// Bitget's sub-account listing and API-key management need a master key and their schemas are
// unverified; the client keeps the Bybit-parity methods and returns an unsupported error
// without sending a request.
// -------------------------------------------------------------------------------------------------

fn user_test_client(base_url: String) -> BitgetHttpClient {
    credentialed_client(base_url)
}

fn user_raw_test_client(base_url: String) -> BitgetRawHttpClient {
    BitgetRawHttpClient::with_credentials(
        "test_api_key".to_string(),
        "test_api_secret".to_string(),
        "test_api_passphrase".to_string(),
        Some(base_url),
        60,
        3,
        1000,
        10_000,
        5_000,
        None,
    )
    .unwrap()
}

#[rstest]
#[case::sub_members("sub_members")]
#[case::sub_members_paged("sub_members_paged")]
#[case::escrow_sub_members("escrow_sub_members")]
#[case::sub_api_keys("sub_api_keys")]
#[case::update_sub_api_key("update_sub_api_key")]
#[case::update_master_api_key("update_master_api_key")]
#[case::fetch_all_sub_members_paged("fetch_all_sub_members_paged")]
#[case::fetch_all_escrow_sub_members("fetch_all_escrow_sub_members")]
#[case::fetch_all_sub_api_keys("fetch_all_sub_api_keys")]
#[tokio::test]
async fn test_sub_account_endpoints_are_unsupported(#[case] endpoint: &str) {
    use nautilus_bitget::http::query::{
        BitgetSubApiKeysParamsBuilder, BitgetSubMembersPageParamsBuilder,
        BitgetUpdateMasterApiParamsBuilder, BitgetUpdateSubApiParamsBuilder,
    };

    // Unreachable host: an unsupported endpoint must fail before any request is sent
    let client = user_test_client("http://127.0.0.1:9".to_string());
    let raw = user_raw_test_client("http://127.0.0.1:9".to_string());
    let page = BitgetSubMembersPageParamsBuilder::default()
        .page_size(50u32)
        .build()
        .unwrap();

    let error = match endpoint {
        "sub_members" => client.get_sub_members().await.map(|_| ()),
        "sub_members_paged" => client.get_sub_members_paged(&page).await.map(|_| ()),
        "escrow_sub_members" => client.get_escrow_sub_members(&page).await.map(|_| ()),
        "sub_api_keys" => {
            let params = BitgetSubApiKeysParamsBuilder::default()
                .sub_member_id("533285".to_string())
                .build()
                .unwrap();
            client.get_sub_api_keys(&params).await.map(|_| ())
        }
        "update_sub_api_key" => {
            let params = BitgetUpdateSubApiParamsBuilder::default()
                .read_only(true)
                .build()
                .unwrap();
            client.update_sub_api_key(&params).await.map(|_| ())
        }
        "update_master_api_key" => {
            let params = BitgetUpdateMasterApiParamsBuilder::default()
                .read_only(false)
                .build()
                .unwrap();
            client.update_master_api_key(&params).await.map(|_| ())
        }
        "fetch_all_sub_members_paged" => raw.fetch_all_sub_members_paged(Some(2)).await.map(|_| ()),
        "fetch_all_escrow_sub_members" => {
            raw.fetch_all_escrow_sub_members(Some(50)).await.map(|_| ())
        }
        "fetch_all_sub_api_keys" => raw
            .fetch_all_sub_api_keys("533285", Some(10))
            .await
            .map(|_| ()),
        _ => unreachable!(),
    }
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("not supported by the Bitget adapter"),
        "unexpected error: {error}"
    );
}

/// The demo `current-position` long (TSLAUSDT) plus an ETHUSDT short derived from it.
fn two_positions() -> Value {
    let mut positions = load_test_data("http_get_positions.json");
    let list = positions["data"]["list"].as_array_mut().unwrap();
    let mut short = list[0].clone();
    short["symbol"] = json!("ETHUSDT");
    short["posSide"] = json!("short");
    short["total"] = json!("5");
    short["available"] = json!("5");
    short["avgPrice"] = json!("3000");
    short["updatedTime"] = json!("1794193100112");
    list.push(short);
    positions
}

#[rstest]
#[case(false, "cache", "Instrument ETHUSDT-PERP not found in cache")]
#[case(true, "cache", "Instrument ETHUSDT-PERP not found in cache")]
#[case(false, "total", "position.size")]
#[case(true, "total", "position.size")]
#[case(false, "avgPrice", "Invalid decimal")]
#[case(true, "avgPrice", "Invalid decimal")]
#[case(false, "updatedTime", "position.updatedTime")]
#[case(true, "updatedTime", "position.updatedTime")]
#[case(false, "symbol", "Position symbol is empty")]
#[case(true, "symbol", "Position symbol is empty")]
#[tokio::test]
async fn test_position_reports_reject_incomplete_rows(
    #[case] scoped: bool,
    #[case] failure: &'static str,
    #[case] expected_error: &'static str,
) {
    let mut positions = two_positions();

    if failure != "cache" {
        positions["data"]["list"][1][failure] =
            json!(if failure == "symbol" { "" } else { "invalid" });
    }

    let (addr, state) = start_position_report_server(positions).await;
    let client = user_test_client(format!("http://{addr}"));
    let instruments: BitgetInstrumentLinearResponse =
        serde_json::from_value(load_test_data("http_get_instruments_linear.json")).unwrap();

    for definition in instruments.result.list {
        if failure != "cache" || definition.symbol == "TSLAUSDT" {
            let instrument = parse_linear_instrument(&definition, 0.into(), 0.into()).unwrap();
            client.cache_instrument(instrument);
        }
    }

    let result = client
        .request_position_status_reports(
            AccountId::new("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            scoped.then_some(InstrumentId::from("ETHUSDT-PERP.BITGET")),
        )
        .await;
    let error =
        result.expect_err("a held position row must not be omitted from successful reports");
    assert!(
        format!("{error:#}").contains(expected_error),
        "unexpected error: {error:#}"
    );

    if failure == "symbol" {
        assert_eq!(
            error.to_string(),
            format!(
                "Position symbol is empty, category=Linear, symbol={:?}, settle_coin=None",
                scoped.then_some("ETHUSDT"),
            )
        );
    } else if failure != "cache" {
        assert_eq!(
            error.to_string(),
            "failed to parse position report for ETHUSDT-PERP"
        );
    }

    // Bitget lists positions per category in one request (no settle coin, no cursor)
    let queries = state.position_queries.lock().await;
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].get("category").map(String::as_str),
        Some("USDT-FUTURES")
    );
    assert_eq!(
        queries[0].get("symbol").map(String::as_str),
        scoped.then_some("ETHUSDT")
    );
    assert_eq!(queries[0].get("settleCoin"), None);
}

#[rstest]
#[case(false, false)]
#[case(true, false)]
#[case(true, true)]
#[tokio::test]
async fn test_position_reports_preserve_held_and_flat_rows(
    #[case] scoped: bool,
    #[case] flat: bool,
) {
    let mut positions = two_positions();

    if flat {
        // A closed one-way position keeps `posSide` with a zero total
        positions["data"]["list"][1]["total"] = json!("0");
        positions["data"]["list"][1]["avgPrice"] = json!("0");
    }

    let (addr, state) = start_position_report_server(positions).await;
    let client = user_test_client(format!("http://{addr}"));
    client
        .request_instruments(BitgetProductType::Linear, None, None)
        .await
        .unwrap();
    let account_id = AccountId::new("BITGET-UNIFIED");
    let ts_before = get_atomic_clock_realtime().get_time_ns();
    let reports = client
        .request_position_status_reports(
            account_id,
            BitgetProductType::Linear,
            scoped.then_some(InstrumentId::from("ETHUSDT-PERP.BITGET")),
        )
        .await
        .unwrap();

    let ts_after = get_atomic_clock_realtime().get_time_ns();

    let expected_rows = [
        (
            "TSLAUSDT-PERP.BITGET",
            PositionSide::Long,
            "0.02",
            "0.02",
            Some("360.72"),
            1_794_193_099_832_000_000_u64,
        ),
        (
            "ETHUSDT-PERP.BITGET",
            if flat {
                PositionSide::Flat
            } else {
                PositionSide::Short
            },
            if flat { "0.00" } else { "5.00" },
            if flat { "0" } else { "-5" },
            if flat { None } else { Some("3000") },
            1_794_193_100_112_000_000_u64,
        ),
    ];

    let expected = if scoped {
        &expected_rows[1..]
    } else {
        &expected_rows[..]
    };

    assert_eq!(reports.len(), expected.len());

    for (report, (id, side, quantity, signed_quantity, avg_price, ts_last)) in
        reports.iter().zip(expected)
    {
        assert_eq!(report.account_id, account_id);
        assert_eq!(report.instrument_id, InstrumentId::from(*id));
        assert_eq!(report.position_side, *side);
        assert_eq!(report.quantity, Quantity::from(*quantity));
        assert_eq!(
            report.signed_decimal_qty,
            rust_decimal::Decimal::from_str_exact(signed_quantity).unwrap()
        );
        assert_eq!(
            report.avg_px_open,
            avg_price.map(|value| rust_decimal::Decimal::from_str_exact(value).unwrap())
        );
        assert_eq!(report.ts_last.as_u64(), *ts_last);
        assert!(report.ts_init >= ts_before && report.ts_init <= ts_after);
        assert_eq!(report.ts_init, reports[0].ts_init);
        assert_eq!(report.venue_position_id, None);
        assert_eq!(
            report.avg_px_open_reconciliation,
            AvgPxReconciliation::Match
        );
        assert_eq!(report.avg_px_open_precision, None);
    }

    if !scoped {
        assert_ne!(reports[0].report_id, reports[1].report_id);
    }

    // One request per call: Bitget has no settle coin pass
    let queries = state.position_queries.lock().await;
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].get("settleCoin"), None);
}

#[rstest]
#[tokio::test]
async fn test_position_reports_empty_list_is_null() {
    // `current-position` returns `"list": null` when the account holds no position
    let (addr, state) =
        start_position_report_server(load_test_data("http_get_positions_empty.json")).await;
    let client = user_test_client(format!("http://{addr}"));
    client
        .request_instruments(BitgetProductType::Linear, None, None)
        .await
        .unwrap();

    let reports = client
        .request_position_status_reports(
            AccountId::new("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            None,
        )
        .await
        .unwrap();

    assert!(reports.is_empty());
    assert_eq!(state.position_queries.lock().await.len(), 1);
}

async fn start_position_report_server(positions: Value) -> (SocketAddr, TestServerState) {
    let state = TestServerState::default();

    let router = Router::new()
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route(
            "/api/v3/position/current-position",
            get(
                move |Query(query): Query<HashMap<String, String>>,
                      State(state): State<TestServerState>| {
                    let mut response = positions.clone();
                    async move {
                        if query.contains_key("symbol")
                            && let Some(list) = response["data"]["list"].as_array_mut()
                        {
                            list.remove(0);
                        }

                        state.position_queries.lock().await.push(query);
                        Json(response)
                    }
                },
            ),
        )
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    wait_for_server(addr, "/api/v3/market/time").await;
    (addr, state)
}
