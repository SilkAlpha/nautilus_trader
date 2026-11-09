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

//! Integration tests for `BitgetExecutionClient`.
//!
//! These tests verify execution client operations including connection,
//! order submission, cancellation, and event handling.

use std::{
    cell::RefCell,
    collections::HashMap,
    net::SocketAddr,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Bytes,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use nautilus_bitget::{
    common::{
        consts::{BITGET_CLIENT_ID, BITGET_VENUE},
        enums::{
            BitgetEnvironment, BitgetMarginMode, BitgetOrderSmpType, BitgetPositionMode,
            BitgetProductType,
        },
    },
    config::BitgetExecutionClientConfig,
    execution::BitgetExecutionClient,
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::{replace_system_event_sender, set_exec_event_sender},
    messages::{
        ExecutionEvent, SystemEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, ExecutionReport,
            GenerateOrderStatusReports, GeneratePositionStatusReports, ModifyOrder, SubmitOrder,
        },
        system::SocketState,
    },
    testing::wait_until_async,
};
use nautilus_core::{UUID4, UnixNanos, params::Params};
use nautilus_live::{ExecutionClientCore, SocketReconnectRegistry, SocketReconnectRequestOutcome};
use nautilus_model::{
    accounts::{AccountAny, MarginAccount},
    enums::{
        AccountType, OmsType, OrderSide, OrderStatus, TimeInForce, TrailingOffsetType, TriggerType,
    },
    events::{AccountState, OrderDenied, OrderEventAny},
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, OrderListId, StrategyId, Symbol, TraderId,
        VenueOrderId,
    },
    orders::{
        LimitIfTouchedOrder, LimitOrder, MarketIfTouchedOrder, MarketOrder, Order, OrderAny,
        StopLimitOrder, StopMarketOrder, TrailingStopMarketOrder,
    },
    types::{AccountBalance, Money, Price, Quantity},
};
use nautilus_network::http::HttpClient;
use rstest::rstest;
use serde_json::{Value, json};
use ustr::Ustr;

#[derive(Clone)]
struct TestServerState {
    ws_connection_count: Arc<tokio::sync::Mutex<usize>>,
    private_ws_connections: Arc<AtomicUsize>,
    trade_ws_connections: Arc<AtomicUsize>,
    trade_order_ret_code: Arc<AtomicUsize>,
    trade_order_requests: Arc<AtomicUsize>,
    trade_order_payloads: Arc<tokio::sync::Mutex<Vec<Value>>>,
    trade_order_req_id_present: Arc<AtomicBool>,
    authenticated: Arc<AtomicBool>,
    login_count: Arc<AtomicUsize>,
    withhold_login: Arc<AtomicBool>,
    subscriptions: Arc<tokio::sync::Mutex<Vec<String>>>,
    private_pushes: Arc<tokio::sync::Mutex<Vec<Value>>>,
    ack_order_id: Arc<tokio::sync::Mutex<Option<String>>>,
    disconnect_trigger: Arc<AtomicBool>,
    reject_trade_websocket: Arc<AtomicBool>,
    empty_orders_realtime: Arc<AtomicBool>,
    rejected_orders_realtime: Arc<AtomicBool>,
    orders_realtime_requests: Arc<AtomicUsize>,
    cancel_order_ret_code: Arc<AtomicUsize>,
    cancel_order_requests: Arc<AtomicUsize>,
    cancel_all_requests: Arc<AtomicUsize>,
    modify_order_status: Arc<AtomicUsize>,
    modify_order_payloads: Arc<tokio::sync::Mutex<Vec<Value>>>,
    position_requests: Arc<AtomicUsize>,
    wallet_balance_requests: Arc<AtomicUsize>,
    ping_count: Arc<AtomicUsize>,
    switch_mode_requests: Arc<tokio::sync::Mutex<Vec<Value>>>,
    set_leverage_requests: Arc<tokio::sync::Mutex<Vec<Value>>>,
    set_margin_mode_requests: Arc<tokio::sync::Mutex<Vec<Value>>>,
}

impl Default for TestServerState {
    fn default() -> Self {
        Self {
            ws_connection_count: Arc::new(tokio::sync::Mutex::new(0)),
            private_ws_connections: Arc::new(AtomicUsize::new(0)),
            trade_ws_connections: Arc::new(AtomicUsize::new(0)),
            trade_order_ret_code: Arc::new(AtomicUsize::new(0)),
            trade_order_requests: Arc::new(AtomicUsize::new(0)),
            trade_order_payloads: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            trade_order_req_id_present: Arc::new(AtomicBool::new(false)),
            authenticated: Arc::new(AtomicBool::new(false)),
            login_count: Arc::new(AtomicUsize::new(0)),
            withhold_login: Arc::new(AtomicBool::new(false)),
            subscriptions: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            private_pushes: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            ack_order_id: Arc::new(tokio::sync::Mutex::new(None)),
            disconnect_trigger: Arc::new(AtomicBool::new(false)),
            reject_trade_websocket: Arc::new(AtomicBool::new(false)),
            empty_orders_realtime: Arc::new(AtomicBool::new(false)),
            rejected_orders_realtime: Arc::new(AtomicBool::new(false)),
            orders_realtime_requests: Arc::new(AtomicUsize::new(0)),
            cancel_order_ret_code: Arc::new(AtomicUsize::new(0)),
            cancel_order_requests: Arc::new(AtomicUsize::new(0)),
            cancel_all_requests: Arc::new(AtomicUsize::new(0)),
            modify_order_status: Arc::new(AtomicUsize::new(0)),
            modify_order_payloads: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            position_requests: Arc::new(AtomicUsize::new(0)),
            wallet_balance_requests: Arc::new(AtomicUsize::new(0)),
            ping_count: Arc::new(AtomicUsize::new(0)),
            switch_mode_requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            set_leverage_requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            set_margin_mode_requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }
}

fn load_test_data(filename: &str) -> Value {
    let path = format!("test_data/{filename}");
    let content = std::fs::read_to_string(path).expect("Failed to read test data");
    serde_json::from_str(&content).expect("Failed to parse test data")
}

fn has_auth_headers(headers: &HeaderMap) -> bool {
    headers.contains_key("access-key")
        && headers.contains_key("access-sign")
        && headers.contains_key("access-timestamp")
        && headers.contains_key("access-passphrase")
}

fn bitget_error(status: StatusCode, code: &str, msg: &str) -> Response {
    (
        status,
        Json(json!({
            "code": code,
            "msg": msg,
            "requestTime": 1794193099412_i64,
            "data": null
        })),
    )
        .into_response()
}

fn unauthorized() -> Response {
    bitget_error(StatusCode::BAD_REQUEST, "40037", "Apikey does not exist")
}

fn bitget_ok(data: Value) -> Response {
    let mut envelope = json!({
        "code": "00000",
        "msg": "success",
        "requestTime": 1794193099412_i64,
    });
    envelope["data"] = data;
    Json(envelope).into_response()
}

async fn handle_get_instruments(query: Query<HashMap<String, String>>) -> impl IntoResponse {
    let category = query.get("category").map(String::as_str);
    let filename = match category {
        Some("USDT-FUTURES") => "http_get_instruments_linear.json",
        Some("SPOT") => "http_get_instruments_spot.json",
        Some("COIN-FUTURES") => "http_get_instruments_inverse.json",
        _ => {
            return bitget_error(StatusCode::BAD_REQUEST, "40034", "Parameter category error");
        }
    };

    let mut instruments = load_test_data(filename);
    if category == Some("USDT-FUTURES") {
        // The order fixtures were captured on AMZNUSDT/COINUSDT, which share TSLAUSDT's rules
        let list = instruments["data"].as_array_mut().unwrap();
        let tsla = list
            .iter()
            .find(|row| row["symbol"] == "TSLAUSDT")
            .unwrap()
            .clone();
        for (symbol, base) in [("AMZNUSDT", "AMZN"), ("COINUSDT", "COIN")] {
            let mut row = tsla.clone();
            row["symbol"] = json!(symbol);
            row["baseCoin"] = json!(base);
            list.push(row);
        }
    }

    Json(instruments).into_response()
}

async fn handle_get_fee_rate(headers: HeaderMap) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }
    let fee_rate = load_test_data("http_get_fee_rate.json");
    Json(fee_rate).into_response()
}

async fn handle_get_wallet_balance(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }
    state
        .wallet_balance_requests
        .fetch_add(1, Ordering::Relaxed);
    let wallet = load_test_data("http_get_wallet_balance.json");
    Json(wallet).into_response()
}

async fn handle_get_account_settings(headers: HeaderMap) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }
    Json(load_test_data("http_get_account_info.json")).into_response()
}

async fn handle_get_positions(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }
    state.position_requests.fetch_add(1, Ordering::Relaxed);
    let positions = load_test_data("http_get_positions.json");
    Json(positions).into_response()
}

// Bitget returns `"list": null` for empty report pages
async fn handle_get_empty_report_list(headers: HeaderMap) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    Json(load_test_data("http_get_executions_empty.json")).into_response()
}

async fn handle_get_strategy_orders(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }
    state
        .orders_realtime_requests
        .fetch_add(1, Ordering::Relaxed);
    Json(load_test_data("http_get_executions_empty.json")).into_response()
}

// A post-only order that would take liquidity is cancelled with `post_only_fill_cancel`
fn post_only_cancelled_order() -> Value {
    let mut order = load_test_data("http_get_order_info.json");
    order["data"]["orderId"] = json!("test-order-id-12345");
    order["data"]["timeInForce"] = json!("post_only");
    order["data"]["orderStatus"] = json!("cancelled");
    order["data"]["cancelReason"] = json!("post_only_fill_cancel");
    order["data"]["cumExecQty"] = json!("0");
    order["data"]["cumExecValue"] = json!("0");
    order["data"]["avgPrice"] = json!("0");
    order["data"]["feeDetail"] = json!([]);
    order
}

async fn handle_get_orders_realtime(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    query: Query<HashMap<String, String>>,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    state
        .orders_realtime_requests
        .fetch_add(1, Ordering::Relaxed);

    // The fixture is the only page: following its cursor yields the venue's empty page
    if state.empty_orders_realtime.load(Ordering::Relaxed) || query.contains_key("cursor") {
        return Json(load_test_data("http_get_positions_empty.json")).into_response();
    }

    let mut orders = load_test_data("http_get_orders_realtime.json");

    if state.rejected_orders_realtime.load(Ordering::Relaxed) {
        let list = orders["data"]["list"].as_array_mut().unwrap();
        list[0]["orderId"] = json!("test-order-id-12345");
        list[0]["orderStatus"] = json!("cancelled");
        list[0]["cancelReason"] = json!("post_only_fill_cancel");
        list[1]["symbol"] = list[0]["symbol"].clone();
    }

    Json(orders).into_response()
}

async fn handle_get_order_info(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    state
        .orders_realtime_requests
        .fetch_add(1, Ordering::Relaxed);

    if state.empty_orders_realtime.load(Ordering::Relaxed) {
        return bitget_error(StatusCode::BAD_REQUEST, "25204", "Order does not exist");
    }

    if state.rejected_orders_realtime.load(Ordering::Relaxed) {
        return Json(post_only_cancelled_order()).into_response();
    }

    Json(load_test_data("http_get_order_info.json")).into_response()
}

async fn handle_post_order(headers: HeaderMap, body: Bytes) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    let Ok(order_req): Result<Value, _> = serde_json::from_slice(&body) else {
        return bitget_error(StatusCode::BAD_REQUEST, "40019", "Parameter body error");
    };

    bitget_ok(json!({
        "orderId": "test-order-id-12345",
        "clientOid": order_req.get("clientOid").and_then(|v| v.as_str()).unwrap_or("")
    }))
}

async fn handle_cancel_order(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    _body: Bytes,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    state.cancel_order_requests.fetch_add(1, Ordering::Relaxed);

    let ret_code = state.cancel_order_ret_code.load(Ordering::Relaxed);

    if ret_code != 0 {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            &ret_code.to_string(),
            &format!("Test venue rejection {ret_code}"),
        );
    }

    bitget_ok(json!({
        "orderId": "test-order-id-12345",
        "clientOid": "test-order"
    }))
}

async fn handle_modify_order(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    if let Ok(value) = serde_json::from_slice::<Value>(&body) {
        state.modify_order_payloads.lock().await.push(value);
    }

    let status = state.modify_order_status.load(Ordering::Relaxed);
    if status == 400 {
        return bitget_error(
            StatusCode::BAD_REQUEST,
            "25571",
            "The modification price and qty is the same as the original value",
        );
    }
    if status != 0 {
        let status = StatusCode::from_u16(u16::try_from(status).unwrap()).unwrap();
        return (status, "service unavailable").into_response();
    }

    bitget_ok(json!({
        "orderId": "test-order-id-12345",
        "clientOid": "test-order"
    }))
}

async fn handle_cancel_all_orders(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    _body: Bytes,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    state.cancel_all_requests.fetch_add(1, Ordering::Relaxed);

    bitget_ok(json!({ "list": [] }))
}

async fn handle_switch_mode(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    if let Ok(value) = serde_json::from_slice::<Value>(&body) {
        state.switch_mode_requests.lock().await.push(value);
    }

    bitget_ok(json!("success"))
}

async fn handle_set_leverage(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    if let Ok(value) = serde_json::from_slice::<Value>(&body) {
        state.set_leverage_requests.lock().await.push(value);
    }

    bitget_ok(json!("success"))
}

async fn handle_set_margin_mode(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !has_auth_headers(&headers) {
        return unauthorized();
    }

    if let Ok(value) = serde_json::from_slice::<Value>(&body) {
        state.set_margin_mode_requests.lock().await.push(value);
    }

    bitget_ok(Value::Null)
}

async fn handle_server_time() -> impl IntoResponse {
    Json(load_test_data("http_get_server_time.json"))
}

async fn handle_private_websocket(
    ws: WebSocketUpgrade,
    State(state): State<TestServerState>,
) -> Response {
    state.private_ws_connections.fetch_add(1, Ordering::Relaxed);
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_trade_websocket(
    ws: WebSocketUpgrade,
    State(state): State<TestServerState>,
) -> Response {
    state.trade_ws_connections.fetch_add(1, Ordering::Relaxed);
    if state.reject_trade_websocket.load(Ordering::Relaxed) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

fn trade_error_msg(code: usize) -> &'static str {
    match code {
        429 => "Too Many Requests",
        25206 => "TSLAUSDT trading price cannot exceed 2%",
        _ => "Test venue rejection",
    }
}

// Answers an `op:"trade"` request the way Bitget acks it on the private socket
fn trade_response(request: &Value, ret_code: usize, order_id: Option<&str>) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let rate_limit = json!([{"limit": "10", "remaining": "9"}]);

    if ret_code != 0 {
        return json!({
            "event": "error",
            "id": id,
            "code": ret_code.to_string(),
            "msg": trade_error_msg(ret_code),
            "connId": "test-conn-id",
            "rateLimit": rate_limit,
        });
    }

    let args: Vec<Value> = request["args"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|arg| {
            json!({
                "code": "0",
                "msg": "Success",
                "symbol": arg.get("symbol").cloned().unwrap_or(Value::Null),
                "orderId": arg
                    .get("orderId")
                    .cloned()
                    .unwrap_or_else(|| json!(order_id.unwrap_or("test-order-id-12345"))),
                "clientOid": arg.get("clientOid").cloned().unwrap_or(Value::Null),
                "cTime": "1794193086386",
            })
        })
        .collect();

    json!({
        "event": "trade",
        "id": id,
        "category": request.get("category").cloned().unwrap_or(Value::Null),
        "topic": request.get("topic").cloned().unwrap_or(Value::Null),
        "args": args,
        "code": "0",
        "msg": "Success",
        "ts": "1794193086388",
        "connId": "test-conn-id",
        "rateLimit": rate_limit,
    })
}

async fn handle_socket(mut socket: WebSocket, state: TestServerState) {
    {
        let mut count = state.ws_connection_count.lock().await;
        *count += 1;
    }

    let mut logged_in = false;
    let mut socket_topics: Vec<String> = Vec::new();

    loop {
        if state.disconnect_trigger.load(Ordering::Relaxed) {
            break;
        }

        // Account pushes only flow on logged-in sockets subscribed to the channel
        if logged_in && !socket_topics.is_empty() {
            let mut pushes = state.private_pushes.lock().await;
            let (ready, rest): (Vec<Value>, Vec<Value>) = pushes.drain(..).partition(|push| {
                socket_topics
                    .iter()
                    .any(|topic| push["arg"]["topic"] == **topic)
            });
            *pushes = rest;
            drop(pushes);

            for push in ready {
                if socket
                    .send(Message::Text(push.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        let msg_opt = match tokio::time::timeout(Duration::from_millis(50), socket.recv()).await {
            Ok(opt) => opt,
            Err(_) => continue,
        };

        let Some(msg) = msg_opt else {
            break;
        };

        let msg = match msg {
            Ok(m) => m,
            Err(_) => break,
        };

        match msg {
            // Bitget heartbeats are the plain text frames `ping`/`pong`
            Message::Text(text) if text.as_str() == "ping" => {
                state.ping_count.fetch_add(1, Ordering::Relaxed);

                if socket.send(Message::Text("pong".into())).await.is_err() {
                    break;
                }
            }
            Message::Text(text) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };

                let op = value.get("op").and_then(|v| v.as_str());
                let args = value
                    .get("args")
                    .and_then(|a| a.as_array())
                    .cloned()
                    .unwrap_or_default();

                let response = match op {
                    Some("login") => {
                        state.login_count.fetch_add(1, Ordering::Relaxed);
                        if state.withhold_login.load(Ordering::Relaxed) {
                            continue;
                        }

                        let login = args.first().cloned().unwrap_or_default();
                        let valid = login["apiKey"] == "test_api_key"
                            && login["passphrase"] == "test_api_passphrase"
                            && login["sign"].as_str().is_some_and(|sign| !sign.is_empty())
                            && login["timestamp"].as_str().is_some_and(|ts| ts.len() == 13);

                        if valid {
                            logged_in = true;
                            state.authenticated.store(true, Ordering::Relaxed);
                            vec![load_test_data("ws_auth_success.json")]
                        } else {
                            vec![load_test_data("ws_auth_failure.json")]
                        }
                    }
                    Some("subscribe") => {
                        let mut subs = state.subscriptions.lock().await;
                        for arg in &args {
                            let topic = arg["topic"].as_str().unwrap_or_default().to_string();
                            socket_topics.push(topic.clone());
                            if !subs.contains(&topic) {
                                subs.push(topic);
                            }
                        }
                        drop(subs);

                        args.iter()
                            .map(|arg| {
                                json!({
                                    "event": "subscribe",
                                    "arg": arg,
                                    "connId": "test-conn-id"
                                })
                            })
                            .collect()
                    }
                    Some("trade") => {
                        state.trade_order_requests.fetch_add(1, Ordering::Relaxed);
                        state.trade_order_payloads.lock().await.push(value.clone());
                        let req_id = value.get("id").and_then(|v| v.as_str());
                        state.trade_order_req_id_present.store(
                            req_id.is_some_and(|req_id| !req_id.is_empty()),
                            Ordering::Relaxed,
                        );
                        let ret_code = state.trade_order_ret_code.load(Ordering::Relaxed);
                        let order_id = state.ack_order_id.lock().await.clone();
                        vec![trade_response(&value, ret_code, order_id.as_deref())]
                    }
                    _ => vec![],
                };

                let mut failed = false;
                for frame in response {
                    if socket
                        .send(Message::Text(frame.to_string().into()))
                        .await
                        .is_err()
                    {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    break;
                }
            }
            Message::Ping(_) => {
                state.ping_count.fetch_add(1, Ordering::Relaxed);

                if socket.send(Message::Pong(vec![].into())).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => {
                break;
            }
            _ => {}
        }
    }

    let mut count = state.ws_connection_count.lock().await;
    *count = count.saturating_sub(1);
}

fn create_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route("/api/v3/account/all-fee-rate", get(handle_get_fee_rate))
        .route("/api/v3/account/assets", get(handle_get_wallet_balance))
        .route("/api/v3/account/settings", get(handle_get_account_settings))
        .route(
            "/api/v3/position/current-position",
            get(handle_get_positions),
        )
        .route(
            "/api/v3/trade/unfilled-orders",
            get(handle_get_orders_realtime),
        )
        .route("/api/v3/trade/order-info", get(handle_get_order_info))
        .route(
            "/api/v3/trade/history-orders",
            get(handle_get_empty_report_list),
        )
        .route("/api/v3/trade/fills", get(handle_get_empty_report_list))
        .route(
            "/api/v3/trade/unfilled-strategy-orders",
            get(handle_get_strategy_orders),
        )
        .route(
            "/api/v3/trade/history-strategy-orders",
            get(handle_get_strategy_orders),
        )
        .route("/api/v3/trade/place-order", post(handle_post_order))
        .route(
            "/api/v3/trade/place-strategy-order",
            post(handle_post_order),
        )
        .route("/api/v3/trade/cancel-order", post(handle_cancel_order))
        .route(
            "/api/v3/trade/cancel-strategy-order",
            post(handle_cancel_order),
        )
        .route(
            "/api/v3/trade/modify-strategy-order",
            post(handle_modify_order),
        )
        .route(
            "/api/v3/trade/cancel-symbol-order",
            post(handle_cancel_all_orders),
        )
        .route("/api/v3/account/set-hold-mode", post(handle_switch_mode))
        .route("/api/v3/account/set-leverage", post(handle_set_leverage))
        .route(
            "/api/v3/account/adjust-account-mode",
            post(handle_set_margin_mode),
        )
        .route("/api/v3/market/time", get(handle_server_time))
        .route("/v3/ws/private", get(handle_private_websocket))
        .route("/v3/ws/private/trade", get(handle_trade_websocket))
        .with_state(state)
}

async fn start_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let state = TestServerState::default();
    let router = create_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let health_url = format!("http://{addr}/api/v3/market/time");
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
    Ok((addr, state))
}

// Both execution sockets are Bitget private connections; the trade path is separate only so the
// mock can count and fail them independently
fn create_test_exec_config(addr: SocketAddr) -> BitgetExecutionClientConfig {
    BitgetExecutionClientConfig {
        api_key: Some("test_api_key".into()),
        api_secret: Some("test_api_secret".into()),
        api_passphrase: Some("test_api_passphrase".into()),
        product_types: vec![BitgetProductType::Linear],
        environment: BitgetEnvironment::Mainnet,
        base_url_http: Some(format!("http://{addr}")),
        base_url_ws_private: Some(format!("ws://{addr}/v3/ws/private")),
        base_url_ws_trade: Some(format!("ws://{addr}/v3/ws/private/trade")),
        proxy_url: None,
        http_timeout_secs: 10,
        max_retries: 1,
        retry_delay_initial_ms: 100,
        retry_delay_max_ms: 1000,
        heartbeat_interval_secs: 5,
        recv_window_ms: 5000,
        account_id: None,
        use_spot_position_reports: false,
        auto_repay_spot_borrows: false,
        futures_leverages: None,
        position_mode: None,
        margin_mode: None,
        transport_backend: Default::default(),
        ..Default::default()
    }
}

fn create_test_demo_exec_config(addr: SocketAddr) -> BitgetExecutionClientConfig {
    let mut config = create_test_exec_config(addr);
    config.environment = BitgetEnvironment::Demo;
    config.max_retries = 0;
    config
}

fn create_test_execution_client(
    addr: SocketAddr,
) -> (
    BitgetExecutionClient,
    tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    create_test_execution_client_with_config(create_test_exec_config(addr))
}

fn create_test_execution_client_with_config(
    config: BitgetExecutionClientConfig,
) -> (
    BitgetExecutionClient,
    tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;

    let cache = Rc::new(RefCell::new(Cache::default()));

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    // Event channel must be set before creating client due to thread-local storage
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let client = BitgetExecutionClient::new(core, config).unwrap();

    (client, rx, cache)
}

#[rstest]
#[case(true, 1)]
#[case(false, 0)]
#[tokio::test]
async fn test_exec_client_scoped_spot_position_reports_follow_config(
    #[case] use_spot_position_reports: bool,
    #[case] expected_reports: usize,
) {
    let (addr, _state) = start_test_server().await.unwrap();
    let mut config = create_test_exec_config(addr);
    config.product_types = vec![BitgetProductType::Spot];
    config.use_spot_position_reports = use_spot_position_reports;
    let (mut client, _rx, cache) = create_test_execution_client_with_config(config);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    client.connect().await.unwrap();

    let reports = client
        .generate_position_status_reports(&GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from("ETHUSDT-SPOT.BITGET")),
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();

    assert_eq!(reports.len(), expected_reports);
    if use_spot_position_reports {
        assert_eq!(reports[0].instrument_id, "ETHUSDT-SPOT.BITGET".into());
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_mixed_unscoped_position_reports_omit_spot() {
    let (addr, state) = start_test_server().await.unwrap();
    let mut config = create_test_exec_config(addr);
    config.product_types = vec![BitgetProductType::Linear, BitgetProductType::Spot];
    config.use_spot_position_reports = true;
    let (mut client, _rx, cache) = create_test_execution_client_with_config(config);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    client.connect().await.unwrap();

    // Measure against a post-connect baseline, since connect issues requests of its own.
    let positions_before = state.position_requests.load(Ordering::Relaxed);
    let wallet_before = state.wallet_balance_requests.load(Ordering::Relaxed);

    let reports = client
        .generate_position_status_reports(&GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            None,
            None,
            None,
        ))
        .await
        .expect("derivative reports must succeed when SPOT coverage is unavailable");

    assert!(
        reports
            .iter()
            .any(|report| report.instrument_id == "TSLAUSDT-PERP.BITGET".into())
    );
    // An unscoped USDT-FUTURES query is one request (Bitget has no settle-coin filter); SPOT
    // adds none.
    assert_eq!(
        state.position_requests.load(Ordering::Relaxed) - positions_before,
        1,
        "the LINEAR product type should still be requested"
    );
    // SPOT is served from wallet balances, so this is what proves it was omitted.
    assert_eq!(
        state.wallet_balance_requests.load(Ordering::Relaxed) - wallet_before,
        0,
        "no SPOT wallet balance request should be issued for a bulk report"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[case(vec![], "BTCUSDT-PERP.BITGET", true)]
#[case(vec![], "BTCUSD_CM-INVERSE.BITGET", false)]
#[case(vec![], "ETHUSDT-SPOT.BITGET", false)]
#[case(vec![], "BTCUSDT.BITGET", false)]
#[case(vec![BitgetProductType::Spot], "BTCUSDT-PERP.BITGET", false)]
#[case(vec![BitgetProductType::Spot], "BTCUSD_CM-INVERSE.BITGET", false)]
#[case(vec![BitgetProductType::Spot], "ETHUSDT-SPOT.BITGET", false)]
#[case(vec![BitgetProductType::Spot], "BTCUSDT.BITGET", false)]
#[case(
    vec![BitgetProductType::Linear, BitgetProductType::Spot],
    "BTCUSDT-PERP.BITGET",
    true
)]
#[case(
    vec![BitgetProductType::Linear, BitgetProductType::Spot],
    "BTCUSD_CM-INVERSE.BITGET",
    false
)]
#[case(
    vec![BitgetProductType::Linear, BitgetProductType::Spot],
    "ETHUSDT-SPOT.BITGET",
    false
)]
#[case(
    vec![BitgetProductType::Linear, BitgetProductType::Spot],
    "BTCUSDT.BITGET",
    false
)]
#[case(
    vec![BitgetProductType::Inverse, BitgetProductType::Spot],
    "BTCUSDT-PERP.BITGET",
    false
)]
#[case(
    vec![BitgetProductType::Inverse, BitgetProductType::Spot],
    "BTCUSD_CM-INVERSE.BITGET",
    true
)]
#[case(
    vec![BitgetProductType::Inverse, BitgetProductType::Spot],
    "ETHUSDT-SPOT.BITGET",
    false
)]
#[case(
    vec![BitgetProductType::Inverse, BitgetProductType::Spot],
    "BTCUSDT.BITGET",
    false
)]
#[tokio::test]
async fn test_exec_client_bulk_position_coverage_by_product_type(
    #[case] product_types: Vec<BitgetProductType>,
    #[case] instrument_id: &str,
    #[case] expected: bool,
) {
    let (addr, _state) = start_test_server().await.unwrap();
    let mut config = create_test_exec_config(addr);
    config.product_types = product_types;
    let (client, _rx, _cache) = create_test_execution_client_with_config(config);

    // An identifier carrying no recognized product-type suffix must fail closed: absence of a
    // report from a bulk request is not evidence the position is flat.
    assert_eq!(
        client.provides_bulk_position_coverage(InstrumentId::from(instrument_id)),
        expected
    );
}

#[rstest]
#[tokio::test]
async fn test_exec_client_mass_status_omits_spot_and_preserves_derivative_positions() {
    let (addr, state) = start_test_server().await.unwrap();
    let mut config = create_test_exec_config(addr);
    config.product_types = vec![BitgetProductType::Linear, BitgetProductType::Spot];
    config.use_spot_position_reports = true;
    let (mut client, _rx, cache) = create_test_execution_client_with_config(config);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    client.connect().await.unwrap();
    state.wallet_balance_requests.store(0, Ordering::Relaxed);

    let mass_status = client
        .generate_mass_status(None)
        .await
        .expect("mass status must not fail when SPOT coverage is unavailable")
        .expect("mass status must be populated");

    assert!(
        mass_status
            .position_reports()
            .contains_key(&InstrumentId::from("TSLAUSDT-PERP.BITGET"))
    );
    assert_eq!(state.wallet_balance_requests.load(Ordering::Relaxed), 0);

    client.disconnect().await.unwrap();
}

fn create_test_demo_execution_client(
    addr: SocketAddr,
) -> (
    BitgetExecutionClient,
    tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;

    let cache = Rc::new(RefCell::new(Cache::default()));

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let client = BitgetExecutionClient::new(core, config).unwrap();

    (client, rx, cache)
}

fn add_test_account_to_cache(cache: &Rc<RefCell<Cache>>, account_id: AccountId) {
    let account_state = AccountState::new(
        account_id,
        AccountType::Margin,
        vec![AccountBalance::new(
            Money::from("10000.0 USDT"),
            Money::from("0 USDT"),
            Money::from("10000.0 USDT"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );

    let account = AccountAny::Margin(MarginAccount::new(account_state, true));
    cache.borrow_mut().add_account(account).unwrap();
}

// Conditional (strategy) orders take Bitget's REST path, so tests of that path cache one first
fn stop_market_order(
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
) -> OrderAny {
    OrderAny::StopMarket(StopMarketOrder::new(
        TraderId::from("TESTER-001"),
        strategy_id,
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Quantity::from("0.01"),
        Price::from("1500.00"),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ))
}

fn add_stop_market_to_cache(
    cache: &Rc<RefCell<Cache>>,
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
) {
    cache
        .borrow_mut()
        .add_order(
            stop_market_order(strategy_id, instrument_id, client_order_id),
            None,
            Some(*BITGET_CLIENT_ID),
            false,
        )
        .unwrap();
}

async fn drain_execution_events(rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>) {
    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}
}

async fn assert_no_cancel_rejected(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    duration: Duration,
) {
    let reject_window = tokio::time::sleep(duration);
    tokio::pin!(reject_window);

    loop {
        tokio::select! {
            () = &mut reject_window => break,
            event = rx.recv() => {
                let event = event.expect("channel closed");
                assert!(
                    !matches!(event, ExecutionEvent::Order(OrderEventAny::CancelRejected(_))),
                    "Ambiguous cancel outcome must not emit OrderCancelRejected: {event:?}",
                );
            }
        }
    }
}

async fn assert_no_modify_rejected(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    duration: Duration,
) {
    let reject_window = tokio::time::sleep(duration);
    tokio::pin!(reject_window);

    loop {
        tokio::select! {
            () = &mut reject_window => break,
            event = rx.recv() => {
                let event = event.expect("channel closed");
                assert!(
                    !matches!(event, ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))),
                    "Ambiguous modify outcome must not emit OrderModifyRejected: {event:?}",
                );
            }
        }
    }
}

#[rstest]
#[tokio::test]
async fn test_exec_client_creation() {
    let (addr, _state) = start_test_server().await.unwrap();
    let (client, _rx, _cache) = create_test_execution_client(addr);

    assert_eq!(client.client_id(), *BITGET_CLIENT_ID);
    assert_eq!(client.venue(), *BITGET_VENUE);
    assert_eq!(client.oms_type(), OmsType::Netting);
    assert!(!client.is_connected());
}

#[rstest]
#[tokio::test]
async fn test_exec_client_connect_disconnect() {
    let (system_tx, mut system_rx) = tokio::sync::mpsc::unbounded_channel();
    replace_system_event_sender(system_tx);
    let (addr, state) = start_test_server().await.unwrap();
    let registry = SocketReconnectRegistry::default();
    let (mut client, _rx, cache) = registry.scope(|| create_test_execution_client(addr));
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));

    client.connect().await.unwrap();

    wait_until_async(
        || async { *state.ws_connection_count.lock().await >= 2 },
        Duration::from_secs(10),
    )
    .await;
    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    let mut connected = Vec::new();
    while connected.len() < 2 {
        let event = tokio::time::timeout(Duration::from_secs(2), system_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let SystemEvent::SocketState(change) = event;
        if change.state == SocketState::Connected {
            connected.push(change.endpoint);
        }
    }
    connected.sort_unstable();
    let expected = vec![
        Ustr::from("bitget-trading"),
        Ustr::from("bitget-user-streams"),
    ];
    assert!(client.is_connected());
    assert!(state.authenticated.load(Ordering::Relaxed));
    assert_eq!(connected, expected);

    for endpoint in &expected {
        let handle = registry.handle(*BITGET_CLIENT_ID, *endpoint).unwrap();
        assert_eq!(
            handle.request_reconnect(),
            SocketReconnectRequestOutcome::Accepted
        );
    }

    let mut disconnected = Vec::new();
    while disconnected.len() < 2 {
        let event = tokio::time::timeout(Duration::from_secs(2), system_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let SystemEvent::SocketState(change) = event;
        if change.state == SocketState::Disconnected {
            disconnected.push(change.endpoint);
        }
    }
    disconnected.sort_unstable();
    assert_eq!(disconnected, expected);

    let subs = state.subscriptions.lock().await;
    assert!(subs.contains(&"order".to_string()));
    assert!(subs.contains(&"strategy-order".to_string()));
    assert!(subs.contains(&"fill".to_string()));
    assert!(subs.contains(&"position".to_string()));
    assert!(subs.contains(&"account".to_string()));
    drop(subs);

    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    assert!(
        expected
            .iter()
            .all(|endpoint| registry.handle(*BITGET_CLIENT_ID, *endpoint).is_none())
    );
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_open_only_retains_recent_closed() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, _rx, cache) = create_test_execution_client(addr);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    client.connect().await.unwrap();
    state
        .rejected_orders_realtime
        .store(true, Ordering::Relaxed);

    let command = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        true,
        Some(InstrumentId::from("AMZNUSDT-PERP.BITGET")),
        None,
        None,
        None,
        None,
    );
    let reports = client
        .generate_order_status_reports(&command)
        .await
        .unwrap();

    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].venue_order_id, "test-order-id-12345".into());
    assert_eq!(reports[0].order_status, OrderStatus::Rejected);
    assert_eq!(reports[1].venue_order_id, "1503520610414125056".into());
    assert_eq!(reports[1].order_status, OrderStatus::Accepted);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_failed_startup_rolls_back_private_stream() {
    let (addr, state) = start_test_server().await.unwrap();
    state.reject_trade_websocket.store(true, Ordering::Relaxed);
    let registry = SocketReconnectRegistry::default();
    let (mut client, _rx, cache) = registry.scope(|| create_test_execution_client(addr));
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));

    let error = client.connect().await.unwrap_err();

    wait_until_async(
        || async { *state.ws_connection_count.lock().await == 0 },
        Duration::from_secs(2),
    )
    .await;
    assert!(
        error.to_string().contains("WebSocket transport error"),
        "unexpected startup error: {error:#}",
    );
    assert_eq!(state.private_ws_connections.load(Ordering::Relaxed), 1);
    assert!(!client.is_connected());
    assert!(
        ["bitget-user-streams", "bitget-trading"]
            .into_iter()
            .all(|endpoint| registry
                .handle(*BITGET_CLIENT_ID, Ustr::from(endpoint))
                .is_none())
    );
}

#[rstest]
#[tokio::test]
async fn test_exec_client_connect_applies_position_mode_for_derivative_symbols() {
    let (addr, state) = start_test_server().await.unwrap();
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache,
    );

    let mut position_mode = std::collections::HashMap::new();
    position_mode.insert("ETHUSDT-PERP".to_string(), BitgetPositionMode::BothSides);
    position_mode.insert(
        "BTCUSD_CM-INVERSE".to_string(),
        BitgetPositionMode::MergedSingle,
    );
    // Spot symbol must be filtered out (hold mode applies to futures only).
    position_mode.insert("BTCUSDT-SPOT".to_string(), BitgetPositionMode::MergedSingle);

    let mut config = create_test_exec_config(addr);
    config.position_mode = Some(position_mode);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();

    client.connect().await.unwrap();

    wait_until_async(
        || async { state.switch_mode_requests.lock().await.len() >= 2 },
        Duration::from_secs(10),
    )
    .await;

    let requests = state.switch_mode_requests.lock().await;
    assert_eq!(
        requests.len(),
        2,
        "set-hold-mode should be called for Linear+Inverse only, not Spot",
    );

    // Bitget's hold mode is account-wide: the body carries only `holdMode`
    let modes: Vec<&str> = requests
        .iter()
        .filter_map(|r| r.get("holdMode").and_then(|v| v.as_str()))
        .collect();
    assert!(modes.contains(&"hedge_mode"));
    assert!(modes.contains(&"one_way_mode"));
    assert!(requests.iter().all(|r| r.get("symbol").is_none()));

    drop(requests);
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_connect_applies_leverage_and_margin_mode() {
    let (addr, state) = start_test_server().await.unwrap();
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache,
    );

    let mut leverages = std::collections::HashMap::new();
    leverages.insert("ETHUSDT-PERP".to_string(), 5);

    let mut config = create_test_exec_config(addr);
    config.futures_leverages = Some(leverages);
    config.margin_mode = Some(BitgetMarginMode::RegularMargin);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();

    client.connect().await.unwrap();

    wait_until_async(
        || async {
            !state.set_leverage_requests.lock().await.is_empty()
                && !state.set_margin_mode_requests.lock().await.is_empty()
        },
        Duration::from_secs(10),
    )
    .await;

    let leverage_reqs = state.set_leverage_requests.lock().await;
    assert_eq!(leverage_reqs.len(), 1);
    assert_eq!(
        leverage_reqs[0].get("symbol").and_then(|v| v.as_str()),
        Some("ETHUSDT"),
    );
    assert_eq!(
        leverage_reqs[0].get("leverage").and_then(|v| v.as_str()),
        Some("5"),
    );
    assert_eq!(
        leverage_reqs[0].get("category").and_then(|v| v.as_str()),
        Some("USDT-FUTURES"),
    );
    drop(leverage_reqs);

    let margin_reqs = state.set_margin_mode_requests.lock().await;
    assert_eq!(margin_reqs.len(), 1);
    assert_eq!(
        margin_reqs[0].get("mode").and_then(|v| v.as_str()),
        Some("basic"),
    );
    drop(margin_reqs);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
// Bitget demo accepts `op:"trade"`, so demo opens and logs in both private sockets
async fn test_exec_client_demo_mode_connects_trade_ws() {
    let (addr, state) = start_test_server().await.unwrap();
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache,
    );

    let config = BitgetExecutionClientConfig {
        api_key: Some("test_api_key".into()),
        api_secret: Some("test_api_secret".into()),
        api_passphrase: Some("test_api_passphrase".into()),
        product_types: vec![BitgetProductType::Linear],
        environment: BitgetEnvironment::Demo,
        base_url_http: Some(format!("http://{addr}")),
        base_url_ws_private: Some(format!("ws://{addr}/v3/ws/private")),
        base_url_ws_trade: Some(format!("ws://{addr}/v3/ws/private/trade")),
        proxy_url: None,
        http_timeout_secs: 10,
        max_retries: 1,
        retry_delay_initial_ms: 100,
        retry_delay_max_ms: 1000,
        heartbeat_interval_secs: 5,
        recv_window_ms: 5000,
        account_id: None,
        use_spot_position_reports: false,
        auto_repay_spot_borrows: false,
        futures_leverages: None,
        position_mode: None,
        margin_mode: None,
        transport_backend: Default::default(),
        ..Default::default()
    };

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();

    // Wait for subscriptions to confirm connection phase is complete
    wait_until_async(
        || async { state.private_ws_connections.load(Ordering::Relaxed) >= 1 },
        Duration::from_secs(10),
    )
    .await;
    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    let private_count = state.private_ws_connections.load(Ordering::Relaxed);
    let trade_count = state.trade_ws_connections.load(Ordering::Relaxed);
    assert_eq!(private_count, 1, "Demo mode should connect to private WS");
    assert_eq!(trade_count, 1, "Demo mode should connect to trade WS");
    assert_eq!(state.login_count.load(Ordering::Relaxed), 2);

    assert!(client.is_connected());
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_query_order() {
    use nautilus_common::messages::execution::QueryOrder;

    let (addr, _state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) = create_test_execution_client(addr);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));

    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(|| async { client.is_connected() }, Duration::from_secs(10)).await;

    // Drain connection events (account state, subscriptions)
    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    let cmd = QueryOrder::new(
        TraderId::from("TESTER-001"),
        Some(*BITGET_CLIENT_ID),
        StrategyId::from("S-001"),
        InstrumentId::new(Symbol::from("TSLAUSDT-PERP"), *BITGET_VENUE),
        ClientOrderId::from("tdcapioc93099"),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None, // correlation_id
    );

    client.query_order(cmd).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for query_order event")
        .expect("channel closed");

    match event {
        ExecutionEvent::Report(ExecutionReport::Order(report)) => {
            assert_eq!(
                report.client_order_id,
                Some(ClientOrderId::from("tdcapioc93099")),
            );
        }
        other => panic!("Expected OrderStatusReport, was {other:?}"),
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[case::derivatives_config_default(
    "ETHUSDT-PERP",
    Some(BitgetOrderSmpType::CancelMaker),
    None,
    Some("cancel_maker")
)]
#[case::spot_param_overrides_config(
    "BTCUSDT-SPOT",
    Some(BitgetOrderSmpType::CancelMaker),
    Some("CancelBoth"),
    Some("cancel_both")
)]
#[case::spot_param_without_config("BTCUSDT-SPOT", None, Some("CancelTaker"), Some("cancel_taker"))]
#[case::unset_omits_field("ETHUSDT-PERP", None, None, None)]
#[tokio::test]
async fn test_exec_client_submit_order_sends_configured_smp_type(
    #[case] symbol: &str,
    #[case] configured: Option<BitgetOrderSmpType>,
    #[case] param: Option<&str>,
    #[case] expected: Option<&str>,
) {
    let (addr, state) = start_test_server().await.unwrap();
    let mut config = create_test_exec_config(addr);
    config.product_types = vec![BitgetProductType::Linear, BitgetProductType::Spot];
    config.smp_type = configured;

    let (mut client, mut rx, cache) = create_test_execution_client_with_config(config);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    client.start().unwrap();
    client.connect().await.unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;
    drain_execution_events(&mut rx).await;

    let trader_id = TraderId::from("TESTER-001");
    let strategy_id = StrategyId::from("S-001");
    let client_id = *BITGET_CLIENT_ID;
    let instrument_id = InstrumentId::new(Symbol::from(symbol), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("test-smp-submit");
    let order = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));
    let init = order.init_event().clone();
    cache
        .borrow_mut()
        .add_order(order, None, Some(client_id), false)
        .unwrap();

    let params = param.map(|value| {
        let mut params = Params::new();
        params.insert("smp_type".to_string(), json!(value));
        params
    });

    let command = SubmitOrder::new(
        trader_id,
        Some(client_id),
        strategy_id,
        instrument_id,
        client_order_id,
        init,
        None,
        None,
        params,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.submit_order(command).unwrap();

    wait_until_async(
        || async { state.trade_order_payloads.lock().await.len() == 1 },
        Duration::from_secs(5),
    )
    .await;

    let payloads = state.trade_order_payloads.lock().await;
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0]["op"].as_str(), Some("trade"));
    assert_eq!(payloads[0]["topic"].as_str(), Some("place-order"));

    let order_args = &payloads[0]["args"][0];

    assert_eq!(
        order_args.get("clientOid").and_then(Value::as_str),
        Some(client_order_id.as_str())
    );
    assert_eq!(order_args.get("stpMode").and_then(Value::as_str), expected);

    drop(payloads);
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_trade_rate_limit_emits_order_rejected() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) = create_test_execution_client(addr);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    client.start().unwrap();
    client.connect().await.unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;
    drain_execution_events(&mut rx).await;
    state.trade_order_ret_code.store(429, Ordering::Relaxed);

    let trader_id = TraderId::from("TESTER-001");
    let strategy_id = StrategyId::from("S-001");
    let client_id = *BITGET_CLIENT_ID;
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("test-ws-rate-limit-reject");
    let order = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));
    let init = order.init_event().clone();
    cache
        .borrow_mut()
        .add_order(order, None, Some(client_id), false)
        .unwrap();
    let command = SubmitOrder::new(
        trader_id,
        Some(client_id),
        strategy_id,
        instrument_id,
        client_order_id,
        init,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.submit_order(command).unwrap();

    let submitted = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for OrderSubmitted")
        .expect("channel closed");
    assert!(
        matches!(submitted, ExecutionEvent::Order(OrderEventAny::Submitted(ref event))
            if event.client_order_id == client_order_id),
        "Expected OrderSubmitted for {client_order_id}, was {submitted:?}",
    );
    wait_until_async(
        || async { state.trade_order_requests.load(Ordering::Relaxed) == 1 },
        Duration::from_secs(5),
    )
    .await;
    assert!(state.trade_order_req_id_present.load(Ordering::Relaxed));
    let rejected = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for OrderRejected")
        .expect("channel closed");
    let ExecutionEvent::Order(OrderEventAny::Rejected(event)) = rejected else {
        panic!("Expected OrderRejected, was {rejected:?}");
    };
    assert_eq!(event.client_order_id, client_order_id);
    assert_eq!(event.reason.to_string(), "Too Many Requests");

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_query_account_does_not_block_within_runtime() {
    use nautilus_common::messages::execution::QueryAccount;

    let (addr, _state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) = create_test_execution_client(addr);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));

    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(|| async { client.is_connected() }, Duration::from_secs(10)).await;

    // Drain connection events (account state, subscriptions)
    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    let cmd = QueryAccount::new(
        TraderId::from("TESTER-001"),
        Some(*BITGET_CLIENT_ID),
        AccountId::from("BITGET-001"),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None, // correlation_id
    );

    client.query_account(cmd).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for query_account event")
        .expect("channel closed");

    assert!(
        matches!(event, ExecutionEvent::Account(_)),
        "Expected ExecutionEvent::Account, was {event:?}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_submit_order_list_demo() {
    use nautilus_common::messages::execution::SubmitOrderList;
    use nautilus_model::orders::OrderList;

    let (addr, state) = start_test_server().await.unwrap();
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = BitgetExecutionClientConfig {
        api_key: Some("test_api_key".into()),
        api_secret: Some("test_api_secret".into()),
        api_passphrase: Some("test_api_passphrase".into()),
        product_types: vec![BitgetProductType::Linear],
        environment: BitgetEnvironment::Demo,
        base_url_http: Some(format!("http://{addr}")),
        base_url_ws_private: Some(format!("ws://{addr}/v3/ws/private")),
        base_url_ws_trade: Some(format!("ws://{addr}/v3/ws/private/trade")),
        proxy_url: None,
        http_timeout_secs: 10,
        max_retries: 1,
        retry_delay_initial_ms: 100,
        retry_delay_max_ms: 1000,
        heartbeat_interval_secs: 5,
        recv_window_ms: 5000,
        account_id: None,
        use_spot_position_reports: false,
        auto_repay_spot_borrows: false,
        futures_leverages: None,
        position_mode: None,
        margin_mode: None,
        transport_backend: Default::default(),
        ..Default::default()
    };

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    // Drain connection events (account state, subscriptions)
    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    let cid1 = ClientOrderId::from("test-list-order-1");
    let cid2 = ClientOrderId::from("test-list-order-2");

    let order1 = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid1,
        OrderSide::Buy,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));
    let order2 = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid2,
        OrderSide::Sell,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));

    let init1 = order1.init_event().clone();
    let init2 = order2.init_event().clone();

    cache
        .borrow_mut()
        .add_order(order1, None, Some(client_id), false)
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order2, None, Some(client_id), false)
        .unwrap();

    let order_list = OrderList::new(
        OrderListId::from("test-list-1"),
        instrument_id,
        strategy_id,
        vec![cid1, cid2],
        UnixNanos::default(),
    );

    let cmd = SubmitOrderList::new(
        trader_id,
        Some(client_id),
        strategy_id,
        order_list,
        vec![init1, init2],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order_list(cmd).unwrap();

    let mut submitted_count = 0;

    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for OrderSubmitted")
            .expect("channel closed");

        if let ExecutionEvent::Order(ref order_event) = event
            && order_event.to_string().contains("OrderSubmitted")
        {
            submitted_count += 1;
        }
    }

    assert_eq!(submitted_count, 2, "Expected 2 OrderSubmitted events");

    // Plain orders go out as one `op:"trade"` batch-place on the trade socket, demo included
    wait_until_async(
        || async { state.trade_order_payloads.lock().await.len() == 1 },
        Duration::from_secs(5),
    )
    .await;
    let payloads = state.trade_order_payloads.lock().await;
    assert_eq!(payloads[0]["topic"], "batch-place");
    assert_eq!(payloads[0]["category"], "usdt-futures");
    assert_eq!(payloads[0]["args"].as_array().unwrap().len(), 2);
    drop(payloads);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_demo_cancel_post_lookup_failure_does_not_reject() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) = create_test_demo_execution_client(addr);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));

    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;
    drain_execution_events(&mut rx).await;

    // A conditional order is cancelled over REST; 40010 (request timed out) leaves the outcome
    // unknown
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("test-cancel-post-lookup-ambiguous");
    add_stop_market_to_cache(
        &cache,
        StrategyId::from("S-001"),
        instrument_id,
        client_order_id,
    );
    state.cancel_order_ret_code.store(40010, Ordering::Relaxed);

    let cmd = CancelOrder::new(
        TraderId::from("TESTER-001"),
        Some(*BITGET_CLIENT_ID),
        StrategyId::from("S-001"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("test-order-id-12345")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    client.cancel_order(cmd).unwrap();

    assert_no_cancel_rejected(&mut rx, Duration::from_millis(300)).await;
    assert_eq!(state.cancel_order_requests.load(Ordering::Relaxed), 1);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_demo_batch_cancel_failure_preserves_owning_strategies() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) = create_test_demo_execution_client(addr);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    state.cancel_order_ret_code.store(25204, Ordering::Relaxed);

    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;
    drain_execution_events(&mut rx).await;

    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    // Conditional orders make the batch go over REST, one cancel per order
    for (index, owner) in ["S-001", "S-002"].into_iter().enumerate() {
        add_stop_market_to_cache(
            &cache,
            StrategyId::from(owner),
            instrument_id,
            ClientOrderId::from(format!("O-DEMO-BATCH-{index}")),
        );
    }
    let cancels: Vec<CancelOrder> = ["S-001", "S-002"]
        .into_iter()
        .enumerate()
        .map(|(index, owner)| {
            CancelOrder::new(
                TraderId::from("TESTER-001"),
                Some(*BITGET_CLIENT_ID),
                StrategyId::from(owner),
                instrument_id,
                ClientOrderId::from(format!("O-DEMO-BATCH-{index}")),
                Some(VenueOrderId::from(format!("venue-batch-{index}"))),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            )
        })
        .collect();

    client
        .batch_cancel_orders(BatchCancelOrders::new(
            TraderId::from("TESTER-001"),
            Some(*BITGET_CLIENT_ID),
            StrategyId::from("S-001"),
            instrument_id,
            cancels.clone(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();

    let mut actual = Vec::new();

    for _ in &cancels {
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for OrderCancelRejected")
            .expect("channel closed");

        match event {
            ExecutionEvent::Order(OrderEventAny::CancelRejected(rejected)) => {
                actual.push((rejected.client_order_id, rejected.strategy_id));
            }
            event => panic!("expected OrderCancelRejected, was {event:?}"),
        }
    }

    let mut expected: Vec<_> = cancels
        .iter()
        .map(|cancel| (cancel.client_order_id, cancel.strategy_id))
        .collect();
    actual.sort();
    expected.sort();

    assert_eq!(actual, expected);
    assert_eq!(state.cancel_order_requests.load(Ordering::Relaxed), 2);
    assert!(rx.try_recv().is_err());

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_demo_sided_cancel_all_with_empty_cache_sends_nothing() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) = create_test_demo_execution_client(addr);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    state.empty_orders_realtime.store(true, Ordering::Relaxed);

    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;
    drain_execution_events(&mut rx).await;

    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);
    assert!(
        cache
            .borrow()
            .orders_open(None, Some(&instrument_id), None, None, None)
            .is_empty()
    );

    let cancel_all_before = state.cancel_all_requests.load(Ordering::Relaxed);
    let cancel_before = state.cancel_order_requests.load(Ordering::Relaxed);

    client
        .cancel_all_orders(CancelAllOrders::new(
            TraderId::from("TESTER-001"),
            Some(*BITGET_CLIENT_ID),
            StrategyId::from("S-001"),
            instrument_id,
            Some(OrderSide::Buy),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();

    assert_no_cancel_rejected(&mut rx, Duration::from_millis(300)).await;
    assert_eq!(
        state.cancel_all_requests.load(Ordering::Relaxed),
        cancel_all_before
    );
    assert_eq!(
        state.cancel_order_requests.load(Ordering::Relaxed),
        cancel_before
    );
    assert!(rx.try_recv().is_err());

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_demo_modify_whole_http_failure_does_not_reject() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) = create_test_demo_execution_client(addr);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));

    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;
    drain_execution_events(&mut rx).await;

    // Conditional orders are modified over REST (`modify-strategy-order`); a 503 leaves the
    // outcome unknown
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("test-modify-http-ambiguous");
    add_stop_market_to_cache(
        &cache,
        StrategyId::from("S-001"),
        instrument_id,
        client_order_id,
    );
    state.modify_order_status.store(503, Ordering::Relaxed);

    let cmd = ModifyOrder::new(
        TraderId::from("TESTER-001"),
        Some(*BITGET_CLIENT_ID),
        StrategyId::from("S-001"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("test-order-id-12345")),
        Some(Quantity::from("0.02")),
        None,
        Some(Price::from("1600.00")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    client.modify_order(cmd).unwrap();

    assert_no_modify_rejected(&mut rx, Duration::from_millis(300)).await;
    let payloads = state.modify_order_payloads.lock().await;
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0]["orderId"], "test-order-id-12345");
    assert_eq!(payloads[0]["clientOid"], "test-modify-http-ambiguous");
    assert_eq!(payloads[0]["qty"], "0.02");
    assert_eq!(payloads[0]["triggerPrice"], "1600.00");
    drop(payloads);
    assert_eq!(state.trade_order_requests.load(Ordering::Relaxed), 0);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_demo_submit_post_lookup_failure_does_not_reject() {
    let (addr, state) = start_test_server().await.unwrap();

    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    state.empty_orders_realtime.store(true, Ordering::Relaxed);
    let order_lookup_requests = state.orders_realtime_requests.load(Ordering::Relaxed);

    let cid = ClientOrderId::from("test-unknown-submit-outcome");
    // Conditional orders go to REST place-strategy-order and are looked up afterwards
    let order = stop_market_order(strategy_id, instrument_id, cid);
    let init = order.init_event().clone();

    cache
        .borrow_mut()
        .add_order(order, None, Some(client_id), false)
        .unwrap();

    let cmd = SubmitOrder::new(
        trader_id,
        Some(client_id),
        strategy_id,
        instrument_id,
        cid,
        init,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order(cmd).unwrap();

    let submitted = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for OrderSubmitted")
        .expect("channel closed");
    assert!(
        matches!(submitted, ExecutionEvent::Order(ref event) if event.to_string().contains("OrderSubmitted")),
        "Expected OrderSubmitted, was {submitted:?}",
    );

    wait_until_async(
        || async { state.orders_realtime_requests.load(Ordering::Relaxed) > order_lookup_requests },
        Duration::from_secs(5),
    )
    .await;

    let reject_window = tokio::time::sleep(Duration::from_millis(300));
    tokio::pin!(reject_window);

    loop {
        tokio::select! {
            () = &mut reject_window => break,
            event = rx.recv() => {
                let event = event.expect("channel closed");
                assert!(
                    !matches!(event, ExecutionEvent::Order(ref order_event) if order_event.to_string().contains("OrderRejected")),
                    "Unknown submit outcome must not emit OrderRejected: {event:?}",
                );
            }
        }
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_demo_submit_take_profit_sends_takeprofit() {
    let (addr, state) = start_test_server().await.unwrap();

    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    let cid = ClientOrderId::from("test-tp-demo-ws");
    let order = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid,
        OrderSide::Buy,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));
    let init = order.init_event().clone();

    cache
        .borrow_mut()
        .add_order(order, None, Some(client_id), false)
        .unwrap();

    let mut params = Params::new();
    params.insert("take_profit".to_string(), json!("3000"));
    params.insert("stop_loss".to_string(), json!("1500"));

    let cmd = SubmitOrder::new(
        trader_id,
        Some(client_id),
        strategy_id,
        instrument_id,
        cid,
        init,
        None,
        None,
        Some(params),
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order(cmd).unwrap();

    // Bitget demo carries attached TP/SL on the WS place-order (lowercase field names)
    wait_until_async(
        || async { state.trade_order_payloads.lock().await.len() == 1 },
        Duration::from_secs(5),
    )
    .await;
    let payloads = state.trade_order_payloads.lock().await;
    let args = &payloads[0]["args"][0];
    assert_eq!(payloads[0]["topic"], "place-order");
    assert_eq!(args["clientOid"], "test-tp-demo-ws");
    assert_eq!(args["takeprofit"], "3000");
    assert_eq!(args["stoploss"], "1500");
    drop(payloads);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_demo_submit_confirmed_rejection_emits_order_rejected() {
    use nautilus_common::messages::execution::SubmitOrderList;
    use nautilus_model::orders::OrderList;

    let (addr, state) = start_test_server().await.unwrap();

    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    state
        .rejected_orders_realtime
        .store(true, Ordering::Relaxed);
    let order_lookup_requests = state.orders_realtime_requests.load(Ordering::Relaxed);

    // A reduce-only list goes to REST place-order (`batch-place` cannot carry reduce-only); the
    // post-submit lookup finds the post-only order cancelled with `post_only_fill_cancel`
    let cid = ClientOrderId::from("test-confirmed-submit-reject");
    let order = OrderAny::Limit(LimitOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid,
        OrderSide::Buy,
        Quantity::from("0.01"),
        Price::from("1600.00"),
        TimeInForce::Gtc,
        None,
        true,
        true,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ));
    let init = order.init_event().clone();

    cache
        .borrow_mut()
        .add_order(order, None, Some(client_id), false)
        .unwrap();

    let cmd = SubmitOrderList::new(
        trader_id,
        Some(client_id),
        strategy_id,
        OrderList::new(
            OrderListId::from("test-confirmed-reject-list"),
            instrument_id,
            strategy_id,
            vec![cid],
            UnixNanos::default(),
        ),
        vec![init],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order_list(cmd).unwrap();

    let submitted = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for OrderSubmitted")
        .expect("channel closed");
    assert!(
        matches!(submitted, ExecutionEvent::Order(OrderEventAny::Submitted(ref event)) if event.client_order_id == cid),
        "Expected OrderSubmitted for {cid}, was {submitted:?}",
    );

    wait_until_async(
        || async { state.orders_realtime_requests.load(Ordering::Relaxed) > order_lookup_requests },
        Duration::from_secs(5),
    )
    .await;

    let rejected = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for OrderRejected")
        .expect("channel closed");
    let ExecutionEvent::Order(OrderEventAny::Rejected(event)) = rejected else {
        panic!("Expected OrderRejected, was {rejected:?}");
    };

    assert_eq!(event.client_order_id, cid);
    assert_eq!(event.reason.to_string(), "post_only_fill_cancel");
    assert!(!event.reconciliation);
    assert!(event.due_post_only);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_submit_order_list_denies_all_on_invalid_leg() {
    use nautilus_common::messages::execution::SubmitOrderList;
    use nautilus_model::orders::OrderList;

    let (addr, state) = start_test_server().await.unwrap();
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = BitgetExecutionClientConfig {
        api_key: Some("test_api_key".into()),
        api_secret: Some("test_api_secret".into()),
        api_passphrase: Some("test_api_passphrase".into()),
        product_types: vec![BitgetProductType::Linear],
        environment: BitgetEnvironment::Demo,
        base_url_http: Some(format!("http://{addr}")),
        base_url_ws_private: Some(format!("ws://{addr}/v3/ws/private")),
        base_url_ws_trade: Some(format!("ws://{addr}/v3/ws/private/trade")),
        proxy_url: None,
        http_timeout_secs: 10,
        max_retries: 1,
        retry_delay_initial_ms: 100,
        retry_delay_max_ms: 1000,
        heartbeat_interval_secs: 5,
        recv_window_ms: 5000,
        account_id: None,
        use_spot_position_reports: false,
        auto_repay_spot_borrows: false,
        futures_leverages: None,
        position_mode: None,
        margin_mode: None,
        transport_backend: Default::default(),
        ..Default::default()
    };

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    // Drain connection events (account state, subscriptions)
    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    // Valid market order + unsupported TrailingStopMarket order
    let cid1 = ClientOrderId::from("test-deny-order-1");
    let cid2 = ClientOrderId::from("test-deny-order-2");

    let order1 = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid1,
        OrderSide::Buy,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));

    let order2 = OrderAny::TrailingStopMarket(TrailingStopMarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid2,
        OrderSide::Sell,
        Quantity::from("0.01"),
        None,
        Price::from("1500.00"),
        TriggerType::LastPrice,
        rust_decimal::Decimal::new(100, 0),
        TrailingOffsetType::BasisPoints,
        TimeInForce::Gtc,
        None,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ));

    let init1 = order1.init_event().clone();
    let init2 = order2.init_event().clone();

    cache
        .borrow_mut()
        .add_order(order1, None, Some(client_id), false)
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order2, None, Some(client_id), false)
        .unwrap();

    let order_list = OrderList::new(
        OrderListId::from("test-deny-list-1"),
        instrument_id,
        strategy_id,
        vec![cid1, cid2],
        UnixNanos::default(),
    );

    let cmd = SubmitOrderList::new(
        trader_id,
        Some(client_id),
        strategy_id,
        order_list,
        vec![init1, init2],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order_list(cmd).unwrap();

    // The whole list is denied: the offending TrailingStopMarket leg carries the specific
    // UNSUPPORTED_ORDER_TYPE reason while the valid leg renders ORDER_LIST_DENIED.
    let mut denied = Vec::new();

    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(ExecutionEvent::Order(ref event)))
                if event.to_string().contains("OrderDenied") =>
            {
                denied.push(event.to_string());
            }
            _ => break,
        }
    }

    assert_eq!(
        denied.len(),
        2,
        "Both orders should be denied when one leg is invalid"
    );

    let offender = denied
        .iter()
        .find(|text| text.contains("client_order_id=test-deny-order-2"))
        .expect("missing denied event for invalid leg");
    assert!(
        offender.contains("UNSUPPORTED_ORDER_TYPE"),
        "offender reason was: {offender}"
    );

    let sibling = denied
        .iter()
        .find(|text| text.contains("client_order_id=test-deny-order-1"))
        .expect("missing denied event for valid leg");
    assert!(
        sibling.contains("ORDER_LIST_DENIED"),
        "sibling reason was: {sibling}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_submit_order_unsupported_order_type_emits_order_denied() {
    let (addr, state) = start_test_server().await.unwrap();

    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    // Bitget does not support TrailingStopMarket, so the single-order path denies before submit
    let cid = ClientOrderId::from("test-unsupported-order-type");
    let order = OrderAny::TrailingStopMarket(TrailingStopMarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid,
        OrderSide::Buy,
        Quantity::from("0.01"),
        None,
        Price::from("1500.00"),
        TriggerType::LastPrice,
        rust_decimal::Decimal::new(100, 0),
        TrailingOffsetType::BasisPoints,
        TimeInForce::Gtc,
        None,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ));
    let init = order.init_event().clone();

    cache
        .borrow_mut()
        .add_order(order, None, Some(client_id), false)
        .unwrap();

    let cmd = SubmitOrder::new(
        trader_id,
        Some(client_id),
        strategy_id,
        instrument_id,
        cid,
        init,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order(cmd).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for OrderDenied")
        .expect("channel closed");
    let text = match event {
        ExecutionEvent::Order(ref order_event) => order_event.to_string(),
        other => panic!("Expected OrderDenied, was {other:?}"),
    };
    assert!(
        text.contains("OrderDenied") && text.contains("UNSUPPORTED_ORDER_TYPE"),
        "Expected OrderDenied with unsupported-order-type reason, was {text}",
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_submit_conditional_order_on_option_denied() {
    let (addr, state) = start_test_server().await.unwrap();

    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(
        Symbol::from("BTC-25SEP26-86500-C-USDT-OPTION"),
        *BITGET_VENUE,
    );

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    let stop_market = OrderAny::StopMarket(StopMarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        ClientOrderId::from("test-option-conditional-1"),
        OrderSide::Buy,
        Quantity::from("0.01"),
        Price::from("1500.00"),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ));

    let stop_limit = OrderAny::StopLimit(StopLimitOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        ClientOrderId::from("test-option-conditional-2"),
        OrderSide::Buy,
        Quantity::from("0.01"),
        Price::from("1490.00"),
        Price::from("1500.00"),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        false,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ));

    let market_if_touched = OrderAny::MarketIfTouched(MarketIfTouchedOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        ClientOrderId::from("test-option-conditional-3"),
        OrderSide::Sell,
        Quantity::from("0.01"),
        Price::from("1600.00"),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ));

    let limit_if_touched = OrderAny::LimitIfTouched(LimitIfTouchedOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        ClientOrderId::from("test-option-conditional-4"),
        OrderSide::Sell,
        Quantity::from("0.01"),
        Price::from("1590.00"),
        Price::from("1600.00"),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        false,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ));

    // Bitget does not support conditional orders for options, so each order is
    // denied before submission without an OrderSubmitted event
    for order in [stop_market, stop_limit, market_if_touched, limit_if_touched] {
        let cid = order.client_order_id();
        let init = order.init_event().clone();

        cache
            .borrow_mut()
            .add_order(order, None, Some(client_id), false)
            .unwrap();

        let cmd = SubmitOrder::new(
            trader_id,
            Some(client_id),
            strategy_id,
            instrument_id,
            cid,
            init,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None, // correlation_id
        );

        client.submit_order(cmd).unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for OrderDenied")
            .expect("channel closed");
        let text = match event {
            ExecutionEvent::Order(ref order_event) => order_event.to_string(),
            other => panic!("Expected OrderDenied, was {other:?}"),
        };
        assert!(
            text.contains("OrderDenied") && text.contains("UNSUPPORTED_ORDER_TYPE"),
            "Expected OrderDenied with unsupported-order-type reason, was {text}",
        );
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_submit_order_list_denies_conditional_leg_on_option() {
    use nautilus_common::messages::execution::SubmitOrderList;
    use nautilus_model::orders::OrderList;

    let (addr, state) = start_test_server().await.unwrap();

    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(
        Symbol::from("BTC-25SEP26-86500-C-USDT-OPTION"),
        *BITGET_VENUE,
    );

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    // Valid market leg + StopMarket leg on an option instrument
    let cid1 = ClientOrderId::from("test-option-list-order-1");
    let cid2 = ClientOrderId::from("test-option-list-order-2");

    let order1 = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid1,
        OrderSide::Buy,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));
    let order2 = OrderAny::StopMarket(StopMarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid2,
        OrderSide::Sell,
        Quantity::from("0.01"),
        Price::from("1400.00"),
        TriggerType::LastPrice,
        TimeInForce::Gtc,
        None,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ));

    let init1 = order1.init_event().clone();
    let init2 = order2.init_event().clone();

    cache
        .borrow_mut()
        .add_order(order1, None, Some(client_id), false)
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order2, None, Some(client_id), false)
        .unwrap();

    let order_list = OrderList::new(
        OrderListId::from("test-option-list-1"),
        instrument_id,
        strategy_id,
        vec![cid1, cid2],
        UnixNanos::default(),
    );

    let cmd = SubmitOrderList::new(
        trader_id,
        Some(client_id),
        strategy_id,
        order_list,
        vec![init1, init2],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order_list(cmd).unwrap();

    // The whole list is denied: the offending StopMarket leg carries the specific
    // UNSUPPORTED_ORDER_TYPE reason while the valid leg renders ORDER_LIST_DENIED.
    let mut denied = Vec::new();

    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(ExecutionEvent::Order(ref event)))
                if event.to_string().contains("OrderDenied") =>
            {
                denied.push(event.to_string());
            }
            _ => break,
        }
    }

    assert_eq!(
        denied.len(),
        2,
        "Both orders should be denied when one leg is invalid"
    );

    let offender = denied
        .iter()
        .find(|text| text.contains("client_order_id=test-option-list-order-2"))
        .expect("missing denied event for invalid leg");
    assert!(
        offender.contains("UNSUPPORTED_ORDER_TYPE"),
        "offender reason was: {offender}"
    );

    let sibling = denied
        .iter()
        .find(|text| text.contains("client_order_id=test-option-list-order-1"))
        .expect("missing denied event for valid leg");
    assert!(
        sibling.contains("ORDER_LIST_DENIED"),
        "sibling reason was: {sibling}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_submit_order_list_denies_incomplete_when_leg_missing() {
    use nautilus_common::messages::execution::SubmitOrderList;
    use nautilus_model::orders::OrderList;

    let (addr, state) = start_test_server().await.unwrap();
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    // Only the first leg is cached; the second is absent, so the whole list is incomplete
    let cid_present = ClientOrderId::from("test-incomplete-present");
    let cid_missing = ClientOrderId::from("test-incomplete-missing");

    let order_present = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid_present,
        OrderSide::Buy,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));
    let order_missing = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid_missing,
        OrderSide::Sell,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));

    let init_present = order_present.init_event().clone();
    let init_missing = order_missing.init_event().clone();

    cache
        .borrow_mut()
        .add_order(order_present, None, Some(client_id), false)
        .unwrap();

    let order_list = OrderList::new(
        OrderListId::from("test-incomplete-list"),
        instrument_id,
        strategy_id,
        vec![cid_present, cid_missing],
        UnixNanos::default(),
    );

    let cmd = SubmitOrderList::new(
        trader_id,
        Some(client_id),
        strategy_id,
        order_list,
        vec![init_present, init_missing],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order_list(cmd).unwrap();

    // The missing leg cannot emit (absent from the cache); the cached leg reports the list cause
    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for OrderDenied")
        .expect("channel closed");
    let text = match event {
        ExecutionEvent::Order(ref order_event) => order_event.to_string(),
        other => panic!("Expected OrderDenied, was {other:?}"),
    };
    assert!(
        text.contains("client_order_id=test-incomplete-present")
            && text.contains("ORDER_LIST_INCOMPLETE"),
        "Expected ORDER_LIST_INCOMPLETE for the cached leg, was {text}",
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_exec_client_submit_order_list_denies_closed_leg() {
    use nautilus_common::messages::execution::SubmitOrderList;
    use nautilus_model::orders::OrderList;

    let (addr, state) = start_test_server().await.unwrap();
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("BITGET-001");
    let client_id = *BITGET_CLIENT_ID;
    let strategy_id = StrategyId::from("S-001");
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);

    let cache = Rc::new(RefCell::new(Cache::default()));
    add_test_account_to_cache(&cache, account_id);

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        *BITGET_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = create_test_demo_exec_config(addr);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let mut client = BitgetExecutionClient::new(core, config).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();

    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 4 },
        Duration::from_secs(10),
    )
    .await;

    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .is_ok()
    {}

    let cid_open = ClientOrderId::from("test-closed-leg-open");
    let cid_closed = ClientOrderId::from("test-closed-leg-closed");

    let order_open = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid_open,
        OrderSide::Buy,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));
    let mut order_closed = OrderAny::Market(MarketOrder::new(
        trader_id,
        strategy_id,
        instrument_id,
        cid_closed,
        OrderSide::Sell,
        Quantity::from("0.01"),
        TimeInForce::Gtc,
        UUID4::new(),
        UnixNanos::default(),
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ));

    let init_open = order_open.init_event().clone();
    let init_closed = order_closed.init_event().clone();

    // Deny the second leg before submission so it is closed when the list is processed
    order_closed
        .apply(OrderEventAny::Denied(OrderDenied::new(
            trader_id,
            strategy_id,
            instrument_id,
            cid_closed,
            Ustr::from("closed before submission"),
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
        )))
        .unwrap();

    cache
        .borrow_mut()
        .add_order(order_open, None, Some(client_id), false)
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order_closed, None, Some(client_id), false)
        .unwrap();

    let order_list = OrderList::new(
        OrderListId::from("test-closed-leg-list"),
        instrument_id,
        strategy_id,
        vec![cid_open, cid_closed],
        UnixNanos::default(),
    );

    let cmd = SubmitOrderList::new(
        trader_id,
        Some(client_id),
        strategy_id,
        order_list,
        vec![init_open, init_closed],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None, // correlation_id
    );

    client.submit_order_list(cmd).unwrap();

    let mut denied = Vec::new();

    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(ExecutionEvent::Order(ref event)))
                if event.to_string().contains("OrderDenied") =>
            {
                denied.push(event.to_string());
            }
            _ => break,
        }
    }

    assert_eq!(denied.len(), 2, "Both legs should be denied: {denied:?}");

    let offender = denied
        .iter()
        .find(|text| text.contains("client_order_id=test-closed-leg-closed"))
        .expect("missing denied event for closed leg");
    assert!(
        offender.contains("VALIDATION_FAILED") && offender.contains("cannot submit closed order"),
        "closed-leg reason was: {offender}"
    );

    let sibling = denied
        .iter()
        .find(|text| text.contains("client_order_id=test-closed-leg-open"))
        .expect("missing denied event for open leg");
    assert!(
        sibling.contains("ORDER_LIST_DENIED"),
        "sibling reason was: {sibling}"
    );

    client.disconnect().await.unwrap();
}

async fn connect_test_client(
    config: BitgetExecutionClientConfig,
    state: &TestServerState,
) -> (
    BitgetExecutionClient,
    tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    let (mut client, mut rx, cache) = create_test_execution_client_with_config(config);
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    client.start().unwrap();
    client.connect().await.unwrap();
    wait_until_async(
        || async { state.subscriptions.lock().await.len() >= 5 },
        Duration::from_secs(10),
    )
    .await;
    drain_execution_events(&mut rx).await;
    (client, rx, cache)
}

fn tsla_limit_order(
    client_order_id: &str,
    price: &str,
    time_in_force: TimeInForce,
    post_only: bool,
) -> OrderAny {
    OrderAny::Limit(LimitOrder::new(
        TraderId::from("TESTER-001"),
        StrategyId::from("S-001"),
        InstrumentId::from("TSLAUSDT-PERP.BITGET"),
        ClientOrderId::from(client_order_id),
        OrderSide::Buy,
        Quantity::from("0.02"),
        Price::from(price),
        time_in_force,
        None,
        post_only,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    ))
}

fn submit_cached(
    client: &BitgetExecutionClient,
    cache: &Rc<RefCell<Cache>>,
    order: OrderAny,
) -> ClientOrderId {
    let client_order_id = order.client_order_id();
    let command = SubmitOrder::new(
        order.trader_id(),
        Some(*BITGET_CLIENT_ID),
        order.strategy_id(),
        order.instrument_id(),
        client_order_id,
        order.init_event().clone(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    cache
        .borrow_mut()
        .add_order(order, None, Some(*BITGET_CLIENT_ID), false)
        .unwrap();
    client.submit_order(command).unwrap();
    client_order_id
}

async fn next_order_event(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
) -> OrderEventAny {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let ExecutionEvent::Order(event) = rx.recv().await.expect("channel closed") {
                return event;
            }
        }
    })
    .await
    .expect("timed out waiting for an order event")
}

// Verified on demo: a crossing post-only order is acked `code "0"` and then pushed `cancelled`
// with `post_only_fill_cancel`, with no `new` before it
#[rstest]
#[tokio::test]
async fn test_exec_client_post_only_cross_emits_rejected_due_post_only() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) =
        connect_test_client(create_test_exec_config(addr), &state).await;
    *state.ack_order_id.lock().await = Some("1503520295321231360".to_string());

    let cid = submit_cached(
        &client,
        &cache,
        tsla_limit_order("tdcapcross93093", "360.72", TimeInForce::Gtc, true),
    );
    assert!(matches!(
        next_order_event(&mut rx).await,
        OrderEventAny::Submitted(_)
    ));
    wait_until_async(
        || async { state.trade_order_requests.load(Ordering::Relaxed) == 1 },
        Duration::from_secs(5),
    )
    .await;
    let payloads = state.trade_order_payloads.lock().await;
    assert_eq!(payloads[0]["args"][0]["timeInForce"], "post_only");
    drop(payloads);

    state
        .private_pushes
        .lock()
        .await
        .push(load_test_data("ws_account_order_post_only_rejected.json"));
    let event = next_order_event(&mut rx).await;

    let OrderEventAny::Rejected(rejected) = event else {
        panic!("Expected OrderRejected, was {event:?}");
    };
    assert_eq!(rejected.client_order_id, cid);
    assert!(rejected.due_post_only);
    client.disconnect().await.unwrap();
}

// Fills come only from the `fill` topic; a replayed `execId` is dropped and the commission is
// the signed sum of `feeDetail`
#[rstest]
#[tokio::test]
async fn test_exec_client_fill_dedupes_exec_id_and_sums_fee() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) =
        connect_test_client(create_test_exec_config(addr), &state).await;
    *state.ack_order_id.lock().await = Some("1503520323393708032".to_string());

    let cid = submit_cached(
        &client,
        &cache,
        tsla_limit_order("tdcapioc93099", "361.44", TimeInForce::Ioc, false),
    );
    assert!(matches!(
        next_order_event(&mut rx).await,
        OrderEventAny::Submitted(_)
    ));

    // Demo push order for an IOC take: `order` filled first, then the `fill` (replayed here)
    let fill = load_test_data("ws_account_execution.json");
    state.private_pushes.lock().await.extend([
        load_test_data("ws_account_order_filled.json"),
        fill.clone(),
        fill,
    ]);

    // The order topic carries no fills, so acceptance is ensured before the fill event
    let event = next_order_event(&mut rx).await;
    assert!(
        matches!(event, OrderEventAny::Accepted(_)),
        "Expected OrderAccepted, was {event:?}"
    );
    let event = next_order_event(&mut rx).await;
    let OrderEventAny::Filled(filled) = event else {
        panic!("Expected OrderFilled, was {event:?}");
    };
    assert_eq!(filled.client_order_id, cid);
    assert_eq!(filled.trade_id.as_str(), "1503520323400871936");
    assert_eq!(filled.last_qty, Quantity::from("0.02"));
    assert_eq!(filled.last_px, Price::from("360.72"));
    assert_eq!(filled.commission, Some(Money::from("0.00288576 USDT")));

    let duplicate = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if let ExecutionEvent::Order(OrderEventAny::Filled(event)) =
                rx.recv().await.expect("channel closed")
            {
                return event;
            }
        }
    })
    .await;
    assert!(duplicate.is_err(), "duplicate execId must not fill twice");
    client.disconnect().await.unwrap();
}

// Orders wait for the trade socket to (re-)login and are rejected locally, never sent, once the
// auth wait expires (10 s by default, shortened here)
#[rstest]
#[tokio::test]
async fn test_exec_client_order_queued_until_login_then_rejected_locally() {
    let (addr, state) = start_test_server().await.unwrap();
    let mut config = create_test_exec_config(addr);
    config.auth_timeout_secs = Some(1);
    let registry = SocketReconnectRegistry::default();
    let (mut client, mut rx, cache) = registry.scope(|| {
        let (client, rx, cache) = create_test_execution_client_with_config(config);
        (client, rx, cache)
    });
    add_test_account_to_cache(&cache, AccountId::from("BITGET-001"));
    client.start().unwrap();
    client.connect().await.unwrap();
    drain_execution_events(&mut rx).await;
    let logins = state.login_count.load(Ordering::Relaxed);
    assert_eq!(logins, 2, "both private sockets log in");

    state.withhold_login.store(true, Ordering::Relaxed);
    let handle = registry
        .handle(*BITGET_CLIENT_ID, Ustr::from("bitget-trading"))
        .unwrap();
    assert_eq!(
        handle.request_reconnect(),
        SocketReconnectRequestOutcome::Accepted
    );
    wait_until_async(
        || async { state.login_count.load(Ordering::Relaxed) > logins },
        Duration::from_secs(10),
    )
    .await;

    let cid = submit_cached(
        &client,
        &cache,
        tsla_limit_order("test-queued-order", "342.62", TimeInForce::Gtc, true),
    );
    assert!(matches!(
        next_order_event(&mut rx).await,
        OrderEventAny::Submitted(_)
    ));
    let event = next_order_event(&mut rx).await;

    let OrderEventAny::Rejected(rejected) = event else {
        panic!("Expected OrderRejected, was {event:?}");
    };
    assert_eq!(rejected.client_order_id, cid);
    assert!(
        rejected.reason.as_str().contains("authenticated"),
        "unexpected reason: {}",
        rejected.reason
    );
    assert_eq!(state.trade_order_requests.load(Ordering::Relaxed), 0);
    client.disconnect().await.unwrap();
}

// A conditional modify the venue refuses is a confirmed rejection
#[rstest]
#[tokio::test]
async fn test_exec_client_conditional_modify_venue_reject_emits_modify_rejected() {
    let (addr, state) = start_test_server().await.unwrap();
    let (mut client, mut rx, cache) =
        connect_test_client(create_test_exec_config(addr), &state).await;
    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);
    let client_order_id = ClientOrderId::from("test-modify-rejected");
    add_stop_market_to_cache(
        &cache,
        StrategyId::from("S-001"),
        instrument_id,
        client_order_id,
    );
    state.modify_order_status.store(400, Ordering::Relaxed);

    client
        .modify_order(ModifyOrder::new(
            TraderId::from("TESTER-001"),
            Some(*BITGET_CLIENT_ID),
            StrategyId::from("S-001"),
            instrument_id,
            client_order_id,
            Some(VenueOrderId::from("test-order-id-12345")),
            None,
            None,
            Some(Price::from("1550.00")),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    let event = next_order_event(&mut rx).await;

    let OrderEventAny::ModifyRejected(rejected) = event else {
        panic!("Expected OrderModifyRejected, was {event:?}");
    };
    assert_eq!(rejected.client_order_id, client_order_id);
    assert!(rejected.reason.as_str().contains("25571"));
    // The venue requires `qty` on every modify, so the cached quantity fills the gap
    let payloads = state.modify_order_payloads.lock().await;
    assert_eq!(payloads[0]["qty"], "0.01");
    assert_eq!(payloads[0]["triggerPrice"], "1550.00");
    drop(payloads);
    client.disconnect().await.unwrap();
}
