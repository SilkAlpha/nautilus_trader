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

//! Integration tests for `BitgetDataClient`.
//!
//! These tests verify the full data flow from WebSocket messages through
//! parsing to event emission via the data event channel.

use std::{
    collections::HashMap,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{IntoResponse, Json, Response},
    routing::get,
};
use nautilus_bitget::{
    common::{
        consts::{BITGET_CLIENT_ID, BITGET_VENUE},
        enums::{BitgetEnvironment, BitgetProductType},
    },
    config::BitgetDataClientConfig,
    data::BitgetDataClient,
    data_types::BitgetLiquidation,
};
use nautilus_common::{
    clients::DataClient,
    live::runner::{replace_system_event_sender, set_data_event_sender},
    messages::{
        DataEvent, SystemEvent,
        data::{
            DataResponse, RequestBars, RequestBookSnapshot, RequestFundingRates, RequestInstrument,
            RequestInstruments, RequestOptionChainReferencePrice, SubscribeBars,
            SubscribeBookDeltas, SubscribeCustomData, SubscribeQuotes, SubscribeTrades,
            UnsubscribeBookDeltas, UnsubscribeCustomData, UnsubscribeQuotes,
        },
        system::SocketState,
    },
    testing::wait_until_async,
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{SocketReconnectRegistry, SocketReconnectRequestOutcome};
use nautilus_model::{
    data::{BarType, CustomData, Data, DataType, OrderBookDeltas, QuoteTick},
    enums::{BookAction, BookType, PositionSide, RecordFlag},
    identifiers::{InstrumentId, OptionSeriesId},
    types::{Price, Quantity},
};
use nautilus_network::http::HttpClient;
use rstest::rstest;
use serde_json::{Value, json};
use ustr::Ustr;

#[derive(Clone)]
struct TestServerState {
    connection_count: Arc<tokio::sync::Mutex<usize>>,
    upgrade_count: Arc<AtomicUsize>,
    instrument_requests: Arc<AtomicUsize>,
    orderbook_requests: Arc<AtomicUsize>,
    upgrade_gate: tokio::sync::watch::Sender<bool>,
    socket_drops: Arc<tokio::sync::Mutex<HashMap<usize, tokio::sync::oneshot::Sender<()>>>>,
    subscriptions: Arc<tokio::sync::Mutex<Vec<String>>>,
    subscription_events: Arc<tokio::sync::Mutex<Vec<(String, bool)>>>,
    disconnect_trigger: Arc<AtomicBool>,
    ping_count: Arc<AtomicUsize>,
    ticker_queries: Arc<tokio::sync::Mutex<Vec<HashMap<String, String>>>>,
    ticker_response: Arc<tokio::sync::Mutex<Option<Value>>>,
    book_updates: Arc<tokio::sync::Mutex<Vec<Value>>>,
    book_snapshot_drops: Arc<AtomicUsize>,
    liquidation_frame: Arc<tokio::sync::Mutex<Option<Value>>>,
}

impl Default for TestServerState {
    fn default() -> Self {
        Self {
            connection_count: Arc::new(tokio::sync::Mutex::new(0)),
            upgrade_count: Arc::new(AtomicUsize::new(0)),
            instrument_requests: Arc::new(AtomicUsize::new(0)),
            orderbook_requests: Arc::new(AtomicUsize::new(0)),
            upgrade_gate: tokio::sync::watch::channel(true).0,
            socket_drops: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            subscriptions: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            subscription_events: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            disconnect_trigger: Arc::new(AtomicBool::new(false)),
            ping_count: Arc::new(AtomicUsize::new(0)),
            ticker_queries: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            ticker_response: Arc::new(tokio::sync::Mutex::new(None)),
            book_updates: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            book_snapshot_drops: Arc::new(AtomicUsize::new(0)),
            liquidation_frame: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

async fn wait_for_socket_registrations(state: &TestServerState, expected: &[usize]) {
    wait_until_async(
        || async {
            let sockets = state.socket_drops.lock().await;
            sockets.len() == expected.len() && expected.iter().all(|id| sockets.contains_key(id))
        },
        Duration::from_secs(5),
    )
    .await;
}

fn load_test_data(filename: &str) -> Value {
    let path = format!("test_data/{filename}");
    let content = std::fs::read_to_string(path).expect("Failed to read test data");
    serde_json::from_str(&content).expect("Failed to parse test data")
}

async fn handle_get_instruments(
    State(state): State<TestServerState>,
    query: Query<HashMap<String, String>>,
) -> impl IntoResponse {
    state.instrument_requests.fetch_add(1, Ordering::Relaxed);
    let category = query.get("category").map(String::as_str);
    let filename = match category {
        Some("USDT-FUTURES") => "http_get_instruments_linear.json",
        Some("SPOT") => "http_get_instruments_spot.json",
        Some("COIN-FUTURES") => "http_get_instruments_inverse.json",
        _ => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({
                    "code": "40034",
                    "msg": "Parameter category error",
                    "requestTime": 1794192837141_i64,
                    "data": null
                })),
            )
                .into_response();
        }
    };

    let instruments = load_test_data(filename);
    Json(instruments).into_response()
}

async fn handle_get_fee_rate() -> impl IntoResponse {
    let fee_rate = load_test_data("http_get_fee_rate.json");
    Json(fee_rate).into_response()
}

async fn handle_get_server_time() -> impl IntoResponse {
    Json(load_test_data("http_get_server_time.json"))
}

async fn handle_get_orderbook(State(state): State<TestServerState>) -> impl IntoResponse {
    state.orderbook_requests.fetch_add(1, Ordering::Relaxed);
    let orderbook = load_test_data("http_get_orderbook.json");
    Json(orderbook).into_response()
}

async fn handle_get_klines(Query(query): Query<HashMap<String, String>>) -> impl IntoResponse {
    assert_eq!(
        query.get("category").map(String::as_str),
        Some("USDT-FUTURES")
    );
    assert_eq!(query.get("symbol").map(String::as_str), Some("BTCUSDT"));
    assert_eq!(query.get("interval").map(String::as_str), Some("5m"));
    Json(load_test_data("http_get_klines_linear.json"))
}

async fn handle_get_funding_history() -> impl IntoResponse {
    let funding = load_test_data("http_get_funding_history.json");
    Json(funding).into_response()
}

async fn handle_get_tickers(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    state.ticker_queries.lock().await.push(query);
    Json(
        state
            .ticker_response
            .lock()
            .await
            .clone()
            .expect("ticker response must be configured"),
    )
}

async fn handle_websocket(ws: WebSocketUpgrade, State(state): State<TestServerState>) -> Response {
    let mut gate = state.upgrade_gate.subscribe();
    gate.wait_for(|open| *open).await.unwrap();
    ws.on_upgrade(move |socket| async move {
        let socket_id = state.upgrade_count.fetch_add(1, Ordering::Relaxed) + 1;
        let (drop_tx, drop_rx) = tokio::sync::oneshot::channel();
        state.socket_drops.lock().await.insert(socket_id, drop_tx);
        handle_socket(socket, state, socket_id, drop_rx).await;
    })
}

// Dotted topic key the client tracks for a Bitget `arg` object
fn arg_key(arg: &Value) -> String {
    let topic = arg["topic"].as_str().unwrap_or_default();
    match (
        arg.get("interval").and_then(Value::as_str),
        arg.get("symbol").and_then(Value::as_str),
    ) {
        (Some(interval), Some(symbol)) => format!("{topic}.{interval}.{symbol}"),
        (None, Some(symbol)) => format!("{topic}.{symbol}"),
        _ => topic.to_string(),
    }
}

fn with_arg(mut frame: Value, arg: &Value) -> Value {
    frame["arg"] = arg.clone();
    frame
}

fn interval_ms(interval: &str) -> u64 {
    let (value, unit) = interval.split_at(interval.len() - 1);
    let value: u64 = value.parse().unwrap();
    value
        * match unit {
            "m" => 60_000,
            "H" => 3_600_000,
            "D" => 86_400_000,
            _ => panic!("unexpected interval {interval}"),
        }
}

// The captured all-symbol liquidation frame re-targeted to the subscribed symbol
fn liquidation_frame(symbol: &str) -> Value {
    let mut frame = load_test_data("ws_all_liquidation.json");
    frame["data"][0]["symbol"] = symbol.into();
    frame["data"][0]["side"] = "buy".into();
    frame["data"][0]["price"] = "96250.5".into();
    frame["data"][0]["amount"] = "0.015".into();
    frame["data"][0]["ts"] = "1739502302929".into();
    frame["data"][1]["symbol"] = symbol.into();
    frame["data"][1]["side"] = "sell".into();
    frame["data"][1]["price"] = "97410.0".into();
    frame["data"][1]["amount"] = "1.250".into();
    frame["data"][1]["ts"] = "1739502303011".into();
    frame
}

async fn handle_socket(
    mut socket: WebSocket,
    state: TestServerState,
    socket_id: usize,
    mut drop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    {
        let mut count = state.connection_count.lock().await;
        *count += 1;
    }

    loop {
        if state.disconnect_trigger.load(Ordering::Relaxed) {
            break;
        }

        for update in std::mem::take(&mut *state.book_updates.lock().await) {
            let topic = arg_key(&update["arg"]);
            if state.subscriptions.lock().await.contains(&topic) {
                socket
                    .send(Message::Text(update.to_string().into()))
                    .await
                    .unwrap();
            }
        }

        let received = tokio::select! {
            _ = &mut drop_rx => break,
            received = tokio::time::timeout(Duration::from_millis(50), socket.recv()) => received,
        };
        let msg_opt = match received {
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

                match op {
                    Some("subscribe") => {
                        for arg in &args {
                            let topic = arg_key(arg);
                            state
                                .subscription_events
                                .lock()
                                .await
                                .push((topic.clone(), true));

                            let mut subs = state.subscriptions.lock().await;
                            if !subs.contains(&topic) {
                                subs.push(topic);
                            }
                        }

                        let mut failed = false;
                        for arg in &args {
                            let ack = json!({
                                "event": "subscribe",
                                "arg": arg,
                                "connId": "test-conn-id"
                            });
                            if socket
                                .send(Message::Text(ack.to_string().into()))
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

                        let Some(arg) = args.first() else {
                            continue;
                        };
                        let channel = arg["topic"].as_str().unwrap_or_default();
                        let symbol = arg["symbol"].as_str().unwrap_or_default();

                        let frames = match channel {
                            "publicTrade" => {
                                vec![with_arg(load_test_data("ws_public_trade.json"), arg)]
                            }
                            // `books1` pushes nothing on subscribe; the client seeds from REST
                            "books1" => vec![],
                            "books5" | "books50" | "books" => {
                                // Answers the subscribe without its snapshot
                                if state
                                    .book_snapshot_drops
                                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |drops| {
                                        drops.checked_sub(1)
                                    })
                                    .is_ok()
                                {
                                    continue;
                                }

                                let fixture = if channel == "books" {
                                    "ws_orderbook_full_snapshot.json"
                                } else {
                                    "ws_orderbook_snapshot.json"
                                };
                                vec![with_arg(load_test_data(fixture), arg)]
                            }
                            "ticker" => {
                                vec![with_arg(load_test_data("ws_ticker_linear.json"), arg)]
                            }
                            "kline" => {
                                // Bitget pushes the forming candle; the next start closes it
                                let first = with_arg(load_test_data("ws_kline.json"), arg);
                                let start: u64 =
                                    first["data"][0]["start"].as_str().unwrap().parse().unwrap();
                                let step = interval_ms(arg["interval"].as_str().unwrap());
                                let mut next = first.clone();
                                next["data"][0]["start"] = (start + step).to_string().into();
                                vec![first, next]
                            }
                            "liquidation" => {
                                let frame = state
                                    .liquidation_frame
                                    .lock()
                                    .await
                                    .clone()
                                    .unwrap_or_else(|| liquidation_frame(symbol));
                                vec![with_arg(frame, arg)]
                            }
                            _ => vec![],
                        };

                        for frame in frames {
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
                    Some("unsubscribe") => {
                        for arg in &args {
                            let topic = arg_key(arg);
                            let mut events = state.subscription_events.lock().await;
                            events.retain(|(t, _)| *t != topic);
                            drop(events);

                            let mut subs = state.subscriptions.lock().await;
                            subs.retain(|s| *s != topic);
                        }

                        let mut failed = false;
                        for arg in &args {
                            let ack = json!({
                                "event": "unsubscribe",
                                "arg": arg,
                                "connId": "test-conn-id"
                            });
                            if socket
                                .send(Message::Text(ack.to_string().into()))
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
                    _ => {}
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

    state.socket_drops.lock().await.remove(&socket_id);
    let mut count = state.connection_count.lock().await;
    *count = count.saturating_sub(1);
}

fn create_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/api/v3/market/instruments", get(handle_get_instruments))
        .route("/api/v3/market/orderbook", get(handle_get_orderbook))
        .route("/api/v3/market/candles", get(handle_get_klines))
        .route(
            "/api/v3/market/history-fund-rate",
            get(handle_get_funding_history),
        )
        .route("/api/v3/market/tickers", get(handle_get_tickers))
        .route("/api/v3/account/all-fee-rate", get(handle_get_fee_rate))
        .route("/api/v3/market/time", get(handle_get_server_time))
        .route("/v3/ws/public", get(handle_websocket))
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

fn create_test_config(addr: SocketAddr) -> BitgetDataClientConfig {
    BitgetDataClientConfig {
        api_key: None,
        api_secret: None,
        api_passphrase: None,
        product_types: vec![BitgetProductType::Linear],
        environment: BitgetEnvironment::Mainnet,
        base_url_http: Some(format!("http://{addr}")),
        base_url_ws_public: Some(format!("ws://{addr}/v3/ws/public")),
        base_url_ws_private: None,
        proxy_url: None,
        http_timeout_secs: 10,
        max_retries: 1,
        retry_delay_initial_ms: 100,
        retry_delay_max_ms: 1000,
        heartbeat_interval_secs: 5,
        recv_window_ms: 5000,
        update_instruments_interval_mins: None,
        instrument_poll_interval_secs: None,
        book_snapshot_timeout_secs: 10,
        transport_backend: Default::default(),
        bars_timestamp_on_close: true,
    }
}

fn liquidation_data_type(instrument_id: Option<&str>, identifier: Option<&str>) -> DataType {
    let metadata = instrument_id.map(|instrument_id| {
        let mut metadata = Params::new();
        metadata.insert(
            "instrument_id".to_string(),
            Value::String(instrument_id.to_string()),
        );
        metadata
    });

    DataType::new(
        "BitgetLiquidation",
        metadata,
        identifier.map(ToString::to_string),
    )
}

fn subscribe_custom_data(data_type: DataType) -> SubscribeCustomData {
    SubscribeCustomData::new(
        Some(*BITGET_CLIENT_ID),
        None,
        data_type,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn unsubscribe_custom_data(data_type: DataType) -> UnsubscribeCustomData {
    UnsubscribeCustomData::new(
        Some(*BITGET_CLIENT_ID),
        None,
        data_type,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

async fn recv_custom_data(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    count: usize,
) -> Vec<CustomData> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        while events.len() < count {
            if let DataEvent::Data(Data::Custom(custom)) = rx.recv().await.unwrap() {
                events.push(custom);
            }
        }

        events
    })
    .await
    .expect("timeout waiting for custom data")
}

fn as_liquidation(custom: &CustomData) -> &BitgetLiquidation {
    custom
        .data
        .as_any()
        .downcast_ref::<BitgetLiquidation>()
        .expect("expected BitgetLiquidation")
}

async fn wait_for_server_topics(state: &TestServerState, expected: &[&str]) {
    wait_until_async(
        || async { *state.subscriptions.lock().await == expected },
        Duration::from_secs(5),
    )
    .await;
}

// Bitget lists no options, so the option-chain reference price resolves to `None` without a
// venue request
#[rstest]
#[tokio::test]
async fn test_option_chain_reference_price_response() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    let series_id = OptionSeriesId::new_derived(
        *BITGET_VENUE,
        Ustr::from("BTC"),
        Ustr::from("USDC"),
        UnixNanos::from(1_743_120_000_000_000_000),
    );
    let instrument_id = InstrumentId::from("BTC-28MAR25-90000-C-OPTION.BITGET");
    let mut params = Params::new();
    params.insert("test-case".to_string(), json!(0));
    let request = RequestOptionChainReferencePrice::new(
        series_id,
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        UUID4::new(),
        UnixNanos::from(42),
        Some(params.clone()),
    );
    let request_id = request.request_id;
    let clock = get_atomic_clock_realtime();
    let before_ns = clock.get_time_ns();

    client
        .request_option_chain_reference_price(request)
        .expect("request option-chain reference price");
    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for option-chain reference price")
        .expect("data event channel closed");
    let after_ns = clock.get_time_ns();
    let DataEvent::Response(DataResponse::OptionChainReferencePrice(response)) = event else {
        panic!("expected option-chain reference price response, received {event:?}");
    };

    assert_eq!(response.correlation_id, request_id);
    assert_eq!(response.client_id, *BITGET_CLIENT_ID);
    assert_eq!(response.series_id, series_id);
    assert_eq!(response.price, None);
    assert!(response.ts_init >= before_ns && response.ts_init <= after_ns);
    assert_eq!(response.params, Some(params));
    assert!(state.ticker_queries.lock().await.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_data_client_connect_disconnect() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let (system_tx, mut system_rx) = tokio::sync::mpsc::unbounded_channel();
    replace_system_event_sender(system_tx);

    let config = create_test_config(addr);
    let registry = SocketReconnectRegistry::default();
    let mut client = registry
        .scope(|| BitgetDataClient::new(*BITGET_CLIENT_ID, config))
        .unwrap();
    assert!(!client.is_connected());

    client.connect().await.unwrap();
    assert!(client.is_connected());

    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;
    let event = tokio::time::timeout(Duration::from_secs(2), system_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let SystemEvent::SocketState(change) = event;
    let endpoint = Ustr::from("bitget-linear-data-streams");
    let handle = registry.handle(*BITGET_CLIENT_ID, endpoint).unwrap();

    assert_eq!(*state.connection_count.lock().await, 1);
    assert_eq!(change.client_id, *BITGET_CLIENT_ID);
    assert_eq!(change.venue, Some(*BITGET_VENUE));
    assert_eq!(change.endpoint, endpoint);
    assert_eq!(change.state, SocketState::Connected);
    assert_eq!(
        handle.request_reconnect(),
        SocketReconnectRequestOutcome::Accepted
    );
    let event = tokio::time::timeout(Duration::from_secs(2), system_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let SystemEvent::SocketState(change) = event;
    assert_eq!(change.endpoint, endpoint);
    assert_eq!(change.state, SocketState::Disconnected);

    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    assert!(registry.handle(*BITGET_CLIENT_ID, endpoint).is_none());
}

#[rstest]
#[case::linear(vec![BitgetProductType::Linear], 1)]
#[case::linear_and_spot_drop_first(vec![BitgetProductType::Linear, BitgetProductType::Spot], 1)]
#[case::linear_and_spot_drop_second(vec![BitgetProductType::Linear, BitgetProductType::Spot], 2)]
#[tokio::test]
async fn test_data_client_transport_reconnect(
    #[case] product_types: Vec<BitgetProductType>,
    #[case] dropped_socket: usize,
) {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let (system_tx, mut system_rx) = tokio::sync::mpsc::unbounded_channel();
    replace_system_event_sender(system_tx);
    let registry = SocketReconnectRegistry::default();
    let mut config = create_test_config(addr);
    let socket_count = product_types.len();
    config.product_types = product_types;
    let mut client = registry
        .scope(|| BitgetDataClient::new(*BITGET_CLIENT_ID, config))
        .unwrap();
    assert!(!client.is_connected());
    assert!(client.is_disconnected());
    client.connect().await.unwrap();

    let mut endpoints = Vec::new();

    for _ in 0..socket_count {
        let event = tokio::time::timeout(Duration::from_secs(5), system_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let SystemEvent::SocketState(change) = event;
        assert_eq!(change.state, SocketState::Connected);
        endpoints.push(change.endpoint);
    }
    let mut registered_sockets: Vec<_> = (1..=socket_count).collect();
    wait_for_socket_registrations(&state, &registered_sockets).await;
    assert!(client.is_connected());
    assert!(!client.is_disconnected());
    assert_eq!(state.upgrade_count.load(Ordering::Relaxed), socket_count);
    let instrument_requests = state.instrument_requests.load(Ordering::Relaxed);
    assert_eq!(instrument_requests, socket_count);

    state.upgrade_gate.send_replace(false);
    state
        .socket_drops
        .lock()
        .await
        .remove(&dropped_socket)
        .unwrap()
        .send(())
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), system_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let SystemEvent::SocketState(change) = event;
    assert_eq!(change.state, SocketState::Disconnected);
    let dropped_endpoint = change.endpoint;
    assert!(endpoints.contains(&dropped_endpoint));
    assert!(
        !client.is_connected(),
        "transport loss must clear aggregate availability"
    );
    assert!(client.is_disconnected());
    registered_sockets.retain(|id| *id != dropped_socket);
    wait_for_socket_registrations(&state, &registered_sockets).await;
    assert_eq!(state.socket_drops.lock().await.len(), socket_count - 1);

    tokio::time::timeout(Duration::from_secs(2), client.connect())
        .await
        .expect("connect mid-reconnect must reuse the established session")
        .unwrap();
    assert!(!client.is_connected());
    assert!(client.is_disconnected());
    assert_eq!(
        state.instrument_requests.load(Ordering::Relaxed),
        instrument_requests
    );

    state.upgrade_gate.send_replace(true);
    let event = tokio::time::timeout(Duration::from_secs(10), system_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let SystemEvent::SocketState(change) = event;
    assert_eq!(change.endpoint, dropped_endpoint);
    assert_eq!(change.state, SocketState::Connected);
    wait_until_async(|| async { client.is_connected() }, Duration::from_secs(5)).await;
    assert!(!client.is_disconnected());
    registered_sockets.push(socket_count + 1);
    wait_for_socket_registrations(&state, &registered_sockets).await;
    assert_eq!(
        state.upgrade_count.load(Ordering::Relaxed),
        socket_count + 1
    );
    assert_eq!(state.socket_drops.lock().await.len(), socket_count);
    assert_eq!(
        state.instrument_requests.load(Ordering::Relaxed),
        instrument_requests
    );

    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    assert!(client.is_disconnected());
    for endpoint in endpoints {
        assert!(registry.handle(*BITGET_CLIENT_ID, endpoint).is_none());
    }
}

#[rstest]
#[tokio::test]
async fn test_data_client_subscribe_trades() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let cmd = SubscribeTrades::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    client.subscribe_trades(cmd).unwrap();

    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for event")
        .expect("channel closed");

    assert!(
        matches!(event, DataEvent::Data(Data::Trade(_))),
        "Expected Trade event, was: {event:?}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_subscribe_quotes_linear() {
    let (addr, state) = start_test_server().await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let cmd = SubscribeQuotes::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    client.subscribe_quotes(cmd).unwrap();

    wait_until_async(
        || async {
            state
                .subscription_events
                .lock()
                .await
                .iter()
                .any(|(topic, subscribed)| topic == "books1.BTCUSDT" && *subscribed)
        },
        Duration::from_secs(5),
    )
    .await;

    // `books1` pushes nothing on subscribe, so the first quote is seeded from REST limit=1
    let quote = next_quote(&mut rx, UnixNanos::new(1_794_192_836_072_000_000)).await;
    assert_eq!(quote.instrument_id, instrument_id);
    assert_eq!(quote.bid_price, Price::from("75228.8"));
    assert_eq!(quote.ask_price, Price::from("75228.9"));
    assert_eq!(quote.bid_size, Quantity::from("4.1459"));
    assert_eq!(quote.ask_size, Quantity::from("2.6781"));

    // Later `books1` snapshots stream quotes
    let mut books1 = load_test_data("ws_orderbook_books1.json");
    books1["ts"] = 1_794_192_890_496_u64.into();
    state.book_updates.lock().await.push(books1);
    let quote = next_quote(&mut rx, UnixNanos::new(1_794_192_890_495_000_000)).await;
    assert_eq!(quote.bid_price, Price::from("75242.8"));
    assert_eq!(quote.ask_price, Price::from("75242.9"));

    client.disconnect().await.unwrap();
}

// `books1` stays silent after a reconnect too, so every reconnect reseeds the quote from REST
#[rstest]
#[tokio::test]
async fn test_data_client_reconnect_reseeds_quotes() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    wait_for_socket_registrations(&state, &[1]).await;
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let seed_ts = UnixNanos::new(1_794_192_836_072_000_000);

    client
        .subscribe_quotes(SubscribeQuotes::new(
            instrument_id,
            Some(*BITGET_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    next_quote(&mut rx, seed_ts).await;
    assert_eq!(state.orderbook_requests.load(Ordering::Relaxed), 1);

    state
        .socket_drops
        .lock()
        .await
        .remove(&1)
        .unwrap()
        .send(())
        .unwrap();
    wait_for_socket_registrations(&state, &[2]).await;
    let quote = next_quote(&mut rx, seed_ts).await;

    assert_eq!(quote.instrument_id, instrument_id);
    assert_eq!(state.orderbook_requests.load(Ordering::Relaxed), 2);
    assert!(
        state
            .subscriptions
            .lock()
            .await
            .iter()
            .any(|topic| topic == "books1.BTCUSDT")
    );
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_subscribe_book_deltas() {
    let (addr, state) = start_test_server().await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let cmd = SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        false,
        None,
        None,
    );
    client.subscribe_book_deltas(cmd).unwrap();

    wait_until_async(
        || async {
            state
                .subscription_events
                .lock()
                .await
                .iter()
                .any(|(topic, _)| topic.starts_with("books"))
        },
        Duration::from_secs(5),
    )
    .await;

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for event")
        .expect("channel closed");

    assert!(
        matches!(event, DataEvent::Data(Data::BookDeltas(_))),
        "Expected Deltas event, was: {event:?}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_book_gap_resubscribes_for_fresh_snapshot() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");

    // Full depth subscribes `books`, the only channel with snapshot + update sequencing
    let book = SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        NonZeroUsize::new(1000),
        false,
        None,
        None,
    );

    // The server answers every subscribe with the `books` snapshot whose seq is 1207360465262;
    // the captured update links to it through pseq, so a larger pseq is a gap
    let mut gapped = load_test_data("ws_orderbook_delta.json");
    gapped["data"][0]["pseq"] = 1_207_360_465_263_u64.into();
    gapped["data"][0]["ts"] = "1709891700000".into();
    let mut linked = load_test_data("ws_orderbook_delta.json");
    linked["data"][0]["ts"] = "1709891701000".into();

    client.subscribe_book_deltas(book).unwrap();
    let initial = next_book_deltas(&mut rx).await;
    state.book_updates.lock().await.push(gapped);
    let resynced = next_book_deltas(&mut rx).await;
    state.book_updates.lock().await.push(linked);
    let update = next_book_deltas(&mut rx).await;

    for snapshot in [&initial, &resynced] {
        assert_eq!(snapshot.deltas[0].action, BookAction::Clear);
        assert!(
            snapshot
                .deltas
                .iter()
                .all(|delta| RecordFlag::F_SNAPSHOT.matches(delta.flags))
        );
    }

    assert_eq!(update.ts_event, UnixNanos::new(1_709_891_701_000_000_000));
    let actions: Vec<_> = update.deltas.iter().map(|delta| delta.action).collect();
    assert_eq!(
        actions
            .iter()
            .filter(|action| **action == BookAction::Delete)
            .count(),
        4,
    );
    assert_eq!(
        actions
            .iter()
            .filter(|action| **action == BookAction::Update)
            .count(),
        11,
    );
    client.disconnect().await.unwrap();
}

// A dropped snapshot must lead to another subscribe within the retry budget, well before the
// one-minute interval that follows it
#[rstest]
#[case::initial_snapshot(false)]
#[case::replacement_snapshot(true)]
#[tokio::test]
async fn test_data_client_book_recovers_dropped_snapshot(#[case] after_gap: bool) {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = BitgetDataClientConfig {
        book_snapshot_timeout_secs: 1,
        ..create_test_config(addr)
    };

    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    let book = SubscribeBookDeltas::new(
        InstrumentId::from("BTCUSDT-PERP.BITGET"),
        BookType::L2_MBP,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        NonZeroUsize::new(1000),
        false,
        None,
        None,
    );

    // The `books` snapshot fixture carries seq 1207360465262
    let mut gapped = load_test_data("ws_orderbook_delta.json");
    gapped["data"][0]["pseq"] = 1_207_360_465_263_u64.into();

    if after_gap {
        client.subscribe_book_deltas(book).unwrap();
        next_book_deltas(&mut rx).await;
        state.book_snapshot_drops.store(1, Ordering::SeqCst);
        state.book_updates.lock().await.push(gapped);
    } else {
        state.book_snapshot_drops.store(1, Ordering::SeqCst);
        client.subscribe_book_deltas(book).unwrap();
    }

    let snapshot = next_book_deltas(&mut rx).await;

    assert_eq!(snapshot.deltas[0].action, BookAction::Clear);
    // Clear plus 1000 levels per side
    assert_eq!(snapshot.deltas.len(), 2001);
    assert_eq!(state.book_snapshot_drops.load(Ordering::SeqCst), 0);
    client.disconnect().await.unwrap();
}

// Quotes and a depth-1 book share the `books1` topic, so dropping the book must keep quotes
// streaming (`books1` is snapshot-per-push, so it never needs gap recovery)
#[rstest]
#[tokio::test]
async fn test_data_client_shared_depth_one_recovery_keeps_quotes() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let topic = "books1.BTCUSDT";

    let quotes = SubscribeQuotes::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    let book = SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        NonZeroUsize::new(1),
        false,
        None,
        None,
    );

    let unsubscribe_book = UnsubscribeBookDeltas::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    let mut snapshot = load_test_data("ws_orderbook_books1.json");
    snapshot["data"][0]["ts"] = "1709891700000".into();
    let mut later = load_test_data("ws_orderbook_books1.json");
    later["data"][0]["ts"] = "1709891800000".into();

    // The first quote is the REST seed taken on subscribe
    client.subscribe_quotes(quotes).unwrap();
    next_quote(&mut rx, UnixNanos::new(1_794_192_836_072_000_000)).await;
    client.subscribe_book_deltas(book).unwrap();
    state.book_updates.lock().await.push(snapshot);
    let synced = next_book_deltas(&mut rx).await;
    client.unsubscribe_book_deltas(&unsubscribe_book).unwrap();
    state.book_updates.lock().await.push(later);
    let quote = next_quote(&mut rx, UnixNanos::new(1_709_891_800_000_000_000)).await;

    assert_eq!(synced.deltas[0].action, BookAction::Clear);
    assert_eq!(synced.ts_event, UnixNanos::new(1_709_891_700_000_000_000));
    assert_eq!(quote.instrument_id, instrument_id);
    assert!(state.subscriptions.lock().await.iter().any(|t| t == topic));
    client.disconnect().await.unwrap();
}

async fn next_quote(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    ts_event: UnixNanos,
) -> QuoteTick {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let DataEvent::Data(Data::Quote(quote)) = rx.recv().await.unwrap()
                && quote.ts_event == ts_event
            {
                return quote;
            }
        }
    })
    .await
    .expect("timeout waiting for quote")
}

async fn next_book_deltas(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) -> OrderBookDeltas {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let DataEvent::Data(Data::BookDeltas(deltas)) = rx.recv().await.unwrap() {
                return *deltas;
            }
        }
    })
    .await
    .expect("timeout waiting for book deltas")
}

#[rstest]
#[tokio::test]
async fn test_data_client_reset_clears_state() {
    let (addr, _state) = start_test_server().await.unwrap();

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();

    client.reset().unwrap();
    assert!(!client.is_connected());

    client.connect().await.unwrap();
    assert!(client.is_connected());

    client.reset().unwrap();
    assert!(!client.is_connected());
}

#[rstest]
#[tokio::test]
async fn test_data_client_emits_instruments_on_connect() {
    let (addr, _state) = start_test_server().await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();

    client.connect().await.unwrap();

    let instruments_received = Arc::new(AtomicUsize::new(0));
    let counter = instruments_received.clone();

    wait_until_async(
        || {
            while let Ok(event) = rx.try_recv() {
                if matches!(event, DataEvent::Instrument(_)) {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
            let count = counter.load(Ordering::Relaxed);
            async move { count > 0 }
        },
        Duration::from_secs(5),
    )
    .await;

    assert!(
        instruments_received.load(Ordering::Relaxed) > 0,
        "Expected to receive instrument events on connect"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_request_book_snapshot() {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    // Drain instrument events from connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let request = RequestBookSnapshot::new(
        instrument_id,
        None,
        Some(*BITGET_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    client.request_book_snapshot(request).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for book snapshot response")
        .expect("channel closed");

    assert!(
        matches!(event, DataEvent::Response(DataResponse::Book(_))),
        "Expected Book response, was: {event:?}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_request_funding_rates() {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    // Drain instrument events from connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let request = RequestFundingRates::new(
        instrument_id,
        None,
        None,
        None,
        Some(*BITGET_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    client.request_funding_rates(request).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for funding rates response")
        .expect("channel closed");

    assert!(
        matches!(event, DataEvent::Response(DataResponse::FundingRates(_))),
        "Expected FundingRates response, was: {event:?}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_request_funding_rates_rejects_spot() {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let instrument_id = InstrumentId::from("BTCUSDT-SPOT.BITGET");
    let request = RequestFundingRates::new(
        instrument_id,
        None,
        None,
        None,
        Some(*BITGET_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let result = client.request_funding_rates(request);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Funding rates not available for Spot instruments"),
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_request_funding_rates_rejects_option() {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let instrument_id = InstrumentId::from("BTC-26DEC25-100000-C-OPTION.BITGET");
    let request = RequestFundingRates::new(
        instrument_id,
        None,
        None,
        None,
        Some(*BITGET_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let result = client.request_funding_rates(request);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Funding rates not available for Option instruments"),
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_data_client_request_instruments() {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    // Drain instrument events from connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    while rx.try_recv().is_ok() {}

    let request = RequestInstruments::new(
        None,
        None,
        Some(*BITGET_CLIENT_ID),
        Some(*BITGET_VENUE),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    client.request_instruments(request).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for instruments response")
        .expect("channel closed");

    assert!(
        matches!(event, DataEvent::Response(DataResponse::Instruments(_))),
        "Expected Instruments response, was: {event:?}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_data_client_request_instrument() {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = create_test_config(addr);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();

    // Drain instrument events from connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let request = RequestInstrument::new(
        instrument_id,
        None,
        None,
        Some(*BITGET_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    client.request_instrument(request).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for instrument response")
        .expect("channel closed");

    assert!(
        matches!(event, DataEvent::Response(DataResponse::Instrument(_))),
        "Expected Instrument response, was: {event:?}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[case::shared_quotes_first(1, true, true)]
#[case::shared_book_first(1, false, false)]
#[case::shared_remove_book_first(1, true, false)]
#[case::shared_remove_quotes_first(1, false, true)]
#[case::deeper_remove_quotes_first(50, true, true)]
#[case::deeper_remove_book_first(50, false, false)]
#[tokio::test]
async fn test_data_client_book_quote_topic_lifetime(
    #[case] depth: usize,
    #[case] quotes_first: bool,
    #[case] remove_quotes_first: bool,
) {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let quotes = SubscribeQuotes::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    let book = SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        NonZeroUsize::new(depth),
        false,
        None,
        None,
    );

    for subscribe_quotes in [quotes_first, !quotes_first] {
        if subscribe_quotes {
            client.subscribe_quotes(quotes.clone()).unwrap();
        } else {
            client.subscribe_book_deltas(book.clone()).unwrap();
        }
    }
    let mut expected_topics = vec!["books1.BTCUSDT".to_string()];
    if depth != 1 {
        expected_topics.push(format!("books{depth}.BTCUSDT"));
    }
    wait_until_async(
        || async {
            let mut topics = state.subscriptions.lock().await.clone();
            topics.sort();
            topics == expected_topics
        },
        Duration::from_secs(5),
    )
    .await;

    let mut different_depth = book.clone();
    different_depth.depth = NonZeroUsize::new(if depth == 1 { 50 } else { 1 });
    assert_eq!(
        client
            .subscribe_book_deltas(different_depth)
            .unwrap_err()
            .to_string(),
        format!("Already subscribed to book depth {depth} for {instrument_id}"),
    );

    // Repeating either request must not acquire another transport reference
    client.subscribe_quotes(quotes).unwrap();
    client.subscribe_book_deltas(book).unwrap();
    let unsubscribe_quotes = UnsubscribeQuotes::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    let unsubscribe_book = UnsubscribeBookDeltas::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    if remove_quotes_first {
        client.unsubscribe_quotes(&unsubscribe_quotes).unwrap();
    } else {
        client.unsubscribe_book_deltas(&unsubscribe_book).unwrap();
    }
    let remaining_depth = if remove_quotes_first { depth } else { 1 };
    let mut snapshot = load_test_data("ws_orderbook_snapshot.json");
    snapshot["arg"]["topic"] = format!("books{remaining_depth}").into();
    snapshot["data"][0]["ts"] = "1709891700000".into();
    state.book_updates.lock().await.push(snapshot);
    let expected_ts = UnixNanos::new(1_709_891_700_000_000_000);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await.unwrap() {
                DataEvent::Data(Data::Quote(quote)) if quote.ts_event == expected_ts => {
                    assert!(!remove_quotes_first);
                    assert_eq!(quote.instrument_id, instrument_id);
                    assert_eq!(quote.bid_price, Price::from("75242.8"));
                    assert_eq!(quote.ask_price, Price::from("75242.9"));
                    break;
                }
                DataEvent::Data(Data::BookDeltas(deltas)) if deltas.ts_event == expected_ts => {
                    assert!(remove_quotes_first);
                    assert_eq!(deltas.instrument_id, instrument_id);
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("remaining subscriber must still receive fresh book data");

    if remove_quotes_first {
        client.unsubscribe_book_deltas(&unsubscribe_book).unwrap();
    } else {
        client.unsubscribe_quotes(&unsubscribe_quotes).unwrap();
    }
    wait_until_async(
        || async { state.subscriptions.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;
    assert!(state.subscriptions.lock().await.is_empty());
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_failed_quote_subscription_keeps_shared_book_topic() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let quotes = SubscribeQuotes::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    let book = SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        NonZeroUsize::new(1),
        false,
        None,
        None,
    );
    let unsubscribe_quotes = UnsubscribeQuotes::new(
        instrument_id,
        Some(*BITGET_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    // The command channel is closed before connect, so the quote subscription fails
    client.subscribe_quotes(quotes).unwrap();
    client.connect().await.unwrap();
    client.subscribe_book_deltas(book).unwrap();
    wait_for_server_topics(&state, &["books1.BTCUSDT"]).await;

    // `books1` pushes nothing on subscribe, so the venue snapshots are injected
    let mut first = load_test_data("ws_orderbook_books1.json");
    first["data"][0]["ts"] = "1709891679000".into();
    state.book_updates.lock().await.push(first);
    let subscribed = next_book_deltas(&mut rx).await;
    client.unsubscribe_quotes(&unsubscribe_quotes).unwrap();
    let mut snapshot = load_test_data("ws_orderbook_books1.json");
    snapshot["data"][0]["ts"] = "1709891700000".into();
    state.book_updates.lock().await.push(snapshot);
    let updated = next_book_deltas(&mut rx).await;

    assert_eq!(
        subscribed.ts_event,
        UnixNanos::new(1_709_891_679_000_000_000)
    );
    assert_eq!(updated.ts_event, UnixNanos::new(1_709_891_700_000_000_000));
    assert_eq!(*state.subscriptions.lock().await, ["books1.BTCUSDT"]);
    client.disconnect().await.unwrap();
}

#[rstest]
#[case::open(false, 1_794_192_840_000_000_000)]
#[case::close(true, 1_794_193_140_000_000_000)]
#[tokio::test]
async fn test_data_client_live_bar_timestamp(#[case] on_close: bool, #[case] expected: u64) {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut config = create_test_config(addr);
    config.bars_timestamp_on_close = on_close;
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();
    let bar_type = BarType::from("BTCUSDT-PERP.BITGET-5-MINUTE-LAST-EXTERNAL");
    client
        .subscribe_bars(SubscribeBars::new(
            bar_type,
            Some(*BITGET_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    let bar = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(DataEvent::Data(Data::Bar(bar))) = rx.recv().await {
                break bar;
            }
        }
    })
    .await
    .unwrap();
    client.disconnect().await.unwrap();
    assert_eq!(bar.bar_type, bar_type);
    assert_eq!(bar.ts_event, UnixNanos::from(expected));
}

#[rstest]
#[case::open(false, 1_794_192_780_000_000_000)]
#[case::close(true, 1_794_193_080_000_000_000)]
#[tokio::test]
async fn test_data_client_request_bar_timestamp(#[case] on_close: bool, #[case] expected: u64) {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut config = create_test_config(addr);
    config.bars_timestamp_on_close = on_close;
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();
    let bar_type = BarType::from("BTCUSDT-PERP.BITGET-5-MINUTE-LAST-EXTERNAL");
    let request = RequestBars::new(
        bar_type,
        None,
        None,
        NonZeroUsize::new(1),
        Some(*BITGET_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let request_id = request.request_id;
    client.request_bars(request).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(DataEvent::Response(DataResponse::Bars(response))) = rx.recv().await {
                break response;
            }
        }
    })
    .await
    .unwrap();
    client.disconnect().await.unwrap();
    assert_eq!(response.correlation_id, request_id);
    assert_eq!(response.bar_type, bar_type);
    assert_eq!(response.data.len(), 1);
    assert_eq!(response.data[0].ts_event, UnixNanos::from(expected));
    assert_eq!(
        response.data[0].ts_init,
        UnixNanos::from(1_794_193_080_000_000_000)
    );
}

#[rstest]
#[case::metadata_only(None)]
#[case::with_identifier(Some("bitget-liquidations"))]
#[tokio::test]
async fn test_data_client_subscribe_liquidations_emits_subscribed_data_type(
    #[case] identifier: Option<&str>,
) {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    let data_type = liquidation_data_type(Some("BTCUSDT-PERP.BITGET"), identifier);

    client
        .subscribe(subscribe_custom_data(data_type.clone()))
        .unwrap();

    wait_for_server_topics(&state, &["liquidation.BTCUSDT"]).await;
    let events = recv_custom_data(&mut rx, 2).await;
    let long = as_liquidation(&events[0]);
    let short = as_liquidation(&events[1]);

    assert_eq!(events[0].data_type, data_type);
    assert_eq!(events[0].data_type.topic(), data_type.topic());
    assert_eq!(events[0].data_type.identifier(), identifier);
    assert_eq!(events[1].data_type, data_type);
    assert_eq!(long.instrument_id, instrument_id);
    assert_eq!(long.position_side, PositionSide::Long);
    assert_eq!(long.bankruptcy_price, Price::from("96250.5"));
    assert_eq!(long.quantity, Quantity::from("0.015"));
    assert_eq!(long.ts_event, UnixNanos::new(1_739_502_302_929_000_000));
    assert_eq!(short.instrument_id, instrument_id);
    assert_eq!(short.position_side, PositionSide::Short);
    assert_eq!(short.bankruptcy_price, Price::from("97410.0"));
    assert_eq!(short.quantity, Quantity::from("1.250"));
    assert_eq!(short.ts_event, UnixNanos::new(1_739_502_303_011_000_000));

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_unsubscribe_liquidations() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    let data_type = liquidation_data_type(Some("BTCUSDT-PERP.BITGET"), None);
    client
        .subscribe(subscribe_custom_data(data_type.clone()))
        .unwrap();
    wait_for_server_topics(&state, &["liquidation.BTCUSDT"]).await;

    client
        .unsubscribe(&unsubscribe_custom_data(data_type))
        .unwrap();

    wait_for_server_topics(&state, &[]).await;
    assert!(state.subscriptions.lock().await.is_empty());
    assert!(state.subscription_events.lock().await.is_empty());

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_subscribe_liquidations_routes_inverse_to_inverse_client() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut frame = liquidation_frame("BTCUSD_CM");
    frame["data"][0]["amount"] = "1500".into();
    frame["data"][1]["amount"] = "25000".into();
    *state.liquidation_frame.lock().await = Some(frame);
    let mut config = create_test_config(addr);
    config.product_types = vec![BitgetProductType::Linear, BitgetProductType::Inverse];
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, config).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("BTCUSD_CM-INVERSE.BITGET");
    let data_type = liquidation_data_type(Some("BTCUSD_CM-INVERSE.BITGET"), None);

    client
        .subscribe(subscribe_custom_data(data_type.clone()))
        .unwrap();

    // Only the inverse stream resolves BTCUSD_CM to the inverse instrument
    wait_for_server_topics(&state, &["liquidation.BTCUSD_CM"]).await;
    let events = recv_custom_data(&mut rx, 2).await;
    let long = as_liquidation(&events[0]);
    let short = as_liquidation(&events[1]);

    assert_eq!(events[0].data_type, data_type);
    assert_eq!(events[1].data_type, data_type);
    assert_eq!(long.instrument_id, instrument_id);
    assert_eq!(long.position_side, PositionSide::Long);
    assert_eq!(long.bankruptcy_price, Price::from("96250.5"));
    assert_eq!(long.quantity, Quantity::from("1500"));
    assert_eq!(long.ts_event, UnixNanos::new(1_739_502_302_929_000_000));
    assert_eq!(short.instrument_id, instrument_id);
    assert_eq!(short.position_side, PositionSide::Short);
    assert_eq!(short.bankruptcy_price, Price::from("97410.0"));
    assert_eq!(short.quantity, Quantity::from("25000"));
    assert_eq!(short.ts_event, UnixNanos::new(1_739_502_303_011_000_000));

    client.disconnect().await.unwrap();
}

#[rstest]
#[case::spot(
    Some("BTCUSDT-SPOT.BITGET"),
    "Liquidations not available for Spot instruments"
)]
#[case::option(
    Some("BTC-26DEC25-100000-C-OPTION.BITGET"),
    "Liquidations not available for Option instruments"
)]
#[case::missing_instrument_id(None, "BitgetLiquidation requires `instrument_id` metadata")]
#[case::invalid_instrument_id(
    Some("BTCUSDT-PERP"),
    concat!(
        "Invalid instrument_id metadata `BTCUSDT-PERP`: invalid `InstrumentId` value ",
        "'BTCUSDT-PERP': missing '.' separator between symbol and venue components",
    )
)]
#[case::other_venue(
    Some("BTCUSDT-PERP.BINANCE"),
    "BitgetLiquidation requires a BITGET instrument, received BTCUSDT-PERP.BINANCE"
)]
#[case::unknown_product(
    Some("BTCUSDT.BITGET"),
    "Liquidations require a -PERP or -INVERSE instrument, received BTCUSDT.BITGET"
)]
#[case::inverse_without_client(
    Some("BTCUSD_CM-INVERSE.BITGET"),
    "no WebSocket client for product type"
)]
#[tokio::test]
async fn test_data_client_subscribe_liquidations_rejects(
    #[case] instrument_id: Option<&str>,
    #[case] expected_error: &str,
) {
    let (addr, _state) = start_test_server().await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    let data_type = liquidation_data_type(instrument_id, None);

    let error = client
        .subscribe(subscribe_custom_data(data_type))
        .unwrap_err();

    assert_eq!(error.to_string(), expected_error);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_unsupported_custom_data_type_is_ignored() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    let unsupported = DataType::new("BitgetOpenInterest", None, None);

    client
        .subscribe(subscribe_custom_data(unsupported.clone()))
        .unwrap();
    client
        .unsubscribe(&unsubscribe_custom_data(unsupported))
        .unwrap();
    client
        .subscribe(subscribe_custom_data(liquidation_data_type(
            Some("BTCUSDT-PERP.BITGET"),
            None,
        )))
        .unwrap();

    wait_for_server_topics(&state, &["liquidation.BTCUSDT"]).await;
    assert_eq!(
        *state.subscription_events.lock().await,
        vec![("liquidation.BTCUSDT".to_string(), true)],
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_duplicate_liquidation_subscription() {
    let (addr, state) = start_test_server().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);
    let mut client = BitgetDataClient::new(*BITGET_CLIENT_ID, create_test_config(addr)).unwrap();
    client.connect().await.unwrap();
    let data_type = liquidation_data_type(Some("BTCUSDT-PERP.BITGET"), None);
    let other_data_type = liquidation_data_type(Some("BTCUSDT-PERP.BITGET"), Some("other"));
    client
        .subscribe(subscribe_custom_data(data_type.clone()))
        .unwrap();
    wait_for_server_topics(&state, &["liquidation.BTCUSDT"]).await;
    recv_custom_data(&mut rx, 2).await;

    // Repeating the request must not acquire another transport reference
    client
        .subscribe(subscribe_custom_data(data_type.clone()))
        .unwrap();
    let error = client
        .subscribe(subscribe_custom_data(other_data_type))
        .unwrap_err();
    client
        .unsubscribe(&unsubscribe_custom_data(data_type.clone()))
        .unwrap();
    wait_for_server_topics(&state, &[]).await;

    let mut frame = liquidation_frame("BTCUSDT");
    frame["data"][0]["ts"] = "1739502310000".into();
    frame["data"][1]["ts"] = "1739502310500".into();
    *state.liquidation_frame.lock().await = Some(frame);
    client
        .subscribe(subscribe_custom_data(data_type.clone()))
        .unwrap();
    wait_for_server_topics(&state, &["liquidation.BTCUSDT"]).await;
    let events = recv_custom_data(&mut rx, 2).await;

    assert_eq!(
        error.to_string(),
        format!(
            "Already subscribed to liquidations for BTCUSDT-PERP.BITGET as {}",
            data_type.topic()
        ),
    );
    assert_eq!(events[0].data_type, data_type);
    assert_eq!(events[1].data_type, data_type);
    assert_eq!(
        as_liquidation(&events[0]).ts_event,
        UnixNanos::new(1_739_502_310_000_000_000)
    );
    assert_eq!(
        as_liquidation(&events[1]).ts_event,
        UnixNanos::new(1_739_502_310_500_000_000)
    );

    client.disconnect().await.unwrap();
}
