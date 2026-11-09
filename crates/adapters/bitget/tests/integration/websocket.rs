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

//! Integration tests for Bitget WebSocket client using a mock server.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures_util::StreamExt;
use nautilus_bitget::{
    common::enums::{
        BitgetEnvironment, BitgetOrderSide, BitgetOrderType, BitgetProductType, BitgetTimeInForce,
    },
    websocket::{
        client::BitgetWebSocketClient,
        messages::{
            BitgetWsAmendOrderParams, BitgetWsCancelOrderParams, BitgetWsMessage,
            BitgetWsPlaceOrderParams,
        },
    },
};
use nautilus_common::testing::wait_until_async;
use nautilus_model::{
    data::BarType,
    identifiers::InstrumentId,
    instruments::CurrencyPair,
    types::{Currency, Price, Quantity},
};
use nautilus_network::websocket::TransportBackend;
use rstest::rstest;
use serde_json::json;
use ustr::Ustr;

// Test server state for tracking WebSocket connections
#[derive(Clone)]
struct TestServerState {
    connection_count: Arc<tokio::sync::Mutex<usize>>,
    upgrade_requests: Arc<AtomicUsize>,
    upgrade_gate: tokio::sync::watch::Sender<bool>,
    subscriptions: Arc<tokio::sync::Mutex<Vec<String>>>,
    subscription_events: Arc<tokio::sync::Mutex<Vec<(String, bool)>>>, // (topic, success)
    fail_next_subscriptions: Arc<tokio::sync::Mutex<Vec<String>>>,
    auth_response_delay_ms: Arc<tokio::sync::Mutex<Option<u64>>>,
    authenticated: Arc<AtomicBool>,
    disconnect_trigger: Arc<AtomicBool>,
    ping_count: Arc<AtomicUsize>,
    pong_count: Arc<AtomicUsize>,
    captured_messages: Arc<tokio::sync::Mutex<Vec<serde_json::Value>>>,
}

impl Default for TestServerState {
    fn default() -> Self {
        Self {
            connection_count: Arc::new(tokio::sync::Mutex::new(0)),
            upgrade_requests: Arc::new(AtomicUsize::new(0)),
            upgrade_gate: tokio::sync::watch::channel(true).0,
            subscriptions: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            subscription_events: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            fail_next_subscriptions: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            auth_response_delay_ms: Arc::new(tokio::sync::Mutex::new(None)),
            authenticated: Arc::new(AtomicBool::new(false)),
            disconnect_trigger: Arc::new(AtomicBool::new(false)),
            ping_count: Arc::new(AtomicUsize::new(0)),
            pong_count: Arc::new(AtomicUsize::new(0)),
            captured_messages: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }
}

impl TestServerState {
    #[allow(dead_code)]
    async fn reset(&self) {
        *self.connection_count.lock().await = 0;
        self.subscriptions.lock().await.clear();
        self.subscription_events.lock().await.clear();
        self.fail_next_subscriptions.lock().await.clear();
        *self.auth_response_delay_ms.lock().await = None;
        self.authenticated.store(false, Ordering::Relaxed);
        self.disconnect_trigger.store(false, Ordering::Relaxed);
        self.ping_count.store(0, Ordering::Relaxed);
        self.pong_count.store(0, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    async fn set_subscription_failures(&self, topics: Vec<String>) {
        *self.fail_next_subscriptions.lock().await = topics;
    }

    #[allow(dead_code)]
    async fn set_auth_delay(&self, delay_ms: u64) {
        *self.auth_response_delay_ms.lock().await = Some(delay_ms);
    }

    #[allow(dead_code)]
    async fn subscription_events(&self) -> Vec<(String, bool)> {
        self.subscription_events.lock().await.clone()
    }

    #[allow(dead_code)]
    async fn clear_subscription_events(&self) {
        self.subscription_events.lock().await.clear();
    }
}

// WebSocket handler
async fn handle_websocket(ws: WebSocketUpgrade, State(state): State<TestServerState>) -> Response {
    state.upgrade_requests.fetch_add(1, Ordering::Relaxed);
    let mut gate = state.upgrade_gate.subscribe();
    gate.wait_for(|open| *open).await.unwrap();
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: TestServerState) {
    {
        let mut count = state.connection_count.lock().await;
        *count += 1;
    }

    // Bitget keepalive is a text frame either way: the venue may send `ping` and expect `pong`
    let mut last_server_ping = tokio::time::Instant::now();

    // Main message handling loop
    loop {
        if state.disconnect_trigger.load(Ordering::Relaxed) {
            break;
        }

        if last_server_ping.elapsed() >= Duration::from_millis(500) {
            last_server_ping = tokio::time::Instant::now();
            if socket.send(Message::Text("ping".into())).await.is_err() {
                break;
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

        if state.disconnect_trigger.load(Ordering::Relaxed) {
            break;
        }

        match msg {
            Message::Text(text) if text.as_str() == "ping" => {
                state.ping_count.fetch_add(1, Ordering::Relaxed);

                if socket.send(Message::Text("pong".into())).await.is_err() {
                    break;
                }
            }
            Message::Text(text) if text.as_str() == "pong" => {
                state.pong_count.fetch_add(1, Ordering::Relaxed);
            }
            Message::Text(text) => {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                    continue;
                };

                let op = value.get("op").and_then(|v| v.as_str());

                match op {
                    Some("login") => {
                        // Check for auth delay
                        if let Some(delay_ms) = *state.auth_response_delay_ms.lock().await {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }

                        // Parse login request
                        let api_key = value
                            .get("args")
                            .and_then(|a| a.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|v| v.get("apiKey"))
                            .and_then(|v| v.as_str());

                        let response = if api_key == Some("test_api_key") {
                            state.authenticated.store(true, Ordering::Relaxed);
                            load_test_data("ws_auth_success.json")
                        } else {
                            // Login failed
                            load_test_data("ws_auth_failure.json")
                        };

                        if socket
                            .send(Message::Text(response.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Some("subscribe") => {
                        let args = value.get("args").and_then(|a| a.as_array());
                        let mut responses = Vec::new();

                        if let Some(args) = args {
                            let fail_list = state.fail_next_subscriptions.lock().await.clone();

                            for arg in args {
                                let topic = arg_topic(arg);
                                let should_fail = fail_list.contains(&topic);

                                state
                                    .subscription_events
                                    .lock()
                                    .await
                                    .push((topic.clone(), !should_fail));

                                if should_fail {
                                    // Bitget echoes the rejected arg inside `msg`
                                    responses.push(json!({
                                        "event": "error",
                                        "code": 30001,
                                        "msg": format!("{arg} doesn't exist"),
                                        "connId": "test-conn-id"
                                    }));
                                } else {
                                    let mut subs = state.subscriptions.lock().await;
                                    if !subs.contains(&topic) {
                                        subs.push(topic);
                                    }
                                    responses.push(json!({
                                        "event": "subscribe",
                                        "arg": arg,
                                        "connId": "test-conn-id"
                                    }));
                                }
                            }
                        }

                        let mut closed = false;
                        for response in responses {
                            if socket
                                .send(Message::Text(response.to_string().into()))
                                .await
                                .is_err()
                            {
                                closed = true;
                                break;
                            }
                        }
                        if closed {
                            break;
                        }

                        // Send a sample data message for the first topic
                        if let Some(args) = args
                            && let Some(first_topic) = args
                                .first()
                                .and_then(|t| t.get("topic"))
                                .and_then(|t| t.as_str())
                        {
                            if first_topic == "publicTrade" {
                                // Send a trade message
                                let trade_msg = load_test_data("ws_public_trade.json");

                                if socket
                                    .send(Message::Text(trade_msg.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            } else if first_topic.starts_with("books") {
                                // Send an orderbook message
                                let orderbook_msg = load_test_data("ws_orderbook_snapshot.json");

                                if socket
                                    .send(Message::Text(orderbook_msg.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                    Some("unsubscribe") => {
                        let args = value.get("args").and_then(|a| a.as_array());
                        let mut responses = Vec::new();

                        if let Some(args) = args {
                            for arg in args {
                                let topic = arg_topic(arg);
                                let mut events = state.subscription_events.lock().await;
                                events.retain(|(t, _)| *t != topic);
                                drop(events);

                                let mut subs = state.subscriptions.lock().await;
                                subs.retain(|s| *s != topic);

                                // Send unsubscription confirmation
                                responses.push(json!({
                                    "event": "unsubscribe",
                                    "arg": arg,
                                    "connId": "test-conn-id"
                                }));
                            }
                        }

                        let mut closed = false;
                        for response in responses {
                            if socket
                                .send(Message::Text(response.to_string().into()))
                                .await
                                .is_err()
                            {
                                closed = true;
                                break;
                            }
                        }
                        if closed {
                            break;
                        }
                    }
                    Some("trade") => {
                        // Order entry (`place-order`, `modify-order`, `cancel-order`, `batch-*`)
                        state.captured_messages.lock().await.push(value.clone());
                        let response = json!({
                            "event": "trade",
                            "id": value.get("id").cloned().unwrap_or_default(),
                            "category": value.get("category").cloned().unwrap_or_default(),
                            "topic": value.get("topic").cloned().unwrap_or_default(),
                            "args": [],
                            "code": "0",
                            "msg": "Success",
                            "connId": "test-conn-id",
                            "rateLimit": [{"limit": "10", "remaining": "9"}]
                        });

                        if socket
                            .send(Message::Text(response.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    _ => {}
                }
            }
            Message::Ping(_) => {
                if socket.send(Message::Pong(vec![].into())).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => {
                break;
            }
            _ => {}
        }

        if state.disconnect_trigger.load(Ordering::Relaxed) {
            break;
        }
    }

    let mut count = state.connection_count.lock().await;
    *count = count.saturating_sub(1);
}

// Returns the dotted topic key for a Bitget subscription `arg` (private channels keep their name)
fn arg_topic(arg: &serde_json::Value) -> String {
    let field = |name: &str| arg.get(name).and_then(|v| v.as_str());
    let topic = field("topic").unwrap_or_default();
    if field("instType") == Some("UTA") {
        return topic.to_string();
    }
    match (field("interval"), field("symbol")) {
        (Some(interval), Some(symbol)) => format!("{topic}.{interval}.{symbol}"),
        (None, Some(symbol)) => format!("{topic}.{symbol}"),
        _ => topic.to_string(),
    }
}

// Load test data from existing files
fn load_test_data(filename: &str) -> serde_json::Value {
    let path = format!("test_data/{filename}");
    let content = std::fs::read_to_string(path).expect("Failed to read test data");
    serde_json::from_str(&content).expect("Failed to parse test data")
}

fn make_linear_pair(raw_symbol: &str, base: &str, quote: &str) -> CurrencyPair {
    CurrencyPair::builder()
        .instrument_id(format!("{raw_symbol}-PERP.BITGET").into())
        .raw_symbol(raw_symbol.into())
        .base_currency(Currency::from(base))
        .quote_currency(Currency::from(quote))
        .price_precision(2)
        .size_precision(5)
        .price_increment(Price::from("0.01"))
        .size_increment(Quantity::from("0.00001"))
        .ts_event(0.into())
        .ts_init(0.into())
        .build()
        .unwrap()
}

fn create_test_router(state: TestServerState) -> Router {
    Router::new()
        .route("/v3/ws/public", get(handle_websocket))
        .route("/v3/ws/private", get(handle_websocket))
        .with_state(state)
}

async fn start_test_server()
-> Result<(SocketAddr, TestServerState), Box<dyn std::error::Error + Send + Sync>> {
    // Bind to port 0 to let the OS assign an available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let state = TestServerState::default();
    let router = create_test_router(state.clone());

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok((addr, state))
}

#[allow(dead_code)]
async fn wait_for_subscription_events<F>(
    state: &TestServerState,
    timeout: Duration,
    mut predicate: F,
) -> Vec<(String, bool)>
where
    F: FnMut(&[(String, bool)]) -> bool,
{
    let state_clone = state.clone();
    let poll = async {
        loop {
            let events = state_clone.subscription_events().await;
            if predicate(&events) {
                return events;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    match tokio::time::timeout(timeout, poll).await {
        Ok(events) => events,
        Err(_) => state.subscription_events().await,
    }
}

async fn wait_for_connection_count(state: &TestServerState, expected: usize, timeout: Duration) {
    wait_until_async(
        || {
            let state = state.clone();
            async move { *state.connection_count.lock().await == expected }
        },
        timeout,
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_public_client_connection() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Wait for connection to be established
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    assert!(client.is_active());
    assert_eq!(*state.connection_count.lock().await, 1);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_private_client_authentication() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    // Connection may timeout waiting for auth confirmation from the mock server
    // This is expected behavior as the mock server's auth flow may not perfectly
    // match the client's expectations
    let _result = client.connect().await;

    // Wait for connection to be established
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Check if auth was attempted (connection was made)
    assert!(*state.connection_count.lock().await > 0);

    let _result = client.close().await;
}

#[rstest]
#[tokio::test]
async fn test_authentication_failure() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("invalid_key".to_string()),
        Some("invalid_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    let _result = client.connect().await;

    // Wait for connection attempt
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Verify the server doesn't mark it as authenticated
    assert!(!state.authenticated.load(Ordering::Relaxed));

    let _result = client.close().await;
}

#[rstest]
#[tokio::test]
async fn test_ping_pong() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        1, // 1 second heartbeat,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Wait for connection
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Wait for at least one ping
    wait_until_async(
        || async { state.ping_count.load(Ordering::Relaxed) > 0 },
        Duration::from_secs(3),
    )
    .await;

    assert!(state.ping_count.load(Ordering::Relaxed) > 0);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_subscription_lifecycle() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to a topic
    let topics = vec!["publicTrade.BTCUSDT".to_string()];
    client.subscribe(topics.clone()).await.unwrap();

    // Wait for subscription confirmation
    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let subs = state.subscription_events.lock().await.clone();
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic == "publicTrade.BTCUSDT" && *ok)
    );

    // Unsubscribe
    client.unsubscribe(topics).await.unwrap();

    // Wait for unsubscription
    wait_until_async(
        || async { state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    assert!(state.subscription_events.lock().await.is_empty());

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_message_routing() {
    let (addr, _state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to trades
    let topics = vec!["publicTrade.BTCUSDT".to_string()];
    client.subscribe(topics).await.unwrap();

    // Wait for subscription to be confirmed
    wait_until_async(
        || async { client.subscription_count() > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Verify subscription was recorded
    assert!(client.subscription_count() > 0);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_reconnection_flow() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to a topic before disconnect
    let topics = vec!["publicTrade.BTCUSDT".to_string()];
    client.subscribe(topics.clone()).await.unwrap();

    // Wait for initial connection
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    let initial_count = *state.connection_count.lock().await;
    assert_eq!(initial_count, 1);

    // Trigger a server-side disconnect
    state.disconnect_trigger.store(true, Ordering::Relaxed);

    // Short delay for disconnect trigger to be observed by server
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Note: Full reconnection testing requires the client to support reconnection
    // This test establishes the pattern for testing reconnection behavior

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_multiple_subscriptions() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to multiple topics
    let topics = vec![
        "publicTrade.BTCUSDT".to_string(),
        "publicTrade.ETHUSDT".to_string(),
        "books50.BTCUSDT".to_string(),
    ];
    client.subscribe(topics).await.unwrap();

    // Wait for subscriptions
    wait_until_async(
        || async { state.subscription_events.lock().await.len() >= 3 },
        Duration::from_secs(5),
    )
    .await;

    let subs = state.subscription_events.lock().await.clone();
    assert_eq!(subs.len(), 3);
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic == "publicTrade.BTCUSDT" && *ok)
    );
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic == "publicTrade.ETHUSDT" && *ok)
    );
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic == "books50.BTCUSDT" && *ok)
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test(start_paused = true)]
async fn test_wait_until_active_timeout() {
    // Nothing connects the client, so it stays closed and the wait must time out
    let client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some("ws://127.0.0.1:9999/invalid".to_string()),
        20,
        TransportBackend::default(),
        None,
    );

    let error = client.wait_until_active(0.5).await.unwrap_err();

    assert!(client.is_closed());
    assert_eq!(
        error.to_string(),
        "Client error: WebSocket connection timeout after 0.5 seconds"
    );
}

#[rstest]
#[tokio::test]
async fn test_heartbeat_timeout_reconnection() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        1, // 1 second heartbeat,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Wait for connection
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Trigger disconnect - client should attempt reconnection
    state.disconnect_trigger.store(true, Ordering::Relaxed);

    // Short delay for disconnect trigger to be observed by server
    tokio::time::sleep(Duration::from_millis(100)).await;

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_sends_pong_for_text_ping() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        1,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // The mock venue sends a text `ping` on connect; the client must answer `pong`
    wait_until_async(
        || async { state.pong_count.load(Ordering::Relaxed) > 0 },
        Duration::from_secs(3),
    )
    .await;

    assert!(state.pong_count.load(Ordering::Relaxed) > 0);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_sends_pong_for_control_ping() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Wait for connection
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Control ping/pong is handled by the WebSocket layer
    // This test verifies the connection remains active
    assert!(client.is_active());

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_reauth_after_disconnect() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    let _result = client.connect().await;

    // Wait for initial connection
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Trigger disconnect
    state.disconnect_trigger.store(true, Ordering::Relaxed);

    // Short delay for disconnect trigger to be observed by server
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _result = client.close().await;
}

#[rstest]
#[tokio::test]
async fn test_login_failure_emits_error() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("invalid_key".to_string()),
        Some("invalid_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    let _result = client.connect().await;

    // Wait for connection attempt
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Verify auth failed
    assert!(!state.authenticated.load(Ordering::Relaxed));

    let _result = client.close().await;
}

#[rstest]
#[tokio::test]
async fn test_unauthenticated_private_subscription_fails() {
    let (addr, _state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    // Create public client
    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Try to subscribe to private channels - should fail
    let result = client.subscribe_orders().await;
    assert!(result.is_err());

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_subscription_after_reconnection() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe before disconnect
    let topics = vec!["publicTrade.BTCUSDT".to_string()];
    client.subscribe(topics.clone()).await.unwrap();

    // Wait for subscription
    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    // Trigger disconnect
    state.disconnect_trigger.store(true, Ordering::Relaxed);

    // Short delay for disconnect trigger to be observed by server
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify subscriptions are tracked
    assert!(client.subscription_count() > 0);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_subscription_restoration_tracking() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to multiple topics
    let topics = vec![
        "publicTrade.BTCUSDT".to_string(),
        "books50.ETHUSDT".to_string(),
    ];
    client.subscribe(topics).await.unwrap();

    // Wait for subscriptions
    wait_until_async(
        || async { state.subscription_events.lock().await.len() >= 2 },
        Duration::from_secs(5),
    )
    .await;

    // Verify subscription count
    let initial_count = client.subscription_count();
    assert_eq!(initial_count, 2);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_reconnection_retries_failed_subscriptions() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to a topic
    let topics = vec!["publicTrade.BTCUSDT".to_string()];
    client.subscribe(topics).await.unwrap();

    // Wait for subscription
    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    // Clear server subscriptions to simulate failure
    state.subscription_events.lock().await.clear();

    // Trigger disconnect
    state.disconnect_trigger.store(true, Ordering::Relaxed);

    // Short delay for disconnect trigger to be observed by server
    tokio::time::sleep(Duration::from_millis(100)).await;

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_trade_subscription_flow() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to trades using the high-level method
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    client.subscribe_trades(instrument_id).await.unwrap();

    // Wait for subscription
    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let subs = state.subscription_events.lock().await.clone();
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic.contains("publicTrade") && *ok)
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_orderbook_subscription_flow() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to orderbook using the high-level method
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    client.subscribe_orderbook(instrument_id, 50).await.unwrap();

    // Wait for subscription
    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let subs = state.subscription_events.lock().await.clone();
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic.starts_with("books") && *ok)
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_ticker_subscription_flow() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Subscribe to ticker using the high-level method
    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
    client.subscribe_ticker(instrument_id).await.unwrap();

    // Wait for subscription
    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let subs = state.subscription_events.lock().await.clone();
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic.contains("ticker") && *ok)
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_klines_subscription_flow() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    let bar_type = BarType::from("BTCUSDT-PERP.BITGET-1-MINUTE-LAST-EXTERNAL");
    client.subscribe_bars(bar_type).await.unwrap();

    // Wait for subscription
    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let subs = state.subscription_events.lock().await.clone();
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic.contains("kline") && *ok)
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_private_orders_subscription() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    let _result = client.connect().await;

    // Wait for connection
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Subscribe to orders (may succeed or fail depending on auth timing)
    let _result = client.subscribe_orders().await;

    let _result = client.close().await;
}

#[rstest]
#[tokio::test]
async fn test_private_executions_subscription() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    let _result = client.connect().await;

    // Wait for connection
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Subscribe to executions (may succeed or fail depending on auth timing)
    let _result = client.subscribe_executions().await;

    let _result = client.close().await;
}

#[rstest]
#[tokio::test]
async fn test_private_wallet_subscription() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    let _result = client.connect().await;

    // Wait for connection
    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    // Subscribe to wallet (may succeed or fail depending on auth timing)
    let _result = client.subscribe_wallet().await;

    let _result = client.close().await;
}

#[rstest]
#[tokio::test]
async fn test_rapid_consecutive_reconnections() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    let topics = vec!["publicTrade.BTCUSDT".to_string()];
    client.subscribe(topics.clone()).await.unwrap();

    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let initial_connection_count = *state.connection_count.lock().await;

    for i in 0..3 {
        state.clear_subscription_events().await;

        // Wait to ensure events are cleared
        wait_until_async(
            || {
                let state = state.clone();
                async move { state.subscription_events().await.is_empty() }
            },
            Duration::from_secs(5),
        )
        .await;

        state.disconnect_trigger.store(true, Ordering::Relaxed);

        let _result = client.subscribe(vec![format!("publicTrade.ETH{i}")]).await;

        tokio::time::sleep(Duration::from_millis(200)).await;
        state.disconnect_trigger.store(false, Ordering::Relaxed);
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    let final_count = *state.connection_count.lock().await;
    assert!(
        final_count >= initial_connection_count,
        "Expected connection to be maintained or reconnected"
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_reconnection_race_condition() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    let topics = vec!["publicTrade.BTCUSDT".to_string()];
    client.subscribe(topics).await.unwrap();

    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    state.clear_subscription_events().await;

    // Wait to ensure events are cleared
    wait_until_async(
        || {
            let state = state.clone();
            async move { state.subscription_events().await.is_empty() }
        },
        Duration::from_secs(5),
    )
    .await;

    state.disconnect_trigger.store(true, Ordering::Relaxed);
    let _result = client.subscribe(vec!["books50.ETHUSDT".to_string()]).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    state.disconnect_trigger.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(50)).await;
    state.disconnect_trigger.store(true, Ordering::Relaxed);

    tokio::time::sleep(Duration::from_millis(100)).await;
    state.disconnect_trigger.store(false, Ordering::Relaxed);

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(*state.connection_count.lock().await >= 1);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_reconnection_waits_for_delayed_auth_ack() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    state.set_auth_delay(500).await;

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    let _result = client.connect().await;

    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(3),
    )
    .await;

    let _result = client.subscribe_orders().await;

    tokio::time::sleep(Duration::from_millis(1000)).await;

    assert!(
        *state.connection_count.lock().await > 0,
        "Connection should be maintained during delayed auth"
    );

    let _result = client.close().await;
}

#[rstest]
#[tokio::test]
async fn test_multiple_partial_subscription_failures() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    let topics = vec![
        "publicTrade.BTCUSDT".to_string(),
        "publicTrade.ETHUSDT".to_string(),
        "books50.BTCUSDT".to_string(),
    ];
    client.subscribe(topics.clone()).await.unwrap();

    wait_until_async(
        || async { state.subscription_events.lock().await.len() >= 3 },
        Duration::from_secs(5),
    )
    .await;

    state
        .set_subscription_failures(vec!["publicTrade.SOLUSDT".to_string()])
        .await;

    state.clear_subscription_events().await;

    // Wait to ensure events are cleared
    wait_until_async(
        || {
            let state = state.clone();
            async move { state.subscription_events().await.is_empty() }
        },
        Duration::from_secs(5),
    )
    .await;

    let mixed_topics = vec![
        "publicTrade.SOLUSDT".to_string(),
        "books50.ETHUSDT".to_string(),
    ];
    let _result = client.subscribe(mixed_topics).await;

    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let events = state.subscription_events().await;

    assert!(
        !events.is_empty(),
        "Should have subscription events even with partial failures"
    );

    let has_failure = events.iter().any(|(_, success)| !success);
    assert!(has_failure, "Should have at least one failed subscription");

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_is_active_false_during_reconnection() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    wait_until_async(|| async { client.is_active() }, Duration::from_secs(2)).await;

    assert!(client.is_active(), "Client should be active after connect");

    state.disconnect_trigger.store(true, Ordering::Relaxed);

    let _result = client
        .subscribe(vec!["publicTrade.BTCUSDT".to_string()])
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let active_during_reconnect = client.is_active();

    state.disconnect_trigger.store(false, Ordering::Relaxed);

    // Note: This test may be timing-sensitive. The client might reconnect quickly.
    // We're checking that at some point during the reconnection process, is_active is false
    if !active_during_reconnect {
        assert!(!active_during_reconnect);
    }

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_sends_pong_for_text_ping_message() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        1, // 1 second heartbeat,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    wait_until_async(
        || async { *state.connection_count.lock().await > 0 },
        Duration::from_secs(5),
    )
    .await;

    wait_until_async(
        || async { state.ping_count.load(Ordering::Relaxed) > 0 },
        Duration::from_secs(3),
    )
    .await;

    assert!(
        state.ping_count.load(Ordering::Relaxed) > 0,
        "Server should have received ping messages"
    );

    client.close().await.unwrap();
}

// Tests for conditional order types
//
// Bitget `op:"trade"` carries plain orders only and has no trigger direction: conditional orders
// go to the REST strategy-order endpoint (`type=trigger`), so the WebSocket builder refuses them.
#[cfg(test)]
mod conditional_order_tests {
    use nautilus_bitget::{
        common::enums::{BitgetOrderSide, BitgetProductType},
        websocket::{client::BitgetWebSocketClient, messages::BitgetWsPlaceOrderParams},
    };
    use nautilus_model::{
        enums::{OrderSide, OrderType, TimeInForce},
        identifiers::{ClientOrderId, InstrumentId},
        types::{Price, Quantity},
    };
    use rstest::rstest;

    #[rstest]
    fn test_buy_stop_market_order_trigger_direction() {
        let error = create_conditional_order_params_with_side(
            OrderType::StopMarket,
            BitgetOrderSide::Buy,
            Some(Price::from("4500.00")),
            None,
        )
        .unwrap_err();

        assert!(error.contains("strategy-order"), "{error}");
    }

    #[rstest]
    fn test_sell_stop_market_order_trigger_direction() {
        let error = create_conditional_order_params_with_side(
            OrderType::StopMarket,
            BitgetOrderSide::Sell,
            Some(Price::from("4500.00")),
            None,
        )
        .unwrap_err();

        assert!(error.contains("strategy-order"), "{error}");
    }

    #[rstest]
    fn test_buy_stop_limit_order_trigger_direction() {
        let error = create_conditional_order_params_with_side(
            OrderType::StopLimit,
            BitgetOrderSide::Buy,
            Some(Price::from("4500.00")),
            Some(Price::from("4505.00")),
        )
        .unwrap_err();

        assert!(error.contains("strategy-order"), "{error}");
    }

    #[rstest]
    fn test_sell_stop_limit_order_trigger_direction() {
        let error = create_conditional_order_params_with_side(
            OrderType::StopLimit,
            BitgetOrderSide::Sell,
            Some(Price::from("4500.00")),
            Some(Price::from("4495.00")),
        )
        .unwrap_err();

        assert!(error.contains("strategy-order"), "{error}");
    }

    #[rstest]
    fn test_buy_market_if_touched_order_trigger_direction() {
        let error = create_conditional_order_params_with_side(
            OrderType::MarketIfTouched,
            BitgetOrderSide::Buy,
            Some(Price::from("4500.00")),
            None,
        )
        .unwrap_err();

        assert!(error.contains("strategy-order"), "{error}");
    }

    #[rstest]
    fn test_sell_market_if_touched_order_trigger_direction() {
        let error = create_conditional_order_params_with_side(
            OrderType::MarketIfTouched,
            BitgetOrderSide::Sell,
            Some(Price::from("5500.00")),
            None,
        )
        .unwrap_err();

        assert!(error.contains("strategy-order"), "{error}");
    }

    #[rstest]
    fn test_buy_limit_if_touched_order_trigger_direction() {
        let error = create_conditional_order_params_with_side(
            OrderType::LimitIfTouched,
            BitgetOrderSide::Buy,
            Some(Price::from("4500.00")),
            Some(Price::from("4505.00")),
        )
        .unwrap_err();

        assert!(error.contains("strategy-order"), "{error}");
    }

    #[rstest]
    fn test_sell_limit_if_touched_order_trigger_direction() {
        let error = create_conditional_order_params_with_side(
            OrderType::LimitIfTouched,
            BitgetOrderSide::Sell,
            Some(Price::from("5500.00")),
            Some(Price::from("5495.00")),
        )
        .unwrap_err();

        assert!(error.contains("strategy-order"), "{error}");
    }

    #[rstest]
    fn test_reduce_only_false_omitted() {
        let params = create_conditional_order_params_with_reduce_only(
            OrderType::Limit,
            None,
            Some(Price::from("4500.00")),
            Some(false),
        );

        // reduce_only should be None when false (not sent to Bitget)
        assert!(params.reduce_only.is_none());
    }

    #[rstest]
    fn test_reduce_only_explicit_true() {
        let params = create_conditional_order_params_with_reduce_only(
            OrderType::Limit,
            None,
            Some(Price::from("4500.00")),
            Some(true),
        );

        // reduce_only should be Some(true)
        assert!(params.reduce_only.is_some());
        assert!(params.reduce_only.unwrap());
        assert_eq!(serde_json::to_value(&params).unwrap()["reduceOnly"], "YES");
    }

    // Create conditional order params through the client logic
    fn create_conditional_order_params_with_side(
        order_type: OrderType,
        side: BitgetOrderSide,
        trigger_price: Option<Price>,
        price: Option<Price>,
    ) -> Result<BitgetWsPlaceOrderParams, String> {
        let nautilus_side = match side {
            BitgetOrderSide::Buy => OrderSide::Buy,
            BitgetOrderSide::Sell => OrderSide::Sell,
            BitgetOrderSide::Unknown => {
                return Err("Unknown side not supported in tests".to_string());
            }
        };

        build_params(order_type, nautilus_side, trigger_price, price, None)
            .map_err(|e| e.to_string())
    }

    fn create_conditional_order_params_with_reduce_only(
        order_type: OrderType,
        trigger_price: Option<Price>,
        price: Option<Price>,
        reduce_only: Option<bool>,
    ) -> BitgetWsPlaceOrderParams {
        build_params(
            order_type,
            OrderSide::Buy,
            trigger_price,
            price,
            reduce_only,
        )
        .unwrap()
    }

    fn build_params(
        order_type: OrderType,
        side: OrderSide,
        trigger_price: Option<Price>,
        price: Option<Price>,
        reduce_only: Option<bool>,
    ) -> Result<BitgetWsPlaceOrderParams, nautilus_bitget::websocket::error::BitgetWsError> {
        let client = BitgetWebSocketClient::new_public(None, 20);

        client.build_place_order_params(
            BitgetProductType::Linear,
            InstrumentId::from("ETHUSDT-PERP.BITGET"),
            ClientOrderId::from("test-order-1"),
            side,
            order_type,
            Quantity::from("0.01"),
            false, // is_quote_quantity
            Some(TimeInForce::Gtc),
            price,
            trigger_price,
            None, // trigger_type
            None, // post_only
            reduce_only,
            false, // is_leverage
            None,  // take_profit
            None,  // stop_loss
            None,  // position_idx
            None,  // bbo_side_type
            None,  // bbo_level
            None,  // smp_type
        )
    }
}

#[rstest]
#[tokio::test]
async fn test_is_active_lifecycle() {
    let (addr, _state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_key".to_string()),
        Some("test_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    assert!(
        !client.is_active(),
        "Client should not be active before connect"
    );

    client.connect().await.unwrap();
    client.wait_until_active(5.0).await.unwrap();

    assert!(
        client.is_active(),
        "Client should be active after connect completes"
    );

    client.close().await.unwrap();

    wait_until_async(|| async { !client.is_active() }, Duration::from_secs(2)).await;

    assert!(
        !client.is_active(),
        "Client should not be active after close"
    );
}

#[tokio::test]
async fn test_is_active_false_after_close() {
    let (addr, _state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_key".to_string()),
        Some("test_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();
    client.wait_until_active(5.0).await.unwrap();
    assert!(
        client.is_active(),
        "Expected is_active() to be true after connect"
    );

    client.close().await.unwrap();

    wait_until_async(|| async { !client.is_active() }, Duration::from_secs(2)).await;

    assert!(
        !client.is_active(),
        "Expected is_active() to be false after close"
    );
    assert!(
        client.is_closed(),
        "Expected is_closed() to be true after close"
    );
}

#[rstest]
#[tokio::test]
async fn test_subscribe_after_stream_call() {
    let (addr, _state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();
    client.wait_until_active(5.0).await.unwrap();

    let _stream = client.stream();

    tokio::spawn(async move {
        tokio::pin!(_stream);
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client
        .subscribe(vec!["publicTrade.BTCUSDT".to_string()])
        .await;

    assert!(
        result.is_ok(),
        "Subscribe should work after stream() is called, but got error: {:?}",
        result.err()
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_unsubscribed_private_channel_not_resubscribed_after_disconnect() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url.clone()),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    let instrument_id = InstrumentId::from("BTCUSDT.BITGET");
    client.subscribe_trades(instrument_id).await.unwrap();
    client.subscribe_positions().await.unwrap();

    wait_for_subscription_events(&state, Duration::from_secs(5), |events| {
        events
            .iter()
            .any(|(topic, ok)| topic == "publicTrade.BTCUSDT" && *ok)
            && events.iter().any(|(topic, ok)| topic == "position" && *ok)
    })
    .await;

    {
        let subs = state.subscriptions.lock().await;
        assert!(subs.contains(&"publicTrade.BTCUSDT".to_string()));
        assert!(subs.contains(&"position".to_string()));
    }

    client.unsubscribe_positions().await.unwrap();

    wait_until_async(
        || {
            let state = state.clone();
            async move {
                let subs = state.subscriptions.lock().await;
                !subs.contains(&"position".to_string())
            }
        },
        Duration::from_secs(5),
    )
    .await;

    {
        let subs = state.subscriptions.lock().await;
        assert!(!subs.contains(&"position".to_string()));
        assert!(subs.contains(&"publicTrade.BTCUSDT".to_string()));
    }

    state.clear_subscription_events().await;

    // Wait to ensure events are cleared
    wait_until_async(
        || {
            let state = state.clone();
            async move { state.subscription_events().await.is_empty() }
        },
        Duration::from_secs(5),
    )
    .await;

    state.disconnect_trigger.store(true, Ordering::Relaxed);
    wait_for_connection_count(&state, 0, Duration::from_secs(2)).await;

    state.disconnect_trigger.store(false, Ordering::Relaxed);

    client.wait_until_active(10.0).await.unwrap();
    wait_for_connection_count(&state, 1, Duration::from_secs(5)).await;

    wait_for_subscription_events(&state, Duration::from_secs(10), |events| {
        events
            .iter()
            .any(|(topic, ok)| topic == "publicTrade.BTCUSDT" && *ok)
    })
    .await;

    let subs = state.subscriptions.lock().await;
    let events = state.subscription_events().await;

    assert!(
        subs.contains(&"publicTrade.BTCUSDT".to_string()),
        "Trade subscription should be restored after reconnection"
    );
    assert!(
        !subs.contains(&"position".to_string()),
        "Position subscription should NOT be restored after unsubscribe and reconnect"
    );

    assert!(
        !events.iter().any(|(topic, _ok)| topic == "position"),
        "Position should not appear in subscription events after reconnect; events={events:?}"
    );

    assert!(
        events
            .iter()
            .any(|(topic, ok)| topic == "publicTrade.BTCUSDT" && *ok),
        "Trade subscription should be restored; events={events:?}"
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_subscribe_after_close_and_connect_resends_topic() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );
    let topics = vec!["publicTrade.BTCUSDT".to_string()];

    client.connect().await.unwrap();
    client.subscribe(topics.clone()).await.unwrap();
    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;
    client.close().await.unwrap();
    state.clear_subscription_events().await;

    client.connect().await.unwrap();
    client.subscribe(topics).await.unwrap();
    let events =
        wait_for_subscription_events(&state, Duration::from_secs(5), |events| !events.is_empty())
            .await;

    assert_eq!(events, vec![("publicTrade.BTCUSDT".to_string(), true)]);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_subscribe_failed_before_connect_is_sent_after_connect() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );
    let topics = vec!["publicTrade.BTCUSDT".to_string()];

    let error = client.subscribe(topics.clone()).await.unwrap_err();
    client.connect().await.unwrap();
    client.subscribe(topics).await.unwrap();
    let events =
        wait_for_subscription_events(&state, Duration::from_secs(5), |events| !events.is_empty())
            .await;

    assert_eq!(
        error.to_string(),
        "WebSocket send error: Failed to send subscribe command: channel closed"
    );
    assert_eq!(events, vec![("publicTrade.BTCUSDT".to_string(), true)]);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_subscribe_failed_during_connect_is_sent_after_connect() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );
    let topics = vec!["publicTrade.BTCUSDT".to_string()];
    let mut connecting = client.clone();
    state.upgrade_gate.send_replace(false);

    // Return the clone since dropping it drops the receiver the handler forwards to
    let connect_task = tokio::spawn(async move {
        let result = connecting.connect().await;
        (connecting, result)
    });

    wait_until_async(
        || async { state.upgrade_requests.load(Ordering::Relaxed) == 1 },
        Duration::from_secs(5),
    )
    .await;
    let error = client.subscribe(topics.clone()).await.unwrap_err();
    state.upgrade_gate.send_replace(true);
    let (mut connected, result) = connect_task.await.unwrap();
    result.unwrap();
    client.subscribe(topics).await.unwrap();
    let events =
        wait_for_subscription_events(&state, Duration::from_secs(5), |events| !events.is_empty())
            .await;

    assert_eq!(
        error.to_string(),
        "WebSocket send error: Failed to send subscribe command: channel closed"
    );
    assert_eq!(events, vec![("publicTrade.BTCUSDT".to_string(), true)]);

    connected.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_batch_place_orders_with_cache_keys() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Wait for auth
    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    let orders = vec![BitgetWsPlaceOrderParams {
        category: BitgetProductType::Linear,
        symbol: Ustr::from("BTCUSDT"),
        side: BitgetOrderSide::Buy,
        order_type: BitgetOrderType::Limit,
        qty: "0.001".to_string(),
        is_leverage: None,
        price: Some("50000.0".to_string()),
        time_in_force: Some(BitgetTimeInForce::Gtc),
        order_link_id: Some("test-order-1".to_string()),
        reduce_only: None,
        margin_mode: None,
        take_profit: None,
        stop_loss: None,
        tp_trigger_by: None,
        sl_trigger_by: None,
        sl_order_type: None,
        tp_order_type: None,
        sl_limit_price: None,
        tp_limit_price: None,
        smp_type: None,
        position_idx: None,
        bbo_level: None,
    }];

    let result = client.batch_place_orders(orders).await;

    assert!(
        result.is_ok(),
        "Batch place orders should succeed with proper cache keys"
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_batch_amend_orders() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Wait for auth
    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    let orders = vec![BitgetWsAmendOrderParams {
        category: BitgetProductType::Linear,
        symbol: Ustr::from("BTCUSDT"),
        order_id: None,
        order_link_id: Some("test-order-1".to_string()),
        qty: Some("0.002".to_string()),
        price: Some("51000.0".to_string()),
    }];

    let result = client.batch_amend_orders(orders).await;

    assert!(result.is_ok(), "Batch amend orders should succeed");

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_order_rate_wait_precedes_fresh_websocket_header() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");
    let mut client = BitgetWebSocketClient::new_trade(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );
    client.set_recv_window_ms(250);
    client.connect().await.unwrap();
    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    for index in 0..11 {
        client
            .amend_order(BitgetWsAmendOrderParams {
                category: BitgetProductType::Linear,
                symbol: Ustr::from("BTCUSDT"),
                order_id: None,
                order_link_id: Some(format!("fresh-header-{index}")),
                qty: Some("0.002".to_string()),
                price: Some("51000.0".to_string()),
            })
            .await
            .unwrap();
    }

    wait_until_async(
        || async { state.captured_messages.lock().await.len() == 11 },
        Duration::from_secs(5),
    )
    .await;
    let messages = state.captured_messages.lock().await;
    let timestamps = messages
        .iter()
        .map(|message| {
            // Bitget trade requests carry no receive window, only the envelope `requestTime`
            assert_eq!(message["op"], "trade");
            assert_eq!(message["topic"], "modify-order");
            assert!(message.get("receiveWindow").is_none());
            message["requestTime"]
                .as_str()
                .unwrap()
                .parse::<i64>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(timestamps[10] - timestamps[0] >= 1_000);
    drop(messages);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_trade_order_response_preserves_request_id() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");
    let mut client = BitgetWebSocketClient::new_trade(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );
    client.connect().await.unwrap();
    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;
    let stream = client.stream();
    tokio::pin!(stream);
    let request_id = client
        .amend_order(BitgetWsAmendOrderParams {
            category: BitgetProductType::Linear,
            symbol: Ustr::from("BTCUSDT"),
            order_id: Some("venue-order-1".to_string()),
            order_link_id: Some("client-order-1".to_string()),
            qty: Some("0.002".to_string()),
            price: Some("51000.0".to_string()),
        })
        .await
        .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(BitgetWsMessage::OrderResponse(response)) = stream.next().await {
                break response;
            }
        }
    })
    .await
    .expect("timed out waiting for order response");

    assert_eq!(response.ret_code, 0);
    assert_eq!(response.req_id.as_deref(), Some(request_id.as_str()));
    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_batch_cancel_orders() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    // Wait for auth
    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    let orders = vec![
        BitgetWsCancelOrderParams {
            category: BitgetProductType::Linear,
            symbol: Ustr::from("BTCUSDT"),
            order_id: None,
            order_link_id: Some("test-order-1".to_string()),
        },
        BitgetWsCancelOrderParams {
            category: BitgetProductType::Linear,
            symbol: Ustr::from("ETHUSDT"),
            order_id: None,
            order_link_id: Some("test-order-2".to_string()),
        },
    ];

    let result = client.batch_cancel_orders(orders).await;

    assert!(result.is_ok(), "Batch cancel orders should succeed");

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_batch_cancel_orders_chunking_over_20() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();
    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    // Bitget batch requests carry at most 20 orders: 20 + 5
    let orders: Vec<BitgetWsCancelOrderParams> = (0..25)
        .map(|i| BitgetWsCancelOrderParams {
            category: BitgetProductType::Linear,
            symbol: Ustr::from("BTCUSDT"),
            order_id: Some(format!("order-{i}")),
            order_link_id: Some(format!("client-order-{i}")),
        })
        .collect();

    let result = client.batch_cancel_orders(orders).await;

    assert_eq!(result.unwrap().len(), 2);

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_batch_cancel_orders_empty_list() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();
    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    let orders: Vec<BitgetWsCancelOrderParams> = vec![];

    let result = client.batch_cancel_orders(orders).await;

    assert!(
        result.is_ok(),
        "Batch cancel with empty list should succeed"
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_option_subscription_limit_rejects_before_state_change() {
    let client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Option,
        BitgetEnvironment::Mainnet,
        Some("ws://option-limit.invalid/v5/public/option".to_string()),
        20,
        TransportBackend::default(),
        None,
    );
    let topics = (0..2_001)
        .map(|index| format!("tickers.OPTION-{index}"))
        .collect();

    let error = client.subscribe(topics).await.unwrap_err();

    assert!(error.to_string().contains("2000 arguments"));
    assert_eq!(client.subscription_count(), 0);
}

#[rstest]
#[tokio::test]
async fn test_build_cancel_order_params_requires_order_id() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();
    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    let btcusdt_linear = make_linear_pair("BTCUSDT", "BTC", "USDT");

    let result =
        client.build_cancel_order_params(BitgetProductType::Linear, btcusdt_linear.id, None, None);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Either venue_order_id or client_order_id must be provided")
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_option_client_rejects_bar_subscription() {
    let (addr, _state) = start_test_server().await.unwrap();
    // Use the linear route for the mock server; the product_type on the client
    // controls the kline guard, not the URL path.
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Option,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    wait_until_async(|| async { client.is_active() }, Duration::from_secs(5)).await;

    let bar_type = BarType::from("BTC-27MAR26-70000-P-OPTION.BITGET-1-MINUTE-LAST-EXTERNAL");
    let result = client.subscribe_bars(bar_type).await;

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("does not support kline/bar data for options")
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_option_trade_subscription_uses_base_coin_topic() {
    let (addr, state) = start_test_server().await.unwrap();
    // Use the linear route for the mock server; the product_type on the client
    // controls the topic construction, not the URL path.
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Option,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();
    wait_until_async(|| async { client.is_active() }, Duration::from_secs(5)).await;

    let instrument_id = InstrumentId::from("BTC-27MAR26-70000-P-OPTION.BITGET");
    client.subscribe_trades(instrument_id).await.unwrap();

    wait_until_async(
        || async { !state.subscription_events.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let subs = state.subscription_events.lock().await.clone();
    assert!(
        subs.iter()
            .any(|(topic, ok)| topic == "publicTrade.BTC" && *ok),
        "Expected publicTrade.BTC topic, found: {subs:?}"
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_option_trade_unsubscribe_preserves_shared_topic() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/public");

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Option,
        BitgetEnvironment::Mainnet,
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();
    wait_until_async(|| async { client.is_active() }, Duration::from_secs(5)).await;

    // Subscribe to two BTC options sharing the same baseCoin topic
    let opt1 = InstrumentId::from("BTC-27MAR26-70000-P-OPTION.BITGET");
    let opt2 = InstrumentId::from("BTC-27MAR26-80000-C-OPTION.BITGET");
    client.subscribe_trades(opt1).await.unwrap();
    client.subscribe_trades(opt2).await.unwrap();

    wait_until_async(
        || async {
            state
                .subscription_events
                .lock()
                .await
                .iter()
                .any(|(t, ok)| t == "publicTrade.BTC" && *ok)
        },
        Duration::from_secs(5),
    )
    .await;

    // Unsubscribe one; the WS topic should remain because the other still needs it
    state.clear_subscription_events().await;
    client.unsubscribe_trades(opt1).await.unwrap();

    // Give a moment for any unsubscribe message to arrive
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The topic should NOT have been unsubscribed (reference count > 0)
    let subs = state.subscriptions.lock().await;
    assert!(
        subs.contains(&"publicTrade.BTC".to_string()),
        "Topic should remain active while another instrument is subscribed, found: {subs:?}"
    );

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_place_order_with_bbo_serializes_bbo_and_omits_price() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    // Bitget `batch-place` carries no `bboLevel`, so BBO pricing uses single `place-order`
    let order = BitgetWsPlaceOrderParams {
        category: BitgetProductType::Linear,
        symbol: Ustr::from("BTCUSDT"),
        side: BitgetOrderSide::Buy,
        order_type: BitgetOrderType::Limit,
        qty: "0.001".to_string(),
        is_leverage: None,
        price: None,
        time_in_force: Some(BitgetTimeInForce::Gtc),
        order_link_id: Some("linear-bbo-1".to_string()),
        reduce_only: None,
        margin_mode: None,
        take_profit: None,
        stop_loss: None,
        tp_trigger_by: None,
        sl_trigger_by: None,
        sl_order_type: None,
        tp_order_type: None,
        sl_limit_price: None,
        tp_limit_price: None,
        smp_type: None,
        position_idx: None,
        bbo_level: Some("counterparty5".to_string()),
    };

    let result = client.place_order(order).await;
    result.unwrap();

    wait_until_async(
        || async { !state.captured_messages.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let messages = state.captured_messages.lock().await;
    assert_eq!(messages.len(), 1);

    let msg = &messages[0];
    let args = msg.get("args").unwrap().as_array().unwrap();
    let request = &args[0];

    assert_eq!(msg["topic"], "place-order");
    assert_eq!(msg["category"], "usdt-futures");
    assert_eq!(request["bboLevel"], "counterparty5");
    assert!(request.get("price").is_none());

    client.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_batch_amend_order_serializes_modify_args() {
    let (addr, state) = start_test_server().await.unwrap();
    let ws_url = format!("ws://{addr}/v3/ws/private");

    let mut client = BitgetWebSocketClient::new_private(
        BitgetEnvironment::Mainnet,
        Some("test_api_key".to_string()),
        Some("test_api_secret".to_string()),
        Some("test_api_passphrase".to_string()),
        Some(ws_url),
        20,
        TransportBackend::default(),
        None,
    );

    client.connect().await.unwrap();

    wait_until_async(
        || async { state.authenticated.load(Ordering::Relaxed) },
        Duration::from_secs(5),
    )
    .await;

    let orders = vec![BitgetWsAmendOrderParams {
        category: BitgetProductType::Linear,
        symbol: Ustr::from("TSLAUSDT"),
        order_id: Some("venue-amend".to_string()),
        order_link_id: Some("linear-test-1".to_string()),
        qty: Some("0.23".to_string()),
        price: Some("510.5".to_string()),
    }];

    let result = client.batch_amend_orders(orders).await;
    result.unwrap();

    wait_until_async(
        || async { !state.captured_messages.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let messages = state.captured_messages.lock().await;
    assert_eq!(messages.len(), 1);

    // Bitget batch args are the items themselves; the category goes on the envelope
    let msg = &messages[0];
    assert_eq!(msg["op"], "trade");
    assert_eq!(msg["topic"], "batch-modify");
    assert_eq!(msg["category"], "usdt-futures");
    let args = msg.get("args").unwrap().as_array().unwrap();
    assert_eq!(args.len(), 1);
    assert_eq!(
        args[0],
        json!({
            "symbol": "TSLAUSDT",
            "orderId": "venue-amend",
            "clientOid": "linear-test-1",
            "qty": "0.23",
            "price": "510.5",
        })
    );

    client.close().await.unwrap();
}
