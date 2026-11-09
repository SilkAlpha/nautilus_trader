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

//! Pagination tests for Bitget adapter.
//!
//! This test suite covers pagination for:
//! 1. Market Data (bars/klines) - chronological ordering, multi-page fetching
//! 2. Execution Endpoints - orders, trade history, positions with cursor pagination

use std::{collections::HashMap, net::SocketAddr};

use axum::{
    Router,
    extract::{Query, State},
    response::Json,
    routing::get,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_bitget::{
    common::{consts::BITGET_VENUE, enums::BitgetProductType, parse::parse_linear_instrument},
    http::{
        client::BitgetHttpClient,
        models::{
            BitgetOpenOrdersResponse, BitgetOrderHistoryResponse, BitgetPositionListResponse,
            BitgetTradeHistoryResponse,
        },
        query::BitgetInstrumentsInfoParamsBuilder,
    },
};
use nautilus_model::{
    data::{BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::{AccountId, InstrumentId, Symbol},
};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::net::TcpListener;

// Generate mock kline data with timestamps
fn generate_kline(timestamp_ms: i64, open: &str, high: &str, low: &str, close: &str) -> Value {
    json!([
        timestamp_ms.to_string(),
        open,
        high,
        low,
        close,
        "100.0",    // volume
        "100000.0"  // turnover
    ])
}

// Mock endpoint that simulates pagination
async fn mock_klines_paginated(Query(params): Query<HashMap<String, String>>) -> Json<Value> {
    let end_ms = params
        .get("endTime")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| Timestamp::now().as_millisecond());

    // Generate bars going backwards from end_ms
    // Each bar is 1 minute apart
    let mut klines = Vec::new();

    for i in 0..1000 {
        let bar_time = end_ms - (i * 60_000);
        klines.push(generate_kline(
            bar_time, "50000.0", "50100.0", "49900.0", "50050.0",
        ));
    }

    // `/api/v3/market/candles` returns the rows as a bare `data` array
    Json(bitget_ok(&json!(klines)))
}

/// Wraps `data` in the Bitget v3 success envelope.
fn bitget_ok(data: &Value) -> Value {
    json!({
        "code": "00000",
        "msg": "success",
        "requestTime": Timestamp::now().as_millisecond(),
        "data": data
    })
}

fn load_test_data(filename: &str) -> Value {
    let path = format!("test_data/{filename}");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

// Mock instrument info endpoint
async fn mock_instruments_info(Query(_params): Query<HashMap<String, String>>) -> Json<Value> {
    Json(bitget_ok(&json!([linear_instrument_definition(
        "ETHUSDT", "ETH"
    )])))
}

async fn start_pagination_test_server() -> Result<SocketAddr, anyhow::Error> {
    let app = Router::new()
        .route("/api/v3/market/candles", get(mock_klines_paginated))
        .route("/api/v3/market/instruments", get(mock_instruments_info));

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(addr)
}

async fn init_instrument_cache(client: &BitgetHttpClient) {
    let mut params = BitgetInstrumentsInfoParamsBuilder::default();
    params.category(BitgetProductType::Linear);
    params.symbol("ETHUSDT".to_string());
    let params = params.build().unwrap();

    let response = client.get_instruments_linear(&params).await.unwrap();
    let ts_init = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();

    for definition in response.result.list {
        let instrument = parse_linear_instrument(&definition, ts_init, ts_init).unwrap();
        client.cache_instrument(instrument);
    }
}

#[rstest]
#[tokio::test]
async fn test_bars_chronological_order_single_page() {
    let addr = start_pagination_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();
    init_instrument_cache(&client).await;

    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);
    let bar_spec = BarSpecification {
        step: std::num::NonZero::new(1).unwrap(),
        aggregation: BarAggregation::Minute,
        price_type: PriceType::Last,
    };
    let bar_type = BarType::new(instrument_id, bar_spec, AggregationSource::External);

    let end = Timestamp::now();
    let start = end - SignedDuration::from_hours(1);

    let bars = client
        .request_bars(
            BitgetProductType::Linear,
            bar_type,
            Some(start),
            Some(end),
            Some(100),
            false,
        )
        .await
        .unwrap();

    // Verify we got bars
    assert!(!bars.is_empty());
    assert!(bars.len() <= 100);

    // Verify chronological order (each bar should be later than the previous)
    for i in 1..bars.len() {
        assert!(
            bars[i].ts_event >= bars[i - 1].ts_event,
            "Bars not in chronological order at index {}: {:?} should be >= {:?}",
            i,
            bars[i].ts_event,
            bars[i - 1].ts_event
        );
    }
}

#[rstest]
#[tokio::test]
async fn test_bars_chronological_order_multiple_pages() {
    let addr = start_pagination_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();
    init_instrument_cache(&client).await;

    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);
    let bar_spec = BarSpecification {
        step: std::num::NonZero::new(1).unwrap(),
        aggregation: BarAggregation::Minute,
        price_type: PriceType::Last,
    };
    let bar_type = BarType::new(instrument_id, bar_spec, AggregationSource::External);

    let end = Timestamp::now();
    let start = end - SignedDuration::from_hours(2 * 24); // Request enough to trigger multiple pages

    let bars = client
        .request_bars(
            BitgetProductType::Linear,
            bar_type,
            Some(start),
            Some(end),
            Some(1500), // More than one page (1000)
            false,
        )
        .await
        .unwrap();

    // Verify we got approximately the requested number of bars
    assert!(!bars.is_empty());
    // Should get around 1500 bars (might be slightly less due to time boundaries)
    assert!(bars.len() >= 1000, "Expected multiple pages of bars");

    // Verify strict chronological order across all pages
    for i in 1..bars.len() {
        assert!(
            bars[i].ts_event >= bars[i - 1].ts_event,
            "Bars not in chronological order at index {}: bar[{}].ts_event={:?} should be >= bar[{}].ts_event={:?}",
            i,
            i,
            bars[i].ts_event,
            i - 1,
            bars[i - 1].ts_event
        );
    }
}

#[rstest]
#[tokio::test]
async fn test_bars_limit_returns_most_recent() {
    let addr = start_pagination_test_server().await.unwrap();
    let base_url = format!("http://{addr}");

    let client = BitgetHttpClient::new(Some(base_url), 60, 3, 1000, 10_000, 5_000, None).unwrap();
    init_instrument_cache(&client).await;

    let instrument_id = InstrumentId::new(Symbol::from("ETHUSDT-PERP"), *BITGET_VENUE);
    let bar_spec = BarSpecification {
        step: std::num::NonZero::new(1).unwrap(),
        aggregation: BarAggregation::Minute,
        price_type: PriceType::Last,
    };
    let bar_type = BarType::new(instrument_id, bar_spec, AggregationSource::External);

    let end = Timestamp::now();
    let start = end - SignedDuration::from_hours(3 * 24); // Request way more than limit

    let bars = client
        .request_bars(
            BitgetProductType::Linear,
            bar_type,
            Some(start),
            Some(end),
            Some(500), // Limit to 500 bars
            false,
        )
        .await
        .unwrap();

    // Verify we got exactly the limit
    assert_eq!(bars.len(), 500);

    // Verify chronological order
    for i in 1..bars.len() {
        assert!(bars[i].ts_event >= bars[i - 1].ts_event);
    }

    // The last bar should be the most recent (close to end time)
    let last_bar_time = bars.last().unwrap().ts_event.to_datetime_utc();
    let time_diff = (end - last_bar_time).get_minutes().abs();
    assert!(
        time_diff < 100,
        "Last bar should be close to end time, but was {time_diff} minutes away"
    );
}

/// Test that BitgetOpenOrdersResponse properly deserializes with cursor
#[rstest]
fn test_open_orders_response_with_cursor() {
    // Bitget returns the smallest order id of the page as `cursor`
    let response: BitgetOpenOrdersResponse =
        serde_json::from_value(load_test_data("http_get_orders_realtime.json")).unwrap();
    assert_eq!(response.ret_code, 0);
    assert_eq!(response.result.list.len(), 2);
    assert_eq!(
        response.result.next_page_cursor,
        Some("1503520610414125056".to_string())
    );
}

/// Test that an exhausted page (`"list": null, "cursor": null`) properly deserializes
#[rstest]
fn test_open_orders_response_empty_cursor() {
    let json = r#"{
        "code": "00000",
        "msg": "success",
        "requestTime": 1794193082681,
        "data": {
            "list": null,
            "cursor": null
        }
    }"#;

    let response: BitgetOpenOrdersResponse = serde_json::from_str(json).unwrap();
    assert_eq!(response.ret_code, 0);
    assert!(response.result.list.is_empty());
    assert_eq!(response.result.next_page_cursor, None);
}

/// Test that order history response supports cursor pagination
#[rstest]
fn test_order_history_response_with_cursor() {
    let response: BitgetOrderHistoryResponse =
        serde_json::from_value(load_test_data("http_get_orders_history.json")).unwrap();
    assert_eq!(
        response.result.next_page_cursor,
        Some("1503520558971654148".to_string())
    );
}

/// Test that trade history response supports cursor pagination
#[rstest]
fn test_trade_history_response_with_cursor() {
    let response: BitgetTradeHistoryResponse =
        serde_json::from_value(load_test_data("http_get_executions.json")).unwrap();
    assert_eq!(
        response.result.next_page_cursor,
        Some("1503520348428283904".to_string())
    );

    // The request after the last page returns `"list": null, "cursor": null`
    let response: BitgetTradeHistoryResponse =
        serde_json::from_value(load_test_data("http_get_executions_empty.json")).unwrap();
    assert!(response.result.list.is_empty());
    assert_eq!(response.result.next_page_cursor, None);
}

/// Test that position list response decodes without a cursor
#[rstest]
fn test_position_list_response_with_cursor() {
    // `current-position` returns `{list}` only: one page per category
    let response: BitgetPositionListResponse =
        serde_json::from_value(load_test_data("http_get_positions.json")).unwrap();
    assert_eq!(response.result.list.len(), 1);
    assert_eq!(response.result.next_page_cursor, None);
}

/// Test the pagination loop pattern that's used in the implementation
#[rstest]
fn test_pagination_loop_pattern() {
    // Simulate pagination responses
    let responses = [
        r#"{"code": "00000", "msg": "success", "requestTime": 123, "data": {"list": ["item1", "item2"], "cursor": "page2"}}"#,
        r#"{"code": "00000", "msg": "success", "requestTime": 123, "data": {"list": ["item3", "item4"], "cursor": "page3"}}"#,
        r#"{"code": "00000", "msg": "success", "requestTime": 123, "data": {"list": ["item5"], "cursor": null}}"#,
    ];

    // Simulate the pagination loop
    let mut all_items: Vec<String> = Vec::new();
    let mut page_count = 0;

    for response_json in &responses {
        #[derive(serde::Deserialize)]
        struct MockResponse {
            data: MockResult,
        }
        #[derive(serde::Deserialize)]
        struct MockResult {
            list: Vec<String>,
            cursor: Option<String>,
        }

        let response: MockResponse = serde_json::from_str(response_json).unwrap();
        all_items.extend(response.data.list);
        page_count += 1;

        let cursor = response.data.cursor;
        if cursor.is_none() || cursor.as_ref().is_none_or(|c| c.is_empty()) {
            break;
        }
    }

    assert_eq!(page_count, 3, "Should have processed 3 pages");
    assert_eq!(all_items.len(), 5, "Should have collected 5 total items");
    assert_eq!(all_items, vec!["item1", "item2", "item3", "item4", "item5"]);
}

/// Test that pagination stops on empty cursor
#[rstest]
fn test_pagination_stops_on_empty_cursor() {
    let cursor: Option<String> = Some(String::new());

    // This is the termination condition used in the pagination loops
    let should_stop = cursor.is_none() || cursor.as_ref().is_none_or(|c| c.is_empty());

    assert!(should_stop, "Empty cursor should terminate pagination");
}

/// Test that pagination continues with valid cursor
#[rstest]
fn test_pagination_continues_with_valid_cursor() {
    let cursor: Option<String> = Some("next-page".to_string());

    // This is the termination condition used in the pagination loops
    let should_stop = cursor.is_none() || cursor.as_ref().is_none_or(|c| c.is_empty());

    assert!(!should_stop, "Valid cursor should continue pagination");
}

/// Test that limit calculation respects remaining items correctly
#[rstest]
fn test_limit_calculation() {
    // Test case 1: limit=10, total=0, should request min(10, 50) = 10
    let limit = 10u32;
    let total = 0;
    let remaining = (limit as usize).saturating_sub(total);
    let page_limit = std::cmp::min(remaining, 50);
    assert_eq!(page_limit, 10, "Should request exactly 10 items");

    // Test case 2: limit=10, total=5, should request min(5, 50) = 5
    let total = 5;
    let remaining = (limit as usize).saturating_sub(total);
    let page_limit = std::cmp::min(remaining, 50);
    assert_eq!(page_limit, 5, "Should request exactly 5 remaining items");

    // Test case 3: limit=10, total=10, should request 0
    let total = 10;
    let remaining = (limit as usize).saturating_sub(total);
    assert_eq!(remaining, 0, "Should have no remaining items to request");

    // Test case 4: limit=10, total=15, should request 0 (saturating)
    let total = 15;
    let remaining = (limit as usize).saturating_sub(total);
    assert_eq!(remaining, 0, "Should saturate at 0 when over limit");

    // Test case 5: limit=100, total=0, should request min(100, 50) = 50 (API max)
    let limit = 100u32;
    let total = 0;
    let remaining = (limit as usize).saturating_sub(total);
    let page_limit = std::cmp::min(remaining, 50);
    assert_eq!(page_limit, 50, "Should respect API maximum of 50");

    // Test case 6: limit=100, total=75, should request min(25, 50) = 25
    let total = 75;
    let remaining = (limit as usize).saturating_sub(total);
    let page_limit = std::cmp::min(remaining, 50);
    assert_eq!(page_limit, 25, "Should request exactly 25 remaining items");

    // Test case 7: no limit (None), should use usize::MAX
    let limit: Option<u32> = None;
    let total = 1000;
    let remaining = if let Some(l) = limit {
        (l as usize).saturating_sub(total)
    } else {
        usize::MAX
    };
    assert_eq!(
        remaining,
        usize::MAX,
        "Should have unlimited remaining when no limit"
    );
}

/// Test execution limit calculation with API max of 100
#[rstest]
fn test_execution_limit_calculation() {
    // Test case 1: limit=50, total=0, should request min(50, 100) = 50
    let limit = 50u32;
    let total = 0;
    let remaining = (limit as usize).saturating_sub(total);
    let page_limit = std::cmp::min(remaining, 100);
    assert_eq!(page_limit, 50, "Should request exactly 50 executions");

    // Test case 2: limit=150, total=0, should request min(150, 100) = 100 (API max)
    let limit = 150u32;
    let total = 0;
    let remaining = (limit as usize).saturating_sub(total);
    let page_limit = std::cmp::min(remaining, 100);
    assert_eq!(page_limit, 100, "Should respect API maximum of 100");

    // Test case 3: limit=150, total=100, should request min(50, 100) = 50
    let total = 100;
    let remaining = (limit as usize).saturating_sub(total);
    let page_limit = std::cmp::min(remaining, 100);
    assert_eq!(
        page_limit, 50,
        "Should request exactly 50 remaining executions"
    );
}

// ---------------------------------------------------------------------------------------------
// Execution endpoints: a `cursor` the venue stops advancing
// ---------------------------------------------------------------------------------------------

/// The cursor served with a repeating order page. Bitget returns the smallest order id of the
/// page, so a page that keeps being re-served keeps producing the same value.
const REPEATED_ORDER_CURSOR: &str = "1503520558966792192";

/// The cursor served with a repeating execution page. This is the value in
/// `test_data/http_get_executions.json` (the oldest `execId` of that page).
const REPEATED_EXECUTION_CURSOR: &str = "1503520348428283904";

/// Which endpoint stops advancing its cursor. The others serve one empty page and finish.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StallingEndpoint {
    OrderRealtime,
    OrderHistory,
    ExecutionList,
}

#[derive(Clone)]
struct CursorStallState {
    stalling: StallingEndpoint,
    cursors: std::sync::Arc<tokio::sync::Mutex<Vec<Option<String>>>>,
}

impl CursorStallState {
    fn new(stalling: StallingEndpoint) -> Self {
        Self {
            stalling,
            cursors: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }
}

/// A cancelled demo order from `test_data/http_get_orders_history.json`, so the mock
/// deserializes through the same model as a live response.
fn stall_order_row(order_id: &str) -> Value {
    let history = load_test_data("http_get_orders_history.json");
    let mut row = history["data"]["list"][3].clone();
    row["orderId"] = json!(order_id);
    row["clientOid"] = json!(format!("client-{order_id}"));
    row["symbol"] = json!("ETHUSDT");
    row
}

/// A demo fill from `test_data/http_get_executions.json`.
fn stall_execution_row(exec_id: &str) -> Value {
    let executions = load_test_data("http_get_executions.json");
    let mut row = executions["data"]["list"][0].clone();
    row["execId"] = json!(exec_id);
    row["clientOid"] = json!(format!("client-{exec_id}"));
    row["symbol"] = json!("ETHUSDT");
    row
}

fn stall_page(rows: &[Value], next_cursor: &str) -> Json<Value> {
    Json(bitget_ok(&json!({
        "list": rows,
        "cursor": next_cursor
    })))
}

fn empty_page() -> Json<Value> {
    Json(bitget_ok(&json!({ "list": null, "cursor": null })))
}

/// Mirrors the page sequence observed on a live account: a page that advances normally, then a
/// page whose cursor addresses itself and is served unchanged for every request after it.
async fn serve_stalling_page(
    state: &CursorStallState,
    cursor: Option<String>,
    endpoint: StallingEndpoint,
) -> Json<Value> {
    if state.stalling != endpoint {
        return empty_page();
    }

    state.cursors.lock().await.push(cursor.clone());

    let (rows, repeated_cursor): (Vec<Value>, &str) = if endpoint == StallingEndpoint::ExecutionList
    {
        (
            vec![stall_execution_row("EXEC-3")],
            REPEATED_EXECUTION_CURSOR,
        )
    } else {
        (
            ["ORD-3", "ORD-4", "ORD-5"]
                .iter()
                .map(|id| stall_order_row(id))
                .collect(),
            REPEATED_ORDER_CURSOR,
        )
    };

    match cursor {
        None => {
            let first = if endpoint == StallingEndpoint::ExecutionList {
                vec![stall_execution_row("EXEC-1"), stall_execution_row("EXEC-2")]
            } else {
                vec![stall_order_row("ORD-1"), stall_order_row("ORD-2")]
            };
            stall_page(&first, "page-2")
        }
        Some(_) => stall_page(&rows, repeated_cursor),
    }
}

async fn mock_stalling_order_realtime(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<CursorStallState>,
) -> Json<Value> {
    serve_stalling_page(
        &state,
        params.get("cursor").cloned(),
        StallingEndpoint::OrderRealtime,
    )
    .await
}

async fn mock_stalling_order_history(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<CursorStallState>,
) -> Json<Value> {
    serve_stalling_page(
        &state,
        params.get("cursor").cloned(),
        StallingEndpoint::OrderHistory,
    )
    .await
}

async fn mock_stalling_execution_list(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<CursorStallState>,
) -> Json<Value> {
    serve_stalling_page(
        &state,
        params.get("cursor").cloned(),
        StallingEndpoint::ExecutionList,
    )
    .await
}

async fn mock_empty_page(State(_state): State<CursorStallState>) -> Json<Value> {
    empty_page()
}

async fn mock_instruments_info_with_state(
    query: Query<HashMap<String, String>>,
    State(_state): State<CursorStallState>,
) -> Json<Value> {
    mock_instruments_info(query).await
}

async fn start_cursor_stall_server(
    stalling: StallingEndpoint,
) -> Result<(SocketAddr, CursorStallState), anyhow::Error> {
    let state = CursorStallState::new(stalling);
    let app = Router::new()
        .route(
            "/api/v3/market/instruments",
            get(mock_instruments_info_with_state),
        )
        .route(
            "/api/v3/trade/unfilled-orders",
            get(mock_stalling_order_realtime),
        )
        .route(
            "/api/v3/trade/history-orders",
            get(mock_stalling_order_history),
        )
        // Conditional orders live on the strategy-order endpoints
        .route(
            "/api/v3/trade/unfilled-strategy-orders",
            get(mock_empty_page),
        )
        .route(
            "/api/v3/trade/history-strategy-orders",
            get(mock_empty_page),
        )
        .route("/api/v3/trade/fills", get(mock_stalling_execution_list))
        .with_state(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok((addr, state))
}

async fn cursor_stall_client(addr: SocketAddr) -> BitgetHttpClient {
    let client = BitgetHttpClient::with_credentials(
        "test_api_key".to_string(),
        "test_api_secret".to_string(),
        "test_api_passphrase".to_string(),
        Some(format!("http://{addr}")),
        60,
        3,
        1000,
        10_000,
        5_000,
        None,
    )
    .unwrap();
    init_instrument_cache(&client).await;
    client
}

/// Asserts the walk reached the repeating page, then that it ended rather than following the
/// cursor forever. The cursor sequence identifies the path: only a walk that follows
/// `cursor` on this endpoint produces it, and the third entry is the request made with
/// the cursor the second page returned about itself.
///
/// The page observed on a live account addresses itself, so the walk reports the cursor as
/// stalled rather than as a longer cycle.
async fn assert_walk_reports_the_repeat<T>(
    outcome: Result<anyhow::Result<T>, tokio::time::error::Elapsed>,
    state: &CursorStallState,
    endpoint: &str,
    repeated_cursor: &str,
    call: &str,
) {
    let cursors = state.cursors.lock().await.clone();
    let repeats = cursors
        .iter()
        .filter(|cursor| cursor.as_deref() == Some(repeated_cursor))
        .count();

    assert_eq!(
        cursors.get(..3).map(<[Option<String>]>::to_vec),
        Some(vec![
            None,
            Some("page-2".to_string()),
            Some(repeated_cursor.to_string()),
        ]),
        "unexpected pagination path for {endpoint}, first four of {} requests: {:?}",
        cursors.len(),
        cursors.get(..4).unwrap_or(&cursors)
    );

    let result = outcome.unwrap_or_else(|_| {
        panic!(
            "{call} never returned; {endpoint} was requested {} times, {repeats} of them with \
             the cursor the venue repeated",
            cursors.len(),
        )
    });

    // Stopping with the rows gathered so far would hand reconciliation a truncated history it
    // cannot tell from a complete one, so the walk reports what the venue did.
    let error = result
        .err()
        .expect("a repeated cursor should be reported, not silently truncated")
        .to_string();
    assert_eq!(
        error,
        format!("{endpoint} pagination cursor did not advance from {repeated_cursor:?}")
    );
}

#[rstest]
#[tokio::test]
async fn test_order_status_reports_terminate_on_repeated_history_cursor() {
    // `generate_order_status_reports` passes `None` for the limit at both of its call sites, so
    // the `remaining == 0` exit is dead on the live reconciliation path and an empty cursor is
    // the only exit left.
    let (addr, state) = start_cursor_stall_server(StallingEndpoint::OrderHistory)
        .await
        .unwrap();
    let client = cursor_stall_client(addr).await;

    let outcome = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        client.request_order_status_reports(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            None,  // no instrument: `generate_mass_status` never sets one
            false, // open_only=false: the history endpoint is walked
            None,  // start
            None,  // end
            None,  // limit
        ),
    )
    .await;

    assert_walk_reports_the_repeat(
        outcome,
        &state,
        "/api/v3/trade/history-orders",
        REPEATED_ORDER_CURSOR,
        "request_order_status_reports",
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_order_status_reports_terminate_on_repeated_open_order_cursor() {
    // The open-order pass of the same call. Its budget is computed from the deduplicated order
    // count, so a page that repeats rows it has already seen adds nothing to it and the
    // `remaining == 0` exit stays out of reach even when a limit is given.
    let (addr, state) = start_cursor_stall_server(StallingEndpoint::OrderRealtime)
        .await
        .unwrap();
    let client = cursor_stall_client(addr).await;

    let outcome = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        client.request_order_status_reports(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            None,
            false,
            None,
            None,
            Some(100), // a limit does not bound this loop
        ),
    )
    .await;

    assert_walk_reports_the_repeat(
        outcome,
        &state,
        "/api/v3/trade/unfilled-orders",
        REPEATED_ORDER_CURSOR,
        "request_order_status_reports",
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_open_only_order_reports_terminate_on_repeated_cursor() {
    // The `open_only` branch is a third loop over the same endpoint, reached by the periodic
    // open-order check rather than by startup reconciliation. Its budget is also computed from a
    // deduplicated count, so a limit does not bound it either.
    let (addr, state) = start_cursor_stall_server(StallingEndpoint::OrderRealtime)
        .await
        .unwrap();
    let client = cursor_stall_client(addr).await;

    let outcome = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        client.request_order_status_reports(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            None,
            true, // open_only
            None,
            None,
            Some(100),
        ),
    )
    .await;

    assert_walk_reports_the_repeat(
        outcome,
        &state,
        "/api/v3/trade/unfilled-orders",
        REPEATED_ORDER_CURSOR,
        "request_order_status_reports",
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_fill_reports_terminate_on_repeated_cursor() {
    let (addr, state) = start_cursor_stall_server(StallingEndpoint::ExecutionList)
        .await
        .unwrap();
    let client = cursor_stall_client(addr).await;

    let outcome = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        client.request_fill_reports(
            AccountId::from("BITGET-UNIFIED"),
            BitgetProductType::Linear,
            None, // no instrument
            None, // start
            None, // end
            None, // limit
        ),
    )
    .await;

    assert_walk_reports_the_repeat(
        outcome,
        &state,
        "/api/v3/trade/fills",
        REPEATED_EXECUTION_CURSOR,
        "request_fill_reports",
    )
    .await;
}

/// The demo ETHUSDT row of `test_data/http_get_instruments_linear.json` under another symbol.
fn linear_instrument_definition(symbol: &str, base_coin: &str) -> Value {
    let instruments = load_test_data("http_get_instruments_linear.json");
    let mut definition = instruments["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["symbol"] == "ETHUSDT")
        .unwrap()
        .clone();
    definition["symbol"] = json!(symbol);
    definition["baseCoin"] = json!(base_coin);
    definition
}
