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

//! Provides the HTTP client integration for the [Bitget](https://bitget.com) REST API.
//!
//! Bitget API reference <https://www.bitget.com/docs/uta/intro>.

use std::{
    cmp::Reverse,
    collections::HashMap,
    fmt::{Debug, Display},
    num::NonZeroU32,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use arc_swap::ArcSwap;
use jiff::Timestamp;
use nautilus_common::cache::InstrumentLookupError;
use nautilus_core::{
    AtomicMap, AtomicTime, env::get_or_env_var_opt, nanos::UnixNanos,
    time::get_atomic_clock_realtime,
};
use nautilus_model::{
    data::{Bar, BarType, FundingRateUpdate, OrderBookDeltas, TradeTick},
    enums::{MarketStatusAction, OrderSide, OrderType, PositionSide, TimeInForce},
    events::account::state::AccountState,
    identifiers::{AccountId, ClientOrderId, InstrumentId, Symbol, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{Price, Quantity},
};
use nautilus_network::{
    http::{HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers},
    ratelimiter::quota::Quota,
    retry::{RetryConfig, RetryError, RetryManager},
};
use rust_decimal::Decimal;
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use super::{
    error::{
        BitgetCancelOrderError, BitgetHttpError, BitgetModifyOrderError, BitgetSubmitOrderError,
    },
    models::{
        BitgetAccountDetailsResponse, BitgetAccountInfoResponse, BitgetBorrowResponse,
        BitgetEscrowSubMembersResponse, BitgetFeeRate, BitgetFeeRateResponse,
        BitgetFundingResponse, BitgetInstrumentInverse, BitgetInstrumentInverseResponse,
        BitgetInstrumentLinear, BitgetInstrumentLinearResponse, BitgetInstrumentOptionResponse,
        BitgetInstrumentSpot, BitgetInstrumentSpotResponse, BitgetKlinesResponse,
        BitgetNoConvertRepayResponse, BitgetOpenOrdersResponse, BitgetOrder,
        BitgetOrderHistoryResponse, BitgetOrderbookResponse, BitgetPlaceOrderResponse,
        BitgetPositionListResponse, BitgetRepayResponse, BitgetServerTimeResponse,
        BitgetSetLeverageResponse, BitgetSetMarginModeResponse, BitgetSetTradingStopResponse,
        BitgetSubApiKeyInfo, BitgetSubApiKeysResponse, BitgetSubMember,
        BitgetSubMembersPagedResponse, BitgetSubMembersResponse, BitgetSwitchModeResponse,
        BitgetTickerData, BitgetTickerOption, BitgetTradeHistoryResponse, BitgetTradesResponse,
        BitgetUpdateMasterApiResponse, BitgetUpdateSubApiResponse, BitgetWalletBalanceResponse,
    },
    query::{
        BitgetAmendOrderParamsBuilder, BitgetBatchAmendOrderEntryBuilder,
        BitgetBatchCancelOrderEntryBuilder, BitgetBatchCancelOrderParamsBuilder,
        BitgetBatchPlaceOrderEntryBuilder, BitgetBorrowParamsBuilder,
        BitgetCancelAllOrdersParamsBuilder, BitgetCancelOrderParamsBuilder,
        BitgetCancelStrategyOrderParams, BitgetFeeRateParams, BitgetFundingParams,
        BitgetFundingParamsBuilder, BitgetInstrumentsInfoParams, BitgetKlinesParams,
        BitgetKlinesParamsBuilder, BitgetModifyStrategyOrderParams, BitgetNativeTpSlParams,
        BitgetNoConvertRepayParamsBuilder, BitgetOpenOrdersParams, BitgetOpenOrdersParamsBuilder,
        BitgetOrderHistoryParams, BitgetOrderHistoryParamsBuilder, BitgetOrderbookParams,
        BitgetOrderbookParamsBuilder, BitgetPlaceOrderParamsBuilder,
        BitgetPlaceStrategyOrderParams, BitgetPositionListParams, BitgetRepayParamsBuilder,
        BitgetSetLeverageParamsBuilder, BitgetSetMarginModeParamsBuilder,
        BitgetSetTradingStopParams, BitgetSubApiKeysParams, BitgetSubMembersPageParams,
        BitgetSwitchModeParamsBuilder, BitgetTickersParams, BitgetTradeHistoryParams,
        BitgetTradesParams, BitgetTradesParamsBuilder, BitgetUpdateMasterApiParams,
        BitgetUpdateSubApiParams, BitgetWalletBalanceParams,
    },
};
use crate::common::{
    consts::{BITGET_DEMO_HEADER, BITGET_VENUE},
    credential::{Credential, credential_env_vars},
    enums::{
        BitgetAccountType, BitgetBboSideType, BitgetCancelType, BitgetContractType,
        BitgetEnvironment, BitgetExecType, BitgetMarginMode, BitgetOpenOnly, BitgetOrderFilter,
        BitgetOrderSide, BitgetOrderSmpType, BitgetOrderType, BitgetPositionIdx,
        BitgetPositionMode, BitgetProductType, BitgetRepayStatus, BitgetTpSlMode,
    },
    models::{BitgetCursorListResponse, BitgetErrorCheck, BitgetResponseCheck},
    parse::{
        bar_spec_to_bitget_interval, make_bitget_symbol, map_time_in_force, parse_account_state,
        parse_fill_report, parse_funding_rate, parse_inverse_instrument, parse_kline_bar,
        parse_linear_instrument, parse_order_status_report, parse_orderbook,
        parse_position_status_report, parse_spot_instrument, parse_trade_tick, spot_leverage,
        spot_market_unit, trigger_direction, validate_client_oid,
    },
    rate_limit::{
        BITGET_RATE_LIMIT_STATUS_HEADER, BitgetRateLimiter, batch_call_limit, batch_endpoint_limit,
        batch_send_limit, batch_weight, category_from_payload,
    },
    retry::should_retry_http,
    symbol::BitgetSymbol,
    urls::bitget_http_base_url,
};

const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;

trait BuilderResultExt<T> {
    fn build_anyhow(self) -> anyhow::Result<T>;
}

impl<T, E: Display> BuilderResultExt<T> for Result<T, E> {
    fn build_anyhow(self) -> anyhow::Result<T> {
        self.map_err(|e| anyhow::anyhow!("{e}"))
    }
}

const BITGET_INSTRUMENTS_INFO: &str = "/api/v3/market/instruments";
const BITGET_ORDER_REALTIME: &str = "/api/v3/trade/unfilled-orders";
const BITGET_ORDER_HISTORY: &str = "/api/v3/trade/history-orders";
const BITGET_EXECUTION_LIST: &str = "/api/v3/trade/fills";
const BITGET_POSITION_LIST: &str = "/api/v3/position/current-position";
const BITGET_ORDER_INFO: &str = "/api/v3/trade/order-info";
const BITGET_STRATEGY_ORDER_REALTIME: &str = "/api/v3/trade/unfilled-strategy-orders";
const BITGET_STRATEGY_ORDER_HISTORY: &str = "/api/v3/trade/history-strategy-orders";

/// Bitget codes for an order that does not exist (or is no longer open).
const BITGET_UNKNOWN_ORDER_CODES: [i32; 5] = [25204, 45057, 22001, 45055, 45134];

/// Returns `true` when a Bitget `offTime` (`-1` or empty when unset) schedules a halt.
fn is_scheduled_off(off_time: &str) -> bool {
    off_time.parse::<i64>().is_ok_and(|ms| ms > 0)
}

/// Keeps only the orders matching the requested IDs (Bitget list endpoints take no ID filter).
fn retain_order_ids(
    orders: &mut Vec<BitgetOrder>,
    order_id: Option<&str>,
    order_link_id: Option<&str>,
) {
    if let Some(order_id) = order_id {
        orders.retain(|order| order.order_id.as_str() == order_id);
    } else if let Some(order_link_id) = order_link_id {
        orders.retain(|order| order.order_link_id.as_str() == order_link_id);
    }
}

/// Returns the error for an endpoint Bitget does not offer through this adapter.
fn unsupported(feature: &str) -> BitgetHttpError {
    BitgetHttpError::ValidationError(format!("{feature} is not supported by the Bitget adapter"))
}

/// Tracks the cursors one paginated walk has already followed.
///
/// A venue can serve a page whose `cursor` addresses that same page. Requesting it
/// returns the same rows and the same cursor, so an empty cursor never arrives and the walk does
/// not terminate. A cursor that does not advance, or that the walk has already followed, has no
/// further page to offer.
///
/// The two cases are separated because they describe different venue behavior: a cursor that
/// repeats the one just used has stopped advancing, while a cursor seen earlier in the walk means
/// the pages have cycled. `nautilus-polymarket` draws the same distinction in
/// `http::pagination::PaginationError`.
#[derive(Debug, Default)]
struct CursorWalk {
    followed: AHashSet<String>,
    last: Option<String>,
}

impl CursorWalk {
    /// Returns the cursor to follow next, or `None` when the walk is complete.
    ///
    /// # Errors
    ///
    /// Returns an error if the cursor stops advancing or repeats one already followed. Stopping
    /// with the rows gathered so far would hand the caller a truncated result it cannot distinguish
    /// from a complete one.
    fn advance(
        &mut self,
        endpoint: &str,
        cursor: Option<String>,
    ) -> anyhow::Result<Option<String>> {
        let Some(cursor) = cursor.filter(|cursor| !cursor.is_empty()) else {
            return Ok(None);
        };

        anyhow::ensure!(
            self.last.as_deref() != Some(cursor.as_str()),
            "{endpoint} pagination cursor did not advance from {cursor:?}",
        );
        anyhow::ensure!(
            self.followed.insert(cursor.clone()),
            "{endpoint} pagination repeated cursor {cursor:?}",
        );

        self.last = Some(cursor.clone());

        Ok(Some(cursor))
    }
}

/// Legacy conservative Bitget REST quota retained for source compatibility.
pub static BITGET_REST_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(10).expect("non-zero")).expect("valid constant")
});

/// Legacy Bitget repayment quota retained for source compatibility.
pub static BITGET_REPAY_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(1).expect("non-zero")).expect("valid constant")
});

/// Raw HTTP client for low-level Bitget API operations.
///
/// This client handles request/response operations with the Bitget API,
/// returning venue-specific response types. It does not parse to Nautilus domain types.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
#[derive(Clone)]
pub struct BitgetRawHttpClient {
    base_url: String,
    client: Arc<ArcSwap<HttpClient>>,
    rate_limiter: BitgetRateLimiter,
    credential: Option<Credential>,
    recv_window_ms: u64,
    demo_trading: Arc<AtomicBool>,
    timeout_secs: u64,
    proxy_url: Option<String>,
    session_generation: Arc<AtomicU64>,
    retry_manager: RetryManager<BitgetHttpError>,
    cancellation_token: Arc<parking_lot::Mutex<CancellationToken>>,
}

impl Default for BitgetRawHttpClient {
    fn default() -> Self {
        Self::new(None, 60, 3, 1000, 10_000, DEFAULT_RECV_WINDOW_MS, None)
            .expect("Failed to create default BitgetRawHttpClient")
    }
}

impl Debug for BitgetRawHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BitgetRawHttpClient))
            .field("base_url", &self.base_url)
            .field("has_credentials", &self.credential.is_some())
            .field("recv_window_ms", &self.recv_window_ms)
            .finish()
    }
}

impl BitgetRawHttpClient {
    /// Cancels all pending HTTP requests.
    pub fn cancel_all_requests(&self) {
        self.cancellation_token.lock().cancel();
    }

    /// Replaces the cancelled token with a fresh one so subsequent
    /// requests are not immediately short-circuited.
    pub fn reset_cancellation_token(&self) {
        let mut guard = self.cancellation_token.lock();
        *guard = CancellationToken::new();
    }

    /// Returns a clone of the current cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.lock().clone()
    }

    /// Creates a new [`BitgetRawHttpClient`] using the default Bitget HTTP URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the retry manager cannot be created.
    pub fn new(
        base_url: Option<String>,
        timeout_secs: u64,
        max_retries: u32,
        retry_delay_ms: u64,
        retry_delay_max_ms: u64,
        recv_window_ms: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        let retry_config = RetryConfig {
            max_retries,
            initial_delay_ms: retry_delay_ms,
            max_delay_ms: retry_delay_max_ms,
            backoff_factor: 2.0,
            jitter_ms: 1000,
            operation_timeout_ms: Some(60_000),
            immediate_first: false,
            max_elapsed_ms: Some(180_000),
        };

        let retry_manager = RetryManager::new(retry_config);
        let base_url = base_url
            .unwrap_or_else(|| bitget_http_base_url(BitgetEnvironment::Mainnet).to_string());
        let rate_limiter = BitgetRateLimiter::for_http(&base_url, None, proxy_url.as_deref());
        let client = Self::build_http_client(timeout_secs, proxy_url.clone())?;
        let session_generation = rate_limiter.http_session_generation();

        Ok(Self {
            base_url,
            client: Arc::new(ArcSwap::from_pointee(client)),
            rate_limiter,
            credential: None,
            recv_window_ms,
            demo_trading: Arc::new(AtomicBool::new(false)),
            timeout_secs,
            proxy_url,
            session_generation: Arc::new(AtomicU64::new(session_generation)),
            retry_manager,
            cancellation_token: Arc::new(parking_lot::Mutex::new(CancellationToken::new())),
        })
    }

    /// Creates a new [`BitgetRawHttpClient`] configured with credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    #[expect(clippy::too_many_arguments)]
    pub fn with_credentials(
        api_key: String,
        api_secret: String,
        api_passphrase: String,
        base_url: Option<String>,
        timeout_secs: u64,
        max_retries: u32,
        retry_delay_ms: u64,
        retry_delay_max_ms: u64,
        recv_window_ms: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        let retry_config = RetryConfig {
            max_retries,
            initial_delay_ms: retry_delay_ms,
            max_delay_ms: retry_delay_max_ms,
            backoff_factor: 2.0,
            jitter_ms: 1000,
            operation_timeout_ms: Some(60_000),
            immediate_first: false,
            max_elapsed_ms: Some(180_000),
        };

        let retry_manager = RetryManager::new(retry_config);
        let base_url = base_url
            .unwrap_or_else(|| bitget_http_base_url(BitgetEnvironment::Mainnet).to_string());
        let credential = Credential::new(api_key, api_secret, api_passphrase);
        let rate_limiter = BitgetRateLimiter::for_http(
            &base_url,
            Some(credential.api_key()),
            proxy_url.as_deref(),
        );
        let client = Self::build_http_client(timeout_secs, proxy_url.clone())?;
        let session_generation = rate_limiter.http_session_generation();

        Ok(Self {
            base_url,
            client: Arc::new(ArcSwap::from_pointee(client)),
            rate_limiter,
            credential: Some(credential),
            recv_window_ms,
            demo_trading: Arc::new(AtomicBool::new(false)),
            timeout_secs,
            proxy_url,
            session_generation: Arc::new(AtomicU64::new(session_generation)),
            retry_manager,
            cancellation_token: Arc::new(parking_lot::Mutex::new(CancellationToken::new())),
        })
    }

    /// Creates a new [`BitgetRawHttpClient`] with environment variable credential resolution.
    ///
    /// If `api_key`, `api_secret` or `api_passphrase` are not provided, they will be loaded from
    /// environment variables based on the environment flags:
    /// - Demo (and testnet, which Bitget does not have): `BITGET_DEMO_API_KEY`,
    ///   `BITGET_DEMO_API_SECRET`, `BITGET_DEMO_API_PASSPHRASE`
    /// - Mainnet: `BITGET_API_KEY`, `BITGET_API_SECRET`, `BITGET_API_PASSPHRASE`
    ///
    /// Demo trading sends the `paptrading: 1` header on every request.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    #[expect(clippy::too_many_arguments)]
    pub fn new_with_env(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url: Option<String>,
        demo: bool,
        testnet: bool,
        timeout_secs: u64,
        max_retries: u32,
        retry_delay_ms: u64,
        retry_delay_max_ms: u64,
        recv_window_ms: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        let environment = if demo {
            BitgetEnvironment::Demo
        } else if testnet {
            BitgetEnvironment::Testnet
        } else {
            BitgetEnvironment::Mainnet
        };
        let base_url =
            Some(base_url.unwrap_or_else(|| bitget_http_base_url(environment).to_string()));
        let (key_var, secret_var, passphrase_var) = credential_env_vars(environment);
        let key = get_or_env_var_opt(api_key, key_var);
        let secret = get_or_env_var_opt(api_secret, secret_var);
        let passphrase = get_or_env_var_opt(api_passphrase, passphrase_var);

        let client = if let (Some(k), Some(s), Some(p)) = (key, secret, passphrase) {
            Self::with_credentials(
                k,
                s,
                p,
                base_url,
                timeout_secs,
                max_retries,
                retry_delay_ms,
                retry_delay_max_ms,
                recv_window_ms,
                proxy_url,
            )?
        } else {
            Self::new(
                base_url,
                timeout_secs,
                max_retries,
                retry_delay_ms,
                retry_delay_max_ms,
                recv_window_ms,
                proxy_url,
            )?
        };
        client.set_demo_trading(environment != BitgetEnvironment::Mainnet);
        Ok(client)
    }

    /// Routes requests to Bitget demo (paper) trading by sending `paptrading: 1`.
    ///
    /// Bitget serves demo trading from the mainnet host, selected by this header.
    pub fn set_demo_trading(&self, demo_trading: bool) {
        self.demo_trading.store(demo_trading, Ordering::Release);
    }

    /// Returns `true` if requests are routed to Bitget demo trading.
    #[must_use]
    pub fn is_demo_trading(&self) -> bool {
        self.demo_trading.load(Ordering::Acquire)
    }

    fn default_headers() -> HashMap<String, String> {
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert("locale".to_string(), "en-US".to_string());
        headers
    }

    fn build_http_client(
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<HttpClient, BitgetHttpError> {
        HttpClient::builder()
            .redirect_policy(HttpRedirectPolicy::Reject)
            .headers(Self::default_headers())
            .header_keys(vec![BITGET_RATE_LIMIT_STATUS_HEADER.to_string()])
            .timeout_secs(timeout_secs)
            .maybe_proxy_url(proxy_url)
            .rate_limiters(Vec::new())
            .build()
            .map_err(|e| {
                BitgetHttpError::NetworkError(format!("Failed to create HTTP client: {e}"))
            })
    }

    fn refresh_http_session(&self, generation: u64) -> Result<(), BitgetHttpError> {
        let current = self.session_generation.load(Ordering::Acquire);
        if current == generation {
            return Ok(());
        }

        let client = Self::build_http_client(self.timeout_secs, self.proxy_url.clone())?;
        self.client.store(Arc::new(client));
        self.session_generation.store(generation, Ordering::Release);
        Ok(())
    }

    fn request_rate_limit(
        endpoint: &str,
        payload: Option<&str>,
    ) -> (Option<BitgetProductType>, u32) {
        let category = category_from_payload(payload);
        let weight = if endpoint.contains("batch") {
            // Bitget batch bodies are bare JSON arrays of entries
            let order_count = payload
                .and_then(|payload| serde_json::from_str::<serde_json::Value>(payload).ok())
                .and_then(|value| value.as_array().map(Vec::len))
                .unwrap_or(1);
            category.map_or(1, |category| batch_weight(category, order_count))
        } else {
            1
        };
        (category, weight)
    }

    fn observe_rate_limit_headers(
        &self,
        endpoint: &str,
        category: Option<BitgetProductType>,
        headers: &HashMap<String, String>,
    ) {
        // Bitget reports only the remaining quota of the endpoint
        let Some(remaining) = headers
            .get(BITGET_RATE_LIMIT_STATUS_HEADER)
            .and_then(|value| value.parse::<u32>().ok())
        else {
            return;
        };
        self.rate_limiter
            .observe_remaining(endpoint, category, remaining);
    }

    // Bitget answers an exceeded quota with HTTP 429
    fn is_rate_limit_403(status: u16, body: &str) -> bool {
        let _ = body;
        status == 429
    }

    fn sign_request(
        &self,
        timestamp: &str,
        method: &str,
        request_path: &str,
        body: Option<&str>,
    ) -> Result<HashMap<String, String>, BitgetHttpError> {
        let credential = self
            .credential
            .as_ref()
            .ok_or(BitgetHttpError::MissingCredentials)?;

        let signature = credential.sign_with_payload(timestamp, method, request_path, body);

        let mut headers = HashMap::new();
        headers.insert("ACCESS-KEY".to_string(), credential.api_key().to_string());
        headers.insert("ACCESS-SIGN".to_string(), signature);
        headers.insert("ACCESS-TIMESTAMP".to_string(), timestamp.to_string());
        headers.insert(
            "ACCESS-PASSPHRASE".to_string(),
            credential.api_passphrase().to_string(),
        );

        Ok(headers)
    }

    async fn send_request<T: DeserializeOwned + BitgetResponseCheck, P: Serialize>(
        &self,
        method: Method,
        endpoint: &str,
        params: Option<&P>,
        body: Option<Vec<u8>>,
        authenticate: bool,
    ) -> Result<T, BitgetHttpError> {
        let endpoint = endpoint.to_string();
        let url = format!("{}{endpoint}", self.base_url);
        let method_clone = method.clone();
        let body_clone = body.clone();

        // Serialize params before closure to avoid reference lifetime issues
        let params_str = if method == Method::GET {
            params
                .map(serde_urlencoded::to_string)
                .transpose()
                .map_err(|e| {
                    BitgetHttpError::JsonError(format!("Failed to serialize params: {e}"))
                })?
        } else {
            None
        };

        let operation = || {
            let url = url.clone();
            let method = method_clone.clone();
            let body = body_clone.clone();
            let endpoint = endpoint.clone();
            let params_str = params_str.clone();

            async move {
                let full_url = if let Some(ref query) = params_str {
                    if query.is_empty() {
                        url
                    } else {
                        format!("{url}?{query}")
                    }
                } else {
                    url
                };

                let sign_payload = if method == Method::GET {
                    params_str.as_deref()
                } else {
                    body.as_ref()
                        .and_then(|body| std::str::from_utf8(body).ok())
                };
                let request_path = match params_str.as_deref() {
                    Some(query) if !query.is_empty() => format!("{endpoint}?{query}"),
                    _ => endpoint.clone(),
                };
                let sign_body = if method == Method::GET {
                    None
                } else {
                    sign_payload
                };
                let (category, weight) = Self::request_rate_limit(&endpoint, sign_payload);

                let generation = self.rate_limiter.http_session_generation();
                self.refresh_http_session(generation)?;
                self.rate_limiter
                    .acquire_http(&endpoint, category, weight, authenticate)
                    .await
                    .map_err(BitgetHttpError::ValidationError)?;
                let generation = self.rate_limiter.http_session_generation();
                self.refresh_http_session(generation)?;

                let mut headers = Self::default_headers();

                if authenticate {
                    let timestamp = get_atomic_clock_realtime().get_time_ms().to_string();
                    let auth_headers =
                        self.sign_request(&timestamp, method.as_str(), &request_path, sign_body)?;
                    headers.extend(auth_headers);
                }

                if self.is_demo_trading() {
                    headers.insert(BITGET_DEMO_HEADER.to_string(), "1".to_string());
                }

                if method == Method::POST || method == Method::PUT {
                    headers.insert("Content-Type".to_string(), "application/json".to_string());
                }

                let response = self
                    .client
                    .load()
                    .request(method, full_url, None, Some(headers), body, None, None)
                    .await?;

                self.observe_rate_limit_headers(&endpoint, category, &response.headers);

                if response.status.as_u16() >= 400 {
                    let body = String::from_utf8_lossy(&response.body).to_string();
                    if Self::is_rate_limit_403(response.status.as_u16(), &body) {
                        let generation = self.rate_limiter.reset_http_sessions();
                        self.refresh_http_session(generation)?;
                    }

                    // Bitget reports business errors (e.g. 25204 unknown order) with HTTP 400 and
                    // the regular `{code, msg}` envelope
                    if response.status.as_u16() != 429
                        && let Ok(error_check) =
                            serde_json::from_slice::<BitgetErrorCheck>(&response.body)
                        && error_check.ret_code != 0
                    {
                        return Err(BitgetHttpError::BitgetError {
                            error_code: error_check.ret_code as i32,
                            message: error_check.ret_msg,
                        });
                    }
                    return Err(BitgetHttpError::UnexpectedStatus {
                        status: response.status.as_u16(),
                        body,
                    });
                }

                // Try to deserialize into the target type
                match serde_json::from_slice::<T>(&response.body) {
                    Ok(result) => {
                        // Check for API-level errors
                        if result.ret_code() != 0 {
                            return Err(BitgetHttpError::BitgetError {
                                error_code: result.ret_code() as i32,
                                message: result.ret_msg().to_string(),
                            });
                        }
                        Ok(result)
                    }
                    Err(json_err) => {
                        // Deserialization failed - check if it's a Bitget error response
                        // (error responses often have result: null which fails typed deserialization)
                        if let Ok(error_check) =
                            serde_json::from_slice::<BitgetErrorCheck>(&response.body)
                            && error_check.ret_code != 0
                        {
                            return Err(BitgetHttpError::BitgetError {
                                error_code: error_check.ret_code as i32,
                                message: error_check.ret_msg,
                            });
                        }
                        // Not a Bitget error, propagate the JSON parse error
                        Err(json_err.into())
                    }
                }
            }
        };

        let create_error = |error: RetryError| -> BitgetHttpError {
            match error {
                RetryError::Canceled => {
                    BitgetHttpError::Canceled("Adapter disconnecting or shutting down".to_string())
                }
                error => BitgetHttpError::NetworkError(error.to_string()),
            }
        };

        let token = self.cancellation_token();

        self.retry_manager
            .invocation(
                endpoint.as_str(),
                operation,
                should_retry_http,
                create_error,
            )
            .cancellation_token(&token)
            .execute()
            .await
    }

    #[cfg(test)]
    fn build_path<S: Serialize>(base: &str, params: &S) -> Result<String, BitgetHttpError> {
        let query = serde_urlencoded::to_string(params)
            .map_err(|e| BitgetHttpError::JsonError(e.to_string()))?;

        if query.is_empty() {
            Ok(base.to_owned())
        } else {
            Ok(format!("{base}?{query}"))
        }
    }

    /// Fetches the current server time from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_server_time(&self) -> Result<BitgetServerTimeResponse, BitgetHttpError> {
        self.send_request::<_, ()>(Method::GET, "/api/v3/market/time", None, None, false)
            .await
    }

    /// Fetches instrument information from Bitget for a given product category.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments<T: DeserializeOwned + BitgetResponseCheck>(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<T, BitgetHttpError> {
        self.send_request(
            Method::GET,
            BITGET_INSTRUMENTS_INFO,
            Some(params),
            None,
            false,
        )
        .await
    }

    /// Fetches spot instrument information from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments_spot(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<BitgetInstrumentSpotResponse, BitgetHttpError> {
        self.get_instruments(params).await
    }

    /// Fetches linear instrument information from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments_linear(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<BitgetInstrumentLinearResponse, BitgetHttpError> {
        self.get_instruments(params).await
    }

    /// Fetches inverse instrument information from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments_inverse(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<BitgetInstrumentInverseResponse, BitgetHttpError> {
        self.get_instruments(params).await
    }

    /// Fetches option instrument information from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments_option(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<BitgetInstrumentOptionResponse, BitgetHttpError> {
        self.get_instruments(params).await
    }

    /// Fetches kline/candlestick data from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_klines(
        &self,
        params: &BitgetKlinesParams,
    ) -> Result<BitgetKlinesResponse, BitgetHttpError> {
        self.send_request(
            Method::GET,
            "/api/v3/market/candles",
            Some(params),
            None,
            false,
        )
        .await
    }

    /// Fetches recent trades from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_recent_trades(
        &self,
        params: &BitgetTradesParams,
    ) -> Result<BitgetTradesResponse, BitgetHttpError> {
        self.send_request(
            Method::GET,
            "/api/v3/market/fills",
            Some(params),
            None,
            false,
        )
        .await
    }

    /// Fetches funding data from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/derivatives>
    pub async fn get_funding_history(
        &self,
        params: &BitgetFundingParams,
    ) -> Result<BitgetFundingResponse, BitgetHttpError> {
        self.send_request(
            Method::GET,
            "/api/v3/market/history-fund-rate",
            Some(params),
            None,
            false,
        )
        .await
    }

    /// Fetches orderbook from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_orderbook(
        &self,
        params: &BitgetOrderbookParams,
    ) -> Result<BitgetOrderbookResponse, BitgetHttpError> {
        self.send_request(
            Method::GET,
            "/api/v3/market/orderbook",
            Some(params),
            None,
            false,
        )
        .await
    }

    /// Fetches open orders (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # Panics
    ///
    /// Panics if the parameter builder fails (should never happen with valid inputs).
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/order-management>
    #[expect(clippy::too_many_arguments)]
    pub async fn get_open_orders(
        &self,
        category: BitgetProductType,
        symbol: Option<String>,
        base_coin: Option<String>,
        settle_coin: Option<String>,
        order_id: Option<String>,
        order_link_id: Option<String>,
        open_only: Option<BitgetOpenOnly>,
        order_filter: Option<BitgetOrderFilter>,
        limit: Option<u32>,
        cursor: Option<String>,
    ) -> Result<BitgetOpenOrdersResponse, BitgetHttpError> {
        let mut builder = BitgetOpenOrdersParamsBuilder::default();
        builder.category(category);

        if let Some(s) = symbol {
            builder.symbol(s);
        }

        if let Some(bc) = base_coin {
            builder.base_coin(bc);
        }

        if let Some(sc) = settle_coin {
            builder.settle_coin(sc);
        }

        if let Some(oi) = order_id {
            builder.order_id(oi);
        }

        if let Some(ol) = order_link_id {
            builder.order_link_id(ol);
        }

        if let Some(oo) = open_only {
            builder.open_only(oo);
        }

        if let Some(of) = order_filter {
            builder.order_filter(of);
        }

        if let Some(l) = limit {
            builder.limit(l);
        }

        if let Some(c) = cursor {
            builder.cursor(c);
        }

        let params = builder
            .build()
            .expect("Failed to build BitgetOpenOrdersParams");

        self.get_open_orders_with(&params).await
    }

    /// Fetches open (or recently closed) orders for prepared parameters.
    ///
    /// Bitget has no combined open-order query: a single order comes from `order-info`,
    /// recent closed orders from the history endpoints and conditional orders from the
    /// strategy-order endpoints (which take no order ID filter, so it is applied here).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_open_orders_with(
        &self,
        params: &BitgetOpenOrdersParams,
    ) -> Result<BitgetOpenOrdersResponse, BitgetHttpError> {
        let is_strategy = params
            .order_filter
            .is_some_and(|filter| filter != BitgetOrderFilter::Order);

        if !is_strategy && (params.order_id.is_some() || params.order_link_id.is_some()) {
            return self
                .get_order_info(
                    params.category,
                    params.order_id.clone(),
                    params.order_link_id.clone(),
                )
                .await;
        }

        let endpoint = match (params.open_only, is_strategy) {
            (Some(BitgetOpenOnly::ClosedRecent), false) => BITGET_ORDER_HISTORY,
            (Some(BitgetOpenOnly::ClosedRecent), true) => BITGET_STRATEGY_ORDER_HISTORY,
            (_, false) => BITGET_ORDER_REALTIME,
            (_, true) => BITGET_STRATEGY_ORDER_REALTIME,
        };

        let mut response: BitgetOpenOrdersResponse = self
            .send_request(Method::GET, endpoint, Some(params), None, true)
            .await?;
        retain_order_ids(
            &mut response.result.list,
            params.order_id.as_deref(),
            params.order_link_id.as_deref(),
        );
        Ok(response)
    }

    /// Fetches closed orders for prepared parameters (strategy history for conditional orders).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_order_history_with(
        &self,
        params: &BitgetOrderHistoryParams,
    ) -> Result<BitgetOrderHistoryResponse, BitgetHttpError> {
        let is_strategy = params
            .order_filter
            .is_some_and(|filter| filter != BitgetOrderFilter::Order);
        let endpoint = if is_strategy {
            BITGET_STRATEGY_ORDER_HISTORY
        } else {
            BITGET_ORDER_HISTORY
        };

        let mut response: BitgetOrderHistoryResponse = self
            .send_request(Method::GET, endpoint, Some(params), None, true)
            .await?;
        retain_order_ids(
            &mut response.result.list,
            params.order_id.as_deref(),
            params.order_link_id.as_deref(),
        );
        Ok(response)
    }

    /// Fetches one order by venue or client order ID (`GET /api/v3/trade/order-info`),
    /// returned as a one-element page (empty when Bitget no longer knows the order).
    ///
    /// Bitget keeps cancelled API orders for only two hours.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_order_info(
        &self,
        category: BitgetProductType,
        order_id: Option<String>,
        order_link_id: Option<String>,
    ) -> Result<BitgetOpenOrdersResponse, BitgetHttpError> {
        let mut params: Vec<(&str, String)> = Vec::with_capacity(1);
        if let Some(order_id) = order_id {
            params.push(("orderId", order_id));
        } else if let Some(order_link_id) = order_link_id {
            params.push(("clientOid", order_link_id));
        }

        let result = self
            .send_request::<crate::common::models::BitgetResponse<BitgetOrder>, _>(
                Method::GET,
                BITGET_ORDER_INFO,
                Some(&params),
                None,
                true,
            )
            .await;

        let (order, ret_code, ret_msg, time) = match result {
            Ok(response) => (
                Some(response.result),
                response.ret_code,
                response.ret_msg,
                response.time,
            ),
            Err(BitgetHttpError::BitgetError {
                error_code,
                message,
            }) if BITGET_UNKNOWN_ORDER_CODES.contains(&error_code) => {
                log::debug!("Order not found on Bitget: {error_code} {message}");
                (None, 0, message, None)
            }
            Err(e) => return Err(e),
        };

        Ok(crate::common::models::BitgetResponse {
            ret_code,
            ret_msg,
            result: crate::common::models::BitgetCursorList {
                list: order.into_iter().collect(),
                next_page_cursor: None,
                category: Some(category),
            },
            ret_ext_info: None,
            time,
        })
    }

    /// Places a new order (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/order-management>
    pub async fn place_order(
        &self,
        request: &serde_json::Value,
    ) -> Result<BitgetPlaceOrderResponse, BitgetHttpError> {
        let body = serde_json::to_vec(request)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/trade/place-order",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Places a conditional (strategy) order (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn place_strategy_order(
        &self,
        request: &serde_json::Value,
    ) -> Result<BitgetPlaceOrderResponse, BitgetHttpError> {
        let body = serde_json::to_vec(request)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/trade/place-strategy-order",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Cancels a conditional (strategy) order (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn cancel_strategy_order(
        &self,
        params: &BitgetCancelStrategyOrderParams,
    ) -> Result<BitgetPlaceOrderResponse, BitgetHttpError> {
        let body = serde_json::to_vec(params)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/trade/cancel-strategy-order",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Modifies a conditional (strategy) order (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn modify_strategy_order(
        &self,
        params: &BitgetModifyStrategyOrderParams,
    ) -> Result<BitgetPlaceOrderResponse, BitgetHttpError> {
        let body = serde_json::to_vec(params)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/trade/modify-strategy-order",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Fetches wallet balance (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn get_wallet_balance(
        &self,
        params: &BitgetWalletBalanceParams,
    ) -> Result<BitgetWalletBalanceResponse, BitgetHttpError> {
        self.send_request(
            Method::GET,
            "/api/v3/account/assets",
            Some(params),
            None,
            true,
        )
        .await
    }

    /// Fetches account information (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/account-settings>
    pub async fn get_account_info(&self) -> Result<BitgetAccountInfoResponse, BitgetHttpError> {
        self.send_request::<_, ()>(Method::GET, "/api/v3/account/settings", None, None, true)
            .await
    }

    /// Fetches account details (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_account_details(
        &self,
    ) -> Result<BitgetAccountDetailsResponse, BitgetHttpError> {
        self.send_request::<_, ()>(Method::GET, "/api/v3/account/info", None, None, true)
            .await
    }

    /// Modifies a sub-account API key (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn update_sub_api_key(
        &self,
        params: &BitgetUpdateSubApiParams,
    ) -> Result<BitgetUpdateSubApiResponse, BitgetHttpError> {
        // `/api/v3/user/update-sub-api` needs a master key and its schema is unverified
        let _ = params;
        Err(unsupported("Sub-account API key update"))
    }

    /// Modifies the master API key that issued the request (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn update_master_api_key(
        &self,
        params: &BitgetUpdateMasterApiParams,
    ) -> Result<BitgetUpdateMasterApiResponse, BitgetHttpError> {
        // Bitget has no endpoint to modify the calling master API key
        let _ = params;
        Err(unsupported("Master API key update"))
    }

    /// Fetches the sub-account list (up to 1000 rows, non-paginated).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_sub_members(&self) -> Result<BitgetSubMembersResponse, BitgetHttpError> {
        // `/api/v3/user/sub-list` needs a master key and its schema is unverified
        Err(unsupported("Sub-account listing"))
    }

    /// Fetches a cursor-paginated sub-account list (`/api/v3/user/sub-list`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_sub_members_paged(
        &self,
        params: &BitgetSubMembersPageParams,
    ) -> Result<BitgetSubMembersPagedResponse, BitgetHttpError> {
        // `/api/v3/user/sub-list` needs a master key and its schema is unverified
        let _ = params;
        Err(unsupported("Sub-account listing"))
    }

    /// Fetches fund-custodial sub-accounts (`/api/v3/user/sub-list`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_escrow_sub_members(
        &self,
        params: &BitgetSubMembersPageParams,
    ) -> Result<BitgetEscrowSubMembersResponse, BitgetHttpError> {
        // Bitget has no fund-custodial (escrow) sub-account listing
        let _ = params;
        Err(unsupported("Escrow sub-account listing"))
    }

    /// Fetches all API keys belonging to a given sub-account.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_sub_api_keys(
        &self,
        params: &BitgetSubApiKeysParams,
    ) -> Result<BitgetSubApiKeysResponse, BitgetHttpError> {
        // `/api/v3/user/sub-api-list` needs a master key and its schema is unverified
        let _ = params;
        Err(unsupported("Sub-account API key listing"))
    }

    /// Fetches every sub-account page via `/api/v3/user/sub-list` and returns the
    /// flattened list. Walks the cursor until Bitget signals end-of-pages.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails or the response cannot be parsed.
    pub async fn fetch_all_sub_members_paged(
        &self,
        page_size: Option<u32>,
    ) -> Result<Vec<BitgetSubMember>, BitgetHttpError> {
        let mut members = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let params = BitgetSubMembersPageParams {
                page_size,
                next_cursor: cursor.take(),
            };
            let mut page = self.get_sub_members_paged(&params).await?;
            let next = page.result.continuation_cursor().map(str::to_owned);
            members.append(&mut page.result.sub_members);

            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        Ok(members)
    }

    /// Fetches every fund-custodial page via `/api/v3/user/sub-list` and
    /// returns the flattened list. Walks the cursor until Bitget signals
    /// end-of-pages.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails or the response cannot be parsed.
    pub async fn fetch_all_escrow_sub_members(
        &self,
        page_size: Option<u32>,
    ) -> Result<Vec<BitgetSubMember>, BitgetHttpError> {
        let mut members = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let params = BitgetSubMembersPageParams {
                page_size,
                next_cursor: cursor.take(),
            };
            let mut page = self.get_escrow_sub_members(&params).await?;
            let next = page.result.continuation_cursor().map(str::to_owned);
            members.append(&mut page.result.sub_members);

            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        Ok(members)
    }

    /// Fetches every page of sub-account API keys for `sub_member_id` and
    /// returns the flattened list. Walks the cursor until Bitget signals
    /// end-of-pages.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails or the response cannot be parsed.
    pub async fn fetch_all_sub_api_keys(
        &self,
        sub_member_id: impl Into<String>,
        limit: Option<u32>,
    ) -> Result<Vec<BitgetSubApiKeyInfo>, BitgetHttpError> {
        let sub_member_id = sub_member_id.into();
        let mut keys = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let params = BitgetSubApiKeysParams {
                sub_member_id: sub_member_id.clone(),
                limit,
                cursor: cursor.take(),
            };
            let mut page = self.get_sub_api_keys(&params).await?;
            let next = page.result.continuation_cursor().map(str::to_owned);
            keys.append(&mut page.result.keys);

            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        Ok(keys)
    }

    /// Fetches trading fee rates for symbols.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn get_fee_rate(
        &self,
        params: &BitgetFeeRateParams,
    ) -> Result<BitgetFeeRateResponse, BitgetHttpError> {
        self.send_request(
            Method::GET,
            "/api/v3/account/all-fee-rate",
            Some(params),
            None,
            true,
        )
        .await
    }

    /// Sets the margin mode for the account.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # Panics
    ///
    /// Panics if required parameters are not provided (should not happen with current implementation).
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/account-settings>
    pub async fn set_margin_mode(
        &self,
        margin_mode: BitgetMarginMode,
    ) -> Result<BitgetSetMarginModeResponse, BitgetHttpError> {
        let params = BitgetSetMarginModeParamsBuilder::default()
            .set_margin_mode(margin_mode)
            .build()
            .expect("Failed to build BitgetSetMarginModeParams");

        let body = serde_json::to_vec(&params)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/account/adjust-account-mode",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Sets leverage for a symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # Panics
    ///
    /// Panics if required parameters are not provided (should not happen with current implementation).
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/account-settings>
    pub async fn set_leverage(
        &self,
        product_type: BitgetProductType,
        symbol: &str,
        buy_leverage: &str,
        sell_leverage: &str,
    ) -> Result<BitgetSetLeverageResponse, BitgetHttpError> {
        let params = BitgetSetLeverageParamsBuilder::default()
            .category(product_type)
            .symbol(symbol.to_string())
            .buy_leverage(buy_leverage.to_string())
            .sell_leverage(sell_leverage.to_string())
            .build()
            .expect("Failed to build BitgetSetLeverageParams");

        let body = serde_json::to_vec(&params)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/account/set-leverage",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Switches position mode for a product type.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # Panics
    ///
    /// Panics if required parameters are not provided (should not happen with current implementation).
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/account-settings>
    pub async fn switch_mode(
        &self,
        product_type: BitgetProductType,
        mode: BitgetPositionMode,
        symbol: Option<String>,
        coin: Option<String>,
    ) -> Result<BitgetSwitchModeResponse, BitgetHttpError> {
        let mut builder = BitgetSwitchModeParamsBuilder::default();
        builder.category(product_type);
        builder.mode(mode);

        if let Some(s) = symbol {
            builder.symbol(s);
        }

        if let Some(c) = coin {
            builder.coin(c);
        }

        let params = builder
            .build()
            .expect("Failed to build BitgetSwitchModeParams");

        let body = serde_json::to_vec(&params)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/account/set-hold-mode",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Sets trading stop parameters including trailing stops.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/position-management>
    pub async fn set_trading_stop(
        &self,
        params: &BitgetSetTradingStopParams,
    ) -> Result<BitgetSetTradingStopResponse, BitgetHttpError> {
        // Bitget sets position TP/SL as a `tpsl` strategy order
        let mut value = serde_json::to_value(params)?;
        if let Some(map) = value.as_object_mut() {
            map.insert("type".to_string(), serde_json::Value::from("tpsl"));
        }
        let body = serde_json::to_vec(&value)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/trade/place-strategy-order",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Manually borrows coins for margin trading.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - Insufficient collateral for the borrow.
    ///
    /// # Panics
    ///
    /// Panics if the parameter builder fails (should never happen with valid inputs).
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn borrow(
        &self,
        coin: &str,
        amount: &str,
    ) -> Result<BitgetBorrowResponse, BitgetHttpError> {
        let params = BitgetBorrowParamsBuilder::default()
            .coin(coin.to_string())
            .amount(amount.to_string())
            .build()
            .expect("Failed to build BitgetBorrowParams");

        let body = serde_json::to_vec(&params)?;
        self.send_request::<_, ()>(
            Method::POST,
            "/api/v3/account/borrow",
            None,
            Some(body),
            true,
        )
        .await
    }

    /// Manually repays borrowed coins without asset conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - Called during the hourly interest-calculation window (mm:04:00-mm:05:30 UTC each hour).
    /// - Insufficient spot balance for repayment.
    ///
    /// # Panics
    ///
    /// Panics if the parameter builder fails (should never happen with valid inputs).
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn no_convert_repay(
        &self,
        coin: &str,
        amount: Option<&str>,
    ) -> Result<BitgetNoConvertRepayResponse, BitgetHttpError> {
        let mut builder = BitgetNoConvertRepayParamsBuilder::default();
        builder.coin(coin.to_string());

        if let Some(amt) = amount {
            builder.amount(amt.to_string());
        }

        let params = builder
            .build()
            .expect("Failed to build BitgetNoConvertRepayParams");

        if let Ok(params_json) = serde_json::to_string(&params) {
            log::debug!("Repay request params: {params_json}");
        }

        let body = serde_json::to_vec(&params)?;
        let result = self
            .send_request::<_, ()>(
                Method::POST,
                "/api/v3/account/repay",
                None,
                Some(body),
                true,
            )
            .await;

        if let Err(ref e) = result
            && let Ok(params_json) = serde_json::to_string(&params)
        {
            log::debug!("Repay request failed with params {params_json}: {e}");
        }

        result
    }

    /// Manually repays borrowed coins, converting other assets if required.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - Called during the hourly interest-calculation window (mm:04:00-mm:05:30 UTC each hour).
    /// - Insufficient balance for repayment.
    ///
    /// # Panics
    ///
    /// Panics if the parameter builder fails (should never happen with valid inputs).
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn repay(
        &self,
        coin: Option<&str>,
        amount: Option<&str>,
    ) -> Result<BitgetRepayResponse, BitgetHttpError> {
        let mut builder = BitgetRepayParamsBuilder::default();

        if let Some(coin) = coin {
            builder.coin(coin.to_string());
        }

        if let Some(amt) = amount {
            builder.amount(amt.to_string());
        }

        let params = builder.build().expect("Failed to build BitgetRepayParams");

        if let Ok(params_json) = serde_json::to_string(&params) {
            log::debug!("Repay request params: {params_json}");
        }

        let body = serde_json::to_vec(&params)?;
        let result = self
            .send_request::<_, ()>(
                Method::POST,
                "/api/v3/account/repay",
                None,
                Some(body),
                true,
            )
            .await;

        if let Err(ref e) = result
            && let Ok(params_json) = serde_json::to_string(&params)
        {
            log::debug!("Repay request failed with params {params_json}: {e}");
        }

        result
    }

    /// Fetches tickers for market data.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_tickers<T: DeserializeOwned + BitgetResponseCheck>(
        &self,
        params: &BitgetTickersParams,
    ) -> Result<T, BitgetHttpError> {
        self.send_request(
            Method::GET,
            "/api/v3/market/tickers",
            Some(params),
            None,
            false,
        )
        .await
    }

    /// Fetches trade execution history (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/order-management>
    pub async fn get_trade_history(
        &self,
        params: &BitgetTradeHistoryParams,
    ) -> Result<BitgetTradeHistoryResponse, BitgetHttpError> {
        self.send_request(Method::GET, BITGET_EXECUTION_LIST, Some(params), None, true)
            .await
    }

    /// Fetches position information (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/position-management>
    pub async fn get_positions(
        &self,
        params: &BitgetPositionListParams,
    ) -> Result<BitgetPositionListResponse, BitgetHttpError> {
        self.send_request(Method::GET, BITGET_POSITION_LIST, Some(params), None, true)
            .await
    }

    /// Returns the base URL used for requests.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns the configured receive window in milliseconds.
    #[must_use]
    pub fn recv_window_ms(&self) -> u64 {
        self.recv_window_ms
    }

    /// Returns the API credential if configured.
    #[must_use]
    pub fn credential(&self) -> Option<&Credential> {
        self.credential.as_ref()
    }
}

/// Provides a HTTP client for connecting to the [Bitget](https://bitget.com) REST API.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
/// High-level HTTP client that wraps the raw client and provides Nautilus domain types.
///
/// This client maintains an instrument cache and uses it to parse venue responses
/// into Nautilus domain objects.
pub struct BitgetHttpClient {
    pub(crate) inner: Arc<BitgetRawHttpClient>,
    pub(crate) instruments_cache: Arc<AtomicMap<Ustr, InstrumentAny>>,
    clock: &'static AtomicTime,
    cache_initialized: Arc<AtomicBool>,
    use_spot_position_reports: Arc<AtomicBool>,
}

impl Clone for BitgetHttpClient {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            instruments_cache: self.instruments_cache.clone(),
            cache_initialized: self.cache_initialized.clone(),
            use_spot_position_reports: self.use_spot_position_reports.clone(),
            clock: self.clock,
        }
    }
}

impl Default for BitgetHttpClient {
    fn default() -> Self {
        Self::new(None, 60, 3, 1000, 10_000, DEFAULT_RECV_WINDOW_MS, None)
            .expect("Failed to create default BitgetHttpClient")
    }
}

impl Debug for BitgetHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BitgetHttpClient))
            .field("inner", &self.inner)
            .finish()
    }
}

impl BitgetHttpClient {
    /// Creates a new [`BitgetHttpClient`] using the default Bitget HTTP URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the retry manager cannot be created.
    pub fn new(
        base_url: Option<String>,
        timeout_secs: u64,
        max_retries: u32,
        retry_delay_ms: u64,
        retry_delay_max_ms: u64,
        recv_window_ms: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Ok(Self {
            inner: Arc::new(BitgetRawHttpClient::new(
                base_url,
                timeout_secs,
                max_retries,
                retry_delay_ms,
                retry_delay_max_ms,
                recv_window_ms,
                proxy_url,
            )?),
            instruments_cache: Arc::new(AtomicMap::new()),
            cache_initialized: Arc::new(AtomicBool::new(false)),
            use_spot_position_reports: Arc::new(AtomicBool::new(false)),
            clock: get_atomic_clock_realtime(),
        })
    }

    /// Creates a new [`BitgetHttpClient`] configured with credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if the retry manager cannot be created.
    #[expect(clippy::too_many_arguments)]
    pub fn with_credentials(
        api_key: String,
        api_secret: String,
        api_passphrase: String,
        base_url: Option<String>,
        timeout_secs: u64,
        max_retries: u32,
        retry_delay_ms: u64,
        retry_delay_max_ms: u64,
        recv_window_ms: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Ok(Self {
            inner: Arc::new(BitgetRawHttpClient::with_credentials(
                api_key,
                api_secret,
                api_passphrase,
                base_url,
                timeout_secs,
                max_retries,
                retry_delay_ms,
                retry_delay_max_ms,
                recv_window_ms,
                proxy_url,
            )?),
            instruments_cache: Arc::new(AtomicMap::new()),
            cache_initialized: Arc::new(AtomicBool::new(false)),
            use_spot_position_reports: Arc::new(AtomicBool::new(false)),
            clock: get_atomic_clock_realtime(),
        })
    }

    /// Creates a new [`BitgetHttpClient`] with optional credentials resolved from environment variables.
    ///
    /// Credentials are resolved in the following order:
    /// 1. Use provided `api_key`/`api_secret`/`api_passphrase` if `Some`
    /// 2. Fall back to environment variables based on environment:
    ///    - Demo (and testnet, which Bitget does not have): `BITGET_DEMO_API_KEY`,
    ///      `BITGET_DEMO_API_SECRET`, `BITGET_DEMO_API_PASSPHRASE`
    ///    - Mainnet: `BITGET_API_KEY`, `BITGET_API_SECRET`, `BITGET_API_PASSPHRASE`
    ///
    /// # Errors
    ///
    /// Returns an error if the retry manager cannot be created.
    #[expect(clippy::too_many_arguments)]
    pub fn new_with_env(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url: Option<String>,
        demo: bool,
        testnet: bool,
        timeout_secs: u64,
        max_retries: u32,
        retry_delay_ms: u64,
        retry_delay_max_ms: u64,
        recv_window_ms: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Ok(Self {
            inner: Arc::new(BitgetRawHttpClient::new_with_env(
                api_key,
                api_secret,
                api_passphrase,
                base_url,
                demo,
                testnet,
                timeout_secs,
                max_retries,
                retry_delay_ms,
                retry_delay_max_ms,
                recv_window_ms,
                proxy_url,
            )?),
            instruments_cache: Arc::new(AtomicMap::new()),
            cache_initialized: Arc::new(AtomicBool::new(false)),
            use_spot_position_reports: Arc::new(AtomicBool::new(false)),
            clock: get_atomic_clock_realtime(),
        })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        self.inner.base_url()
    }

    #[must_use]
    pub fn recv_window_ms(&self) -> u64 {
        self.inner.recv_window_ms()
    }

    #[must_use]
    pub fn credential(&self) -> Option<&Credential> {
        self.inner.credential()
    }

    /// Routes requests to Bitget demo (paper) trading (`paptrading: 1` header).
    pub fn set_demo_trading(&self, demo_trading: bool) {
        self.inner.set_demo_trading(demo_trading);
    }

    pub fn set_use_spot_position_reports(&self, use_spot_position_reports: bool) {
        self.use_spot_position_reports
            .store(use_spot_position_reports, Ordering::Relaxed);
    }

    pub fn cancel_all_requests(&self) {
        self.inner.cancel_all_requests();
    }

    pub fn reset_cancellation_token(&self) {
        self.inner.reset_cancellation_token();
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation_token()
    }

    /// Any existing instrument with the same symbol will be replaced.
    pub fn cache_instrument(&self, instrument: InstrumentAny) {
        self.instruments_cache
            .insert(instrument.symbol().inner(), instrument);
        self.cache_initialized.store(true, Ordering::Release);
    }

    /// Any existing instruments with the same symbols will be replaced.
    pub fn cache_instruments(&self, instruments: &[InstrumentAny]) {
        self.instruments_cache.rcu(|m| {
            for instrument in instruments {
                m.insert(instrument.symbol().inner(), instrument.clone());
            }
        });
        self.cache_initialized.store(true, Ordering::Release);
    }

    pub fn get_instrument(&self, symbol: &Ustr) -> Option<InstrumentAny> {
        self.instruments_cache.get_cloned(symbol)
    }

    fn instrument_from_cache(&self, symbol: &Symbol) -> anyhow::Result<InstrumentAny> {
        self.get_instrument(&symbol.inner()).ok_or_else(|| {
            anyhow::anyhow!(
                "Instrument {symbol} not found in cache, ensure instruments loaded first"
            )
        })
    }

    #[must_use]
    fn generate_ts_init(&self) -> UnixNanos {
        self.clock.get_time_ns()
    }

    /// Fetches the current server time from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_server_time(&self) -> Result<BitgetServerTimeResponse, BitgetHttpError> {
        self.inner.get_server_time().await
    }

    /// Fetches instrument information from Bitget for a given product category.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments<T: DeserializeOwned + BitgetResponseCheck>(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<T, BitgetHttpError> {
        self.inner.get_instruments(params).await
    }

    /// Fetches spot instrument information from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments_spot(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<BitgetInstrumentSpotResponse, BitgetHttpError> {
        self.inner.get_instruments_spot(params).await
    }

    /// Fetches linear instrument information from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments_linear(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<BitgetInstrumentLinearResponse, BitgetHttpError> {
        self.inner.get_instruments_linear(params).await
    }

    /// Fetches inverse instrument information from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments_inverse(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<BitgetInstrumentInverseResponse, BitgetHttpError> {
        self.inner.get_instruments_inverse(params).await
    }

    /// Fetches option instrument information from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_instruments_option(
        &self,
        params: &BitgetInstrumentsInfoParams,
    ) -> Result<BitgetInstrumentOptionResponse, BitgetHttpError> {
        self.inner.get_instruments_option(params).await
    }

    /// Fetches kline/candlestick data from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_klines(
        &self,
        params: &BitgetKlinesParams,
    ) -> Result<BitgetKlinesResponse, BitgetHttpError> {
        self.inner.get_klines(params).await
    }

    /// Fetches recent trades from Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn get_recent_trades(
        &self,
        params: &BitgetTradesParams,
    ) -> Result<BitgetTradesResponse, BitgetHttpError> {
        self.inner.get_recent_trades(params).await
    }

    /// Fetches open orders (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/order-management>
    #[expect(clippy::too_many_arguments)]
    pub async fn get_open_orders(
        &self,
        category: BitgetProductType,
        symbol: Option<String>,
        base_coin: Option<String>,
        settle_coin: Option<String>,
        order_id: Option<String>,
        order_link_id: Option<String>,
        open_only: Option<BitgetOpenOnly>,
        order_filter: Option<BitgetOrderFilter>,
        limit: Option<u32>,
        cursor: Option<String>,
    ) -> Result<BitgetOpenOrdersResponse, BitgetHttpError> {
        self.inner
            .get_open_orders(
                category,
                symbol,
                base_coin,
                settle_coin,
                order_id,
                order_link_id,
                open_only,
                order_filter,
                limit,
                cursor,
            )
            .await
    }

    /// Places a new order (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/order-management>
    pub async fn place_order(
        &self,
        request: &serde_json::Value,
    ) -> Result<BitgetPlaceOrderResponse, BitgetHttpError> {
        self.inner.place_order(request).await
    }

    /// Fetches wallet balance (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn get_wallet_balance(
        &self,
        params: &BitgetWalletBalanceParams,
    ) -> Result<BitgetWalletBalanceResponse, BitgetHttpError> {
        self.inner.get_wallet_balance(params).await
    }

    /// Fetches account information (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/account-settings>
    pub async fn get_account_info(&self) -> Result<BitgetAccountInfoResponse, BitgetHttpError> {
        self.inner.get_account_info().await
    }

    /// Fetches API key information including account details (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    pub async fn get_account_details(
        &self,
    ) -> Result<BitgetAccountDetailsResponse, BitgetHttpError> {
        self.inner.get_account_details().await
    }

    /// Modifies a sub-account API key (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    pub async fn update_sub_api_key(
        &self,
        params: &BitgetUpdateSubApiParams,
    ) -> Result<BitgetUpdateSubApiResponse, BitgetHttpError> {
        self.inner.update_sub_api_key(params).await
    }

    /// Modifies the master API key that issued the request (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    pub async fn update_master_api_key(
        &self,
        params: &BitgetUpdateMasterApiParams,
    ) -> Result<BitgetUpdateMasterApiResponse, BitgetHttpError> {
        self.inner.update_master_api_key(params).await
    }

    /// Fetches the sub-account list (up to 1000 rows, non-paginated).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    pub async fn get_sub_members(&self) -> Result<BitgetSubMembersResponse, BitgetHttpError> {
        self.inner.get_sub_members().await
    }

    /// Fetches a cursor-paginated sub-account list.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    pub async fn get_sub_members_paged(
        &self,
        params: &BitgetSubMembersPageParams,
    ) -> Result<BitgetSubMembersPagedResponse, BitgetHttpError> {
        self.inner.get_sub_members_paged(params).await
    }

    /// Fetches fund-custodial sub-accounts.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    pub async fn get_escrow_sub_members(
        &self,
        params: &BitgetSubMembersPageParams,
    ) -> Result<BitgetEscrowSubMembersResponse, BitgetHttpError> {
        self.inner.get_escrow_sub_members(params).await
    }

    /// Fetches all API keys belonging to a given sub-account.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The response cannot be parsed.
    pub async fn get_sub_api_keys(
        &self,
        params: &BitgetSubApiKeysParams,
    ) -> Result<BitgetSubApiKeysResponse, BitgetHttpError> {
        self.inner.get_sub_api_keys(params).await
    }

    /// Fetches position information (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/position-management>
    pub async fn get_positions(
        &self,
        params: &BitgetPositionListParams,
    ) -> Result<BitgetPositionListResponse, BitgetHttpError> {
        self.inner.get_positions(params).await
    }

    /// Fetches fee rate (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn get_fee_rate(
        &self,
        params: &BitgetFeeRateParams,
    ) -> Result<BitgetFeeRateResponse, BitgetHttpError> {
        self.inner.get_fee_rate(params).await
    }

    /// Sets margin mode (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/account-settings>
    pub async fn set_margin_mode(
        &self,
        margin_mode: BitgetMarginMode,
    ) -> Result<BitgetSetMarginModeResponse, BitgetHttpError> {
        self.inner.set_margin_mode(margin_mode).await
    }

    /// Sets leverage for a symbol (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/account-settings>
    pub async fn set_leverage(
        &self,
        product_type: BitgetProductType,
        symbol: &str,
        buy_leverage: &str,
        sell_leverage: &str,
    ) -> Result<BitgetSetLeverageResponse, BitgetHttpError> {
        self.inner
            .set_leverage(product_type, symbol, buy_leverage, sell_leverage)
            .await
    }

    /// Switches position mode (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/account/account-settings>
    pub async fn switch_mode(
        &self,
        product_type: BitgetProductType,
        mode: BitgetPositionMode,
        symbol: Option<String>,
        coin: Option<String>,
    ) -> Result<BitgetSwitchModeResponse, BitgetHttpError> {
        self.inner
            .switch_mode(product_type, mode, symbol, coin)
            .await
    }

    /// Sets trading stop parameters including trailing stops (requires authentication).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    ///
    /// # References
    ///
    /// - <https://www.bitget.com/docs/catalog/trading/position-management>
    pub async fn set_trading_stop(
        &self,
        params: &BitgetSetTradingStopParams,
    ) -> Result<BitgetSetTradingStopResponse, BitgetHttpError> {
        self.inner.set_trading_stop(params).await
    }

    /// Get the outstanding spot borrow amount for a specific coin.
    ///
    /// Returns zero if no borrow exists.
    ///
    /// # Parameters
    ///
    /// - `coin`: The coin to check (e.g., "BTC", "ETH")
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The coin is not found in the wallet.
    pub async fn get_spot_borrow_amount(&self, coin: &str) -> anyhow::Result<Decimal> {
        let params = BitgetWalletBalanceParams {
            account_type: BitgetAccountType::Unified,
            coin: Some(coin.to_string()),
        };

        let response = self.inner.get_wallet_balance(&params).await?;

        // Bitget reports borrowed coins as the unified account `debt`
        let borrow_amount = response
            .result
            .coin
            .iter()
            .find(|c| c.coin == coin)
            .map(|balance| balance.borrow_amount.as_str())
            .filter(|debt| !debt.is_empty())
            .map_or(Ok(Decimal::ZERO), |debt| {
                debt.parse::<Decimal>()
                    .with_context(|| format!("invalid debt '{debt}' for {coin}"))
            })?;

        Ok(borrow_amount)
    }

    /// Borrows coins for spot margin trading.
    ///
    /// This should be called before opening short spot positions.
    ///
    /// # Parameters
    ///
    /// - `coin`: The coin to repay (e.g., "BTC", "ETH")
    /// - `amount`: Optional amount to borrow. If None, repays all outstanding borrows.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - Insufficient collateral for the borrow.
    pub async fn borrow_spot(
        &self,
        coin: &str,
        amount: Quantity,
    ) -> anyhow::Result<BitgetBorrowResponse> {
        let amount_str = amount.to_string();
        self.inner
            .borrow(coin, &amount_str)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to borrow {amount} {coin}: {e}"))
    }

    /// Repays spot borrows for a specific coin.
    ///
    /// This should be called after closing short spot positions to avoid accruing interest.
    ///
    /// # Parameters
    ///
    /// - `coin`: The coin to repay (e.g., "BTC", "ETH")
    /// - `amount`: Optional amount to repay. If None, repays all outstanding borrows.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - Called during the hourly interest-calculation window (mm:04:00-mm:05:30 UTC each hour).
    /// - Insufficient spot balance for repayment.
    pub async fn repay_spot_borrow(
        &self,
        coin: &str,
        amount: Option<Quantity>,
    ) -> anyhow::Result<BitgetNoConvertRepayResponse> {
        let amount_str = amount.as_ref().map(|q| q.to_string());
        let response = self
            .inner
            .no_convert_repay(coin, amount_str.as_deref())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to repay spot borrow for {coin}: {e}"))?;
        Self::ensure_repay_accepted(coin, response.result.result_status)?;
        Ok(response)
    }

    /// Repays spot borrows for a specific coin, converting other assets if required.
    ///
    /// Unlike [`Self::repay_spot_borrow`], this uses the venue's manual repay endpoint,
    /// which may draw on other holdings when the debt coin's spot balance is insufficient.
    ///
    /// # Parameters
    ///
    /// - `coin`: The coin to repay (e.g., "BTC", "ETH")
    /// - `amount`: Optional amount to repay. If None, repays all outstanding borrows.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - Called during the hourly interest-calculation window (mm:04:00-mm:05:30 UTC each hour).
    /// - Insufficient balance for repayment.
    pub async fn repay_spot_borrow_with_conversion(
        &self,
        coin: &str,
        amount: Option<Quantity>,
    ) -> anyhow::Result<BitgetRepayResponse> {
        let amount_str = amount.as_ref().map(|q| q.to_string());
        let response = self
            .inner
            .repay(Some(coin), amount_str.as_deref())
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to repay spot borrow (with conversion) for {coin}: {e}")
            })?;
        Self::ensure_repay_accepted(coin, response.result.result_status)?;
        Ok(response)
    }

    fn ensure_repay_accepted(coin: &str, status: BitgetRepayStatus) -> anyhow::Result<()> {
        anyhow::ensure!(
            status != BitgetRepayStatus::Failed,
            "Bitget repay for {coin} returned result status {status}"
        );
        Ok(())
    }

    /// Generate SPOT position reports from wallet balances.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The wallet balance request fails.
    /// - Parsing fails.
    async fn generate_spot_position_reports_from_wallet(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let params = BitgetWalletBalanceParams {
            account_type: BitgetAccountType::Unified,
            coin: None,
        };

        let response = self.inner.get_wallet_balance(&params).await?;
        let ts_init = self.generate_ts_init();

        let mut wallet_by_coin: HashMap<Ustr, Decimal> = HashMap::new();

        for coin_balance in &response.result.coin {
            // Bitget reports borrowed coins as `debt`
            let debt = coin_balance
                .borrow_amount
                .parse::<Decimal>()
                .unwrap_or(Decimal::ZERO);
            let balance = coin_balance.wallet_balance - debt;
            *wallet_by_coin
                .entry(coin_balance.coin)
                .or_insert(Decimal::ZERO) += balance;
        }

        let mut reports = Vec::new();

        if let Some(instrument) = self
            .instruments_cache
            .get_cloned(&instrument_id.symbol.inner())
        {
            let base_currency = instrument
                .base_currency()
                .expect("SPOT instrument should have base currency");
            let coin = base_currency.code;
            let wallet_balance = wallet_by_coin.get(&coin).copied().unwrap_or(Decimal::ZERO);

            let side = if wallet_balance > Decimal::ZERO {
                PositionSide::Long
            } else if wallet_balance < Decimal::ZERO {
                PositionSide::Short
            } else {
                PositionSide::Flat
            };

            let abs_balance = wallet_balance.abs();
            let quantity = Quantity::from_decimal_dp(abs_balance, instrument.size_precision())?;

            let report = PositionStatusReport::new(
                account_id,
                instrument_id,
                side,
                quantity,
                ts_init,
                ts_init,
                None,
                None,
                None,
            );

            reports.push(report);
        }

        Ok(reports)
    }

    /// Submit a new order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - Order validation fails.
    /// - The order is rejected.
    /// - The API returns an error.
    #[expect(clippy::too_many_arguments)]
    pub async fn submit_order(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        order_side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        time_in_force: Option<TimeInForce>,
        price: Option<Price>,
        trigger_price: Option<Price>,
        post_only: Option<bool>,
        reduce_only: bool,
        is_quote_quantity: bool,
        is_leverage: bool,
        position_idx: Option<BitgetPositionIdx>,
        bbo_side_type: Option<BitgetBboSideType>,
        bbo_level: Option<String>,
        smp_type: Option<BitgetOrderSmpType>,
        native_tp_sl: Option<&BitgetNativeTpSlParams>,
    ) -> anyhow::Result<OrderStatusReport> {
        let instrument = self.instrument_from_cache(&instrument_id.symbol)?;
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;
        validate_client_oid(client_order_id.as_str())?;

        let bitget_side = match order_side {
            OrderSide::Buy => BitgetOrderSide::Buy,
            OrderSide::Sell => BitgetOrderSide::Sell,
        };

        // For stop/conditional orders, Bitget uses Market/Limit with trigger parameters
        let (bitget_order_type, is_stop_order) = match order_type {
            OrderType::Market => (BitgetOrderType::Market, false),
            OrderType::Limit => (BitgetOrderType::Limit, false),
            OrderType::StopMarket | OrderType::MarketIfTouched => (BitgetOrderType::Market, true),
            OrderType::StopLimit | OrderType::LimitIfTouched => (BitgetOrderType::Limit, true),
            _ => anyhow::bail!("Unsupported order type: {order_type:?}"),
        };

        let bitget_tif = map_time_in_force(bitget_order_type, time_in_force, post_only)
            .map_err(|tif| anyhow::anyhow!("Unsupported time in force: {tif:?}"))?;
        // Bitget spot market buys are sized in the quote coin only (no `marketUnit`)
        anyhow::ensure!(
            !(product_type == BitgetProductType::Spot
                && bitget_order_type == BitgetOrderType::Market
                && order_side == OrderSide::Buy
                && !is_quote_quantity),
            "Bitget spot market buy orders require a quote quantity"
        );
        let market_unit = spot_market_unit(product_type, bitget_order_type, is_quote_quantity);
        let trigger_dir = trigger_direction(order_type, order_side, is_stop_order);

        let mut order_entry = BitgetBatchPlaceOrderEntryBuilder::default();
        order_entry.symbol(bitget_symbol.raw_symbol().to_string());
        order_entry.side(bitget_side);
        order_entry.order_type(bitget_order_type);
        order_entry.qty(quantity.to_string());
        order_entry.time_in_force(bitget_tif);
        order_entry.order_link_id(client_order_id.to_string());
        order_entry.market_unit(market_unit);
        order_entry.trigger_direction(trigger_dir);

        if bbo_side_type.is_none()
            && let Some(price) = price
        {
            order_entry.price(Some(price.to_string()));
        }

        if let Some(trigger_price) = trigger_price {
            order_entry.trigger_price(Some(trigger_price.to_string()));
        }

        if reduce_only {
            order_entry.reduce_only(Some(true));
        }

        order_entry.is_leverage(spot_leverage(product_type, is_leverage));

        if let Some(idx) = position_idx {
            order_entry.position_idx(Some(idx));
        }

        // Bitget takes one `bboLevel` value: the side type followed by the level (e.g. `queue1`)
        let bbo_level = bbo_side_type.zip(bbo_level).map(|(side_type, level)| {
            let side_type = match side_type {
                BitgetBboSideType::Queue => "queue",
                BitgetBboSideType::Counterparty => "counterparty",
            };
            format!("{side_type}{level}")
        });
        order_entry.bbo_side_type(bbo_side_type);
        order_entry.bbo_level(bbo_level);
        order_entry.smp_type(smp_type);

        if let Some(tp_sl) = native_tp_sl {
            if let Some(ref tp) = tp_sl.take_profit {
                order_entry.take_profit(Some(tp.clone()));
            }

            if let Some(ref sl) = tp_sl.stop_loss {
                order_entry.stop_loss(Some(sl.clone()));
            }

            if let Some(tp_trigger) = tp_sl.tp_trigger_by {
                order_entry.tp_trigger_by(Some(tp_trigger));
            }

            if let Some(sl_trigger) = tp_sl.sl_trigger_by {
                order_entry.sl_trigger_by(Some(sl_trigger));
            }

            if let Some(tp_ot) = tp_sl.tp_order_type {
                order_entry.tp_order_type(Some(tp_ot));
            }

            if let Some(sl_ot) = tp_sl.sl_order_type {
                order_entry.sl_order_type(Some(sl_ot));
            }

            if let Some(ref tp_lp) = tp_sl.tp_limit_price {
                order_entry.tp_limit_price(Some(tp_lp.clone()));
            }

            if let Some(ref sl_lp) = tp_sl.sl_limit_price {
                order_entry.sl_limit_price(Some(sl_lp.clone()));
            }

            // Default to `Full` when TP or SL is set without an explicit mode, mirroring the WS
            // path, so Bitget accepts the field instead of rejecting it.
            let mode = tp_sl.tpsl_mode.or_else(|| {
                (tp_sl.take_profit.is_some() || tp_sl.stop_loss.is_some())
                    .then_some(BitgetTpSlMode::Full)
            });

            if let Some(m) = mode {
                order_entry.tpsl_mode(Some(m));
            }

            if let Some(close) = tp_sl.close_on_trigger {
                order_entry.close_on_trigger(Some(close));
            }

            if let Some(ref iv) = tp_sl.order_iv {
                order_entry.order_iv(Some(iv.clone()));
            }

            if let Some(mmp) = tp_sl.mmp {
                order_entry.mmp(Some(mmp));
            }
        }

        let order_entry = order_entry.build().build_anyhow()?;

        let mut params = BitgetPlaceOrderParamsBuilder::default();
        params.category(product_type);
        params.order(order_entry);

        let params = params.build().build_anyhow()?;

        // Bitget places conditional orders through the strategy-order endpoint
        let (response, lookup_endpoint) = if is_stop_order {
            let strategy = BitgetPlaceStrategyOrderParams::trigger_from(&params);
            let body = serde_json::to_value(&strategy)?;
            (
                self.inner.place_strategy_order(&body).await?,
                BITGET_STRATEGY_ORDER_REALTIME,
            )
        } else {
            let body = serde_json::to_value(&params)?;
            (self.inner.place_order(&body).await?, BITGET_ORDER_REALTIME)
        };

        let order_id = response
            .result
            .order_id
            .ok_or(BitgetSubmitOrderError::MissingOrderId)?;

        let order = self
            .query_order_by_id(
                product_type,
                order_id.as_str(),
                lookup_endpoint,
                "after submission",
            )
            .await
            .map_err(|source| BitgetSubmitOrderError::PostSubmitLookup { source })?;

        // Only bail on rejection if there are no fills
        // If the order has fills (cum_exec_qty > 0), let the parser remap Rejected -> Canceled.
        // A post-only order that would take liquidity is reported as Cancelled with
        // cancelReason=post_only_fill_cancel (not Rejected), so treat that as a rejection too.
        let is_post_only_cancel = order.cancel_type == BitgetCancelType::CancelByPostOnly;
        let is_rejection = order.order_status == crate::common::enums::BitgetOrderStatus::Rejected
            || (order.order_status == crate::common::enums::BitgetOrderStatus::Canceled
                && is_post_only_cancel);
        if is_rejection && (order.cum_exec_qty.as_str() == "0" || order.cum_exec_qty.is_empty()) {
            let reason = if is_post_only_cancel {
                "post_only_fill_cancel".to_string()
            } else {
                order.reject_reason.to_string()
            };
            return Err(BitgetSubmitOrderError::Rejected { reason }.into());
        }

        let ts_init = self.generate_ts_init();

        parse_order_status_report(&order, &instrument, account_id, ts_init)
    }

    /// Cancel an order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The order doesn't exist.
    /// - The API returns an error.
    pub async fn cancel_order(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
    ) -> anyhow::Result<OrderStatusReport> {
        let instrument = self.instrument_from_cache(&instrument_id.symbol)?;
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

        let mut cancel_entry = BitgetBatchCancelOrderEntryBuilder::default();
        cancel_entry.symbol(bitget_symbol.raw_symbol().to_string());

        if let Some(venue_order_id) = venue_order_id {
            cancel_entry.order_id(venue_order_id.to_string());
        } else if let Some(client_order_id) = client_order_id {
            cancel_entry.order_link_id(client_order_id.to_string());
        } else {
            anyhow::bail!("Either client_order_id or venue_order_id must be provided");
        }

        let cancel_entry = cancel_entry.build().build_anyhow()?;

        let mut params = BitgetCancelOrderParamsBuilder::default();
        params.category(product_type);
        params.order(cancel_entry);

        let params = params.build().build_anyhow()?;
        let body = serde_json::to_vec(&params)?;

        let response: BitgetPlaceOrderResponse = self
            .inner
            .send_request::<_, ()>(
                Method::POST,
                "/api/v3/trade/cancel-order",
                None,
                Some(body),
                true,
            )
            .await?;

        let order_id = response
            .result
            .order_id
            .ok_or(BitgetCancelOrderError::MissingOrderId)?;

        let order = self
            .query_order_by_id(
                product_type,
                order_id.as_str(),
                BITGET_ORDER_HISTORY,
                "after cancellation",
            )
            .await
            .map_err(|source| BitgetCancelOrderError::PostCancelLookup { source })?;

        let ts_init = self.generate_ts_init();

        parse_order_status_report(&order, &instrument, account_id, ts_init)
    }

    /// Cancels a conditional (strategy) order; Bitget keeps these apart from plain orders.
    ///
    /// The outcome arrives on the `strategy-order` WebSocket topic.
    ///
    /// # Errors
    ///
    /// Returns an error if neither identifier is provided or the request fails.
    pub async fn cancel_strategy_order(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
    ) -> anyhow::Result<()> {
        self.instrument_from_cache(&instrument_id.symbol)?;
        anyhow::ensure!(
            client_order_id.is_some() || venue_order_id.is_some(),
            "Either client_order_id or venue_order_id must be provided"
        );
        let params = BitgetCancelStrategyOrderParams {
            category: product_type,
            order_id: venue_order_id.map(|id| id.to_string()),
            order_link_id: client_order_id.map(|id| id.to_string()),
        };
        self.inner.cancel_strategy_order(&params).await?;
        Ok(())
    }

    /// Modifies a conditional (strategy) order; Bitget keeps these apart from plain orders.
    ///
    /// The venue requires the full quantity on every modify. The outcome arrives on the
    /// `strategy-order` WebSocket topic.
    ///
    /// # Errors
    ///
    /// Returns an error if neither identifier is provided or the request fails.
    pub async fn modify_strategy_order(
        &self,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
        quantity: Quantity,
        price: Option<Price>,
        trigger_price: Option<Price>,
    ) -> anyhow::Result<()> {
        self.instrument_from_cache(&instrument_id.symbol)?;
        anyhow::ensure!(
            client_order_id.is_some() || venue_order_id.is_some(),
            "Either client_order_id or venue_order_id must be provided"
        );
        let params = BitgetModifyStrategyOrderParams {
            order_id: venue_order_id.map(|id| id.to_string()),
            order_link_id: client_order_id.map(|id| id.to_string()),
            qty: quantity.to_string(),
            trigger_price: trigger_price.map(|p| p.to_string()),
            trigger_order_price: price.map(|p| p.to_string()),
        };
        self.inner.modify_strategy_order(&params).await?;
        Ok(())
    }

    /// Batch cancel multiple orders.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - Any of the orders don't exist.
    /// - The API returns an error.
    pub async fn batch_cancel_orders(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_ids: Vec<InstrumentId>,
        client_order_ids: Vec<Option<ClientOrderId>>,
        venue_order_ids: Vec<Option<VenueOrderId>>,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        if instrument_ids.len() != client_order_ids.len()
            || instrument_ids.len() != venue_order_ids.len()
        {
            anyhow::bail!(
                "instrument_ids, client_order_ids, and venue_order_ids must have the same length"
            );
        }

        if instrument_ids.is_empty() {
            return Ok(Vec::new());
        }

        let call_limit = batch_call_limit(product_type);
        if instrument_ids.len() > call_limit {
            anyhow::bail!(
                "Batch cancel limit is {call_limit} orders for {}",
                product_type.as_str()
            );
        }

        let mut cancel_entries = Vec::new();

        for ((instrument_id, client_order_id), venue_order_id) in instrument_ids
            .iter()
            .zip(client_order_ids.iter())
            .zip(venue_order_ids.iter())
        {
            let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;
            let mut cancel_entry = BitgetBatchCancelOrderEntryBuilder::default();
            cancel_entry.symbol(bitget_symbol.raw_symbol().to_string());

            if let Some(venue_order_id) = venue_order_id {
                cancel_entry.order_id(venue_order_id.to_string());
            } else if let Some(client_order_id) = client_order_id {
                cancel_entry.order_link_id(client_order_id.to_string());
            } else {
                anyhow::bail!(
                    "Either client_order_id or venue_order_id must be provided for each order"
                );
            }

            cancel_entries.push(cancel_entry.build().build_anyhow()?);
        }

        let chunk_limit = batch_endpoint_limit(product_type).min(batch_send_limit(product_type));
        for chunk in cancel_entries.chunks(chunk_limit) {
            let mut params = BitgetBatchCancelOrderParamsBuilder::default();
            params.category(product_type);
            params.request(chunk.to_vec());

            let params = params.build().build_anyhow()?;
            let body = serde_json::to_vec(&params)?;

            // Bitget answers with one `{orderId, clientOid, code, msg}` entry per order
            let _response: crate::common::models::BitgetListResponse<serde_json::Value> = self
                .inner
                .send_request::<_, ()>(
                    Method::POST,
                    "/api/v3/trade/cancel-batch",
                    None,
                    Some(body),
                    true,
                )
                .await?;
        }

        // Query each order to get full details after cancellation
        let mut reports = Vec::new();

        for (instrument_id, (client_order_id, venue_order_id)) in instrument_ids
            .iter()
            .zip(client_order_ids.iter().zip(venue_order_ids.iter()))
        {
            let Ok(instrument) = self.instrument_from_cache(&instrument_id.symbol) else {
                log::debug!(
                    "Skipping cancelled order report for instrument not in cache: symbol={}",
                    instrument_id.symbol
                );
                continue;
            };

            let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

            let mut query_params = BitgetOpenOrdersParamsBuilder::default();
            query_params.category(product_type);
            query_params.symbol(bitget_symbol.raw_symbol().to_string());

            if let Some(venue_order_id) = venue_order_id {
                query_params.order_id(venue_order_id.to_string());
            } else if let Some(client_order_id) = client_order_id {
                query_params.order_link_id(client_order_id.to_string());
            }

            let query_params = query_params.build().build_anyhow()?;
            let order_response: BitgetOpenOrdersResponse =
                self.inner.get_open_orders_with(&query_params).await?;

            if let Some(order) = order_response.result.list.into_iter().next() {
                let ts_init = self.generate_ts_init();
                let report = parse_order_status_report(&order, &instrument, account_id, ts_init)?;
                reports.push(report);
            }
        }

        Ok(reports)
    }

    /// Cancel all orders for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    pub async fn cancel_all_orders(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let instrument = self.instrument_from_cache(&instrument_id.symbol)?;
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

        let mut params = BitgetCancelAllOrdersParamsBuilder::default();
        params.category(product_type);
        params.symbol(bitget_symbol.raw_symbol().to_string());

        let params = params.build().build_anyhow()?;
        let body = serde_json::to_vec(&params)?;

        let _response: crate::common::models::BitgetListResponse<serde_json::Value> = self
            .inner
            .send_request::<_, ()>(
                Method::POST,
                "/api/v3/trade/cancel-symbol-order",
                None,
                Some(body),
                true,
            )
            .await?;

        // Query the order history to get all canceled orders
        let mut query_params = BitgetOrderHistoryParamsBuilder::default();
        query_params.category(product_type);
        query_params.symbol(bitget_symbol.raw_symbol().to_string());
        query_params.limit(50u32);

        let query_params = query_params.build().build_anyhow()?;
        let order_response: BitgetOrderHistoryResponse =
            self.inner.get_order_history_with(&query_params).await?;

        let ts_init = self.generate_ts_init();

        let mut reports = Vec::new();

        for order in order_response.result.list {
            if let Ok(report) = parse_order_status_report(&order, &instrument, account_id, ts_init)
            {
                reports.push(report);
            }
        }

        Ok(reports)
    }

    /// Modify an existing order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The order doesn't exist.
    /// - The order is already closed.
    /// - The API returns an error.
    #[expect(clippy::too_many_arguments)]
    pub async fn modify_order(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
        quantity: Option<Quantity>,
        price: Option<Price>,
    ) -> anyhow::Result<OrderStatusReport> {
        let instrument = self.instrument_from_cache(&instrument_id.symbol)?;
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

        let mut amend_entry = BitgetBatchAmendOrderEntryBuilder::default();
        amend_entry.symbol(bitget_symbol.raw_symbol().to_string());

        if let Some(venue_order_id) = venue_order_id {
            amend_entry.order_id(venue_order_id.to_string());
        } else if let Some(client_order_id) = client_order_id {
            amend_entry.order_link_id(client_order_id.to_string());
        } else {
            anyhow::bail!("Either client_order_id or venue_order_id must be provided");
        }

        if let Some(quantity) = quantity {
            amend_entry.qty(Some(quantity.to_string()));
        }

        if let Some(price) = price {
            amend_entry.price(Some(price.to_string()));
        }

        let amend_entry = amend_entry.build().build_anyhow()?;

        let mut params = BitgetAmendOrderParamsBuilder::default();
        params.category(product_type);
        params.order(amend_entry);

        let params = params.build().build_anyhow()?;
        let body = serde_json::to_vec(&params)?;

        let response: BitgetPlaceOrderResponse = self
            .inner
            .send_request::<_, ()>(
                Method::POST,
                "/api/v3/trade/modify-order",
                None,
                Some(body),
                true,
            )
            .await?;

        let order_id = response
            .result
            .order_id
            .ok_or(BitgetModifyOrderError::MissingOrderId)?;

        let order = self
            .query_order_by_id(
                product_type,
                order_id.as_str(),
                BITGET_ORDER_REALTIME,
                "after amendment",
            )
            .await
            .map_err(|source| BitgetModifyOrderError::PostModifyLookup { source })?;

        let ts_init = self.generate_ts_init();

        parse_order_status_report(&order, &instrument, account_id, ts_init)
    }

    /// Query a single order by client order ID or venue order ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    pub async fn query_order(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        log::debug!(
            "query_order: instrument_id={instrument_id}, client_order_id={client_order_id:?}, venue_order_id={venue_order_id:?}"
        );

        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

        let mut params = BitgetOpenOrdersParamsBuilder::default();
        params.category(product_type);
        // Use the raw Bitget symbol (e.g., "ETHUSDT") not the full instrument symbol
        params.symbol(bitget_symbol.raw_symbol().to_string());

        if let Some(venue_order_id) = venue_order_id {
            params.order_id(venue_order_id.to_string());
        } else if let Some(client_order_id) = client_order_id {
            params.order_link_id(client_order_id.to_string());
        } else {
            anyhow::bail!("Either client_order_id or venue_order_id must be provided");
        }

        let params = params.build().build_anyhow()?;
        let mut response: BitgetOpenOrdersResponse =
            self.inner.get_open_orders_with(&params).await?;

        // Options do not support the StopOrder filter
        if response.result.list.is_empty() && product_type != BitgetProductType::Option {
            log::debug!("Order not found in open orders, trying with StopOrder filter");

            let mut stop_params = BitgetOpenOrdersParamsBuilder::default();
            stop_params.category(product_type);
            stop_params.symbol(bitget_symbol.raw_symbol().to_string());
            stop_params.order_filter(BitgetOrderFilter::StopOrder);

            if let Some(venue_order_id) = venue_order_id {
                stop_params.order_id(venue_order_id.to_string());
            } else if let Some(client_order_id) = client_order_id {
                stop_params.order_link_id(client_order_id.to_string());
            }

            let stop_params = stop_params.build().build_anyhow()?;
            response = self.inner.get_open_orders_with(&stop_params).await?;
        }

        // If not found in open orders, check order history
        if response.result.list.is_empty() {
            log::debug!("Order not found in open orders, checking order history");

            let mut history_params = BitgetOrderHistoryParamsBuilder::default();
            history_params.category(product_type);
            history_params.symbol(bitget_symbol.raw_symbol().to_string());

            if let Some(venue_order_id) = venue_order_id {
                history_params.order_id(venue_order_id.to_string());
            } else if let Some(client_order_id) = client_order_id {
                history_params.order_link_id(client_order_id.to_string());
            }

            let history_params = history_params.build().build_anyhow()?;

            let mut history_response: BitgetOrderHistoryResponse =
                self.inner.get_order_history_with(&history_params).await?;

            if history_response.result.list.is_empty() && product_type == BitgetProductType::Option
            {
                log::debug!("Option order not found in order history");
                return Ok(None);
            }

            // Options do not support the StopOrder filter
            if history_response.result.list.is_empty() && product_type != BitgetProductType::Option
            {
                log::debug!("Order not found in order history, trying with StopOrder filter");

                let mut stop_history_params = BitgetOrderHistoryParamsBuilder::default();
                stop_history_params.category(product_type);
                stop_history_params.symbol(bitget_symbol.raw_symbol().to_string());
                stop_history_params.order_filter(BitgetOrderFilter::StopOrder);

                if let Some(venue_order_id) = venue_order_id {
                    stop_history_params.order_id(venue_order_id.to_string());
                } else if let Some(client_order_id) = client_order_id {
                    stop_history_params.order_link_id(client_order_id.to_string());
                }

                let stop_history_params = stop_history_params
                    .build()
                    .map_err(|e| anyhow::anyhow!(e))?;

                history_response = self
                    .inner
                    .get_order_history_with(&stop_history_params)
                    .await?;

                if history_response.result.list.is_empty() {
                    log::debug!("Order not found in order history with StopOrder filter either");
                    return Ok(None);
                }
            }

            // Move the order from history response to the response list
            response.result.list = history_response.result.list;
        }

        let order = &response.result.list[0];
        let ts_init = self.generate_ts_init();

        log::debug!(
            "Query order response: symbol={}, order_id={}, order_link_id={}",
            order.symbol.as_str(),
            order.order_id.as_str(),
            order.order_link_id.as_str()
        );

        let instrument = self
            .instrument_from_cache(&instrument_id.symbol)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to query order {}: {}",
                    client_order_id
                        .as_ref()
                        .map(|id| id.to_string())
                        .or_else(|| venue_order_id.as_ref().map(|id| id.to_string()))
                        .unwrap_or_else(|| "unknown".to_string()),
                    e
                )
            })?;

        log::debug!("Retrieved instrument from cache: id={}", instrument.id());

        let report =
            parse_order_status_report(order, &instrument, account_id, ts_init).map_err(|e| {
                log::debug!(
                    "Failed to parse order status report for {}: {}",
                    order.order_link_id.as_str(),
                    e
                );
                e
            })?;

        log::debug!(
            "Successfully created OrderStatusReport for {}",
            order.order_link_id.as_str()
        );

        Ok(Some(report))
    }

    async fn paginate_instruments<D, F>(
        &self,
        product_type: BitgetProductType,
        symbol: &Option<String>,
        base_coin: Option<Ustr>,
        mut parse: F,
    ) -> anyhow::Result<Vec<InstrumentAny>>
    where
        D: DeserializeOwned,
        BitgetCursorListResponse<D>: BitgetResponseCheck,
        F: FnMut(&D) -> Option<InstrumentAny>,
    {
        let mut instruments = Vec::new();
        let mut cursor: Option<String> = None;
        let mut cursor_walk = CursorWalk::default();

        loop {
            let params = BitgetInstrumentsInfoParams {
                category: product_type,
                symbol: symbol.clone(),
                status: None,
                base_coin: base_coin.map(|u| u.to_string()),
                limit: Some(1000),
                cursor: cursor.clone(),
            };

            let response: BitgetCursorListResponse<D> = self.inner.get_instruments(&params).await?;

            for definition in &response.result.list {
                if let Some(instrument) = parse(definition) {
                    instruments.push(instrument);
                }
            }

            // A short instrument list is a visible gap, since the risk engine denies the next
            // order on a missing instrument by name, while a short reconciliation history is
            // silent, so the other walks return this error and this one keeps what it has.
            cursor = match cursor_walk
                .advance(BITGET_INSTRUMENTS_INFO, response.result.next_page_cursor)
            {
                Ok(cursor) => cursor,
                Err(e) => {
                    log::warn!(
                        "{e}, keeping the {} instrument(s) already read",
                        instruments.len()
                    );
                    break;
                }
            };

            if cursor.is_none() {
                break;
            }
        }

        Ok(instruments)
    }

    /// Fetches instrument info and returns the current status of each symbol.
    ///
    /// Paginates through the instruments endpoint collecting only
    /// `(InstrumentId, MarketStatusAction)` pairs. This avoids full instrument
    /// parsing.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn request_instrument_statuses(
        &self,
        product_type: BitgetProductType,
    ) -> anyhow::Result<AHashMap<InstrumentId, MarketStatusAction>> {
        let mut statuses = AHashMap::new();
        let mut cursor: Option<String> = None;
        let mut cursor_walk = CursorWalk::default();

        loop {
            let params = BitgetInstrumentsInfoParams {
                category: product_type,
                symbol: None,
                status: None,
                base_coin: None,
                limit: Some(1000),
                cursor: cursor.clone(),
            };

            match product_type {
                BitgetProductType::Spot => {
                    let response: BitgetCursorListResponse<BitgetInstrumentSpot> =
                        self.inner.get_instruments(&params).await?;

                    for def in &response.result.list {
                        let symbol = make_bitget_symbol(def.symbol, product_type);
                        let id = InstrumentId::new(Symbol::from(symbol), *BITGET_VENUE);
                        statuses.insert(id, MarketStatusAction::from(def.status));
                    }
                    cursor = response.result.next_page_cursor;
                }
                BitgetProductType::Linear => {
                    let response: BitgetCursorListResponse<BitgetInstrumentLinear> =
                        self.inner.get_instruments(&params).await?;

                    for def in &response.result.list {
                        let symbol = make_bitget_symbol(def.symbol, product_type);
                        let id = InstrumentId::new(Symbol::from(symbol), *BITGET_VENUE);
                        let status = MarketStatusAction::from(def.status);
                        if status == MarketStatusAction::Trading
                            && def.contract_type() == BitgetContractType::LinearPerpetual
                            && is_scheduled_off(&def.off_time)
                        {
                            statuses.insert(id, MarketStatusAction::PreClose);
                        } else {
                            statuses.insert(id, status);
                        }
                    }
                    cursor = response.result.next_page_cursor;
                }
                BitgetProductType::Inverse => {
                    let response: BitgetCursorListResponse<BitgetInstrumentInverse> =
                        self.inner.get_instruments(&params).await?;

                    for def in &response.result.list {
                        let symbol = make_bitget_symbol(def.symbol, product_type);
                        let id = InstrumentId::new(Symbol::from(symbol), *BITGET_VENUE);
                        let status = MarketStatusAction::from(def.status);
                        if status == MarketStatusAction::Trading
                            && def.contract_type() == BitgetContractType::InversePerpetual
                            && is_scheduled_off(&def.off_time)
                        {
                            statuses.insert(id, MarketStatusAction::PreClose);
                        } else {
                            statuses.insert(id, status);
                        }
                    }
                    cursor = response.result.next_page_cursor;
                }
                BitgetProductType::Option => {
                    anyhow::bail!("Option instruments are not supported by Bitget")
                }
            }

            cursor = cursor_walk.advance(BITGET_INSTRUMENTS_INFO, cursor)?;
            if cursor.is_none() {
                break;
            }
        }

        Ok(statuses)
    }

    /// Request instruments for a given product type.
    ///
    /// When `base_coin` is provided, the request is narrowed to that base coin.
    /// This is required for `Option`: Bitget's API returns only `BTC` options when
    /// `baseCoin` is omitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or parsing fails.
    pub async fn request_instruments(
        &self,
        product_type: BitgetProductType,
        symbol: Option<String>,
        base_coin: Option<Ustr>,
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let ts_init = self.generate_ts_init();

        let instruments = match product_type {
            BitgetProductType::Spot => {
                self.paginate_instruments::<BitgetInstrumentSpot, _>(
                    product_type,
                    &symbol,
                    base_coin,
                    |def| parse_spot_instrument(def, ts_init, ts_init).ok(),
                )
                .await?
            }
            BitgetProductType::Linear => {
                self.paginate_instruments::<BitgetInstrumentLinear, _>(
                    product_type,
                    &symbol,
                    base_coin,
                    |def| parse_linear_instrument(def, ts_init, ts_init).ok(),
                )
                .await?
            }
            BitgetProductType::Inverse => {
                self.paginate_instruments::<BitgetInstrumentInverse, _>(
                    product_type,
                    &symbol,
                    base_coin,
                    |def| parse_inverse_instrument(def, ts_init, ts_init).ok(),
                )
                .await?
            }
            BitgetProductType::Option => {
                // Bitget lists no options; the parse path is kept for API parity
                anyhow::bail!("Option instruments are not supported by Bitget")
            }
        };

        self.cache_instruments(&instruments);

        Ok(instruments)
    }

    /// Requests full instrument definitions and their market statuses in a single endpoint pass.
    ///
    /// Both are parsed from the same `/api/v3/market/instruments` response, avoiding a second
    /// round-trip when a caller needs definitions and statuses together (e.g. the polling loop
    /// serving both instrument and status subscriptions).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or parsing fails.
    pub async fn request_instruments_with_statuses(
        &self,
        product_type: BitgetProductType,
    ) -> anyhow::Result<(
        Vec<InstrumentAny>,
        AHashMap<InstrumentId, MarketStatusAction>,
    )> {
        let ts_init = self.generate_ts_init();
        let mut statuses = AHashMap::new();

        // A perp with an `offTime` is scheduled for a trading halt or delisting.
        let perp_status = |status: MarketStatusAction, is_scheduled_perp: bool| {
            if status == MarketStatusAction::Trading && is_scheduled_perp {
                MarketStatusAction::PreClose
            } else {
                status
            }
        };

        let instruments = match product_type {
            BitgetProductType::Spot => {
                self.paginate_instruments::<BitgetInstrumentSpot, _>(
                    product_type,
                    &None::<String>,
                    None,
                    |def| {
                        let id = InstrumentId::new(
                            Symbol::from(make_bitget_symbol(def.symbol, product_type)),
                            *BITGET_VENUE,
                        );
                        statuses.insert(id, MarketStatusAction::from(def.status));
                        parse_spot_instrument(def, ts_init, ts_init).ok()
                    },
                )
                .await?
            }
            BitgetProductType::Linear => {
                self.paginate_instruments::<BitgetInstrumentLinear, _>(
                    product_type,
                    &None::<String>,
                    None,
                    |def| {
                        let id = InstrumentId::new(
                            Symbol::from(make_bitget_symbol(def.symbol, product_type)),
                            *BITGET_VENUE,
                        );
                        let scheduled = def.contract_type() == BitgetContractType::LinearPerpetual
                            && is_scheduled_off(&def.off_time);
                        statuses.insert(id, perp_status(def.status.into(), scheduled));
                        parse_linear_instrument(def, ts_init, ts_init).ok()
                    },
                )
                .await?
            }
            BitgetProductType::Inverse => {
                self.paginate_instruments::<BitgetInstrumentInverse, _>(
                    product_type,
                    &None::<String>,
                    None,
                    |def| {
                        let id = InstrumentId::new(
                            Symbol::from(make_bitget_symbol(def.symbol, product_type)),
                            *BITGET_VENUE,
                        );
                        let scheduled = def.contract_type() == BitgetContractType::InversePerpetual
                            && is_scheduled_off(&def.off_time);
                        statuses.insert(id, perp_status(def.status.into(), scheduled));
                        parse_inverse_instrument(def, ts_init, ts_init).ok()
                    },
                )
                .await?
            }
            BitgetProductType::Option => {
                anyhow::bail!("Option instruments are not supported by Bitget")
            }
        };

        self.cache_instruments(&instruments);

        Ok((instruments, statuses))
    }

    /// Request ticker information for market data.
    ///
    /// Fetches ticker data from Bitget's `/api/v3/market/tickers` endpoint and returns
    /// a unified `BitgetTickerData` structure compatible with all product types.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or parsing fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn request_tickers(
        &self,
        params: &BitgetTickersParams,
    ) -> anyhow::Result<Vec<BitgetTickerData>> {
        use super::models::{BitgetTickersLinearResponse, BitgetTickersSpotResponse};

        match params.category {
            BitgetProductType::Spot => {
                let response: BitgetTickersSpotResponse = self.inner.get_tickers(params).await?;
                Ok(response.result.list.into_iter().map(Into::into).collect())
            }
            BitgetProductType::Linear | BitgetProductType::Inverse => {
                let response: BitgetTickersLinearResponse = self.inner.get_tickers(params).await?;
                Ok(response.result.list.into_iter().map(Into::into).collect())
            }
            BitgetProductType::Option => {
                anyhow::bail!("Option tickers are not supported by Bitget")
            }
        }
    }

    /// Requests raw option tickers for a given base coin.
    ///
    /// Returns `Vec<BitgetTickerOption>` with the raw fields including `underlying_price`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn request_option_tickers_raw(
        &self,
        base_coin: &str,
    ) -> anyhow::Result<Vec<BitgetTickerOption>> {
        // Bitget lists no options
        anyhow::bail!("Option tickers for {base_coin} are not supported by Bitget")
    }

    /// Request raw option tickers with custom params.
    ///
    /// This allows fetching a single instrument by setting `symbol` in the params,
    /// instead of fetching all options for a base coin.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn request_option_tickers_raw_with_params(
        &self,
        params: &BitgetTickersParams,
    ) -> anyhow::Result<Vec<BitgetTickerOption>> {
        // Bitget lists no options
        anyhow::bail!(
            "Option tickers are not supported by Bitget (category {:?})",
            params.category
        )
    }

    /// Request recent trade tick history for a given symbol.
    ///
    /// Returns the most recent public trades from Bitget's `/api/v3/market/fills` endpoint.
    /// This endpoint only provides recent trades (up to 1000 most recent), typically covering
    /// only the last few minutes for active markets.
    ///
    /// **Note**: For historical trade data with time ranges, use the klines endpoint instead.
    /// The Bitget public API does not support fetching historical trades by time range.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The instrument is not found in cache.
    /// - The request fails.
    /// - Parsing fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn request_trades(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        limit: Option<u32>,
    ) -> anyhow::Result<Vec<TradeTick>> {
        let instrument = self.instrument_from_cache_by_id(instrument_id)?;
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

        let mut params_builder = BitgetTradesParamsBuilder::default();
        params_builder.category(product_type);
        params_builder.symbol(bitget_symbol.raw_symbol().to_string());

        if let Some(limit_val) = limit {
            params_builder.limit(limit_val);
        }

        let params = params_builder.build().build_anyhow()?;
        let response = self.inner.get_recent_trades(&params).await?;

        let mut trades = Vec::new();

        for trade in response.result.list {
            if let Ok(trade_tick) = parse_trade_tick(&trade, &instrument, None) {
                trades.push(trade_tick);
            }
        }

        Ok(trades)
    }

    /// Request funding rate history for a given symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The instrument is not found in cache.
    /// - The request fails.
    /// - Parsing fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/market/derivatives>
    pub async fn request_funding_rates(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
        limit: Option<u32>,
    ) -> anyhow::Result<Vec<FundingRateUpdate>> {
        let instrument = self.instrument_from_cache_by_id(instrument_id)?;
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

        let start_ms = start.map(|dt| dt.as_millisecond());
        let end_ms = end.map(|dt| dt.as_millisecond());
        let mut seen_timestamps: AHashSet<i64> = AHashSet::new();

        let mut raw_funding_rates = Vec::new();

        // Bitget pages funding history by page number (`cursor`, 1..=100, newest first) and
        // takes no time range, so the window is applied to each page
        let mut page = 1u32;

        loop {
            let mut params_builder = BitgetFundingParamsBuilder::default();
            params_builder.category(product_type);
            params_builder.symbol(bitget_symbol.raw_symbol().to_string());
            params_builder.limit(limit.unwrap_or(100).clamp(1, 100)); // 100 is the maximum for the Bitget API
            params_builder.cursor(page);

            let params = params_builder.build().build_anyhow()?;
            let response = self.inner.get_funding_history(&params).await?;

            let funding_rates = response.result.list;

            let mut new_funding_rates_with_ts: Vec<(i64, _)> = funding_rates
                .into_iter()
                .filter_map(|f| {
                    let Ok(ts) = f.funding_rate_timestamp.parse::<i64>() else {
                        return None;
                    };

                    seen_timestamps.insert(ts).then_some((ts, f))
                })
                .collect();

            new_funding_rates_with_ts.sort_by_key(|(ts, _)| Reverse(*ts));

            let earliest_funding_time = match new_funding_rates_with_ts.last() {
                Some((last_ts, _)) => *last_ts,
                None => break,
            };

            let new_funding_rates = new_funding_rates_with_ts
                .into_iter()
                .filter(|(ts, _)| {
                    end_ms.is_none_or(|end| *ts <= end) && start_ms.is_none_or(|start| *ts >= start)
                })
                .map(|(_, f)| f);
            raw_funding_rates.extend(new_funding_rates);

            // Check if we've reached the requested limit
            if let Some(limit_val) = limit
                && raw_funding_rates.len() >= limit_val as usize
            {
                break;
            }

            if let Some(start_val) = start_ms
                && earliest_funding_time <= start_val
            {
                break;
            }

            if page >= 100 {
                break;
            }

            // Move to the next (older) page
            page += 1;
        }

        if let Some(limit_val) = limit {
            raw_funding_rates.truncate(limit_val as usize);
        }
        let mut rates: Vec<FundingRateUpdate> = Vec::with_capacity(raw_funding_rates.len());

        for window in raw_funding_rates.windows(2) {
            let raw = &window[0];
            let timestamp = raw
                .funding_rate_timestamp
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("invalid funding_rate_timestamp"))?;
            let older_timestamp = window[1]
                .funding_rate_timestamp
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("invalid funding_rate_timestamp"))?;

            let interval_millis = timestamp - older_timestamp;
            let rate = parse_funding_rate(raw, &instrument, Some(interval_millis))?;

            rates.push(rate);
        }

        if let Some(last_raw) = raw_funding_rates.last() {
            let rate = parse_funding_rate(last_raw, &instrument, None)?;
            rates.push(rate);
        }

        rates.reverse();

        Ok(rates)
    }

    /// Request an orderbook snapshot for a given symbol.
    ///
    /// Bitget limits the amount of levels (depth) to `1..=1000` (default: `5`) for every
    /// product type.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The instrument is not found in cache.
    /// - The request fails.
    /// - Parsing fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn request_orderbook_snapshot(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        limit: Option<u32>,
    ) -> anyhow::Result<OrderBookDeltas> {
        let instrument = self.instrument_from_cache_by_id(instrument_id)?;
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

        let mut params_builder = BitgetOrderbookParamsBuilder::default();
        params_builder.category(product_type);
        params_builder.symbol(bitget_symbol.raw_symbol().to_string());

        if let Some(limit) = limit {
            let max_limit = match product_type {
                BitgetProductType::Option => 25,
                BitgetProductType::Spot
                | BitgetProductType::Linear
                | BitgetProductType::Inverse => 1000,
            };
            let clamped_limit = limit.min(max_limit);
            if limit > max_limit {
                log::warn!(
                    "Bitget orderbook snapshot request depth limit exceeds venue maximum; clamping: limit={limit}, clamped_limit={clamped_limit}",
                );
            }
            params_builder.limit(clamped_limit);
        }

        let params = params_builder.build().build_anyhow()?;
        let response = self.inner.get_orderbook(&params).await?;

        let deltas = parse_orderbook(&response.result, &instrument, None)?;

        Ok(deltas)
    }

    /// Request bar/kline history for a given symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The instrument is not found in cache.
    /// - The request fails.
    /// - Parsing fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/market/market-data>
    pub async fn request_bars(
        &self,
        product_type: BitgetProductType,
        bar_type: BarType,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
        limit: Option<u32>,
        timestamp_on_close: bool,
    ) -> anyhow::Result<Vec<Bar>> {
        let instrument_id = bar_type.instrument_id();
        let instrument = self.instrument_from_cache_by_id(instrument_id)?;
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())?;

        // Convert Nautilus BarSpec to Bitget interval
        let interval = bar_spec_to_bitget_interval(
            bar_type.spec().aggregation,
            bar_type.spec().step.get() as u64,
        )?;

        let start_ms = start.map(|dt| dt.as_millisecond());
        let mut seen_timestamps: AHashSet<i64> = AHashSet::new();
        let current_time_ms = get_atomic_clock_realtime().get_time_ms() as i64;

        // Pagination strategy: work backwards from end time
        // - Each page fetched is older than the previous page
        // - Within each page, bars are in chronological order (oldest to newest)
        // - We collect pages in reverse order (newest first) then reverse at the end
        // Example with 2 pages:
        //   Page 1 (most recent): bars [T=2000..2999]
        //   Page 2 (older):       bars [T=1000..1999]
        //   Collected: [[T=2000..2999], [T=1000..1999]]
        //   After reverse + flatten: [T=1000..1999, T=2000..2999] ✓ chronological
        let mut pages: Vec<Vec<Bar>> = Vec::new();
        let mut total_bars = 0usize;
        let mut current_end = end.map(|dt| dt.as_millisecond());
        let mut page_count = 0;

        loop {
            page_count += 1;

            let mut params_builder = BitgetKlinesParamsBuilder::default();
            params_builder.category(product_type);
            params_builder.symbol(bitget_symbol.raw_symbol().to_string());
            params_builder.interval(interval);
            params_builder.limit(1000u32); // Limit for data size per page (maximum for the Bitget API)

            if let Some(start_val) = start_ms {
                params_builder.start(start_val);
            }

            if let Some(end_val) = current_end {
                params_builder.end(end_val);
            }

            let params = params_builder.build().build_anyhow()?;
            let response = self.inner.get_klines(&params).await?;

            let klines = response.result.list;
            if klines.is_empty() {
                break;
            }

            // Parse timestamps once and pair with klines for sorting
            let mut klines_with_ts: Vec<(i64, _)> = klines
                .into_iter()
                .filter_map(|k| k.start.parse::<i64>().ok().map(|ts| (ts, k)))
                .collect();

            klines_with_ts.sort_by_key(|(ts, _)| *ts);

            // Check if we have any new timestamps
            let has_new = klines_with_ts
                .iter()
                .any(|(ts, _)| !seen_timestamps.contains(ts));

            if !has_new {
                break;
            }

            let mut page_bars = Vec::with_capacity(klines_with_ts.len());

            let mut earliest_ts: Option<i64> = None;

            for (start_time, kline) in &klines_with_ts {
                // Track earliest timestamp for pagination
                if earliest_ts.is_none_or(|ts| *start_time < ts) {
                    earliest_ts = Some(*start_time);
                }

                let bar_end_time = interval.bar_end_time_ms(*start_time);
                if bar_end_time > current_time_ms {
                    continue;
                }

                if !seen_timestamps.contains(start_time)
                    && let Ok(bar) =
                        parse_kline_bar(kline, &instrument, bar_type, timestamp_on_close, None)
                {
                    page_bars.push(bar);
                    seen_timestamps.insert(*start_time);
                }
            }

            // page_bars may be empty if all klines were partial, but pagination
            // continues to fetch older closed bars
            total_bars += page_bars.len();
            pages.push(page_bars);

            // Check if we've reached the requested limit
            if let Some(limit_val) = limit
                && total_bars >= limit_val as usize
            {
                break;
            }

            // Move end time backwards to get earlier data
            // Set new end to be 1ms before the first bar of this page
            let Some(earliest_bar_time) = earliest_ts else {
                break;
            };

            if let Some(start_val) = start_ms
                && earliest_bar_time <= start_val
            {
                break;
            }

            current_end = Some(earliest_bar_time - 1);

            // Safety check to prevent infinite loops
            if page_count > 100 {
                break;
            }
        }

        // Reverse pages and flatten to get chronological order (oldest to newest)
        let mut all_bars: Vec<Bar> = Vec::with_capacity(total_bars);
        for page in pages.into_iter().rev() {
            all_bars.extend(page);
        }

        // If limit is specified and we have more bars, return the last N bars (most recent)
        if let Some(limit_val) = limit {
            let limit_usize = limit_val as usize;
            if all_bars.len() > limit_usize {
                let start_idx = all_bars.len() - limit_usize;
                return Ok(all_bars[start_idx..].to_vec());
            }
        }

        Ok(all_bars)
    }

    fn instrument_from_cache_by_id(
        &self,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<InstrumentAny> {
        self.get_instrument(&instrument_id.symbol.inner())
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id).into())
    }

    /// Requests trading fee rates for the specified product type and optional filters.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - Parsing fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn request_fee_rates(
        &self,
        product_type: BitgetProductType,
        symbol: Option<String>,
        base_coin: Option<String>,
    ) -> anyhow::Result<Vec<BitgetFeeRate>> {
        let params = BitgetFeeRateParams {
            category: product_type,
            symbol,
            base_coin,
        };

        let response = self.inner.get_fee_rate(&params).await?;
        Ok(response.result.list)
    }

    /// Requests the current account state for the specified account type.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - Parsing fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/account/assets-balance>
    pub async fn request_account_state(
        &self,
        account_type: BitgetAccountType,
        account_id: AccountId,
    ) -> anyhow::Result<AccountState> {
        let params = BitgetWalletBalanceParams {
            account_type,
            coin: None,
        };

        let response = self.inner.get_wallet_balance(&params).await?;
        let ts_init = self.generate_ts_init();

        // Take the first wallet balance from the list
        // Bitget returns the single unified account object
        parse_account_state(&response.result, account_id, ts_init)
    }

    /// Request multiple order status reports.
    ///
    /// Orders for instruments not currently loaded in cache will be skipped.
    ///
    /// When `open_only` is true the realtime endpoint is queried for currently
    /// open orders and again for recently closed orders, so terminal reports
    /// are included. The closed pass fetches the most recent page only and is
    /// not constrained by `start` or `end`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credentials are missing.
    /// - The request fails.
    /// - The API returns an error.
    #[expect(clippy::too_many_arguments)]
    pub async fn request_order_status_reports(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: Option<InstrumentId>,
        open_only: bool,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
        limit: Option<u32>,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        // Extract symbol parameter from instrument_id if provided
        let symbol_param = if let Some(id) = instrument_id.as_ref() {
            let symbol_str = id.symbol.as_str();
            if symbol_str.is_empty() {
                None
            } else {
                Some(BitgetSymbol::new(symbol_str)?.raw_symbol().to_string())
            }
        } else {
            None
        };

        // Bitget filters by category only (USDC-margined contracts are a separate category)
        let settle_coins_to_query: Vec<Option<String>> = vec![None];

        let mut all_collected_orders = Vec::new();
        let mut total_collected_across_coins = 0;

        for settle_coin in settle_coins_to_query {
            let remaining_limit = if let Some(limit) = limit {
                let remaining = (limit as usize).saturating_sub(total_collected_across_coins);
                if remaining == 0 {
                    break;
                }
                Some(remaining as u32)
            } else {
                None
            };

            let orders_for_coin = if open_only {
                let mut all_orders = Vec::new();
                let mut seen_ids: AHashSet<Ustr> = AHashSet::new();

                // Query regular orders then conditional (stop/MIT) orders.
                // Options do not support the StopOrder filter.
                let order_filters: Vec<Option<BitgetOrderFilter>> =
                    if product_type == BitgetProductType::Option {
                        vec![None]
                    } else {
                        vec![None, Some(BitgetOrderFilter::StopOrder)]
                    };

                let open_only_modes = [None, Some(BitgetOpenOnly::ClosedRecent)];

                for oo in open_only_modes {
                    for order_filter in &order_filters {
                        let mut cursor: Option<String> = None;
                        let mut cursor_walk = CursorWalk::default();

                        loop {
                            let remaining = if let Some(limit) = remaining_limit {
                                (limit as usize).saturating_sub(all_orders.len())
                            } else {
                                usize::MAX
                            };

                            if remaining == 0 {
                                break;
                            }

                            // Max 50 per Bitget API
                            let page_limit = std::cmp::min(remaining, 50);

                            let mut p = BitgetOpenOrdersParamsBuilder::default();
                            p.category(product_type);

                            if let Some(symbol) = symbol_param.clone() {
                                p.symbol(symbol);
                            }

                            if let Some(coin) = settle_coin.clone() {
                                p.settle_coin(coin);
                            }

                            if let Some(of) = order_filter {
                                p.order_filter(*of);
                            }

                            if let Some(oo) = oo {
                                p.open_only(oo);
                            }
                            p.limit(page_limit as u32);

                            if let Some(c) = cursor {
                                p.cursor(c);
                            }
                            let params = p.build().build_anyhow()?;
                            let response: BitgetOpenOrdersResponse =
                                self.inner.get_open_orders_with(&params).await?;

                            for order in response.result.list {
                                if seen_ids.insert(order.order_id) {
                                    all_orders.push(order);
                                }
                            }

                            // The closed pass only needs the most recent page
                            if oo.is_some() {
                                break;
                            }

                            cursor = cursor_walk
                                .advance(BITGET_ORDER_REALTIME, response.result.next_page_cursor)?;

                            if cursor.is_none() {
                                break;
                            }
                        }
                    }
                }

                all_orders
            } else {
                // Query both realtime and history endpoints
                // Realtime has current open orders, history may lag for recent orders
                let mut all_orders = Vec::new();
                let mut open_orders = Vec::new();
                let mut seen_open_ids: AHashSet<Ustr> = AHashSet::new();

                // Query regular orders then conditional (stop/MIT) orders.
                // Options do not support the StopOrder filter.
                let order_filters: Vec<Option<BitgetOrderFilter>> =
                    if product_type == BitgetProductType::Option {
                        vec![None]
                    } else {
                        vec![None, Some(BitgetOrderFilter::StopOrder)]
                    };

                for order_filter in &order_filters {
                    let mut cursor: Option<String> = None;
                    let mut cursor_walk = CursorWalk::default();

                    loop {
                        let remaining = if let Some(limit) = remaining_limit {
                            (limit as usize).saturating_sub(open_orders.len())
                        } else {
                            usize::MAX
                        };

                        if remaining == 0 {
                            break;
                        }

                        // Max 50 per Bitget API
                        let page_limit = std::cmp::min(remaining, 50);

                        let mut open_params = BitgetOpenOrdersParamsBuilder::default();
                        open_params.category(product_type);

                        if let Some(symbol) = symbol_param.clone() {
                            open_params.symbol(symbol);
                        }

                        if let Some(coin) = settle_coin.clone() {
                            open_params.settle_coin(coin);
                        }

                        if let Some(of) = order_filter {
                            open_params.order_filter(*of);
                        }
                        open_params.limit(page_limit as u32);

                        if let Some(c) = cursor {
                            open_params.cursor(c);
                        }
                        let open_params = open_params.build().build_anyhow()?;
                        let open_response: BitgetOpenOrdersResponse =
                            self.inner.get_open_orders_with(&open_params).await?;

                        for order in open_response.result.list {
                            if !seen_open_ids.contains(&order.order_id) {
                                seen_open_ids.insert(order.order_id);
                                open_orders.push(order);
                            }
                        }

                        cursor = cursor_walk.advance(
                            BITGET_ORDER_REALTIME,
                            open_response.result.next_page_cursor,
                        )?;

                        if cursor.is_none() {
                            break;
                        }
                    }
                }

                let seen_order_ids: AHashSet<Ustr> = seen_open_ids;
                let total_open_orders = open_orders.len();

                all_orders.extend(open_orders);

                let mut total_history_orders = 0;

                for order_filter in &order_filters {
                    let mut cursor: Option<String> = None;
                    let mut cursor_walk = CursorWalk::default();

                    loop {
                        let total_orders = total_open_orders + total_history_orders;
                        let remaining = if let Some(limit) = remaining_limit {
                            (limit as usize).saturating_sub(total_orders)
                        } else {
                            usize::MAX
                        };

                        if remaining == 0 {
                            break;
                        }

                        // Max 50 per Bitget API
                        let page_limit = std::cmp::min(remaining, 50);

                        let mut history_params = BitgetOrderHistoryParamsBuilder::default();
                        history_params.category(product_type);

                        if let Some(symbol) = symbol_param.clone() {
                            history_params.symbol(symbol);
                        }

                        if let Some(coin) = settle_coin.clone() {
                            history_params.settle_coin(coin);
                        }

                        if let Some(of) = order_filter {
                            history_params.order_filter(*of);
                        }

                        if let Some(start) = start {
                            history_params.start_time(start.as_millisecond());
                        }

                        if let Some(end) = end {
                            history_params.end_time(end.as_millisecond());
                        }
                        history_params.limit(page_limit as u32);

                        if let Some(c) = cursor {
                            history_params.cursor(c);
                        }
                        let history_params = history_params.build().build_anyhow()?;
                        let history_response: BitgetOrderHistoryResponse =
                            self.inner.get_order_history_with(&history_params).await?;

                        // Open orders might appear in both realtime and history
                        for order in history_response.result.list {
                            if !seen_order_ids.contains(&order.order_id) {
                                all_orders.push(order);
                                total_history_orders += 1;
                            }
                        }

                        cursor = cursor_walk.advance(
                            BITGET_ORDER_HISTORY,
                            history_response.result.next_page_cursor,
                        )?;

                        if cursor.is_none() {
                            break;
                        }
                    }
                }

                all_orders
            };

            total_collected_across_coins += orders_for_coin.len();
            all_collected_orders.extend(orders_for_coin);
        }

        let ts_init = self.generate_ts_init();

        let mut reports = Vec::new();

        for order in all_collected_orders {
            if let Some(ref instrument_id) = instrument_id {
                let instrument = self.instrument_from_cache(&instrument_id.symbol)?;

                if let Ok(report) =
                    parse_order_status_report(&order, &instrument, account_id, ts_init)
                {
                    reports.push(report);
                }
            } else {
                // Bitget returns raw symbol (e.g. "ETHUSDT"), need to add product suffix for cache lookup
                // Note: instruments are stored in cache by symbol only (without venue)
                if !order.symbol.is_empty() {
                    let symbol_with_product =
                        Symbol::from_ustr_unchecked(make_bitget_symbol(order.symbol, product_type));

                    let Ok(instrument) = self.instrument_from_cache(&symbol_with_product) else {
                        log::debug!(
                            "Skipping order report for instrument not in cache: symbol={}, full_symbol={}",
                            order.symbol,
                            symbol_with_product
                        );
                        continue;
                    };

                    match parse_order_status_report(&order, &instrument, account_id, ts_init) {
                        Ok(report) => reports.push(report),
                        Err(e) => {
                            log::error!("Failed to parse order status report: {e}");
                        }
                    }
                }
            }
        }

        Ok(reports)
    }

    /// Fetches execution history (fills) for the account and returns a list of [`FillReport`]s.
    ///
    /// Executions for instruments not currently loaded in cache will be skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/trading/order-management>
    pub async fn request_fill_reports(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: Option<InstrumentId>,
        start: Option<i64>,
        end: Option<i64>,
        limit: Option<u32>,
    ) -> anyhow::Result<Vec<FillReport>> {
        // Build query parameters
        let symbol = if let Some(id) = instrument_id {
            let bitget_symbol = BitgetSymbol::new(id.symbol.as_str())?;
            Some(bitget_symbol.raw_symbol().to_string())
        } else {
            None
        };

        // Fetch all executions with pagination
        let mut all_executions = Vec::new();
        let mut cursor: Option<String> = None;
        let mut cursor_walk = CursorWalk::default();
        let mut total_executions = 0;

        loop {
            // Calculate how many more executions we can request
            let remaining = if let Some(limit) = limit {
                (limit as usize).saturating_sub(total_executions)
            } else {
                usize::MAX
            };

            // If we've reached the limit, stop
            if remaining == 0 {
                break;
            }

            // Size the page request to respect caller's limit (max 100 per Bitget API)
            let page_limit = std::cmp::min(remaining, 100);

            let params = BitgetTradeHistoryParams {
                category: product_type,
                symbol: symbol.clone(),
                base_coin: None,
                order_id: None,
                order_link_id: None,
                start_time: start,
                end_time: end,
                exec_type: None,
                limit: Some(page_limit as u32),
                cursor: cursor.clone(),
            };

            let response = self.inner.get_trade_history(&params).await?;
            for execution in response.result.list {
                // `/api/v3/trade/fills` takes no symbol filter
                if symbol
                    .as_deref()
                    .is_some_and(|symbol| execution.symbol.as_str() != symbol)
                {
                    continue;
                }

                if execution.exec_type == BitgetExecType::Funding {
                    log::debug!(
                        "Skipping funding execution: symbol={}, order_id={}, exec_id={}",
                        execution.symbol,
                        execution.order_id,
                        execution.exec_id,
                    );
                    continue;
                }

                all_executions.push(execution);
                total_executions += 1;
            }

            cursor =
                cursor_walk.advance(BITGET_EXECUTION_LIST, response.result.next_page_cursor)?;
            if cursor.is_none() {
                break;
            }
        }

        let ts_init = self.generate_ts_init();
        let mut reports = Vec::new();

        for execution in all_executions {
            // Get instrument for this execution
            // Bitget returns raw symbol (e.g. "ETHUSDT"), need to add product suffix for cache lookup
            let symbol_with_product =
                Symbol::from_ustr_unchecked(make_bitget_symbol(execution.symbol, product_type));

            let Ok(instrument) = self.instrument_from_cache(&symbol_with_product) else {
                log::debug!(
                    "Skipping fill report for instrument not in cache: symbol={}, full_symbol={}",
                    execution.symbol,
                    symbol_with_product
                );
                continue;
            };

            match parse_fill_report(&execution, account_id, &instrument, ts_init) {
                Ok(report) => reports.push(report),
                Err(e) => {
                    log::error!("Failed to parse fill report: {e}");
                }
            }
        }

        Ok(reports)
    }

    /// Fetches position information for the account and returns a list of [`PositionStatusReport`]s.
    ///
    /// Derivative position reports fail as a set if any row cannot be resolved or parsed.
    /// When SPOT position reports are enabled, an uncached instrument yields no report.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The instrument ID is invalid.
    /// - Pagination cannot advance.
    /// - A derivative position row has no symbol.
    /// - A derivative position's instrument is not cached.
    /// - A derivative position row cannot be parsed.
    /// - SPOT position reports are enabled and no instrument is specified, because wallet balances
    ///   carry no pair identity.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/catalog/trading/position-management>
    pub async fn request_position_status_reports(
        &self,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        // Handle SPOT position reports via wallet balances if flag is enabled
        if product_type == BitgetProductType::Spot {
            if self.use_spot_position_reports.load(Ordering::Relaxed) {
                let Some(instrument_id) = instrument_id else {
                    anyhow::bail!(
                        "SPOT wallet balances carry no pair identity and cannot be attributed for a bulk position report request"
                    );
                };
                return self
                    .generate_spot_position_reports_from_wallet(account_id, instrument_id)
                    .await;
            } else {
                // Return empty vector when SPOT position reports are disabled
                return Ok(Vec::new());
            }
        }

        let ts_init = self.generate_ts_init();
        let mut reports = Vec::new();

        // Build query parameters based on whether a specific instrument is requested
        let symbol = if let Some(id) = instrument_id {
            let symbol_str = id.symbol.as_str();
            if symbol_str.is_empty() {
                anyhow::bail!("InstrumentId symbol is empty");
            }
            let bitget_symbol = BitgetSymbol::new(symbol_str)?;
            Some(bitget_symbol.raw_symbol().to_string())
        } else {
            None
        };

        // Bitget filters positions by category only
        let settle_coins: &[Option<&str>] = &[None];

        for settle_coin in settle_coins {
            let params = BitgetPositionListParams {
                category: product_type,
                symbol: symbol.clone(),
                base_coin: None,
                settle_coin: settle_coin.map(str::to_string),
                limit: Some(200), // Max 200 per request
                cursor: None,
            };

            self.request_position_pages(account_id, params, ts_init, &mut reports)
                .await?;
        }

        Ok(reports)
    }

    async fn request_position_pages(
        &self,
        account_id: AccountId,
        mut params: BitgetPositionListParams,
        ts_init: UnixNanos,
        reports: &mut Vec<PositionStatusReport>,
    ) -> anyhow::Result<()> {
        let mut cursor_walk = CursorWalk::default();

        loop {
            let response = self.inner.get_positions(&params).await?;

            for position in response.result.list {
                if position.symbol.is_empty() {
                    anyhow::bail!(
                        "Position symbol is empty, category={:?}, symbol={:?}, settle_coin={:?}",
                        params.category,
                        params.symbol,
                        params.settle_coin,
                    );
                }

                let symbol = Symbol::new(format!(
                    "{}{}",
                    position.symbol.as_str(),
                    params.category.suffix()
                ));
                let instrument = self.instrument_from_cache(&symbol)?;
                let report =
                    parse_position_status_report(&position, account_id, &instrument, ts_init)
                        .with_context(|| format!("failed to parse position report for {symbol}"))?;
                reports.push(report);
            }

            params.cursor =
                cursor_walk.advance(BITGET_POSITION_LIST, response.result.next_page_cursor)?;

            if params.cursor.is_none() {
                break;
            }
        }

        Ok(())
    }

    async fn query_order_by_id(
        &self,
        product_type: BitgetProductType,
        order_id: &str,
        endpoint: &str,
        context: &str,
    ) -> anyhow::Result<BitgetOrder> {
        let mut query_params = BitgetOpenOrdersParamsBuilder::default();
        query_params.category(product_type);
        query_params.order_id(order_id.to_string());

        // Bitget serves single orders from `order-info`; conditional orders only from the
        // strategy-order lists
        if endpoint == BITGET_STRATEGY_ORDER_REALTIME {
            query_params.order_filter(BitgetOrderFilter::StopOrder);
        }

        let query_params = query_params.build().build_anyhow()?;
        let order_response: BitgetOpenOrdersResponse =
            self.inner.get_open_orders_with(&query_params).await?;

        order_response
            .result
            .list
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No order returned {context}"))
    }
}

#[cfg(test)]
mod tests {
    use nautilus_testkit::http::assert_http_redirect_rejected;
    use rstest::rstest;

    use super::*;

    #[tokio::test]
    async fn test_authenticated_client_rejects_redirects() {
        let client = BitgetRawHttpClient::build_http_client(3, None).unwrap();
        assert_http_redirect_rejected(|url| async move {
            client
                .get(url, None, None, Some(3), None)
                .await
                .unwrap()
                .status
                .as_u16()
        })
        .await;
    }

    #[rstest]
    fn test_client_creation() {
        let client = BitgetHttpClient::new(None, 60, 3, 1000, 10_000, 5_000, None);
        assert!(client.is_ok());

        let client = client.unwrap();
        assert!(client.base_url().contains("bitget.com"));
        assert!(client.credential().is_none());
    }

    #[rstest]
    fn test_client_with_credentials() {
        let client = BitgetHttpClient::with_credentials(
            "test_key".to_string(),
            "test_secret".to_string(),
            "test_passphrase".to_string(),
            Some("https://api.bitget.com".to_string()),
            60,
            3,
            1000,
            10_000,
            5_000,
            None,
        );
        assert!(client.is_ok());

        let client = client.unwrap();
        assert!(client.credential().is_some());
        assert_eq!(
            client.credential().unwrap().api_passphrase(),
            "test_passphrase"
        );
    }

    #[rstest]
    fn test_new_with_env_demo_routes_to_demo_trading() {
        let client = BitgetRawHttpClient::new_with_env(
            Some("k".to_string()),
            Some("s".to_string()),
            Some("p".to_string()),
            None,
            true,
            false,
            60,
            3,
            1000,
            10_000,
            5_000,
            None,
        )
        .unwrap();

        assert!(client.is_demo_trading());
        assert_eq!(client.base_url(), "https://api.bitget.com");
    }

    #[rstest]
    fn test_build_path_with_params() {
        #[derive(Serialize)]
        struct TestParams {
            category: String,
            symbol: String,
        }

        let params = TestParams {
            category: "USDT-FUTURES".to_string(),
            symbol: "BTCUSDT".to_string(),
        };

        let path = BitgetRawHttpClient::build_path("/api/v3/market/test", &params);
        assert!(path.is_ok());
        assert!(path.unwrap().contains("category=USDT-FUTURES"));
    }

    #[rstest]
    fn test_build_path_without_params() {
        let params = ();
        let path = BitgetRawHttpClient::build_path("/api/v3/market/time", &params);
        assert!(path.is_ok());
        assert_eq!(path.unwrap(), "/api/v3/market/time");
    }

    #[rstest]
    fn test_params_serialization_matches_build_path() {
        // This test ensures our new serialization produces the same result as the old build_path
        #[derive(Serialize)]
        struct TestParams {
            category: String,
            limit: u32,
        }

        let params = TestParams {
            category: "spot".to_string(),
            limit: 50,
        };

        // Old way: build_path serialized params
        let old_path = BitgetRawHttpClient::build_path(BITGET_ORDER_REALTIME, &params).unwrap();
        let old_query = old_path.split('?').nth(1).unwrap_or("");

        // New way: direct serialization
        let new_query = serde_urlencoded::to_string(&params).unwrap();

        // They must match for signatures to work
        assert_eq!(old_query, new_query);
    }

    #[rstest]
    fn test_params_serialization_order() {
        // Verify that serialization order is deterministic
        #[derive(Serialize)]
        struct OrderParams {
            category: String,
            symbol: String,
            limit: u32,
        }

        let params = OrderParams {
            category: "spot".to_string(),
            symbol: "BTCUSDT".to_string(),
            limit: 50,
        };

        // Serialize multiple times to ensure consistent ordering
        let query1 = serde_urlencoded::to_string(&params).unwrap();
        let query2 = serde_urlencoded::to_string(&params).unwrap();
        let query3 = serde_urlencoded::to_string(&params).unwrap();

        assert_eq!(query1, query2);
        assert_eq!(query2, query3);

        // The query should contain all params
        assert!(query1.contains("category=spot"));
        assert!(query1.contains("symbol=BTCUSDT"));
        assert!(query1.contains("limit=50"));
    }

    #[rstest]
    #[case(429, "Too Many Requests", true)]
    #[case(403, "Access too frequent", false)]
    #[case(400, "Too Many Requests", false)]
    fn test_rate_limit_403_detection(
        #[case] status: u16,
        #[case] body: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(
            BitgetRawHttpClient::is_rate_limit_403(status, body),
            expected
        );
    }

    #[rstest]
    fn test_cursor_walk_stops_on_absent_or_empty_cursor() {
        let mut walk = CursorWalk::default();

        assert_eq!(walk.advance("/endpoint", None).unwrap(), None);
        assert_eq!(
            walk.advance("/endpoint", Some(String::new())).unwrap(),
            None
        );
        assert!(walk.followed.is_empty());
    }

    #[rstest]
    fn test_cursor_walk_follows_advancing_cursors() {
        let mut walk = CursorWalk::default();

        for page in ["page-2", "page-3", "page-4"] {
            assert_eq!(
                walk.advance("/endpoint", Some(page.to_string())).unwrap(),
                Some(page.to_string())
            );
        }
    }

    #[rstest]
    fn test_cursor_walk_rejects_a_cursor_that_does_not_advance() {
        let mut walk = CursorWalk::default();
        walk.advance("/endpoint", Some("page-2".to_string()))
            .unwrap();

        let error = walk
            .advance("/endpoint", Some("page-2".to_string()))
            .expect_err("a cursor that addresses the page it came with has no next page");

        assert_eq!(
            error.to_string(),
            r#"/endpoint pagination cursor did not advance from "page-2""#
        );
    }

    #[rstest]
    #[case::two_cycle(vec!["page-2", "page-3"], "page-2")]
    #[case::long_cycle(vec!["page-2", "page-3", "page-4", "page-5"], "page-3")]
    fn test_cursor_walk_rejects_a_cursor_already_followed(
        #[case] followed: Vec<&str>,
        #[case] repeated: &str,
    ) {
        let mut walk = CursorWalk::default();
        for cursor in followed {
            walk.advance("/endpoint", Some(cursor.to_string())).unwrap();
        }

        let error = walk
            .advance("/endpoint", Some(repeated.to_string()))
            .expect_err("a cursor already followed has no further page to offer");

        assert_eq!(
            error.to_string(),
            format!(r#"/endpoint pagination repeated cursor "{repeated}""#)
        );
    }
}
