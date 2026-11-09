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

//! Bitget WebSocket client providing public market data streaming.
//!
//! Bitget API reference <https://www.bitget.com/docs/uta/quick-start>.

use std::{
    collections::HashSet,
    fmt::Debug,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
#[cfg(test)]
use nautilus_common::live::get_runtime;
use nautilus_core::{AtomicMap, AtomicSet, UUID4, string::secret::SecretString};
use nautilus_live::{
    SocketControl,
    book::snapshot::SnapshotGate,
    task::{SharedTaskSlot, TaskJoinOutcome},
};
use nautilus_model::{
    data::BarType,
    enums::{AggregationSource, OrderSide, OrderType, PriceType, TimeInForce, TriggerType},
    identifiers::{AccountId, ClientOrderId, InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use nautilus_network::{
    ConnectionUrl,
    http::create_standard_nautilus_headers,
    mode::ConnectionMode,
    ratelimiter::{RateLimiter, clock::MonotonicClock},
    websocket::{
        AuthTracker, InitialConnectRetryPolicy, SubscriptionState, TransportBackend,
        WebSocketClient, WebSocketConfig, channel_message_handler,
    },
};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use ustr::Ustr;
use zeroize::Zeroizing;

use crate::{
    common::{
        consts::{BITGET_WS_HEARTBEAT_PAYLOAD, BITGET_WS_TOPIC_DELIMITER},
        credential::Credential,
        enums::{
            BitgetBboSideType, BitgetEnvironment, BitgetOrderSide, BitgetOrderSmpType,
            BitgetOrderType, BitgetPositionIdx, BitgetProductType, BitgetWsOrderRequestOp,
            resolve_trigger_type,
        },
        parse::{
            bar_spec_to_bitget_interval, extract_base_coin, extract_raw_symbol, map_time_in_force,
            spot_leverage,
        },
        rate_limit::{
            BITGET_OPTION_SUBSCRIPTION_LIMIT, BitgetRateLimiter, batch_send_limit, batch_weight,
            websocket_connection_key, websocket_connection_limiter,
        },
        symbol::BitgetSymbol,
        urls::{bitget_ws_private_url, bitget_ws_public_url, bitget_ws_trade_url},
    },
    websocket::{
        enums::{BitgetWsOperation, BitgetWsPrivateChannel, BitgetWsPublicChannel},
        error::{BitgetWsError, BitgetWsResult},
        handler::{BitgetWsFeedHandler, BitgetWsOrderCommand, HandlerCommand},
        messages::{
            BITGET_WS_PRIVATE_INST_TYPE, BitgetAuthRequest, BitgetSubscription,
            BitgetWsAmendOrderParams, BitgetWsArg, BitgetWsBatchAmendItem,
            BitgetWsBatchAmendOrderArgs, BitgetWsBatchCancelItem, BitgetWsBatchCancelOrderArgs,
            BitgetWsBatchPlaceItem, BitgetWsBatchPlaceOrderArgs, BitgetWsCancelOrderParams,
            BitgetWsMessage, BitgetWsPlaceOrderParams,
        },
    },
};

// Orders wait this long for (re-)login before they are rejected locally as not sent
const AUTH_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bitget batch order maximum per request.
pub const BATCH_PROCESSING_LIMIT: usize = 20;
/// Public/market data WebSocket client for Bitget.
pub struct BitgetWebSocketClient {
    url: ConnectionUrl,
    environment: BitgetEnvironment,
    product_type: Option<BitgetProductType>,
    credential: Option<Credential>,
    requires_auth: bool,
    auth_tracker: AuthTracker,
    heartbeat: Option<u64>,
    auth_wait_timeout: Duration,
    connection_mode: Arc<ArcSwap<AtomicU8>>,
    cmd_tx: Arc<tokio::sync::RwLock<tokio::sync::mpsc::UnboundedSender<HandlerCommand>>>,
    out_rx: Option<Arc<tokio::sync::mpsc::UnboundedReceiver<BitgetWsMessage>>>,
    signal: Arc<AtomicBool>,
    task_handle: Arc<SharedTaskSlot<()>>,
    connect_lock: Arc<tokio::sync::Mutex<()>>,
    subscriptions: SubscriptionState,
    subscription_guard: Arc<tokio::sync::Mutex<()>>,
    rate_limiter: BitgetRateLimiter,
    recv_window_ms: Arc<AtomicU64>,
    account_id: Option<AccountId>,
    mm_level: Arc<AtomicU8>,
    bar_types_cache: Arc<AtomicMap<String, BarType>>,
    instruments_cache: Arc<AtomicMap<Ustr, InstrumentAny>>,
    trade_subs: Arc<AtomicSet<InstrumentId>>,
    option_greeks_subs: Arc<AtomicSet<InstrumentId>>,
    transport_backend: TransportBackend,
    cancellation_token: Arc<ArcSwap<CancellationToken>>,
    proxy_url: Option<SecretString>,
    socket_control: Option<SocketControl>,
}

struct ConnectRollback {
    signal: Arc<AtomicBool>,
    cancellation_token: Arc<ArcSwap<CancellationToken>>,
    task_handle: Arc<SharedTaskSlot<()>>,
    armed: bool,
}

impl ConnectRollback {
    fn new(client: &BitgetWebSocketClient) -> Self {
        Self {
            signal: Arc::clone(&client.signal),
            cancellation_token: Arc::clone(&client.cancellation_token),
            task_handle: Arc::clone(&client.task_handle),
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ConnectRollback {
    fn drop(&mut self) {
        if self.armed {
            self.signal.store(true, Ordering::Release);
            self.cancellation_token.load().cancel();
            self.task_handle.abort();
        }
    }
}

impl Debug for BitgetWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BitgetWebSocketClient))
            .field("url", &self.url)
            .field("environment", &self.environment)
            .field("product_type", &self.product_type)
            .field("requires_auth", &self.requires_auth)
            .field("heartbeat", &self.heartbeat)
            .field("confirmed_subscriptions", &self.subscriptions.len())
            .finish()
    }
}

impl Clone for BitgetWebSocketClient {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            environment: self.environment,
            product_type: self.product_type,
            credential: self.credential.clone(),
            requires_auth: self.requires_auth,
            auth_tracker: self.auth_tracker.clone(),
            heartbeat: self.heartbeat,
            auth_wait_timeout: self.auth_wait_timeout,
            connection_mode: Arc::clone(&self.connection_mode),
            cmd_tx: Arc::clone(&self.cmd_tx),
            out_rx: None, // Each clone gets its own receiver
            signal: Arc::clone(&self.signal),
            task_handle: Arc::clone(&self.task_handle),
            connect_lock: Arc::clone(&self.connect_lock),
            subscriptions: self.subscriptions.clone(),
            subscription_guard: Arc::clone(&self.subscription_guard),
            rate_limiter: self.rate_limiter.clone(),
            recv_window_ms: Arc::clone(&self.recv_window_ms),
            account_id: self.account_id,
            mm_level: Arc::clone(&self.mm_level),
            bar_types_cache: Arc::clone(&self.bar_types_cache),
            instruments_cache: Arc::clone(&self.instruments_cache),
            trade_subs: Arc::clone(&self.trade_subs),
            option_greeks_subs: Arc::clone(&self.option_greeks_subs),
            transport_backend: self.transport_backend,
            cancellation_token: Arc::clone(&self.cancellation_token),
            proxy_url: self.proxy_url.clone(),
            socket_control: self.socket_control.clone(),
        }
    }
}

impl BitgetWebSocketClient {
    fn initial_connect_retry_policy() -> InitialConnectRetryPolicy {
        InitialConnectRetryPolicy {
            max_attempts: NonZeroU32::new(5).expect("initial connect attempts must be non-zero"),
            delay_initial: Duration::from_millis(500),
            delay_max: Duration::from_secs(5),
            backoff_factor: 2.0,
            jitter_ms: 250,
        }
    }

    /// Creates a new Bitget public WebSocket client.
    #[must_use]
    pub fn new_public(url: Option<String>, heartbeat: u64) -> Self {
        Self::new_public_with(
            BitgetProductType::Linear,
            BitgetEnvironment::Mainnet,
            url,
            heartbeat,
            TransportBackend::default(),
            None,
        )
    }

    /// Sets the timeout for waiting on (re)authentication before failing an
    /// authenticated operation. Defaults to `AUTH_WAIT_TIMEOUT` (10s).
    pub fn set_auth_wait_timeout(&mut self, timeout: Duration) {
        self.auth_wait_timeout = timeout;
    }

    /// Sets the receive window for WebSocket trade commands.
    ///
    /// Bitget trade requests carry no receive window (a per-order `receiveWindow` would cancel
    /// resting orders), so the value is stored only.
    pub fn set_recv_window_ms(&self, recv_window_ms: u64) {
        self.recv_window_ms.store(recv_window_ms, Ordering::Release);
    }

    /// Creates a new Bitget public WebSocket client targeting the specified product/environment.
    #[must_use]
    pub fn new_public_with(
        product_type: BitgetProductType,
        environment: BitgetEnvironment,
        url: Option<String>,
        heartbeat: u64,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::unbounded_channel::<HandlerCommand>();

        let initial_mode = AtomicU8::new(ConnectionMode::Closed.as_u8());
        let connection_mode = Arc::new(ArcSwap::from_pointee(initial_mode));
        let resolved_url = url.unwrap_or_else(|| bitget_ws_public_url(product_type, environment));
        let rate_limiter =
            BitgetRateLimiter::for_websocket(&resolved_url, None, proxy_url.as_deref());

        Self {
            url: resolved_url.into(),
            environment,
            product_type: Some(product_type),
            credential: None,
            requires_auth: false,
            auth_tracker: AuthTracker::new(),
            heartbeat: Some(heartbeat),
            auth_wait_timeout: AUTH_WAIT_TIMEOUT,
            connection_mode,
            cmd_tx: Arc::new(tokio::sync::RwLock::new(cmd_tx)),
            out_rx: None,
            signal: Arc::new(AtomicBool::new(false)),
            task_handle: Arc::new(SharedTaskSlot::new()),
            connect_lock: Arc::new(tokio::sync::Mutex::new(())),
            subscriptions: SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER),
            subscription_guard: Arc::new(tokio::sync::Mutex::new(())),
            rate_limiter,
            recv_window_ms: Arc::new(AtomicU64::new(5_000)),
            bar_types_cache: Arc::new(AtomicMap::new()),
            instruments_cache: Arc::new(AtomicMap::new()),
            trade_subs: Arc::new(AtomicSet::new()),
            option_greeks_subs: Arc::new(AtomicSet::new()),
            account_id: None,
            mm_level: Arc::new(AtomicU8::new(0)),
            transport_backend,
            cancellation_token: Arc::new(ArcSwap::from_pointee(CancellationToken::new())),
            proxy_url: proxy_url.map(SecretString::from),
            socket_control: None,
        }
    }

    /// Configures socket state reporting and reconnect control.
    #[must_use]
    pub fn with_socket_control(mut self, control: SocketControl) -> Self {
        self.socket_control = Some(control);
        self
    }

    /// Creates a new Bitget private WebSocket client.
    ///
    /// If `api_key`, `api_secret` or `api_passphrase` are not provided, they will be loaded from
    /// environment variables based on the environment:
    /// - Demo/Testnet: `BITGET_DEMO_API_KEY`, `BITGET_DEMO_API_SECRET`, `BITGET_DEMO_API_PASSPHRASE`
    /// - Mainnet: `BITGET_API_KEY`, `BITGET_API_SECRET`, `BITGET_API_PASSPHRASE`
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new_private(
        environment: BitgetEnvironment,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        url: Option<String>,
        heartbeat: u64,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        let credential = Credential::resolve(api_key, api_secret, api_passphrase, environment);

        let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::unbounded_channel::<HandlerCommand>();

        let initial_mode = AtomicU8::new(ConnectionMode::Closed.as_u8());
        let connection_mode = Arc::new(ArcSwap::from_pointee(initial_mode));
        let resolved_url = url.unwrap_or_else(|| bitget_ws_private_url(environment).to_string());
        let rate_limiter = BitgetRateLimiter::for_websocket(
            &resolved_url,
            credential.as_ref().map(Credential::api_key),
            proxy_url.as_deref(),
        );

        Self {
            url: resolved_url.into(),
            environment,
            product_type: None,
            credential,
            requires_auth: true,
            auth_tracker: AuthTracker::new(),
            heartbeat: Some(heartbeat),
            auth_wait_timeout: AUTH_WAIT_TIMEOUT,
            connection_mode,
            cmd_tx: Arc::new(tokio::sync::RwLock::new(cmd_tx)),
            out_rx: None,
            signal: Arc::new(AtomicBool::new(false)),
            task_handle: Arc::new(SharedTaskSlot::new()),
            connect_lock: Arc::new(tokio::sync::Mutex::new(())),
            subscriptions: SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER),
            subscription_guard: Arc::new(tokio::sync::Mutex::new(())),
            rate_limiter,
            recv_window_ms: Arc::new(AtomicU64::new(5_000)),
            bar_types_cache: Arc::new(AtomicMap::new()),
            instruments_cache: Arc::new(AtomicMap::new()),
            trade_subs: Arc::new(AtomicSet::new()),
            option_greeks_subs: Arc::new(AtomicSet::new()),
            account_id: None,
            mm_level: Arc::new(AtomicU8::new(0)),
            transport_backend,
            cancellation_token: Arc::new(ArcSwap::from_pointee(CancellationToken::new())),
            proxy_url: proxy_url.map(SecretString::from),
            socket_control: None,
        }
    }

    /// Creates a new Bitget trade WebSocket client for order operations.
    ///
    /// Bitget has no separate trade endpoint: `op:"trade"` requests run on a logged-in private
    /// connection, so this client logs in to the private URL.
    ///
    /// If `api_key`, `api_secret` or `api_passphrase` are not provided, they will be loaded from
    /// environment variables based on the environment:
    /// - Demo/Testnet: `BITGET_DEMO_API_KEY`, `BITGET_DEMO_API_SECRET`, `BITGET_DEMO_API_PASSPHRASE`
    /// - Mainnet: `BITGET_API_KEY`, `BITGET_API_SECRET`, `BITGET_API_PASSPHRASE`
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new_trade(
        environment: BitgetEnvironment,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        url: Option<String>,
        heartbeat: u64,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        let credential = Credential::resolve(api_key, api_secret, api_passphrase, environment);

        let (cmd_tx, _) = tokio::sync::mpsc::unbounded_channel::<HandlerCommand>();

        let initial_mode = AtomicU8::new(ConnectionMode::Closed.as_u8());
        let connection_mode = Arc::new(ArcSwap::from_pointee(initial_mode));
        let resolved_url = url.unwrap_or_else(|| bitget_ws_trade_url(environment).to_string());
        let rate_limiter = BitgetRateLimiter::for_websocket(
            &resolved_url,
            credential.as_ref().map(Credential::api_key),
            proxy_url.as_deref(),
        );

        Self {
            url: resolved_url.into(),
            environment,
            product_type: None,
            credential,
            requires_auth: true,
            auth_tracker: AuthTracker::new(),
            heartbeat: Some(heartbeat),
            auth_wait_timeout: AUTH_WAIT_TIMEOUT,
            connection_mode,
            cmd_tx: Arc::new(tokio::sync::RwLock::new(cmd_tx)),
            out_rx: None,
            signal: Arc::new(AtomicBool::new(false)),
            task_handle: Arc::new(SharedTaskSlot::new()),
            connect_lock: Arc::new(tokio::sync::Mutex::new(())),
            subscriptions: SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER),
            subscription_guard: Arc::new(tokio::sync::Mutex::new(())),
            rate_limiter,
            recv_window_ms: Arc::new(AtomicU64::new(5_000)),
            bar_types_cache: Arc::new(AtomicMap::new()),
            instruments_cache: Arc::new(AtomicMap::new()),
            trade_subs: Arc::new(AtomicSet::new()),
            option_greeks_subs: Arc::new(AtomicSet::new()),
            account_id: None,
            mm_level: Arc::new(AtomicU8::new(0)),
            transport_backend,
            cancellation_token: Arc::new(ArcSwap::from_pointee(CancellationToken::new())),
            proxy_url: proxy_url.map(SecretString::from),
            socket_control: None,
        }
    }

    pub(crate) fn begin_shutdown(&self) {
        self.cancellation_token.load().cancel();
        self.signal.store(true, Ordering::Release);
    }

    /// Establishes the WebSocket connection.
    ///
    /// Clears subscription state from earlier sessions, so callers resubscribe after connecting.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying WebSocket connection cannot be established,
    /// after retrying multiple times with exponential backoff.
    pub async fn connect(&mut self) -> BitgetWsResult<()> {
        let connect_lock = Arc::clone(&self.connect_lock);
        let _guard = connect_lock.lock().await;
        self.connect_locked().await
    }

    async fn connect_locked(&mut self) -> BitgetWsResult<()> {
        if !self.task_handle.is_empty() {
            self.close_locked().await?;
        }
        self.signal.store(false, Ordering::Relaxed);
        let cancellation_token = CancellationToken::new();
        self.cancellation_token
            .store(Arc::new(cancellation_token.clone()));

        let (raw_handler, raw_rx) = channel_message_handler();

        // Inbound Ping frames are answered by the transport, so no ping handler is needed;
        // the reader routes them away from the message channel and the handler never sees them.

        // Bitget keepalive is the plain text `ping`, answered with `pong`
        let ping_msg = BITGET_WS_HEARTBEAT_PAYLOAD.to_string();

        let config = WebSocketConfig {
            url: self.url.expose().to_owned(),
            headers: Self::default_headers(),
            heartbeat_interval_secs: self.heartbeat,
            heartbeat_payload: Some(ping_msg),
            connect_timeout_ms: Some(5_000),
            reconnect_delay_initial_ms: Some(500),
            reconnect_delay_max_ms: Some(5_000),
            reconnect_backoff_factor: Some(1.5),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            heartbeat_timeout_secs: None,
            idle_timeout_ms: None,
            writer_capacity: None,
            backend: self.transport_backend,
            proxy_url: self
                .proxy_url
                .as_ref()
                .map(|value| value.expose_secret().to_owned()),
            max_message_size_bytes: None,
            max_frame_size_bytes: None,
        };

        let message_rate_limiter = Arc::new(RateLimiter::<Ustr, MonotonicClock>::new_with_quota(
            None,
            vec![],
        ));
        let connection_rate_limiter = websocket_connection_limiter(
            self.url.expose(),
            self.proxy_url.as_ref().map(SecretString::expose_secret),
        );
        let connection_rate_keys: Arc<[Ustr]> = Arc::from([websocket_connection_key()]);
        let client = WebSocketClient::builder()
            .config(config.clone())
            .message_handler(raw_handler.clone())
            .rate_limiter(Arc::clone(&message_rate_limiter))
            .connection_rate_limiter(Arc::clone(&connection_rate_limiter))
            .connection_rate_keys(Arc::clone(&connection_rate_keys))
            .initial_connect_retry_policy(Self::initial_connect_retry_policy())
            .cancellation_token(cancellation_token)
            .maybe_state_sink(self.socket_control.as_ref().map(SocketControl::sink))
            .connect()
            .await
            .map_err(|e| {
                BitgetWsError::Transport(format!(
                    "WebSocket connection failed: {}: {}",
                    self.url.origin(),
                    e.redacted_message()
                ))
            })?;

        self.connection_mode.store(client.connection_mode_atomic());
        let reconnect_handle = client.reconnect_handle();
        client.set_auth_tracker(self.auth_tracker.clone(), self.requires_auth);

        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<BitgetWsMessage>();
        self.out_rx = Some(Arc::new(out_rx));

        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<HandlerCommand>();

        // Queue the client first so no subscribe reaches the handler before it
        cmd_tx
            .send(HandlerCommand::SetClient(client))
            .map_err(|e| BitgetWsError::Send(e.to_string()))?;

        {
            // Commands sent before this swap went to a closed channel and close skips
            // unsubscribes, so the new session starts from clean subscription state.
            let _guard = self.subscription_guard.lock().await;
            *self.cmd_tx.write().await = cmd_tx.clone();
            self.subscriptions.clear();
        }

        let signal = Arc::clone(&self.signal);
        let subscriptions = self.subscriptions.clone();
        let credential = self.credential.clone();
        let requires_auth = self.requires_auth;
        let cmd_tx_for_reconnect = cmd_tx.clone();
        let auth_tracker = self.auth_tracker.clone();
        let auth_tracker_for_handler = auth_tracker.clone();
        let rate_limiter = self.rate_limiter.clone();
        let inst_type = self.inst_type();
        let mut rollback = ConnectRollback::new(self);

        if let Err(e) = self.task_handle.spawn(async move {
            let mut handler = BitgetWsFeedHandler::new(
                signal.clone(),
                cmd_rx,
                raw_rx,
                auth_tracker_for_handler,
                subscriptions.clone(),
                inst_type,
                rate_limiter,
            );

            // Resubscribe all tracked subscriptions after reconnection
            let resubscribe_all = || async {
                let topics = subscriptions.all_topics();

                if topics.is_empty() {
                    return;
                }

                log::debug!(
                    "Resubscribing to confirmed subscriptions: count={}",
                    topics.len()
                );

                for topic in &topics {
                    subscriptions.mark_subscribe(topic.as_str());
                }

                let mut payloads = Vec::with_capacity(topics.len());
                for topic in &topics {
                    let message = BitgetSubscription {
                        op: BitgetWsOperation::Subscribe,
                        args: vec![BitgetWsArg::from_topic(topic, inst_type)],
                    };

                    if let Ok(payload) = serde_json::to_string(&message) {
                        payloads.push(payload);
                    }
                }

                let cmd = HandlerCommand::Subscribe { topics: payloads };

                if let Err(e) = cmd_tx_for_reconnect.send(cmd) {
                    log::error!("Failed to send resubscribe command: {e}");
                }
            };

            // Run message processing with reconnection handling
            loop {
                match handler.next().await {
                    Some(BitgetWsMessage::Reconnected) => {
                        if signal.load(Ordering::Relaxed) {
                            continue;
                        }

                        log::info!("WebSocket reconnected");

                        subscriptions.reset_after_reconnect();

                        if requires_auth {
                            log::debug!("Re-authenticating after reconnection");

                            if let Some(cred) = &credential {
                                // Begin auth attempt so succeed() will update state
                                let _rx = auth_tracker.begin();

                                let auth_message = Zeroizing::new(login_request(cred));

                                if let Ok(payload) =
                                    serde_json::to_string(&*auth_message).map(SecretString::from)
                                {
                                    let cmd = HandlerCommand::Authenticate { payload };
                                    if let Err(e) = cmd_tx_for_reconnect.send(cmd) {
                                        log::error!(
                                            "Failed to send reconnection auth command: error={e}"
                                        );
                                    }
                                } else {
                                    log::error!("Failed to serialize reconnection auth message");
                                }
                            }
                        }

                        // Unauthenticated sessions resubscribe immediately after reconnection,
                        // authenticated sessions wait for Auth message
                        if !requires_auth {
                            log::debug!("No authentication required, resubscribing immediately");
                            resubscribe_all().await;
                        }

                        // Forward to out_tx so caller sees the Reconnected message
                        if out_tx.send(BitgetWsMessage::Reconnected).is_err() {
                            if handler.is_stopped() {
                                log::debug!("Receiver dropped, stopping");
                            } else {
                                log::error!("Receiver dropped, stopping");
                            }
                            break;
                        }
                    }
                    Some(BitgetWsMessage::Auth(ref auth)) => {
                        let is_success = auth.success.unwrap_or(false) || auth.ret_code == Some(0);
                        if is_success {
                            log::debug!("Authenticated, resubscribing");
                            resubscribe_all().await;
                        }

                        if out_tx.send(BitgetWsMessage::Auth(auth.clone())).is_err() {
                            if handler.is_stopped() {
                                log::debug!("Failed to send message (receiver dropped)");
                            } else {
                                log::error!("Failed to send message (receiver dropped)");
                            }
                            break;
                        }
                    }
                    Some(msg) => {
                        if out_tx.send(msg).is_err() {
                            if handler.is_stopped() {
                                log::debug!("Failed to send message (receiver dropped)");
                            } else {
                                log::error!("Failed to send message (receiver dropped)");
                            }
                            break;
                        }
                    }
                    None => {
                        // Stream ended - check if it's a stop signal
                        if handler.is_stopped() {
                            log::debug!("Stop signal received, ending message processing");
                            break;
                        }
                        // Otherwise it's an unexpected stream end
                        log::warn!("WebSocket stream ended unexpectedly");
                        break;
                    }
                }
            }

            log::debug!("Handler task exiting");
        }) {
            let shutdown_result = self.close_locked().await;
            return Err(BitgetWsError::ClientError(match shutdown_result {
                Ok(()) => format!("Failed to start WebSocket handler task: {e}"),
                Err(shutdown_error) => format!(
                    "Failed to start WebSocket handler task: {e}; startup rollback failed: \
                     {shutdown_error}"
                ),
            }));
        }

        if let Some(control) = &self.socket_control {
            control.register(move || reconnect_handle.request_reconnect());
        }

        if requires_auth && let Err(e) = self.authenticate_if_required().await {
            let result = match self.close_locked().await {
                Ok(()) => Err(e),
                Err(shutdown_error) => Err(BitgetWsError::ClientError(format!(
                    "{e}; startup rollback failed: {shutdown_error}"
                ))),
            };
            rollback.disarm();
            return result;
        }

        rollback.disarm();
        Ok(())
    }

    /// Disconnects the WebSocket client and stops the background task.
    pub async fn close(&mut self) -> BitgetWsResult<()> {
        let connect_lock = Arc::clone(&self.connect_lock);
        let _guard = connect_lock.lock().await;
        self.close_locked().await
    }

    async fn close_locked(&self) -> BitgetWsResult<()> {
        log::debug!("Starting close process");

        self.signal.store(true, Ordering::Relaxed);
        self.cancellation_token.load().cancel();

        let cmd = HandlerCommand::Disconnect;
        if let Err(e) = self.cmd_tx.read().await.send(cmd) {
            log::debug!(
                "Failed to send disconnect command (handler may already be shut down): {e}"
            );
        }

        let task_result = if self.task_handle.is_empty() {
            log::debug!("No task handle to await");
            Ok(())
        } else {
            log::debug!("Waiting for task handle to complete");

            if let Some(outcome) = self
                .task_handle
                .finish(Duration::from_secs(2), Duration::from_secs(2))
                .await
            {
                match outcome {
                    TaskJoinOutcome::Completed(()) | TaskJoinOutcome::Aborted => Ok(()),
                    TaskJoinOutcome::Failed(error) => Err(BitgetWsError::ClientError(format!(
                        "WebSocket handler task failed: {error}"
                    ))),
                    TaskJoinOutcome::Incomplete => Err(BitgetWsError::ClientError(
                        "WebSocket handler task did not stop after abort".to_string(),
                    )),
                }
            } else {
                Ok(())
            }
        };

        self.auth_tracker.invalidate();

        if let Some(control) = &self.socket_control {
            control.deregister();
        }

        log::debug!("Closed");

        task_result
    }

    /// Returns a value indicating whether the client is active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        let connection_mode_arc = self.connection_mode.load();
        ConnectionMode::from_atomic(&connection_mode_arc).is_active()
            && !self.signal.load(Ordering::Relaxed)
    }

    /// Returns a value indicating whether the client is closed.
    pub fn is_closed(&self) -> bool {
        let connection_mode_arc = self.connection_mode.load();
        ConnectionMode::from_atomic(&connection_mode_arc).is_closed()
            || self.signal.load(Ordering::Relaxed)
    }

    /// Waits until the WebSocket client becomes active or times out.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout is exceeded before the client becomes active.
    pub async fn wait_until_active(&self, timeout_secs: f64) -> BitgetWsResult<()> {
        let timeout = tokio::time::Duration::from_secs_f64(timeout_secs);

        tokio::time::timeout(timeout, async {
            while !self.is_active() {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            BitgetWsError::ClientError(format!(
                "WebSocket connection timeout after {timeout_secs} seconds"
            ))
        })?;

        Ok(())
    }

    /// Subscribe to the provided topic strings.
    pub async fn subscribe(&self, topics: Vec<String>) -> BitgetWsResult<()> {
        if topics.is_empty() {
            return Ok(());
        }
        let _guard = self.subscription_guard.lock().await;

        log::debug!("Subscribing to topics: {topics:?}");

        let payloads = self.register_subscribe(topics)?;

        if payloads.is_empty() {
            return Ok(());
        }

        let cmd = HandlerCommand::Subscribe { topics: payloads };
        self.cmd_tx
            .read()
            .await
            .send(cmd)
            .map_err(|e| BitgetWsError::Send(format!("Failed to send subscribe command: {e}")))?;

        Ok(())
    }

    // Adds a reference to each topic, returning subscribe payloads for topics no other
    // subscription holds; the caller holds the subscription guard
    fn register_subscribe(&self, topics: Vec<String>) -> BitgetWsResult<Vec<String>> {
        if self.product_type == Some(BitgetProductType::Option) {
            let occupied_topics = self
                .subscriptions
                .all_topics()
                .into_iter()
                .chain(self.subscriptions.pending_unsubscribe_topics())
                .collect::<HashSet<_>>();
            let new_topics = topics
                .iter()
                .filter(|topic| !occupied_topics.contains(topic.as_str()))
                .collect::<HashSet<_>>()
                .len();
            let requested = occupied_topics.len() + new_topics;
            if requested > BITGET_OPTION_SUBSCRIPTION_LIMIT {
                return Err(BitgetWsError::ClientError(format!(
                    "Option WebSocket subscription limit is {BITGET_OPTION_SUBSCRIPTION_LIMIT} arguments per connection, requested {requested}"
                )));
            }
        }

        // Use reference counting to deduplicate subscriptions
        let mut topics_to_send = Vec::new();

        for topic in topics {
            // Returns true if this is the first subscription (ref count 0 -> 1)
            if self.subscriptions.add_reference(&topic) {
                self.subscriptions.mark_subscribe(&topic);
                topics_to_send.push(topic.clone());
            } else {
                log::debug!("Already subscribed to {topic}, skipping duplicate subscription");
            }
        }

        // Serialize subscription messages
        let mut payloads = Vec::with_capacity(topics_to_send.len());
        for topic in &topics_to_send {
            let message = BitgetSubscription {
                op: BitgetWsOperation::Subscribe,
                args: vec![BitgetWsArg::from_topic(topic, self.inst_type())],
            };
            let payload = serde_json::to_string(&message).map_err(|e| {
                BitgetWsError::Json(format!("Failed to serialize subscription: {e}"))
            })?;
            payloads.push(payload);
        }

        Ok(payloads)
    }

    /// Unsubscribe from the provided topics.
    pub async fn unsubscribe(&self, topics: Vec<String>) -> BitgetWsResult<()> {
        if topics.is_empty() {
            return Ok(());
        }

        log::debug!("Attempting to unsubscribe from topics: {topics:?}");

        if self.signal.load(Ordering::Relaxed) {
            log::debug!("Shutdown signal detected, skipping unsubscribe");
            return Ok(());
        }
        let _guard = self.subscription_guard.lock().await;

        // Use reference counting to avoid unsubscribing while other consumers still need the topic
        let mut topics_to_send = Vec::new();

        for topic in topics {
            // Returns true if this was the last subscription (ref count 1 -> 0)
            if self.subscriptions.remove_reference(&topic) {
                self.subscriptions.mark_unsubscribe(&topic);
                topics_to_send.push(topic.clone());
            } else {
                log::debug!("Topic {topic} still has active subscriptions, not unsubscribing");
            }
        }

        if topics_to_send.is_empty() {
            return Ok(());
        }

        // Serialize unsubscription messages
        let mut payloads = Vec::with_capacity(topics_to_send.len());
        for topic in &topics_to_send {
            let message = BitgetSubscription {
                op: BitgetWsOperation::Unsubscribe,
                args: vec![BitgetWsArg::from_topic(topic, self.inst_type())],
            };

            if let Ok(payload) = serde_json::to_string(&message) {
                payloads.push(payload);
            }
        }

        let cmd = HandlerCommand::Unsubscribe { topics: payloads };
        if let Err(e) = self.cmd_tx.read().await.send(cmd) {
            log::debug!("Failed to send unsubscribe command: error={e}");
        }

        Ok(())
    }

    /// Returns a stream of venue-typed [`BitgetWsMessage`] items.
    ///
    /// # Panics
    ///
    /// Panics if called before [`Self::connect`] or if the stream has already been taken.
    pub fn stream(&mut self) -> impl futures_util::Stream<Item = BitgetWsMessage> + use<> {
        let rx = self
            .out_rx
            .take()
            .expect("Stream receiver already taken or client not connected");
        let mut rx = Arc::try_unwrap(rx).expect("Cannot take ownership - other references exist");
        async_stream::stream! {
            while let Some(msg) = rx.recv().await {
                yield msg;
            }
        }
    }

    /// Returns the number of currently registered subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Returns the credential associated with this client, if any.
    #[must_use]
    pub fn credential(&self) -> Option<&Credential> {
        self.credential.as_ref()
    }

    /// Sets the account ID for account message parsing.
    pub fn set_account_id(&mut self, account_id: AccountId) {
        self.account_id = Some(account_id);
    }

    /// Sets the account market maker level.
    pub fn set_mm_level(&self, mm_level: u8) {
        self.mm_level.store(mm_level, Ordering::Relaxed);
    }

    /// Returns the account ID if set.
    #[must_use]
    pub fn account_id(&self) -> Option<AccountId> {
        self.account_id
    }

    /// Returns the product type for public connections.
    #[must_use]
    pub fn product_type(&self) -> Option<BitgetProductType> {
        self.product_type
    }

    // Bitget subscription args carry the product as `instType`; private channels use `UTA`
    fn inst_type(&self) -> &'static str {
        self.product_type.map_or(
            BITGET_WS_PRIVATE_INST_TYPE,
            BitgetProductType::as_ws_inst_type,
        )
    }

    /// Returns a reference to the bar types cache.
    #[must_use]
    pub fn bar_types_cache(&self) -> &Arc<AtomicMap<String, BarType>> {
        &self.bar_types_cache
    }

    /// Adds an instrument to the shared instruments cache.
    pub fn cache_instrument(&self, instrument: InstrumentAny) {
        self.instruments_cache
            .insert(instrument.id().symbol.inner(), instrument);
    }

    /// Returns a snapshot of the instruments cache keyed by symbol.
    #[must_use]
    pub fn instruments_snapshot(&self) -> ahash::AHashMap<Ustr, InstrumentAny> {
        (**self.instruments_cache.load()).clone()
    }

    /// Adds an instrument ID to the option greeks subscription set.
    pub fn add_option_greeks_sub(&self, instrument_id: InstrumentId) {
        self.option_greeks_subs.insert(instrument_id);
    }

    /// Removes an instrument ID from the option greeks subscription set.
    pub fn remove_option_greeks_sub(&self, instrument_id: &InstrumentId) {
        self.option_greeks_subs.remove(instrument_id);
    }

    /// Returns a reference to the option greeks subscription set.
    #[must_use]
    pub fn option_greeks_subs(&self) -> &Arc<AtomicSet<InstrumentId>> {
        &self.option_greeks_subs
    }

    /// Returns a reference to the trade subscriptions set.
    #[must_use]
    pub fn trade_subs(&self) -> &Arc<AtomicSet<InstrumentId>> {
        &self.trade_subs
    }

    /// Returns a reference to the live instruments cache Arc.
    #[must_use]
    pub fn instruments_cache_ref(&self) -> &Arc<AtomicMap<Ustr, InstrumentAny>> {
        &self.instruments_cache
    }

    /// Subscribes to orderbook updates for a specific instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel>
    pub async fn subscribe_orderbook(
        &self,
        instrument_id: InstrumentId,
        depth: u32,
    ) -> BitgetWsResult<()> {
        let topic = orderbook_topic(instrument_id, depth);
        self.subscribe(vec![topic]).await
    }

    /// Subscribes to an order book topic, opening `gate` once the connection confirms the
    /// subscribe write.
    ///
    /// A topic another subscription already holds, such as the depth-1 topic quotes use, sends
    /// nothing and opens `gate` at once. A failed write returns [`BitgetWsError::Transport`]; any
    /// other error means the request never reached the connection.
    pub(crate) async fn subscribe_book(
        &self,
        instrument_id: InstrumentId,
        depth: u32,
        cancel: CancellationToken,
        gate: SnapshotGate,
    ) -> BitgetWsResult<()> {
        let (completion, receiver) = tokio::sync::oneshot::channel();

        {
            let _guard = self.subscription_guard.lock().await;
            let topic = orderbook_topic(instrument_id, depth);

            let Some(payload) = self.register_subscribe(vec![topic])?.pop() else {
                gate.open();
                return Ok(());
            };

            let cmd = HandlerCommand::SubscribeBook {
                payload,
                gate,
                completion,
            };

            self.cmd_tx.read().await.send(cmd).map_err(|e| {
                BitgetWsError::Send(format!("Failed to send subscribe command: {e}"))
            })?;
        }

        tokio::select! {
            biased;
            () = cancel.cancelled() => Ok(()),
            result = receiver => result.map_err(|e| BitgetWsError::Send(e.to_string()))?,
        }
    }

    /// Requests a fresh snapshot by replacing an order book subscription on the current
    /// connection.
    pub(crate) async fn resubscribe_book(
        &self,
        instrument_id: InstrumentId,
        depth: u32,
        cancel: CancellationToken,
        gate: SnapshotGate,
    ) -> BitgetWsResult<()> {
        let topic = orderbook_topic(instrument_id, depth);
        let (completion, receiver) = tokio::sync::oneshot::channel();
        let sender = self.cmd_tx.read().await;

        if cancel.is_cancelled() {
            return Err(BitgetWsError::ClientError(
                "Book recovery canceled".to_string(),
            ));
        }

        let cmd = HandlerCommand::ResubscribeBook {
            topic,
            cancel,
            gate,
            completion,
        };

        sender
            .send(cmd)
            .map_err(|e| BitgetWsError::Send(format!("Failed to send resubscribe command: {e}")))?;
        drop(sender);

        receiver
            .await
            .map_err(|e| BitgetWsError::Send(e.to_string()))?
    }

    /// Unsubscribes from orderbook updates for a specific instrument.
    pub async fn unsubscribe_orderbook(
        &self,
        instrument_id: InstrumentId,
        depth: u32,
    ) -> BitgetWsResult<()> {
        let topic = orderbook_topic(instrument_id, depth);
        self.unsubscribe(vec![topic]).await
    }

    /// Subscribes to public trade updates for a specific instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/public/New-Trades-Channel>
    pub async fn subscribe_trades(&self, instrument_id: InstrumentId) -> BitgetWsResult<()> {
        self.trade_subs.insert(instrument_id);
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
        // Bybit-style option base-coin topics never apply: Bitget lists no options
        let topic_symbol = match self.product_type {
            Some(BitgetProductType::Option) => extract_base_coin(raw_symbol),
            _ => raw_symbol,
        };
        let topic = format!(
            "{}.{topic_symbol}",
            BitgetWsPublicChannel::PublicTrade.as_ref()
        );
        self.subscribe(vec![topic]).await
    }

    /// Unsubscribes from public trade updates for a specific instrument.
    pub async fn unsubscribe_trades(&self, instrument_id: InstrumentId) -> BitgetWsResult<()> {
        self.trade_subs.remove(&instrument_id);
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
        let topic_symbol = match self.product_type {
            Some(BitgetProductType::Option) => extract_base_coin(raw_symbol),
            _ => raw_symbol,
        };
        let topic = format!(
            "{}.{topic_symbol}",
            BitgetWsPublicChannel::PublicTrade.as_ref()
        );
        self.unsubscribe(vec![topic]).await
    }

    /// Subscribes to public liquidation updates for a specific instrument.
    ///
    /// Bitget publishes this stream for futures only, aggregated to one record per side per second.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/public/Liquidation-Channel>
    pub async fn subscribe_liquidations(&self, instrument_id: InstrumentId) -> BitgetWsResult<()> {
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
        let topic = format!(
            "{}.{raw_symbol}",
            BitgetWsPublicChannel::AllLiquidation.as_ref()
        );
        self.subscribe(vec![topic]).await
    }

    /// Unsubscribes from public liquidation updates for a specific instrument.
    pub async fn unsubscribe_liquidations(
        &self,
        instrument_id: InstrumentId,
    ) -> BitgetWsResult<()> {
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
        let topic = format!(
            "{}.{raw_symbol}",
            BitgetWsPublicChannel::AllLiquidation.as_ref()
        );
        self.unsubscribe(vec![topic]).await
    }

    /// Subscribes to ticker updates for a specific instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/public/Tickers-Channel>
    pub async fn subscribe_ticker(&self, instrument_id: InstrumentId) -> BitgetWsResult<()> {
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
        let topic = format!("{}.{raw_symbol}", BitgetWsPublicChannel::Tickers.as_ref());
        self.subscribe(vec![topic]).await
    }

    /// Unsubscribes from ticker updates for a specific instrument.
    pub async fn unsubscribe_ticker(&self, instrument_id: InstrumentId) -> BitgetWsResult<()> {
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
        let topic = format!("{}.{raw_symbol}", BitgetWsPublicChannel::Tickers.as_ref());
        self.unsubscribe(vec![topic]).await
    }

    /// Subscribes to kline/candlestick updates for a specific instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/public/Candlesticks-Channel>
    pub async fn subscribe_bars(&self, bar_type: BarType) -> BitgetWsResult<()> {
        if self.product_type == Some(BitgetProductType::Option) {
            return Err(BitgetWsError::ClientError(
                "Bitget does not support kline/bar data for options".to_string(),
            ));
        }

        let spec = bar_type.spec();

        if spec.price_type != PriceType::Last {
            return Err(BitgetWsError::ClientError(format!(
                "Invalid bar type: Bitget bars only support LAST price type, received {}",
                spec.price_type
            )));
        }

        if bar_type.aggregation_source() != AggregationSource::External {
            return Err(BitgetWsError::ClientError(format!(
                "Invalid bar type: Bitget bars only support EXTERNAL aggregation source, received {}",
                bar_type.aggregation_source()
            )));
        }

        let interval = bar_spec_to_bitget_interval(spec.aggregation, spec.step.get() as u64)
            .map_err(|e| BitgetWsError::ClientError(e.to_string()))?;

        let instrument_id = bar_type.instrument_id();
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
        let topic = format!(
            "{}.{}.{raw_symbol}",
            BitgetWsPublicChannel::Kline.as_ref(),
            interval
        );

        // Coordinate with reference counting to avoid duplicate cache entries
        if self.subscriptions.get_reference_count(&topic) == 0 {
            self.bar_types_cache.insert(topic.clone(), bar_type);
        }

        self.subscribe(vec![topic]).await
    }

    /// Unsubscribes from kline/candlestick updates for a specific instrument.
    pub async fn unsubscribe_bars(&self, bar_type: BarType) -> BitgetWsResult<()> {
        let spec = bar_type.spec();
        let interval = bar_spec_to_bitget_interval(spec.aggregation, spec.step.get() as u64)
            .map_err(|e| BitgetWsError::ClientError(e.to_string()))?;

        let instrument_id = bar_type.instrument_id();
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
        let topic = format!(
            "{}.{}.{raw_symbol}",
            BitgetWsPublicChannel::Kline.as_ref(),
            interval
        );

        // Coordinate with reference counting to preserve cache for other subscribers
        if self.subscriptions.get_reference_count(&topic) == 1 {
            self.bar_types_cache.remove(&topic);
        }

        self.unsubscribe(vec![topic]).await
    }

    /// Subscribes to order updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails or if not authenticated.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/private/Order-Channel>
    pub async fn subscribe_orders(&self) -> BitgetWsResult<()> {
        if !self.requires_auth {
            return Err(BitgetWsError::Authentication(
                "Order subscription requires authentication".to_string(),
            ));
        }
        // Bitget pushes conditional orders on a separate `strategy-order` channel
        self.subscribe(vec![
            BitgetWsPrivateChannel::Order.as_ref().to_string(),
            BitgetWsPrivateChannel::StrategyOrder.as_ref().to_string(),
        ])
        .await
    }

    /// Unsubscribes from order updates.
    pub async fn unsubscribe_orders(&self) -> BitgetWsResult<()> {
        self.unsubscribe(vec![
            BitgetWsPrivateChannel::Order.as_ref().to_string(),
            BitgetWsPrivateChannel::StrategyOrder.as_ref().to_string(),
        ])
        .await
    }

    /// Subscribes to execution/fill updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails or if not authenticated.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/private/Fill-Channel>
    pub async fn subscribe_executions(&self) -> BitgetWsResult<()> {
        if !self.requires_auth {
            return Err(BitgetWsError::Authentication(
                "Execution subscription requires authentication".to_string(),
            ));
        }
        self.subscribe(vec![BitgetWsPrivateChannel::Execution.as_ref().to_string()])
            .await
    }

    /// Unsubscribes from execution/fill updates.
    pub async fn unsubscribe_executions(&self) -> BitgetWsResult<()> {
        self.unsubscribe(vec![BitgetWsPrivateChannel::Execution.as_ref().to_string()])
            .await
    }

    /// Subscribes to fast execution updates (slim payload, lower latency).
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails or if not authenticated.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/private/Fast-Fill-Channel>
    pub async fn subscribe_executions_fast(&self) -> BitgetWsResult<()> {
        if !self.requires_auth {
            return Err(BitgetWsError::Authentication(
                "Fast execution subscription requires authentication".to_string(),
            ));
        }
        self.subscribe(vec![
            BitgetWsPrivateChannel::ExecutionFast.as_ref().to_string(),
        ])
        .await
    }

    /// Unsubscribes from fast execution updates.
    pub async fn unsubscribe_executions_fast(&self) -> BitgetWsResult<()> {
        self.unsubscribe(vec![
            BitgetWsPrivateChannel::ExecutionFast.as_ref().to_string(),
        ])
        .await
    }

    /// Subscribes to position updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails or if not authenticated.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/private/Positions-Channel>
    pub async fn subscribe_positions(&self) -> BitgetWsResult<()> {
        if !self.requires_auth {
            return Err(BitgetWsError::Authentication(
                "Position subscription requires authentication".to_string(),
            ));
        }
        self.subscribe(vec![BitgetWsPrivateChannel::Position.as_ref().to_string()])
            .await
    }

    /// Unsubscribes from position updates.
    pub async fn unsubscribe_positions(&self) -> BitgetWsResult<()> {
        self.unsubscribe(vec![BitgetWsPrivateChannel::Position.as_ref().to_string()])
            .await
    }

    /// Subscribes to wallet/balance updates.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription request fails or if not authenticated.
    ///
    /// # References
    ///
    /// <https://www.bitget.com/docs/uta/websocket/private/Account-Channel>
    pub async fn subscribe_wallet(&self) -> BitgetWsResult<()> {
        if !self.requires_auth {
            return Err(BitgetWsError::Authentication(
                "Wallet subscription requires authentication".to_string(),
            ));
        }
        self.subscribe(vec![BitgetWsPrivateChannel::Wallet.as_ref().to_string()])
            .await
    }

    /// Unsubscribes from wallet/balance updates.
    pub async fn unsubscribe_wallet(&self) -> BitgetWsResult<()> {
        self.unsubscribe(vec![BitgetWsPrivateChannel::Wallet.as_ref().to_string()])
            .await
    }

    /// Waits for the session to be authenticated, aborting early if the client
    /// enters a terminal state (closed or disconnecting) during the wait.
    async fn require_authenticated(&self) -> BitgetWsResult<()> {
        if self.is_closed() {
            return Err(BitgetWsError::ClientError(
                "WebSocket client is closed".to_string(),
            ));
        }

        if self.auth_tracker.is_authenticated() {
            return Ok(());
        }

        tokio::select! {
            authenticated = self.auth_tracker.wait_for_authenticated(self.auth_wait_timeout) => {
                if authenticated {
                    Ok(())
                } else {
                    Err(BitgetWsError::Authentication(
                        "Must be authenticated".to_string(),
                    ))
                }
            }
            () = async {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    if self.is_closed() {
                        return;
                    }
                }
            } => {
                Err(BitgetWsError::ClientError(
                    "WebSocket client closed during authentication wait".to_string(),
                ))
            }
        }
    }

    /// Allocates correlation IDs for product-specific batch chunks.
    #[must_use]
    pub(crate) fn batch_request_ids(
        category: BitgetProductType,
        order_count: usize,
    ) -> Vec<String> {
        let request_count = order_count.div_ceil(batch_send_limit(category));
        (0..request_count)
            .map(|_| UUID4::new().to_string())
            .collect()
    }

    fn batch_category(
        mut categories: impl Iterator<Item = BitgetProductType>,
    ) -> BitgetWsResult<BitgetProductType> {
        let category = categories.next().ok_or_else(|| {
            BitgetWsError::ClientError("Batch order request cannot be empty".to_string())
        })?;

        if categories.any(|candidate| candidate != category) {
            return Err(BitgetWsError::ClientError(
                "Batch order request cannot mix product categories".to_string(),
            ));
        }
        Ok(category)
    }

    /// Places an order via WebSocket, returning the request ID for correlation.
    ///
    /// # Errors
    ///
    /// Returns an error if the order request fails or if not authenticated.
    pub async fn place_order(&self, params: BitgetWsPlaceOrderParams) -> BitgetWsResult<String> {
        let req_id = UUID4::new().to_string();
        self.place_order_with_id(params, req_id.clone()).await?;
        Ok(req_id)
    }

    pub(crate) async fn place_order_with_id(
        &self,
        params: BitgetWsPlaceOrderParams,
        req_id: String,
    ) -> BitgetWsResult<()> {
        self.require_authenticated().await?;
        let category = params.category;

        // Bitget has no Nautilus `apiCode` rebate identifier, so no referer is sent
        let command = BitgetWsOrderCommand {
            req_id,
            op: BitgetWsOrderRequestOp::Create,
            category,
            weight: 1,
            referer: None,
            args: vec![serde_json::to_value(params)?],
        };
        self.send_cmd(HandlerCommand::SendOrder { command }).await
    }

    /// Amends an existing order via WebSocket, returning the request ID for correlation.
    ///
    /// # Errors
    ///
    /// Returns an error if the amend request fails or if not authenticated.
    pub async fn amend_order(&self, params: BitgetWsAmendOrderParams) -> BitgetWsResult<String> {
        let req_id = UUID4::new().to_string();
        self.amend_order_with_id(params, req_id.clone()).await?;
        Ok(req_id)
    }

    pub(crate) async fn amend_order_with_id(
        &self,
        params: BitgetWsAmendOrderParams,
        req_id: String,
    ) -> BitgetWsResult<()> {
        self.require_authenticated().await?;
        let command = BitgetWsOrderCommand {
            category: params.category,
            req_id,
            op: BitgetWsOrderRequestOp::Amend,
            weight: 1,
            referer: None,
            args: vec![serde_json::to_value(params)?],
        };
        self.send_cmd(HandlerCommand::SendOrder { command }).await
    }

    /// Cancels an order via WebSocket, returning the request ID for correlation.
    ///
    /// # Errors
    ///
    /// Returns an error if the cancel request fails or if not authenticated.
    pub async fn cancel_order(&self, params: BitgetWsCancelOrderParams) -> BitgetWsResult<String> {
        let req_id = UUID4::new().to_string();
        self.cancel_order_with_id(params, req_id.clone()).await?;
        Ok(req_id)
    }

    pub(crate) async fn cancel_order_with_id(
        &self,
        params: BitgetWsCancelOrderParams,
        req_id: String,
    ) -> BitgetWsResult<()> {
        self.require_authenticated().await?;
        let command = BitgetWsOrderCommand {
            category: params.category,
            req_id,
            op: BitgetWsOrderRequestOp::Cancel,
            weight: 1,
            referer: None,
            args: vec![serde_json::to_value(params)?],
        };
        self.send_cmd(HandlerCommand::SendOrder { command }).await
    }

    /// Batch creates multiple orders via WebSocket, returning the request ID for correlation.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch request fails or if not authenticated.
    pub async fn batch_place_orders(
        &self,
        orders: Vec<BitgetWsPlaceOrderParams>,
    ) -> BitgetWsResult<Vec<String>> {
        self.require_authenticated().await?;

        if orders.is_empty() {
            log::warn!("Batch place orders called with empty orders list");
            return Ok(vec![]);
        }

        let category = Self::batch_category(orders.iter().map(|order| order.category))?;
        let req_ids = Self::batch_request_ids(category, orders.len());
        self.batch_place_orders_with_ids(orders, req_ids.clone())
            .await?;
        Ok(req_ids)
    }

    pub(crate) async fn batch_place_orders_with_ids(
        &self,
        orders: Vec<BitgetWsPlaceOrderParams>,
        req_ids: Vec<String>,
    ) -> BitgetWsResult<()> {
        self.require_authenticated().await?;
        let category = Self::batch_category(orders.iter().map(|order| order.category))?;
        let chunk_limit = batch_send_limit(category);
        if req_ids.len() != orders.len().div_ceil(chunk_limit) {
            return Err(BitgetWsError::ClientError(
                "Batch request ID count does not match order chunks".to_string(),
            ));
        }

        let mut commands = Vec::with_capacity(req_ids.len());
        for (orders, req_id) in orders.chunks(chunk_limit).zip(req_ids) {
            commands.push(self.build_batch_place_command(orders.to_vec(), req_id)?);
        }
        self.send_cmd(HandlerCommand::SendOrders { commands }).await
    }

    fn build_batch_place_command(
        &self,
        orders: Vec<BitgetWsPlaceOrderParams>,
        req_id: String,
    ) -> BitgetWsResult<BitgetWsOrderCommand> {
        let category = orders[0].category;
        let order_count = orders.len();

        // Bitget `batch-place` rejects reduce-only orders
        if orders.iter().any(|order| order.reduce_only == Some(true)) {
            return Err(BitgetWsError::ClientError(
                "Bitget batch-place does not support reduce-only orders".to_string(),
            ));
        }

        // Bitget `batch-place` has no `bboLevel`, so a BBO order would go out without a price
        if orders.iter().any(|order| order.bbo_level.is_some()) {
            return Err(BitgetWsError::ClientError(
                "Bitget batch-place does not support BBO orders".to_string(),
            ));
        }

        let request_items: Vec<BitgetWsBatchPlaceItem> = orders
            .into_iter()
            .map(|order| BitgetWsBatchPlaceItem {
                symbol: order.symbol,
                side: order.side,
                order_type: order.order_type,
                qty: order.qty,
                price: order.price,
                time_in_force: order.time_in_force,
                order_link_id: order.order_link_id,
                take_profit: order.take_profit,
                stop_loss: order.stop_loss,
                tp_trigger_by: order.tp_trigger_by,
                sl_trigger_by: order.sl_trigger_by,
                sl_order_type: order.sl_order_type,
                tp_order_type: order.tp_order_type,
                sl_limit_price: order.sl_limit_price,
                tp_limit_price: order.tp_limit_price,
                smp_type: order.smp_type,
                position_idx: order.position_idx,
            })
            .collect();

        let args = BitgetWsBatchPlaceOrderArgs {
            category,
            request: request_items,
        };

        // Bitget batch args are the items themselves; the category goes on the envelope
        Ok(BitgetWsOrderCommand {
            req_id,
            op: BitgetWsOrderRequestOp::CreateBatch,
            category: args.category,
            weight: batch_weight(category, order_count),
            referer: None,
            args: args
                .request
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<_, _>>()?,
        })
    }

    /// Batch amends multiple orders via WebSocket.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch request fails or if not authenticated.
    pub async fn batch_amend_orders(
        &self,
        orders: Vec<BitgetWsAmendOrderParams>,
    ) -> BitgetWsResult<Vec<String>> {
        self.require_authenticated().await?;

        if orders.is_empty() {
            log::warn!("Batch amend orders called with empty orders list");
            return Ok(vec![]);
        }

        let category = Self::batch_category(orders.iter().map(|order| order.category))?;
        let req_ids = Self::batch_request_ids(category, orders.len());
        self.batch_amend_orders_with_ids(orders, req_ids.clone())
            .await?;
        Ok(req_ids)
    }

    pub(crate) async fn batch_amend_orders_with_ids(
        &self,
        orders: Vec<BitgetWsAmendOrderParams>,
        req_ids: Vec<String>,
    ) -> BitgetWsResult<()> {
        self.require_authenticated().await?;
        let category = Self::batch_category(orders.iter().map(|order| order.category))?;
        let chunk_limit = batch_send_limit(category);
        if req_ids.len() != orders.len().div_ceil(chunk_limit) {
            return Err(BitgetWsError::ClientError(
                "Batch request ID count does not match order chunks".to_string(),
            ));
        }

        let mut commands = Vec::with_capacity(req_ids.len());
        for (orders, req_id) in orders.chunks(chunk_limit).zip(req_ids) {
            commands.push(Self::build_batch_amend_command(orders.to_vec(), req_id)?);
        }
        self.send_cmd(HandlerCommand::SendOrders { commands }).await
    }

    fn build_batch_amend_command(
        orders: Vec<BitgetWsAmendOrderParams>,
        req_id: String,
    ) -> BitgetWsResult<BitgetWsOrderCommand> {
        let category = orders[0].category;
        let order_count = orders.len();

        let request_items = orders
            .into_iter()
            .map(|order| BitgetWsBatchAmendItem {
                symbol: order.symbol,
                order_id: order.order_id,
                order_link_id: order.order_link_id,
                qty: order.qty,
                price: order.price,
            })
            .collect();

        let args = BitgetWsBatchAmendOrderArgs {
            category,
            request: request_items,
        };

        Ok(BitgetWsOrderCommand {
            req_id,
            op: BitgetWsOrderRequestOp::AmendBatch,
            category: args.category,
            weight: batch_weight(category, order_count),
            referer: None,
            args: args
                .request
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<_, _>>()?,
        })
    }

    /// Batch cancels multiple orders via WebSocket, returning the request ID for correlation.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch request fails or if not authenticated.
    pub async fn batch_cancel_orders(
        &self,
        orders: Vec<BitgetWsCancelOrderParams>,
    ) -> BitgetWsResult<Vec<String>> {
        self.require_authenticated().await?;

        if orders.is_empty() {
            log::warn!("Batch cancel orders called with empty orders list");
            return Ok(vec![]);
        }

        let category = Self::batch_category(orders.iter().map(|order| order.category))?;
        let req_ids = Self::batch_request_ids(category, orders.len());
        self.batch_cancel_orders_with_ids(orders, req_ids.clone())
            .await?;
        Ok(req_ids)
    }

    pub(crate) async fn batch_cancel_orders_with_ids(
        &self,
        orders: Vec<BitgetWsCancelOrderParams>,
        req_ids: Vec<String>,
    ) -> BitgetWsResult<()> {
        self.require_authenticated().await?;

        if orders.is_empty() {
            return Ok(());
        }

        let category = Self::batch_category(orders.iter().map(|order| order.category))?;
        let chunk_limit = batch_send_limit(category);
        if req_ids.len() != orders.len().div_ceil(chunk_limit) {
            return Err(BitgetWsError::ClientError(
                "Batch request ID count does not match order chunks".to_string(),
            ));
        }

        let mut commands = Vec::with_capacity(req_ids.len());
        for (orders, req_id) in orders.chunks(chunk_limit).zip(req_ids) {
            commands.push(Self::build_batch_cancel_command(orders.to_vec(), req_id)?);
        }
        self.send_cmd(HandlerCommand::SendOrders { commands }).await
    }

    fn build_batch_cancel_command(
        orders: Vec<BitgetWsCancelOrderParams>,
        req_id: String,
    ) -> BitgetWsResult<BitgetWsOrderCommand> {
        let category = orders[0].category;
        let order_count = orders.len();

        let request_items: Vec<BitgetWsBatchCancelItem> = orders
            .into_iter()
            .map(|order| BitgetWsBatchCancelItem {
                symbol: order.symbol,
                order_id: order.order_id,
                order_link_id: order.order_link_id,
            })
            .collect();

        let args = BitgetWsBatchCancelOrderArgs {
            category,
            request: request_items,
        };

        Ok(BitgetWsOrderCommand {
            req_id,
            op: BitgetWsOrderRequestOp::CancelBatch,
            category: args.category,
            weight: batch_weight(category, order_count),
            referer: None,
            args: args
                .request
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<_, _>>()?,
        })
    }

    /// Submits an order using Nautilus domain objects.
    ///
    /// # Errors
    ///
    /// Returns an error if order submission fails or if not authenticated.
    #[expect(clippy::too_many_arguments)]
    pub async fn submit_order(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        order_side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        is_quote_quantity: bool,
        time_in_force: Option<TimeInForce>,
        price: Option<Price>,
        trigger_price: Option<Price>,
        trigger_type: Option<TriggerType>,
        post_only: Option<bool>,
        reduce_only: Option<bool>,
        is_leverage: bool,
        position_idx: Option<BitgetPositionIdx>,
        bbo_side_type: Option<BitgetBboSideType>,
        bbo_level: Option<String>,
        smp_type: Option<BitgetOrderSmpType>,
    ) -> BitgetWsResult<String> {
        let params = self.build_place_order_params(
            product_type,
            instrument_id,
            client_order_id,
            order_side,
            order_type,
            quantity,
            is_quote_quantity,
            time_in_force,
            price,
            trigger_price,
            trigger_type,
            post_only,
            reduce_only,
            is_leverage,
            None,
            None,
            position_idx,
            bbo_side_type,
            bbo_level,
            smp_type,
        )?;

        self.place_order(params).await
    }

    /// Modifies an existing order using Nautilus domain objects.
    ///
    /// # Errors
    ///
    /// Returns an error if modification fails or if not authenticated.
    pub async fn modify_order(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
        quantity: Option<Quantity>,
        price: Option<Price>,
    ) -> BitgetWsResult<String> {
        let params = self.build_amend_order_params(
            product_type,
            instrument_id,
            venue_order_id,
            Some(client_order_id),
            quantity,
            price,
        )?;

        self.amend_order(params).await
    }

    /// Cancels an order using Nautilus domain objects.
    ///
    /// # Errors
    ///
    /// Returns an error if cancellation fails or if not authenticated.
    pub async fn cancel_order_by_id(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
    ) -> BitgetWsResult<String> {
        let params = self.build_cancel_order_params(
            product_type,
            instrument_id,
            venue_order_id,
            Some(client_order_id),
        )?;

        self.cancel_order(params).await
    }

    /// Builds order params for placing an order.
    #[expect(clippy::too_many_arguments)]
    pub fn build_place_order_params(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        order_side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        is_quote_quantity: bool,
        time_in_force: Option<TimeInForce>,
        price: Option<Price>,
        trigger_price: Option<Price>,
        trigger_type: Option<TriggerType>,
        post_only: Option<bool>,
        reduce_only: Option<bool>,
        is_leverage: bool,
        take_profit: Option<Price>,
        stop_loss: Option<Price>,
        position_idx: Option<BitgetPositionIdx>,
        bbo_side_type: Option<BitgetBboSideType>,
        bbo_level: Option<String>,
        smp_type: Option<BitgetOrderSmpType>,
    ) -> BitgetWsResult<BitgetWsPlaceOrderParams> {
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())
            .map_err(|e| BitgetWsError::ClientError(e.to_string()))?;
        let raw_symbol = Ustr::from(bitget_symbol.raw_symbol());

        let bitget_side = match order_side {
            OrderSide::Buy => BitgetOrderSide::Buy,
            OrderSide::Sell => BitgetOrderSide::Sell,
        };

        // Bitget `op:"trade"` places plain orders only; conditional orders use the REST
        // strategy-order endpoint
        let bitget_order_type = match order_type {
            OrderType::Market => BitgetOrderType::Market,
            OrderType::Limit => BitgetOrderType::Limit,
            OrderType::StopMarket
            | OrderType::MarketIfTouched
            | OrderType::StopLimit
            | OrderType::LimitIfTouched => {
                return Err(BitgetWsError::ClientError(format!(
                    "Bitget WebSocket trade does not support {order_type:?} orders, \
                     use the REST strategy-order endpoint"
                )));
            }
            _ => {
                return Err(BitgetWsError::ClientError(format!(
                    "Unsupported order type: {order_type:?}"
                )));
            }
        };

        if trigger_price.is_some() {
            return Err(BitgetWsError::ClientError(
                "Bitget WebSocket trade does not support trigger prices".to_string(),
            ));
        }

        // Bitget spot market buys are always sized in the quote coin
        if product_type == BitgetProductType::Spot
            && bitget_order_type == BitgetOrderType::Market
            && order_side == OrderSide::Buy
            && !is_quote_quantity
        {
            return Err(BitgetWsError::ClientError(
                "Bitget spot market buy orders require a quote quantity".to_string(),
            ));
        }

        let bitget_tif =
            map_time_in_force(bitget_order_type, time_in_force, post_only).map_err(|tif| {
                BitgetWsError::ClientError(format!("Unsupported time in force: {tif:?}"))
            })?;
        let is_leverage_value = spot_leverage(product_type, is_leverage);

        // Bitget combines the BBO side and level into one `bboLevel` value (`queue1`)
        let bbo_level = match (bbo_side_type, bbo_level) {
            (Some(BitgetBboSideType::Queue), Some(level)) => Some(format!("queue{level}")),
            (Some(BitgetBboSideType::Counterparty), Some(level)) => {
                Some(format!("counterparty{level}"))
            }
            _ => None,
        };

        let params = BitgetWsPlaceOrderParams {
            category: product_type,
            symbol: raw_symbol,
            side: bitget_side,
            order_type: bitget_order_type,
            qty: quantity.to_string(),
            is_leverage: is_leverage_value,
            price: if bbo_level.is_some() {
                None
            } else {
                price.map(|p| p.to_string())
            },
            time_in_force: bitget_tif,
            order_link_id: Some(client_order_id.to_string()),
            reduce_only: reduce_only.filter(|&r| r),
            // Verified account setup is crossed margin; spot takes no margin mode
            margin_mode: (!product_type.is_spot()).then(|| "crossed".to_string()),
            take_profit: take_profit.map(|p| p.to_string()),
            stop_loss: stop_loss.map(|p| p.to_string()),
            tp_trigger_by: take_profit.map(|_| resolve_trigger_type(trigger_type)),
            sl_trigger_by: stop_loss.map(|_| resolve_trigger_type(trigger_type)),
            sl_order_type: None,
            tp_order_type: None,
            sl_limit_price: None,
            tp_limit_price: None,
            smp_type,
            position_idx,
            bbo_level,
        };

        Ok(params)
    }

    /// Builds order params for amending an order.
    pub fn build_amend_order_params(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        venue_order_id: Option<VenueOrderId>,
        client_order_id: Option<ClientOrderId>,
        quantity: Option<Quantity>,
        price: Option<Price>,
    ) -> BitgetWsResult<BitgetWsAmendOrderParams> {
        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())
            .map_err(|e| BitgetWsError::ClientError(e.to_string()))?;
        let raw_symbol = Ustr::from(bitget_symbol.raw_symbol());

        Ok(BitgetWsAmendOrderParams {
            category: product_type,
            symbol: raw_symbol,
            order_id: venue_order_id.map(|v| v.to_string()),
            order_link_id: client_order_id.map(|c| c.to_string()),
            qty: quantity.map(|q| q.to_string()),
            price: price.map(|p| p.to_string()),
        })
    }

    /// Builds order params for canceling an order via WebSocket.
    ///
    /// # Errors
    ///
    /// Returns an error if symbol parsing fails or if neither venue_order_id
    /// nor client_order_id is provided.
    pub fn build_cancel_order_params(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        venue_order_id: Option<VenueOrderId>,
        client_order_id: Option<ClientOrderId>,
    ) -> BitgetWsResult<BitgetWsCancelOrderParams> {
        if venue_order_id.is_none() && client_order_id.is_none() {
            return Err(BitgetWsError::ClientError(
                "Either venue_order_id or client_order_id must be provided".to_string(),
            ));
        }

        let bitget_symbol = BitgetSymbol::new(instrument_id.symbol.as_str())
            .map_err(|e| BitgetWsError::ClientError(e.to_string()))?;
        let raw_symbol = Ustr::from(bitget_symbol.raw_symbol());

        Ok(BitgetWsCancelOrderParams {
            category: product_type,
            symbol: raw_symbol,
            order_id: venue_order_id.map(|v| v.to_string()),
            order_link_id: client_order_id.map(|c| c.to_string()),
        })
    }

    fn default_headers() -> Vec<(String, String)> {
        let mut headers = create_standard_nautilus_headers();
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
        headers
    }

    async fn authenticate_if_required(&self) -> BitgetWsResult<()> {
        if !self.requires_auth {
            return Ok(());
        }

        let credential = self.credential.as_ref().ok_or_else(|| {
            BitgetWsError::Authentication("Credentials required for authentication".to_string())
        })?;

        let auth_message = Zeroizing::new(login_request(credential));

        let payload = SecretString::from(serde_json::to_string(&*auth_message)?);
        drop(auth_message);

        // Begin auth attempt so succeed() will update state
        let _rx = self.auth_tracker.begin();

        self.cmd_tx
            .read()
            .await
            .send(HandlerCommand::Authenticate { payload })
            .map_err(|e| BitgetWsError::Send(format!("Failed to send auth command: {e}")))?;

        Ok(())
    }

    async fn send_cmd(&self, cmd: HandlerCommand) -> BitgetWsResult<()> {
        self.cmd_tx
            .read()
            .await
            .send(cmd)
            .map_err(|e| BitgetWsError::Send(e.to_string()))
    }
}

impl Drop for BitgetWebSocketClient {
    fn drop(&mut self) {
        if Arc::strong_count(&self.task_handle) == 1 && !self.task_handle.is_empty() {
            self.cancellation_token.load().cancel();
            self.signal.store(true, Ordering::Relaxed);
            self.task_handle.abort();
        }
    }
}

// Builds the Bitget login frame: sign = Base64(HMAC_SHA256(secret, ts + "GET" + "/user/verify"))
fn login_request(credential: &Credential) -> BitgetAuthRequest {
    let timestamp = jiff::Timestamp::now().as_millisecond();
    let signature = credential.sign_websocket_auth(timestamp);

    BitgetAuthRequest {
        op: BitgetWsOperation::Auth,
        args: vec![json!({
            "apiKey": credential.api_key(),
            "passphrase": credential.api_passphrase(),
            "timestamp": timestamp.to_string(),
            "sign": signature,
        })],
    }
}

// Bitget depth channels: `books1`, `books5`, `books50` (snapshots) or full-depth `books`
fn orderbook_topic(instrument_id: InstrumentId, depth: u32) -> String {
    let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str());
    let channel = BitgetWsPublicChannel::OrderBook.as_ref();
    match depth {
        1 | 5 | 50 => format!("{channel}{depth}.{raw_symbol}"),
        _ => format!("{channel}.{raw_symbol}"),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::Value;

    use super::*;
    use crate::{
        common::{
            rate_limit::{BITGET_OPTION_SUBSCRIPTION_LIMIT, batch_send_limit, batch_weight},
            testing::load_test_json,
        },
        websocket::{messages::BitgetWsFrame, parse_bitget_ws_frame},
    };

    #[rstest]
    #[case("wss://stream.example.com/feed", "wss://stream.example.com")]
    #[case(
        "wss://SYN-user:SYN-password@stream.example.com:9443/SYN-path?token=SYN-query#SYN-fragment",
        "wss://stream.example.com:9443"
    )]
    fn test_debug_preserves_url_origin(#[case] url: &str, #[case] origin: &str) {
        let client = BitgetWebSocketClient::new_public(Some(url.to_string()), 30);
        let debug = format!("{client:?}");
        assert!(debug.contains(&format!("url: {origin:?}")));
        assert!(!debug.contains("SYN-"));
        assert_eq!(client.url.expose(), url);
    }

    #[tokio::test]
    async fn test_drop_clone_does_not_cancel_handler() {
        let client = BitgetWebSocketClient::new_public(Some("wss://test".to_string()), 30);
        let cancellation_token = CancellationToken::new();
        client
            .cancellation_token
            .store(Arc::new(cancellation_token.clone()));
        client
            .task_handle
            .insert(get_runtime().spawn(std::future::pending()));
        let clone = client.clone();

        drop(clone);

        assert!(!cancellation_token.is_cancelled());
        assert!(!client.task_handle.is_empty());
    }

    // An unconnected client rejects every command, so success proves nothing was sent
    #[tokio::test]
    async fn test_subscribe_book_opens_gate_for_held_topic_without_sending() {
        let client = BitgetWebSocketClient::new_public(None, 20);
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
        let topic = "books1.BTCUSDT";
        assert!(client.subscriptions.add_reference(topic));
        let gate = SnapshotGate::default();
        gate.lock().close();

        let result = client
            .subscribe_book(instrument_id, 1, CancellationToken::new(), gate.clone())
            .await;

        assert!(result.is_ok());
        assert!(!gate.lock().is_closed());
        assert_eq!(client.subscriptions.get_reference_count(topic), 2);
    }

    #[tokio::test]
    async fn test_subscribe_book_keeps_gate_closed_when_send_fails() {
        let client = BitgetWebSocketClient::new_public(None, 20);
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
        let gate = SnapshotGate::default();
        gate.lock().close();

        let error = client
            .subscribe_book(instrument_id, 50, CancellationToken::new(), gate.clone())
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "WebSocket send error: Failed to send subscribe command: channel closed"
        );
        assert!(gate.lock().is_closed());
        assert_eq!(
            client.subscriptions.get_reference_count("books50.BTCUSDT"),
            1
        );
    }

    #[rstest]
    fn classify_orderbook_snapshot() {
        let json: Value = serde_json::from_str(&load_test_json("ws_orderbook_snapshot.json"))
            .expect("invalid fixture");
        let frame = parse_bitget_ws_frame(json);
        assert!(matches!(frame, BitgetWsFrame::Orderbook(_)));
    }

    #[rstest]
    fn classify_trade_snapshot() {
        let json: Value =
            serde_json::from_str(&load_test_json("ws_public_trade.json")).expect("invalid fixture");
        let frame = parse_bitget_ws_frame(json);
        assert!(matches!(frame, BitgetWsFrame::Trade(_)));
    }

    #[rstest]
    fn classify_ticker_linear_snapshot() {
        let json: Value = serde_json::from_str(&load_test_json("ws_ticker_linear.json"))
            .expect("invalid fixture");
        let frame = parse_bitget_ws_frame(json);
        assert!(matches!(frame, BitgetWsFrame::TickerLinear(_)));
    }

    #[rstest]
    fn classify_orderbook_books1_snapshot() {
        let json: Value = serde_json::from_str(&load_test_json("ws_orderbook_books1.json"))
            .expect("invalid fixture");
        let frame = parse_bitget_ws_frame(json);
        assert!(matches!(frame, BitgetWsFrame::Orderbook(_)));
    }

    #[rstest]
    #[case(1, "books1.BTCUSDT")]
    #[case(5, "books5.BTCUSDT")]
    #[case(50, "books50.BTCUSDT")]
    #[case(1000, "books.BTCUSDT")]
    fn orderbook_topic_maps_depth_to_bitget_channel(#[case] depth: u32, #[case] expected: &str) {
        let topic = orderbook_topic(InstrumentId::from("BTCUSDT-PERP.BITGET"), depth);
        assert_eq!(topic, expected);
    }

    #[rstest]
    fn login_request_signs_verify_path() {
        let credential = Credential::new("test-key", "test-secret", "test-passphrase");
        let request = login_request(&credential);
        let json = serde_json::to_value(&request).unwrap();
        let args = &json["args"][0];
        let timestamp: i64 = args["timestamp"].as_str().unwrap().parse().unwrap();

        assert_eq!(json["op"], "login");
        assert_eq!(args["apiKey"], "test-key");
        assert_eq!(args["passphrase"], "test-passphrase");
        assert_eq!(args["sign"], credential.sign_websocket_auth(timestamp));
    }

    #[rstest]
    fn test_race_unsubscribe_failure_recovery() {
        let subscriptions = SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER);
        let topic = "publicTrade.BTCUSDT";

        subscriptions.mark_subscribe(topic);
        subscriptions.confirm_subscribe(topic);
        assert_eq!(subscriptions.len(), 1);

        subscriptions.mark_unsubscribe(topic);
        assert_eq!(subscriptions.len(), 0);
        assert_eq!(subscriptions.pending_unsubscribe_topics(), vec![topic]);

        subscriptions.confirm_unsubscribe(topic);
        subscriptions.mark_subscribe(topic);
        subscriptions.confirm_subscribe(topic);

        assert_eq!(subscriptions.len(), 1);
        assert!(subscriptions.pending_unsubscribe_topics().is_empty());
        assert!(subscriptions.pending_subscribe_topics().is_empty());

        let all = subscriptions.all_topics();
        assert_eq!(all.len(), 1);
        assert!(all.contains(&topic.to_string()));
    }

    #[rstest]
    fn test_race_resubscribe_before_unsubscribe_ack() {
        let subscriptions = SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER);
        let topic = "books50.BTCUSDT";

        subscriptions.mark_subscribe(topic);
        subscriptions.confirm_subscribe(topic);
        assert_eq!(subscriptions.len(), 1);

        subscriptions.mark_unsubscribe(topic);
        assert_eq!(subscriptions.len(), 0);
        assert_eq!(subscriptions.pending_unsubscribe_topics(), vec![topic]);

        subscriptions.mark_subscribe(topic);
        assert_eq!(subscriptions.pending_subscribe_topics(), vec![topic]);

        subscriptions.confirm_unsubscribe(topic);
        assert!(subscriptions.pending_unsubscribe_topics().is_empty());
        assert_eq!(subscriptions.pending_subscribe_topics(), vec![topic]);

        subscriptions.confirm_subscribe(topic);
        assert_eq!(subscriptions.len(), 1);
        assert!(subscriptions.pending_subscribe_topics().is_empty());

        let all = subscriptions.all_topics();
        assert_eq!(all.len(), 1);
        assert!(all.contains(&topic.to_string()));
    }

    #[rstest]
    fn test_race_late_subscribe_confirmation_after_unsubscribe() {
        let subscriptions = SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER);
        let topic = "ticker.ETHUSDT";

        subscriptions.mark_subscribe(topic);
        assert_eq!(subscriptions.pending_subscribe_topics(), vec![topic]);

        subscriptions.mark_unsubscribe(topic);
        assert!(subscriptions.pending_subscribe_topics().is_empty());
        assert_eq!(subscriptions.pending_unsubscribe_topics(), vec![topic]);

        subscriptions.confirm_subscribe(topic);
        assert_eq!(subscriptions.len(), 0);
        assert_eq!(subscriptions.pending_unsubscribe_topics(), vec![topic]);

        subscriptions.confirm_unsubscribe(topic);

        assert!(subscriptions.is_empty());
        assert!(subscriptions.all_topics().is_empty());
    }

    #[rstest]
    fn test_race_reconnection_with_pending_states() {
        let subscriptions = SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER);

        let trade_btc = "publicTrade.BTCUSDT";
        subscriptions.mark_subscribe(trade_btc);
        subscriptions.confirm_subscribe(trade_btc);

        let trade_eth = "publicTrade.ETHUSDT";
        subscriptions.mark_subscribe(trade_eth);

        let book_btc = "books50.BTCUSDT";
        subscriptions.mark_subscribe(book_btc);
        subscriptions.confirm_subscribe(book_btc);
        subscriptions.mark_unsubscribe(book_btc);

        let topics_to_restore = subscriptions.all_topics();

        assert_eq!(topics_to_restore.len(), 2);
        assert!(topics_to_restore.contains(&trade_btc.to_string()));
        assert!(topics_to_restore.contains(&trade_eth.to_string()));
        assert!(!topics_to_restore.contains(&book_btc.to_string()));
    }

    #[tokio::test]
    async fn option_limit_counts_pending_unsubscriptions() {
        let client = BitgetWebSocketClient::new_public_with(
            BitgetProductType::Option,
            BitgetEnvironment::Mainnet,
            Some("ws://option-pending-limit.invalid/v3/ws/public".to_string()),
            20,
            TransportBackend::default(),
            None,
        );

        for index in 0..BITGET_OPTION_SUBSCRIPTION_LIMIT {
            let topic = format!("ticker.OPTION-{index}");
            assert!(client.subscriptions.add_reference(&topic));
            client.subscriptions.mark_subscribe(&topic);
            client.subscriptions.confirm_subscribe(&topic);
        }
        let pending = "ticker.OPTION-0";
        assert!(client.subscriptions.remove_reference(pending));
        client.subscriptions.mark_unsubscribe(pending);

        let new_topic = "ticker.OPTION-new";
        let error = client
            .subscribe(vec![new_topic.to_string()])
            .await
            .unwrap_err();

        assert!(error.to_string().contains("2000 arguments"));
        assert_eq!(client.subscriptions.get_reference_count(new_topic), 0);
        assert_eq!(
            client.subscriptions.pending_unsubscribe_topics(),
            vec![pending]
        );
    }

    #[tokio::test]
    async fn batch_chunks_enter_handler_atomically() {
        let client = BitgetWebSocketClient::new_trade(
            BitgetEnvironment::Testnet,
            Some("test-key".to_string()),
            Some("test-secret".to_string()),
            Some("test-passphrase".to_string()),
            None,
            20,
            TransportBackend::default(),
            None,
        );
        client
            .connection_mode
            .load()
            .store(ConnectionMode::Active.as_u8(), Ordering::Release);
        client.auth_tracker.succeed();
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        *client.cmd_tx.write().await = cmd_tx;
        let orders = (0..21)
            .map(|index| BitgetWsCancelOrderParams {
                category: BitgetProductType::Linear,
                symbol: Ustr::from("BTCUSDT"),
                order_id: Some(format!("order-{index}")),
                order_link_id: Some(format!("client-order-{index}")),
            })
            .collect::<Vec<_>>();
        let req_ids =
            BitgetWebSocketClient::batch_request_ids(BitgetProductType::Linear, orders.len());

        client
            .batch_cancel_orders_with_ids(orders, req_ids.clone())
            .await
            .unwrap();

        let command = cmd_rx.recv().await.expect("expected batch command");
        let HandlerCommand::SendOrders { commands } = command else {
            panic!("expected atomic batch command, was {command:?}");
        };
        let limit = batch_send_limit(BitgetProductType::Linear);
        assert_eq!(commands.len(), 21_usize.div_ceil(limit));
        assert_eq!(
            commands
                .iter()
                .map(|command| command.req_id.as_str())
                .collect::<Vec<_>>(),
            req_ids.iter().map(String::as_str).collect::<Vec<_>>()
        );
        assert_eq!(
            commands
                .iter()
                .map(|command| command.weight)
                .collect::<Vec<_>>(),
            (0..21)
                .collect::<Vec<_>>()
                .chunks(limit)
                .map(|chunk| batch_weight(BitgetProductType::Linear, chunk.len()))
                .collect::<Vec<_>>()
        );
        // Bitget batch args are the order items themselves
        assert_eq!(commands[0].args.len(), limit.min(21));
        assert_eq!(commands[0].args[0]["clientOid"], "client-order-0");
        assert!(cmd_rx.try_recv().is_err());
    }

    fn batch_nested_items(command: &BitgetWsOrderCommand) -> &[Value] {
        &command.args
    }

    #[rstest]
    fn batch_place_command_carries_smp_type_per_order() {
        let client = BitgetWebSocketClient::new_trade(
            BitgetEnvironment::Testnet,
            Some("test-key".to_string()),
            Some("test-secret".to_string()),
            Some("test-passphrase".to_string()),
            None,
            20,
            TransportBackend::default(),
            None,
        );

        let expectations = [
            (Some(BitgetOrderSmpType::CancelMaker), Some("cancel_maker")),
            (Some(BitgetOrderSmpType::CancelBoth), Some("cancel_both")),
            (None, None),
        ];

        let orders = expectations
            .into_iter()
            .enumerate()
            .map(|(index, (smp_type, _))| {
                client
                    .build_place_order_params(
                        BitgetProductType::Linear,
                        InstrumentId::from("ETHUSDT-PERP.BITGET"),
                        ClientOrderId::from(format!("smp-batch-{index}").as_str()),
                        OrderSide::Buy,
                        OrderType::Limit,
                        Quantity::from("1.0"),
                        false,
                        Some(TimeInForce::Gtc),
                        Some(Price::from("50000.0")),
                        None,
                        None,
                        None,
                        None,
                        false,
                        None,
                        None,
                        None,
                        None,
                        None,
                        smp_type,
                    )
                    .expect("failed to build params")
            })
            .collect::<Vec<_>>();

        let command = client
            .build_batch_place_command(orders, "req-smp-batch".to_string())
            .expect("failed to build batch command");

        let items = batch_nested_items(&command);

        assert_eq!(items.len(), expectations.len());

        for (item, (_, expected)) in items.iter().zip(expectations) {
            assert_eq!(item.get("stpMode").and_then(Value::as_str), expected);
        }
    }

    #[rstest]
    fn test_race_duplicate_subscribe_messages_idempotent() {
        let subscriptions = SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER);
        let topic = "publicTrade.BTCUSDT";

        subscriptions.mark_subscribe(topic);
        subscriptions.confirm_subscribe(topic);
        assert_eq!(subscriptions.len(), 1);

        subscriptions.mark_subscribe(topic);
        assert!(subscriptions.pending_subscribe_topics().is_empty());
        assert_eq!(subscriptions.len(), 1);

        subscriptions.confirm_subscribe(topic);
        assert_eq!(subscriptions.len(), 1);

        let all = subscriptions.all_topics();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0], topic);
    }

    #[rstest]
    #[case::spot_with_leverage(BitgetProductType::Spot, true, Some(1))]
    #[case::spot_without_leverage(BitgetProductType::Spot, false, Some(0))]
    #[case::linear_with_leverage(BitgetProductType::Linear, true, None)]
    #[case::linear_without_leverage(BitgetProductType::Linear, false, None)]
    #[case::inverse_with_leverage(BitgetProductType::Inverse, true, None)]
    #[case::option_with_leverage(BitgetProductType::Option, true, None)]
    fn test_is_leverage_parameter(
        #[case] product_type: BitgetProductType,
        #[case] is_leverage: bool,
        #[case] expected: Option<i32>,
    ) {
        let symbol = match product_type {
            BitgetProductType::Spot => "BTCUSDT-SPOT.BITGET",
            BitgetProductType::Linear => "ETHUSDT-PERP.BITGET",
            BitgetProductType::Inverse => "BTCUSD_CM-INVERSE.BITGET",
            BitgetProductType::Option => "BTC-31MAY24-50000-C-OPTION.BITGET",
        };

        let instrument_id = InstrumentId::from(symbol);
        let client_order_id = ClientOrderId::from("test-order-1");
        let quantity = Quantity::from("1.0");

        let client = BitgetWebSocketClient::new_trade(
            BitgetEnvironment::Testnet,
            Some("test-key".to_string()),
            Some("test-secret".to_string()),
            Some("test-passphrase".to_string()),
            None,
            20,
            TransportBackend::default(),
            None,
        );

        let params = client
            .build_place_order_params(
                product_type,
                instrument_id,
                client_order_id,
                OrderSide::Buy,
                OrderType::Limit,
                quantity,
                false,
                Some(TimeInForce::Gtc),
                Some(Price::from("50000.0")),
                None,
                None,
                None,
                None,
                is_leverage,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("Failed to build params");

        assert_eq!(params.is_leverage, expected);
    }

    // Bitget spot market buys are sized in the quote coin only (no `marketUnit`)
    #[rstest]
    #[case::spot_market_quote_quantity(BitgetProductType::Spot, OrderType::Market, true, true)]
    #[case::spot_market_base_quantity(BitgetProductType::Spot, OrderType::Market, false, false)]
    #[case::spot_limit_no_unit(BitgetProductType::Spot, OrderType::Limit, false, true)]
    #[case::spot_limit_quote(BitgetProductType::Spot, OrderType::Limit, true, true)]
    #[case::linear_market_no_unit(BitgetProductType::Linear, OrderType::Market, false, true)]
    #[case::inverse_market_no_unit(BitgetProductType::Inverse, OrderType::Market, true, true)]
    fn test_is_quote_quantity_parameter(
        #[case] product_type: BitgetProductType,
        #[case] order_type: OrderType,
        #[case] is_quote_quantity: bool,
        #[case] expected_ok: bool,
    ) {
        let symbol = match product_type {
            BitgetProductType::Spot => "BTCUSDT-SPOT.BITGET",
            BitgetProductType::Linear => "ETHUSDT-PERP.BITGET",
            BitgetProductType::Inverse => "BTCUSD_CM-INVERSE.BITGET",
            BitgetProductType::Option => "BTC-31MAY24-50000-C-OPTION.BITGET",
        };

        let client = BitgetWebSocketClient::new_trade(
            BitgetEnvironment::Testnet,
            Some("test-key".to_string()),
            Some("test-secret".to_string()),
            Some("test-passphrase".to_string()),
            None,
            20,
            TransportBackend::default(),
            None,
        );

        let result = client.build_place_order_params(
            product_type,
            InstrumentId::from(symbol),
            ClientOrderId::from("test-order-1"),
            OrderSide::Buy,
            order_type,
            Quantity::from("1.0"),
            is_quote_quantity,
            Some(TimeInForce::Gtc),
            if order_type == OrderType::Market {
                None
            } else {
                Some(Price::from("50000.0"))
            },
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert_eq!(result.is_ok(), expected_ok);
    }

    #[rstest]
    #[case(OrderType::StopMarket)]
    #[case(OrderType::StopLimit)]
    #[case(OrderType::MarketIfTouched)]
    #[case(OrderType::LimitIfTouched)]
    fn test_build_place_order_params_rejects_conditional_orders(#[case] order_type: OrderType) {
        let client = BitgetWebSocketClient::new_trade(
            BitgetEnvironment::Testnet,
            Some("test-key".to_string()),
            Some("test-secret".to_string()),
            Some("test-passphrase".to_string()),
            None,
            20,
            TransportBackend::default(),
            None,
        );

        let error = client
            .build_place_order_params(
                BitgetProductType::Linear,
                InstrumentId::from("ETHUSDT-PERP.BITGET"),
                ClientOrderId::from("test-stop-1"),
                OrderSide::Buy,
                order_type,
                Quantity::from("1.0"),
                false,
                Some(TimeInForce::Gtc),
                Some(Price::from("50000.0")),
                Some(Price::from("51000.0")),
                None,
                None,
                None,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap_err();

        assert!(error.to_string().contains("strategy-order"));
    }

    #[rstest]
    fn test_build_place_order_params_with_bbo_omits_price() {
        let client = BitgetWebSocketClient::new_trade(
            BitgetEnvironment::Testnet,
            Some("test-key".to_string()),
            Some("test-secret".to_string()),
            Some("test-passphrase".to_string()),
            None,
            20,
            TransportBackend::default(),
            None,
        );

        let params = client
            .build_place_order_params(
                BitgetProductType::Linear,
                InstrumentId::from("ETHUSDT-PERP.BITGET"),
                ClientOrderId::from("test-bbo-order-1"),
                OrderSide::Buy,
                OrderType::Limit,
                Quantity::from("1.0"),
                false,
                Some(TimeInForce::Gtc),
                Some(Price::from("50000.0")),
                None,
                None,
                None,
                None,
                false,
                None,
                None,
                None,
                Some(BitgetBboSideType::Queue),
                Some("2".to_string()),
                None,
            )
            .expect("Failed to build params");

        assert_eq!(params.price, None);
        assert_eq!(params.bbo_level.as_deref(), Some("queue2"));
    }
}
