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

//! WebSocket message handler for Bitget.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use dashmap::DashMap;
use nautilus_core::string::secret::SecretString;
use nautilus_live::book::snapshot::SnapshotGate;
use nautilus_network::{
    error::SendError,
    retry::{RetryManager, create_websocket_retry_manager},
    websocket::{AuthTracker, SubscriptionState, WebSocketClient},
};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use super::{
    enums::BitgetWsOperation,
    error::{
        BitgetWsError, BitgetWsResult, create_bitget_timeout_error, should_retry_bitget_error,
    },
    messages::{
        BitgetSubscription, BitgetWebSocketError, BitgetWsArg, BitgetWsFrame, BitgetWsHeader,
        BitgetWsMessage, BitgetWsOrderResponse, BitgetWsRequest, BitgetWsResponse,
        BitgetWsSubscriptionMsg,
    },
    parse::parse_bitget_ws_frame,
};
use crate::common::{
    consts::{BITGET_WS_HEARTBEAT_PAYLOAD, BITGET_WS_PONG_PAYLOAD},
    enums::{BitgetProductType, BitgetWsOrderRequestOp},
    rate_limit::BitgetRateLimiter,
};

/// Semantic order command whose time-sensitive header is built at send time.
#[derive(Debug)]
pub struct BitgetWsOrderCommand {
    pub(crate) req_id: String,
    pub(crate) op: BitgetWsOrderRequestOp,
    pub(crate) category: BitgetProductType,
    pub(crate) weight: u32,
    pub(crate) referer: Option<String>,
    pub(crate) args: Vec<Value>,
}

#[derive(Clone, Copy, Debug)]
struct PendingRate {
    endpoint: &'static str,
    category: BitgetProductType,
}

#[derive(Debug)]
enum OrderSendFailure {
    NotSent(BitgetWsError),
    Ambiguous(BitgetWsError),
}

/// Commands sent from the outer client to the inner message handler.
#[derive(Debug)]
pub enum HandlerCommand {
    SetClient(WebSocketClient),
    Disconnect,
    Authenticate {
        payload: SecretString,
    },
    Subscribe {
        topics: Vec<String>,
    },
    /// Subscribes to a book topic, opening `gate` once the connection confirms the write.
    ///
    /// A queued write always runs, since another subscription can hold the same topic.
    SubscribeBook {
        payload: String,
        gate: SnapshotGate,
        completion: tokio::sync::oneshot::Sender<BitgetWsResult<()>>,
    },
    Unsubscribe {
        topics: Vec<String>,
    },
    /// Replaces a book subscription on the current connection, opening `gate` once both writes
    /// complete.
    ResubscribeBook {
        topic: String,
        cancel: CancellationToken,
        gate: SnapshotGate,
        completion: tokio::sync::oneshot::Sender<BitgetWsResult<()>>,
    },
    SendOrder {
        command: BitgetWsOrderCommand,
    },
    SendOrders {
        commands: Vec<BitgetWsOrderCommand>,
    },
}

pub(super) struct BitgetWsFeedHandler {
    signal: Arc<AtomicBool>,
    inner: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    auth_tracker: AuthTracker,
    subscriptions: SubscriptionState,
    inst_type: &'static str,
    rate_limiter: BitgetRateLimiter,
    pending_rates: DashMap<String, PendingRate>,
    pending_orders: VecDeque<BitgetWsOrderCommand>,
    retry_manager: RetryManager<BitgetWsError>,
}

impl BitgetWsFeedHandler {
    /// Creates a new [`BitgetWsFeedHandler`] instance.
    pub(super) fn new(
        signal: Arc<AtomicBool>,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
        auth_tracker: AuthTracker,
        subscriptions: SubscriptionState,
        inst_type: &'static str,
        rate_limiter: BitgetRateLimiter,
    ) -> Self {
        Self {
            signal,
            inner: None,
            cmd_rx,
            raw_rx,
            auth_tracker,
            subscriptions,
            inst_type,
            rate_limiter,
            pending_rates: DashMap::new(),
            pending_orders: VecDeque::new(),
            retry_manager: create_websocket_retry_manager(),
        }
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.signal.load(Ordering::Relaxed)
    }

    /// Sends a WebSocket message with retry logic.
    async fn send_with_retry(&self, payload: String) -> Result<(), BitgetWsError> {
        self.send_secret_with_retry(payload.into(), None).await
    }

    async fn send_secret_with_retry(
        &self,
        payload: SecretString,
        connection_epoch: Option<u64>,
    ) -> Result<(), BitgetWsError> {
        if let Some(client) = &self.inner {
            self.retry_manager
                .invocation(
                    "websocket_send",
                    || {
                        let payload = payload.clone();
                        async move {
                            let payload = payload.expose_secret().to_owned();

                            let result = match connection_epoch {
                                Some(epoch) => {
                                    client.send_text_on_connection(payload, None, epoch).await
                                }
                                None => client.send_text(payload, None).await,
                            };

                            result.map_err(|e| match e {
                                SendError::ConnectionChanged => BitgetWsError::Authentication(
                                    "Connection changed before authentication".to_string(),
                                ),
                                e => BitgetWsError::Transport(format!("Send failed: {e}")),
                            })
                        }
                    },
                    should_retry_bitget_error,
                    |e| create_bitget_timeout_error(e.to_string()),
                )
                .execute()
                .await
        } else {
            Err(BitgetWsError::ClientError(
                "No active WebSocket client".to_string(),
            ))
        }
    }

    // Confirms the write on the current connection; a failed write is not replayed on the next
    async fn write_book_subscription(&self, payload: String) -> BitgetWsResult<()> {
        let client = self.inner.as_ref().ok_or(BitgetWsError::NotConnected)?;
        let epoch = client.connection_epoch();

        client
            .send_text_on_connection(payload, None, epoch)
            .await
            .map_err(|e| BitgetWsError::Transport(e.to_string()))
    }

    // Writes both requests on one connection so the venue replaces the subscription it serves,
    // and never stops between them, since a depth-1 topic can also carry quotes
    async fn resubscribe_book(&self, topic: &str) -> BitgetWsResult<()> {
        let client = self.inner.as_ref().ok_or(BitgetWsError::NotConnected)?;
        let epoch = client.connection_epoch();

        // Keeps the topic replayed on reconnect until the venue confirms the new subscription
        self.subscriptions.mark_failure(topic);

        let topic = topic.to_string();

        for op in [BitgetWsOperation::Unsubscribe, BitgetWsOperation::Subscribe] {
            let request = BitgetSubscription {
                op,
                args: vec![BitgetWsArg::from_topic(&topic, self.inst_type)],
            };

            let payload = serde_json::to_string(&request)?;
            client.send_text_on_connection(payload, None, epoch).await?;
        }

        Ok(())
    }

    async fn send_order(&self, command: &BitgetWsOrderCommand) -> Result<bool, OrderSendFailure> {
        if !self.auth_tracker.is_authenticated() {
            return Ok(false);
        }

        let client = self.inner.as_ref().ok_or_else(|| {
            OrderSendFailure::NotSent(BitgetWsError::ClientError(
                "No active WebSocket client".to_string(),
            ))
        })?;
        let (endpoint, _) = order_operation(command.op);

        self.rate_limiter
            .acquire_ws_order(endpoint, command.category, command.weight)
            .await
            .map_err(|e| OrderSendFailure::NotSent(BitgetWsError::ClientError(e)))?;

        if !self.auth_tracker.is_authenticated() {
            return Ok(false);
        }

        let connection_epoch = client.connection_epoch();
        // Bitget has no request signature or receive window on `op:"trade"`: a per-order
        // `receiveWindow` would cancel resting orders once it elapses
        let request = BitgetWsRequest {
            req_id: command.req_id.clone(),
            op: BitgetWsOperation::Trade,
            category: command.category.as_ws_inst_type(),
            topic: command.op,
            header: BitgetWsHeader::with_referer(command.referer.clone()),
            args: command.args.clone(),
        };
        let payload = serde_json::to_string(&request)
            .map_err(|e| OrderSendFailure::NotSent(BitgetWsError::Json(e.to_string())))?;
        self.pending_rates.insert(
            command.req_id.clone(),
            PendingRate {
                endpoint,
                category: command.category,
            },
        );

        match client
            .send_text_on_connection(payload, None, connection_epoch)
            .await
        {
            Ok(()) => Ok(true),
            Err(e) => self.classify_order_send_error(&command.req_id, e),
        }
    }

    fn classify_order_send_error(
        &self,
        req_id: &str,
        error: SendError,
    ) -> Result<bool, OrderSendFailure> {
        match error {
            SendError::ConnectionChanged => {
                self.pending_rates.remove(req_id);
                self.auth_tracker.invalidate();
                Ok(false)
            }
            SendError::Timeout => {
                self.pending_rates.remove(req_id);
                self.auth_tracker.invalidate();
                Err(OrderSendFailure::NotSent(BitgetWsError::ClientError(
                    "Order command was not written".to_string(),
                )))
            }
            SendError::InvalidInput(_) | SendError::BufferFull | SendError::Closed => {
                self.pending_rates.remove(req_id);
                Err(OrderSendFailure::NotSent(BitgetWsError::ClientError(
                    "Order command was not written".to_string(),
                )))
            }
            error => Err(OrderSendFailure::Ambiguous(BitgetWsError::Send(
                error.to_string(),
            ))),
        }
    }

    pub(super) async fn next(&mut self) -> Option<BitgetWsMessage> {
        loop {
            if self.auth_tracker.is_authenticated()
                && let Some(command) = self.pending_orders.pop_front()
            {
                let req_id = command.req_id.clone();
                match self.send_order(&command).await {
                    Ok(true) => {}
                    Ok(false) => self.pending_orders.push_front(command),
                    Err(OrderSendFailure::NotSent(error)) => {
                        return Some(order_not_sent_message(&command, &error));
                    }
                    Err(OrderSendFailure::Ambiguous(error)) => {
                        log::warn!("Ambiguous order send failure: req_id={req_id}, error={error}");
                    }
                }
                continue;
            }

            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        HandlerCommand::SetClient(client) => {
                            log::debug!("WebSocketClient received by handler");
                            self.inner = Some(client);
                        }
                        HandlerCommand::Disconnect => {
                            log::debug!("Disconnect command received");

                            if let Some(client) = self.inner.take() {
                                client.disconnect().await;
                            }
                        }
                        HandlerCommand::Authenticate { payload } => {
                            log::debug!("Authenticate command received");

                            let epoch = self.inner.as_ref().map(WebSocketClient::connection_epoch);
                            if let Err(e) = self.send_secret_with_retry(payload, epoch).await {
                                log::error!("Failed to send authentication after retries: {e}");
                            }
                        }
                        HandlerCommand::Subscribe { topics } => {
                            for topic in topics {
                                log::debug!("Subscribing to topic: topic={topic}");
                                if let Err(e) = self.send_with_retry(topic.clone()).await {
                                    log::error!("Failed to send subscription after retries: topic={topic}, error={e}");
                                }
                            }
                        }
                        HandlerCommand::SubscribeBook { payload, gate, completion } => {
                            let result = self.write_book_subscription(payload).await;

                            if result.is_ok() {
                                gate.open();
                            }
                            let _ = completion.send(result);
                        }
                        HandlerCommand::Unsubscribe { topics } => {
                            for topic in topics {
                                log::debug!("Unsubscribing from topic: topic={topic}");
                                if let Err(e) = self.send_with_retry(topic.clone()).await {
                                    log::error!("Failed to send unsubscription after retries: topic={topic}, error={e}");
                                }
                            }
                        }
                        HandlerCommand::ResubscribeBook { topic, cancel, gate, completion } => {
                            if cancel.is_cancelled() {
                                continue;
                            }

                            let result = self.resubscribe_book(&topic).await;

                            if result.is_ok() {
                                gate.open();
                            }
                            let _ = completion.send(result);
                        }
                        HandlerCommand::SendOrder { command } => {
                            let req_id = command.req_id.clone();
                            match self.send_order(&command).await {
                                Ok(true) => {}
                                Ok(false) => self.pending_orders.push_back(command),
                                Err(OrderSendFailure::NotSent(error)) => {
                                    return Some(order_not_sent_message(&command, &error));
                                }
                                Err(OrderSendFailure::Ambiguous(error)) => {
                                    log::warn!(
                                        "Ambiguous order send failure: req_id={req_id}, error={error}"
                                    );
                                }
                            }
                        }
                        HandlerCommand::SendOrders { commands } => {
                            self.pending_orders.extend(commands);
                        }
                    }
                }

                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    if self.signal.load(Ordering::Relaxed) {
                        log::debug!("Stop signal received during idle period");
                        return None;
                    }
                }

                msg = self.raw_rx.recv() => {
                    let msg = match msg {
                        Some(msg) => msg,
                        None => {
                            log::debug!("WebSocket stream closed");
                            return None;
                        }
                    };

                    if let Message::Ping(data) = &msg {
                        log::trace!("Received ping frame with {} bytes", data.len());

                        if let Some(client) = &self.inner
                            && let Err(e) = client.send_pong(data.to_vec()).await
                        {
                            log::warn!("Failed to send pong frame: error={e}");
                        }
                        continue;
                    }

                    // Bitget keepalive is a text frame: answer a venue `ping` with `pong`
                    if let Message::Text(text) = &msg
                        && text.trim() == BITGET_WS_HEARTBEAT_PAYLOAD
                    {
                        if let Some(client) = &self.inner
                            && let Err(e) = client.send_text(BITGET_WS_PONG_PAYLOAD.to_string(), None).await
                        {
                            log::warn!("Failed to send pong: error={e}");
                        }
                        continue;
                    }

                    let frame = match Self::parse_raw_frame(msg) {
                        Some(frame) => frame,
                        None => continue,
                    };

                    if self.signal.load(Ordering::Relaxed) {
                        log::debug!("Stop signal received");
                        return None;
                    }

                    match frame {
                        BitgetWsFrame::Subscription(ref sub_msg) => {
                            self.handle_subscription_ack(sub_msg);
                        }
                        BitgetWsFrame::Auth(auth_response) => {
                            let is_success = auth_response.success.unwrap_or(false)
                                || (auth_response.ret_code == Some(0));

                            if is_success {
                                self.auth_tracker.succeed();
                                log::debug!("WebSocket authenticated");
                            } else {
                                let error_msg = auth_response
                                    .ret_msg
                                    .as_deref()
                                    .unwrap_or("Authentication rejected");
                                self.auth_tracker.fail(error_msg);
                                log::error!("WebSocket authentication failed: error={error_msg}");
                            }
                            return Some(BitgetWsMessage::Auth(auth_response));
                        }
                        BitgetWsFrame::ErrorResponse(ref resp) => {
                            // Failed subscription/unsubscription ACKs arrive as
                            // ErrorResponse when success=false. Route them through
                            // the subscription state machine when the op field is
                            // present, otherwise forward as a generic error.
                            if let Some(op) = &resp.op {
                                if *op == BitgetWsOperation::Subscribe
                                    || *op == BitgetWsOperation::Unsubscribe
                                {
                                    self.handle_subscription_error(resp);
                                } else {
                                    let error = BitgetWebSocketError::from_response(resp);
                                    return Some(BitgetWsMessage::Error(error));
                                }
                            } else {
                                let error = BitgetWebSocketError::from_response(resp);
                                return Some(BitgetWsMessage::Error(error));
                            }
                        }
                        BitgetWsFrame::OrderResponse(resp) => {
                            self.observe_order_rate_limit(&resp);
                            return Some(BitgetWsMessage::OrderResponse(resp));
                        }
                        BitgetWsFrame::Orderbook(msg) => {
                            return Some(BitgetWsMessage::Orderbook(msg));
                        }
                        BitgetWsFrame::Trade(msg) => {
                            return Some(BitgetWsMessage::Trade(msg));
                        }
                        BitgetWsFrame::Liquidation(msg) => {
                            return Some(BitgetWsMessage::Liquidation(msg));
                        }
                        BitgetWsFrame::Kline(msg) => {
                            return Some(BitgetWsMessage::Kline(msg));
                        }
                        BitgetWsFrame::TickerLinear(msg) => {
                            return Some(BitgetWsMessage::TickerLinear(msg));
                        }
                        BitgetWsFrame::TickerOption(msg) => {
                            return Some(BitgetWsMessage::TickerOption(msg));
                        }
                        BitgetWsFrame::AccountOrder(msg) => {
                            return Some(BitgetWsMessage::AccountOrder(msg));
                        }
                        BitgetWsFrame::AccountStrategyOrder(msg) => {
                            return Some(BitgetWsMessage::AccountStrategyOrder(msg));
                        }
                        BitgetWsFrame::AccountExecution(msg) => {
                            return Some(BitgetWsMessage::AccountExecution(msg));
                        }
                        BitgetWsFrame::AccountExecutionFast(msg) => {
                            return Some(BitgetWsMessage::AccountExecutionFast(msg));
                        }
                        BitgetWsFrame::AccountWallet(msg) => {
                            return Some(BitgetWsMessage::AccountWallet(msg));
                        }
                        BitgetWsFrame::AccountPosition(msg) => {
                            return Some(BitgetWsMessage::AccountPosition(msg));
                        }
                        BitgetWsFrame::Reconnected => {
                            self.auth_tracker.invalidate();
                            return Some(BitgetWsMessage::Reconnected);
                        }
                        BitgetWsFrame::Unknown(value) => {
                            log::debug!("Unknown WebSocket frame: {value}");
                        }
                    }
                }
            }
        }
    }

    fn observe_order_rate_limit(&self, response: &super::messages::BitgetWsOrderResponse) {
        let Some(req_id) = response.req_id.as_deref() else {
            return;
        };
        let Some((_, pending)) = self.pending_rates.remove(req_id) else {
            return;
        };
        // Bitget reports the remaining quota as `rateLimit:[{limit, remaining}]` without a reset time
        let Some(rate) = response.header.as_ref().and_then(|rates| rates.first()) else {
            return;
        };
        let (Ok(limit), Ok(remaining)) = (rate.limit.parse::<u32>(), rate.remaining.parse::<u32>())
        else {
            return;
        };
        self.rate_limiter.observe_account(
            pending.endpoint,
            Some(pending.category),
            limit,
            remaining,
            None,
        );
    }

    fn handle_subscription_ack(&self, sub_msg: &BitgetWsSubscriptionMsg) {
        match sub_msg.op {
            BitgetWsOperation::Subscribe => {
                if sub_msg.success {
                    if let Some(topic) = &sub_msg.req_id {
                        self.subscriptions.confirm_subscribe(topic);
                        log::debug!("Subscription confirmed: topic={topic}");
                    } else {
                        // No req_id, fall back to confirming all pending
                        for topic in self.subscriptions.pending_subscribe_topics() {
                            self.subscriptions.confirm_subscribe(&topic);
                            log::debug!("Subscription confirmed (bulk): topic={topic}");
                        }
                    }
                } else if let Some(topic) = &sub_msg.req_id {
                    self.subscriptions.mark_failure(topic);
                    log::warn!(
                        "Subscription failed: topic={topic}, error={:?}",
                        sub_msg.ret_msg
                    );
                } else {
                    for topic in self.subscriptions.pending_subscribe_topics() {
                        self.subscriptions.mark_failure(&topic);
                        log::warn!(
                            "Subscription failed (bulk): topic={topic}, error={:?}",
                            sub_msg.ret_msg
                        );
                    }
                }
            }
            BitgetWsOperation::Unsubscribe => {
                if sub_msg.success {
                    if let Some(topic) = &sub_msg.req_id {
                        self.subscriptions.confirm_unsubscribe(topic);
                        log::debug!("Unsubscription confirmed: topic={topic}");
                    } else {
                        for topic in self.subscriptions.pending_unsubscribe_topics() {
                            self.subscriptions.confirm_unsubscribe(&topic);
                            log::debug!("Unsubscription confirmed (bulk): topic={topic}");
                        }
                    }
                } else {
                    let topic_desc = sub_msg.req_id.as_deref().unwrap_or("unknown");
                    log::warn!(
                        "Unsubscription failed: topic={topic_desc}, error={:?}",
                        sub_msg.ret_msg
                    );
                }
            }
            _ => {}
        }
    }

    fn handle_subscription_error(&self, resp: &BitgetWsResponse) {
        let topic = resp.req_id.as_deref().unwrap_or("unknown");
        let error_msg = resp.ret_msg.as_deref().unwrap_or("unknown error");

        match resp.op {
            Some(BitgetWsOperation::Subscribe) => {
                // Duplicate subscribe is harmless: the topic is active on the
                // venue, so confirm it instead of looping retries every reconnect.
                if is_already_subscribed_error(error_msg)
                    && let Some(ref req_id) = resp.req_id
                {
                    self.subscriptions.confirm_subscribe(req_id);
                    log::debug!("Subscription duplicate ignored: topic={topic}, error={error_msg}");
                    return;
                }

                if let Some(ref req_id) = resp.req_id {
                    self.subscriptions.mark_failure(req_id);
                } else {
                    for t in self.subscriptions.pending_subscribe_topics() {
                        self.subscriptions.mark_failure(&t);
                    }
                }
                log::warn!("Subscription error: topic={topic}, error={error_msg}");
            }
            Some(BitgetWsOperation::Unsubscribe) => {
                log::warn!("Unsubscription error: topic={topic}, error={error_msg}");
            }
            _ => {}
        }
    }

    fn parse_raw_frame(msg: Message) -> Option<BitgetWsFrame> {
        match msg {
            Message::Text(text) => {
                if text == nautilus_network::RECONNECTED {
                    log::debug!("Received WebSocket reconnected signal");
                    return Some(BitgetWsFrame::Reconnected);
                }

                if text.trim().eq_ignore_ascii_case(BITGET_WS_PONG_PAYLOAD) {
                    return None;
                }

                log::trace!("Raw websocket message: {text}");

                let value: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!("Failed to parse WebSocket message: {e}: {text}");
                        return None;
                    }
                };

                Some(parse_bitget_ws_frame(value))
            }
            Message::Binary(msg) => {
                log::debug!("Raw binary frame ({} bytes)", msg.len());
                log::trace!("Raw binary: {msg:?}");
                None
            }
            Message::Close(_) => {
                log::debug!("Received close message, waiting for reconnection");
                None
            }
            _ => None,
        }
    }
}

// REST and WebSocket order entry share one quota per endpoint
fn order_operation(op: BitgetWsOrderRequestOp) -> (&'static str, &'static str) {
    match op {
        BitgetWsOrderRequestOp::Create => ("/api/v3/trade/place-order", "place-order"),
        BitgetWsOrderRequestOp::Amend => ("/api/v3/trade/modify-order", "modify-order"),
        BitgetWsOrderRequestOp::Cancel => ("/api/v3/trade/cancel-order", "cancel-order"),
        BitgetWsOrderRequestOp::CreateBatch => ("/api/v3/trade/place-batch", "batch-place"),
        BitgetWsOrderRequestOp::AmendBatch => ("/api/v3/trade/batch-modify-order", "batch-modify"),
        BitgetWsOrderRequestOp::CancelBatch => ("/api/v3/trade/cancel-batch", "batch-cancel"),
    }
}

fn order_not_sent_message(
    command: &BitgetWsOrderCommand,
    error: &BitgetWsError,
) -> BitgetWsMessage {
    let (_, op) = order_operation(command.op);
    BitgetWsMessage::OrderResponse(BitgetWsOrderResponse {
        op: Ustr::from(op),
        conn_id: None,
        ret_code: -1,
        ret_msg: error.to_string(),
        data: Value::Array(vec![]),
        req_id: Some(command.req_id.clone()),
        header: None,
    })
}

fn is_already_subscribed_error(error_msg: &str) -> bool {
    error_msg
        .to_ascii_lowercase()
        .contains("already subscribed")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use nautilus_common::testing::wait_until_async;
    use nautilus_core::string::secret::REDACTED;
    use nautilus_network::websocket::{WebSocketConfig, channel_message_handler};
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::common::{
        consts::BITGET_WS_TOPIC_DELIMITER, rate_limit::BitgetRateLimiter, testing::load_test_json,
    };

    fn create_test_handler() -> BitgetWsFeedHandler {
        let signal = Arc::new(AtomicBool::new(false));
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let auth_tracker = AuthTracker::new();
        let subscriptions = SubscriptionState::new(BITGET_WS_TOPIC_DELIMITER);

        BitgetWsFeedHandler::new(
            signal,
            cmd_rx,
            raw_rx,
            auth_tracker,
            subscriptions,
            "usdt-futures",
            BitgetRateLimiter::for_websocket(
                "wss://bitget-handler-test.invalid",
                Some("test-key"),
                None,
            ),
        )
    }

    #[tokio::test]
    async fn test_authentication_with_full_replay_buffer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (replayed_tx, replayed_rx) = tokio::sync::oneshot::channel();

        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let _first = tokio_tungstenite::accept_async(first).await.unwrap();
            let (replacement, _) = listener.accept().await.unwrap();
            let mut replacement = tokio_tungstenite::accept_async(replacement).await.unwrap();
            assert_eq!(
                replacement.next().await.unwrap().unwrap(),
                Message::text("authenticate")
            );
            replacement
                .send(Message::text(load_test_json("ws_auth_success.json")))
                .await
                .unwrap();
            assert_eq!(
                replacement.next().await.unwrap().unwrap(),
                Message::text("held")
            );
            replayed_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });

        let mut handler = create_test_handler();
        let tracker = handler.auth_tracker.clone();
        let (message_handler, raw_rx) = channel_message_handler();
        handler.raw_rx = raw_rx;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        handler.cmd_rx = cmd_rx;
        let config = WebSocketConfig::builder()
            .url(format!("ws://{address}"))
            .writer_capacity(1)
            .reconnect_delay_initial_ms(1)
            .reconnect_delay_max_ms(1)
            .reconnect_jitter_ms(0)
            .build()
            .unwrap();
        let client = WebSocketClient::builder()
            .config(config)
            .message_handler(message_handler)
            .connect()
            .await
            .unwrap();
        client.set_auth_tracker(tracker.clone(), true);

        // Enqueue and request reconnect without yielding so the writer retains this message
        client.send_text("held".to_string(), None).await.unwrap();
        assert!(client.request_reconnect());
        wait_until_async(
            || async { client.is_active() && client.connection_epoch() == 1 },
            Duration::from_secs(5),
        )
        .await;
        assert!(matches!(
            client.send_text("overflow".to_string(), None).await,
            Err(SendError::BufferFull)
        ));
        handler.inner = Some(client);
        let stale = tokio::time::timeout(
            Duration::from_secs(5),
            handler.send_secret_with_retry(SecretString::from("stale-auth".to_string()), Some(0)),
        )
        .await
        .unwrap();
        assert!(
            matches!(stale, Err(BitgetWsError::Authentication(message)) if message == "Connection changed before authentication")
        );
        cmd_tx
            .send(HandlerCommand::Authenticate {
                payload: SecretString::from("authenticate".to_string()),
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(BitgetWsMessage::Auth(response)) = handler.next().await {
                    assert_eq!(response.ret_code, Some(0));
                    break;
                }
            }
        })
        .await
        .unwrap();

        assert!(tracker.is_authenticated());
        tokio::time::timeout(Duration::from_secs(5), replayed_rx)
            .await
            .unwrap()
            .unwrap();
        handler.inner.as_ref().unwrap().disconnect().await;
        server.abort();
    }

    async fn connect_book_handler() -> (
        BitgetWsFeedHandler,
        tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
        tokio::sync::mpsc::UnboundedReceiver<Value>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (requests_tx, requests_rx) = tokio::sync::mpsc::unbounded_channel();

        // Forwards each request as it arrives, since a completed write may still be queued
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();

            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let request = serde_json::from_str::<Value>(&text).unwrap();

                if requests_tx.send(request).is_err() {
                    break;
                }
            }
        });

        let mut handler = create_test_handler();
        let (message_handler, raw_rx) = channel_message_handler();
        handler.raw_rx = raw_rx;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        handler.cmd_rx = cmd_rx;
        let config = WebSocketConfig::builder()
            .url(format!("ws://{address}"))
            .build()
            .unwrap();
        let client = WebSocketClient::builder()
            .config(config)
            .message_handler(message_handler)
            .connect()
            .await
            .unwrap();
        handler.inner = Some(client);

        (handler, cmd_tx, requests_rx)
    }

    async fn complete_book_command(
        handler: &mut BitgetWsFeedHandler,
        receiver: tokio::sync::oneshot::Receiver<BitgetWsResult<()>>,
    ) -> Result<BitgetWsResult<()>, tokio::sync::oneshot::error::RecvError> {
        let drive = async {
            loop {
                handler.next().await;
            }
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = receiver => result,
                () = drive => unreachable!("handler loop never ends"),
            }
        })
        .await
        .unwrap()
    }

    async fn next_request(requests: &mut tokio::sync::mpsc::UnboundedReceiver<Value>) -> Value {
        tokio::time::timeout(Duration::from_secs(5), requests.recv())
            .await
            .unwrap()
            .unwrap()
    }

    fn subscription_request(op: &str, topic: &str) -> Value {
        serde_json::json!({"op": op, "args": [BitgetWsArg::from_topic(topic, "usdt-futures")]})
    }

    fn subscribe_book_command(
        topic: &str,
        gate: SnapshotGate,
    ) -> (
        HandlerCommand,
        tokio::sync::oneshot::Receiver<BitgetWsResult<()>>,
    ) {
        let payload = subscription_request("subscribe", topic).to_string();
        let (completion, receiver) = tokio::sync::oneshot::channel();

        let command = HandlerCommand::SubscribeBook {
            payload,
            gate,
            completion,
        };

        (command, receiver)
    }

    #[tokio::test]
    async fn test_subscribe_book_opens_gate_after_write() {
        let (mut handler, cmd_tx, mut requests) = connect_book_handler().await;
        let topic = "books.BTCUSDT";
        let gate = SnapshotGate::default();
        gate.lock().close();
        let (command, receiver) = subscribe_book_command(topic, gate.clone());

        cmd_tx.send(command).unwrap();
        let result = complete_book_command(&mut handler, receiver).await;
        let request = next_request(&mut requests).await;

        assert!(matches!(result, Ok(Ok(()))));
        assert!(!gate.lock().is_closed());
        assert_eq!(request, subscription_request("subscribe", topic));
        handler.inner.as_ref().unwrap().disconnect().await;
    }

    #[tokio::test]
    async fn test_resubscribe_book_replaces_subscription_and_opens_gate() {
        let (mut handler, cmd_tx, mut requests) = connect_book_handler().await;
        let topic = "books.BTCUSDT";
        handler.subscriptions.mark_subscribe(topic);
        handler.subscriptions.confirm_subscribe(topic);
        let gate = SnapshotGate::default();
        gate.lock().close();
        let (completion, receiver) = tokio::sync::oneshot::channel();

        cmd_tx
            .send(HandlerCommand::ResubscribeBook {
                topic: topic.to_string(),
                cancel: CancellationToken::new(),
                gate: gate.clone(),
                completion,
            })
            .unwrap();

        let result = complete_book_command(&mut handler, receiver).await;
        let pending = handler.subscriptions.pending_subscribe_topics();
        let unsubscribe = next_request(&mut requests).await;
        let subscribe = next_request(&mut requests).await;

        assert!(matches!(result, Ok(Ok(()))));
        assert!(!gate.lock().is_closed());
        assert_eq!(pending, [topic]);
        assert_eq!(unsubscribe, subscription_request("unsubscribe", topic));
        assert_eq!(subscribe, subscription_request("subscribe", topic));
        handler.inner.as_ref().unwrap().disconnect().await;
    }

    #[tokio::test]
    async fn test_subscribe_book_write_failure_keeps_gate_closed() {
        let (mut handler, cmd_tx, _requests) = connect_book_handler().await;
        handler.inner.as_ref().unwrap().disconnect().await;
        let gate = SnapshotGate::default();
        gate.lock().close();
        let (command, receiver) = subscribe_book_command("books.BTCUSDT", gate.clone());

        cmd_tx.send(command).unwrap();
        let result = complete_book_command(&mut handler, receiver).await;

        assert!(matches!(result, Ok(Err(BitgetWsError::Transport(_)))));
        assert!(gate.lock().is_closed());
    }

    // A later subscribe arrives first; any replacement write would complete before its completion
    #[tokio::test]
    async fn test_resubscribe_book_cancelled_before_write_sends_nothing() {
        let (mut handler, cmd_tx, mut requests) = connect_book_handler().await;
        let topic = "books.BTCUSDT";
        handler.subscriptions.mark_subscribe(topic);
        handler.subscriptions.confirm_subscribe(topic);
        let gate = SnapshotGate::default();
        gate.lock().close();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (completion, receiver) = tokio::sync::oneshot::channel();
        let sentinel = "books.ETHUSDT";
        let (sentinel_command, sentinel_receiver) =
            subscribe_book_command(sentinel, SnapshotGate::default());

        cmd_tx
            .send(HandlerCommand::ResubscribeBook {
                topic: topic.to_string(),
                cancel,
                gate: gate.clone(),
                completion,
            })
            .unwrap();

        let result = complete_book_command(&mut handler, receiver).await;
        cmd_tx.send(sentinel_command).unwrap();
        let sentinel_result = complete_book_command(&mut handler, sentinel_receiver).await;
        let pending = handler.subscriptions.pending_subscribe_topics();
        let request = next_request(&mut requests).await;

        assert!(result.is_err());
        assert!(matches!(sentinel_result, Ok(Ok(()))));
        assert!(gate.lock().is_closed());
        assert!(pending.is_empty());
        assert_eq!(request, subscription_request("subscribe", sentinel));
        handler.inner.as_ref().unwrap().disconnect().await;
    }

    #[rstest]
    fn test_authenticate_command_debug_redacts_payload() {
        let payload = "authentication-secret";
        let command = HandlerCommand::Authenticate {
            payload: SecretString::from(payload.to_string()),
        };

        let debug = format!("{command:?}");

        assert!(debug.contains(REDACTED));
        assert!(!debug.contains(payload));
    }

    fn load_value(fixture: &str) -> serde_json::Value {
        let json = load_test_json(fixture);
        serde_json::from_str(&json).unwrap()
    }

    #[rstest]
    fn test_handler_initializes() {
        let _handler = create_test_handler();
    }

    #[tokio::test]
    async fn order_without_writer_produces_correlated_terminal_response() {
        let handler = create_test_handler();
        handler.auth_tracker.succeed();
        let command = BitgetWsOrderCommand {
            req_id: "not-sent-request".to_string(),
            op: BitgetWsOrderRequestOp::Create,
            category: BitgetProductType::Linear,
            weight: 1,
            referer: None,
            args: vec![serde_json::json!({"symbol": "BTCUSDT"})],
        };

        let failure = handler.send_order(&command).await.unwrap_err();
        let OrderSendFailure::NotSent(error) = failure else {
            panic!("Expected definitive not-sent failure, was {failure:?}");
        };
        let BitgetWsMessage::OrderResponse(response) = order_not_sent_message(&command, &error)
        else {
            panic!("Expected order response");
        };

        assert_eq!(response.op, "place-order");
        assert_eq!(response.ret_code, -1);
        assert_eq!(response.req_id.as_deref(), Some("not-sent-request"));
        assert_eq!(response.ret_msg, "Client error: No active WebSocket client");
    }

    #[rstest]
    fn connection_change_invalidates_authentication_before_requeue() {
        let handler = create_test_handler();
        handler.auth_tracker.succeed();
        handler.pending_rates.insert(
            "reconnect-request".to_string(),
            PendingRate {
                endpoint: "/api/v3/trade/place-order",
                category: BitgetProductType::Linear,
            },
        );

        let result = handler
            .classify_order_send_error("reconnect-request", SendError::ConnectionChanged)
            .unwrap();

        assert!(!result);
        assert!(!handler.auth_tracker.is_authenticated());
        assert!(!handler.pending_rates.contains_key("reconnect-request"));
    }

    #[rstest]
    fn pre_active_timeout_is_terminal_not_sent() {
        let handler = create_test_handler();
        handler.auth_tracker.succeed();
        handler.pending_rates.insert(
            "timeout-request".to_string(),
            PendingRate {
                endpoint: "/api/v3/trade/place-order",
                category: BitgetProductType::Linear,
            },
        );

        let failure = handler
            .classify_order_send_error("timeout-request", SendError::Timeout)
            .unwrap_err();

        assert!(matches!(failure, OrderSendFailure::NotSent(_)));
        assert!(!handler.auth_tracker.is_authenticated());
        assert!(!handler.pending_rates.contains_key("timeout-request"));
    }

    #[rstest]
    fn buffer_full_is_not_sent_and_preserves_authentication() {
        let handler = create_test_handler();
        handler.auth_tracker.succeed();
        handler.pending_rates.insert(
            "full-request".to_string(),
            PendingRate {
                endpoint: "/api/v3/trade/place-order",
                category: BitgetProductType::Linear,
            },
        );

        let failure = handler
            .classify_order_send_error("full-request", SendError::BufferFull)
            .unwrap_err();

        assert!(matches!(failure, OrderSendFailure::NotSent(_)));
        assert!(handler.auth_tracker.is_authenticated());
        assert!(!handler.pending_rates.contains_key("full-request"));
    }

    #[rstest]
    fn test_parse_frame_auth_success() {
        let value = load_value("ws_auth_success.json");
        let frame = parse_bitget_ws_frame(value);
        match frame {
            BitgetWsFrame::Auth(auth) => {
                assert_eq!(
                    auth.conn_id.as_deref(),
                    Some("068b4afffe0ebff5-00003912-00207636-3f38547f12f362d9-556f6561")
                );
                assert_eq!(auth.ret_code, Some(0));
                assert_eq!(auth.op, BitgetWsOperation::Auth);
            }
            other => panic!("Expected Auth, was {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_frame_auth_failure() {
        let value = load_value("ws_auth_failure.json");
        let frame = parse_bitget_ws_frame(value);
        match frame {
            BitgetWsFrame::ErrorResponse(resp) => {
                assert_eq!(resp.op, Some(BitgetWsOperation::Auth));
                assert_eq!(resp.ret_code, Some(30011));
                assert_eq!(resp.ret_msg.as_deref(), Some("Invalid ACCESS_KEY"));
            }
            other => panic!("Expected ErrorResponse, was {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_frame_subscription_ack() {
        let value = load_value("ws_subscription_ack.json");
        let frame = parse_bitget_ws_frame(value);
        match frame {
            BitgetWsFrame::Subscription(sub) => {
                assert!(sub.success);
                assert_eq!(sub.op, BitgetWsOperation::Subscribe);
                assert_eq!(sub.req_id.as_deref(), Some("books5.BTCUSDT"));
            }
            other => panic!("Expected Subscription, was {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_frame_subscription_failure() {
        let value = load_value("ws_subscription_failure.json");
        let frame = parse_bitget_ws_frame(value);
        match frame {
            BitgetWsFrame::ErrorResponse(resp) => {
                // Bitget echoes the rejected arg inside `msg`, which recovers the topic key
                assert_eq!(resp.op, Some(BitgetWsOperation::Subscribe));
                assert_eq!(resp.ret_code, Some(30001));
                assert_eq!(resp.req_id.as_deref(), Some("ticker.NOSUCHUSDT"));
            }
            other => panic!("Expected ErrorResponse, was {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_frame_order_response() {
        let value = load_value("ws_order_response.json");
        let frame = parse_bitget_ws_frame(value);
        match frame {
            BitgetWsFrame::OrderResponse(resp) => {
                assert_eq!(resp.op, "place-order");
                assert_eq!(resp.ret_code, 0);
                assert_eq!(resp.ret_msg, "Success");
                assert_eq!(resp.req_id.as_deref(), Some("cap17941930863671"));
                assert_eq!(resp.header.as_ref().unwrap()[0].remaining, "19");
            }
            other => panic!("Expected OrderResponse, was {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_frame_order_response_error() {
        let value = load_value("ws_order_response_error.json");
        let frame = parse_bitget_ws_frame(value);
        match frame {
            BitgetWsFrame::OrderResponse(resp) => {
                assert_eq!(resp.op, "");
                assert_eq!(resp.ret_code, 25206);
                assert_eq!(resp.req_id.as_deref(), Some("cap17941930907953"));
            }
            other => panic!("Expected OrderResponse, was {other:?}"),
        }
    }

    #[rstest]
    fn test_parse_frame_orderbook() {
        let value = load_value("ws_orderbook_snapshot.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::Orderbook(_)),
            "Expected Orderbook, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_trade() {
        let value = load_value("ws_public_trade.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::Trade(_)),
            "Expected Trade, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_kline() {
        let value = load_value("ws_kline.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::Kline(_)),
            "Expected Kline, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_ticker_linear() {
        let value = load_value("ws_ticker_linear.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::TickerLinear(_)),
            "Expected TickerLinear, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_funding_rate_is_unknown() {
        // Funding comes from `ticker`; the `funding-rate` channel is not subscribed
        let value = load_value("ws_funding_rate.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::Unknown(_)),
            "Expected Unknown, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_account_order() {
        let value = load_value("ws_account_order.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::AccountOrder(_)),
            "Expected AccountOrder, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_account_strategy_order() {
        let value = load_value("ws_account_order_buy_stop_market.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::AccountStrategyOrder(_)),
            "Expected AccountStrategyOrder, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_account_execution() {
        let value = load_value("ws_account_execution.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::AccountExecution(_)),
            "Expected AccountExecution, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_account_wallet() {
        let value = load_value("ws_account_wallet.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::AccountWallet(_)),
            "Expected AccountWallet, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_account_position() {
        let value = load_value("ws_account_position.json");
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::AccountPosition(_)),
            "Expected AccountPosition, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_frame_unknown_message() {
        let value: serde_json::Value = serde_json::json!({"foo": "bar"});
        let frame = parse_bitget_ws_frame(value);
        assert!(
            matches!(frame, BitgetWsFrame::Unknown(_)),
            "Expected Unknown, was {frame:?}"
        );
    }

    #[rstest]
    fn test_parse_raw_reconnected_signal() {
        let msg = Message::Text(nautilus_network::RECONNECTED.to_string().into());
        let result = BitgetWsFeedHandler::parse_raw_frame(msg);
        assert!(
            matches!(result, Some(BitgetWsFrame::Reconnected)),
            "Expected Some(Reconnected), was {result:?}"
        );
    }

    #[rstest]
    fn test_parse_raw_pong_text() {
        let msg = Message::Text("pong".into());
        let result = BitgetWsFeedHandler::parse_raw_frame(msg);
        assert!(result.is_none(), "Expected None for pong, was {result:?}");
    }

    #[rstest]
    fn test_parse_raw_ping_text_is_not_json() {
        // The handler answers a venue `ping` before parsing; parsing alone drops it as non-JSON
        let msg = Message::Text("ping".into());
        let result = BitgetWsFeedHandler::parse_raw_frame(msg);
        assert!(result.is_none(), "Expected None for ping, was {result:?}");
    }

    #[rstest]
    fn test_parse_raw_valid_json() {
        let json = load_test_json("ws_public_trade.json");
        let msg = Message::Text(json.into());
        let result = BitgetWsFeedHandler::parse_raw_frame(msg);
        assert!(
            matches!(result, Some(BitgetWsFrame::Trade(_))),
            "Expected Some(Trade), was {result:?}"
        );
    }

    #[rstest]
    fn test_parse_raw_invalid_json() {
        let msg = Message::Text("not valid json".into());
        let result = BitgetWsFeedHandler::parse_raw_frame(msg);
        assert!(
            result.is_none(),
            "Expected None for invalid JSON, was {result:?}"
        );
    }

    #[rstest]
    fn test_parse_raw_binary_message() {
        let msg = Message::Binary(vec![0x01, 0x02].into());
        let result = BitgetWsFeedHandler::parse_raw_frame(msg);
        assert!(result.is_none(), "Expected None for binary, was {result:?}");
    }

    #[rstest]
    fn test_subscription_ack_with_req_id_confirms_only_that_topic() {
        let handler = create_test_handler();
        handler.subscriptions.mark_subscribe("books.BTCUSDT");
        handler.subscriptions.mark_subscribe("publicTrade.BTCUSDT");

        let ack = BitgetWsSubscriptionMsg {
            success: true,
            op: BitgetWsOperation::Subscribe,
            conn_id: None,
            req_id: Some("books.BTCUSDT".to_string()),
            ret_msg: None,
        };

        handler.handle_subscription_ack(&ack);

        // Only orderbook should be confirmed, trade stays pending
        assert!(
            handler
                .subscriptions
                .pending_subscribe_topics()
                .contains(&"publicTrade.BTCUSDT".to_string())
        );
        assert!(
            !handler
                .subscriptions
                .pending_subscribe_topics()
                .contains(&"books.BTCUSDT".to_string())
        );
    }

    #[rstest]
    fn test_subscription_failure_with_req_id_marks_only_that_topic() {
        let handler = create_test_handler();
        handler.subscriptions.mark_subscribe("books.BTCUSDT");
        handler.subscriptions.mark_subscribe("publicTrade.BTCUSDT");

        let ack = BitgetWsSubscriptionMsg {
            success: false,
            op: BitgetWsOperation::Subscribe,
            conn_id: None,
            req_id: Some("books.BTCUSDT".to_string()),
            ret_msg: Some("Invalid topic".to_string()),
        };

        handler.handle_subscription_ack(&ack);

        // Orderbook should be marked as failed (back to pending for retry)
        // Trade should remain pending (unaffected)
        let pending = handler.subscriptions.pending_subscribe_topics();
        assert!(pending.contains(&"books.BTCUSDT".to_string()));
        assert!(pending.contains(&"publicTrade.BTCUSDT".to_string()));
    }

    #[rstest]
    fn test_error_response_with_subscribe_op_triggers_mark_failure() {
        let handler = create_test_handler();
        handler
            .subscriptions
            .mark_subscribe("invalid.topic.BTCUSDT");

        let resp = BitgetWsResponse {
            op: Some(BitgetWsOperation::Subscribe),
            topic: None,
            success: Some(false),
            conn_id: None,
            req_id: Some("invalid.topic.BTCUSDT".to_string()),
            ret_code: Some(10001),
            ret_msg: Some("Invalid topic".to_string()),
        };

        handler.handle_subscription_error(&resp);

        // Topic should still be in pending (mark_failure moves confirmed -> pending)
        let pending = handler.subscriptions.pending_subscribe_topics();
        assert!(pending.contains(&"invalid.topic.BTCUSDT".to_string()));
    }

    #[rstest]
    fn test_already_subscribed_error_confirms_topic() {
        let handler = create_test_handler();
        handler.subscriptions.mark_subscribe("ticker.ETHUSDT");

        let resp = BitgetWsResponse {
            op: Some(BitgetWsOperation::Subscribe),
            topic: None,
            success: Some(false),
            conn_id: None,
            req_id: Some("ticker.ETHUSDT".to_string()),
            ret_code: Some(30001),
            ret_msg: Some("error:already subscribed,topic:ticker.ETHUSDT".to_string()),
        };

        handler.handle_subscription_error(&resp);

        let pending = handler.subscriptions.pending_subscribe_topics();
        assert!(!pending.contains(&"ticker.ETHUSDT".to_string()));
        let symbols = handler.subscriptions.confirmed();
        let entry = symbols.get(&Ustr::from("ticker")).expect("channel present");
        assert!(entry.contains(&Ustr::from("ETHUSDT")));
    }

    #[rstest]
    fn test_subscription_ack_without_req_id_confirms_all_pending() {
        let handler = create_test_handler();
        handler.subscriptions.mark_subscribe("books.BTCUSDT");
        handler.subscriptions.mark_subscribe("publicTrade.BTCUSDT");

        let ack = BitgetWsSubscriptionMsg {
            success: true,
            op: BitgetWsOperation::Subscribe,
            conn_id: None,
            req_id: None,
            ret_msg: None,
        };

        handler.handle_subscription_ack(&ack);

        // Both should be confirmed when no req_id
        assert!(handler.subscriptions.pending_subscribe_topics().is_empty());
    }
}
