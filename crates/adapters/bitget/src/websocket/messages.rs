// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! WebSocket message types for Bitget public and private channels.
//!
//! Bitget v3 frames carry the channel as an `arg` object (`instType`, `topic`, `symbol`,
//! `interval`) instead of a topic string. The client keeps Bybit-style dotted topic keys
//! (`books5.BTCUSDT`, `kline.1m.BTCUSDT`, `order`) for subscription state; [`BitgetWsArg`]
//! converts between the two, and each push envelope exposes the key as `topic`.

use std::fmt::Debug;

use nautilus_core::string::secret::{REDACTED, zeroize_json_value};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};
use serde_json::Value;
use ustr::Ustr;
use zeroize::Zeroize;

use crate::{
    common::{
        consts::BITGET_WS_TOPIC_DELIMITER,
        enums::{
            BitgetExecType, BitgetOrderSide, BitgetOrderSmpType, BitgetOrderStatus,
            BitgetOrderType, BitgetPositionIdx, BitgetProductType, BitgetTimeInForce,
            BitgetTriggerType, BitgetWsOrderRequestOp,
        },
        parse::deserialize_i64_or_string,
    },
    websocket::enums::{BitgetWsOperation, BitgetWsPrivateChannel},
};

/// `instType` used by every private channel subscription.
pub const BITGET_WS_PRIVATE_INST_TYPE: &str = "UTA";

/// Channel argument of subscribe requests, acks and pushes.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsArg {
    pub inst_type: Ustr,
    pub topic: Ustr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<Ustr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<Ustr>,
}

impl BitgetWsArg {
    /// Builds the argument for a dotted topic key.
    ///
    /// Public keys are `{topic}.{symbol}` or `kline.{interval}.{symbol}`; private keys are the bare
    /// channel name (`fast-fill` and `strategy-order` subscribe with the `default` symbol).
    #[must_use]
    pub fn from_topic(topic: &str, inst_type: &str) -> Self {
        if inst_type.eq_ignore_ascii_case(BITGET_WS_PRIVATE_INST_TYPE) {
            let symbol =
                matches!(topic, "fast-fill" | "strategy-order").then(|| Ustr::from("default"));
            return Self {
                inst_type: Ustr::from(BITGET_WS_PRIVATE_INST_TYPE),
                topic: Ustr::from(topic),
                symbol,
                interval: None,
            };
        }

        let (channel, rest) = topic
            .split_once(BITGET_WS_TOPIC_DELIMITER)
            .unwrap_or((topic, ""));
        let (interval, symbol) = if channel == "kline" {
            match rest.split_once(BITGET_WS_TOPIC_DELIMITER) {
                Some((interval, symbol)) => (Some(Ustr::from(interval)), symbol),
                None => (None, rest),
            }
        } else {
            (None, rest)
        };

        Self {
            inst_type: Ustr::from(inst_type),
            topic: Ustr::from(channel),
            symbol: (!symbol.is_empty()).then(|| Ustr::from(symbol)),
            interval,
        }
    }

    /// Returns the dotted topic key tracked by the subscription state.
    #[must_use]
    pub fn to_topic(&self) -> String {
        let d = BITGET_WS_TOPIC_DELIMITER;
        if self
            .inst_type
            .as_str()
            .eq_ignore_ascii_case(BITGET_WS_PRIVATE_INST_TYPE)
        {
            return self.topic.to_string();
        }

        match (self.interval, self.symbol) {
            (Some(interval), Some(symbol)) => format!("{}{d}{interval}{d}{symbol}", self.topic),
            (None, Some(symbol)) => format!("{}{d}{symbol}", self.topic),
            _ => self.topic.to_string(),
        }
    }
}

/// Returns the private `instType` for private channel names, otherwise the public one.
fn inst_type_for_topic(topic: &str) -> &'static str {
    let channel = topic
        .split_once(BITGET_WS_TOPIC_DELIMITER)
        .map_or(topic, |(channel, _)| channel);
    if channel.parse::<BitgetWsPrivateChannel>().is_ok() {
        BITGET_WS_PRIVATE_INST_TYPE
    } else {
        BitgetProductType::Linear.as_ws_inst_type()
    }
}

/// Serde adapter storing a push/ack `arg` object as its dotted topic key.
///
/// Serializing infers the `instType` from the channel name (public keys serialize as
/// `usdt-futures`); only test fixtures round-trip frames.
pub mod topic_arg {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use ustr::Ustr;

    use super::{BitgetWsArg, inst_type_for_topic};

    pub fn serialize<S: Serializer>(topic: &Ustr, s: S) -> Result<S::Ok, S::Error> {
        BitgetWsArg::from_topic(topic, inst_type_for_topic(topic)).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Ustr, D::Error> {
        Ok(Ustr::from(&BitgetWsArg::deserialize(d)?.to_topic()))
    }

    /// Optional variant for frames whose `arg` may be absent.
    pub mod option {
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        use super::super::{BitgetWsArg, inst_type_for_topic};

        pub fn serialize<S: Serializer>(topic: &Option<String>, s: S) -> Result<S::Ok, S::Error> {
            topic
                .as_deref()
                .map(|topic| BitgetWsArg::from_topic(topic, inst_type_for_topic(topic)))
                .serialize(s)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
            Ok(Option::<BitgetWsArg>::deserialize(d)?.map(|arg| arg.to_topic()))
        }
    }
}

/// Deserializes a string-or-number field into a `String` (Bitget encodes `code`/`id` both ways).
fn deserialize_string_or_number<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    Ok(match Option::<Value>::deserialize(d)? {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(other) => {
            return Err(D::Error::custom(format!(
                "expected string or number, received {other}"
            )));
        }
    })
}

/// Deserializes a string-or-number result code into `i64` (`0` when absent).
fn deserialize_code<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    deserialize_string_or_number(d)?.map_or(Ok(0), |code| {
        code.parse()
            .map_err(|e| D::Error::custom(format!("invalid code {code:?}: {e}")))
    })
}

fn deserialize_optional_code<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    deserialize_string_or_number(d)?
        .map(|code| {
            code.parse()
                .map_err(|e| D::Error::custom(format!("invalid code {code:?}: {e}")))
        })
        .transpose()
}

/// Deserializes Bitget `yes`/`no` (`YES`/`NO`, `true`/`false`) flags, case-insensitively.
pub(crate) fn deserialize_yes_no<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    let raw = Option::<String>::deserialize(d)?.unwrap_or_default();
    match raw.to_ascii_lowercase().as_str() {
        "yes" | "true" => Ok(true),
        "no" | "false" | "" => Ok(false),
        other => Err(D::Error::custom(format!(
            "expected yes/no, received {other:?}"
        ))),
    }
}

/// Serializes an optional reduce-only flag as Bitget's `YES`/`NO`.
fn serialize_yes_no<S: Serializer>(value: &Option<bool>, s: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(true) => s.serialize_str("YES"),
        Some(false) => s.serialize_str("NO"),
        None => s.serialize_none(),
    }
}

/// Serializes Bybit-style `isLeverage` (1 = borrow) as Bitget spot `autoBorrow` (`yes`/`no`).
fn serialize_auto_borrow<S: Serializer>(value: &Option<i32>, s: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(1) => s.serialize_str("yes"),
        Some(_) => s.serialize_str("no"),
        None => s.serialize_none(),
    }
}

/// Serializes a hedge-mode position index as Bitget `posSide` (one-way mode sends none).
fn serialize_pos_side<S: Serializer>(
    value: &Option<BitgetPositionIdx>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(BitgetPositionIdx::BuyHedge) => s.serialize_str("long"),
        Some(BitgetPositionIdx::SellHedge) => s.serialize_str("short"),
        _ => s.serialize_none(),
    }
}

fn deserialize_pos_side<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<BitgetPositionIdx>, D::Error> {
    Ok(match Option::<String>::deserialize(d)?.as_deref() {
        Some("long") => Some(BitgetPositionIdx::BuyHedge),
        Some("short") => Some(BitgetPositionIdx::SellHedge),
        _ => None,
    })
}

fn deserialize_optional_yes_no<'de, D: Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
    deserialize_yes_no(d).map(Some)
}

fn deserialize_auto_borrow<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i32>, D::Error> {
    deserialize_yes_no(d).map(|borrow| Some(i32::from(borrow)))
}

fn is_none_or_one_way(value: &Option<BitgetPositionIdx>) -> bool {
    !matches!(
        value,
        Some(BitgetPositionIdx::BuyHedge | BitgetPositionIdx::SellHedge)
    )
}

/// Deserializes a WS `category` (lowercase `usdt-futures`) or REST one (`USDT-FUTURES`).
pub(crate) fn deserialize_ws_category<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<BitgetProductType, D::Error> {
    let raw = String::deserialize(d)?;
    BitgetProductType::from_category(&raw)
        .ok_or_else(|| D::Error::custom(format!("unsupported Bitget category {raw:?}")))
}

/// Deserializes a push `data` array whose single element is the payload.
fn deserialize_first<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        Many(Vec<T>),
        One(T),
    }

    match OneOrMany::<T>::deserialize(d)? {
        OneOrMany::One(value) => Ok(value),
        OneOrMany::Many(values) => values
            .into_iter()
            .next()
            .ok_or_else(|| D::Error::custom("empty data array")),
    }
}

fn serialize_as_list<S: Serializer, T: Serialize>(value: &T, s: S) -> Result<S::Ok, S::Error> {
    [value].serialize(s)
}

/// Deserializes a push `data` that is a list, or a single object (`fast-fill`).
fn deserialize_one_or_many<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        Many(Vec<T>),
        One(T),
    }

    Ok(match OneOrMany::<T>::deserialize(d)? {
        OneOrMany::One(value) => vec![value],
        OneOrMany::Many(values) => values,
    })
}

/// Deserializes a `tradeScope` (`maker`/`taker`) as the maker flag.
fn deserialize_is_maker<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    Ok(String::deserialize(d)?.eq_ignore_ascii_case("maker"))
}

fn serialize_trade_scope<S: Serializer>(is_maker: &bool, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(if *is_maker { "maker" } else { "taker" })
}

/// Bitget WebSocket subscription message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitgetSubscription {
    pub op: BitgetWsOperation,
    pub args: Vec<BitgetWsArg>,
}

/// Bitget WebSocket authentication message.
#[derive(Clone, Serialize, Deserialize)]
pub struct BitgetAuthRequest {
    pub op: BitgetWsOperation,
    pub args: Vec<serde_json::Value>,
}

impl Debug for BitgetAuthRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BitgetAuthRequest))
            .field("op", &self.op)
            .field("args", &REDACTED)
            .finish()
    }
}

impl Zeroize for BitgetAuthRequest {
    fn zeroize(&mut self) {
        for arg in &mut self.args {
            zeroize_json_value(arg);
        }
    }
}

/// Wire-level frame deserialized from a Bitget WebSocket message.
///
/// Represents the raw protocol layer before the handler converts data
/// variants into the public [`BitgetWsMessage`] API.
#[derive(Debug, Clone)]
pub enum BitgetWsFrame {
    /// Authentication acknowledgement.
    Auth(BitgetWsAuthResponse),
    /// Subscription acknowledgement.
    Subscription(BitgetWsSubscriptionMsg),
    /// Order operation response (`op:"trade"`) from the private connection.
    OrderResponse(BitgetWsOrderResponse),
    /// Error response from the venue.
    ErrorResponse(BitgetWsResponse),
    /// Orderbook snapshot or delta.
    Orderbook(BitgetWsOrderbookDepthMsg),
    /// Trade updates.
    Trade(BitgetWsTradeMsg),
    /// Public liquidation updates.
    Liquidation(BitgetWsLiquidationMsg),
    /// Kline updates.
    Kline(BitgetWsKlineMsg),
    /// Linear/inverse ticker update.
    TickerLinear(BitgetWsTickerLinearMsg),
    /// Option ticker update (Bitget lists no options; never produced).
    TickerOption(BitgetWsTickerOptionMsg),
    /// Order updates from private channel.
    AccountOrder(BitgetWsAccountOrderMsg),
    /// Conditional (trigger/TP/SL) order updates from private channel.
    AccountStrategyOrder(BitgetWsAccountStrategyOrderMsg),
    /// Execution/fill updates from private channel.
    AccountExecution(BitgetWsAccountExecutionMsg),
    /// Fast execution updates from private channel (slim payload).
    AccountExecutionFast(BitgetWsAccountExecutionFastMsg),
    /// Wallet/balance updates from private channel.
    AccountWallet(BitgetWsAccountWalletMsg),
    /// Position updates from private channel.
    AccountPosition(BitgetWsAccountPositionMsg),
    /// Payload that does not match any known frame type.
    Unknown(Value),
    /// Notification that the underlying connection reconnected.
    Reconnected,
}

/// High-level message emitted by the Bitget WebSocket client.
#[derive(Debug, Clone)]
pub enum BitgetWsMessage {
    /// Authentication acknowledgement.
    Auth(BitgetWsAuthResponse),
    /// Order operation response (`op:"trade"`) from the private connection.
    OrderResponse(BitgetWsOrderResponse),
    /// Orderbook snapshot or delta.
    Orderbook(BitgetWsOrderbookDepthMsg),
    /// Trade updates.
    Trade(BitgetWsTradeMsg),
    /// Public liquidation updates.
    Liquidation(BitgetWsLiquidationMsg),
    /// Kline updates.
    Kline(BitgetWsKlineMsg),
    /// Linear/inverse ticker update.
    TickerLinear(BitgetWsTickerLinearMsg),
    /// Option ticker update (Bitget lists no options; never produced).
    TickerOption(BitgetWsTickerOptionMsg),
    /// Order updates from private channel.
    AccountOrder(BitgetWsAccountOrderMsg),
    /// Conditional (trigger/TP/SL) order updates from private channel.
    AccountStrategyOrder(BitgetWsAccountStrategyOrderMsg),
    /// Execution/fill updates from private channel.
    AccountExecution(BitgetWsAccountExecutionMsg),
    /// Fast execution updates from private channel (slim payload).
    AccountExecutionFast(BitgetWsAccountExecutionFastMsg),
    /// Wallet/balance updates from private channel.
    AccountWallet(BitgetWsAccountWalletMsg),
    /// Position updates from private channel.
    AccountPosition(BitgetWsAccountPositionMsg),
    /// Error received from the venue or client lifecycle.
    Error(BitgetWebSocketError),
    /// Notification that the underlying connection reconnected.
    Reconnected,
}

/// Represents an error event surfaced by the WebSocket client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWebSocketError {
    /// Error/return code reported by Bitget.
    pub code: i64,
    /// Human readable message.
    pub message: String,
    /// Optional connection identifier.
    #[serde(default)]
    pub conn_id: Option<String>,
    /// Optional topic associated with the error (when applicable).
    #[serde(default)]
    pub topic: Option<String>,
    /// Optional request identifier related to the failure.
    #[serde(default)]
    pub req_id: Option<String>,
}

impl BitgetWebSocketError {
    /// Creates a new error with the provided code/message.
    #[must_use]
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            conn_id: None,
            topic: None,
            req_id: None,
        }
    }

    /// Builds an error payload from a generic response frame.
    #[must_use]
    pub fn from_response(response: &BitgetWsResponse) -> Self {
        // Build a more informative error message when ret_msg is missing
        let message = response.ret_msg.clone().unwrap_or_else(|| {
            let mut parts = vec![];

            if let Some(op) = &response.op {
                parts.push(format!("op={op}"));
            }

            if let Some(topic) = &response.topic {
                parts.push(format!("topic={topic}"));
            }

            if let Some(success) = response.success {
                parts.push(format!("success={success}"));
            }

            if parts.is_empty() {
                "Bitget websocket error (no error message provided)".to_string()
            } else {
                format!("Bitget websocket error: {}", parts.join(", "))
            }
        });

        Self {
            code: response.ret_code.unwrap_or_default(),
            message,
            conn_id: response.conn_id.clone(),
            topic: response.topic.map(|t| t.to_string()),
            req_id: response.req_id.clone(),
        }
    }

    /// Convenience constructor for client-side errors (e.g. parsing failures).
    #[must_use]
    pub fn from_message(message: impl Into<String>) -> Self {
        Self::new(-1, message)
    }
}

/// Generic `op:"trade"` request for Bitget trading commands.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsRequest<T> {
    /// Request ID for correlation (echoed back in the response `id`).
    #[serde(rename = "id")]
    pub req_id: String,
    /// Operation type (always `trade`).
    pub op: BitgetWsOperation,
    /// Lowercase product category (`usdt-futures`).
    pub category: &'static str,
    /// Trade topic (`place-order`, `cancel-order`, ...).
    pub topic: BitgetWsOrderRequestOp,
    /// Envelope fields such as the client request time.
    #[serde(flatten)]
    pub header: BitgetWsHeader,
    /// Arguments payload for the operation.
    pub args: Vec<T>,
}

/// Envelope fields of WebSocket trade requests.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsHeader {
    /// Client request time in milliseconds; enables per-order `receiveWindow`.
    pub request_time: String,
    /// Optional API rebate code (`apiCode`).
    #[serde(rename = "apiCode", skip_serializing_if = "Option::is_none")]
    pub referer: Option<String>,
}

impl BitgetWsHeader {
    /// Creates a new header with the current timestamp.
    #[must_use]
    pub fn now() -> Self {
        Self::with_referer(None)
    }

    /// Creates a new header with the current timestamp and optional rebate code.
    #[must_use]
    pub fn with_referer(referer: Option<String>) -> Self {
        use nautilus_core::time::get_atomic_clock_realtime;
        Self {
            request_time: get_atomic_clock_realtime().get_time_ms().to_string(),
            referer,
        }
    }
}

/// Parameters for placing an order via WebSocket (`place-order` args item).
///
/// Bitget carries the category on the request envelope, posts reduce-only as `YES`/`NO`, and
/// takes no trigger fields here: conditional orders use the REST strategy-order endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsPlaceOrderParams {
    #[serde(skip, default)]
    pub category: BitgetProductType,
    pub symbol: Ustr,
    pub side: BitgetOrderSide,
    pub order_type: BitgetOrderType,
    pub qty: String,
    #[serde(
        rename = "autoBorrow",
        serialize_with = "serialize_auto_borrow",
        deserialize_with = "deserialize_auto_borrow",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub is_leverage: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<BitgetTimeInForce>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
    #[serde(
        serialize_with = "serialize_yes_no",
        deserialize_with = "deserialize_optional_yes_no",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub reduce_only: Option<bool>,
    #[serde(rename = "marginMode", skip_serializing_if = "Option::is_none")]
    pub margin_mode: Option<String>,
    #[serde(rename = "takeprofit", skip_serializing_if = "Option::is_none")]
    pub take_profit: Option<String>,
    #[serde(rename = "stoploss", skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_trigger_by: Option<BitgetTriggerType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_trigger_by: Option<BitgetTriggerType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_order_type: Option<BitgetOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_order_type: Option<BitgetOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_limit_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_limit_price: Option<String>,
    #[serde(rename = "stpMode", skip_serializing_if = "Option::is_none")]
    pub smp_type: Option<BitgetOrderSmpType>,
    #[serde(
        rename = "posSide",
        serialize_with = "serialize_pos_side",
        deserialize_with = "deserialize_pos_side",
        skip_serializing_if = "is_none_or_one_way",
        default
    )]
    pub position_idx: Option<BitgetPositionIdx>,
    /// Combined Bitget BBO level (`queue1`, `queue5`, `counterparty1`, `counterparty5`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbo_level: Option<String>,
}

/// Parameters for amending an order via WebSocket (`modify-order` args item).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAmendOrderParams {
    #[serde(skip, default)]
    pub category: BitgetProductType,
    pub symbol: Ustr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
}

/// Item in a batch amend request (without category field).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsBatchAmendItem {
    pub symbol: Ustr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
}

/// Arguments for batch amend order operation via WebSocket.
///
/// Bitget sends the items directly as `args`; the category goes on the envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitgetWsBatchAmendOrderArgs {
    pub category: BitgetProductType,
    pub request: Vec<BitgetWsBatchAmendItem>,
}

/// Parameters for canceling an order via WebSocket (`cancel-order` args item).
///
/// Bitget identifies the order by `orderId` or `clientOid` only; the symbol is kept for routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsCancelOrderParams {
    #[serde(skip, default)]
    pub category: BitgetProductType,
    #[serde(skip_serializing, default)]
    pub symbol: Ustr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
}

/// Item in a batch cancel request (without category field).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsBatchCancelItem {
    #[serde(skip_serializing, default)]
    pub symbol: Ustr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
}

/// Arguments for batch cancel order operation via WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitgetWsBatchCancelOrderArgs {
    pub category: BitgetProductType,
    pub request: Vec<BitgetWsBatchCancelItem>,
}

/// Item in a batch place request (same as [`BitgetWsPlaceOrderParams`] without category).
///
/// Bitget `batch-place` does not accept reduce-only orders.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsBatchPlaceItem {
    pub symbol: Ustr,
    pub side: BitgetOrderSide,
    pub order_type: BitgetOrderType,
    pub qty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<BitgetTimeInForce>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
    #[serde(rename = "takeprofit", skip_serializing_if = "Option::is_none")]
    pub take_profit: Option<String>,
    #[serde(rename = "stoploss", skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_trigger_by: Option<BitgetTriggerType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_trigger_by: Option<BitgetTriggerType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_order_type: Option<BitgetOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_order_type: Option<BitgetOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_limit_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_limit_price: Option<String>,
    #[serde(rename = "stpMode", skip_serializing_if = "Option::is_none")]
    pub smp_type: Option<BitgetOrderSmpType>,
    #[serde(
        rename = "posSide",
        serialize_with = "serialize_pos_side",
        deserialize_with = "deserialize_pos_side",
        skip_serializing_if = "is_none_or_one_way",
        default
    )]
    pub position_idx: Option<BitgetPositionIdx>,
}

/// Arguments for batch place order operation via WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitgetWsBatchPlaceOrderArgs {
    pub category: BitgetProductType,
    pub request: Vec<BitgetWsBatchPlaceItem>,
}

const fn default_true() -> bool {
    true
}

/// Subscription acknowledgement returned by Bitget (`event` `subscribe`/`unsubscribe`).
///
/// Bitget acks echo the channel `arg` instead of a request id; `req_id` carries its topic key.
/// Failures arrive as `event:"error"` frames, so an ack is always a success.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsSubscriptionMsg {
    #[serde(default = "default_true", skip_serializing)]
    pub success: bool,
    #[serde(rename = "event")]
    pub op: BitgetWsOperation,
    #[serde(default)]
    pub conn_id: Option<String>,
    #[serde(rename = "arg", default, with = "topic_arg::option")]
    pub req_id: Option<String>,
    #[serde(rename = "msg", default)]
    pub ret_msg: Option<String>,
}

/// Generic `event:"error"` frame returned by the endpoint.
///
/// `op` and `req_id` are recovered by the frame parser: subscription failures echo the rejected
/// `arg` inside `msg`, login failures carry login error codes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsResponse {
    #[serde(default, skip)]
    pub op: Option<BitgetWsOperation>,
    #[serde(default)]
    pub topic: Option<Ustr>,
    #[serde(default)]
    pub success: Option<bool>,
    #[serde(default)]
    pub conn_id: Option<String>,
    #[serde(rename = "arg", default, with = "topic_arg::option")]
    pub req_id: Option<String>,
    #[serde(
        rename = "code",
        default,
        deserialize_with = "deserialize_optional_code"
    )]
    pub ret_code: Option<i64>,
    #[serde(rename = "msg", default)]
    pub ret_msg: Option<String>,
}

/// Remaining quota reported on WebSocket trade responses.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BitgetWsRateLimit {
    /// Quota of this dimension.
    #[serde(default)]
    pub limit: String,
    /// Remaining requests.
    #[serde(default)]
    pub remaining: String,
}

/// Order operation response (`event:"trade"`, or `event:"error"` with an `id`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsOrderResponse {
    /// Trade topic (`place-order`, `cancel-order`, ...); empty on request-level errors.
    #[serde(rename = "topic", default)]
    pub op: Ustr,
    /// Connection ID.
    #[serde(default)]
    pub conn_id: Option<String>,
    /// Return code (`"0"` = success, non-zero = error).
    #[serde(rename = "code", default, deserialize_with = "deserialize_code")]
    pub ret_code: i64,
    /// Return message.
    #[serde(rename = "msg", default)]
    pub ret_msg: String,
    /// Per-order results (`args`): one object per order with `orderId`, `clientOid` and, for
    /// batches, a per-item `code`/`msg`.
    #[serde(rename = "args", default)]
    pub data: Value,
    /// Request ID for correlation (echoed `id`).
    #[serde(
        rename = "id",
        default,
        deserialize_with = "deserialize_string_or_number"
    )]
    pub req_id: Option<String>,
    /// Remaining rate-limit quota (`rateLimit`).
    #[serde(rename = "rateLimit", default)]
    pub header: Option<Vec<BitgetWsRateLimit>>,
}

impl BitgetWsOrderResponse {
    /// Extracts individual order errors from the per-item `code`/`msg` of batch operations.
    ///
    /// For batch operations, even when the outer code is 0, individual orders may fail.
    #[must_use]
    pub fn extract_batch_errors(&self) -> Vec<BitgetBatchOrderError> {
        self.data
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| {
                        let code = match item.get("code")? {
                            Value::String(code) => code.parse().ok()?,
                            Value::Number(code) => code.as_i64()?,
                            _ => return None,
                        };
                        let msg = item
                            .get("msg")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        Some(BitgetBatchOrderError { code, msg })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Error information for individual orders in a batch operation.
#[derive(Clone, Debug)]
pub struct BitgetBatchOrderError {
    /// Error code (0 = success, non-zero = error).
    pub code: i64,
    /// Error message.
    pub msg: String,
}

/// Authentication acknowledgement for private channels (`event:"login"`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAuthResponse {
    #[serde(rename = "event")]
    pub op: BitgetWsOperation,
    #[serde(default)]
    pub conn_id: Option<String>,
    #[serde(
        rename = "code",
        default,
        deserialize_with = "deserialize_optional_code"
    )]
    pub ret_code: Option<i64>,
    #[serde(rename = "msg", default)]
    pub ret_msg: Option<String>,
    #[serde(default)]
    pub success: Option<bool>,
}

/// Representation of a kline/candlestick event on the public stream.
///
/// Bitget pushes the open candle about once a second without a close flag; a candle is final
/// once a push with a later `start` arrives.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsKline {
    #[serde(deserialize_with = "deserialize_i64_or_string")]
    pub start: i64,
    pub open: String,
    pub close: String,
    pub high: String,
    pub low: String,
    #[serde(default)]
    pub volume: String,
    #[serde(default)]
    pub turnover: String,
}

/// Envelope for kline updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsKlineMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    pub ts: i64,
    #[serde(rename = "action")]
    pub msg_type: Ustr,
    pub data: Vec<BitgetWsKline>,
}

/// Orderbook depth payload consisting of raw ladder levels.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BitgetWsOrderbookDepth {
    /// Symbol (taken from the frame `arg`; Bitget omits it from `data`).
    #[serde(default, skip_serializing)]
    pub s: Ustr,
    /// Bid levels represented as `[price, size]` string pairs.
    pub b: Vec<Vec<String>>,
    /// Ask levels represented as `[price, size]` string pairs.
    pub a: Vec<Vec<String>>,
    /// Sequence number of this frame.
    #[serde(deserialize_with = "deserialize_i64_or_string")]
    pub seq: i64,
    /// Sequence number of the previous frame (`0` on snapshots and after a reset).
    #[serde(default, deserialize_with = "deserialize_i64_or_string")]
    pub pseq: i64,
    /// Matching engine timestamp in milliseconds.
    #[serde(default)]
    pub ts: String,
}

/// Envelope for orderbook depth snapshots and updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsOrderbookDepthMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "action")]
    pub msg_type: Ustr,
    pub ts: i64,
    #[serde(
        deserialize_with = "deserialize_first",
        serialize_with = "serialize_as_list"
    )]
    pub data: BitgetWsOrderbookDepth,
}

/// Linear/Inverse ticker event payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsTickerLinear {
    /// Symbol (taken from the frame `arg`; Bitget omits it from `data`).
    #[serde(default, skip_serializing)]
    pub symbol: Ustr,
    #[serde(default)]
    pub price24h_pcnt: Option<String>,
    #[serde(default)]
    pub last_price: Option<String>,
    #[serde(default)]
    pub open_price24h: Option<String>,
    #[serde(default)]
    pub high_price24h: Option<String>,
    #[serde(default)]
    pub low_price24h: Option<String>,
    #[serde(default)]
    pub mark_price: Option<String>,
    #[serde(default)]
    pub index_price: Option<String>,
    #[serde(default)]
    pub open_interest: Option<String>,
    #[serde(default)]
    pub turnover24h: Option<String>,
    #[serde(default)]
    pub volume24h: Option<String>,
    #[serde(default)]
    pub next_funding_time: Option<String>,
    #[serde(default)]
    pub funding_rate: Option<String>,
    #[serde(default)]
    pub bid1_price: Option<String>,
    #[serde(default)]
    pub bid1_size: Option<String>,
    #[serde(default)]
    pub ask1_price: Option<String>,
    #[serde(default)]
    pub ask1_size: Option<String>,
    /// Funding interval in hours; not in the Bitget ticker (instrument `fundInterval`).
    #[serde(default)]
    pub funding_interval_hour: Option<String>,
    #[serde(default)]
    pub delivery_time: Option<String>,
    #[serde(default)]
    pub delivery_status: Option<String>,
}

/// Envelope for linear ticker updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsTickerLinearMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "action")]
    pub msg_type: Ustr,
    pub ts: i64,
    #[serde(
        deserialize_with = "deserialize_first",
        serialize_with = "serialize_as_list"
    )]
    pub data: BitgetWsTickerLinear,
}

/// Option ticker event payload.
///
/// Bitget lists no options; the type is kept for API parity and never produced.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsTickerOption {
    pub symbol: Ustr,
    pub bid_price: String,
    pub bid_size: String,
    pub bid_iv: String,
    pub ask_price: String,
    pub ask_size: String,
    pub ask_iv: String,
    pub last_price: String,
    pub high_price24h: String,
    pub low_price24h: String,
    pub mark_price: String,
    pub index_price: String,
    pub mark_price_iv: String,
    pub underlying_price: String,
    pub open_interest: String,
    pub turnover24h: String,
    pub volume24h: String,
    pub total_volume: String,
    pub total_turnover: String,
    pub delta: String,
    pub gamma: String,
    pub vega: String,
    pub theta: String,
    pub predicted_delivery_price: String,
    pub change24h: String,
}

/// Envelope for option ticker updates (never produced on Bitget).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsTickerOptionMsg {
    #[serde(default)]
    pub id: Option<String>,
    pub topic: Ustr,
    #[serde(rename = "type")]
    pub msg_type: Ustr,
    pub ts: i64,
    pub data: BitgetWsTickerOption,
}

/// Trade event payload containing trade executions on public feeds (`publicTrade`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BitgetWsTrade {
    #[serde(rename = "T", deserialize_with = "deserialize_i64_or_string")]
    pub t: i64,
    /// Symbol (taken from the frame `arg`; Bitget omits it from `data`).
    #[serde(rename = "s", default, skip_serializing)]
    pub s: Ustr,
    #[serde(rename = "S")]
    pub taker_side: BitgetOrderSide,
    #[serde(rename = "v")]
    pub v: String,
    #[serde(rename = "p")]
    pub p: String,
    #[serde(rename = "i")]
    pub i: String,
    #[serde(rename = "L")]
    #[serde(default)]
    pub l: Option<String>,
    /// Retail price improvement flag (`yes`/`no`).
    #[serde(rename = "isRPI", default)]
    pub is_rpi: Option<String>,
}

/// Envelope for public trade updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsTradeMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "action")]
    pub msg_type: Ustr,
    pub ts: i64,
    pub data: Vec<BitgetWsTrade>,
}

/// Liquidation entry from the public `liquidation` stream.
///
/// `side` is the side of the liquidated position, where `buy` means a long position was
/// liquidated.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BitgetWsLiquidation {
    #[serde(rename = "ts", deserialize_with = "deserialize_i64_or_string")]
    pub t: i64,
    #[serde(rename = "symbol")]
    pub s: Ustr,
    #[serde(rename = "side")]
    pub side: BitgetOrderSide,
    #[serde(rename = "amount")]
    pub v: String,
    #[serde(rename = "price")]
    pub p: String,
}

/// Envelope for public liquidation updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsLiquidationMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "action")]
    pub msg_type: Ustr,
    pub ts: i64,
    pub data: Vec<BitgetWsLiquidation>,
}

/// Fee entry of `feeDetail` lists on order and fill pushes.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsFeeDetail {
    #[serde(default)]
    pub fee_coin: Option<Ustr>,
    #[serde(default)]
    pub fee: Option<String>,
}

/// Sums `feeDetail` fees (positive = cost) and returns them with the first fee coin.
///
/// # Errors
///
/// Returns an error if a fee is not a decimal.
pub fn sum_fee_detail(fees: &[BitgetWsFeeDetail]) -> anyhow::Result<(Decimal, Option<Ustr>)> {
    let mut total = Decimal::ZERO;
    let mut coin = None;

    for detail in fees {
        if let Some(fee) = detail.fee.as_deref().filter(|fee| !fee.is_empty()) {
            total += fee
                .parse::<Decimal>()
                .map_err(|e| anyhow::anyhow!("invalid feeDetail fee {fee:?}: {e}"))?;
        }

        if coin.is_none() {
            coin = detail.fee_coin.filter(|c| !c.is_empty());
        }
    }

    Ok((total, coin))
}

/// Returns the Bybit-style position index for a Bitget `holdMode`/`holdSide` pair.
fn hold_position_idx(hold_mode: &str, hold_side: &str) -> i32 {
    match (hold_mode, hold_side) {
        ("hedge_mode", "long") => 1,
        ("hedge_mode", "short") => 2,
        _ => 0,
    }
}

/// Private order stream payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountOrder {
    #[serde(deserialize_with = "deserialize_ws_category")]
    pub category: BitgetProductType,
    pub symbol: Ustr,
    pub order_id: Ustr,
    pub side: BitgetOrderSide,
    pub order_type: BitgetOrderType,
    pub price: String,
    pub qty: String,
    #[serde(default)]
    pub amount: String,
    pub time_in_force: BitgetTimeInForce,
    pub order_status: BitgetOrderStatus,
    #[serde(rename = "clientOid", default)]
    pub order_link_id: Ustr,
    #[serde(deserialize_with = "deserialize_yes_no", default)]
    pub reduce_only: bool,
    pub cum_exec_qty: String,
    pub cum_exec_value: String,
    pub avg_price: String,
    #[serde(default)]
    pub hold_mode: Ustr,
    #[serde(default)]
    pub hold_side: Ustr,
    #[serde(default)]
    pub trade_side: Ustr,
    #[serde(default)]
    pub margin_mode: Ustr,
    #[serde(default)]
    pub margin_coin: Ustr,
    #[serde(default)]
    pub leverage: String,
    #[serde(default)]
    pub fee_detail: Vec<BitgetWsFeeDetail>,
    pub created_time: String,
    pub updated_time: String,
    /// Cancel reason (`post_only_fill_cancel`, `normal_cancel`, ...); empty unless cancelled.
    #[serde(rename = "cancelReason", default)]
    pub reject_reason: Ustr,
    #[serde(rename = "takeprofit", default)]
    pub take_profit: String,
    #[serde(rename = "stoploss", default)]
    pub stop_loss: String,
    #[serde(default)]
    pub tp_trigger_by: Ustr,
    #[serde(default)]
    pub sl_trigger_by: Ustr,
    #[serde(default)]
    pub tp_limit_price: String,
    #[serde(default)]
    pub sl_limit_price: String,
    #[serde(rename = "stpMode", default)]
    pub smp_type: Ustr,
    #[serde(default)]
    pub delegate_type: Ustr,
}

impl BitgetWsAccountOrder {
    /// Returns the Bybit-style position index (0 one-way, 1/2 hedge long/short).
    #[must_use]
    pub fn position_idx(&self) -> i32 {
        hold_position_idx(self.hold_mode.as_str(), self.hold_side.as_str())
    }

    /// Returns the cumulative fee as the signed sum of `feeDetail`.
    ///
    /// # Errors
    ///
    /// Returns an error if a fee is not a decimal.
    pub fn cum_exec_fee(&self) -> anyhow::Result<Decimal> {
        Ok(sum_fee_detail(&self.fee_detail)?.0)
    }
}

/// Envelope for account order updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountOrderMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "ts")]
    pub creation_time: i64,
    pub data: Vec<BitgetWsAccountOrder>,
}

/// Private conditional order payload (`strategy-order` topic: trigger and TP/SL orders).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountStrategyOrder {
    /// Strategy type (`trigger`, `tpsl`, `trailing_stop`, ...).
    #[serde(rename = "type")]
    pub strategy_type: Ustr,
    #[serde(deserialize_with = "deserialize_ws_category")]
    pub category: BitgetProductType,
    pub symbol: Ustr,
    pub order_id: Ustr,
    #[serde(rename = "clientOid", default)]
    pub order_link_id: Ustr,
    pub qty: String,
    pub side: BitgetOrderSide,
    #[serde(default)]
    pub pos_side: Ustr,
    #[serde(deserialize_with = "deserialize_yes_no", default)]
    pub reduce_only: bool,
    /// Strategy status (`pending`, `submitting`, `success`, `failed`, `cancelled`).
    pub status: BitgetOrderStatus,
    pub created_time: String,
    pub updated_time: String,
    /// Trigger price source (`market` last price, `mark`).
    #[serde(default)]
    pub trigger_by: Ustr,
    #[serde(default)]
    pub trigger_price: String,
    /// Order type placed on trigger (`limit`, `market`).
    #[serde(default)]
    pub trigger_order_type: Ustr,
    #[serde(default)]
    pub trigger_order_price: String,
    #[serde(default)]
    pub take_profit: String,
    #[serde(default)]
    pub stop_loss: String,
    #[serde(default)]
    pub tp_trigger_by: Ustr,
    #[serde(default)]
    pub sl_trigger_by: Ustr,
    #[serde(default)]
    pub tp_order_type: Ustr,
    #[serde(default)]
    pub sl_order_type: Ustr,
    #[serde(default)]
    pub tp_limit_price: String,
    #[serde(default)]
    pub sl_limit_price: String,
}

/// Envelope for conditional order updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountStrategyOrderMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "ts")]
    pub creation_time: i64,
    pub data: Vec<BitgetWsAccountStrategyOrder>,
}

/// Private execution (`fill` topic) payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountExecution {
    #[serde(deserialize_with = "deserialize_ws_category")]
    pub category: BitgetProductType,
    pub symbol: Ustr,
    pub exec_id: String,
    #[serde(default)]
    pub exec_link_id: String,
    pub exec_price: String,
    pub exec_qty: String,
    pub exec_value: String,
    #[serde(default)]
    pub exec_pnl: String,
    #[serde(
        rename = "tradeScope",
        deserialize_with = "deserialize_is_maker",
        serialize_with = "serialize_trade_scope"
    )]
    pub is_maker: bool,
    #[serde(default)]
    pub fee_detail: Vec<BitgetWsFeeDetail>,
    pub order_id: Ustr,
    #[serde(rename = "clientOid", default)]
    pub order_link_id: Ustr,
    pub order_type: BitgetOrderType,
    pub side: BitgetOrderSide,
    #[serde(default)]
    pub hold_side: Ustr,
    /// Trade side (`buy_single`, `reduce_sell_single`, `burst_*` liquidation, `dte_sys_adl_*`).
    #[serde(default)]
    pub trade_side: Ustr,
    pub exec_time: String,
    #[serde(default)]
    pub updated_time: String,
    #[serde(rename = "isRPI", default)]
    pub is_rpi: Option<String>,
}

impl BitgetWsAccountExecution {
    /// Returns the fee as the signed sum of `feeDetail` and its coin.
    ///
    /// # Errors
    ///
    /// Returns an error if a fee is not a decimal.
    pub fn exec_fee(&self) -> anyhow::Result<(Decimal, Option<Ustr>)> {
        sum_fee_detail(&self.fee_detail)
    }

    /// Returns the execution type derived from `tradeSide` (liquidation, ADL, delivery, trade).
    #[must_use]
    pub fn exec_type(&self) -> BitgetExecType {
        BitgetExecType::from_trade_side(self.trade_side.as_str())
    }
}

/// Envelope for account execution updates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountExecutionMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "ts")]
    pub creation_time: i64,
    pub data: Vec<BitgetWsAccountExecution>,
}

/// Slim execution payload delivered on the `fast-fill` private channel.
///
/// The fast stream omits fee, value and trade-side fields that the standard `fill`
/// channel provides; subscribe to both if you need full fill data.
///
/// # References
/// - <https://www.bitget.com/docs/uta/websocket/private/Fast-Fill-Channel>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountExecutionFast {
    #[serde(deserialize_with = "deserialize_ws_category")]
    pub category: BitgetProductType,
    pub symbol: Ustr,
    pub exec_id: String,
    pub exec_price: String,
    pub exec_qty: String,
    pub order_id: Ustr,
    #[serde(rename = "clientOid", default)]
    pub order_link_id: Ustr,
    pub side: BitgetOrderSide,
    pub exec_time: String,
    #[serde(
        rename = "tradeScope",
        deserialize_with = "deserialize_is_maker",
        serialize_with = "serialize_trade_scope"
    )]
    pub is_maker: bool,
}

/// Envelope for account fast-execution updates.
///
/// The fast stream pushes one object as `data`, which is read as a one-item list.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountExecutionFastMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "ts")]
    pub creation_time: i64,
    #[serde(deserialize_with = "deserialize_one_or_many")]
    pub data: Vec<BitgetWsAccountExecutionFast>,
}

/// Coin level wallet update payload on private streams (`account` topic).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountWalletCoin {
    pub coin: Ustr,
    #[serde(default)]
    pub balance: String,
    #[serde(default)]
    pub balance_original: String,
    #[serde(default)]
    pub locked: String,
    pub equity: String,
    #[serde(default)]
    pub usd_value: String,
    pub available: String,
    #[serde(default)]
    pub borrow: String,
    #[serde(default)]
    pub debts: String,
    #[serde(default)]
    pub bonus: String,
}

/// Wallet summary payload covering all coins.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountWallet {
    pub total_equity: String,
    #[serde(default)]
    pub eff_equity: String,
    #[serde(default)]
    pub mmr: String,
    #[serde(default)]
    pub imr: String,
    #[serde(default)]
    pub mgn_ratio: String,
    #[serde(default)]
    pub position_mgn_ratio: String,
    #[serde(rename = "unrealisedPnL", default)]
    pub unrealised_pnl: String,
    pub coin: Vec<BitgetWsAccountWalletCoin>,
}

/// Envelope for wallet updates on private streams.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountWalletMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "ts")]
    pub creation_time: i64,
    pub data: Vec<BitgetWsAccountWallet>,
}

/// Position data from private position stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountPosition {
    pub symbol: Ustr,
    #[serde(default)]
    pub margin_coin: Ustr,
    #[serde(default)]
    pub margin_size: String,
    #[serde(default)]
    pub margin_mode: Ustr,
    #[serde(default)]
    pub hold_mode: Ustr,
    /// Position direction (`long`/`short`), also set in one-way mode.
    pub pos_side: Ustr,
    pub size: String,
    #[serde(default)]
    pub available: String,
    #[serde(default)]
    pub frozen: String,
    #[serde(default)]
    pub avg_price: String,
    #[serde(default)]
    pub leverage: String,
    #[serde(default)]
    pub cur_realised_pnl: String,
    #[serde(default)]
    pub unrealised_pnl: String,
    #[serde(default)]
    pub liq_price: String,
    #[serde(default)]
    pub mmr: String,
    #[serde(default)]
    pub mark_price: String,
    #[serde(default)]
    pub break_even_price: String,
    #[serde(default)]
    pub total_funding_fee: String,
    #[serde(default)]
    pub open_fee_total: String,
    #[serde(default)]
    pub close_fee_total: String,
    /// `opening` while the position is held, `ended` once closed.
    #[serde(default)]
    pub position_status: Ustr,
    pub created_time: String,
    pub updated_time: String,
}

impl BitgetWsAccountPosition {
    /// Returns the product type: positions carry no category, so it follows the margin coin.
    #[must_use]
    pub fn category(&self) -> BitgetProductType {
        match self.margin_coin.as_str() {
            "USDT" | "USDC" | "" => BitgetProductType::Linear,
            _ => BitgetProductType::Inverse,
        }
    }

    /// Returns the Bybit-style position index (0 one-way, 1/2 hedge long/short).
    #[must_use]
    pub fn position_idx(&self) -> i32 {
        hold_position_idx(self.hold_mode.as_str(), self.pos_side.as_str())
    }

    /// Returns the average entry price, or `None` when flat or empty.
    #[must_use]
    pub fn entry_price(&self) -> Option<Decimal> {
        self.avg_price
            .parse::<Decimal>()
            .ok()
            .filter(|price| !price.is_zero())
    }
}

/// Envelope for position updates on private streams.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountPositionMsg {
    #[serde(rename = "arg", with = "topic_arg")]
    pub topic: Ustr,
    #[serde(rename = "ts")]
    pub creation_time: i64,
    pub data: Vec<BitgetWsAccountPosition>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::common::testing::load_test_json;

    fn place_params() -> BitgetWsPlaceOrderParams {
        BitgetWsPlaceOrderParams {
            category: BitgetProductType::Linear,
            symbol: Ustr::from("BTCUSDT"),
            side: BitgetOrderSide::Buy,
            order_type: BitgetOrderType::Limit,
            qty: "0.01".to_string(),
            is_leverage: None,
            price: Some("50000".to_string()),
            time_in_force: Some(BitgetTimeInForce::Gtc),
            order_link_id: None,
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
        }
    }

    #[rstest]
    fn auth_request_serializes_and_redacts_debug() {
        let request = BitgetAuthRequest {
            op: BitgetWsOperation::Auth,
            args: vec![serde_json::json!({
                "apiKey": "api-key-value",
                "passphrase": "passphrase-value",
                "timestamp": "1700000000000",
                "sign": "signature-value",
            })],
        };

        let json = serde_json::to_value(&request).unwrap();
        let formatted = format!("{request:?}");

        assert_eq!(json["op"], "login");
        assert_eq!(json["args"][0]["apiKey"], "api-key-value");
        assert_eq!(json["args"][0]["timestamp"], "1700000000000");
        assert_eq!(json["args"][0]["sign"], "signature-value");
        assert!(formatted.contains(REDACTED));
        assert!(!formatted.contains("api-key-value"));
        assert!(!formatted.contains("signature-value"));
    }

    #[rstest]
    #[case(
        "books5.BTCUSDT",
        "usdt-futures",
        r#"{"instType":"usdt-futures","topic":"books5","symbol":"BTCUSDT"}"#
    )]
    #[case(
        "kline.1m.BTCUSDT",
        "usdt-futures",
        r#"{"instType":"usdt-futures","topic":"kline","symbol":"BTCUSDT","interval":"1m"}"#
    )]
    #[case("order", "UTA", r#"{"instType":"UTA","topic":"order"}"#)]
    #[case(
        "fast-fill",
        "UTA",
        r#"{"instType":"UTA","topic":"fast-fill","symbol":"default"}"#
    )]
    fn ws_arg_topic_round_trip(#[case] topic: &str, #[case] inst_type: &str, #[case] json: &str) {
        let arg = BitgetWsArg::from_topic(topic, inst_type);

        assert_eq!(serde_json::to_string(&arg).unwrap(), json);
        assert_eq!(arg.to_topic(), topic);
    }

    #[rstest]
    fn deserialize_account_execution_fast_msg() {
        // Bitget pushes a single object as `data` on the fast channel
        let json = load_test_json("ws_account_execution_fast.json");
        let msg: BitgetWsAccountExecutionFastMsg = serde_json::from_str(&json).unwrap();

        assert_eq!(msg.topic, "fast-fill");
        assert_eq!(msg.creation_time, 1_794_193_099_834);
        assert_eq!(msg.data.len(), 1);
        let exec = &msg.data[0];
        assert_eq!(exec.category, BitgetProductType::Linear);
        assert_eq!(exec.symbol, Ustr::from("TSLAUSDT"));
        assert_eq!(exec.exec_id, "1503520323400871936");
        assert_eq!(exec.exec_price, "360.72");
        assert_eq!(exec.exec_qty, "0.02");
        assert_eq!(exec.order_id, Ustr::from("1503520323393708032"));
        assert_eq!(exec.order_link_id, Ustr::from("tdcapioc93099"));
        assert_eq!(exec.side, BitgetOrderSide::Buy);
        assert!(!exec.is_maker);
        assert_eq!(exec.exec_time, "1794193099832");
    }

    #[rstest]
    fn deserialize_account_execution_fast_msg_accepts_list_data() {
        let mut json: Value =
            serde_json::from_str(&load_test_json("ws_account_execution_fast.json")).unwrap();
        json["data"] = Value::Array(vec![json["data"].clone()]);

        let msg: BitgetWsAccountExecutionFastMsg = serde_json::from_value(json).unwrap();

        assert_eq!(msg.data.len(), 1);
    }

    #[rstest]
    fn deserialize_account_execution_sums_fee_detail() {
        let json = load_test_json("ws_account_execution.json");
        let msg: BitgetWsAccountExecutionMsg = serde_json::from_str(&json).unwrap();
        let exec = &msg.data[0];

        assert_eq!(msg.topic, "fill");
        assert!(!exec.is_maker);
        assert_eq!(exec.exec_type(), BitgetExecType::Trade);
        assert_eq!(
            exec.exec_fee().unwrap(),
            (dec!(0.00288576), Some(Ustr::from("USDT")))
        );
    }

    #[rstest]
    #[case("ws_account_position.json", "long", 1)]
    #[case("ws_account_position_short.json", "short", 1)]
    #[case("ws_account_position_closed.json", "long", 1)]
    fn deserialize_account_position(
        #[case] fixture: &str,
        #[case] pos_side: &str,
        #[case] count: usize,
    ) {
        let msg: BitgetWsAccountPositionMsg =
            serde_json::from_str(&load_test_json(fixture)).unwrap();
        let position = &msg.data[0];

        assert_eq!(msg.data.len(), count);
        assert_eq!(position.pos_side, pos_side);
        assert_eq!(position.category(), BitgetProductType::Linear);
        assert_eq!(position.position_idx(), 0);
    }

    #[rstest]
    fn serialize_place_params_uses_bitget_fields() {
        let mut params = place_params();
        params.order_link_id = Some("test-1".to_string());
        params.reduce_only = Some(true);
        params.margin_mode = Some("crossed".to_string());

        let json: Value = serde_json::to_value(&params).unwrap();

        assert_eq!(json["clientOid"], "test-1");
        assert_eq!(json["reduceOnly"], "YES");
        assert_eq!(json["marginMode"], "crossed");
        assert_eq!(json["timeInForce"], "gtc");
        assert!(json.get("category").is_none());
    }

    #[rstest]
    fn serialize_place_params_omits_optional_fields_when_none() {
        let json = serde_json::to_string(&place_params()).unwrap();

        assert!(!json.contains("stpMode"));
        assert!(!json.contains("posSide"));
        assert!(!json.contains("reduceOnly"));
        assert!(!json.contains("autoBorrow"));
    }

    #[rstest]
    #[case(BitgetOrderSmpType::None, "none")]
    #[case(BitgetOrderSmpType::CancelMaker, "cancel_maker")]
    #[case(BitgetOrderSmpType::CancelTaker, "cancel_taker")]
    #[case(BitgetOrderSmpType::CancelBoth, "cancel_both")]
    fn serialize_place_params_includes_smp_type_when_set(
        #[case] smp_type: BitgetOrderSmpType,
        #[case] expected: &str,
    ) {
        let mut params = place_params();
        params.smp_type = Some(smp_type);

        let json: serde_json::Value = serde_json::to_value(&params).unwrap();

        assert_eq!(json.get("stpMode").and_then(Value::as_str), Some(expected));
    }

    #[rstest]
    fn serialize_place_params_includes_bbo_when_set() {
        let mut params = place_params();
        params.price = None;
        params.bbo_level = Some("queue5".to_string());

        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains("\"bboLevel\":\"queue5\""));
        assert!(!json.contains("\"price\""));
    }

    #[rstest]
    #[case(BitgetPositionIdx::BuyHedge, "long")]
    #[case(BitgetPositionIdx::SellHedge, "short")]
    fn serialize_place_params_includes_position_idx_when_set(
        #[case] idx: BitgetPositionIdx,
        #[case] expected: &str,
    ) {
        let mut params = place_params();
        params.position_idx = Some(idx);

        let json: Value = serde_json::to_value(&params).unwrap();
        assert_eq!(json["posSide"], expected);
    }

    // One-way mode sends no `posSide`, so it decodes as no position index
    #[rstest]
    #[case(None, None)]
    #[case(Some(BitgetPositionIdx::OneWay), None)]
    #[case(Some(BitgetPositionIdx::BuyHedge), Some(BitgetPositionIdx::BuyHedge))]
    #[case(Some(BitgetPositionIdx::SellHedge), Some(BitgetPositionIdx::SellHedge))]
    fn place_params_position_idx_roundtrip(
        #[case] idx: Option<BitgetPositionIdx>,
        #[case] expected: Option<BitgetPositionIdx>,
    ) {
        let mut params = place_params();
        params.position_idx = idx;

        let json = serde_json::to_string(&params).unwrap();
        let decoded: BitgetWsPlaceOrderParams = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.position_idx, expected);
    }

    #[rstest]
    fn deserialize_account_order_frame_uses_enums() {
        let json = load_test_json("ws_account_order.json");
        let frame: BitgetWsAccountOrderMsg = serde_json::from_str(&json).unwrap();
        let order = &frame.data[0];

        assert_eq!(frame.topic, "order");
        assert_eq!(order.category, BitgetProductType::Linear);
        assert_eq!(order.side, BitgetOrderSide::Buy);
        assert_eq!(order.order_type, BitgetOrderType::Limit);
        assert_eq!(order.time_in_force, BitgetTimeInForce::PostOnly);
        assert_eq!(order.order_status, BitgetOrderStatus::New);
        assert_eq!(order.order_link_id, "tdcaprest93086");
        assert!(!order.reduce_only);
        assert_eq!(order.position_idx(), 0);
        assert_eq!(order.cum_exec_fee().unwrap(), Decimal::ZERO);
    }

    #[rstest]
    #[case("no", false)]
    #[case("yes", true)]
    #[case("NO", false)]
    #[case("YES", true)]
    fn deserialize_account_order_frame_accepts_reduce_only_case(
        #[case] value: &str,
        #[case] expected: bool,
    ) {
        let mut json: Value =
            serde_json::from_str(&load_test_json("ws_account_order.json")).unwrap();
        json["data"][0]["reduceOnly"] = Value::String(value.to_string());

        let frame: BitgetWsAccountOrderMsg = serde_json::from_value(json).unwrap();

        assert_eq!(frame.data[0].reduce_only, expected);
    }

    #[rstest]
    fn deserialize_account_order_post_only_cancel() {
        let json = load_test_json("ws_account_order_post_only_rejected.json");
        let frame: BitgetWsAccountOrderMsg = serde_json::from_str(&json).unwrap();
        let order = &frame.data[0];

        assert_eq!(order.order_status, BitgetOrderStatus::Canceled);
        assert_eq!(order.reject_reason, "post_only_fill_cancel");
    }

    #[rstest]
    fn deserialize_account_strategy_order() {
        let json = load_test_json("ws_account_order_buy_stop_market.json");
        let frame: BitgetWsAccountStrategyOrderMsg = serde_json::from_str(&json).unwrap();
        let order = &frame.data[0];

        assert_eq!(frame.topic, "strategy-order");
        assert_eq!(order.strategy_type, "trigger");
        assert_eq!(order.status, BitgetOrderStatus::Untriggered);
        assert_eq!(order.trigger_price, "396.89");
        assert_eq!(order.trigger_by, "mark");
        assert!(!order.reduce_only);
    }

    // Bybit's per-order-type fixtures, each a Bitget `strategy-order` push
    #[rstest]
    #[case(
        "ws_account_order_stop_market.json",
        "trigger",
        BitgetOrderSide::Sell,
        false
    )]
    #[case(
        "ws_account_order_stop_limit.json",
        "trigger",
        BitgetOrderSide::Sell,
        false
    )]
    #[case(
        "ws_account_order_market_if_touched.json",
        "trigger",
        BitgetOrderSide::Buy,
        false
    )]
    #[case(
        "ws_account_order_sell_market_if_touched.json",
        "trigger",
        BitgetOrderSide::Sell,
        false
    )]
    #[case(
        "ws_account_order_limit_if_touched.json",
        "trigger",
        BitgetOrderSide::Buy,
        false
    )]
    #[case(
        "ws_account_order_take_profit.json",
        "tpsl",
        BitgetOrderSide::Sell,
        true
    )]
    #[case("ws_account_order_stop_loss.json", "tpsl", BitgetOrderSide::Sell, true)]
    fn deserialize_account_strategy_order_variants(
        #[case] fixture: &str,
        #[case] strategy_type: &str,
        #[case] side: BitgetOrderSide,
        #[case] reduce_only: bool,
    ) {
        let frame: BitgetWsAccountStrategyOrderMsg =
            serde_json::from_str(&load_test_json(fixture)).unwrap();
        let order = &frame.data[0];

        assert_eq!(frame.topic, "strategy-order");
        assert_eq!(order.strategy_type, strategy_type);
        assert_eq!(order.side, side);
        assert_eq!(order.reduce_only, reduce_only);
    }

    #[rstest]
    fn deserialize_account_execution_adl() {
        let msg: BitgetWsAccountExecutionMsg =
            serde_json::from_str(&load_test_json("ws_account_execution_adl.json")).unwrap();

        assert_eq!(msg.data[0].exec_type(), BitgetExecType::AdlTrade);
    }

    #[rstest]
    fn deserialize_account_execution_fast_envelope() {
        let msg: BitgetWsAccountExecutionFastMsg = serde_json::from_str(&load_test_json(
            "ws_account_execution_fast_envelope_id.json",
        ))
        .unwrap();

        assert_eq!(msg.data.len(), 1);
    }

    #[rstest]
    fn deserialize_account_position_with_open_time() {
        let msg: BitgetWsAccountPositionMsg =
            serde_json::from_str(&load_test_json("ws_account_position_with_open_time.json"))
                .unwrap();

        assert!(!msg.data[0].created_time.is_empty());
    }

    #[rstest]
    fn deserialize_account_wallet_locked_exceeds_total() {
        let frame: BitgetWsAccountWalletMsg = serde_json::from_str(&load_test_json(
            "ws_account_wallet_locked_exceeds_total.json",
        ))
        .unwrap();
        let usdt = &frame.data[0].coin[1];

        assert!(usdt.locked.parse::<f64>().unwrap() > usdt.balance.parse::<f64>().unwrap());
    }

    #[rstest]
    fn deserialize_account_wallet() {
        let json = load_test_json("ws_account_wallet.json");
        let frame: BitgetWsAccountWalletMsg = serde_json::from_str(&json).unwrap();

        assert_eq!(frame.topic, "account");
        assert_eq!(frame.data[0].coin.len(), 2);
        assert_eq!(frame.data[0].coin[1].coin, "USDT");
    }

    #[rstest]
    fn deserialize_orderbook_frames_fill_seq() {
        let snapshot: BitgetWsOrderbookDepthMsg =
            serde_json::from_str(&load_test_json("ws_orderbook_full_snapshot.json")).unwrap();
        let delta: BitgetWsOrderbookDepthMsg =
            serde_json::from_str(&load_test_json("ws_orderbook_delta.json")).unwrap();

        assert_eq!(snapshot.topic, "books.BTCUSDT");
        assert_eq!(snapshot.msg_type, "snapshot");
        assert_eq!(delta.msg_type, "update");
        assert_eq!(delta.data.pseq, snapshot.data.seq);
    }

    #[rstest]
    fn order_response_extracts_batch_errors() {
        let response: BitgetWsOrderResponse = serde_json::from_value(serde_json::json!({
            "event": "trade",
            "id": "batch-1",
            "topic": "batch-cancel",
            "args": [
                {"code": "0", "msg": "Success", "orderId": "1", "clientOid": "a"},
                {"code": "25204", "msg": "Order does not exist", "orderId": "2", "clientOid": "b"}
            ],
            "code": "0",
            "msg": "Success"
        }))
        .unwrap();

        let errors = response.extract_batch_errors();

        assert_eq!(response.ret_code, 0);
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[1].code, 25204);
        assert_eq!(errors[1].msg, "Order does not exist");
    }
}
