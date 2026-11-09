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

//! Enumerations for Bitget WebSocket operations and channels.

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};

/// WebSocket operation type.
#[derive(
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum BitgetWsOperation {
    /// Subscribe to topics.
    Subscribe,
    /// Unsubscribe from topics.
    Unsubscribe,
    /// Authenticate connection.
    #[serde(rename = "login")]
    #[strum(serialize = "login")]
    Auth,
    /// Ping message (Bitget sends the plain text `ping`, not a JSON frame).
    Ping,
    /// Pong message (Bitget answers with the plain text `pong`).
    Pong,
    /// Order entry request (`op:"trade"`) on the private connection.
    Trade,
}

/// Private authenticated channel types.
#[derive(
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum BitgetWsPrivateChannel {
    /// Order updates.
    Order,
    /// Execution/fill updates (Bitget topic `fill`).
    #[serde(rename = "fill")]
    #[strum(serialize = "fill")]
    Execution,
    /// Fast execution stream (slimmer payload, lower latency; Bitget topic `fast-fill`).
    #[serde(rename = "fast-fill")]
    #[strum(serialize = "fast-fill")]
    ExecutionFast,
    /// Position updates.
    Position,
    /// Wallet/balance updates (Bitget topic `account`).
    #[serde(rename = "account")]
    #[strum(serialize = "account")]
    Wallet,
    /// Conditional (trigger/TP/SL) order updates; Bitget pushes these apart from `order`.
    #[serde(rename = "strategy-order")]
    #[strum(serialize = "strategy-order")]
    StrategyOrder,
}

/// Public channel types.
#[derive(
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum BitgetWsPublicChannel {
    /// Order book updates (Bitget topics `books`, `books1`, `books5`, `books50`).
    #[serde(rename = "books")]
    #[strum(serialize = "books")]
    OrderBook,
    /// Public trades.
    #[serde(rename = "publicTrade")]
    #[strum(serialize = "publicTrade")]
    PublicTrade,
    /// Trade updates (no separate Bitget topic; trades arrive on `publicTrade`).
    Trade,
    /// Kline/candlestick updates.
    Kline,
    /// Ticker updates (Bitget topic `ticker`).
    #[serde(rename = "ticker")]
    #[strum(serialize = "ticker")]
    Tickers,
    /// Public liquidation updates (Bitget topic `liquidation`).
    #[serde(rename = "liquidation")]
    #[strum(serialize = "liquidation")]
    AllLiquidation,
    /// Funding rate updates (Bitget topic `funding-rate`).
    #[serde(rename = "funding-rate")]
    #[strum(serialize = "funding-rate")]
    FundingRate,
}
