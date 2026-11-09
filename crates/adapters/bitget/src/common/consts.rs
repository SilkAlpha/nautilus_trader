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

//! Core constants shared across the Bitget adapter components.

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

/// Venue identifier string.
pub const BITGET: &str = "BITGET";

/// Static venue instance.
pub static BITGET_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(BITGET)));

/// Static client ID instance.
pub static BITGET_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(BITGET)));

pub(crate) const BITGET_WS_TOPIC_DELIMITER: char = '.';

pub(crate) const BITGET_DEFAULT_ORDERBOOK_DEPTH: u32 = 50;
pub(crate) const BITGET_QUOTE_DEPTH: u32 = 1;

// `books1`, `books5`, `books50` and full-depth `books`; see
// <https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel>.
pub(crate) const BITGET_BOOK_DEPTHS: [u32; 4] = [1, 5, 50, 1000];

/// Text heartbeat sent every 30 seconds; the venue answers with `pong`.
pub(crate) const BITGET_WS_HEARTBEAT_PAYLOAD: &str = "ping";
pub(crate) const BITGET_WS_PONG_PAYLOAD: &str = "pong";

/// Path signed by the WebSocket login (`timestamp + "GET" + "/user/verify"`).
pub(crate) const BITGET_WS_LOGIN_PATH: &str = "/user/verify";

/// Header that routes REST requests to demo (paper) trading.
pub(crate) const BITGET_DEMO_HEADER: &str = "paptrading";
