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

//! Runtime resolution of Bitget REST and WebSocket base URLs.

use super::enums::{BitgetEnvironment, BitgetProductType};

const HOST_MAINNET: &str = "ws.bitget.com";
// Bitget has no testnet: demo (paper) trading is the only sandbox
const HOST_DEMO: &str = "wspap.bitget.com";

/// Returns the REST base URL; demo trading uses the same host with the `paptrading: 1` header.
#[must_use]
pub const fn bitget_http_base_url(environment: BitgetEnvironment) -> &'static str {
    match environment {
        BitgetEnvironment::Mainnet | BitgetEnvironment::Demo | BitgetEnvironment::Testnet => {
            "https://api.bitget.com"
        }
    }
}

fn ws_host(environment: BitgetEnvironment) -> &'static str {
    match environment {
        BitgetEnvironment::Mainnet => HOST_MAINNET,
        BitgetEnvironment::Demo | BitgetEnvironment::Testnet => HOST_DEMO,
    }
}

/// Returns the public WebSocket URL.
///
/// Bitget serves every product type on one public endpoint (selected per subscription by
/// `instType`), so `product_type` does not change the URL.
#[must_use]
pub fn bitget_ws_public_url(
    product_type: BitgetProductType,
    environment: BitgetEnvironment,
) -> String {
    let _ = product_type;
    format!("wss://{}/v3/ws/public", ws_host(environment))
}

/// Returns the private WebSocket URL.
#[must_use]
pub const fn bitget_ws_private_url(environment: BitgetEnvironment) -> &'static str {
    match environment {
        BitgetEnvironment::Mainnet => "wss://ws.bitget.com/v3/ws/private",
        BitgetEnvironment::Demo | BitgetEnvironment::Testnet => {
            "wss://wspap.bitget.com/v3/ws/private"
        }
    }
}

/// Returns the WebSocket URL used for order entry.
///
/// Bitget has no separate trade endpoint: `op:"trade"` requests go on a logged-in private socket.
#[must_use]
pub const fn bitget_ws_trade_url(environment: BitgetEnvironment) -> &'static str {
    bitget_ws_private_url(environment)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn resolves_public_urls() {
        assert_eq!(
            bitget_ws_public_url(BitgetProductType::Linear, BitgetEnvironment::Mainnet),
            "wss://ws.bitget.com/v3/ws/public"
        );
        assert_eq!(
            bitget_ws_public_url(BitgetProductType::Spot, BitgetEnvironment::Demo),
            "wss://wspap.bitget.com/v3/ws/public"
        );
        assert_eq!(
            bitget_ws_public_url(BitgetProductType::Inverse, BitgetEnvironment::Testnet),
            "wss://wspap.bitget.com/v3/ws/public"
        );
    }

    #[rstest]
    fn resolves_private_urls() {
        assert_eq!(
            bitget_ws_private_url(BitgetEnvironment::Mainnet),
            "wss://ws.bitget.com/v3/ws/private"
        );
        assert_eq!(
            bitget_ws_private_url(BitgetEnvironment::Demo),
            "wss://wspap.bitget.com/v3/ws/private"
        );
        assert_eq!(
            bitget_ws_private_url(BitgetEnvironment::Testnet),
            "wss://wspap.bitget.com/v3/ws/private"
        );
    }

    #[rstest]
    fn resolves_trade_urls() {
        assert_eq!(
            bitget_ws_trade_url(BitgetEnvironment::Mainnet),
            "wss://ws.bitget.com/v3/ws/private"
        );
        assert_eq!(
            bitget_ws_trade_url(BitgetEnvironment::Demo),
            "wss://wspap.bitget.com/v3/ws/private"
        );
        assert_eq!(
            bitget_ws_trade_url(BitgetEnvironment::Testnet),
            "wss://wspap.bitget.com/v3/ws/private"
        );
    }

    #[rstest]
    fn resolves_http_urls() {
        assert_eq!(
            bitget_http_base_url(BitgetEnvironment::Mainnet),
            "https://api.bitget.com"
        );
        assert_eq!(
            bitget_http_base_url(BitgetEnvironment::Demo),
            "https://api.bitget.com"
        );
        assert_eq!(
            bitget_http_base_url(BitgetEnvironment::Testnet),
            "https://api.bitget.com"
        );
    }
}
