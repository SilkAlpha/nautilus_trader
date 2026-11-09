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

//! Connects to the Bitget public WebSocket feed and streams specified market data topics.
//! Useful when manually validating the Rust WebSocket client implementation.

use std::error::Error;

use futures_util::StreamExt;
use nautilus_bitget::{
    common::enums::{BitgetEnvironment, BitgetProductType},
    websocket::{client::BitgetWebSocketClient, messages::BitgetWsMessage},
};
use nautilus_network::websocket::TransportBackend;
use tokio::{pin, signal};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    nautilus_common::logging::ensure_logging_initialized();

    let mut client = BitgetWebSocketClient::new_public_with(
        BitgetProductType::Linear,
        BitgetEnvironment::Mainnet,
        None,
        20,
        TransportBackend::default(),
        None,
    );
    client.connect().await?;

    client
        .subscribe(vec![
            "books1.BTCUSDT".to_string(),
            "publicTrade.BTCUSDT".to_string(),
            "ticker.BTCUSDT".to_string(),
        ])
        .await?;

    let stream = client.stream();
    let shutdown = signal::ctrl_c();
    pin!(stream);
    pin!(shutdown);

    log::info!("Streaming Bitget market data; press Ctrl+C to exit");

    loop {
        tokio::select! {
            Some(event) = stream.next() => {
                match event {
                    BitgetWsMessage::Orderbook(msg) => {
                        log::info!(
                            "orderbook: topic={}, type={}, count={}",
                            msg.topic, msg.msg_type, msg.data.b.len() + msg.data.a.len()
                        );
                    }
                    BitgetWsMessage::Trade(msg) => {
                        for trade in &msg.data {
                            log::info!(
                                "trade: symbol={}, price={}, size={}, side={:?}",
                                trade.s, trade.p, trade.v, trade.taker_side
                            );
                        }
                    }
                    BitgetWsMessage::Kline(msg) => {
                        // Bitget klines carry no confirm flag: a later `start` closes a bar
                        for kline in &msg.data {
                            log::info!(
                                "kline: topic={}, start={}, close={}",
                                msg.topic, kline.start, kline.close
                            );
                        }
                    }
                    BitgetWsMessage::TickerLinear(msg) => {
                        log::info!(
                            "ticker linear: symbol={}, last_price={:?}, mark_price={:?}",
                            msg.data.symbol, msg.data.last_price, msg.data.mark_price
                        );
                    }
                    BitgetWsMessage::TickerOption(msg) => {
                        log::info!(
                            "ticker option: symbol={}, bid={}, ask={}",
                            msg.data.symbol, msg.data.bid_price, msg.data.ask_price
                        );
                    }
                    BitgetWsMessage::Error(err) => {
                        log::warn!("WebSocket error: code={}, message={}", err.code, err.message);
                    }
                    BitgetWsMessage::Reconnected => {
                        log::warn!("WebSocket reconnected");
                    }
                    BitgetWsMessage::Auth(result) => {
                        log::info!("Auth result: success={:?}", result.success);
                    }
                    _ => {
                        log::trace!("Other message received");
                    }
                }
            }
            _ = &mut shutdown => {
                log::info!("Received Ctrl+C, closing connection");
                client.close().await?;
                break;
            }
            else => break,
        }
    }

    Ok(())
}
