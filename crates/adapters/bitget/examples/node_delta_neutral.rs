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

//! Example demonstrating the delta-neutral volatility strategy on Bitget options.
//!
//! This runs the `DeltaNeutralVol` strategy which:
//! 1. Discovers BTC option instruments from the cache
//! 2. Selects OTM call and put strikes for a short strangle
//! 3. Subscribes to option greeks and hedge instrument quotes
//! 4. Enters the strangle with IV-based limit orders (via `order_iv` param)
//! 5. Delta-hedges with the underlying perpetual on a periodic timer
//!
//! The strategy uses `order_iv` to price option orders by implied volatility. Bitget lists no
//! options, so on Bitget the example exits before connecting.
//!
//! Edit the constants below to change the option family, hedge instrument, target
//! deltas, and hedging behavior. Entry is disabled by default (`ENTER_STRANGLE`).
//!
//! Run with: `cargo run --example bitget-delta-neutral --package nautilus-bitget --features examples`
//!
//! Credentials are read from the environment when set:
//! - `BITGET_API_KEY`
//! - `BITGET_API_SECRET`

use nautilus_bitget::{
    common::{consts::BITGET_CLIENT_ID, enums::BitgetProductType},
    config::{BitgetDataClientConfig, BitgetExecutionClientConfig},
    factories::{BitgetDataClientFactory, BitgetExecutionClientFactory},
};
use nautilus_common::enums::Environment;
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{AccountId, InstrumentId, TraderId};
use nautilus_trading::examples::strategies::delta_neutral_vol::{
    DeltaNeutralVol, DeltaNeutralVolConfig,
};

const TRADER_ID: &str = "TESTER-001";
const ACCOUNT_ID: &str = "BITGET-001";
const NODE_NAME: &str = "BITGET-DELTA-NEUTRAL-001";

const OPTION_FAMILY: &str = "BTC";
const HEDGE_INSTRUMENT_ID: &str = "BTCUSDT-PERP.BITGET";

const CONTRACTS: u64 = 1;
const REHEDGE_DELTA_THRESHOLD: f64 = 0.5;
const REHEDGE_INTERVAL_SECS: u64 = 30;
const ENTER_STRANGLE: bool = false;
const IV_PARAM_KEY: &str = "order_iv";

/// Bitget lists no option instruments (kept for parity with the other adapters' examples).
const BITGET_LISTS_OPTIONS: bool = false;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    // Bitget lists no option instruments, so this example has nothing to run against
    if !BITGET_LISTS_OPTIONS {
        eprintln!("Options are not supported by Bitget; this example exits without connecting");
        return Ok(());
    }

    let environment = Environment::Live;
    let trader_id = TraderId::from(TRADER_ID);
    let account_id = AccountId::from(ACCOUNT_ID);
    let client_id = *BITGET_CLIENT_ID;

    let data_config = BitgetDataClientConfig {
        api_key: None,
        api_secret: None,
        product_types: vec![BitgetProductType::Option, BitgetProductType::Linear],
        ..Default::default()
    };

    let exec_config = BitgetExecutionClientConfig {
        api_key: None,
        api_secret: None,
        product_types: vec![BitgetProductType::Option, BitgetProductType::Linear],
        account_id: Some(account_id),
        ..Default::default()
    };

    let data_factory = BitgetDataClientFactory::new();
    let exec_factory = BitgetExecutionClientFactory::new();

    let hedge_instrument_id = InstrumentId::from(HEDGE_INSTRUMENT_ID);

    let strategy_config = DeltaNeutralVolConfig::builder()
        .option_family(OPTION_FAMILY.to_string())
        .hedge_instrument_id(hedge_instrument_id)
        .client_id(client_id)
        .contracts(CONTRACTS)
        .rehedge_delta_threshold(REHEDGE_DELTA_THRESHOLD)
        .rehedge_interval_secs(REHEDGE_INTERVAL_SECS)
        .enter_strangle(ENTER_STRANGLE)
        .iv_param_key(IV_PARAM_KEY.to_string())
        .build();

    let strategy = DeltaNeutralVol::new(strategy_config);

    let mut node = LiveNode::builder(trader_id, environment)?
        .with_name(NODE_NAME.to_string())
        .add_data_client(None, Box::new(data_factory), Box::new(data_config))?
        .add_exec_client(None, Box::new(exec_factory), Box::new(exec_config))?
        .with_reconciliation(true)
        .with_delay_post_stop_secs(5)
        .build()?;

    node.add_strategy(strategy)?;
    node.run().await?;

    Ok(())
}
