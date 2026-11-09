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

//! Example demonstrating live execution testing with the Bitget adapter.
//!
//! Edit the constants below to change the environment, target instrument, and order size.
//!
//! Run with: `cargo run --example bitget-exec-tester --package nautilus-bitget --features examples`
//!
//! Required credential environment variables (demo):
//! - `BITGET_DEMO_API_KEY`
//! - `BITGET_DEMO_API_SECRET`
//! - `BITGET_DEMO_API_PASSPHRASE`

use nautilus_bitget::{
    common::{
        consts::BITGET_CLIENT_ID,
        enums::{BitgetEnvironment, BitgetProductType},
    },
    config::{BitgetDataClientConfig, BitgetExecutionClientConfig},
    factories::{BitgetDataClientFactory, BitgetExecutionClientFactory},
};
use nautilus_common::enums::Environment;
use nautilus_live::{config::LiveExecutionEngineConfig, node::LiveNode};
use nautilus_model::{
    identifiers::{AccountId, InstrumentId, StrategyId, TraderId},
    types::Quantity,
};
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use nautilus_trading::strategy::StrategyConfig;

// WARNING: With `DRY_RUN = false`, this tester submits orders to the configured
// environment and may use real funds. Set `DRY_RUN = true` to connect without
// submitting orders or sending shutdown cancel/close commands.
const DRY_RUN: bool = false;
// Bitget has no testnet; demo trading (`paptrading: 1`) is the sandbox
const BITGET_ENVIRONMENT: BitgetEnvironment = BitgetEnvironment::Demo;
const TRADER_ID: &str = "TESTER-001";
const ACCOUNT_ID: &str = "BITGET-001";
const NODE_NAME: &str = "BITGET-EXEC-TESTER-001";
const STRATEGY_ID: &str = "EXEC_TESTER-001";
// Demo lists a subset of symbols; TSLAUSDT is one, and 0.02 clears the 5 USDT minimum notional
const INSTRUMENT_ID: &str = "TSLAUSDT-PERP.BITGET";
const ORDER_QTY: &str = "0.02";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let bitget_environment = BITGET_ENVIRONMENT;
    let environment = Environment::Live;
    let trader_id = TraderId::from(TRADER_ID);
    let account_id = AccountId::from(ACCOUNT_ID);
    let node_name = NODE_NAME.to_string();
    let client_id = *BITGET_CLIENT_ID;
    let instrument_id = InstrumentId::from(INSTRUMENT_ID);

    let data_config = BitgetDataClientConfig {
        environment: bitget_environment,
        api_key: None,        // Will use 'BITGET_DEMO_API_KEY' env var
        api_secret: None,     // Will use 'BITGET_DEMO_API_SECRET' env var
        api_passphrase: None, // Will use 'BITGET_DEMO_API_PASSPHRASE' env var
        product_types: vec![BitgetProductType::Spot, BitgetProductType::Linear],
        ..Default::default()
    };

    let exec_config = BitgetExecutionClientConfig {
        environment: bitget_environment,
        api_key: None,        // Will use 'BITGET_DEMO_API_KEY' env var
        api_secret: None,     // Will use 'BITGET_DEMO_API_SECRET' env var
        api_passphrase: None, // Will use 'BITGET_DEMO_API_PASSPHRASE' env var
        product_types: vec![BitgetProductType::Spot, BitgetProductType::Linear],
        account_id: Some(account_id),
        ..Default::default()
    };

    let data_factory = BitgetDataClientFactory::new();
    let exec_factory = BitgetExecutionClientFactory::new();
    let exec_engine_config = LiveExecutionEngineConfig {
        open_check_interval_secs: Some(10.0),
        position_check_interval_secs: Some(30.0),
        ..Default::default()
    };

    let mut node = LiveNode::builder(trader_id, environment)?
        .with_name(node_name)
        .with_exec_engine_config(exec_engine_config)
        .add_data_client(None, Box::new(data_factory), Box::new(data_config))?
        .add_exec_client(None, Box::new(exec_factory), Box::new(exec_config))?
        .with_reconciliation(true)
        .with_delay_post_stop_secs(5)
        .build()?;

    let order_qty = Quantity::from(ORDER_QTY);

    let tester_config = ExecTesterConfig::builder()
        .base(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY_ID)),
            external_order_instrument_ids: Some(vec![instrument_id]),
            ..Default::default()
        })
        .instrument_id(instrument_id)
        .client_id(client_id)
        .order_qty(order_qty)
        .dry_run(DRY_RUN)
        .log_data(false)
        .open_position_on_start_qty(order_qty.as_decimal())
        .use_post_only(true)
        .build()?;

    let tester = ExecTester::new(tester_config);

    node.add_strategy(tester)?;
    node.run().await?;

    Ok(())
}
