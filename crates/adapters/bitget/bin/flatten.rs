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

//! Cancels all derivatives orders and flattens all derivatives positions on Bitget.
//!
//! Iterates Linear and Inverse product types. Spot and Option are skipped: Spot
//! holdings are balances rather than positions, and Option flattening needs
//! per-leg analysis that this script does not perform.
//!
//! Run with:
//! ```bash
//! cargo run -p nautilus-bitget --bin bitget-flatten
//! ```
//!
//! Environment variables (any of):
//! - `BITGET_API_KEY` / `BITGET_API_SECRET` / `BITGET_API_PASSPHRASE` (mainnet, default)
//! - `BITGET_DEMO_API_KEY` / `BITGET_DEMO_API_SECRET` / `BITGET_DEMO_API_PASSPHRASE` (demo;
//!   Bitget has no testnet, so testnet also uses the demo keys and venue)
//!
//! Selector flags (mutually exclusive):
//! - `BITGET_DEMO=true` selects demo
//! - `BITGET_TESTNET=true` selects testnet
//! - otherwise mainnet

use std::time::Duration;

use nautilus_bitget::{
    common::enums::{BitgetPositionIdx, BitgetProductType},
    http::client::BitgetHttpClient,
};
use nautilus_core::UUID4;
use nautilus_model::{
    enums::{OrderSide, OrderType, PositionSide, TimeInForce},
    identifiers::{AccountId, ClientOrderId},
    reports::position::PositionStatusReport,
};

const VENUE_SUFFIX: &str = "BITGET";
const FLATTEN_PRODUCT_TYPES: &[BitgetProductType] =
    &[BitgetProductType::Linear, BitgetProductType::Inverse];

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    nautilus_common::logging::ensure_logging_initialized();

    let demo = env_flag("BITGET_DEMO");
    let testnet = env_flag("BITGET_TESTNET");

    if demo && testnet {
        anyhow::bail!("BITGET_DEMO and BITGET_TESTNET cannot both be set");
    }

    let env_label = if demo {
        "DEMO"
    } else if testnet {
        "TESTNET"
    } else {
        "MAINNET"
    };
    log::warn!("Bitget flatten starting against {env_label}");

    let client = BitgetHttpClient::new_with_env(
        None, None, None, None, demo, testnet, 30, 3, 1_000, 10_000, 5_000, None,
    )?;

    let account_id = AccountId::new(format!("{VENUE_SUFFIX}-UNIFIED"));

    for product_type in FLATTEN_PRODUCT_TYPES {
        let instruments = client
            .request_instruments(*product_type, None, None)
            .await?;
        log::info!(
            "Bootstrapped {} {product_type:?} instruments",
            instruments.len()
        );
    }

    let mut closed_any = false;

    for product_type in FLATTEN_PRODUCT_TYPES {
        let positions = client
            .request_position_status_reports(account_id, *product_type, None)
            .await?;
        let open: Vec<&PositionStatusReport> = positions
            .iter()
            .filter(|p| p.position_side != PositionSide::Flat && !p.quantity.is_zero())
            .collect();

        if open.is_empty() {
            log::info!("{product_type:?}: no open positions");
            continue;
        }

        log::info!("{product_type:?}: closing {} position(s)", open.len());

        for position in &open {
            close_position(&client, account_id, *product_type, position).await?;
            closed_any = true;
        }
    }

    if !closed_any {
        log::info!("Flatten complete: nothing to close");
        return Ok(());
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    verify_flat(&client, account_id).await
}

async fn close_position(
    client: &BitgetHttpClient,
    account_id: AccountId,
    product_type: BitgetProductType,
    position: &PositionStatusReport,
) -> anyhow::Result<()> {
    let instrument_id = position.instrument_id;

    // Cancel any working orders on this symbol so they cannot fill into the
    // close, then submit the reduce-only Market close.
    match client
        .cancel_all_orders(account_id, product_type, instrument_id)
        .await
    {
        Ok(cancelled) => {
            if !cancelled.is_empty() {
                log::info!(
                    "{instrument_id}: cancelled {} open order(s)",
                    cancelled.len(),
                );
            }
        }
        Err(e) => log::warn!("{instrument_id}: cancel_all_orders failed: {e}"),
    }

    let close_side = match position.position_side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Flat => return Ok(()),
    };

    let position_idx = position
        .venue_position_id
        .as_ref()
        .and_then(|pid| infer_position_idx(pid.as_str(), position.position_side));

    // Bitget `clientOid` is at most 32 characters of `[A-Za-z0-9_:-]`
    let uuid = UUID4::new().to_string().replace('-', "");
    let cid = ClientOrderId::new(format!("flatten-{}", &uuid[..24]));
    log::info!(
        "{instrument_id}: closing {} {} via reduce-only Market",
        position.quantity,
        close_side,
    );

    let report = client
        .submit_order(
            account_id,
            product_type,
            instrument_id,
            cid,
            close_side,
            OrderType::Market,
            position.quantity,
            Some(TimeInForce::Ioc),
            None,
            None,
            Some(false),
            true,
            false,
            false,
            position_idx,
            None,
            None,
            None,
            None,
        )
        .await?;

    log::info!(
        "{instrument_id}: submitted close venue_order_id={}",
        report.venue_order_id,
    );
    Ok(())
}

fn infer_position_idx(venue_pid: &str, side: PositionSide) -> Option<BitgetPositionIdx> {
    // Hedge-mode venue position IDs are "<SYMBOL>-LONG" / "<SYMBOL>-SHORT"
    // and one-way mode reports None.
    if venue_pid.ends_with("-LONG") {
        Some(BitgetPositionIdx::BuyHedge)
    } else if venue_pid.ends_with("-SHORT") {
        Some(BitgetPositionIdx::SellHedge)
    } else {
        match side {
            PositionSide::Long => Some(BitgetPositionIdx::OneWay),
            PositionSide::Short => Some(BitgetPositionIdx::OneWay),
            PositionSide::Flat => None,
        }
    }
}

async fn verify_flat(client: &BitgetHttpClient, account_id: AccountId) -> anyhow::Result<()> {
    let mut residual = Vec::new();

    for product_type in FLATTEN_PRODUCT_TYPES {
        let positions = client
            .request_position_status_reports(account_id, *product_type, None)
            .await?;

        for p in positions {
            if p.position_side != PositionSide::Flat && !p.quantity.is_zero() {
                residual.push((product_type, p));
            }
        }
    }

    if residual.is_empty() {
        log::info!("Flatten complete: all derivatives positions closed");
        return Ok(());
    }

    for (product_type, p) in &residual {
        log::error!(
            "Residual {product_type:?} position: {} side={:?} qty={}",
            p.instrument_id,
            p.position_side,
            p.quantity,
        );
    }
    anyhow::bail!("{} residual position(s) after flatten", residual.len())
}
