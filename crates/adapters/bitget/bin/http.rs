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

//! Demonstration binary for testing Bitget HTTP client endpoints.
//!
//! Run with:
//! ```bash
//! cargo run -p nautilus-bitget --bin bitget-http
//! ```
//!
//! For authenticated endpoints (demo trading; Bitget has no testnet), set environment variables:
//! ```bash
//! export BITGET_DEMO_API_KEY=your_key
//! export BITGET_DEMO_API_SECRET=your_secret
//! export BITGET_DEMO_API_PASSPHRASE=your_passphrase
//! cargo run -p nautilus-bitget --bin bitget-http
//! ```

use nautilus_bitget::{
    common::{
        credential::Credential,
        enums::{BitgetEnvironment, BitgetKlineInterval, BitgetProductType},
    },
    http::{
        client::BitgetHttpClient,
        query::{
            BitgetInstrumentsInfoParamsBuilder, BitgetKlinesParamsBuilder,
            BitgetTradesParamsBuilder,
        },
    },
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nautilus_common::logging::ensure_logging_initialized();

    println!("=== Bitget HTTP Client Demo ===\n");

    // Test public endpoints
    test_public_endpoints().await?;

    if Credential::resolve(None, None, None, BitgetEnvironment::Demo).is_some() {
        println!("\n=== Testing Authenticated Endpoints ===");
        test_authenticated_endpoints().await?;
    } else {
        println!(
            "\n[SKIP] Skipping authenticated endpoints (set BITGET_DEMO_API_KEY, BITGET_DEMO_API_SECRET and BITGET_DEMO_API_PASSPHRASE to test)"
        );
    }

    Ok(())
}

async fn test_public_endpoints() -> anyhow::Result<()> {
    let client = BitgetHttpClient::new(None, 60, 3, 1000, 10_000, 5_000, None)?;

    // Test 1: Get server time
    println!("1. Testing GET /api/v3/market/time");

    match client.get_server_time().await {
        Ok(response) => {
            println!(
                "   [OK] Server time: {} (seconds)",
                response.result.time_second_str()
            );
            println!(
                "   [OK] Server time: {} (nanos)",
                response.result.time_nano_str()
            );
        }
        Err(e) => {
            println!("   [ERROR] {e}");
            return Err(e.into());
        }
    }

    // Test 2: Get linear instruments
    println!("\n2. Testing GET /api/v3/market/instruments (USDT-FUTURES)");
    let params = BitgetInstrumentsInfoParamsBuilder::default()
        .category(BitgetProductType::Linear)
        .symbol("BTCUSDT")
        .build()?;

    match client.get_instruments_linear(&params).await {
        Ok(response) => {
            println!("   [OK] Found {} instruments", response.result.list.len());
            if let Some(first) = response.result.list.first() {
                println!("   [OK] First instrument: {}", first.symbol);
                println!("   [OK] Status: {:?}", first.status);
            }
        }
        Err(e) => {
            println!("   [ERROR] {e}");
            return Err(e.into());
        }
    }

    // Test 3: Get spot instruments
    println!("\n3. Testing GET /api/v3/market/instruments (SPOT)");
    let params = BitgetInstrumentsInfoParamsBuilder::default()
        .category(BitgetProductType::Spot)
        .limit(5u32)
        .build()?;

    match client.get_instruments_spot(&params).await {
        Ok(response) => {
            println!("   [OK] Found {} instruments", response.result.list.len());
            for instrument in response.result.list.iter().take(3) {
                println!("   - {}: {:?}", instrument.symbol, instrument.status);
            }
        }
        Err(e) => {
            println!("   [ERROR] {e}");
            return Err(e.into());
        }
    }

    // Test 4: Get klines
    println!("\n4. Testing GET /api/v3/market/candles");
    let params = BitgetKlinesParamsBuilder::default()
        .category(BitgetProductType::Linear)
        .symbol("BTCUSDT")
        .interval(BitgetKlineInterval::Minute1)
        .limit(5u32)
        .build()?;

    match client.get_klines(&params).await {
        Ok(response) => {
            println!("   [OK] Found {} klines", response.result.list.len());
            if let Some(first) = response.result.list.first() {
                println!(
                    "   [OK] First kline: O={}, H={}, L={}, C={}",
                    first.open, first.high, first.low, first.close
                );
            }
        }
        Err(e) => {
            println!("   [ERROR] {e}");
            return Err(e.into());
        }
    }

    // Test 5: Get recent trades
    println!("\n5. Testing GET /api/v3/market/fills");
    let params = BitgetTradesParamsBuilder::default()
        .category(BitgetProductType::Linear)
        .symbol("BTCUSDT")
        .limit(5u32)
        .build()?;

    match client.get_recent_trades(&params).await {
        Ok(response) => {
            println!("   [OK] Found {} recent trades", response.result.list.len());
            for trade in response.result.list.iter().take(3) {
                println!(
                    "   - Price: {}, Size: {}, Side: {:?}",
                    trade.price, trade.size, trade.side
                );
            }
        }
        Err(e) => {
            println!("   [ERROR] {e}");
            return Err(e.into());
        }
    }

    println!("\n[SUCCESS] All public endpoint tests passed!");
    Ok(())
}

async fn test_authenticated_endpoints() -> anyhow::Result<()> {
    let client = BitgetHttpClient::new_with_env(
        None, None, None, None, true, false, 60, 3, 1000, 10_000, 5_000, None,
    )?;

    // Test 1: Get open orders
    println!("\n1. Testing GET /api/v3/trade/unfilled-orders (open orders)");

    match client
        .get_open_orders(
            BitgetProductType::Linear,
            Some("BTCUSDT".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    {
        Ok(response) => {
            println!("   [OK] Found {} open orders", response.result.list.len());
            for order in response.result.list.iter().take(3) {
                println!(
                    "   - Order: {} | {:?} | {} @ {}",
                    order.order_id, order.side, order.qty, order.price
                );
            }
        }
        Err(e) => {
            println!("   [WARN] Error (may be expected if no orders): {e}");
        }
    }

    // Test 2: Place order (commented out to avoid actual order placement)
    println!("\n2. Testing POST /api/v3/trade/place-order (order placement)");
    println!("   [SKIP] Skipping actual order placement to avoid unintended trades");
    println!("   [INFO] Uncomment in code to test order placement");

    /*
    let order_request = serde_json::json!({
        "category": "USDT-FUTURES",
        "symbol": "BTCUSDT",
        "side": "buy",
        "orderType": "limit",
        "qty": "0.001",
        "price": "20000",
        "timeInForce": "post_only",
        "clientOid": format!("test-{}", jiff::Timestamp::now().as_second())
    });

    match client.place_order(&order_request).await {
        Ok(response) => {
            println!("   [OK] Order placed successfully");
            println!("   [OK] Order ID: {:?}", response.result.order_id);
            println!("   [OK] Order Link ID: {:?}", response.result.order_link_id);
        }
        Err(e) => {
            println!("   [ERROR] {e}");
            return Err(e.into());
        }
    }
    */

    println!("\n[SUCCESS] Authenticated endpoint tests completed!");
    Ok(())
}
