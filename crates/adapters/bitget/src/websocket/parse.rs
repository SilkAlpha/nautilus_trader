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

//! Parsers for Bitget WebSocket payloads.

use std::convert::TryFrom;

use anyhow::Context;
use nautilus_core::{datetime::NANOSECONDS_IN_MILLISECOND, nanos::UnixNanos, uuid::UUID4};
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate,
        OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick, greeks::OptionGreekValues,
        option_chain::OptionGreeks,
    },
    enums::{
        AccountType, AggressorSide, BookAction, GreeksConvention, LiquiditySide, OrderSide,
        OrderStatus, OrderType, PositionSide, RecordFlag, TimeInForce, TriggerType,
    },
    events::account::state::AccountState,
    identifiers::{AccountId, ClientOrderId, InstrumentId, PositionId, TradeId, VenueOrderId},
    instruments::{Instrument, any::InstrumentAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use super::{
    enums::{BitgetWsOperation, BitgetWsPrivateChannel, BitgetWsPublicChannel},
    messages::{
        BitgetWsAccountExecution, BitgetWsAccountExecutionFast, BitgetWsAccountOrder,
        BitgetWsAccountPosition, BitgetWsAccountStrategyOrder, BitgetWsAccountWallet, BitgetWsArg,
        BitgetWsAuthResponse, BitgetWsFrame, BitgetWsKline, BitgetWsLiquidation,
        BitgetWsOrderbookDepthMsg, BitgetWsResponse, BitgetWsTickerLinear, BitgetWsTickerLinearMsg,
        BitgetWsTickerOptionMsg, BitgetWsTrade, BitgetWsTradeMsg,
    },
};
use crate::{
    common::{
        consts::{BITGET_BOOK_DEPTHS, BITGET_QUOTE_DEPTH},
        enums::{BitgetOrderSide, BitgetOrderStatus, BitgetOrderType, BitgetTimeInForce},
        parse::{
            bitget_rejection_due_post_only, get_currency, make_hedge_venue_position_id,
            parse_book_level, parse_millis_timestamp, parse_price_with_precision,
            parse_quantity_with_precision,
        },
    },
    data_types::BitgetLiquidation,
};

/// Login error codes (login failure, invalid key/passphrase/timestamp, expired, bad sign).
const BITGET_LOGIN_ERROR_CODES: [i64; 6] = [30005, 30011, 30012, 30013, 30014, 30015];

/// Classifies a parsed JSON value into a typed Bitget WebSocket frame.
///
/// Bitget control frames carry an `event` (`login`, `subscribe`, `unsubscribe`, `trade`,
/// `error`); pushes carry the channel `arg` and an `action`.
///
/// Returns `Unknown(value)` if no specific type matches.
pub fn parse_bitget_ws_frame(value: serde_json::Value) -> BitgetWsFrame {
    if let Some(event) = value.get("event").and_then(serde_json::Value::as_str) {
        return match event {
            "login" => serde_json::from_value::<BitgetWsAuthResponse>(value.clone())
                .map_or_else(|_| BitgetWsFrame::Unknown(value), BitgetWsFrame::Auth),
            "subscribe" | "unsubscribe" => serde_json::from_value(value.clone()).map_or_else(
                |_| BitgetWsFrame::Unknown(value),
                BitgetWsFrame::Subscription,
            ),
            // A request-level error on an `op:"trade"` request echoes its `id`
            "trade" => serde_json::from_value(value.clone()).map_or_else(
                |_| BitgetWsFrame::Unknown(value),
                BitgetWsFrame::OrderResponse,
            ),
            "error" if value.get("id").is_some() => serde_json::from_value(value.clone())
                .map_or_else(
                    |_| BitgetWsFrame::Unknown(value),
                    BitgetWsFrame::OrderResponse,
                ),
            "error" => match serde_json::from_value::<BitgetWsResponse>(value.clone()) {
                Ok(resp) => BitgetWsFrame::ErrorResponse(classify_error_response(resp)),
                Err(_) => BitgetWsFrame::Unknown(value),
            },
            _ => BitgetWsFrame::Unknown(value),
        };
    }

    let Some(topic) = value
        .get("arg")
        .and_then(|arg| arg.get("topic"))
        .and_then(serde_json::Value::as_str)
    else {
        return BitgetWsFrame::Unknown(value);
    };
    let symbol = value
        .get("arg")
        .and_then(|arg| arg.get("symbol"))
        .and_then(serde_json::Value::as_str)
        .map(Ustr::from)
        .unwrap_or_default();

    if topic.starts_with(BitgetWsPublicChannel::OrderBook.as_ref()) {
        return serde_json::from_value::<BitgetWsOrderbookDepthMsg>(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            |mut msg| {
                msg.data.s = symbol;
                BitgetWsFrame::Orderbook(msg)
            },
        );
    }

    if topic == BitgetWsPublicChannel::PublicTrade.as_ref() {
        return serde_json::from_value::<BitgetWsTradeMsg>(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            |mut msg| {
                for trade in &mut msg.data {
                    trade.s = symbol;
                }
                BitgetWsFrame::Trade(msg)
            },
        );
    }

    if topic == BitgetWsPublicChannel::Kline.as_ref() {
        return serde_json::from_value(value.clone())
            .map_or_else(|_| BitgetWsFrame::Unknown(value), BitgetWsFrame::Kline);
    }

    if topic == BitgetWsPublicChannel::Tickers.as_ref() {
        return serde_json::from_value::<BitgetWsTickerLinearMsg>(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            |mut msg| {
                msg.data.symbol = symbol;
                BitgetWsFrame::TickerLinear(msg)
            },
        );
    }

    if topic == BitgetWsPublicChannel::AllLiquidation.as_ref() {
        return serde_json::from_value(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            BitgetWsFrame::Liquidation,
        );
    }

    if topic == BitgetWsPrivateChannel::Order.as_ref() {
        return serde_json::from_value(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            BitgetWsFrame::AccountOrder,
        );
    }

    if topic == BitgetWsPrivateChannel::StrategyOrder.as_ref() {
        return serde_json::from_value(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            BitgetWsFrame::AccountStrategyOrder,
        );
    }

    if topic == BitgetWsPrivateChannel::ExecutionFast.as_ref() {
        return serde_json::from_value(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            BitgetWsFrame::AccountExecutionFast,
        );
    }

    if topic == BitgetWsPrivateChannel::Execution.as_ref() {
        return serde_json::from_value(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            BitgetWsFrame::AccountExecution,
        );
    }

    if topic == BitgetWsPrivateChannel::Wallet.as_ref() {
        return serde_json::from_value(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            BitgetWsFrame::AccountWallet,
        );
    }

    if topic == BitgetWsPrivateChannel::Position.as_ref() {
        return serde_json::from_value(value.clone()).map_or_else(
            |_| BitgetWsFrame::Unknown(value),
            BitgetWsFrame::AccountPosition,
        );
    }

    BitgetWsFrame::Unknown(value)
}

// Recovers the operation an `event:"error"` frame refers to: login failures carry login error
// codes, and subscription failures echo the rejected `arg` as JSON at the start of `msg`
fn classify_error_response(mut resp: BitgetWsResponse) -> BitgetWsResponse {
    if resp
        .ret_code
        .is_some_and(|code| BITGET_LOGIN_ERROR_CODES.contains(&code))
    {
        resp.op = Some(BitgetWsOperation::Auth);
        return resp;
    }

    if resp.req_id.is_none()
        && let Some(msg) = resp.ret_msg.as_deref()
        && msg.starts_with('{')
        && let Some(end) = msg.find('}')
        && let Ok(arg) = serde_json::from_str::<BitgetWsArg>(&msg[..=end])
    {
        resp.op = Some(BitgetWsOperation::Subscribe);
        resp.req_id = Some(arg.to_topic());
    } else if resp.req_id.is_some() {
        resp.op = Some(BitgetWsOperation::Subscribe);
    }

    resp
}

/// Parses a Bitget WebSocket topic string into its components.
///
/// # Errors
///
/// Returns an error if the topic format is invalid.
pub fn parse_topic(topic: &str) -> anyhow::Result<Vec<&str>> {
    let parts: Vec<&str> = topic.split('.').collect();
    if parts.is_empty() {
        anyhow::bail!("Invalid topic format: empty topic");
    }
    Ok(parts)
}

/// Parses a Bitget kline topic into (interval, symbol).
///
/// Topic format: "kline.{interval}.{symbol}" (e.g., "kline.1m.BTCUSDT")
///
/// # Errors
///
/// Returns an error if the topic format is invalid.
pub fn parse_kline_topic(topic: &str) -> anyhow::Result<(&str, &str)> {
    let kline = BitgetWsPublicChannel::Kline.as_ref();
    let parts = parse_topic(topic)?;
    if parts.len() != 3 || parts[0] != kline {
        anyhow::bail!(
            "Invalid kline topic format: expected '{kline}.{{interval}}.{{symbol}}', was '{topic}'"
        );
    }
    Ok((parts[1], parts[2]))
}

/// Parses a WebSocket trade frame into a [`TradeTick`].
pub fn parse_ws_trade_tick(
    trade: &BitgetWsTrade,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let price = parse_price_with_precision(&trade.p, instrument.price_precision(), "trade.p")?;
    let size = parse_quantity_with_precision(&trade.v, instrument.size_precision(), "trade.v")?;
    let aggressor: AggressorSide = trade.taker_side.into();
    let trade_id = TradeId::new_checked(trade.i.as_str())
        .context("invalid trade identifier in Bitget trade message")?;
    let ts_event = parse_millis_i64(trade.t, "trade.T")?;

    TradeTick::new_checked(
        instrument.id(),
        price,
        size,
        aggressor,
        trade_id,
        ts_event,
        ts_init,
    )
    .context("failed to construct TradeTick from Bitget trade message")
}

/// Parses a WebSocket liquidation entry into a [`BitgetLiquidation`].
///
/// Bitget reports the side of the liquidated position, so `buy` maps to
/// [`PositionSide::Long`] and `sell` maps to [`PositionSide::Short`].
///
/// # Errors
///
/// Returns an error if the side is neither `Buy` nor `Sell`, or if the price, size, or
/// timestamp cannot be parsed.
pub fn parse_ws_liquidation(
    liquidation: &BitgetWsLiquidation,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<BitgetLiquidation> {
    let position_side = match liquidation.side {
        BitgetOrderSide::Buy => PositionSide::Long,
        BitgetOrderSide::Sell => PositionSide::Short,
        side @ BitgetOrderSide::Unknown => {
            anyhow::bail!("Invalid liquidation side: expected Buy or Sell, was {side:?}")
        }
    };

    let bankruptcy_price = parse_price_with_precision(
        &liquidation.p,
        instrument.price_precision(),
        "liquidation.p",
    )?;
    let quantity = parse_quantity_with_precision(
        &liquidation.v,
        instrument.size_precision(),
        "liquidation.v",
    )?;
    let ts_event = parse_millis_i64(liquidation.t, "liquidation.ts")?;

    Ok(BitgetLiquidation::new(
        instrument.id(),
        position_side,
        bankruptcy_price,
        quantity,
        ts_event,
        ts_init,
    ))
}

/// Parses an order book depth message into [`OrderBookDeltas`].
pub fn parse_orderbook_deltas(
    msg: &BitgetWsOrderbookDepthMsg,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    let is_snapshot = msg.msg_type.eq_ignore_ascii_case("snapshot");
    let ts_event = book_ts_event(msg)?;
    let ts_init = if ts_init.is_zero() { ts_event } else { ts_init };

    let depth = &msg.data;
    let instrument_id = instrument.id();
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    // Bitget numbers frames with `seq` only, used as both update and sequence id
    let update_id =
        u64::try_from(depth.seq).context("received negative seq in Bitget order book message")?;
    let sequence = update_id;

    let total_levels = depth.b.len() + depth.a.len();
    let capacity = total_levels + usize::from(is_snapshot);

    let snapshot_flag = if is_snapshot {
        RecordFlag::F_SNAPSHOT as u8
    } else {
        0
    };
    let mut deltas = Vec::with_capacity(capacity);

    if is_snapshot {
        deltas.push(OrderBookDelta::clear(
            instrument_id,
            sequence,
            ts_event,
            ts_init,
        ));
    }
    let mut processed = 0_usize;

    let mut push_level = |values: &[String], side: OrderSide| -> anyhow::Result<()> {
        let (price, size) = parse_book_level(values, price_precision, size_precision, "orderbook")?;
        let action = if size.is_zero() {
            BookAction::Delete
        } else if is_snapshot {
            BookAction::Add
        } else {
            BookAction::Update
        };

        processed += 1;
        let mut flags = RecordFlag::F_MBP as u8 | snapshot_flag;

        if processed == total_levels {
            flags |= RecordFlag::F_LAST as u8;
        }

        let order = BookOrder::new(side, price, size, update_id);
        let delta = OrderBookDelta::new_checked(
            instrument_id,
            action,
            order,
            flags,
            sequence,
            ts_event,
            ts_init,
        )
        .context("failed to construct OrderBookDelta from Bitget book level")?;
        deltas.push(delta);
        Ok(())
    };

    for level in &depth.b {
        push_level(level, OrderSide::Buy)?;
    }

    for level in &depth.a {
        push_level(level, OrderSide::Sell)?;
    }

    if total_levels == 0
        && let Some(last) = deltas.last_mut()
    {
        last.flags |= RecordFlag::F_LAST as u8;
    }

    OrderBookDeltas::new_checked(instrument_id, deltas)
        .context("failed to assemble OrderBookDeltas from Bitget message")
}

/// Parses a depth-1 order book snapshot into a [`QuoteTick`].
///
/// # Errors
///
/// Returns an error if the depth or message type differs, a side is missing, or price or size data
/// is invalid.
pub fn parse_orderbook_quote(
    msg: &BitgetWsOrderbookDepthMsg,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    let (depth, _) = parse_orderbook_topic(msg.topic.as_str())?;
    anyhow::ensure!(
        depth == BITGET_QUOTE_DEPTH && msg.msg_type == "snapshot",
        "Expected depth-1 orderbook snapshot"
    );
    let ts_event = book_ts_event(msg)?;
    let ts_init = if ts_init.is_zero() { ts_event } else { ts_init };
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let bid = msg
        .data
        .b
        .first()
        .context("orderbook snapshot missing bid")?;
    let ask = msg
        .data
        .a
        .first()
        .context("orderbook snapshot missing ask")?;
    let (bid_price, bid_size) = parse_book_level(bid, price_precision, size_precision, "bid")?;
    let (ask_price, ask_size) = parse_book_level(ask, price_precision, size_precision, "ask")?;

    QuoteTick::new_checked(
        instrument.id(),
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    )
    .context("failed to construct QuoteTick from Bitget order book message")
}

/// Parses a Bitget book topic key (`books1.BTCUSDT`, `books.BTCUSDT`) into (depth, symbol).
///
/// The full-depth `books` channel reports the deepest supported depth.
pub(crate) fn parse_orderbook_topic(topic: &str) -> anyhow::Result<(u32, &str)> {
    let (channel, symbol) = topic
        .split_once('.')
        .with_context(|| format!("Invalid orderbook topic: {topic}"))?;
    let depth = channel
        .strip_prefix(BitgetWsPublicChannel::OrderBook.as_ref())
        .with_context(|| format!("Invalid orderbook topic: {topic}"))?;
    let depth = if depth.is_empty() {
        BITGET_BOOK_DEPTHS[BITGET_BOOK_DEPTHS.len() - 1]
    } else {
        depth.parse()?
    };
    anyhow::ensure!(!symbol.is_empty(), "missing orderbook symbol");
    Ok((depth, symbol))
}

/// Parses an option ticker payload into a [`QuoteTick`].
pub fn parse_ticker_option_quote(
    msg: &BitgetWsTickerOptionMsg,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    let ts_event = parse_millis_i64(msg.ts, "ticker.ts")?;
    let ts_init = if ts_init.is_zero() { ts_event } else { ts_init };
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    let data = &msg.data;
    let bid_price =
        parse_price_with_precision(&data.bid_price, price_precision, "ticker.bidPrice")?;
    let ask_price =
        parse_price_with_precision(&data.ask_price, price_precision, "ticker.askPrice")?;
    let bid_size = parse_quantity_with_precision(&data.bid_size, size_precision, "ticker.bidSize")?;
    let ask_size = parse_quantity_with_precision(&data.ask_size, size_precision, "ticker.askSize")?;

    QuoteTick::new_checked(
        instrument.id(),
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    )
    .context("failed to construct QuoteTick from Bitget option ticker message")
}

/// Parses a linear ticker payload into a [`FundingRateUpdate`].
///
/// # Errors
///
/// Returns an error if funding rate, funding interval or next funding time fields are missing or cannot be parsed.
pub fn parse_ticker_linear_funding(
    data: &BitgetWsTickerLinear,
    instrument_id: InstrumentId,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<FundingRateUpdate> {
    let funding_rate_str = data
        .funding_rate
        .as_ref()
        .context("Bitget ticker missing funding_rate")?;

    if funding_rate_str.is_empty() {
        anyhow::bail!(
            "empty funding_rate for {instrument_id} (dated futures do not have funding rates)"
        );
    }

    let funding_rate = funding_rate_str
        .as_str()
        .parse::<Decimal>()
        .with_context(|| {
            format!("invalid funding_rate value '{funding_rate_str}' for {instrument_id}")
        })?;

    let funding_interval = if let Some(funding_interval_hour) = &data.funding_interval_hour {
        let funding_interval_hour = funding_interval_hour
            .as_str()
            .parse::<u16>()
            .context("invalid funding_interval_hour value")?;
        Some(
            funding_interval_hour
                .checked_mul(60)
                .ok_or_else(|| anyhow::anyhow!("funding_interval_hour out of bounds"))?,
        )
    } else {
        None
    };

    let next_funding_ns = if let Some(next_funding_time) = &data.next_funding_time {
        let next_funding_millis = next_funding_time
            .as_str()
            .parse::<i64>()
            .context("invalid next_funding_time value")?;
        Some(parse_millis_i64(next_funding_millis, "next_funding_time")?)
    } else {
        None
    };

    Ok(FundingRateUpdate::new(
        instrument_id,
        funding_rate,
        funding_interval,
        next_funding_ns,
        ts_event,
        ts_init,
    ))
}

/// Parses a linear/inverse ticker payload into a [`MarkPriceUpdate`].
///
/// # Errors
///
/// Returns an error if the mark_price field is missing or cannot be parsed.
pub fn parse_ticker_linear_mark_price(
    data: &BitgetWsTickerLinear,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<MarkPriceUpdate> {
    let mark_price_str = data
        .mark_price
        .as_ref()
        .context("Bitget ticker missing mark_price")?;

    let price =
        parse_price_with_precision(mark_price_str, instrument.price_precision(), "mark_price")?;

    Ok(MarkPriceUpdate::new(
        instrument.id(),
        price,
        ts_event,
        ts_init,
    ))
}

/// Parses a linear/inverse ticker payload into an [`IndexPriceUpdate`].
///
/// # Errors
///
/// Returns an error if the index_price field is missing or cannot be parsed.
pub fn parse_ticker_linear_index_price(
    data: &BitgetWsTickerLinear,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<IndexPriceUpdate> {
    let index_price_str = data
        .index_price
        .as_ref()
        .context("Bitget ticker missing index_price")?;

    let price =
        parse_price_with_precision(index_price_str, instrument.price_precision(), "index_price")?;

    Ok(IndexPriceUpdate::new(
        instrument.id(),
        price,
        ts_event,
        ts_init,
    ))
}

/// Parses an option ticker payload into a [`MarkPriceUpdate`].
///
/// # Errors
///
/// Returns an error if the mark_price field cannot be parsed.
pub fn parse_ticker_option_mark_price(
    msg: &BitgetWsTickerOptionMsg,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<MarkPriceUpdate> {
    let ts_event = parse_millis_i64(msg.ts, "ticker.ts")?;

    let price = parse_price_with_precision(
        &msg.data.mark_price,
        instrument.price_precision(),
        "mark_price",
    )?;

    Ok(MarkPriceUpdate::new(
        instrument.id(),
        price,
        ts_event,
        ts_init,
    ))
}

/// Parses an option ticker payload into an [`IndexPriceUpdate`].
///
/// # Errors
///
/// Returns an error if the index_price field cannot be parsed.
pub fn parse_ticker_option_index_price(
    msg: &BitgetWsTickerOptionMsg,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<IndexPriceUpdate> {
    let ts_event = parse_millis_i64(msg.ts, "ticker.ts")?;

    let price = parse_price_with_precision(
        &msg.data.index_price,
        instrument.price_precision(),
        "index_price",
    )?;

    Ok(IndexPriceUpdate::new(
        instrument.id(),
        price,
        ts_event,
        ts_init,
    ))
}

/// Parses an option ticker payload into [`OptionGreeks`].
///
/// # Errors
///
/// Returns an error if any of the greek fields cannot be parsed as f64.
pub fn parse_ticker_option_greeks(
    msg: &BitgetWsTickerOptionMsg,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<OptionGreeks> {
    let ts_event = parse_millis_i64(msg.ts, "ticker.ts")?;

    let delta: f64 = msg.data.delta.parse().context("invalid delta")?;
    let gamma: f64 = msg.data.gamma.parse().context("invalid gamma")?;
    let vega: f64 = msg.data.vega.parse().context("invalid vega")?;
    let theta: f64 = msg.data.theta.parse().context("invalid theta")?;

    let bid_iv: f64 = msg.data.bid_iv.parse().context("invalid bid_iv")?;
    let ask_iv: f64 = msg.data.ask_iv.parse().context("invalid ask_iv")?;
    let mark_iv: f64 = msg
        .data
        .mark_price_iv
        .parse()
        .context("invalid mark_price_iv")?;
    let underlying_price: f64 = msg
        .data
        .underlying_price
        .parse()
        .context("invalid underlying_price")?;
    let open_interest: f64 = msg
        .data
        .open_interest
        .parse()
        .context("invalid open_interest")?;

    Ok(OptionGreeks {
        instrument_id: instrument.id(),
        convention: GreeksConvention::BlackScholes,
        greeks: OptionGreekValues {
            delta,
            gamma,
            vega,
            theta,
            rho: 0.0, // Bitget doesn't provide rho
        },
        mark_iv: Some(mark_iv),
        bid_iv: Some(bid_iv),
        ask_iv: Some(ask_iv),
        underlying_price: Some(underlying_price),
        open_interest: Some(open_interest),
        ts_event,
        ts_init,
    })
}

// Bitget stamps book data with the matching engine time (`data.ts`); the envelope `ts` is
// the gateway push time and serves as fallback
fn book_ts_event(msg: &BitgetWsOrderbookDepthMsg) -> anyhow::Result<UnixNanos> {
    if msg.data.ts.is_empty() {
        parse_millis_i64(msg.ts, "orderbook.ts")
    } else {
        parse_millis_timestamp(&msg.data.ts, "orderbook.data.ts")
    }
}

pub(crate) fn parse_millis_i64(value: i64, field: &str) -> anyhow::Result<UnixNanos> {
    if value < 0 {
        Err(anyhow::anyhow!("{field} must be non-negative, was {value}"))
    } else {
        let nanos = (value as u64)
            .checked_mul(NANOSECONDS_IN_MILLISECOND)
            .ok_or_else(|| anyhow::anyhow!("millisecond timestamp overflowed"))?;
        Ok(UnixNanos::from(nanos))
    }
}

/// Parses a WebSocket kline payload into a [`Bar`].
///
/// # Errors
///
/// Returns an error if price or volume fields cannot be parsed or if the bar cannot be constructed.
pub fn parse_ws_kline_bar(
    kline: &BitgetWsKline,
    instrument: &InstrumentAny,
    bar_type: BarType,
    timestamp_on_close: bool,
    ts_init: UnixNanos,
) -> anyhow::Result<Bar> {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    let open = parse_price_with_precision(&kline.open, price_precision, "kline.open")?;
    let high = parse_price_with_precision(&kline.high, price_precision, "kline.high")?;
    let low = parse_price_with_precision(&kline.low, price_precision, "kline.low")?;
    let close = parse_price_with_precision(&kline.close, price_precision, "kline.close")?;
    let volume = parse_quantity_with_precision(&kline.volume, size_precision, "kline.volume")?;

    let ts_open = parse_millis_i64(kline.start, "kline.start")?;
    let interval_ns = bar_type.spec().timedelta().as_nanos();
    let interval_ns = u64::try_from(interval_ns)
        .context("bar interval overflowed the u64 range for nanoseconds")?;
    let ts_close = ts_open
        .as_u64()
        .checked_add(interval_ns)
        .map(UnixNanos::from)
        .context("bar timestamp overflowed when adjusting to close time")?;

    let ts_event = if timestamp_on_close {
        ts_close
    } else {
        ts_open
    };
    let ts_init = if ts_init.is_zero() { ts_close } else { ts_init };

    Bar::new_checked(bar_type, open, high, low, close, volume, ts_event, ts_init)
        .context("failed to construct Bar from Bitget WebSocket kline")
}

/// Parses a WebSocket account order payload into an [`OrderStatusReport`].
///
/// # Errors
///
/// Returns an error if price or quantity fields cannot be parsed or timestamps are invalid.
pub fn parse_ws_order_status_report(
    order: &BitgetWsAccountOrder,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let instrument_id = instrument.id();
    let venue_order_id = VenueOrderId::new(order.order_id);
    let order_side: Option<OrderSide> = order.side.into();

    // The `order` channel carries plain orders only; conditional ones use `strategy-order`
    let order_type = match order.order_type {
        BitgetOrderType::Market => OrderType::Market,
        _ => OrderType::Limit,
    };

    let time_in_force: TimeInForce = match order.time_in_force {
        BitgetTimeInForce::Gtc => TimeInForce::Gtc,
        BitgetTimeInForce::Ioc => TimeInForce::Ioc,
        BitgetTimeInForce::Fok => TimeInForce::Fok,
        BitgetTimeInForce::PostOnly | BitgetTimeInForce::Rpi => TimeInForce::Gtc,
    };

    let quantity =
        parse_quantity_with_precision(&order.qty, instrument.size_precision(), "order.qty")?;

    let filled_qty = parse_quantity_with_precision(
        &order.cum_exec_qty,
        instrument.size_precision(),
        "order.cumExecQty",
    )?;

    // Map Bitget order status to Nautilus order status
    // Special case: if Bitget reports "Rejected" but the order has fills, treat it as Canceled.
    // This handles the case where the exchange partially fills an order then rejects the
    // remaining quantity (e.g., due to margin, risk limits, or liquidity constraints).
    // The state machine does not allow PARTIALLY_FILLED -> REJECTED transitions.
    let order_status: OrderStatus = match order.order_status {
        BitgetOrderStatus::Created | BitgetOrderStatus::New | BitgetOrderStatus::Untriggered => {
            OrderStatus::Accepted
        }
        BitgetOrderStatus::Rejected => {
            if filled_qty.is_positive() {
                OrderStatus::Canceled
            } else {
                OrderStatus::Rejected
            }
        }
        BitgetOrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        BitgetOrderStatus::Filled => OrderStatus::Filled,
        // A post-only order that would take liquidity is reported as Cancelled with
        // rejectReason=EC_PostOnlyWillTakeLiquidity (not Rejected). Surface it as Rejected
        // for consistency with the tracked-order event path.
        BitgetOrderStatus::Canceled
            if filled_qty.is_zero()
                && bitget_rejection_due_post_only(order.reject_reason.as_str()) =>
        {
            OrderStatus::Rejected
        }
        BitgetOrderStatus::Canceled | BitgetOrderStatus::PartiallyFilledCanceled => {
            OrderStatus::Canceled
        }
        BitgetOrderStatus::Triggered => OrderStatus::Triggered,
        BitgetOrderStatus::Deactivated => OrderStatus::Canceled,
    };

    let ts_accepted = parse_millis_timestamp(&order.created_time, "order.createdTime")?;
    let ts_last = parse_millis_timestamp(&order.updated_time, "order.updatedTime")?;

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        None,
        venue_order_id,
        order_side,
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        ts_accepted,
        ts_last,
        ts_init,
        Some(UUID4::new()),
    );

    if !order.order_link_id.is_empty() {
        report = report.with_client_order_id(ClientOrderId::new(order.order_link_id));
    }

    if !order.price.is_empty() && order.price != "0" {
        let price =
            parse_price_with_precision(&order.price, instrument.price_precision(), "order.price")?;
        report = report.with_price(price);
    }

    if !order.avg_price.is_empty() && order.avg_price != "0" {
        let avg_px = order.avg_price.parse::<Decimal>().with_context(|| {
            format!("Failed to parse avg_price='{}' as Decimal", order.avg_price)
        })?;
        report = report.with_avg_px(avg_px);
    }

    if let Some(venue_position_id) =
        make_hedge_venue_position_id(instrument_id, order.position_idx())
    {
        report = report.with_venue_position_id(venue_position_id);
    }

    if order.reduce_only {
        report = report.with_reduce_only(true);
    }

    if matches!(
        order.time_in_force,
        BitgetTimeInForce::PostOnly | BitgetTimeInForce::Rpi
    ) {
        report = report.with_post_only(true);
    }

    if !order.reject_reason.is_empty() {
        report = report.with_cancel_reason(order.reject_reason.to_string());
    }

    Ok(report)
}

/// Parses a WebSocket conditional (`strategy-order`) payload into an [`OrderStatusReport`].
///
/// Bitget runs trigger and TP/SL orders apart from plain orders: `pending` waits for the trigger,
/// `success` places the child order, `failed` and `cancelled` are terminal.
///
/// # Errors
///
/// Returns an error if price or quantity fields cannot be parsed or timestamps are invalid.
pub fn parse_ws_strategy_order_status_report(
    order: &BitgetWsAccountStrategyOrder,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let instrument_id = instrument.id();
    let price_precision = instrument.price_precision();
    let order_type = if order.trigger_order_type == "limit" {
        OrderType::StopLimit
    } else {
        OrderType::StopMarket
    };
    let order_status = match order.status {
        BitgetOrderStatus::Untriggered | BitgetOrderStatus::Created | BitgetOrderStatus::New => {
            OrderStatus::Accepted
        }
        BitgetOrderStatus::Triggered => OrderStatus::Triggered,
        BitgetOrderStatus::Rejected => OrderStatus::Rejected,
        _ => OrderStatus::Canceled,
    };
    let quantity =
        parse_quantity_with_precision(&order.qty, instrument.size_precision(), "strategy.qty")?;
    let ts_accepted = parse_millis_timestamp(&order.created_time, "strategy.createdTime")?;
    let ts_last = parse_millis_timestamp(&order.updated_time, "strategy.updatedTime")?;

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        None,
        VenueOrderId::new(order.order_id),
        order.side.into(),
        order_type,
        TimeInForce::Gtc,
        order_status,
        quantity,
        Quantity::zero(instrument.size_precision()),
        ts_accepted,
        ts_last,
        ts_init,
        Some(UUID4::new()),
    );

    if !order.order_link_id.is_empty() {
        report = report.with_client_order_id(ClientOrderId::new(order.order_link_id));
    }

    if !order.trigger_price.is_empty() {
        let trigger_price =
            parse_price_with_precision(&order.trigger_price, price_precision, "triggerPrice")?;
        let trigger_type = if order.trigger_by == "mark" {
            TriggerType::MarkPrice
        } else {
            TriggerType::LastPrice
        };
        report = report
            .with_trigger_price(trigger_price)
            .with_trigger_type(trigger_type);
    }

    if !order.trigger_order_price.is_empty() {
        let price = parse_price_with_precision(
            &order.trigger_order_price,
            price_precision,
            "triggerOrderPrice",
        )?;
        report = report.with_price(price);
    }

    if order.reduce_only {
        report = report.with_reduce_only(true);
    }

    Ok(report)
}

/// Parses a WebSocket account execution payload into a [`FillReport`].
///
/// # Errors
///
/// Returns an error if price or quantity fields cannot be parsed or timestamps are invalid.
pub fn parse_ws_fill_report(
    execution: &BitgetWsAccountExecution,
    account_id: AccountId,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<FillReport> {
    let instrument_id = instrument.id();
    let venue_order_id = VenueOrderId::new(execution.order_id);
    let trade_id = TradeId::new_checked(execution.exec_id.as_str())
        .context("invalid execId in Bitget WebSocket execution payload")?;

    let order_side = OrderSide::try_from(execution.side)?;
    let last_qty = parse_quantity_with_precision(
        &execution.exec_qty,
        instrument.size_precision(),
        "execution.execQty",
    )?;
    let last_px = parse_price_with_precision(
        &execution.exec_price,
        instrument.price_precision(),
        "execution.execPrice",
    )?;

    let liquidity_side = if execution.is_maker {
        LiquiditySide::Maker
    } else {
        LiquiditySide::Taker
    };

    // Bitget lists fees per coin in `feeDetail` (positive = cost)
    let (fee_decimal, fee_coin) = execution.exec_fee()?;
    let commission_currency =
        fee_coin.map_or_else(|| instrument.quote_currency(), |coin| get_currency(&coin));
    let commission = Money::from_decimal(fee_decimal, commission_currency)
        .with_context(|| format!("Failed to create commission from feeDetail={fee_decimal}"))?;
    let ts_event = parse_millis_timestamp(&execution.exec_time, "execution.execTime")?;

    let client_order_id = if execution.order_link_id.is_empty() {
        None
    } else {
        Some(ClientOrderId::new(execution.order_link_id))
    };

    Ok(FillReport::new(
        account_id,
        instrument_id,
        venue_order_id,
        trade_id,
        order_side,
        last_qty,
        last_px,
        commission,
        liquidity_side,
        client_order_id,
        None, // venue_position_id: fill data lacks the hold mode
        ts_event,
        ts_init,
        None, // report_id
    ))
}

/// Parses a fast-stream WebSocket execution payload into a [`FillReport`].
///
/// The `fast-fill` channel omits fee and trade-side fields, so the resulting
/// report carries zero commission. Liquidity side is derived from the payload's
/// `tradeScope`. Pair with the standard `fill` channel if exact fee data
/// is required.
///
/// `venue_position_id` should be supplied for tracked hedge-mode orders so the
/// emitted report carries the long/short position identity that the standard
/// channel preserves via `OrderFilled`.
///
/// # Errors
///
/// Returns an error if price or quantity fields cannot be parsed or timestamps are invalid.
pub fn parse_ws_fill_report_fast(
    execution: &BitgetWsAccountExecutionFast,
    account_id: AccountId,
    instrument: &InstrumentAny,
    venue_position_id: Option<PositionId>,
    ts_init: UnixNanos,
) -> anyhow::Result<FillReport> {
    let instrument_id = instrument.id();
    let venue_order_id = VenueOrderId::new(execution.order_id);
    let trade_id = TradeId::new_checked(execution.exec_id.as_str())
        .context("invalid execId in Bitget WebSocket fast-execution payload")?;

    let order_side = OrderSide::try_from(execution.side)?;
    let last_qty = parse_quantity_with_precision(
        &execution.exec_qty,
        instrument.size_precision(),
        "execution.execQty",
    )?;
    let last_px = parse_price_with_precision(
        &execution.exec_price,
        instrument.price_precision(),
        "execution.execPrice",
    )?;

    let liquidity_side = if execution.is_maker {
        LiquiditySide::Maker
    } else {
        LiquiditySide::Taker
    };

    // fast-fill carries no fee data (no rate or currency)
    let commission_currency = instrument.quote_currency();
    let commission = Money::from_decimal(Decimal::ZERO, commission_currency)
        .with_context(|| format!("Failed to create zero commission for {commission_currency}"))?;
    let ts_event = parse_millis_timestamp(&execution.exec_time, "execution.execTime")?;

    let client_order_id = if execution.order_link_id.is_empty() {
        None
    } else {
        Some(ClientOrderId::new(execution.order_link_id))
    };

    Ok(FillReport::new(
        account_id,
        instrument_id,
        venue_order_id,
        trade_id,
        order_side,
        last_qty,
        last_px,
        commission,
        liquidity_side,
        client_order_id,
        venue_position_id,
        ts_event,
        ts_init,
        None,
    ))
}

/// Parses a WebSocket account position payload into a [`PositionStatusReport`].
///
/// # Errors
///
/// Returns an error if position size or prices cannot be parsed.
pub fn parse_ws_position_status_report(
    position: &BitgetWsAccountPosition,
    account_id: AccountId,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<PositionStatusReport> {
    let instrument_id = instrument.id();

    // Parse absolute size as unsigned Quantity
    let quantity = parse_quantity_with_precision(
        &position.size,
        instrument.size_precision(),
        "position.size",
    )?;

    // Bitget reports `posSide` long/short even in one-way mode; a closed position arrives as
    // `positionStatus=ended` with size 0
    let position_side = if quantity.is_zero() || position.position_status == "ended" {
        PositionSide::Flat
    } else {
        match position.pos_side.as_str() {
            "long" => PositionSide::Long,
            "short" => PositionSide::Short,
            _ => PositionSide::Flat,
        }
    };

    let ts_last = parse_millis_timestamp(&position.updated_time, "position.updatedTime")?;

    let venue_position_id = make_hedge_venue_position_id(instrument_id, position.position_idx());

    Ok(PositionStatusReport::new(
        account_id,
        instrument_id,
        position_side,
        quantity,
        ts_last,
        ts_init,
        None, // report_id
        venue_position_id,
        position.entry_price(), // avg_px_open
    ))
}

/// Parses a WebSocket account wallet payload into an [`AccountState`].
///
/// # Errors
///
/// Returns an error if balance fields cannot be parsed.
pub fn parse_ws_account_state(
    wallet: &BitgetWsAccountWallet,
    account_id: AccountId,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<AccountState> {
    let mut balances = Vec::new();

    // Bitget reports margin only at account level (USD `imr`/`mmr`), so per-coin balances are
    // `equity` with the part not `available` locked, and no per-coin margins
    let margins: Vec<MarginBalance> = Vec::new();

    for coin_data in &wallet.coin {
        let currency = get_currency(coin_data.coin.as_str());
        let total_dec: Decimal = coin_data
            .equity
            .parse()
            .with_context(|| format!("invalid equity '{}'", coin_data.equity))?;
        let available_dec: Decimal = coin_data
            .available
            .parse()
            .with_context(|| format!("invalid available '{}'", coin_data.available))?;
        let locked_dec = (total_dec - available_dec).max(Decimal::ZERO);

        balances.push(AccountBalance::from_total_and_locked(
            total_dec, locked_dec, currency,
        )?);
    }

    Ok(AccountState::new(
        account_id,
        AccountType::Margin, // Bitget unified account
        balances,
        margins,
        true, // is_reported
        UUID4::new(),
        ts_event,
        ts_init,
        None, // base_currency
    ))
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::BarSpecification,
        enums::{AggregationSource, BarAggregation, PositionSide, PriceType},
        identifiers::PositionId,
        types::Price,
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;
    use serde_json::Value;

    use super::*;
    use crate::{
        common::{enums::BitgetExecType, parse::parse_linear_instrument, testing::load_test_json},
        http::models::BitgetInstrumentLinearResponse,
        websocket::messages::{
            BitgetWsAccountExecutionMsg, BitgetWsAccountOrderMsg, BitgetWsAccountPositionMsg,
            BitgetWsAccountStrategyOrderMsg, BitgetWsAccountWalletMsg, BitgetWsKlineMsg,
            BitgetWsLiquidationMsg,
        },
    };

    const TS: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

    fn instrument_at(index: usize) -> InstrumentAny {
        let json = load_test_json("http_get_instruments_linear.json");
        let response: BitgetInstrumentLinearResponse = serde_json::from_str(&json).unwrap();
        parse_linear_instrument(&response.result.list[index], TS, TS).unwrap()
    }

    // BTCUSDT: public frames were captured on BTCUSDT
    fn linear_instrument() -> InstrumentAny {
        instrument_at(0)
    }

    // TSLAUSDT: private frames were captured on the demo account with TSLAUSDT
    fn tsla_instrument() -> InstrumentAny {
        instrument_at(2)
    }

    fn frame(fixture: &str) -> BitgetWsFrame {
        parse_bitget_ws_frame(serde_json::from_str(&load_test_json(fixture)).unwrap())
    }

    fn orderbook_msg(fixture: &str) -> BitgetWsOrderbookDepthMsg {
        match frame(fixture) {
            BitgetWsFrame::Orderbook(msg) => msg,
            other => panic!("Expected Orderbook, was {other:?}"),
        }
    }

    fn order_msg(fixture: &str) -> BitgetWsAccountOrderMsg {
        serde_json::from_str(&load_test_json(fixture)).unwrap()
    }

    fn execution_msg() -> BitgetWsAccountExecutionMsg {
        serde_json::from_str(&load_test_json("ws_account_execution.json")).unwrap()
    }

    #[rstest]
    fn parse_ws_trade_into_trade_tick() {
        let instrument = linear_instrument();
        let BitgetWsFrame::Trade(msg) = frame("ws_public_trade.json") else {
            panic!("Expected Trade frame");
        };
        let trade = &msg.data[0];

        let tick = parse_ws_trade_tick(trade, &instrument, TS).unwrap();

        assert_eq!(trade.s, "BTCUSDT");
        assert_eq!(tick.instrument_id, instrument.id());
        assert_eq!(tick.price, instrument.make_price(75242.8));
        assert_eq!(tick.size, instrument.make_qty(0.001, None));
        assert_eq!(tick.aggressor_side, AggressorSide::Sell);
        assert_eq!(tick.trade_id.to_string(), "1503519448486395904");
        assert_eq!(tick.ts_event, UnixNanos::new(1_794_192_891_236_000_000));
    }

    #[rstest]
    fn parse_bitget_ws_frame_routes_all_liquidation_topic() {
        let frame = frame("ws_all_liquidation.json");

        match frame {
            BitgetWsFrame::Liquidation(msg) => {
                assert_eq!(msg.topic, "liquidation");
                assert_eq!(msg.data.len(), 2);
            }
            other => panic!("Expected Liquidation, was {other:?}"),
        }
    }

    #[rstest]
    #[case::sell_is_short(0, PositionSide::Short, "0.2", "30.7515", 1_794_192_963_576_000_000)]
    #[case::buy_is_long(1, PositionSide::Long, "1.1", "98.8536", 1_794_192_961_705_000_000)]
    fn parse_ws_liquidation_into_bitget_liquidation(
        #[case] index: usize,
        #[case] expected_side: PositionSide,
        #[case] expected_price: &str,
        #[case] expected_quantity: &str,
        #[case] expected_ts_event: u64,
    ) {
        let instrument = linear_instrument();
        let msg: BitgetWsLiquidationMsg =
            serde_json::from_str(&load_test_json("ws_all_liquidation.json")).unwrap();

        let liquidation = parse_ws_liquidation(&msg.data[index], &instrument, TS).unwrap();

        assert_eq!(liquidation.instrument_id, instrument.id());
        assert_eq!(liquidation.position_side, expected_side);
        assert_eq!(liquidation.bankruptcy_price, Price::from(expected_price));
        assert_eq!(liquidation.quantity.to_string(), expected_quantity);
        assert_eq!(liquidation.ts_event, UnixNanos::new(expected_ts_event));
        assert_eq!(liquidation.ts_init, TS);
    }

    #[rstest]
    fn parse_ws_liquidation_rejects_unknown_side() {
        let instrument = linear_instrument();
        let mut msg: BitgetWsLiquidationMsg =
            serde_json::from_str(&load_test_json("ws_all_liquidation.json")).unwrap();
        msg.data[0].side = BitgetOrderSide::Unknown;

        let result = parse_ws_liquidation(&msg.data[0], &instrument, TS);

        assert!(result.is_err());
    }

    #[rstest]
    fn parse_orderbook_snapshot_into_deltas() {
        let instrument = linear_instrument();
        let msg = orderbook_msg("ws_orderbook_snapshot.json");

        let deltas = parse_orderbook_deltas(&msg, &instrument, TS).unwrap();

        assert_eq!(msg.data.s, "BTCUSDT");
        assert_eq!(deltas.deltas.len(), 11);
        assert_eq!(deltas.deltas[0].action, BookAction::Clear);
        assert_eq!(deltas.deltas[1].action, BookAction::Add);
        assert_eq!(deltas.deltas[1].order.side, Some(OrderSide::Buy));
        assert_eq!(deltas.deltas[1].order.price, instrument.make_price(75242.8));
        assert_eq!(
            deltas.deltas[1].order.size,
            instrument.make_qty(3.5691, None)
        );
        assert_eq!(deltas.sequence, 1_207_360_136_242);
        assert_eq!(deltas.ts_event, UnixNanos::new(1_794_192_890_496_000_000));
        assert!(deltas.deltas.last().unwrap().flags & RecordFlag::F_LAST as u8 != 0);
    }

    #[rstest]
    fn parse_orderbook_delta_marks_actions() {
        let instrument = linear_instrument();
        let msg = orderbook_msg("ws_orderbook_delta.json");

        let deltas = parse_orderbook_deltas(&msg, &instrument, TS).unwrap();

        assert_eq!(deltas.deltas.len(), 15);
        assert_eq!(deltas.sequence, 1_207_360_466_935);
        let deletes = deltas
            .deltas
            .iter()
            .filter(|delta| delta.action == BookAction::Delete)
            .count();
        let updates = deltas
            .deltas
            .iter()
            .filter(|delta| delta.action == BookAction::Update)
            .count();
        assert_eq!(deletes, 4);
        assert_eq!(updates, 11);
    }

    #[rstest]
    fn parse_orderbook_full_snapshot_links_to_first_delta() {
        let snapshot = orderbook_msg("ws_orderbook_full_snapshot.json");
        let delta = orderbook_msg("ws_orderbook_delta.json");

        let (depth, symbol) = parse_orderbook_topic(snapshot.topic.as_str()).unwrap();

        assert_eq!(depth, 1000);
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(delta.data.pseq, snapshot.data.seq);
    }

    #[rstest]
    fn parse_orderbook_quote_produces_top_of_book() {
        let instrument = linear_instrument();
        let msg = orderbook_msg("ws_orderbook_books1.json");

        let quote = parse_orderbook_quote(&msg, &instrument, TS).unwrap();

        assert_eq!(quote.instrument_id, instrument.id());
        assert_eq!(quote.bid_price, instrument.make_price(75242.8));
        assert_eq!(quote.ask_price, instrument.make_price(75242.9));
        assert_eq!(quote.bid_size, instrument.make_qty(3.5691, None));
        assert_eq!(quote.ask_size, instrument.make_qty(3.9769, None));
        assert_eq!(quote.ts_event, UnixNanos::new(1_794_192_890_495_000_000));
    }

    #[rstest]
    #[case::delta("books1.BTCUSDT", "update", false, false, "Expected depth-1")]
    #[case::depth_5("books5.BTCUSDT", "snapshot", false, false, "Expected depth-1")]
    #[case::missing_bid("books1.BTCUSDT", "snapshot", true, false, "missing bid")]
    #[case::missing_ask("books1.BTCUSDT", "snapshot", false, true, "missing ask")]
    fn parse_orderbook_quote_rejects_non_top_of_book(
        #[case] topic: &str,
        #[case] msg_type: &str,
        #[case] missing_bid: bool,
        #[case] missing_ask: bool,
        #[case] expected_error: &str,
    ) {
        let instrument = linear_instrument();
        let mut msg = orderbook_msg("ws_orderbook_books1.json");
        msg.topic = Ustr::from(topic);
        msg.msg_type = Ustr::from(msg_type);

        if missing_bid {
            msg.data.b.clear();
        }

        if missing_ask {
            msg.data.a.clear();
        }

        let error = parse_orderbook_quote(&msg, &instrument, TS).unwrap_err();

        assert!(
            error.to_string().contains(expected_error),
            "unexpected error: {error}"
        );
    }

    #[rstest]
    #[case::timestamp_on_open(false, 1_794_192_840_000_000_000)]
    #[case::timestamp_on_close(true, 1_794_192_900_000_000_000)]
    fn parse_ws_kline_into_bar(#[case] timestamp_on_close: bool, #[case] expected_ts_event: u64) {
        let instrument = linear_instrument();
        let bar_type = BarType::new(
            instrument.id(),
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            AggregationSource::External,
        );
        let msg: BitgetWsKlineMsg = serde_json::from_str(&load_test_json("ws_kline.json")).unwrap();

        let bar = parse_ws_kline_bar(&msg.data[0], &instrument, bar_type, timestamp_on_close, TS)
            .unwrap();

        let (interval, symbol) = parse_kline_topic(msg.topic.as_str()).unwrap();
        assert_eq!((interval, symbol), ("1m", "BTCUSDT"));
        assert_eq!(bar.open, instrument.make_price(75228.8));
        assert_eq!(bar.high, instrument.make_price(75249.6));
        assert_eq!(bar.low, instrument.make_price(75228.8));
        assert_eq!(bar.close, instrument.make_price(75242.8));
        assert_eq!(bar.volume, instrument.make_qty(7.7891, None));
        assert_eq!(bar.ts_event, UnixNanos::new(expected_ts_event));
        assert_eq!(bar.ts_init, TS);
    }

    #[rstest]
    #[case::timestamp_on_open(false)]
    #[case::timestamp_on_close(true)]
    fn parse_ws_kline_zero_ts_init_falls_back_to_close(#[case] timestamp_on_close: bool) {
        let instrument = linear_instrument();
        let bar_type = BarType::new(
            instrument.id(),
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            AggregationSource::External,
        );
        let msg: BitgetWsKlineMsg = serde_json::from_str(&load_test_json("ws_kline.json")).unwrap();

        let bar = parse_ws_kline_bar(
            &msg.data[0],
            &instrument,
            bar_type,
            timestamp_on_close,
            UnixNanos::default(),
        )
        .unwrap();

        assert_eq!(bar.ts_init, UnixNanos::new(1_794_192_900_000_000_000));
    }

    #[rstest]
    fn parse_ws_order_into_order_status_report() {
        let instrument = tsla_instrument();
        let msg = order_msg("ws_account_order_filled.json");
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_order_status_report(&msg.data[0], &instrument, account_id, TS).unwrap();

        assert_eq!(report.account_id, account_id);
        assert_eq!(report.instrument_id, instrument.id());
        assert_eq!(report.venue_order_id.to_string(), "1503520323393708032");
        assert_eq!(report.client_order_id.unwrap().to_string(), "tdcapioc93099");
        assert_eq!(report.order_side, Some(OrderSide::Buy));
        assert_eq!(report.order_type, OrderType::Limit);
        assert_eq!(report.time_in_force, TimeInForce::Ioc);
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.quantity, instrument.make_qty(0.02, None));
        assert_eq!(report.filled_qty, instrument.make_qty(0.02, None));
        assert_eq!(report.price, Some(instrument.make_price(361.44)));
        assert_eq!(
            report.ts_accepted,
            UnixNanos::new(1_794_193_099_830_000_000)
        );
        assert_eq!(report.ts_last, UnixNanos::new(1_794_193_099_832_000_000));
        assert!(!report.reduce_only);
        assert!(!report.post_only);
    }

    #[rstest]
    fn parse_ws_order_avg_price_keeps_every_digit_the_venue_sent() {
        let instrument = tsla_instrument();
        let mut msg = order_msg("ws_account_order_filled.json");
        msg.data[0].avg_price = "360.7212345".to_string();
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_order_status_report(&msg.data[0], &instrument, account_id, TS).unwrap();

        assert_eq!(report.avg_px, Some(dec!(360.7212345)));
    }

    #[rstest]
    fn parse_ws_order_partially_filled_rejected_maps_to_canceled() {
        // Bitget cancels the unfilled rest of an IOC (`ioc_not_full_cancel`) with fills kept
        let instrument = tsla_instrument();
        let msg = order_msg("ws_account_order_partially_filled_rejected.json");
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_order_status_report(&msg.data[0], &instrument, account_id, TS).unwrap();

        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert_eq!(report.filled_qty, instrument.make_qty(3.71, None));
        assert_eq!(
            report.cancel_reason,
            Some("ioc_not_full_cancel".to_string())
        );
    }

    #[rstest]
    fn parse_ws_order_post_only_cancel_maps_to_rejected() {
        let instrument = tsla_instrument();
        let msg = order_msg("ws_account_order_post_only_rejected.json");
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_order_status_report(&msg.data[0], &instrument, account_id, TS).unwrap();

        assert_eq!(report.order_status, OrderStatus::Rejected);
        assert!(report.post_only);
        assert_eq!(
            report.cancel_reason,
            Some("post_only_fill_cancel".to_string())
        );
    }

    #[rstest]
    fn parse_ws_order_resting_post_only_maps_to_accepted() {
        let instrument = tsla_instrument();
        let msg = order_msg("ws_account_order.json");
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_order_status_report(&msg.data[0], &instrument, account_id, TS).unwrap();

        assert_eq!(report.order_status, OrderStatus::Accepted);
        assert_eq!(report.time_in_force, TimeInForce::Gtc);
        assert!(report.post_only);
        assert_eq!(report.avg_px, None);
    }

    #[rstest]
    fn parse_ws_execution_into_fill_report() {
        let instrument = tsla_instrument();
        let msg = execution_msg();
        let account_id = AccountId::new("BITGET-001");

        let report = parse_ws_fill_report(&msg.data[0], account_id, &instrument, TS).unwrap();

        assert_eq!(report.account_id, account_id);
        assert_eq!(report.instrument_id, instrument.id());
        assert_eq!(report.venue_order_id.to_string(), "1503520323393708032");
        assert_eq!(report.trade_id.to_string(), "1503520323400871936");
        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.last_qty, instrument.make_qty(0.02, None));
        assert_eq!(report.last_px, instrument.make_price(360.72));
        assert_eq!(report.commission.as_decimal(), dec!(0.00288576));
        assert_eq!(report.commission.currency.code, "USDT");
        assert_eq!(report.liquidity_side, LiquiditySide::Taker);
        assert_eq!(
            report.client_order_id.as_ref().unwrap().to_string(),
            "tdcapioc93099"
        );
        assert_eq!(report.ts_event, UnixNanos::new(1_794_193_099_832_000_000));
    }

    #[rstest]
    fn parse_ws_adl_execution_into_fill_report() {
        // ADL fills are marked by `tradeSide` (`dte_sys_adl_*`) on Bitget
        let instrument = tsla_instrument();
        let mut msg = execution_msg();
        let execution = &mut msg.data[0];
        execution.trade_side = Ustr::from("dte_sys_adl_sell_in_single_side_mode");
        execution.order_link_id = Ustr::default();
        let account_id = AccountId::new("BITGET-001");

        assert_eq!(execution.exec_type(), BitgetExecType::AdlTrade);
        assert!(execution.exec_type().is_exchange_generated());

        let report = parse_ws_fill_report(execution, account_id, &instrument, TS).unwrap();

        assert_eq!(report.client_order_id, None);
        assert_eq!(report.last_qty, instrument.make_qty(0.02, None));
    }

    #[rstest]
    #[case::liquidation("burst_sell_single", BitgetExecType::BustTrade)]
    #[case::delivery("delivery_sell_single", BitgetExecType::Delivery)]
    fn parse_ws_exchange_generated_execution_into_fill_report(
        #[case] trade_side: &str,
        #[case] expected: BitgetExecType,
    ) {
        let instrument = tsla_instrument();
        let mut msg = execution_msg();
        let execution = &mut msg.data[0];
        execution.trade_side = Ustr::from(trade_side);
        let account_id = AccountId::new("BITGET-001");

        assert_eq!(execution.exec_type(), expected);
        assert!(execution.exec_type().is_exchange_generated());

        let report = parse_ws_fill_report(execution, account_id, &instrument, TS).unwrap();

        assert_eq!(report.last_px, instrument.make_price(360.72));
    }

    #[rstest]
    fn parse_ws_fill_report_venue_position_id_is_none() {
        let instrument = tsla_instrument();
        let msg = execution_msg();
        let account_id = AccountId::new("BITGET-001");

        let report = parse_ws_fill_report(&msg.data[0], account_id, &instrument, TS).unwrap();

        assert_eq!(report.venue_position_id, None);
    }

    #[rstest]
    fn parse_ws_fill_report_uses_payload_fee_currency() {
        let instrument = tsla_instrument();
        let mut msg = execution_msg();
        msg.data[0].fee_detail[0].fee_coin = Some(Ustr::from("BGB"));
        let account_id = AccountId::new("BITGET-001");

        let report = parse_ws_fill_report(&msg.data[0], account_id, &instrument, TS).unwrap();

        assert_eq!(report.commission.currency.code, "BGB");
    }

    #[rstest]
    fn parse_ws_fill_report_sums_fee_detail_entries() {
        let instrument = tsla_instrument();
        let mut msg = execution_msg();
        let mut rebate = msg.data[0].fee_detail[0].clone();
        rebate.fee = Some("-0.00088576".to_string());
        msg.data[0].fee_detail.push(rebate);
        let account_id = AccountId::new("BITGET-001");

        let report = parse_ws_fill_report(&msg.data[0], account_id, &instrument, TS).unwrap();

        assert_eq!(report.commission.as_decimal(), dec!(0.002));
    }

    fn fast_execution(is_maker: bool, order_link_id: &str) -> BitgetWsAccountExecutionFast {
        let msg: crate::websocket::messages::BitgetWsAccountExecutionFastMsg =
            serde_json::from_str(&load_test_json("ws_account_execution_fast.json")).unwrap();
        let mut exec = msg.data[0].clone();
        exec.is_maker = is_maker;
        exec.order_link_id = Ustr::from(order_link_id);
        exec
    }

    #[rstest]
    #[case(true, "", LiquiditySide::Maker, None)]
    #[case(false, "link-1", LiquiditySide::Taker, Some("link-1"))]
    fn parse_ws_fill_report_fast_maps_is_maker_and_link_id(
        #[case] is_maker: bool,
        #[case] order_link_id: &str,
        #[case] expected_liquidity: LiquiditySide,
        #[case] expected_cid: Option<&str>,
    ) {
        let instrument = tsla_instrument();
        let exec = fast_execution(is_maker, order_link_id);
        let account_id = AccountId::new("BITGET-001");

        let report = parse_ws_fill_report_fast(&exec, account_id, &instrument, None, TS).unwrap();

        assert_eq!(report.account_id, account_id);
        assert_eq!(report.instrument_id, instrument.id());
        assert_eq!(report.venue_order_id.to_string(), "1503520323393708032");
        assert_eq!(report.trade_id.to_string(), "1503520323400871936");
        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.last_qty, instrument.make_qty(0.02, None));
        assert_eq!(report.last_px, instrument.make_price(360.72));
        assert_eq!(report.commission.as_f64(), 0.0);
        assert_eq!(report.liquidity_side, expected_liquidity);
        assert_eq!(
            report.client_order_id.map(|c| c.to_string()),
            expected_cid.map(str::to_string),
        );
        assert_eq!(report.venue_position_id, None);
        assert_eq!(report.ts_event, UnixNanos::new(1_794_193_099_832_000_000));
    }

    #[rstest]
    fn parse_ws_fill_report_fast_preserves_venue_position_id() {
        let instrument = tsla_instrument();
        let exec = fast_execution(false, "link-hedge");
        let account_id = AccountId::new("BITGET-001");
        let venue_pid = PositionId::from("TSLAUSDT-PERP.BITGET-LONG");

        let report =
            parse_ws_fill_report_fast(&exec, account_id, &instrument, Some(venue_pid), TS).unwrap();

        assert_eq!(report.venue_position_id, Some(venue_pid));
    }

    #[rstest]
    fn parse_bitget_ws_frame_routes_execution_fast_topic() {
        let frame = frame("ws_account_execution_fast.json");
        assert!(
            matches!(frame, BitgetWsFrame::AccountExecutionFast(_)),
            "expected AccountExecutionFast, found {frame:?}",
        );
    }

    #[rstest]
    fn parse_bitget_ws_frame_routes_standard_execution_topic() {
        // `fill` and `fast-fill` are distinct channel names, routed by exact match
        let frame = frame("ws_account_execution.json");
        assert!(
            matches!(frame, BitgetWsFrame::AccountExecution(_)),
            "expected AccountExecution, found {frame:?}",
        );
    }

    #[rstest]
    fn parse_bitget_ws_frame_unrecognized_exec_type_still_routes_execution() {
        let mut value: Value =
            serde_json::from_str(&load_test_json("ws_account_execution.json")).unwrap();
        value["data"][0]["tradeSide"] = serde_json::json!("some_future_trade_side");

        let frame = parse_bitget_ws_frame(value);

        match frame {
            BitgetWsFrame::AccountExecution(msg) => {
                assert_eq!(msg.data[0].exec_type(), BitgetExecType::Trade);
            }
            other => panic!("Expected AccountExecution, found {other:?}"),
        }
    }

    #[rstest]
    fn parse_ws_order_status_report_venue_position_id_is_none_for_one_way() {
        let instrument = tsla_instrument();
        let msg = order_msg("ws_account_order.json");
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_order_status_report(&msg.data[0], &instrument, account_id, TS).unwrap();

        assert_eq!(report.venue_position_id, None);
    }

    #[rstest]
    fn parse_ws_order_status_report_venue_position_id_for_hedge() {
        let instrument = tsla_instrument();
        let mut order = order_msg("ws_account_order.json").data[0].clone();
        order.hold_mode = Ustr::from("hedge_mode");
        let account_id = AccountId::new("BITGET-001");

        let report = parse_ws_order_status_report(&order, &instrument, account_id, TS).unwrap();

        assert_eq!(
            report.venue_position_id,
            Some(PositionId::from("TSLAUSDT-PERP.BITGET-LONG"))
        );
    }

    #[rstest]
    fn parse_ws_position_into_position_status_report() {
        let instrument = tsla_instrument();
        let msg: BitgetWsAccountPositionMsg =
            serde_json::from_str(&load_test_json("ws_account_position.json")).unwrap();
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_position_status_report(&msg.data[0], account_id, &instrument, TS).unwrap();

        assert_eq!(report.account_id, account_id);
        assert_eq!(report.instrument_id, instrument.id());
        assert_eq!(report.position_side, PositionSide::Long);
        assert_eq!(report.quantity, instrument.make_qty(0.02, None));
        assert_eq!(report.avg_px_open, Some(dec!(360.72)));
        assert_eq!(report.venue_position_id, None);
        assert_eq!(report.ts_last, UnixNanos::new(1_794_193_099_832_000_000));
    }

    #[rstest]
    fn parse_ws_position_status_report_venue_position_id_for_hedge() {
        let instrument = tsla_instrument();
        let mut msg: BitgetWsAccountPositionMsg =
            serde_json::from_str(&load_test_json("ws_account_position.json")).unwrap();
        msg.data[0].hold_mode = Ustr::from("hedge_mode");
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_position_status_report(&msg.data[0], account_id, &instrument, TS).unwrap();

        assert_eq!(
            report.venue_position_id,
            Some(PositionId::from("TSLAUSDT-PERP.BITGET-LONG"))
        );
    }

    #[rstest]
    fn parse_ws_position_short_into_position_status_report() {
        let instrument = tsla_instrument();
        let msg: BitgetWsAccountPositionMsg =
            serde_json::from_str(&load_test_json("ws_account_position_short.json")).unwrap();
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_position_status_report(&msg.data[0], account_id, &instrument, TS).unwrap();

        assert_eq!(report.position_side, PositionSide::Short);
        assert_eq!(report.quantity, instrument.make_qty(0.02, None));
        assert_eq!(report.avg_px_open, Some(dec!(360.61)));
    }

    #[rstest]
    fn parse_ws_position_closed_into_flat_report() {
        let instrument = tsla_instrument();
        let msg: BitgetWsAccountPositionMsg =
            serde_json::from_str(&load_test_json("ws_account_position_closed.json")).unwrap();
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_position_status_report(&msg.data[0], account_id, &instrument, TS).unwrap();

        assert_eq!(report.position_side, PositionSide::Flat);
        assert!(report.quantity.is_zero());
        assert_eq!(report.avg_px_open, None);
    }

    #[rstest]
    fn parse_ws_wallet_into_account_state() {
        let msg: BitgetWsAccountWalletMsg =
            serde_json::from_str(&load_test_json("ws_account_wallet.json")).unwrap();
        let account_id = AccountId::new("BITGET-001");

        let state = parse_ws_account_state(&msg.data[0], account_id, TS, TS).unwrap();

        assert_eq!(state.account_id, account_id);
        assert_eq!(state.account_type, AccountType::Margin);
        assert_eq!(state.balances.len(), 2);
        assert!(state.margins.is_empty());
        let usdt = state
            .balances
            .iter()
            .find(|b| b.currency.code == "USDT")
            .unwrap();
        assert_eq!(usdt.total.as_decimal(), dec!(49998.30404312));
        assert_eq!(usdt.free.as_decimal(), dec!(49998.30404312));
        assert!(usdt.locked.is_zero());
    }

    #[rstest]
    fn parse_ws_wallet_with_small_order_calculates_free_correctly() {
        let msg: BitgetWsAccountWalletMsg =
            serde_json::from_str(&load_test_json("ws_account_wallet_small_order.json")).unwrap();
        let account_id = AccountId::new("BITGET-001");
        let mut wallet = msg.data[0].clone();
        wallet.coin[0].available = "49990.30055736".to_string();

        let state = parse_ws_account_state(&wallet, account_id, TS, TS).unwrap();
        let usdt = &state.balances[0];

        assert_eq!(usdt.total.as_decimal(), dec!(49998.30055736));
        assert_eq!(usdt.locked.as_decimal(), dec!(8));
        assert_eq!(usdt.free.as_decimal(), dec!(49990.30055736));
    }

    #[rstest]
    fn parse_ws_wallet_clamps_locked_to_zero_when_available_exceeds_equity() {
        let msg: BitgetWsAccountWalletMsg =
            serde_json::from_str(&load_test_json("ws_account_wallet_small_order.json")).unwrap();
        let account_id = AccountId::new("BITGET-001");
        let mut wallet = msg.data[0].clone();
        wallet.coin[0].available = "50000".to_string();

        let state = parse_ws_account_state(&wallet, account_id, TS, TS).unwrap();

        assert!(state.balances[0].locked.is_zero());
    }

    fn ticker_msg() -> BitgetWsTickerLinearMsg {
        match frame("ws_ticker_linear.json") {
            BitgetWsFrame::TickerLinear(msg) => msg,
            other => panic!("Expected TickerLinear, was {other:?}"),
        }
    }

    #[rstest]
    fn parse_ticker_linear_into_funding_rate() {
        let instrument = linear_instrument();
        let msg = ticker_msg();

        let funding = parse_ticker_linear_funding(&msg.data, instrument.id(), TS, TS).unwrap();

        assert_eq!(msg.data.symbol, "BTCUSDT");
        assert_eq!(funding.rate, dec!(0.000054));
        // The Bitget ticker has no interval: it comes from the instrument `fundInterval`
        assert_eq!(funding.interval, None);
        assert_eq!(
            funding.next_funding_ns,
            Some(UnixNanos::new(1_794_211_200_000_000_000))
        );
    }

    #[rstest]
    fn parse_ticker_linear_into_mark_price() {
        let instrument = linear_instrument();
        let msg = ticker_msg();

        let mark = parse_ticker_linear_mark_price(&msg.data, &instrument, TS, TS).unwrap();

        assert_eq!(mark.value, instrument.make_price(75244.9));
    }

    #[rstest]
    fn parse_ticker_linear_into_index_price() {
        // Bitget sends the index with up to 16 decimals; it is rounded to the price precision
        let instrument = linear_instrument();
        let msg = ticker_msg();

        let index = parse_ticker_linear_index_price(&msg.data, &instrument, TS, TS).unwrap();

        assert_eq!(index.value, instrument.make_price(75276.5));
    }

    #[rstest]
    fn parse_ws_order_stop_market_buy_preserves_type() {
        let instrument = tsla_instrument();
        let msg: BitgetWsAccountStrategyOrderMsg =
            serde_json::from_str(&load_test_json("ws_account_order_buy_stop_market.json")).unwrap();
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_strategy_order_status_report(&msg.data[0], &instrument, account_id, TS)
                .unwrap();

        assert_eq!(report.order_type, OrderType::StopMarket);
        assert_eq!(report.order_side, Some(OrderSide::Buy));
        assert_eq!(report.order_status, OrderStatus::Accepted);
        assert_eq!(report.trigger_price, Some(instrument.make_price(396.89)));
        assert_eq!(report.trigger_type, Some(TriggerType::MarkPrice));
        assert_eq!(report.price, None);
    }

    #[rstest]
    fn parse_ws_order_stop_limit_preserves_type() {
        let instrument = tsla_instrument();
        let mut msg: BitgetWsAccountStrategyOrderMsg =
            serde_json::from_str(&load_test_json("ws_account_order_buy_stop_market.json")).unwrap();
        msg.data[0].trigger_order_type = Ustr::from("limit");
        msg.data[0].trigger_order_price = "397.5".to_string();
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_strategy_order_status_report(&msg.data[0], &instrument, account_id, TS)
                .unwrap();

        assert_eq!(report.order_type, OrderType::StopLimit);
        assert_eq!(report.price, Some(instrument.make_price(397.5)));
    }

    #[rstest]
    fn parse_ws_strategy_order_cancelled_maps_to_canceled() {
        let instrument = tsla_instrument();
        let msg: BitgetWsAccountStrategyOrderMsg =
            serde_json::from_str(&load_test_json("ws_strategy_order_cancelled.json")).unwrap();
        let account_id = AccountId::new("BITGET-001");

        let report =
            parse_ws_strategy_order_status_report(&msg.data[0], &instrument, account_id, TS)
                .unwrap();

        assert_eq!(report.order_status, OrderStatus::Canceled);
    }
}
