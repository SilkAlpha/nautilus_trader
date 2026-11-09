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

//! Conversion functions that translate Bitget API schemas into Nautilus instruments.

use std::{convert::TryFrom, str::FromStr};

use anyhow::Context;
pub use nautilus_core::serialization::{
    deserialize_decimal_or_zero, deserialize_optional_decimal_or_zero,
    deserialize_optional_decimal_str, deserialize_string_to_u8,
};
use serde::{Deserialize, de::Error};

/// Serde adapter for Bitget `ON`/`OFF` string fields that represent booleans.
///
/// Use as `#[serde(with = "on_off_bool")]`. Unknown values deserialize as an
/// error rather than silently coercing, so field renames surface rather than
/// decoding to the wrong value.
pub mod on_off_bool {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &bool, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(if *value { "ON" } else { "OFF" })
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
        let raw = String::deserialize(d)?;
        match raw.as_str() {
            "ON" => Ok(true),
            "OFF" => Ok(false),
            other => Err(D::Error::custom(format!(
                "expected 'ON' or 'OFF', received {other:?}"
            ))),
        }
    }
}

/// Serde adapter that accepts `readOnly` as either a bool or `0`/`1` integer.
///
/// Sub-account key listings return `readOnly` as a bool and the key update endpoints as an
/// integer. Deserializing through this module keeps the Rust field a plain `bool` across all
/// DTOs.
pub mod bool_or_int {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &bool, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bool(*value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum BoolOrInt {
            Bool(bool),
            Int(i64),
        }

        match BoolOrInt::deserialize(d)? {
            BoolOrInt::Bool(b) => Ok(b),
            BoolOrInt::Int(0) => Ok(false),
            BoolOrInt::Int(1) => Ok(true),
            BoolOrInt::Int(n) => Err(D::Error::custom(format!(
                "expected bool or 0/1, received {n}"
            ))),
        }
    }
}

/// Deserializes an integer from either a JSON integer or base-10 integer string.
///
/// Bitget responses can encode the same integer field in both forms.
pub(crate) fn deserialize_int_or_string<'de, T, D>(d: D) -> Result<T, D::Error>
where
    T: serde::Deserialize<'de> + std::str::FromStr,
    T::Err: std::fmt::Display,
    D: serde::Deserializer<'de>,
{
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum IntOrString<T> {
        Int(T),
        Str(String),
    }

    match IntOrString::<T>::deserialize(d)? {
        IntOrString::Int(value) => Ok(value),
        IntOrString::Str(value) => value.parse().map_err(|e| {
            D::Error::custom(format!(
                "expected {}, received {value:?}: {e}",
                std::any::type_name::<T>()
            ))
        }),
    }
}

/// Deserializes an `i32` from either a JSON integer or base-10 integer string.
///
/// Bitget order responses can encode `smpGroup` in both forms.
pub(crate) fn deserialize_i32_or_string<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<i32, D::Error> {
    deserialize_int_or_string(d)
}

/// Deserializes an `i64` from either a JSON integer or base-10 integer string.
///
/// Bitget position responses can encode `riskId` in both forms.
pub(crate) fn deserialize_i64_or_string<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<i64, D::Error> {
    deserialize_int_or_string(d)
}

/// Serde adapter for Bitget `yes`/`no` string fields that represent booleans.
///
/// Accepts `YES`/`NO` (REST orders), `yes`/`no` (WebSocket orders) and `true`/`false`
/// (strategy orders), case-insensitively; serializes as `YES`/`NO`.
pub mod yes_no_bool {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &bool, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(if *value { "YES" } else { "NO" })
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
        let raw = Option::<String>::deserialize(d)?.unwrap_or_default();
        if raw.eq_ignore_ascii_case("yes") || raw.eq_ignore_ascii_case("true") {
            Ok(true)
        } else if raw.is_empty()
            || raw.eq_ignore_ascii_case("no")
            || raw.eq_ignore_ascii_case("false")
        {
            Ok(false)
        } else {
            Err(D::Error::custom(format!(
                "expected 'yes' or 'no', received {raw:?}"
            )))
        }
    }
}

/// Deserializes Bitget `feeDetail` (`[{"feeCoin":"USDT","fee":"0.0029"}]`) into the signed sum of
/// its `fee` values, as a decimal string (positive = cost, as Bitget reports it).
///
/// Empty or `null` entries (`[{"feeCoin":null,"fee":null}]` on unfilled orders) count as zero.
pub(crate) fn deserialize_fee_detail_sum<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<String, D::Error> {
    #[derive(Deserialize)]
    struct FeeDetail {
        #[serde(default)]
        fee: Option<String>,
    }

    let details = Option::<Vec<FeeDetail>>::deserialize(d)?.unwrap_or_default();
    let mut total = rust_decimal::Decimal::ZERO;
    for detail in details {
        if let Some(fee) = detail.fee.as_deref().filter(|fee| !fee.is_empty()) {
            total += rust_decimal::Decimal::from_str(fee).map_err(D::Error::custom)?;
        }
    }
    Ok(total.normalize().to_string())
}

/// Deserializes a string that Bitget may send as `null` (mapped to an empty string) or as a
/// JSON number.
pub(crate) fn deserialize_string_or_null<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<String, D::Error> {
    Ok(match Option::<serde_json::Value>::deserialize(d)? {
        Some(serde_json::Value::String(s)) => s,
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        _ => String::new(),
    })
}

/// Deserializes a Bitget response code from a string (`"00000"`, `"25204"`) or a number.
///
/// REST envelopes use `"00000"` for success, WebSocket acks `"0"` or `0`; both map to `0`.
pub(crate) fn deserialize_code<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    deserialize_int_or_string(d)
}

/// Deserializes `null` (or a missing field with `#[serde(default)]`) as `T::default()`.
///
/// Bitget returns `"list": null` instead of `[]` for empty pages (e.g. `/api/v3/trade/fills`).
pub(crate) fn deserialize_null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// Deserializes an optional pagination cursor from a string, a number, or `null`.
///
/// Empty strings map to `None`.
pub(crate) fn deserialize_optional_cursor<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    Ok(match Option::<serde_json::Value>::deserialize(d)? {
        Some(serde_json::Value::String(s)) if !s.is_empty() => Some(s),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    })
}

/// Round-trips `Option<bool>` as `0`/`1` integers for Bitget request bodies
/// that advertise `readOnly` as an integer on the wire.
pub mod opt_bool_as_int {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &Option<bool>, s: S) -> Result<S::Ok, S::Error> {
        value.map(i32::from).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
        match Option::<i32>::deserialize(d)? {
            None => Ok(None),
            Some(0) => Ok(Some(false)),
            Some(1) => Ok(Some(true)),
            Some(n) => Err(D::Error::custom(format!("expected 0 or 1, received {n}"))),
        }
    }
}

/// Serde adapter that treats the masked secret literal (`"******"`) and empty
/// strings as `None`, preserving real values as `Some`.
///
/// Bitget responses never expose a usable secret: `list-sub-apikeys` returns
/// `"******"`, while the update endpoints return `""`. Surfacing `Option<String>`
/// keeps callers from accidentally treating the sentinel as a real credential.
pub mod masked_secret {
    use nautilus_core::string::secret::SecretString;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &Option<SecretString>, s: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(v) => v.serialize(s),
            None => "".serialize(s),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<SecretString>, D::Error> {
        let raw = Option::<String>::deserialize(d)?;
        Ok(match raw.as_deref() {
            None | Some("" | "******") => None,
            Some(_) => raw.map(SecretString::from),
        })
    }
}
use nautilus_core::{
    Params, UUID4,
    datetime::{NANOSECONDS_IN_MILLISECOND, nanos_to_millis as nanos_to_millis_u64},
    nanos::UnixNanos,
};
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, FundingRateUpdate, OrderBookDelta, OrderBookDeltas, TradeTick,
    },
    enums::{
        AccountType, AggressorSide, BarAggregation, BookAction, LiquiditySide, OptionKind,
        OrderSide, OrderStatus, OrderType, PositionSide, RecordFlag, TimeInForce, TriggerType,
    },
    events::account::state::AccountState,
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, PositionId, Symbol, TradeId, VenueOrderId,
    },
    instruments::{
        Instrument, any::InstrumentAny, crypto_future::CryptoFuture, crypto_option::CryptoOption,
        crypto_perpetual::CryptoPerpetual, currency_pair::CurrencyPair,
    },
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    common::{
        enums::{
            BitgetBboSideType, BitgetCancelType, BitgetContractType, BitgetKlineInterval,
            BitgetMarginTrading, BitgetMarketUnit, BitgetOptionType, BitgetOrderSide,
            BitgetOrderSmpType, BitgetOrderStatus, BitgetOrderType, BitgetPositionIdx,
            BitgetPositionMode, BitgetPositionSide, BitgetProductType, BitgetStopOrderType,
            BitgetSymbolType, BitgetTimeInForce, BitgetTpSlMode, BitgetTriggerDirection,
            BitgetTriggerType,
        },
        symbol::BitgetSymbol,
    },
    http::{
        models::{
            BitgetExecution, BitgetFunding, BitgetInstrumentInverse, BitgetInstrumentLinear,
            BitgetInstrumentOption, BitgetInstrumentSpot, BitgetKline, BitgetOrderbookResult,
            BitgetPosition, BitgetTrade, BitgetWalletBalance,
        },
        query::BitgetNativeTpSlParams,
    },
    websocket::parse::parse_millis_i64,
};

const BITGET_HOUR_INTERVALS: &[u64] = &[1, 2, 4, 6, 12];

const BITGET_POST_ONLY_REJECT_REASON: &str = "post_only_fill_cancel";

const BITGET_CLIENT_OID_MAX_LEN: usize = 32;

/// Returns whether a Bitget rejection reason indicates a post-only order that
/// would have taken liquidity (crossed the book).
///
/// Bitget accepts such an order and then pushes it `cancelled` with
/// `cancelReason=post_only_fill_cancel`; there is no reject code.
#[must_use]
pub fn bitget_rejection_due_post_only(reason: &str) -> bool {
    reason.contains(BITGET_POST_ONLY_REJECT_REASON)
}

/// Validates that a client order ID can be sent verbatim as `clientOid`.
///
/// The REST and WebSocket docs list different character sets; this accepts their intersection
/// `[A-Za-z0-9_:-]{1,32}`.
///
/// # Errors
///
/// Returns an error if the ID is too long or contains other characters.
pub fn validate_client_oid(client_order_id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !client_order_id.is_empty() && client_order_id.len() <= BITGET_CLIENT_OID_MAX_LEN,
        "client_order_id '{client_order_id}' must be 1-{BITGET_CLIENT_OID_MAX_LEN} characters for Bitget"
    );
    anyhow::ensure!(
        client_order_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b':' | b'-')),
        "client_order_id '{client_order_id}' contains characters outside [A-Za-z0-9_:-]"
    );
    Ok(())
}

/// Extracts the raw symbol from a Bitget symbol by removing the product type suffix.
#[must_use]
pub fn extract_raw_symbol(symbol: &str) -> &str {
    symbol.rsplit_once('-').map_or(symbol, |(prefix, _)| prefix)
}

/// Extracts the base coin from a Bitget option symbol.
///
/// For example, `"BTC-27MAR26-70000-P"` returns `"BTC"`.
#[must_use]
pub fn extract_base_coin(symbol: &str) -> &str {
    symbol.split_once('-').map_or(symbol, |(base, _)| base)
}

/// Constructs a full Bitget symbol from a raw symbol and product type.
///
/// Returns a `Ustr` for efficient string interning and comparisons.
#[must_use]
pub fn make_bitget_symbol<S: AsRef<str>>(raw_symbol: S, product_type: BitgetProductType) -> Ustr {
    let raw = raw_symbol.as_ref();
    Ustr::from(&format!("{raw}{}", product_type.suffix()))
}

/// Converts a Bitget kline interval string to a Nautilus bar aggregation and step.
///
/// Bitget interval strings: 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M
#[must_use]
pub fn bitget_interval_to_bar_spec(interval: &str) -> Option<(usize, BarAggregation)> {
    match interval {
        "1m" => Some((1, BarAggregation::Minute)),
        "3m" => Some((3, BarAggregation::Minute)),
        "5m" => Some((5, BarAggregation::Minute)),
        "15m" => Some((15, BarAggregation::Minute)),
        "30m" => Some((30, BarAggregation::Minute)),
        "1H" => Some((1, BarAggregation::Hour)),
        "2H" => Some((2, BarAggregation::Hour)),
        "4H" => Some((4, BarAggregation::Hour)),
        "6H" => Some((6, BarAggregation::Hour)),
        "12H" => Some((12, BarAggregation::Hour)),
        "1D" => Some((1, BarAggregation::Day)),
        "1W" => Some((1, BarAggregation::Week)),
        "1M" => Some((1, BarAggregation::Month)),
        _ => None,
    }
}

/// Converts a Nautilus bar aggregation and step to a Bitget kline interval.
///
/// Bitget supported intervals: 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M
///
/// # Errors
///
/// Returns an error if the aggregation type or step is not supported by Bitget.
pub fn bar_spec_to_bitget_interval(
    aggregation: BarAggregation,
    step: u64,
) -> anyhow::Result<BitgetKlineInterval> {
    match aggregation {
        BarAggregation::Minute => match step {
            1 => Ok(BitgetKlineInterval::Minute1),
            3 => Ok(BitgetKlineInterval::Minute3),
            5 => Ok(BitgetKlineInterval::Minute5),
            15 => Ok(BitgetKlineInterval::Minute15),
            30 => Ok(BitgetKlineInterval::Minute30),
            _ => anyhow::bail!(
                "Bitget only supports minute intervals 1, 3, 5, 15, 30 (use HOUR for >= 60)"
            ),
        },
        BarAggregation::Hour => match step {
            1 => Ok(BitgetKlineInterval::Hour1),
            2 => Ok(BitgetKlineInterval::Hour2),
            4 => Ok(BitgetKlineInterval::Hour4),
            6 => Ok(BitgetKlineInterval::Hour6),
            12 => Ok(BitgetKlineInterval::Hour12),
            _ => anyhow::bail!(
                "Bitget only supports the following hour intervals: {BITGET_HOUR_INTERVALS:?}"
            ),
        },
        BarAggregation::Day => {
            if step != 1 {
                anyhow::bail!("Bitget only supports 1 DAY interval bars");
            }
            Ok(BitgetKlineInterval::Day1)
        }
        BarAggregation::Week => {
            if step != 1 {
                anyhow::bail!("Bitget only supports 1 WEEK interval bars");
            }
            Ok(BitgetKlineInterval::Week1)
        }
        BarAggregation::Month => {
            if step != 1 {
                anyhow::bail!("Bitget only supports 1 MONTH interval bars");
            }
            Ok(BitgetKlineInterval::Month1)
        }
        _ => {
            anyhow::bail!("Bitget does not support {aggregation:?} bars");
        }
    }
}

fn default_margin() -> Decimal {
    Decimal::new(1, 1)
}

/// Parses a spot instrument definition returned by Bitget into a Nautilus currency pair.
///
/// # Panics
///
/// Panics if the constructed instrument fails validation.
pub fn parse_spot_instrument(
    definition: &BitgetInstrumentSpot,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let base_currency = get_currency(definition.base_coin.as_str());
    let quote_currency = get_currency(definition.quote_coin.as_str());

    let symbol = BitgetSymbol::new(format!("{}-SPOT", definition.symbol))?;
    let instrument_id = symbol.to_instrument_id();
    let raw_symbol = Symbol::new(symbol.raw_symbol());

    let price_filter = definition.price_filter();
    let lot_size_filter = definition.lot_size_filter();
    let price_increment = parse_price(&price_filter.tick_size, "pricePrecision")?;
    let size_increment = parse_quantity(&lot_size_filter.base_precision, "quantityPrecision")?;
    let lot_size = Some(size_increment);
    // Bitget reports `0` for "no limit"
    let max_quantity = parse_optional_positive(&lot_size_filter.max_order_qty)
        .map(|value| parse_quantity(value, "maxOrderQty"))
        .transpose()?;
    let min_quantity = parse_optional_positive(&lot_size_filter.min_order_qty)
        .map(|value| parse_quantity(value, "minOrderQty"))
        .transpose()?;
    let min_notional = parse_optional_positive(&lot_size_filter.min_order_amt)
        .map(|value| {
            Money::from_decimal(parse_decimal(value, "minOrderAmount")?, quote_currency)
                .context("invalid minOrderAmount")
        })
        .transpose()?;

    let margin_trading_supported = matches!(
        definition.margin_trading,
        BitgetMarginTrading::Both | BitgetMarginTrading::UtaOnly
    );

    let mut info = build_instrument_info(
        definition.symbol_type,
        definition.xstock_multiplier.as_deref(),
    )
    .unwrap_or_default();
    info.insert(
        "margin_trading".to_string(),
        serde_json::Value::Bool(margin_trading_supported),
    );

    let instrument = CurrencyPair::builder()
        .instrument_id(instrument_id)
        .raw_symbol(raw_symbol)
        .base_currency(base_currency)
        .quote_currency(quote_currency)
        .price_precision(price_increment.precision)
        .size_precision(size_increment.precision)
        .price_increment(price_increment)
        .size_increment(size_increment)
        .maybe_lot_size(lot_size)
        .maybe_max_quantity(max_quantity)
        .maybe_min_quantity(min_quantity)
        .maybe_min_notional(min_notional)
        .margin_init(default_margin())
        .margin_maint(default_margin())
        .info(info)
        .ts_event(ts_event)
        .ts_init(ts_init)
        .build()
        .unwrap();

    Ok(InstrumentAny::CurrencyPair(instrument))
}

/// Parses a linear contract definition (perpetual or dated future) into a Nautilus instrument.
///
/// # Panics
///
/// Panics if the constructed instrument fails validation.
pub fn parse_linear_instrument(
    definition: &BitgetInstrumentLinear,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    // Validate required fields
    anyhow::ensure!(
        !definition.base_coin.is_empty(),
        "base_coin is empty for symbol '{}'",
        definition.symbol
    );
    anyhow::ensure!(
        !definition.quote_coin.is_empty(),
        "quote_coin is empty for symbol '{}'",
        definition.symbol
    );

    let base_currency = get_currency(definition.base_coin.as_str());
    let quote_currency = get_currency(definition.quote_coin.as_str());
    let settlement_currency = resolve_settlement_currency(
        definition.settle_coin().as_str(),
        base_currency,
        quote_currency,
    )?;

    let symbol = BitgetSymbol::new(format!(
        "{}{}",
        definition.symbol,
        BitgetProductType::Linear.suffix()
    ))?;
    let instrument_id = symbol.to_instrument_id();
    let raw_symbol = Symbol::new(symbol.raw_symbol());

    // The steps (`priceMultiplier`/`quantityMultiplier`) can be coarser than `10^-precision`
    let price_precision: u8 = definition
        .price_scale
        .parse()
        .context("invalid pricePrecision")?;
    let size_precision: u8 = definition
        .quantity_precision
        .parse()
        .context("invalid quantityPrecision")?;
    let price_filter = definition.price_filter();
    let lot_size_filter = definition.lot_size_filter();
    let price_increment =
        parse_price_with_precision(&price_filter.tick_size, price_precision, "priceMultiplier")?;
    let size_increment = parse_quantity_with_precision(
        &lot_size_filter.qty_step,
        size_precision,
        "quantityMultiplier",
    )?;
    let lot_size = Some(size_increment);
    // `maxOrderQty` caps limit orders; Bitget reports `0` for "no limit"
    let max_quantity = parse_optional_positive(&lot_size_filter.max_order_qty)
        .map(|value| parse_quantity_with_precision(value, size_precision, "maxOrderQty"))
        .transpose()?;
    let min_quantity = Some(parse_quantity_with_precision(
        &lot_size_filter.min_order_qty,
        size_precision,
        "minOrderQty",
    )?);
    // Bitget bounds prices by a ratio band around the reference price, not by absolute limits
    let max_price = parse_optional_positive(&price_filter.max_price)
        .map(|value| parse_price(value, "maxPrice"))
        .transpose()?;
    let min_price = parse_optional_positive(&price_filter.min_price)
        .map(|value| parse_price(value, "minPrice"))
        .transpose()?;
    let min_notional = parse_optional_notional(
        lot_size_filter.min_notional_value.as_deref(),
        quote_currency,
        "minOrderAmount",
    )?;

    let mut info = build_instrument_info(definition.symbol_type, None).unwrap_or_default();
    // Kept for the funding interval (hours) and fee schedule, which Bitget only reports here
    info.insert(
        "fund_interval".to_string(),
        serde_json::Value::String((definition.funding_interval / 60).to_string()),
    );
    info.insert(
        "maker_fee_rate".to_string(),
        serde_json::Value::String(definition.maker_fee_rate.clone()),
    );
    info.insert(
        "taker_fee_rate".to_string(),
        serde_json::Value::String(definition.taker_fee_rate.clone()),
    );
    let info = Some(info);

    match definition.contract_type() {
        BitgetContractType::LinearPerpetual => {
            let instrument = CryptoPerpetual::builder()
                .instrument_id(instrument_id)
                .raw_symbol(raw_symbol)
                .base_currency(base_currency)
                .quote_currency(quote_currency)
                .settlement_currency(settlement_currency)
                .is_inverse(false)
                .price_precision(price_precision)
                .size_precision(size_precision)
                .price_increment(price_increment)
                .size_increment(size_increment)
                .maybe_lot_size(lot_size)
                .maybe_max_quantity(max_quantity)
                .maybe_min_quantity(min_quantity)
                .maybe_min_notional(min_notional)
                .maybe_max_price(max_price)
                .maybe_min_price(min_price)
                .margin_init(default_margin())
                .margin_maint(default_margin())
                .maybe_info(info)
                .ts_event(ts_event)
                .ts_init(ts_init)
                .build()
                .unwrap();
            Ok(InstrumentAny::CryptoPerpetual(instrument))
        }
        BitgetContractType::LinearFutures => {
            let activation_ns = parse_millis_timestamp(&definition.launch_time, "launchTime")?;
            let expiration_ns = parse_millis_timestamp(&definition.delivery_time, "deliveryTime")?;
            let instrument = CryptoFuture::builder()
                .instrument_id(instrument_id)
                .raw_symbol(raw_symbol)
                .underlying(base_currency)
                .quote_currency(quote_currency)
                .settlement_currency(settlement_currency)
                .is_inverse(false)
                .activation_ns(activation_ns)
                .expiration_ns(expiration_ns)
                .price_precision(price_precision)
                .size_precision(size_precision)
                .price_increment(price_increment)
                .size_increment(size_increment)
                .maybe_lot_size(lot_size)
                .maybe_max_quantity(max_quantity)
                .maybe_min_quantity(min_quantity)
                .maybe_min_notional(min_notional)
                .maybe_max_price(max_price)
                .maybe_min_price(min_price)
                .margin_init(default_margin())
                .margin_maint(default_margin())
                .maybe_info(info)
                .ts_event(ts_event)
                .ts_init(ts_init)
                .build()
                .unwrap();
            Ok(InstrumentAny::CryptoFuture(instrument))
        }
        other => Err(anyhow::anyhow!(
            "unsupported linear contract variant: {other:?}"
        )),
    }
}

/// Parses Bitget's `minNotionalValue` string (when present) into a `Money` value
/// denominated in the instrument's quote currency. Returns `Ok(None)` if the
/// field is absent or an empty string.
fn parse_optional_notional(
    raw: Option<&str>,
    currency: Currency,
    field: &str,
) -> anyhow::Result<Option<Money>> {
    let Some(s) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let amount: f64 = s
        .parse()
        .with_context(|| format!("invalid f64 for {field}: {s:?}"))?;

    if !amount.is_finite() || amount <= 0.0 {
        return Ok(None);
    }
    Ok(Some(Money::new(amount, currency)))
}

/// Parses an inverse contract definition into a Nautilus instrument.
///
/// # Panics
///
/// Panics if the constructed instrument fails validation.
pub fn parse_inverse_instrument(
    definition: &BitgetInstrumentInverse,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    // Validate required fields
    anyhow::ensure!(
        !definition.base_coin.is_empty(),
        "base_coin is empty for symbol '{}'",
        definition.symbol
    );
    anyhow::ensure!(
        !definition.quote_coin.is_empty(),
        "quote_coin is empty for symbol '{}'",
        definition.symbol
    );

    let base_currency = get_currency(definition.base_coin.as_str());
    let quote_currency = get_currency(definition.quote_coin.as_str());
    let settlement_currency = resolve_settlement_currency(
        definition.settle_coin().as_str(),
        base_currency,
        quote_currency,
    )?;

    let symbol = BitgetSymbol::new(format!("{}-INVERSE", definition.symbol))?;
    let instrument_id = symbol.to_instrument_id();
    let raw_symbol = Symbol::new(symbol.raw_symbol());

    // The steps (`priceMultiplier`/`quantityMultiplier`) can be coarser than `10^-precision`
    let price_precision: u8 = definition
        .price_scale
        .parse()
        .context("invalid pricePrecision")?;
    let size_precision: u8 = definition
        .quantity_precision
        .parse()
        .context("invalid quantityPrecision")?;
    let price_filter = definition.price_filter();
    let lot_size_filter = definition.lot_size_filter();
    let price_increment =
        parse_price_with_precision(&price_filter.tick_size, price_precision, "priceMultiplier")?;
    let size_increment = parse_quantity_with_precision(
        &lot_size_filter.qty_step,
        size_precision,
        "quantityMultiplier",
    )?;
    let lot_size = Some(size_increment);
    // `maxOrderQty` caps limit orders; Bitget reports `0` for "no limit"
    let max_quantity = parse_optional_positive(&lot_size_filter.max_order_qty)
        .map(|value| parse_quantity_with_precision(value, size_precision, "maxOrderQty"))
        .transpose()?;
    let min_quantity = Some(parse_quantity_with_precision(
        &lot_size_filter.min_order_qty,
        size_precision,
        "minOrderQty",
    )?);
    // Bitget bounds prices by a ratio band around the reference price, not by absolute limits
    let max_price = parse_optional_positive(&price_filter.max_price)
        .map(|value| parse_price(value, "maxPrice"))
        .transpose()?;
    let min_price = parse_optional_positive(&price_filter.min_price)
        .map(|value| parse_price(value, "minPrice"))
        .transpose()?;
    let min_notional = parse_optional_notional(
        lot_size_filter.min_notional_value.as_deref(),
        quote_currency,
        "minOrderAmount",
    )?;

    let mut info = build_instrument_info(definition.symbol_type, None).unwrap_or_default();
    // Kept for the funding interval (hours) and fee schedule, which Bitget only reports here
    info.insert(
        "fund_interval".to_string(),
        serde_json::Value::String((definition.funding_interval / 60).to_string()),
    );
    info.insert(
        "maker_fee_rate".to_string(),
        serde_json::Value::String(definition.maker_fee_rate.clone()),
    );
    info.insert(
        "taker_fee_rate".to_string(),
        serde_json::Value::String(definition.taker_fee_rate.clone()),
    );
    let info = Some(info);

    match definition.contract_type() {
        BitgetContractType::InversePerpetual => {
            let instrument = CryptoPerpetual::builder()
                .instrument_id(instrument_id)
                .raw_symbol(raw_symbol)
                .base_currency(base_currency)
                .quote_currency(quote_currency)
                .settlement_currency(settlement_currency)
                .is_inverse(true)
                .price_precision(price_precision)
                .size_precision(size_precision)
                .price_increment(price_increment)
                .size_increment(size_increment)
                .maybe_lot_size(lot_size)
                .maybe_max_quantity(max_quantity)
                .maybe_min_quantity(min_quantity)
                .maybe_min_notional(min_notional)
                .maybe_max_price(max_price)
                .maybe_min_price(min_price)
                .margin_init(default_margin())
                .margin_maint(default_margin())
                .maybe_info(info)
                .ts_event(ts_event)
                .ts_init(ts_init)
                .build()
                .unwrap();
            Ok(InstrumentAny::CryptoPerpetual(instrument))
        }
        BitgetContractType::InverseFutures => {
            let activation_ns = parse_millis_timestamp(&definition.launch_time, "launchTime")?;
            let expiration_ns = parse_millis_timestamp(&definition.delivery_time, "deliveryTime")?;
            let instrument = CryptoFuture::builder()
                .instrument_id(instrument_id)
                .raw_symbol(raw_symbol)
                .underlying(base_currency)
                .quote_currency(quote_currency)
                .settlement_currency(settlement_currency)
                .is_inverse(true)
                .activation_ns(activation_ns)
                .expiration_ns(expiration_ns)
                .price_precision(price_precision)
                .size_precision(size_precision)
                .price_increment(price_increment)
                .size_increment(size_increment)
                .maybe_lot_size(lot_size)
                .maybe_max_quantity(max_quantity)
                .maybe_min_quantity(min_quantity)
                .maybe_min_notional(min_notional)
                .maybe_max_price(max_price)
                .maybe_min_price(min_price)
                .margin_init(default_margin())
                .margin_maint(default_margin())
                .maybe_info(info)
                .ts_event(ts_event)
                .ts_init(ts_init)
                .build()
                .unwrap();
            Ok(InstrumentAny::CryptoFuture(instrument))
        }
        other => Err(anyhow::anyhow!(
            "unsupported inverse contract variant: {other:?}"
        )),
    }
}

fn build_instrument_info(
    symbol_type: Option<BitgetSymbolType>,
    xstock_multiplier: Option<&str>,
) -> Option<Params> {
    let symbol_type = symbol_type?;
    let value = symbol_type.as_str()?;
    let mut info = Params::new();
    info.insert(
        "symbol_type".to_string(),
        serde_json::Value::String(value.to_string()),
    );

    if symbol_type == BitgetSymbolType::Xstocks
        && let Some(multiplier) = xstock_multiplier.filter(|value| !value.is_empty())
    {
        info.insert(
            "xstock_multiplier".to_string(),
            serde_json::Value::String(multiplier.to_string()),
        );
    }

    Some(info)
}

/// Parses a Bitget option contract definition into a Nautilus [`CryptoOption`].
///
/// # Panics
///
/// Panics if the constructed instrument fails validation.
pub fn parse_option_instrument(
    definition: &BitgetInstrumentOption,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let symbol = BitgetSymbol::new(format!("{}-OPTION", definition.symbol))?;
    let instrument_id = symbol.to_instrument_id();
    let raw_symbol = Symbol::new(symbol.raw_symbol());
    let underlying = get_currency(definition.base_coin.as_str());
    let quote_currency = get_currency(definition.quote_coin.as_str());
    let settlement_currency = get_currency(definition.settle_coin.as_str());
    // Bitget Options are linear contracts - they are margined and settled in stablecoins
    let is_inverse = false;

    // Bitget lists no options; kept for API parity
    let price_increment = parse_price(&definition.price_filter.tick_size, "priceFilter.tickSize")?;
    let max_price = Some(parse_price(
        &definition.price_filter.max_price,
        "priceFilter.maxPrice",
    )?);
    let min_price = Some(parse_price(
        &definition.price_filter.min_price,
        "priceFilter.minPrice",
    )?);
    let lot_size = parse_quantity(
        &definition.lot_size_filter.qty_step,
        "lotSizeFilter.qtyStep",
    )?;
    let max_quantity = Some(parse_quantity(
        &definition.lot_size_filter.max_order_qty,
        "lotSizeFilter.maxOrderQty",
    )?);
    let min_quantity = Some(parse_quantity(
        &definition.lot_size_filter.min_order_qty,
        "lotSizeFilter.minOrderQty",
    )?);

    let option_kind = match definition.options_type {
        BitgetOptionType::Call => OptionKind::Call,
        BitgetOptionType::Put => OptionKind::Put,
    };

    let strike_price = extract_strike_from_symbol(&definition.symbol)?;
    let activation_ns = parse_millis_timestamp(&definition.launch_time, "launchTime")?;
    let expiration_ns = parse_millis_timestamp(&definition.delivery_time, "deliveryTime")?;

    let instrument = CryptoOption::builder()
        .instrument_id(instrument_id)
        .raw_symbol(raw_symbol)
        .underlying(underlying)
        .quote_currency(quote_currency)
        .settlement_currency(settlement_currency)
        .is_inverse(is_inverse)
        .option_kind(option_kind)
        .strike_price(strike_price)
        .activation_ns(activation_ns)
        .expiration_ns(expiration_ns)
        .price_precision(price_increment.precision)
        .size_precision(lot_size.precision)
        .price_increment(price_increment)
        // Lot size represents size increment.
        .size_increment(lot_size)
        .multiplier(Quantity::from(1_u32))
        .lot_size(lot_size)
        .maybe_max_quantity(max_quantity)
        .maybe_min_quantity(min_quantity)
        .maybe_max_price(max_price)
        .maybe_min_price(min_price)
        .ts_event(ts_event)
        .ts_init(ts_init)
        .build()
        .unwrap();

    Ok(InstrumentAny::CryptoOption(instrument))
}

/// Parses a REST trade payload into a [`TradeTick`].
pub fn parse_trade_tick(
    trade: &BitgetTrade,
    instrument: &InstrumentAny,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<TradeTick> {
    let price =
        parse_price_with_precision(&trade.price, instrument.price_precision(), "trade.price")?;
    let size =
        parse_quantity_with_precision(&trade.size, instrument.size_precision(), "trade.size")?;
    let aggressor: AggressorSide = trade.side.into();
    let trade_id = TradeId::new_checked(trade.exec_id.as_str())
        .context("invalid exec_id in Bitget trade payload")?;
    let ts_event = parse_millis_timestamp(&trade.time, "trade.time")?;
    let ts_init = ts_init.unwrap_or(ts_event);

    TradeTick::new_checked(
        instrument.id(),
        price,
        size,
        aggressor,
        trade_id,
        ts_event,
        ts_init,
    )
    .context("failed to construct TradeTick from Bitget trade payload")
}

/// Parses a REST funding payload into a [`FundingRateUpdate`].
pub fn parse_funding_rate(
    funding: &BitgetFunding,
    instrument: &InstrumentAny,
    interval_millis: Option<i64>,
) -> anyhow::Result<FundingRateUpdate> {
    let rate = parse_decimal(&funding.funding_rate, "funding.rate")?;
    let ts_event = parse_millis_timestamp(&funding.funding_rate_timestamp, "funding.timestamp")?;
    let interval = interval_millis
        .map(|ms| u16::try_from(ms / 60_000).context("interval milliseconds out of bounds"))
        .transpose()?;

    Ok(FundingRateUpdate::new(
        instrument.id(),
        rate,
        interval,
        None, // next_funding_ns not provided with historical funding rates
        ts_event,
        ts_event,
    ))
}

/// Parses an order book response into [`OrderBookDeltas`].
pub fn parse_orderbook(
    result: &BitgetOrderbookResult,
    instrument: &InstrumentAny,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<OrderBookDeltas> {
    let ts_event = parse_millis_i64(result.ts, "orderbook.timestamp")?;
    let ts_init = ts_init.unwrap_or(ts_event);

    let instrument_id = instrument.id();
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let update_id = u64::try_from(result.u)
        .context("received negative update id in Bitget order book message")?;
    let sequence = u64::try_from(result.seq)
        .context("received negative sequence in Bitget order book message")?;

    let total_levels = result.b.len() + result.a.len();
    let mut deltas = Vec::with_capacity(total_levels + 1);

    let mut clear = OrderBookDelta::clear(instrument_id, sequence, ts_event, ts_init);

    if total_levels == 0 {
        clear.flags |= RecordFlag::F_LAST as u8;
    }
    deltas.push(clear);

    let mut processed = 0_usize;

    let mut push_level = |values: &[String], side: OrderSide| -> anyhow::Result<()> {
        let (price, size) = parse_book_level(values, price_precision, size_precision, "orderbook")?;

        processed += 1;
        let mut flags = RecordFlag::F_MBP as u8;

        if processed == total_levels {
            flags |= RecordFlag::F_LAST as u8;
        }

        let order = BookOrder::new(side, price, size, update_id);
        let delta = OrderBookDelta::new_checked(
            instrument_id,
            BookAction::Add,
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

    for level in &result.b {
        push_level(level, OrderSide::Buy)?;
    }

    for level in &result.a {
        push_level(level, OrderSide::Sell)?;
    }

    OrderBookDeltas::new_checked(instrument_id, deltas)
        .context("failed to assemble OrderBookDeltas from Bitget message")
}

pub fn parse_book_level(
    level: &[String],
    price_precision: u8,
    size_precision: u8,
    label: &str,
) -> anyhow::Result<(Price, Quantity)> {
    let price_str = level
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing price component in {label} level"))?;
    let size_str = level
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("missing size component in {label} level"))?;
    let price = parse_price_with_precision(price_str, price_precision, label)?;
    let size = parse_quantity_with_precision(size_str, size_precision, label)?;
    Ok((price, size))
}

/// Parses a kline entry into a [`Bar`].
pub fn parse_kline_bar(
    kline: &BitgetKline,
    instrument: &InstrumentAny,
    bar_type: BarType,
    timestamp_on_close: bool,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<Bar> {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    let open = parse_price_with_precision(&kline.open, price_precision, "kline.open")?;
    let high = parse_price_with_precision(&kline.high, price_precision, "kline.high")?;
    let low = parse_price_with_precision(&kline.low, price_precision, "kline.low")?;
    let close = parse_price_with_precision(&kline.close, price_precision, "kline.close")?;
    let volume = parse_quantity_with_precision(&kline.volume, size_precision, "kline.volume")?;

    let ts_open = parse_millis_timestamp(&kline.start, "kline.start")?;
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
    let ts_init = ts_init.unwrap_or(ts_close); // Received time or close time

    Bar::new_checked(bar_type, open, high, low, close, volume, ts_event, ts_init)
        .context("failed to construct Bar from Bitget kline entry")
}

/// Constructs a venue position ID from an instrument and Bitget position index.
///
/// Position index values: 0 = one-way mode, 1 = buy-side hedge, 2 = sell-side hedge.
#[must_use]
pub fn make_venue_position_id(instrument_id: InstrumentId, position_idx: i32) -> PositionId {
    let side = match position_idx {
        0 => "ONEWAY",
        1 => "LONG",
        2 => "SHORT",
        _ => "UNKNOWN",
    };
    PositionId::new(format!("{instrument_id}-{side}"))
}

/// Constructs a venue position ID only for hedge-mode Bitget position indexes.
#[must_use]
pub fn make_hedge_venue_position_id(
    instrument_id: InstrumentId,
    position_idx: i32,
) -> Option<PositionId> {
    match position_idx {
        1 | 2 => Some(make_venue_position_id(instrument_id, position_idx)),
        _ => None,
    }
}

/// Resolves the position index (0 one-way, 1 long, 2 short) of a Bitget order or
/// position from its `holdMode` and `posSide`/`holdSide`.
#[must_use]
pub fn resolve_order_position_idx(hold_mode: &str, pos_side: BitgetPositionSide) -> i32 {
    match (hold_mode, pos_side) {
        ("hedge_mode", BitgetPositionSide::Buy) => BitgetPositionIdx::BuyHedge as i32,
        ("hedge_mode", BitgetPositionSide::Sell) => BitgetPositionIdx::SellHedge as i32,
        _ => BitgetPositionIdx::OneWay as i32,
    }
}

/// Resolves the `positionIdx` to send with an order under a given position mode.
///
/// In hedge mode `positionIdx` identifies the position being affected (1 = long,
/// 2 = short), not the trade direction. A reduce-only sell closes a long position
/// and a reduce-only buy closes a short position. A manual override always wins.
#[must_use]
pub fn resolve_position_idx(
    position_mode: Option<BitgetPositionMode>,
    order_side: BitgetOrderSide,
    is_reduce_only: bool,
    manual_override: Option<BitgetPositionIdx>,
) -> Option<BitgetPositionIdx> {
    if manual_override.is_some() {
        return manual_override;
    }

    let mode = position_mode?;
    match mode {
        BitgetPositionMode::BothSides => Some(match (order_side, is_reduce_only) {
            (BitgetOrderSide::Buy, false) | (BitgetOrderSide::Sell, true) => {
                BitgetPositionIdx::BuyHedge
            }
            (BitgetOrderSide::Sell, false) | (BitgetOrderSide::Buy, true) => {
                BitgetPositionIdx::SellHedge
            }
            (BitgetOrderSide::Unknown, _) => BitgetPositionIdx::OneWay,
        }),
        BitgetPositionMode::MergedSingle => Some(BitgetPositionIdx::OneWay),
    }
}

/// Parses a Bitget execution into a Nautilus FillReport.
///
/// # Errors
///
/// Returns an error if:
/// - Required price or quantity fields cannot be parsed.
/// - The execution timestamp cannot be parsed.
/// - Numeric conversions fail.
pub fn parse_fill_report(
    execution: &BitgetExecution,
    account_id: AccountId,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<FillReport> {
    let instrument_id = instrument.id();
    let venue_order_id = VenueOrderId::new(execution.order_id);
    let trade_id = TradeId::new_checked(execution.exec_id.as_str())
        .context("invalid execId in Bitget execution payload")?;

    let order_side = OrderSide::try_from(execution.side)?;

    let last_px = parse_price_with_precision(
        &execution.exec_price,
        instrument.price_precision(),
        "execution.execPrice",
    )?;

    let last_qty = parse_quantity_with_precision(
        &execution.exec_qty,
        instrument.size_precision(),
        "execution.execQty",
    )?;

    let fee_decimal: Decimal = execution
        .exec_fee
        .parse()
        .with_context(|| format!("Failed to parse execFee='{}'", execution.exec_fee))?;
    // Bitget fills carry the fee coin in `feeDetail`; contracts settle fees in the
    // settlement currency
    let currency = if execution.fee_currency.is_empty() {
        instrument.settlement_currency()
    } else {
        get_currency(&execution.fee_currency)
    };
    let commission = Money::from_decimal(fee_decimal, currency).with_context(|| {
        format!(
            "Failed to create commission from execFee='{}'",
            execution.exec_fee
        )
    })?;

    // Determine liquidity side from is_maker flag
    let liquidity_side = if execution.is_maker {
        LiquiditySide::Maker
    } else {
        LiquiditySide::Taker
    };

    let ts_event = parse_millis_timestamp(&execution.exec_time, "execution.execTime")?;

    // Parse client_order_id if present
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
        None, // venue_position_id: execution data lacks position_idx
        ts_event,
        ts_init,
        None, // Will generate a new UUID4
    ))
}

/// Parses a Bitget position into a Nautilus PositionStatusReport.
///
/// # Errors
///
/// Returns an error if:
/// - Position quantity or price fields cannot be parsed.
/// - The position timestamp cannot be parsed.
/// - Numeric conversions fail.
pub fn parse_position_status_report(
    position: &BitgetPosition,
    account_id: AccountId,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<PositionStatusReport> {
    let instrument_id = instrument.id();

    let size = parse_quantity_with_precision(
        &position.size,
        instrument.size_precision(),
        "position.size",
    )?;

    // Determine position side and quantity (Bitget keeps `posSide` on a closed, zero position)
    let (position_side, quantity) = match position.side {
        _ if size.is_zero() => (PositionSide::Flat, size),
        BitgetPositionSide::Buy => (PositionSide::Long, size),
        BitgetPositionSide::Sell => (PositionSide::Short, size),
        BitgetPositionSide::Flat => {
            let qty = Quantity::zero(instrument.size_precision());
            (PositionSide::Flat, qty)
        }
    };

    // Parse average entry price
    let avg_px_open = if position.avg_price.is_empty() || position.avg_price == "0" {
        None
    } else {
        Some(Decimal::from_str(&position.avg_price)?)
    };

    // Use ts_init if updatedTime is empty (initial/flat positions)
    let ts_last = if position.updated_time.is_empty() {
        ts_init
    } else {
        parse_millis_timestamp(&position.updated_time, "position.updatedTime")?
    };

    // Bitget ranks open positions 1-5 by ADL priority (5 = next to be deleveraged);
    // 0 means the account has no open position or is flat.
    if position.adl_rank_indicator >= 4 {
        log::warn!(
            "Elevated ADL risk: {} position size={} adl_rank={}",
            instrument_id,
            position.size,
            position.adl_rank_indicator,
        );
    }

    let venue_position_id = make_hedge_venue_position_id(
        instrument_id,
        resolve_order_position_idx(&position.hold_mode, position.side),
    );

    Ok(PositionStatusReport::new(
        account_id,
        instrument_id,
        position_side,
        quantity,
        ts_last,
        ts_init,
        None, // Will generate a new UUID4
        venue_position_id,
        avg_px_open,
    ))
}

/// Parses a Bitget wallet balance into a Nautilus account state.
///
/// # Errors
///
/// Returns an error if:
/// - Balance data cannot be parsed.
/// - Currency is invalid.
pub fn parse_account_state(
    wallet_balance: &BitgetWalletBalance,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<AccountState> {
    let mut balances = Vec::new();

    for coin in &wallet_balance.coin {
        // Total is the coin `equity` (balance plus unrealized PnL) and free is `available`; the
        // remainder (margin and order holds) is reported as locked
        let total_dec = parse_decimal(&coin.equity, "equity")?;
        let available_dec = parse_decimal(&coin.available_to_withdraw, "available")?;
        let locked_dec = (total_dec - available_dec)
            .max(Decimal::ZERO)
            .min(total_dec.max(Decimal::ZERO));

        let currency = get_currency(&coin.coin);
        balances.push(AccountBalance::from_total_and_locked(
            total_dec, locked_dec, currency,
        )?);
    }

    let mut margins = Vec::new();

    for coin in &wallet_balance.coin {
        // Position IM is reserved against open positions; order IM is reserved against
        // pending orders. Sum both so an account that only has open orders still
        // reports a non-zero initial margin.
        let position_im_f64 = match &coin.total_position_im {
            Some(im) if !im.is_empty() => im.parse::<f64>()?,
            _ => 0.0,
        };
        let order_im_f64 = match &coin.total_order_im {
            Some(im) if !im.is_empty() => im.parse::<f64>()?,
            _ => 0.0,
        };
        let initial_margin_f64 = position_im_f64 + order_im_f64;

        let maintenance_margin_f64 = match &coin.total_position_mm {
            Some(mm) if !mm.is_empty() => mm.parse::<f64>()?,
            _ => 0.0,
        };

        if initial_margin_f64 == 0.0 && maintenance_margin_f64 == 0.0 {
            continue;
        }

        let currency = get_currency(&coin.coin);
        let initial_margin = Money::new(initial_margin_f64, currency);
        let maintenance_margin = Money::new(maintenance_margin_f64, currency);

        margins.push(MarginBalance::new(initial_margin, maintenance_margin, None));
    }

    let account_type = AccountType::Margin;
    let is_reported = true;
    let event_id = UUID4::new();

    // Use current time as ts_event since Bitget doesn't provide this in wallet balance
    let ts_event = ts_init;

    Ok(AccountState::new(
        account_id,
        account_type,
        balances,
        margins,
        is_reported,
        event_id,
        ts_event,
        ts_init,
        None,
    ))
}

pub(crate) fn parse_price_with_precision(
    value: &str,
    precision: u8,
    field: &str,
) -> anyhow::Result<Price> {
    let parsed = parse_decimal(value, field)?;
    Price::from_decimal_dp(parsed, precision).with_context(|| {
        format!("Failed to construct Price for {field} with precision {precision}")
    })
}

pub(crate) fn parse_quantity_with_precision(
    value: &str,
    precision: u8,
    field: &str,
) -> anyhow::Result<Quantity> {
    let parsed = parse_decimal(value, field)?;
    Quantity::from_decimal_dp(parsed, precision).with_context(|| {
        format!("Failed to construct Quantity for {field} with precision {precision}")
    })
}

pub(crate) fn parse_price(value: &str, field: &str) -> anyhow::Result<Price> {
    Price::from_str(value).map_err(|e| anyhow::anyhow!("Failed to parse {field}='{value}': {e}"))
}

pub(crate) fn parse_quantity(value: &str, field: &str) -> anyhow::Result<Quantity> {
    Quantity::from_str(value).map_err(|e| anyhow::anyhow!("Failed to parse {field}='{value}': {e}"))
}

pub(crate) fn parse_decimal(value: &str, field: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(value)
        .map_err(|e| anyhow::anyhow!("Failed to parse {field}='{value}' as Decimal: {e}"))
}

/// Returns `Some(value)` unless the value is empty or numerically zero.
pub(crate) fn parse_optional_positive(value: &str) -> Option<&str> {
    (!value.is_empty() && value.parse::<f64>().is_ok_and(|v| v != 0.0)).then_some(value)
}

pub(crate) fn parse_millis_timestamp(value: &str, field: &str) -> anyhow::Result<UnixNanos> {
    let millis: u64 = value
        .parse()
        .with_context(|| format!("Failed to parse {field}='{value}' as u64 millis"))?;
    let nanos = millis
        .checked_mul(NANOSECONDS_IN_MILLISECOND)
        .context("millisecond timestamp overflowed when converting to nanoseconds")?;
    Ok(UnixNanos::from(nanos))
}

fn resolve_settlement_currency(
    settle_coin: &str,
    base_currency: Currency,
    quote_currency: Currency,
) -> anyhow::Result<Currency> {
    if settle_coin.eq_ignore_ascii_case(base_currency.code.as_str()) {
        Ok(base_currency)
    } else if settle_coin.eq_ignore_ascii_case(quote_currency.code.as_str()) {
        Ok(quote_currency)
    } else {
        Err(anyhow::anyhow!(
            "unrecognized settlement currency '{settle_coin}'"
        ))
    }
}

/// Returns a currency from the internal map or creates a new crypto currency.
///
/// Uses [`Currency::get_or_create_crypto`] to handle unknown currency codes,
/// which automatically registers newly listed Bitget assets.
pub fn get_currency(code: &str) -> Currency {
    Currency::get_or_create_crypto(code)
}

fn extract_strike_from_symbol(symbol: &str) -> anyhow::Result<Price> {
    let parts: Vec<&str> = symbol.split('-').collect();
    let strike = parts
        .get(2)
        .ok_or_else(|| anyhow::anyhow!("invalid option symbol '{symbol}'"))?;
    parse_price(strike, "option strike")
}

/// Resolves a Nautilus [`OrderType`] from Bitget order classification fields.
///
/// Bitget represents conditional orders using a combination of `orderType` (Market/Limit),
/// `stopOrderType` (Stop, TakeProfit, StopLoss, etc.), `triggerDirection` (RisesTo/FallsTo),
/// and `side` (Buy/Sell). This function maps all combinations to the appropriate Nautilus
/// conditional order types.
///
/// When `triggerDirection` is `None`, the stop order type is informational only (a parent
/// order with TP/SL metadata attached), so the order is classified as plain Market/Limit.
#[must_use]
pub fn parse_bitget_order_type(
    order_type: BitgetOrderType,
    stop_order_type: BitgetStopOrderType,
    trigger_direction: BitgetTriggerDirection,
    side: BitgetOrderSide,
) -> OrderType {
    if matches!(
        stop_order_type,
        BitgetStopOrderType::None | BitgetStopOrderType::Unknown
    ) {
        return match order_type {
            BitgetOrderType::Market => OrderType::Market,
            BitgetOrderType::Limit | BitgetOrderType::Unknown => OrderType::Limit,
        };
    }

    // Bitget reports no trigger direction: strategy (trigger, TP/SL, trailing) orders are
    // stop orders on the side they execute
    if trigger_direction == BitgetTriggerDirection::None {
        return match order_type {
            BitgetOrderType::Market => OrderType::StopMarket,
            BitgetOrderType::Limit | BitgetOrderType::Unknown => OrderType::StopLimit,
        };
    }

    // TrailingStop maps to StopMarket/StopLimit because Bitget does not
    // provide the trailing offset fields needed for the dedicated types.
    match (order_type, trigger_direction, side) {
        (BitgetOrderType::Market, BitgetTriggerDirection::RisesTo, BitgetOrderSide::Buy) => {
            OrderType::StopMarket
        }
        (BitgetOrderType::Market, BitgetTriggerDirection::FallsTo, BitgetOrderSide::Buy) => {
            OrderType::MarketIfTouched
        }
        (BitgetOrderType::Market, BitgetTriggerDirection::FallsTo, BitgetOrderSide::Sell) => {
            OrderType::StopMarket
        }
        (BitgetOrderType::Market, BitgetTriggerDirection::RisesTo, BitgetOrderSide::Sell) => {
            OrderType::MarketIfTouched
        }
        (BitgetOrderType::Limit, BitgetTriggerDirection::RisesTo, BitgetOrderSide::Buy) => {
            OrderType::StopLimit
        }
        (BitgetOrderType::Limit, BitgetTriggerDirection::FallsTo, BitgetOrderSide::Buy) => {
            OrderType::LimitIfTouched
        }
        (BitgetOrderType::Limit, BitgetTriggerDirection::FallsTo, BitgetOrderSide::Sell) => {
            OrderType::StopLimit
        }
        (BitgetOrderType::Limit, BitgetTriggerDirection::RisesTo, BitgetOrderSide::Sell) => {
            OrderType::LimitIfTouched
        }
        _ => match order_type {
            BitgetOrderType::Market => OrderType::Market,
            BitgetOrderType::Limit | BitgetOrderType::Unknown => OrderType::Limit,
        },
    }
}

/// Parses a Bitget order into a Nautilus OrderStatusReport.
pub fn parse_order_status_report(
    order: &crate::http::models::BitgetOrder,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let instrument_id = instrument.id();
    let venue_order_id = VenueOrderId::new(order.order_id);

    let order_side: Option<OrderSide> = order.side.into();

    let order_type = parse_bitget_order_type(
        order.order_type,
        order.stop_order_type,
        order.trigger_direction,
        order.side,
    );

    let time_in_force: TimeInForce = match order.time_in_force {
        BitgetTimeInForce::Gtc => TimeInForce::Gtc,
        BitgetTimeInForce::Ioc => TimeInForce::Ioc,
        BitgetTimeInForce::Fok => TimeInForce::Fok,
        BitgetTimeInForce::PostOnly | BitgetTimeInForce::Rpi => TimeInForce::Gtc,
    };

    let quantity =
        parse_quantity_with_precision(&order.qty, instrument.size_precision(), "order.qty")?;

    // Strategy orders carry no `cumExecQty`
    let filled_qty = if order.cum_exec_qty.is_empty() {
        Quantity::zero(instrument.size_precision())
    } else {
        parse_quantity_with_precision(
            &order.cum_exec_qty,
            instrument.size_precision(),
            "order.cumExecQty",
        )?
    };

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
        // cancelReason=post_only_fill_cancel (not Rejected). Surface it as Rejected
        // for consistency with the tracked-order event path.
        BitgetOrderStatus::Canceled
            if filled_qty.is_zero() && order.cancel_type == BitgetCancelType::CancelByPostOnly =>
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

    if let Some(avg_price) = &order.avg_price
        && !avg_price.is_empty()
        && avg_price != "0"
    {
        let avg_px = avg_price
            .parse::<Decimal>()
            .with_context(|| format!("Failed to parse avg_price='{avg_price}' as Decimal"))?;
        report = report.with_avg_px(avg_px);
    }

    if !order.trigger_price.is_empty() && order.trigger_price != "0" {
        let trigger_price = parse_price_with_precision(
            &order.trigger_price,
            instrument.price_precision(),
            "order.triggerPrice",
        )?;
        report = report.with_trigger_price(trigger_price);

        // Set trigger_type for conditional orders
        let trigger_type: TriggerType = order.trigger_by.into();
        report = report.with_trigger_type(trigger_type);
    }

    let position_idx = resolve_order_position_idx(&order.hold_mode, order.pos_side);
    if let Some(venue_position_id) = make_hedge_venue_position_id(instrument_id, position_idx) {
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

    Ok(report)
}

/// Returns the `marketUnit` parameter for spot market orders.
#[must_use]
pub fn spot_market_unit(
    product_type: BitgetProductType,
    order_type: BitgetOrderType,
    is_quote_quantity: bool,
) -> Option<BitgetMarketUnit> {
    if product_type == BitgetProductType::Spot && order_type == BitgetOrderType::Market {
        if is_quote_quantity {
            Some(BitgetMarketUnit::QuoteCoin)
        } else {
            Some(BitgetMarketUnit::BaseCoin)
        }
    } else {
        None
    }
}

/// Returns the `isLeverage` parameter (spot-only).
#[must_use]
pub fn spot_leverage(product_type: BitgetProductType, is_leverage: bool) -> Option<i32> {
    if product_type == BitgetProductType::Spot {
        Some(i32::from(is_leverage))
    } else {
        None
    }
}

/// Returns the trigger direction for stop and MIT orders.
#[must_use]
pub fn trigger_direction(
    order_type: OrderType,
    order_side: OrderSide,
    is_stop_order: bool,
) -> Option<BitgetTriggerDirection> {
    if !is_stop_order {
        return None;
    }

    match (order_type, order_side) {
        (OrderType::StopMarket | OrderType::StopLimit, OrderSide::Buy) => {
            Some(BitgetTriggerDirection::RisesTo)
        }
        (OrderType::StopMarket | OrderType::StopLimit, OrderSide::Sell) => {
            Some(BitgetTriggerDirection::FallsTo)
        }
        (OrderType::MarketIfTouched | OrderType::LimitIfTouched, OrderSide::Buy) => {
            Some(BitgetTriggerDirection::FallsTo)
        }
        (OrderType::MarketIfTouched | OrderType::LimitIfTouched, OrderSide::Sell) => {
            Some(BitgetTriggerDirection::RisesTo)
        }
        _ => None,
    }
}

/// Maps Nautilus time-in-force to Bitget's TIF.
///
/// Returns `Err(tif)` with the unsupported value for caller-specific error wrapping.
pub fn map_time_in_force(
    order_type: BitgetOrderType,
    time_in_force: Option<TimeInForce>,
    post_only: Option<bool>,
) -> Result<Option<BitgetTimeInForce>, TimeInForce> {
    if order_type == BitgetOrderType::Market {
        return Ok(None);
    }

    if post_only == Some(true) {
        return Ok(Some(BitgetTimeInForce::PostOnly));
    }

    match time_in_force {
        Some(TimeInForce::Gtc) => Ok(Some(BitgetTimeInForce::Gtc)),
        Some(TimeInForce::Ioc) => Ok(Some(BitgetTimeInForce::Ioc)),
        Some(TimeInForce::Fok) => Ok(Some(BitgetTimeInForce::Fok)),
        Some(tif) => Err(tif),
        None => Ok(None),
    }
}

/// Converts an optional `UnixNanos` timestamp to optional milliseconds.
pub fn nanos_to_millis(value: Option<UnixNanos>) -> Option<i64> {
    value.map(|nanos| nanos_to_millis_u64(nanos.as_u64()) as i64)
}

/// Parsed and validated Bitget TP/SL parameters from a `SubmitOrder.params` map.
#[derive(Debug, Default)]
pub struct BitgetTpSlParams {
    pub take_profit: Option<Price>,
    pub stop_loss: Option<Price>,
    pub tp_trigger_by: Option<BitgetTriggerType>,
    pub sl_trigger_by: Option<BitgetTriggerType>,
    pub tp_order_type: Option<BitgetOrderType>,
    pub sl_order_type: Option<BitgetOrderType>,
    pub tp_limit_price: Option<String>,
    pub sl_limit_price: Option<String>,
    pub tp_trigger_price: Option<String>,
    pub sl_trigger_price: Option<String>,
    pub tpsl_mode: Option<BitgetTpSlMode>,
    pub close_on_trigger: Option<bool>,
    pub is_leverage: bool,
    pub order_iv: Option<String>,
    pub mmp: Option<bool>,
    pub smp_type: Option<BitgetOrderSmpType>,
    pub position_idx: Option<BitgetPositionIdx>,
    pub bbo_side_type: Option<BitgetBboSideType>,
    pub bbo_level: Option<String>,
}

impl BitgetTpSlParams {
    pub fn has_tp_sl(&self) -> bool {
        self.take_profit.is_some() || self.stop_loss.is_some()
    }

    pub fn has_bbo(&self) -> bool {
        self.bbo_side_type.is_some()
    }

    /// Projects the native TP/SL and option fields onto the bundle the HTTP `submit_order` entry
    /// expects. BBO, `position_idx`, `smp_type`, and leverage stay separate because they are
    /// already first-class arguments on the `submit_order` signature.
    #[must_use]
    pub fn to_native_tp_sl(&self) -> BitgetNativeTpSlParams {
        BitgetNativeTpSlParams {
            take_profit: self.take_profit.map(|p| p.to_string()),
            stop_loss: self.stop_loss.map(|p| p.to_string()),
            tp_trigger_by: self.tp_trigger_by,
            sl_trigger_by: self.sl_trigger_by,
            tp_order_type: self.tp_order_type,
            sl_order_type: self.sl_order_type,
            tp_limit_price: self.tp_limit_price.clone(),
            sl_limit_price: self.sl_limit_price.clone(),
            tpsl_mode: self.tpsl_mode,
            close_on_trigger: self.close_on_trigger,
            order_iv: self.order_iv.clone(),
            mmp: self.mmp,
        }
    }
}

/// Extracts a string value from params, accepting both string and numeric JSON values.
pub fn get_price_str(params: &Params, key: &str) -> Option<String> {
    let value = params.get(key)?;
    if let Some(s) = value.as_str() {
        Some(s.to_string())
    } else if let Some(n) = value.as_f64() {
        Some(n.to_string())
    } else if let Some(n) = value.as_i64() {
        Some(n.to_string())
    } else {
        value.as_u64().map(|n| n.to_string())
    }
}

/// Parses a Bitget self-match prevention type from an order parameter or configuration value.
///
/// # Errors
///
/// Returns an error if the value falls outside the four types Bitget accepts on an order.
pub fn parse_smp_type(s: &str) -> anyhow::Result<BitgetOrderSmpType> {
    match s.to_ascii_lowercase().as_str() {
        "none" => Ok(BitgetOrderSmpType::None),
        "cancelmaker" => Ok(BitgetOrderSmpType::CancelMaker),
        "canceltaker" => Ok(BitgetOrderSmpType::CancelTaker),
        "cancelboth" => Ok(BitgetOrderSmpType::CancelBoth),
        _ => anyhow::bail!(
            "invalid Bitget smp_type: '{s}', expected None, CancelMaker, CancelTaker, or CancelBoth"
        ),
    }
}

/// Deserializes an optional self-match prevention type for a client configuration.
///
/// Routes the configured text through [`parse_smp_type`] so a serialized config reports an unknown
/// value instead of carrying it silently.
///
/// # Errors
///
/// Returns an error if the value falls outside the four types Bitget accepts on an order.
pub fn deserialize_optional_smp_type<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<BitgetOrderSmpType>, D::Error> {
    let Some(value) = Option::<String>::deserialize(d)? else {
        return Ok(None);
    };

    parse_smp_type(&value).map(Some).map_err(D::Error::custom)
}

pub fn parse_bbo_side_type(s: &str) -> anyhow::Result<BitgetBboSideType> {
    match s.to_ascii_lowercase().as_str() {
        "queue" => Ok(BitgetBboSideType::Queue),
        "counterparty" => Ok(BitgetBboSideType::Counterparty),
        _ => anyhow::bail!("invalid Bitget bbo_side_type: '{s}', expected Queue or Counterparty"),
    }
}

pub fn parse_bbo_level(s: String) -> anyhow::Result<String> {
    match s.as_str() {
        "1" | "2" | "3" | "4" | "5" => Ok(s),
        _ => anyhow::bail!("invalid 'bbo_level': '{s}', expected 1, 2, 3, 4, or 5"),
    }
}

/// Parses Bitget TP/SL parameters from an optional params map.
pub fn parse_bitget_tp_sl_params(params: Option<&Params>) -> anyhow::Result<BitgetTpSlParams> {
    let Some(params) = params else {
        return Ok(BitgetTpSlParams::default());
    };

    let mut result = BitgetTpSlParams {
        is_leverage: params.get_bool("is_leverage").unwrap_or(false),
        ..Default::default()
    };

    if let Some(s) = get_price_str(params, "take_profit") {
        let p =
            Price::from_str(&s).map_err(|e| anyhow::anyhow!("invalid 'take_profit' price: {e}"))?;

        if p.as_f64() < 0.0 {
            anyhow::bail!("invalid 'take_profit' price: '{s}', expected a non-negative value");
        }
        result.take_profit = Some(p);
    }

    if let Some(s) = get_price_str(params, "stop_loss") {
        let p =
            Price::from_str(&s).map_err(|e| anyhow::anyhow!("invalid 'stop_loss' price: {e}"))?;

        if p.as_f64() < 0.0 {
            anyhow::bail!("invalid 'stop_loss' price: '{s}', expected a non-negative value");
        }
        result.stop_loss = Some(p);
    }

    for (key, setter) in [
        (
            "tp_limit_price",
            &mut result.tp_limit_price as &mut Option<String>,
        ),
        ("sl_limit_price", &mut result.sl_limit_price),
        ("tp_trigger_price", &mut result.tp_trigger_price),
        ("sl_trigger_price", &mut result.sl_trigger_price),
    ] {
        if let Some(s) = get_price_str(params, key) {
            let v: f64 = s
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid price for '{key}': '{s}'"))?;

            if !v.is_finite() || v < 0.0 {
                anyhow::bail!(
                    "invalid price for '{key}': '{s}', expected a finite non-negative number"
                );
            }
            *setter = Some(s);
        }
    }

    if let Some(s) = params.get_str("tp_trigger_by") {
        result.tp_trigger_by = Some(parse_trigger_type(s)?);
    }

    if let Some(s) = params.get_str("sl_trigger_by") {
        result.sl_trigger_by = Some(parse_trigger_type(s)?);
    }

    if let Some(s) = params.get_str("tp_order_type") {
        result.tp_order_type = Some(parse_tp_sl_order_type(s)?);
    }

    if let Some(s) = params.get_str("sl_order_type") {
        result.sl_order_type = Some(parse_tp_sl_order_type(s)?);
    }

    if let Some(s) = params.get_str("tpsl_mode") {
        result.tpsl_mode = Some(parse_tpsl_mode(s)?);
    }

    let has_tp_fields = result.tp_trigger_by.is_some()
        || result.tp_order_type.is_some()
        || result.tp_limit_price.is_some()
        || result.tp_trigger_price.is_some();

    let has_sl_fields = result.sl_trigger_by.is_some()
        || result.sl_order_type.is_some()
        || result.sl_limit_price.is_some()
        || result.sl_trigger_price.is_some();

    if result.take_profit.is_none() && has_tp_fields {
        anyhow::bail!("TP override fields require 'take_profit' to be set");
    }

    if result.stop_loss.is_none() && has_sl_fields {
        anyhow::bail!("SL override fields require 'stop_loss' to be set");
    }

    if result.tp_order_type == Some(BitgetOrderType::Limit) && result.tp_limit_price.is_none() {
        anyhow::bail!("'tp_order_type' is 'Limit' but 'tp_limit_price' was not provided");
    }

    if result.sl_order_type == Some(BitgetOrderType::Limit) && result.sl_limit_price.is_none() {
        anyhow::bail!("'sl_order_type' is 'Limit' but 'sl_limit_price' was not provided");
    }

    if result.tp_limit_price.is_some() && result.tp_order_type != Some(BitgetOrderType::Limit) {
        anyhow::bail!("'tp_limit_price' requires 'tp_order_type' to be 'Limit'");
    }

    if result.sl_limit_price.is_some() && result.sl_order_type != Some(BitgetOrderType::Limit) {
        anyhow::bail!("'sl_limit_price' requires 'sl_order_type' to be 'Limit'");
    }

    result.close_on_trigger = params.get_bool("close_on_trigger");

    if let Some(value) = params.get("order_iv") {
        match get_price_str(params, "order_iv") {
            Some(s) => result.order_iv = Some(s),
            None => {
                anyhow::bail!("invalid type for 'order_iv': {value}, expected string or number")
            }
        }
    }

    if let Some(value) = params.get("mmp") {
        match value.as_bool() {
            Some(b) => result.mmp = Some(b),
            None => anyhow::bail!("invalid type for 'mmp': {value}, expected bool"),
        }
    }

    if let Some(value) = params.get("smp_type") {
        let smp_type = value.as_str().ok_or_else(|| {
            anyhow::anyhow!("invalid type for 'smp_type': {value}, expected string")
        })?;
        result.smp_type = Some(parse_smp_type(smp_type)?);
    }

    if let Some(value) = params.get("position_idx") {
        let idx = value.as_i64().ok_or_else(|| {
            anyhow::anyhow!("invalid type for 'position_idx': {value}, expected integer")
        })?;
        result.position_idx = Some(match idx {
            0 => BitgetPositionIdx::OneWay,
            1 => BitgetPositionIdx::BuyHedge,
            2 => BitgetPositionIdx::SellHedge,
            _ => anyhow::bail!("invalid 'position_idx': {idx}, expected 0, 1, or 2"),
        });
    }

    let has_bbo_side_type = params.get("bbo_side_type").is_some();
    let has_bbo_level = params.get("bbo_level").is_some();

    if has_bbo_side_type != has_bbo_level {
        anyhow::bail!("'bbo_side_type' and 'bbo_level' must be provided together");
    }

    if let Some(value) = params.get("bbo_side_type") {
        let side_type = value.as_str().ok_or_else(|| {
            anyhow::anyhow!("invalid type for 'bbo_side_type': {value}, expected string")
        })?;
        result.bbo_side_type = Some(parse_bbo_side_type(side_type)?);
    }

    if let Some(value) = params.get("bbo_level") {
        let level = if let Some(s) = value.as_str() {
            s.to_string()
        } else if let Some(i) = value.as_i64() {
            i.to_string()
        } else if let Some(u) = value.as_u64() {
            u.to_string()
        } else {
            anyhow::bail!("invalid type for 'bbo_level': {value}, expected string or integer");
        };
        result.bbo_level = Some(parse_bbo_level(level)?);
    }

    Ok(result)
}

pub(crate) fn parse_trigger_type(s: &str) -> anyhow::Result<BitgetTriggerType> {
    match s {
        "LastPrice" => Ok(BitgetTriggerType::LastPrice),
        "MarkPrice" => Ok(BitgetTriggerType::MarkPrice),
        "IndexPrice" => anyhow::bail!(
            "invalid Bitget trigger type: 'IndexPrice' is not supported by Bitget, expected LastPrice or MarkPrice"
        ),
        _ => anyhow::bail!("invalid Bitget trigger type: '{s}', expected LastPrice or MarkPrice"),
    }
}

pub(crate) fn parse_tp_sl_order_type(s: &str) -> anyhow::Result<BitgetOrderType> {
    match s {
        "Market" => Ok(BitgetOrderType::Market),
        "Limit" => Ok(BitgetOrderType::Limit),
        _ => anyhow::bail!("invalid Bitget TP/SL order type: '{s}', expected Market or Limit"),
    }
}

// A plain `serde_json` deserialize would accept unknown strings: `BitgetTpSlMode` carries a
// `#[serde(other)] Unknown` variant, so garbage would silently map to `Unknown`.
pub(crate) fn parse_tpsl_mode(s: &str) -> anyhow::Result<BitgetTpSlMode> {
    match s {
        "Full" => Ok(BitgetTpSlMode::Full),
        "Partial" => Ok(BitgetTpSlMode::Partial),
        _ => anyhow::bail!("invalid Bitget TP/SL mode: '{s}', expected Full or Partial"),
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::BarSpecification,
        enums::{AggregationSource, BarAggregation, PositionSide, PriceType},
    };
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::{
        common::{
            enums::{
                BitgetExecType, BitgetOrderSide, BitgetOrderType, BitgetStopOrderType,
                BitgetTriggerDirection,
            },
            testing::load_test_json,
        },
        http::models::{
            BitgetInstrumentInverseResponse, BitgetInstrumentLinearResponse,
            BitgetInstrumentSpotResponse, BitgetKlinesResponse, BitgetOpenOrdersResponse,
            BitgetPositionListResponse, BitgetTradeHistoryResponse, BitgetTradesResponse,
        },
    };

    const TS: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

    fn linear_instrument() -> InstrumentAny {
        let json = load_test_json("http_get_instruments_linear.json");
        let response: BitgetInstrumentLinearResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];
        parse_linear_instrument(instrument, TS, TS).unwrap()
    }

    // TSLAUSDT is the third row of the linear fixture (the demo account trades it)
    fn tsla_instrument() -> InstrumentAny {
        let json = load_test_json("http_get_instruments_linear.json");
        let response: BitgetInstrumentLinearResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[2];
        assert_eq!(instrument.symbol, "TSLAUSDT");
        parse_linear_instrument(instrument, TS, TS).unwrap()
    }

    #[rstest]
    fn test_parse_price_with_precision_preserves_decimal_value() {
        let price = parse_price_with_precision("9000000.000000001", 9, "test.price").unwrap();

        assert_eq!(price, Price::from("9000000.000000001"));
        assert_eq!(price.precision, 9);
    }

    #[rstest]
    #[case("25.000", 2, "25.00")]
    #[case("9000000.000000001", 9, "9000000.000000001")]
    fn test_parse_quantity_with_precision_preserves_decimal_value(
        #[case] value: &str,
        #[case] precision: u8,
        #[case] expected: &str,
    ) {
        let quantity = parse_quantity_with_precision(value, precision, "test.quantity").unwrap();

        assert_eq!(quantity, Quantity::from(expected));
        assert_eq!(quantity.precision, precision);
    }

    #[rstest]
    #[case::post_only_cross("post_only_fill_cancel", true)]
    #[case::post_only_cross_with_prefix("Order rejected: post_only_fill_cancel", true)]
    #[case::other_reason("ioc_not_full_cancel", false)]
    #[case::generic("Order rejected by venue", false)]
    #[case::empty("", false)]
    fn test_bitget_rejection_due_post_only(#[case] reason: &str, #[case] expected: bool) {
        assert_eq!(bitget_rejection_due_post_only(reason), expected);
    }

    #[rstest]
    fn parse_spot_instrument_builds_currency_pair() {
        let json = load_test_json("http_get_instruments_spot.json");
        let response: BitgetInstrumentSpotResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        let parsed = parse_spot_instrument(instrument, TS, TS).unwrap();
        match parsed {
            InstrumentAny::CurrencyPair(pair) => {
                assert_eq!(pair.id.to_string(), "BTCUSDT-SPOT.BITGET");
                assert_eq!(pair.price_increment, Price::from_str("0.01").unwrap());
                assert_eq!(pair.size_increment, Quantity::from_str("0.000001").unwrap());
                assert_eq!(pair.base_currency.code, "BTC");
                assert_eq!(pair.quote_currency.code, "USDT");
                assert_eq!(
                    pair.min_notional,
                    Some(Money::from_decimal(Decimal::new(1, 0), Currency::USDT()).unwrap()),
                );
                // Bitget reports `0` (no limit) for spot min/max quantity
                assert_eq!(pair.max_quantity, None);
                assert_eq!(
                    pair.info.as_ref().unwrap().get_bool("margin_trading"),
                    Some(false)
                );
            }
            _ => panic!("expected CurrencyPair"),
        }
    }

    #[rstest]
    fn parse_spot_instrument_forwards_symbol_info() {
        let json = load_test_json("http_get_instruments_spot_xstocks.json");
        let response: BitgetInstrumentSpotResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        let parsed = parse_spot_instrument(instrument, TS, TS).unwrap();
        match parsed {
            InstrumentAny::CurrencyPair(pair) => {
                let info = pair.info.as_ref().unwrap();
                assert_eq!(pair.id.to_string(), "RTSLAUSDT-SPOT.BITGET");
                assert_eq!(info.get_bool("margin_trading"), Some(false));
                assert_eq!(info.get_str("symbol_type"), Some("stock"));
                assert_eq!(info.get_str("xstock_multiplier"), None);
                assert_eq!(info.len(), 2);
            }
            other => panic!("unexpected instrument variant: {other:?}"),
        }
    }

    #[rstest]
    #[case::unknown(BitgetSymbolType::Other, "1", None, 1)]
    #[case::non_xstock(BitgetSymbolType::Adventure, "1", Some("adventure"), 2)]
    #[case::empty_xstock_multiplier(BitgetSymbolType::Xstocks, "", Some("xstocks"), 2)]
    fn parse_spot_instrument_omits_inapplicable_symbol_info(
        #[case] symbol_type: BitgetSymbolType,
        #[case] xstock_multiplier: &str,
        #[case] expected_symbol_type: Option<&str>,
        #[case] expected_len: usize,
    ) {
        let json = load_test_json("http_get_instruments_spot.json");
        let response: BitgetInstrumentSpotResponse = serde_json::from_str(&json).unwrap();
        let mut instrument = response.result.list[0].clone();
        instrument.symbol_type = Some(symbol_type);
        instrument.xstock_multiplier = Some(xstock_multiplier.to_string());

        let parsed = parse_spot_instrument(&instrument, TS, TS).unwrap();
        match parsed {
            InstrumentAny::CurrencyPair(pair) => {
                let info = pair.info.as_ref().unwrap();
                assert_eq!(info.get_bool("margin_trading"), Some(false));
                assert_eq!(info.get_str("symbol_type"), expected_symbol_type);
                assert_eq!(info.get("xstock_multiplier"), None);
                assert_eq!(info.len(), expected_len);
            }
            other => panic!("unexpected instrument variant: {other:?}"),
        }
    }

    #[rstest]
    #[case(BitgetMarginTrading::Both, true)]
    #[case(BitgetMarginTrading::UtaOnly, true)]
    #[case(BitgetMarginTrading::None, false)]
    #[case(BitgetMarginTrading::Other, false)]
    fn parse_spot_instrument_maps_margin_trading(
        #[case] margin_trading: BitgetMarginTrading,
        #[case] expected: bool,
    ) {
        let json = load_test_json("http_get_instruments_spot.json");
        let response: BitgetInstrumentSpotResponse = serde_json::from_str(&json).unwrap();
        let mut instrument = response.result.list[0].clone();
        instrument.margin_trading = margin_trading;

        let parsed = parse_spot_instrument(&instrument, TS, TS).unwrap();
        match parsed {
            InstrumentAny::CurrencyPair(pair) => {
                assert_eq!(
                    pair.info.as_ref().unwrap().get_bool("margin_trading"),
                    Some(expected)
                );
            }
            _ => panic!("expected CurrencyPair"),
        }
    }

    #[rstest]
    fn parse_linear_perpetual_instrument_builds_crypto_perpetual() {
        let json = load_test_json("http_get_instruments_linear.json");
        let response: BitgetInstrumentLinearResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        let parsed = parse_linear_instrument(instrument, TS, TS).unwrap();
        match parsed {
            InstrumentAny::CryptoPerpetual(perp) => {
                assert_eq!(perp.id.to_string(), "BTCUSDT-PERP.BITGET");
                assert!(!perp.is_inverse);
                assert_eq!(perp.price_increment, Price::from_str("0.1").unwrap());
                assert_eq!(perp.size_increment, Quantity::from_str("0.0001").unwrap());
                assert_eq!(perp.min_notional, Some(Money::new(5.0, Currency::USDT())),);
                assert_eq!(
                    perp.max_quantity,
                    Some(Quantity::from_str("1200.0000").unwrap())
                );
                let info = perp.info.unwrap();
                assert_eq!(info.get_str("fund_interval"), Some("8"));
                assert_eq!(info.get_str("maker_fee_rate"), Some("0.0002"));
                assert_eq!(info.get_str("taker_fee_rate"), Some("0.0006"));
            }
            other => panic!("unexpected instrument variant: {other:?}"),
        }
    }

    #[rstest]
    #[case::perpetual(BitgetContractType::LinearPerpetual)]
    #[case::future(BitgetContractType::LinearFutures)]
    fn parse_linear_instrument_forwards_symbol_info(#[case] contract_type: BitgetContractType) {
        let json = load_test_json("http_get_instruments_linear_symbol_type.json");
        let response: BitgetInstrumentLinearResponse = serde_json::from_str(&json).unwrap();
        let mut instrument = response.result.list[0].clone();
        if contract_type == BitgetContractType::LinearFutures {
            instrument.contract_kind = "delivery".to_string();
            instrument.delivery_time = "1798185600000".to_string();
        }

        let parsed = parse_linear_instrument(&instrument, TS, TS).unwrap();
        let info = match (contract_type, parsed) {
            (BitgetContractType::LinearPerpetual, InstrumentAny::CryptoPerpetual(perp)) => {
                perp.info.unwrap()
            }
            (BitgetContractType::LinearFutures, InstrumentAny::CryptoFuture(future)) => {
                future.info.unwrap()
            }
            (_, other) => panic!("unexpected instrument variant: {other:?}"),
        };

        assert_eq!(info.get_str("symbol_type"), Some("stock"));
        assert_eq!(info.len(), 4);
    }

    #[rstest]
    fn parse_inverse_perpetual_instrument_builds_inverse_perpetual() {
        let json = load_test_json("http_get_instruments_inverse.json");
        let response: BitgetInstrumentInverseResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        let parsed = parse_inverse_instrument(instrument, TS, TS).unwrap();
        match parsed {
            InstrumentAny::CryptoPerpetual(perp) => {
                assert_eq!(perp.id.to_string(), "BTCUSD_CM-INVERSE.BITGET");
                assert!(perp.is_inverse);
                assert_eq!(perp.price_increment, Price::from_str("0.1").unwrap());
                assert_eq!(perp.size_increment, Quantity::from_str("1").unwrap());
                assert_eq!(perp.settlement_currency.code, "BTC");
                assert_eq!(perp.min_notional, Some(Money::new(5.0, Currency::USD())));
            }
            other => panic!("unexpected instrument variant: {other:?}"),
        }
    }

    #[rstest]
    #[case::perpetual(BitgetContractType::InversePerpetual)]
    #[case::future(BitgetContractType::InverseFutures)]
    fn parse_inverse_instrument_forwards_symbol_info(#[case] contract_type: BitgetContractType) {
        let json = load_test_json("http_get_instruments_inverse_symbol_type.json");
        let response: BitgetInstrumentInverseResponse = serde_json::from_str(&json).unwrap();
        let mut instrument = response.result.list[0].clone();
        if contract_type == BitgetContractType::InversePerpetual {
            instrument.contract_kind = "perpetual".to_string();
        }

        let parsed = parse_inverse_instrument(&instrument, TS, TS).unwrap();
        let info = match (contract_type, parsed) {
            (BitgetContractType::InversePerpetual, InstrumentAny::CryptoPerpetual(perp)) => {
                perp.info.unwrap()
            }
            (BitgetContractType::InverseFutures, InstrumentAny::CryptoFuture(future)) => {
                future.info.unwrap()
            }
            (_, other) => panic!("unexpected instrument variant: {other:?}"),
        };

        assert_eq!(info.get_str("symbol_type"), Some("crypto"));
        assert_eq!(info.len(), 4);
    }

    #[rstest]
    fn test_extract_base_coin_from_option_symbol() {
        assert_eq!(extract_base_coin("BTC-27MAR26-70000-P"), "BTC");
        assert_eq!(extract_base_coin("ETH-26JUN26-16000-C"), "ETH");
        assert_eq!(extract_base_coin("SOL-30MAR26-200-P-USDT"), "SOL");
        assert_eq!(extract_base_coin("BTC"), "BTC");
    }

    #[rstest]
    fn test_extract_base_coin_from_nautilus_option_symbol() {
        // After extract_raw_symbol strips the "-OPTION" suffix
        let raw = extract_raw_symbol("BTC-27MAR26-70000-P-USDT-OPTION");
        assert_eq!(extract_base_coin(raw), "BTC");
    }

    #[rstest]
    fn parse_http_trade_into_trade_tick() {
        let instrument = linear_instrument();
        let json = load_test_json("http_get_trades_recent.json");
        let response: BitgetTradesResponse = serde_json::from_str(&json).unwrap();
        let trade = &response.result.list[0];

        let tick = parse_trade_tick(trade, &instrument, Some(TS)).unwrap();

        assert_eq!(tick.instrument_id, instrument.id());
        assert_eq!(tick.price, instrument.make_price(75228.8));
        assert_eq!(tick.size, instrument.make_qty(0.002, None));
        assert_eq!(tick.aggressor_side, AggressorSide::Sell);
        assert_eq!(tick.trade_id.to_string(), "1503519194437402628");
        assert_eq!(tick.ts_event, UnixNanos::new(1_794_192_830_666_000_000));
    }

    #[rstest]
    fn parse_kline_into_bar() {
        let instrument = linear_instrument();
        let json = load_test_json("http_get_klines_linear.json");
        let response: BitgetKlinesResponse = serde_json::from_str(&json).unwrap();
        let kline = &response.result.list[0];

        let bar_type = BarType::new(
            instrument.id(),
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            AggregationSource::External,
        );

        let bar = parse_kline_bar(kline, &instrument, bar_type, false, Some(TS)).unwrap();

        assert_eq!(bar.bar_type.to_string(), bar_type.to_string());
        assert_eq!(bar.open, instrument.make_price(75225.3));
        assert_eq!(bar.high, instrument.make_price(75265.4));
        assert_eq!(bar.low, instrument.make_price(75208.8));
        assert_eq!(bar.close, instrument.make_price(75265.3));
        assert_eq!(bar.volume, instrument.make_qty(15.9647, None));
        assert_eq!(bar.ts_event, UnixNanos::new(1_794_192_660_000_000_000));
    }

    #[rstest]
    fn parse_http_position_short_into_position_status_report() {
        use crate::http::models::BitgetPositionListResponse;

        let json = load_test_json("http_get_positions.json");
        let response: BitgetPositionListResponse = serde_json::from_str(&json).unwrap();

        // One-way mode keeps an unsigned size with `posSide`; flip the captured long to short
        let mut short_position = response.result.list[0].clone();
        assert_eq!(short_position.symbol, "TSLAUSDT");
        short_position.side = BitgetPositionSide::Sell;

        let tsla_instrument = tsla_instrument();

        let account_id = AccountId::new("BITGET-001");
        let report =
            parse_position_status_report(&short_position, account_id, &tsla_instrument, TS)
                .unwrap();

        // Verify short position is correctly parsed
        assert_eq!(report.account_id, account_id);
        assert_eq!(report.instrument_id.symbol.as_str(), "TSLAUSDT-PERP");
        assert_eq!(report.position_side, PositionSide::Short);
        assert_eq!(report.quantity, tsla_instrument.make_qty(0.02, None));
        assert_eq!(
            report.avg_px_open,
            Some(Decimal::from_str("360.72").unwrap())
        );
        assert_eq!(report.ts_last, UnixNanos::new(1_794_193_099_832_000_000));
    }

    #[rstest]
    fn parse_http_position_preserves_decimal_quantity() {
        use crate::http::models::BitgetPositionListResponse;

        let json = load_test_json("http_get_positions.json");
        let response: BitgetPositionListResponse = serde_json::from_str(&json).unwrap();
        let mut position = response.result.list[0].clone();
        position.size = "9000000.000000001".to_string();

        let json = load_test_json("http_get_instruments_linear.json");
        let response: BitgetInstrumentLinearResponse = serde_json::from_str(&json).unwrap();
        let mut definition = response.result.list[0].clone();
        definition.quantity_multiplier = "0.000000001".to_string();
        definition.quantity_precision = "9".to_string();
        let instrument = parse_linear_instrument(&definition, TS, TS).unwrap();

        let report =
            parse_position_status_report(&position, AccountId::new("BITGET-001"), &instrument, TS)
                .unwrap();

        assert_eq!(report.quantity, Quantity::from("9000000.000000001"));
        assert_eq!(report.quantity.precision, 9);
    }

    #[rstest]
    fn parse_http_order_partially_filled_rejected_maps_to_canceled() {
        use crate::{common::models::BitgetResponse, http::models::BitgetOrder};

        let instrument = tsla_instrument();
        let json = load_test_json("http_get_order_partially_filled_rejected.json");
        let response: BitgetResponse<BitgetOrder> = serde_json::from_str(&json).unwrap();
        let order = &response.result;
        let account_id = AccountId::new("BITGET-001");

        let report = parse_order_status_report(order, &instrument, account_id, TS).unwrap();

        // Bitget reports an IOC remainder as `cancelled` (ioc_not_full_cancel) with fills
        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert_eq!(report.filled_qty, instrument.make_qty(3.71, None));
        assert_eq!(
            report.client_order_id.as_ref().unwrap().to_string(),
            "tdcappart93215"
        );
    }

    #[rstest]
    #[case(BarAggregation::Minute, 1, BitgetKlineInterval::Minute1)]
    #[case(BarAggregation::Minute, 3, BitgetKlineInterval::Minute3)]
    #[case(BarAggregation::Minute, 5, BitgetKlineInterval::Minute5)]
    #[case(BarAggregation::Minute, 15, BitgetKlineInterval::Minute15)]
    #[case(BarAggregation::Minute, 30, BitgetKlineInterval::Minute30)]
    fn test_bar_spec_to_bitget_interval_minutes(
        #[case] aggregation: BarAggregation,
        #[case] step: u64,
        #[case] expected: BitgetKlineInterval,
    ) {
        let result = bar_spec_to_bitget_interval(aggregation, step).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(BarAggregation::Hour, 1, BitgetKlineInterval::Hour1)]
    #[case(BarAggregation::Hour, 2, BitgetKlineInterval::Hour2)]
    #[case(BarAggregation::Hour, 4, BitgetKlineInterval::Hour4)]
    #[case(BarAggregation::Hour, 6, BitgetKlineInterval::Hour6)]
    #[case(BarAggregation::Hour, 12, BitgetKlineInterval::Hour12)]
    fn test_bar_spec_to_bitget_interval_hours(
        #[case] aggregation: BarAggregation,
        #[case] step: u64,
        #[case] expected: BitgetKlineInterval,
    ) {
        let result = bar_spec_to_bitget_interval(aggregation, step).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(BarAggregation::Day, 1, BitgetKlineInterval::Day1)]
    #[case(BarAggregation::Week, 1, BitgetKlineInterval::Week1)]
    #[case(BarAggregation::Month, 1, BitgetKlineInterval::Month1)]
    fn test_bar_spec_to_bitget_interval_day_week_month(
        #[case] aggregation: BarAggregation,
        #[case] step: u64,
        #[case] expected: BitgetKlineInterval,
    ) {
        let result = bar_spec_to_bitget_interval(aggregation, step).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(BarAggregation::Minute, 2)]
    #[case(BarAggregation::Minute, 10)]
    #[case(BarAggregation::Hour, 3)]
    #[case(BarAggregation::Hour, 24)]
    #[case(BarAggregation::Day, 2)]
    #[case(BarAggregation::Week, 2)]
    #[case(BarAggregation::Month, 2)]
    fn test_bar_spec_to_bitget_interval_unsupported_steps(
        #[case] aggregation: BarAggregation,
        #[case] step: u64,
    ) {
        let result = bar_spec_to_bitget_interval(aggregation, step);
        result.unwrap_err();
    }

    #[rstest]
    fn test_bar_spec_to_bitget_interval_unsupported_aggregation() {
        let result = bar_spec_to_bitget_interval(BarAggregation::Second, 1);
        result.unwrap_err();
    }

    #[rstest]
    #[case("1m", 1, BarAggregation::Minute)]
    #[case("3m", 3, BarAggregation::Minute)]
    #[case("5m", 5, BarAggregation::Minute)]
    #[case("15m", 15, BarAggregation::Minute)]
    #[case("30m", 30, BarAggregation::Minute)]
    fn test_bitget_interval_to_bar_spec_minutes(
        #[case] interval: &str,
        #[case] expected_step: usize,
        #[case] expected_aggregation: BarAggregation,
    ) {
        let result = bitget_interval_to_bar_spec(interval).unwrap();
        assert_eq!(result, (expected_step, expected_aggregation));
    }

    #[rstest]
    #[case("1H", 1, BarAggregation::Hour)]
    #[case("2H", 2, BarAggregation::Hour)]
    #[case("4H", 4, BarAggregation::Hour)]
    #[case("6H", 6, BarAggregation::Hour)]
    #[case("12H", 12, BarAggregation::Hour)]
    fn test_bitget_interval_to_bar_spec_hours(
        #[case] interval: &str,
        #[case] expected_step: usize,
        #[case] expected_aggregation: BarAggregation,
    ) {
        let result = bitget_interval_to_bar_spec(interval).unwrap();
        assert_eq!(result, (expected_step, expected_aggregation));
    }

    #[rstest]
    #[case("1D", 1, BarAggregation::Day)]
    #[case("1W", 1, BarAggregation::Week)]
    #[case("1M", 1, BarAggregation::Month)]
    fn test_bitget_interval_to_bar_spec_day_week_month(
        #[case] interval: &str,
        #[case] expected_step: usize,
        #[case] expected_aggregation: BarAggregation,
    ) {
        let result = bitget_interval_to_bar_spec(interval).unwrap();
        assert_eq!(result, (expected_step, expected_aggregation));
    }

    #[rstest]
    #[case("1")]
    #[case("60")]
    #[case("D")]
    #[case("invalid")]
    #[case("")]
    fn test_bitget_interval_to_bar_spec_unsupported(#[case] interval: &str) {
        let result = bitget_interval_to_bar_spec(interval);
        assert!(result.is_none());
    }

    fn params_from(pairs: &[(&str, serde_json::Value)]) -> Params {
        let mut p = Params::new();
        for (k, v) in pairs {
            p.insert(k.to_string(), v.clone());
        }
        p
    }

    #[rstest]
    fn test_parse_tp_sl_params_none_returns_defaults() {
        let result = parse_bitget_tp_sl_params(None).unwrap();
        assert!(!result.is_leverage);
        assert!(!result.has_tp_sl());
        assert!(!result.has_bbo());
        assert!(result.order_iv.is_none());
        assert!(result.mmp.is_none());
    }

    #[rstest]
    fn test_parse_tp_sl_params_empty_returns_defaults() {
        let p = Params::new();
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();
        assert!(!result.is_leverage);
        assert!(!result.has_tp_sl());
        assert!(!result.has_bbo());
        assert!(result.order_iv.is_none());
        assert!(result.mmp.is_none());
    }

    #[rstest]
    fn test_parse_tp_sl_params_valid_full() {
        let p = params_from(&[
            ("take_profit", json!("55000.00")),
            ("stop_loss", json!("47000.00")),
            ("tp_trigger_by", json!("MarkPrice")),
            ("sl_trigger_by", json!("LastPrice")),
            ("tp_order_type", json!("Limit")),
            ("tp_limit_price", json!("54990.00")),
            ("sl_order_type", json!("Market")),
            ("close_on_trigger", json!(true)),
            ("is_leverage", json!(true)),
        ]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();

        assert!(result.has_tp_sl());
        assert!(result.take_profit.is_some());
        assert!(result.stop_loss.is_some());
        assert_eq!(result.tp_trigger_by, Some(BitgetTriggerType::MarkPrice));
        assert_eq!(result.sl_trigger_by, Some(BitgetTriggerType::LastPrice));
        assert_eq!(result.tp_order_type, Some(BitgetOrderType::Limit));
        assert_eq!(result.sl_order_type, Some(BitgetOrderType::Market));
        assert_eq!(result.tp_limit_price.as_deref(), Some("54990.00"));
        assert_eq!(result.close_on_trigger, Some(true));
        assert!(result.is_leverage);
    }

    #[rstest]
    fn test_parse_tp_sl_params_preserves_tpsl_mode() {
        let p = params_from(&[
            ("take_profit", json!("55000.00")),
            ("tpsl_mode", json!("Partial")),
        ]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();

        assert_eq!(result.tpsl_mode, Some(BitgetTpSlMode::Partial));
    }

    #[rstest]
    #[case("Unknown")]
    #[case("partial")]
    #[case("garbage")]
    fn test_parse_tp_sl_params_rejects_invalid_tpsl_mode(#[case] mode: &str) {
        let p = params_from(&[("tpsl_mode", json!(mode))]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();

        assert!(err.to_string().contains("invalid Bitget TP/SL mode"));
    }

    #[rstest]
    #[case("None", BitgetOrderSmpType::None)]
    #[case("CancelMaker", BitgetOrderSmpType::CancelMaker)]
    #[case("canceltaker", BitgetOrderSmpType::CancelTaker)]
    #[case("CANCELBOTH", BitgetOrderSmpType::CancelBoth)]
    fn test_parse_tp_sl_params_valid_smp_type(
        #[case] value: &str,
        #[case] expected: BitgetOrderSmpType,
    ) {
        let p = params_from(&[("smp_type", json!(value))]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();

        assert_eq!(result.smp_type, Some(expected));
    }

    #[rstest]
    fn test_parse_tp_sl_params_smp_type_absent_stays_none() {
        let p = params_from(&[("mmp", json!(true))]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();

        assert_eq!(result.smp_type, None);
    }

    #[rstest]
    #[case(json!("Other"), "invalid Bitget smp_type: 'Other'")]
    #[case(json!("cancel_maker"), "invalid Bitget smp_type: 'cancel_maker'")]
    #[case(json!(""), "invalid Bitget smp_type: ''")]
    #[case(json!(1), "invalid type for 'smp_type'")]
    #[case(json!(true), "invalid type for 'smp_type'")]
    fn test_parse_tp_sl_params_rejects_invalid_smp_type(
        #[case] value: serde_json::Value,
        #[case] expected: &str,
    ) {
        let p = params_from(&[("smp_type", value)]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();

        assert!(
            err.to_string().contains(expected),
            "expected '{expected}', was '{err}'"
        );
    }

    #[rstest]
    fn test_parse_tp_sl_params_valid_bbo() {
        let p = params_from(&[("bbo_side_type", json!("queue")), ("bbo_level", json!(3))]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();

        assert!(result.has_bbo());
        assert_eq!(result.bbo_side_type, Some(BitgetBboSideType::Queue));
        assert_eq!(result.bbo_level.as_deref(), Some("3"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_invalid_bbo() {
        let cases = vec![
            (
                params_from(&[("bbo_side_type", json!("Queue"))]),
                "must be provided together",
            ),
            (
                params_from(&[("bbo_level", json!("1"))]),
                "must be provided together",
            ),
            (
                params_from(&[
                    ("bbo_side_type", json!("invalid")),
                    ("bbo_level", json!("1")),
                ]),
                "invalid Bitget bbo_side_type",
            ),
            (
                params_from(&[("bbo_side_type", json!("Queue")), ("bbo_level", json!("6"))]),
                "invalid 'bbo_level'",
            ),
            (
                params_from(&[("bbo_side_type", json!(1)), ("bbo_level", json!("1"))]),
                "invalid type for 'bbo_side_type'",
            ),
            (
                params_from(&[
                    ("bbo_side_type", json!("Queue")),
                    ("bbo_level", json!(true)),
                ]),
                "invalid type for 'bbo_level'",
            ),
        ];

        for (p, expected) in cases {
            let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
            assert!(err.to_string().contains(expected));
        }
    }

    #[rstest]
    #[case("abc")]
    #[case("nan")]
    #[case("inf")]
    #[case("-1.0")]
    fn test_parse_tp_sl_params_rejects_invalid_take_profit(#[case] price: &str) {
        let p = params_from(&[("take_profit", json!(price))]);
        parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
    }

    #[rstest]
    #[case("abc")]
    #[case("nan")]
    #[case("inf")]
    fn test_parse_tp_sl_params_rejects_invalid_stop_loss(#[case] price: &str) {
        let p = params_from(&[("stop_loss", json!(price))]);
        parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
    }

    #[rstest]
    #[case("nan")]
    #[case("inf")]
    #[case("-5.0")]
    #[case("not_a_number")]
    fn test_parse_tp_sl_params_rejects_invalid_limit_price(#[case] price: &str) {
        let p = params_from(&[
            ("take_profit", json!("55000.00")),
            ("tp_order_type", json!("Limit")),
            ("tp_limit_price", json!(price)),
        ]);
        parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_index_price_trigger() {
        let p = params_from(&[
            ("stop_loss", json!("47000.00")),
            ("sl_trigger_by", json!("IndexPrice")),
        ]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("not supported by Bitget"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_invalid_trigger_type() {
        let p = params_from(&[
            ("take_profit", json!("55000.00")),
            ("tp_trigger_by", json!("InvalidType")),
        ]);
        parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_invalid_order_type() {
        let p = params_from(&[
            ("stop_loss", json!("47000.00")),
            ("sl_order_type", json!("Stop")),
        ]);
        parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_limit_without_limit_price() {
        let p = params_from(&[
            ("take_profit", json!("55000.00")),
            ("tp_order_type", json!("Limit")),
        ]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("tp_limit_price"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_limit_price_without_limit_type() {
        let p = params_from(&[
            ("take_profit", json!("55000.00")),
            ("tp_limit_price", json!("54990.00")),
        ]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("tp_order_type"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_orphaned_tp_fields() {
        let p = params_from(&[("tp_trigger_by", json!("MarkPrice"))]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("TP override fields require"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_accepts_numeric_prices() {
        let p = params_from(&[("take_profit", json!(55000.0)), ("stop_loss", json!(47000))]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();
        assert!(result.take_profit.is_some());
        assert!(result.stop_loss.is_some());
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_orphaned_sl_fields() {
        let p = params_from(&[("sl_trigger_by", json!("LastPrice"))]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("SL override fields require"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_bool_order_iv() {
        let p = params_from(&[("order_iv", json!(true))]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("order_iv"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_rejects_string_mmp() {
        let p = params_from(&[("mmp", json!("true"))]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("mmp"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_order_iv_string() {
        let p = params_from(&[("order_iv", json!("0.75"))]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();
        assert_eq!(result.order_iv.as_deref(), Some("0.75"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_order_iv_numeric() {
        let p = params_from(&[("order_iv", json!(0.75))]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();
        assert_eq!(result.order_iv.as_deref(), Some("0.75"));
    }

    #[rstest]
    fn test_parse_tp_sl_params_mmp() {
        let p = params_from(&[("mmp", json!(true))]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();
        assert_eq!(result.mmp, Some(true));
    }

    #[rstest]
    #[case(0, BitgetPositionIdx::OneWay)]
    #[case(1, BitgetPositionIdx::BuyHedge)]
    #[case(2, BitgetPositionIdx::SellHedge)]
    fn test_parse_tp_sl_params_position_idx_valid(
        #[case] idx: i64,
        #[case] expected: BitgetPositionIdx,
    ) {
        let p = params_from(&[("position_idx", json!(idx))]);
        let result = parse_bitget_tp_sl_params(Some(&p)).unwrap();
        assert_eq!(result.position_idx, Some(expected));
    }

    #[rstest]
    #[case(json!(3))]
    #[case(json!(-1))]
    #[case(json!("1"))]
    #[case(json!(true))]
    fn test_parse_tp_sl_params_position_idx_invalid(#[case] value: serde_json::Value) {
        let p = params_from(&[("position_idx", value)]);
        let err = parse_bitget_tp_sl_params(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("position_idx"));
    }

    #[rstest]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::TakeProfit,
        BitgetTriggerDirection::RisesTo,
        BitgetOrderSide::Sell,
        OrderType::MarketIfTouched
    )]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::StopLoss,
        BitgetTriggerDirection::FallsTo,
        BitgetOrderSide::Sell,
        OrderType::StopMarket
    )]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::TakeProfit,
        BitgetTriggerDirection::FallsTo,
        BitgetOrderSide::Buy,
        OrderType::MarketIfTouched
    )]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::StopLoss,
        BitgetTriggerDirection::RisesTo,
        BitgetOrderSide::Buy,
        OrderType::StopMarket
    )]
    #[case(
        BitgetOrderType::Limit,
        BitgetStopOrderType::TakeProfit,
        BitgetTriggerDirection::RisesTo,
        BitgetOrderSide::Sell,
        OrderType::LimitIfTouched
    )]
    #[case(
        BitgetOrderType::Limit,
        BitgetStopOrderType::StopLoss,
        BitgetTriggerDirection::FallsTo,
        BitgetOrderSide::Sell,
        OrderType::StopLimit
    )]
    #[case(
        BitgetOrderType::Limit,
        BitgetStopOrderType::PartialTakeProfit,
        BitgetTriggerDirection::FallsTo,
        BitgetOrderSide::Buy,
        OrderType::LimitIfTouched
    )]
    #[case(
        BitgetOrderType::Limit,
        BitgetStopOrderType::PartialStopLoss,
        BitgetTriggerDirection::RisesTo,
        BitgetOrderSide::Buy,
        OrderType::StopLimit
    )]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::TpslOrder,
        BitgetTriggerDirection::FallsTo,
        BitgetOrderSide::Sell,
        OrderType::StopMarket
    )]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::Stop,
        BitgetTriggerDirection::RisesTo,
        BitgetOrderSide::Buy,
        OrderType::StopMarket
    )]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::Stop,
        BitgetTriggerDirection::FallsTo,
        BitgetOrderSide::Sell,
        OrderType::StopMarket
    )]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::TrailingStop,
        BitgetTriggerDirection::FallsTo,
        BitgetOrderSide::Sell,
        OrderType::StopMarket
    )]
    #[case(
        BitgetOrderType::Limit,
        BitgetStopOrderType::TrailingStop,
        BitgetTriggerDirection::RisesTo,
        BitgetOrderSide::Buy,
        OrderType::StopLimit
    )]
    fn test_parse_bitget_order_type_conditional(
        #[case] order_type: BitgetOrderType,
        #[case] stop_order_type: BitgetStopOrderType,
        #[case] trigger_direction: BitgetTriggerDirection,
        #[case] side: BitgetOrderSide,
        #[case] expected: OrderType,
    ) {
        let result = parse_bitget_order_type(order_type, stop_order_type, trigger_direction, side);
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::None,
        BitgetTriggerDirection::None,
        BitgetOrderSide::Buy,
        OrderType::Market
    )]
    #[case(
        BitgetOrderType::Limit,
        BitgetStopOrderType::Unknown,
        BitgetTriggerDirection::None,
        BitgetOrderSide::Sell,
        OrderType::Limit
    )]
    // Bitget strategy orders report no trigger direction: they are stop orders
    #[case(
        BitgetOrderType::Market,
        BitgetStopOrderType::Stop,
        BitgetTriggerDirection::None,
        BitgetOrderSide::Sell,
        OrderType::StopMarket
    )]
    #[case(
        BitgetOrderType::Limit,
        BitgetStopOrderType::TpslOrder,
        BitgetTriggerDirection::None,
        BitgetOrderSide::Buy,
        OrderType::StopLimit
    )]
    fn test_parse_bitget_order_type_plain(
        #[case] order_type: BitgetOrderType,
        #[case] stop_order_type: BitgetStopOrderType,
        #[case] trigger_direction: BitgetTriggerDirection,
        #[case] side: BitgetOrderSide,
        #[case] expected: OrderType,
    ) {
        let result = parse_bitget_order_type(order_type, stop_order_type, trigger_direction, side);
        assert_eq!(result, expected);
    }

    // Bitget conditional orders are `trigger` strategy orders (captured on demo)
    #[rstest]
    fn test_parse_order_status_report_take_profit() {
        let instrument = tsla_instrument();
        let json = load_test_json("http_get_strategy_orders_history.json");
        let response: BitgetOpenOrdersResponse = serde_json::from_str(&json).unwrap();
        let order = &response.result.list[0];
        let account_id = AccountId::new("BITGET-001");

        let report = parse_order_status_report(order, &instrument, account_id, TS).unwrap();

        assert_eq!(report.order_type, OrderType::StopMarket);
        assert_eq!(report.order_side, OrderSide::Buy.into());
        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert!(report.trigger_price.is_some());
        assert_eq!(
            report.trigger_price.unwrap(),
            Price::from_str("396.89").unwrap()
        );
        assert_eq!(report.trigger_type, Some(TriggerType::MarkPrice));
        assert!(!report.reduce_only);
    }

    #[rstest]
    fn test_parse_order_status_report_stop_loss_limit() {
        let instrument = tsla_instrument();
        let json = load_test_json("http_get_strategy_orders_history.json");
        let response: BitgetOpenOrdersResponse = serde_json::from_str(&json).unwrap();
        let mut order = response.result.list[0].clone();
        order.order_type = BitgetOrderType::Limit;
        order.price = "397.50".to_string();
        order.order_status = crate::common::enums::BitgetOrderStatus::Untriggered;
        let account_id = AccountId::new("BITGET-001");

        let report = parse_order_status_report(&order, &instrument, account_id, TS).unwrap();

        assert_eq!(report.order_type, OrderType::StopLimit);
        assert_eq!(report.order_side, OrderSide::Buy.into());
        assert_eq!(report.order_status, OrderStatus::Accepted);
        assert_eq!(
            report.trigger_price.unwrap(),
            Price::from_str("396.89").unwrap()
        );
        assert_eq!(report.price.unwrap(), Price::from_str("397.50").unwrap());
        assert_eq!(report.trigger_type, Some(TriggerType::MarkPrice));
    }

    #[rstest]
    fn test_parse_order_status_report_post_only_cancel_is_rejected() {
        let instrument = tsla_instrument();
        let json = load_test_json("http_get_orders_realtime.json");
        let response: BitgetOpenOrdersResponse = serde_json::from_str(&json).unwrap();
        let mut order = response.result.list[0].clone();
        order.order_status = crate::common::enums::BitgetOrderStatus::Canceled;
        order.cancel_type = BitgetCancelType::CancelByPostOnly;
        let account_id = AccountId::new("BITGET-001");

        let report = parse_order_status_report(&order, &instrument, account_id, TS).unwrap();

        assert_eq!(report.order_status, OrderStatus::Rejected);
        assert!(report.post_only);
    }

    #[rstest]
    #[case::oneway(0, "BTCUSDT-PERP.BITGET-ONEWAY")]
    #[case::long(1, "BTCUSDT-PERP.BITGET-LONG")]
    #[case::short(2, "BTCUSDT-PERP.BITGET-SHORT")]
    #[case::unknown(99, "BTCUSDT-PERP.BITGET-UNKNOWN")]
    fn test_make_venue_position_id(#[case] position_idx: i32, #[case] expected: &str) {
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
        let result = make_venue_position_id(instrument_id, position_idx);
        assert_eq!(result, PositionId::from(expected));
    }

    #[rstest]
    #[case::oneway(0, None)]
    #[case::long(1, Some("BTCUSDT-PERP.BITGET-LONG"))]
    #[case::short(2, Some("BTCUSDT-PERP.BITGET-SHORT"))]
    #[case::unknown(99, None)]
    fn test_make_hedge_venue_position_id(
        #[case] position_idx: i32,
        #[case] expected: Option<&str>,
    ) {
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
        let result = make_hedge_venue_position_id(instrument_id, position_idx);
        assert_eq!(result, expected.map(PositionId::from));
    }

    #[rstest]
    #[case::buy_open(BitgetOrderSide::Buy, false, BitgetPositionIdx::BuyHedge)]
    #[case::sell_open(BitgetOrderSide::Sell, false, BitgetPositionIdx::SellHedge)]
    #[case::sell_close_long(BitgetOrderSide::Sell, true, BitgetPositionIdx::BuyHedge)]
    #[case::buy_close_short(BitgetOrderSide::Buy, true, BitgetPositionIdx::SellHedge)]
    fn test_resolve_position_idx_hedge_mode(
        #[case] side: BitgetOrderSide,
        #[case] is_reduce_only: bool,
        #[case] expected: BitgetPositionIdx,
    ) {
        let idx = resolve_position_idx(
            Some(BitgetPositionMode::BothSides),
            side,
            is_reduce_only,
            None,
        );
        assert_eq!(idx, Some(expected));
    }

    #[rstest]
    fn test_resolve_position_idx_one_way_mode() {
        let idx = resolve_position_idx(
            Some(BitgetPositionMode::MergedSingle),
            BitgetOrderSide::Buy,
            false,
            None,
        );
        assert_eq!(idx, Some(BitgetPositionIdx::OneWay));
    }

    #[rstest]
    fn test_resolve_position_idx_manual_override_wins() {
        let idx = resolve_position_idx(
            Some(BitgetPositionMode::BothSides),
            BitgetOrderSide::Buy,
            false,
            Some(BitgetPositionIdx::SellHedge),
        );
        assert_eq!(idx, Some(BitgetPositionIdx::SellHedge));
    }

    #[rstest]
    fn test_resolve_position_idx_returns_none_when_unconfigured() {
        let idx = resolve_position_idx(None, BitgetOrderSide::Buy, false, None);
        assert!(idx.is_none());
    }

    #[rstest]
    fn test_parse_fill_report_venue_position_id_is_none() {
        let instrument = tsla_instrument();
        let json = load_test_json("http_get_executions.json");
        let response: BitgetTradeHistoryResponse = serde_json::from_str(&json).unwrap();
        let execution = &response.result.list[0];
        let account_id = AccountId::new("BITGET-001");

        let report = parse_fill_report(execution, account_id, &instrument, TS).unwrap();

        assert_eq!(report.venue_position_id, None);
    }

    #[rstest]
    #[case::rebase(
        "dte_rebase_buy_in_single_side_mode",
        BitgetExecType::CorporateAction,
        true
    )]
    #[case::liquidation("burst_sell_single", BitgetExecType::BustTrade, true)]
    #[case::adl("dte_sys_adl_sell_in_single_side_mode", BitgetExecType::AdlTrade, true)]
    #[case::delivery("delivery_close_long", BitgetExecType::Delivery, true)]
    #[case::one_way("buy_single", BitgetExecType::Trade, false)]
    #[case::hedge("open", BitgetExecType::Trade, false)]
    fn test_parse_http_exec_type_fill_report(
        #[case] trade_side: &str,
        #[case] expected: BitgetExecType,
        #[case] exchange_generated: bool,
    ) {
        let instrument = tsla_instrument();
        let json = load_test_json("http_get_executions.json");
        let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
        value["data"]["list"][0]["tradeSide"] = json!(trade_side);
        let response: BitgetTradeHistoryResponse = serde_json::from_value(value).unwrap();
        let execution = &response.result.list[0];
        let account_id = AccountId::new("BITGET-001");

        assert_eq!(execution.exec_type, expected);
        assert_eq!(
            execution.exec_type.is_exchange_generated(),
            exchange_generated
        );

        let report = parse_fill_report(execution, account_id, &instrument, TS).unwrap();

        assert_eq!(
            report.venue_order_id,
            VenueOrderId::from("1503520820376788992")
        );
        // Bitget reports the fee paid as a positive amount in `feeDetail`
        assert_eq!(report.commission, Money::from("0.53523428 USDT"));
        assert_eq!(report.liquidity_side, LiquiditySide::Taker);
    }

    #[rstest]
    fn test_parse_order_status_report_venue_position_id_for_hedge() {
        let instrument = tsla_instrument();
        let json = load_test_json("http_get_orders_history.json");
        let response: BitgetOpenOrdersResponse = serde_json::from_str(&json).unwrap();
        let mut order = response.result.list[0].clone();
        order.hold_mode = "hedge_mode".to_string();
        order.pos_side = BitgetPositionSide::Sell;
        let account_id = AccountId::new("BITGET-001");

        let report = parse_order_status_report(&order, &instrument, account_id, TS).unwrap();

        assert_eq!(
            report.venue_position_id,
            Some(PositionId::from("TSLAUSDT-PERP.BITGET-SHORT"))
        );
    }

    #[rstest]
    fn test_parse_position_status_report_venue_position_id_for_hedge() {
        let json = load_test_json("http_get_positions.json");
        let response: BitgetPositionListResponse = serde_json::from_str(&json).unwrap();
        let mut position = response.result.list[0].clone();
        position.hold_mode = "hedge_mode".to_string();
        let instrument = tsla_instrument();
        let account_id = AccountId::new("BITGET-001");

        let report = parse_position_status_report(&position, account_id, &instrument, TS).unwrap();

        assert_eq!(
            report.venue_position_id,
            Some(PositionId::from("TSLAUSDT-PERP.BITGET-LONG"))
        );
    }

    #[rstest]
    fn test_parse_order_status_report_venue_position_id_is_none() {
        let instrument = tsla_instrument();
        let json = load_test_json("http_get_orders_history.json");
        let response: BitgetOpenOrdersResponse = serde_json::from_str(&json).unwrap();
        let order = &response.result.list[0]; // one-way mode order
        let account_id = AccountId::new("BITGET-001");

        let report = parse_order_status_report(order, &instrument, account_id, TS).unwrap();

        assert_eq!(report.venue_position_id, None);
    }
}
