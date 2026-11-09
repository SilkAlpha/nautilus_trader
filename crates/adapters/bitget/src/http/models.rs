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

//! Data transfer objects for deserializing Bitget HTTP API payloads.

use nautilus_core::string::secret::SecretString;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use crate::common::{
    enums::{
        BitgetAccountType, BitgetApiKeyType, BitgetCancelType, BitgetContractType,
        BitgetCreateType, BitgetExecType, BitgetInnovationFlag, BitgetInstrumentStatus,
        BitgetMarginMode, BitgetMarginTrading, BitgetOptionType, BitgetOrderSide,
        BitgetOrderStatus, BitgetOrderType, BitgetPositionIdx, BitgetPositionMode,
        BitgetPositionSide, BitgetPositionStatus, BitgetProductType, BitgetRepayStatus,
        BitgetSmpType, BitgetStopOrderType, BitgetSymbolType, BitgetTimeInForce, BitgetTpSlMode,
        BitgetTriggerDirection, BitgetTriggerType, BitgetUnifiedMarginStatus,
    },
    models::{
        BitgetCursorList, BitgetCursorListResponse, BitgetListResponse, BitgetResponse,
        LeverageFilter, LinearLotSizeFilter, LinearPriceFilter, OptionLotSizeFilter,
        SpotLotSizeFilter, SpotPriceFilter,
    },
    parse::{
        bool_or_int, deserialize_decimal_or_zero, deserialize_fee_detail_sum,
        deserialize_i32_or_string, deserialize_i64_or_string, deserialize_null_default,
        deserialize_optional_decimal_or_zero, deserialize_string_or_null, masked_secret,
        yes_no_bool,
    },
};

/// Cursor-paginated list of orders for Python bindings.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetOrderCursorList {
    /// Collection of orders returned by the endpoint.
    pub list: Vec<BitgetOrder>,
    /// Pagination cursor for the next page.
    pub next_page_cursor: Option<String>,
    /// Optional product category when the API includes it.
    #[serde(default)]
    pub category: Option<BitgetProductType>,
}

impl From<BitgetCursorList<BitgetOrder>> for BitgetOrderCursorList {
    fn from(cursor_list: BitgetCursorList<BitgetOrder>) -> Self {
        Self {
            list: cursor_list.list,
            next_page_cursor: cursor_list.next_page_cursor,
            category: cursor_list.category,
        }
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl BitgetOrderCursorList {
    #[getter]
    #[must_use]
    pub fn list(&self) -> Vec<BitgetOrder> {
        self.list.clone()
    }

    #[getter]
    #[must_use]
    pub fn next_page_cursor(&self) -> Option<&str> {
        self.next_page_cursor.as_deref()
    }

    #[getter]
    #[must_use]
    pub fn category(&self) -> Option<BitgetProductType> {
        self.category
    }
}

/// Response payload returned by `GET /api/v3/market/time`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
#[serde(rename_all = "camelCase")]
pub struct BitgetServerTime {
    /// Server timestamp in milliseconds represented as string.
    pub server_time: String,
    /// Server timestamp in microseconds represented as string.
    pub server_time_micros: String,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl BitgetServerTime {
    #[getter]
    #[must_use]
    pub fn time_second(&self) -> String {
        self.time_second_str()
    }

    #[getter]
    #[must_use]
    pub fn time_nano(&self) -> String {
        self.time_nano_str()
    }
}

impl BitgetServerTime {
    /// Returns the server time in whole seconds (Bitget reports milliseconds).
    #[must_use]
    pub fn time_second_str(&self) -> String {
        self.server_time
            .parse::<u64>()
            .map_or_else(|_| String::new(), |ms| (ms / 1_000).to_string())
    }

    /// Returns the server time in nanoseconds (Bitget reports microseconds).
    #[must_use]
    pub fn time_nano_str(&self) -> String {
        self.server_time_micros
            .parse::<u64>()
            .map_or_else(|_| String::new(), |us| (us * 1_000).to_string())
    }
}

/// Type alias for the server time response envelope.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetServerTimeResponse = BitgetResponse<BitgetServerTime>;

/// Ticker payload for spot instruments.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetTickerSpot {
    pub symbol: Ustr,
    pub bid1_price: String,
    pub bid1_size: String,
    pub ask1_price: String,
    pub ask1_size: String,
    pub last_price: String,
    #[serde(rename = "openPrice24h")]
    pub prev_price24h: String,
    pub price24h_pcnt: String,
    pub high_price24h: String,
    pub low_price24h: String,
    pub turnover24h: String,
    pub volume24h: String,
    #[serde(default)]
    pub usd_index_price: String,
}

/// Ticker payload for linear and inverse perpetual/futures instruments.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetTickerLinear {
    pub symbol: Ustr,
    pub last_price: String,
    pub index_price: String,
    pub mark_price: String,
    #[serde(rename = "openPrice24h")]
    pub prev_price24h: String,
    pub price24h_pcnt: String,
    pub high_price24h: String,
    pub low_price24h: String,
    #[serde(default)]
    pub prev_price1h: String,
    pub open_interest: String,
    #[serde(default)]
    pub open_interest_value: String,
    pub turnover24h: String,
    pub volume24h: String,
    pub funding_rate: String,
    /// Not part of the REST ticker (WebSocket `ticker` carries it).
    #[serde(default)]
    pub next_funding_time: String,
    #[serde(default)]
    pub predicted_delivery_price: String,
    #[serde(default)]
    pub basis_rate: String,
    #[serde(default)]
    pub delivery_fee_rate: String,
    #[serde(default)]
    pub delivery_time: String,
    pub ask1_size: String,
    pub bid1_price: String,
    pub ask1_price: String,
    pub bid1_size: String,
    #[serde(default)]
    pub basis: String,
}

/// Ticker payload for option instruments (Bitget lists no options; kept for API parity).
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetTickerOption {
    pub symbol: Ustr,
    pub bid1_price: String,
    pub bid1_size: String,
    pub bid1_iv: String,
    pub ask1_price: String,
    pub ask1_size: String,
    pub ask1_iv: String,
    pub last_price: String,
    pub high_price24h: String,
    pub low_price24h: String,
    pub mark_price: String,
    pub index_price: String,
    pub mark_iv: String,
    pub underlying_price: String,
    pub open_interest: String,
    pub turnover24h: String,
    pub volume24h: String,
    pub total_volume: String,
    pub total_turnover: String,
    pub delta: String,
    pub gamma: String,
    pub vega: String,
    pub theta: String,
    pub predicted_delivery_price: String,
    pub change24h: String,
}

/// Response alias for spot ticker requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetTickersSpotResponse = BitgetListResponse<BitgetTickerSpot>;
/// Response alias for linear/inverse ticker requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetTickersLinearResponse = BitgetListResponse<BitgetTickerLinear>;
/// Response alias for option ticker requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetTickersOptionResponse = BitgetListResponse<BitgetTickerOption>;

/// Unified ticker data structure containing common fields across all product types.
///
/// This simplified ticker structure is designed to work across SPOT, LINEAR, and OPTION products,
/// containing only the most commonly used fields.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetTickerData {
    pub symbol: Ustr,
    pub bid1_price: String,
    pub bid1_size: String,
    pub ask1_price: String,
    pub ask1_size: String,
    pub last_price: String,
    pub high_price24h: String,
    pub low_price24h: String,
    pub turnover24h: String,
    pub volume24h: String,
    #[serde(default)]
    pub open_interest: Option<String>,
    #[serde(default)]
    pub funding_rate: Option<String>,
    #[serde(default)]
    pub next_funding_time: Option<String>,
    #[serde(default)]
    pub mark_price: Option<String>,
    #[serde(default)]
    pub index_price: Option<String>,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl BitgetTickerData {
    #[getter]
    #[must_use]
    pub fn symbol(&self) -> &str {
        self.symbol.as_str()
    }

    #[getter]
    #[must_use]
    pub fn bid1_price(&self) -> &str {
        &self.bid1_price
    }

    #[getter]
    #[must_use]
    pub fn bid1_size(&self) -> &str {
        &self.bid1_size
    }

    #[getter]
    #[must_use]
    pub fn ask1_price(&self) -> &str {
        &self.ask1_price
    }

    #[getter]
    #[must_use]
    pub fn ask1_size(&self) -> &str {
        &self.ask1_size
    }

    #[getter]
    #[must_use]
    pub fn last_price(&self) -> &str {
        &self.last_price
    }

    #[getter]
    #[must_use]
    pub fn high_price24h(&self) -> &str {
        &self.high_price24h
    }

    #[getter]
    #[must_use]
    pub fn low_price24h(&self) -> &str {
        &self.low_price24h
    }

    #[getter]
    #[must_use]
    pub fn turnover24h(&self) -> &str {
        &self.turnover24h
    }

    #[getter]
    #[must_use]
    pub fn volume24h(&self) -> &str {
        &self.volume24h
    }

    #[getter]
    #[must_use]
    pub fn open_interest(&self) -> Option<&str> {
        self.open_interest.as_deref()
    }

    #[getter]
    #[must_use]
    pub fn funding_rate(&self) -> Option<&str> {
        self.funding_rate.as_deref()
    }

    #[getter]
    #[must_use]
    pub fn next_funding_time(&self) -> Option<&str> {
        self.next_funding_time.as_deref()
    }

    #[getter]
    #[must_use]
    pub fn mark_price(&self) -> Option<&str> {
        self.mark_price.as_deref()
    }

    #[getter]
    #[must_use]
    pub fn index_price(&self) -> Option<&str> {
        self.index_price.as_deref()
    }
}

impl From<BitgetTickerSpot> for BitgetTickerData {
    fn from(ticker: BitgetTickerSpot) -> Self {
        Self {
            symbol: ticker.symbol,
            bid1_price: ticker.bid1_price,
            bid1_size: ticker.bid1_size,
            ask1_price: ticker.ask1_price,
            ask1_size: ticker.ask1_size,
            last_price: ticker.last_price,
            high_price24h: ticker.high_price24h,
            low_price24h: ticker.low_price24h,
            turnover24h: ticker.turnover24h,
            volume24h: ticker.volume24h,
            open_interest: None,
            funding_rate: None,
            next_funding_time: None,
            mark_price: None,
            index_price: None,
        }
    }
}

impl From<BitgetTickerLinear> for BitgetTickerData {
    fn from(ticker: BitgetTickerLinear) -> Self {
        Self {
            symbol: ticker.symbol,
            bid1_price: ticker.bid1_price,
            bid1_size: ticker.bid1_size,
            ask1_price: ticker.ask1_price,
            ask1_size: ticker.ask1_size,
            last_price: ticker.last_price,
            high_price24h: ticker.high_price24h,
            low_price24h: ticker.low_price24h,
            turnover24h: ticker.turnover24h,
            volume24h: ticker.volume24h,
            open_interest: Some(ticker.open_interest),
            funding_rate: Some(ticker.funding_rate),
            next_funding_time: Some(ticker.next_funding_time),
            mark_price: Some(ticker.mark_price),
            index_price: Some(ticker.index_price),
        }
    }
}

impl From<BitgetTickerOption> for BitgetTickerData {
    fn from(ticker: BitgetTickerOption) -> Self {
        Self {
            symbol: ticker.symbol,
            bid1_price: ticker.bid1_price,
            bid1_size: ticker.bid1_size,
            ask1_price: ticker.ask1_price,
            ask1_size: ticker.ask1_size,
            last_price: ticker.last_price,
            high_price24h: ticker.high_price24h,
            low_price24h: ticker.low_price24h,
            turnover24h: ticker.turnover24h,
            volume24h: ticker.volume24h,
            open_interest: Some(ticker.open_interest),
            funding_rate: None,
            next_funding_time: None,
            mark_price: Some(ticker.mark_price),
            index_price: Some(ticker.index_price),
        }
    }
}

/// Kline/candlestick entry returned by `GET /api/v3/market/candles`.
///
/// Bitget returns klines as arrays with 7 elements:
/// [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize)]
pub struct BitgetKline {
    pub start: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub turnover: String,
}

impl<'de> Deserialize<'de> for BitgetKline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let [start, open, high, low, close, volume, turnover]: [String; 7] =
            Deserialize::deserialize(deserializer)?;
        Ok(Self {
            start,
            open,
            high,
            low,
            close,
            volume,
            turnover,
        })
    }
}

/// Kline list result returned by Bitget.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
///
/// Bitget returns the candles as a bare array in `data`; `category` and `symbol` are not echoed
/// and are set by the caller.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "Vec<BitgetKline>")]
pub struct BitgetKlineResult {
    pub category: BitgetProductType,
    pub symbol: Ustr,
    pub list: Vec<BitgetKline>,
}

impl From<Vec<BitgetKline>> for BitgetKlineResult {
    fn from(list: Vec<BitgetKline>) -> Self {
        Self {
            category: BitgetProductType::default(),
            symbol: Ustr::default(),
            list,
        }
    }
}

/// Response alias for kline history requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetKlinesResponse = BitgetResponse<BitgetKlineResult>;

/// Trade entry returned by `GET /api/v3/market/fills`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetTrade {
    pub exec_id: String,
    /// Not echoed by Bitget; set by the caller.
    #[serde(default)]
    pub symbol: Ustr,
    pub price: String,
    pub size: String,
    pub side: BitgetOrderSide,
    #[serde(rename = "ts")]
    pub time: String,
    #[serde(default)]
    pub is_block_trade: bool,
    #[serde(default)]
    pub m_p: Option<String>,
    #[serde(default)]
    pub i_p: Option<String>,
    #[serde(default)]
    pub mlv: Option<String>,
    #[serde(default)]
    pub iv: Option<String>,
}

/// Trade list result returned by Bitget.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
///
/// Bitget returns the trades as a bare array in `data`; `category` is set by the caller.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "Vec<BitgetTrade>")]
pub struct BitgetTradeResult {
    pub category: BitgetProductType,
    pub list: Vec<BitgetTrade>,
}

impl From<Vec<BitgetTrade>> for BitgetTradeResult {
    fn from(list: Vec<BitgetTrade>) -> Self {
        Self {
            category: BitgetProductType::default(),
            list,
        }
    }
}

/// Response alias for recent trades requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetTradesResponse = BitgetResponse<BitgetTradeResult>;

/// Funding entry returned by `GET /api/v3/market/history-fund-rate`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/derivatives>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetFunding {
    pub symbol: Ustr,
    pub funding_rate: String,
    pub funding_rate_timestamp: String,
}

/// Funding list result returned by Bitget.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/derivatives>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetFundingResult {
    /// Not echoed by Bitget; set by the caller.
    #[serde(default)]
    pub category: BitgetProductType,
    #[serde(
        rename = "resultList",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub list: Vec<BitgetFunding>,
}

/// Response alias for historical funding requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/derivatives>
pub type BitgetFundingResponse = BitgetResponse<BitgetFundingResult>;

/// Orderbook result returned by Bitget.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetOrderbookResult {
    /// Symbol (not echoed by Bitget; set by the caller).
    #[serde(default)]
    pub s: Ustr,
    /// Bid levels represented as `[price, size]` string pairs (Bitget sends JSON numbers).
    #[serde(deserialize_with = "deserialize_book_levels")]
    pub b: Vec<[String; 2]>,
    /// Ask levels represented as `[price, size]` string pairs (Bitget sends JSON numbers).
    #[serde(deserialize_with = "deserialize_book_levels")]
    pub a: Vec<[String; 2]>,
    #[serde(deserialize_with = "deserialize_i64_or_string")]
    pub ts: i64,
    /// Update identifier (Bitget's REST book has no sequence; always 0).
    #[serde(default)]
    pub u: i64,
    /// Cross sequence number (Bitget's REST book has no sequence; always 0).
    #[serde(default)]
    pub seq: i64,
    #[serde(default)]
    pub cts: i64,
}

/// Deserializes order book levels given as JSON numbers or strings into `[price, size]` strings.
fn deserialize_book_levels<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<[String; 2]>, D::Error> {
    let levels = Vec::<[serde_json::Value; 2]>::deserialize(d)?;
    Ok(levels
        .into_iter()
        .map(|[price, size]| [json_scalar_to_string(price), json_scalar_to_string(size)])
        .collect())
}

fn json_scalar_to_string(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

/// Response alias for orderbook requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetOrderbookResponse = BitgetResponse<BitgetOrderbookResult>;

/// Instrument definition for spot symbols.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetInstrumentSpot {
    pub symbol: Ustr,
    pub base_coin: Ustr,
    pub quote_coin: Ustr,
    /// No Bitget field; always `Other`.
    #[serde(default = "default_innovation")]
    pub innovation: BitgetInnovationFlag,
    pub status: BitgetInstrumentStatus,
    /// No Bitget field; always `Other`.
    #[serde(default = "default_margin_trading")]
    pub margin_trading: BitgetMarginTrading,
    /// Price decimal places; the tick size is `10^-pricePrecision`.
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub price_precision: String,
    /// Quantity decimal places; the lot step is `10^-quantityPrecision`.
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub quantity_precision: String,
    /// Quote-amount decimal places of market orders.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub quote_precision: String,
    /// Minimum order quantity (`0` = no limit).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub min_order_qty: String,
    /// Maximum order quantity (`0` = no limit).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub max_order_qty: String,
    /// Minimum order notional in quote currency.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub min_order_amount: String,
    /// Maximum market order notional in quote currency.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub max_market_order_amount: String,
    /// `yes` for Bitget "Reality" stock tokens (rTokens).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub is_reality: String,
    #[serde(default)]
    pub symbol_id: Option<i64>,
    #[serde(default, rename = "instrumentType")]
    pub symbol_type: Option<BitgetSymbolType>,
    /// No Bitget field.
    #[serde(default)]
    pub xstock_multiplier: Option<String>,
}

impl BitgetInstrumentSpot {
    /// Returns the lot size filter built from the flat Bitget fields.
    #[must_use]
    pub fn lot_size_filter(&self) -> SpotLotSizeFilter {
        SpotLotSizeFilter {
            base_precision: precision_to_step(&self.quantity_precision),
            quote_precision: precision_to_step(&self.quote_precision),
            min_order_qty: self.min_order_qty.clone(),
            max_order_qty: self.max_order_qty.clone(),
            min_order_amt: self.min_order_amount.clone(),
            max_order_amt: self.max_market_order_amount.clone(),
        }
    }

    /// Returns the price filter built from the flat Bitget fields.
    #[must_use]
    pub fn price_filter(&self) -> SpotPriceFilter {
        SpotPriceFilter {
            tick_size: precision_to_step(&self.price_precision),
        }
    }
}

const fn default_innovation() -> BitgetInnovationFlag {
    BitgetInnovationFlag::Other
}

const fn default_margin_trading() -> BitgetMarginTrading {
    BitgetMarginTrading::Other
}

/// Converts a Bitget decimal-places value (e.g. `"4"`) into a step string (e.g. `"0.0001"`).
fn precision_to_step(precision: &str) -> String {
    precision.parse::<u32>().map_or_else(
        |_| String::new(),
        |places| Decimal::new(1, places).to_string(),
    )
}

/// Instrument definition for linear contracts.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetInstrumentLinear {
    pub symbol: Ustr,
    /// Bitget `type` (`perpetual`/`delivery`); see `contract_type()`.
    #[serde(rename = "type")]
    pub contract_kind: String,
    pub status: BitgetInstrumentStatus,
    pub base_coin: Ustr,
    pub quote_coin: Ustr,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub launch_time: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub delivery_time: String,
    /// No Bitget field.
    #[serde(default)]
    pub delivery_fee_rate: String,
    /// Price decimal places.
    #[serde(
        rename = "pricePrecision",
        deserialize_with = "deserialize_string_or_null"
    )]
    pub price_scale: String,
    /// Quantity decimal places.
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub quantity_precision: String,
    /// Price step (an order price must be a multiple of it).
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub price_multiplier: String,
    /// Quantity step (an order quantity must be a multiple of it).
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub quantity_multiplier: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub min_order_qty: String,
    /// Maximum quantity of a single limit order (`0` = no limit).
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub max_order_qty: String,
    /// Maximum quantity of a single market order.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub max_market_order_qty: String,
    /// Minimum order notional in quote currency.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub min_order_amount: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub min_leverage: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub max_leverage: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub maker_fee_rate: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub taker_fee_rate: String,
    /// Maximum buy price as a ratio away from the reference price.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub buy_limit_price_ratio: String,
    /// Minimum sell price as a ratio away from the reference price.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub sell_limit_price_ratio: String,
    /// Scheduled halt time in milliseconds (`-1` when none).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub off_time: String,
    /// No Bitget field; Bitget UTA trades every listed contract.
    #[serde(default = "default_true")]
    pub unified_margin_trade: bool,
    /// Funding interval in minutes (Bitget `fundInterval` is in hours; `0` for deliveries).
    #[serde(
        rename = "fundInterval",
        deserialize_with = "deserialize_hours_to_minutes"
    )]
    pub funding_interval: i64,
    /// Bitget sends no settle coin: quote coin for USDT-FUTURES, base coin for COIN-FUTURES.
    #[serde(default)]
    pub settle_coin: Ustr,
    #[serde(default)]
    pub symbol_id: Option<i64>,
    #[serde(default, rename = "instrumentType")]
    pub symbol_type: Option<BitgetSymbolType>,
}

impl BitgetInstrumentLinear {
    /// Returns the contract type derived from the category and Bitget `type`.
    #[must_use]
    pub fn contract_type(&self) -> BitgetContractType {
        match (self.contract_kind.as_str(), false) {
            ("delivery", false) => BitgetContractType::LinearFutures,
            ("delivery", true) => BitgetContractType::InverseFutures,
            (_, false) => BitgetContractType::LinearPerpetual,
            (_, true) => BitgetContractType::InversePerpetual,
        }
    }

    /// Returns the price filter built from the flat Bitget fields.
    #[must_use]
    pub fn price_filter(&self) -> LinearPriceFilter {
        LinearPriceFilter {
            min_price: String::new(),
            max_price: String::new(),
            tick_size: self.price_multiplier.clone(),
        }
    }

    /// Returns the lot size filter built from the flat Bitget fields.
    #[must_use]
    pub fn lot_size_filter(&self) -> LinearLotSizeFilter {
        LinearLotSizeFilter {
            max_order_qty: self.max_order_qty.clone(),
            min_order_qty: self.min_order_qty.clone(),
            qty_step: self.quantity_multiplier.clone(),
            post_only_max_order_qty: None,
            max_mkt_order_qty: Some(self.max_market_order_qty.clone()),
            min_notional_value: Some(self.min_order_amount.clone()),
        }
    }

    /// Returns the leverage filter built from the flat Bitget fields.
    #[must_use]
    pub fn leverage_filter(&self) -> LeverageFilter {
        LeverageFilter {
            min_leverage: self.min_leverage.clone(),
            max_leverage: self.max_leverage.clone(),
            leverage_step: "1".to_string(),
        }
    }

    /// Returns the settlement coin (`settleCoin` is not sent by Bitget).
    #[must_use]
    pub fn settle_coin(&self) -> Ustr {
        if self.settle_coin.is_empty() {
            self.quote_coin
        } else {
            self.settle_coin
        }
    }
}

/// Instrument definition for inverse contracts.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetInstrumentInverse {
    pub symbol: Ustr,
    /// Bitget `type` (`perpetual`/`delivery`); see `contract_type()`.
    #[serde(rename = "type")]
    pub contract_kind: String,
    pub status: BitgetInstrumentStatus,
    pub base_coin: Ustr,
    pub quote_coin: Ustr,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub launch_time: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub delivery_time: String,
    /// No Bitget field.
    #[serde(default)]
    pub delivery_fee_rate: String,
    /// Price decimal places.
    #[serde(
        rename = "pricePrecision",
        deserialize_with = "deserialize_string_or_null"
    )]
    pub price_scale: String,
    /// Quantity decimal places.
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub quantity_precision: String,
    /// Price step (an order price must be a multiple of it).
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub price_multiplier: String,
    /// Quantity step (an order quantity must be a multiple of it).
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub quantity_multiplier: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub min_order_qty: String,
    /// Maximum quantity of a single limit order (`0` = no limit).
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub max_order_qty: String,
    /// Maximum quantity of a single market order.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub max_market_order_qty: String,
    /// Minimum order notional in quote currency.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub min_order_amount: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub min_leverage: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub max_leverage: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub maker_fee_rate: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub taker_fee_rate: String,
    /// Maximum buy price as a ratio away from the reference price.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub buy_limit_price_ratio: String,
    /// Minimum sell price as a ratio away from the reference price.
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub sell_limit_price_ratio: String,
    /// Scheduled halt time in milliseconds (`-1` when none).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub off_time: String,
    /// No Bitget field; Bitget UTA trades every listed contract.
    #[serde(default = "default_true")]
    pub unified_margin_trade: bool,
    /// Funding interval in minutes (Bitget `fundInterval` is in hours; `0` for deliveries).
    #[serde(
        rename = "fundInterval",
        deserialize_with = "deserialize_hours_to_minutes"
    )]
    pub funding_interval: i64,
    /// Bitget sends no settle coin: quote coin for USDT-FUTURES, base coin for COIN-FUTURES.
    #[serde(default)]
    pub settle_coin: Ustr,
    #[serde(default)]
    pub symbol_id: Option<i64>,
    #[serde(default, rename = "instrumentType")]
    pub symbol_type: Option<BitgetSymbolType>,
}

impl BitgetInstrumentInverse {
    /// Returns the contract type derived from the category and Bitget `type`.
    #[must_use]
    pub fn contract_type(&self) -> BitgetContractType {
        match (self.contract_kind.as_str(), true) {
            ("delivery", false) => BitgetContractType::LinearFutures,
            ("delivery", true) => BitgetContractType::InverseFutures,
            (_, false) => BitgetContractType::LinearPerpetual,
            (_, true) => BitgetContractType::InversePerpetual,
        }
    }

    /// Returns the price filter built from the flat Bitget fields.
    #[must_use]
    pub fn price_filter(&self) -> LinearPriceFilter {
        LinearPriceFilter {
            min_price: String::new(),
            max_price: String::new(),
            tick_size: self.price_multiplier.clone(),
        }
    }

    /// Returns the lot size filter built from the flat Bitget fields.
    #[must_use]
    pub fn lot_size_filter(&self) -> LinearLotSizeFilter {
        LinearLotSizeFilter {
            max_order_qty: self.max_order_qty.clone(),
            min_order_qty: self.min_order_qty.clone(),
            qty_step: self.quantity_multiplier.clone(),
            post_only_max_order_qty: None,
            max_mkt_order_qty: Some(self.max_market_order_qty.clone()),
            min_notional_value: Some(self.min_order_amount.clone()),
        }
    }

    /// Returns the leverage filter built from the flat Bitget fields.
    #[must_use]
    pub fn leverage_filter(&self) -> LeverageFilter {
        LeverageFilter {
            min_leverage: self.min_leverage.clone(),
            max_leverage: self.max_leverage.clone(),
            leverage_step: "1".to_string(),
        }
    }

    /// Returns the settlement coin (`settleCoin` is not sent by Bitget).
    #[must_use]
    pub fn settle_coin(&self) -> Ustr {
        if self.settle_coin.is_empty() {
            self.base_coin
        } else {
            self.settle_coin
        }
    }
}

const fn default_true() -> bool {
    true
}

/// Deserializes Bitget `fundInterval` (hours, as a string; empty for deliveries) into minutes.
fn deserialize_hours_to_minutes<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    let raw = deserialize_string_or_null(d)?;
    if raw.is_empty() {
        return Ok(0);
    }
    raw.parse::<i64>()
        .map(|hours| hours * 60)
        .map_err(serde::de::Error::custom)
}

/// Instrument definition for option contracts (Bitget lists no options; kept for API parity).
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetInstrumentOption {
    pub symbol: Ustr,
    pub status: BitgetInstrumentStatus,
    pub base_coin: Ustr,
    pub quote_coin: Ustr,
    pub settle_coin: Ustr,
    pub options_type: BitgetOptionType,
    pub launch_time: String,
    pub delivery_time: String,
    pub delivery_fee_rate: String,
    pub price_filter: LinearPriceFilter,
    pub lot_size_filter: OptionLotSizeFilter,
    #[serde(default)]
    pub symbol_id: Option<i64>,
}

/// Response alias for instrument info requests that return spot instruments.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetInstrumentSpotResponse = BitgetCursorListResponse<BitgetInstrumentSpot>;
/// Response alias for instrument info requests that return linear contracts.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetInstrumentLinearResponse = BitgetCursorListResponse<BitgetInstrumentLinear>;
/// Response alias for instrument info requests that return inverse contracts.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetInstrumentInverseResponse = BitgetCursorListResponse<BitgetInstrumentInverse>;
/// Response alias for instrument info requests that return option contracts.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
pub type BitgetInstrumentOptionResponse = BitgetCursorListResponse<BitgetInstrumentOption>;

/// Fee rate structure returned by `GET /api/v3/account/all-fee-rate`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetFeeRate {
    pub symbol: Ustr,
    pub taker_fee_rate: String,
    pub maker_fee_rate: String,
    #[serde(default)]
    pub base_coin: Option<Ustr>,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl BitgetFeeRate {
    #[getter]
    #[must_use]
    pub fn symbol(&self) -> &str {
        self.symbol.as_str()
    }

    #[getter]
    #[must_use]
    pub fn taker_fee_rate(&self) -> &str {
        &self.taker_fee_rate
    }

    #[getter]
    #[must_use]
    pub fn maker_fee_rate(&self) -> &str {
        &self.maker_fee_rate
    }

    #[getter]
    #[must_use]
    pub fn base_coin(&self) -> Option<&str> {
        self.base_coin.as_ref().map(|u| u.as_str())
    }
}

/// Response alias for fee rate requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
pub type BitgetFeeRateResponse = BitgetListResponse<BitgetFeeRate>;

/// Account balance snapshot coin entry.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetCoinBalance {
    /// No Bitget field.
    #[serde(default)]
    pub available_to_borrow: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub bonus: String,
    /// Interest base (`interestBase`).
    #[serde(
        rename = "interestBase",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub accrued_interest: String,
    /// Available balance (`available`).
    #[serde(rename = "available")]
    pub available_to_withdraw: String,
    /// No Bitget field.
    #[serde(default, rename = "totalOrderIM")]
    pub total_order_im: Option<String>,
    pub equity: String,
    pub usd_value: String,
    /// Liabilities (`debt`).
    #[serde(
        rename = "debt",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub borrow_amount: String,
    /// No Bitget field.
    #[serde(default, rename = "totalPositionMM")]
    pub total_position_mm: Option<String>,
    /// No Bitget field.
    #[serde(default, rename = "totalPositionIM")]
    pub total_position_im: Option<String>,
    /// Wallet balance (`balance`).
    #[serde(rename = "balance", deserialize_with = "deserialize_decimal_or_zero")]
    pub wallet_balance: Decimal,
    /// No Bitget field per coin.
    #[serde(default)]
    pub unrealised_pnl: String,
    /// No Bitget field.
    #[serde(default)]
    pub cum_realised_pnl: String,
    #[serde(deserialize_with = "deserialize_decimal_or_zero")]
    pub locked: Decimal,
    /// No Bitget field; every unified asset is collateral.
    #[serde(default = "default_true")]
    pub collateral_switch: bool,
    /// No Bitget field; every unified asset is collateral.
    #[serde(default = "default_true")]
    pub margin_collateral: bool,
    pub coin: Ustr,
    /// No Bitget field.
    #[serde(default)]
    pub spot_hedging_qty: Option<String>,
    /// No Bitget field (borrowing is reported as `debt`).
    #[serde(default, deserialize_with = "deserialize_optional_decimal_or_zero")]
    pub spot_borrow: Decimal,
}

/// Wallet balance snapshot containing per-coin balances.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWalletBalance {
    /// Account equity in USD (`accountEquity`).
    #[serde(rename = "accountEquity")]
    pub total_equity: String,
    /// No Bitget field.
    #[serde(default, rename = "accountIMRate")]
    pub account_im_rate: String,
    /// Effective equity (`effEquity`).
    #[serde(
        rename = "effEquity",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub total_margin_balance: String,
    /// Initial margin in USD (`imr`).
    #[serde(
        rename = "imr",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub total_initial_margin: String,
    /// Bitget UTA has a single unified wallet.
    #[serde(default = "default_account_type")]
    pub account_type: BitgetAccountType,
    /// No Bitget field.
    #[serde(default)]
    pub total_available_balance: String,
    /// Margin ratio (`mgnRatio`).
    #[serde(
        rename = "mgnRatio",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub account_mm_rate: String,
    /// Unrealized PnL in USD (`unrealisedPnl`).
    #[serde(
        rename = "unrealisedPnl",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub total_perp_upl: String,
    /// USDT-denominated equity (`usdtEquity`).
    #[serde(
        rename = "usdtEquity",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub total_wallet_balance: String,
    /// No Bitget field.
    #[serde(default, rename = "accountLTV")]
    pub account_ltv: String,
    /// Maintenance margin in USD (`mmr`).
    #[serde(
        rename = "mmr",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub total_maintenance_margin: String,
    /// Per-coin balances (`assets`, non-zero balances only).
    #[serde(
        rename = "assets",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub coin: Vec<BitgetCoinBalance>,
}

const fn default_account_type() -> BitgetAccountType {
    BitgetAccountType::Unified
}

/// Response alias for wallet balance requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
///
/// Bitget returns one unified account object in `data` (no list).
pub type BitgetWalletBalanceResponse = BitgetResponse<BitgetWalletBalance>;

/// Account-level configuration returned by `GET /api/v3/account/settings`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetAccountInfo {
    /// Account mode (`accountMode`: `unified`/`hybrid`/...).
    #[serde(rename = "accountMode")]
    pub unified_margin_status: BitgetUnifiedMarginStatus,
    /// Account level (`accountLevel`: `basic`/`advanced`/`isolated`).
    #[serde(rename = "accountLevel")]
    pub margin_mode: BitgetMarginMode,
    /// No Bitget field.
    #[serde(default)]
    pub is_master_trader: bool,
    /// Delta switch (`deltaSwitch`: `yes`/`no`).
    #[serde(rename = "deltaSwitch", default, with = "yes_no_bool")]
    pub spot_hedging_status: bool,
    /// No Bitget field.
    #[serde(default)]
    pub updated_time: String,
    /// No Bitget field (no disconnection-protection setting).
    #[serde(default)]
    pub dcp_status: bool,
    /// No Bitget field.
    #[serde(default, deserialize_with = "deserialize_i32_or_string")]
    pub time_window: i32,
    /// No Bitget field.
    #[serde(default, deserialize_with = "deserialize_i32_or_string")]
    pub smp_group: i32,
    /// Account UID.
    #[serde(default)]
    pub uid: String,
    /// Position (hold) mode.
    #[serde(default)]
    pub hold_mode: Option<BitgetPositionMode>,
    /// Account default self-trade prevention mode.
    #[serde(default)]
    pub stp_mode: Option<BitgetSmpType>,
    /// Asset mode (e.g. `multi_assets`).
    #[serde(default)]
    pub asset_mode: String,
    /// Repay mode (`auto`/`manual`).
    #[serde(default)]
    pub repay_mode: String,
    /// Per-symbol margin mode and leverage.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub symbol_config_list: Vec<BitgetSymbolConfig>,
}

/// Per-symbol margin mode and leverage from `GET /api/v3/account/settings`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSymbolConfig {
    pub category: BitgetProductType,
    pub symbol: Ustr,
    pub margin_mode: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub leverage: String,
}

/// Response alias for account info requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
pub type BitgetAccountInfoResponse = BitgetResponse<BitgetAccountInfo>;

/// Order representation as returned by order-related endpoints.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
#[serde(rename_all = "camelCase")]
pub struct BitgetOrder {
    pub order_id: Ustr,
    #[serde(rename = "clientOid", default)]
    pub order_link_id: Ustr,
    /// No Bitget field.
    #[serde(default)]
    pub block_trade_id: Option<Ustr>,
    pub symbol: Ustr,
    /// Limit price (`0` or empty for market orders); strategy orders report `triggerOrderPrice`.
    #[serde(
        alias = "triggerOrderPrice",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub price: String,
    pub qty: String,
    pub side: BitgetOrderSide,
    /// No Bitget field.
    #[serde(default)]
    pub is_leverage: String,
    /// Bitget sends `holdMode` + `posSide` instead; see `parse::resolve_order_position_idx`.
    #[serde(default, deserialize_with = "deserialize_i32_or_string")]
    pub position_idx: i32,
    /// Order status (`orderStatus`; strategy orders report `status`).
    #[serde(rename = "orderStatus", alias = "status")]
    pub order_status: BitgetOrderStatus,
    /// Cancel reason (`cancelReason`, empty when not cancelled).
    #[serde(
        rename = "cancelReason",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub cancel_type: BitgetCancelType,
    /// No Bitget field: rejected orders never reach the order lists.
    #[serde(default)]
    pub reject_reason: Ustr,
    #[serde(default)]
    pub avg_price: Option<String>,
    /// No Bitget field (`qty - cumExecQty`).
    #[serde(default)]
    pub leaves_qty: String,
    /// No Bitget field.
    #[serde(default)]
    pub leaves_value: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub cum_exec_qty: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub cum_exec_value: String,
    /// Signed sum of `feeDetail[].fee` (positive = cost).
    #[serde(
        rename = "feeDetail",
        default,
        deserialize_with = "deserialize_fee_detail_sum"
    )]
    pub cum_exec_fee: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub time_in_force: BitgetTimeInForce,
    /// Order type (`orderType`; strategy orders report `triggerOrderType`).
    #[serde(
        alias = "triggerOrderType",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub order_type: BitgetOrderType,
    /// Strategy order type (`type`: `trigger`, `tpsl`, ...); `None` for plain orders.
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub stop_order_type: BitgetStopOrderType,
    /// No Bitget field.
    #[serde(default)]
    pub order_iv: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub trigger_price: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub take_profit: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub stop_loss: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub tp_trigger_by: BitgetTriggerType,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub sl_trigger_by: BitgetTriggerType,
    /// No Bitget field (the venue infers the direction from the trigger price).
    #[serde(default)]
    pub trigger_direction: BitgetTriggerDirection,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub trigger_by: BitgetTriggerType,
    /// No Bitget field.
    #[serde(default)]
    pub last_price_on_created: String,
    /// `YES`/`NO` (REST), `yes`/`no` (WebSocket), `false` (strategy orders).
    #[serde(default, with = "yes_no_bool")]
    pub reduce_only: bool,
    /// No Bitget field.
    #[serde(default)]
    pub close_on_trigger: bool,
    /// Self-trade prevention mode (`stpMode`).
    #[serde(
        rename = "stpMode",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub smp_type: BitgetSmpType,
    /// No Bitget field.
    #[serde(default, deserialize_with = "deserialize_i32_or_string")]
    pub smp_group: i32,
    /// No Bitget field.
    #[serde(default)]
    pub smp_order_id: Ustr,
    #[serde(default)]
    pub tpsl_mode: Option<BitgetTpSlMode>,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub tp_limit_price: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub sl_limit_price: String,
    /// Delegate type (`delegateType`: `normal`, `market`, `liquidation`, ...).
    #[serde(rename = "delegateType", default)]
    pub place_type: Ustr,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub created_time: String,
    #[serde(deserialize_with = "deserialize_string_or_null")]
    pub updated_time: String,
    /// Product category (`USDT-FUTURES`, ...).
    #[serde(default)]
    pub category: Option<BitgetProductType>,
    /// Position side the order acts on (`posSide`; WebSocket `holdSide`).
    #[serde(
        alias = "holdSide",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub pos_side: BitgetPositionSide,
    /// Hold mode (`one_way_mode`/`hedge_mode`, empty for spot).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub hold_mode: String,
    /// Trade side (`open`/`close`, or the one-way `*_single` values).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub trade_side: String,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl BitgetOrder {
    #[getter]
    #[must_use]
    pub fn order_id(&self) -> &str {
        self.order_id.as_str()
    }

    #[getter]
    #[must_use]
    pub fn order_link_id(&self) -> &str {
        self.order_link_id.as_str()
    }

    #[getter]
    #[must_use]
    pub fn block_trade_id(&self) -> Option<&str> {
        self.block_trade_id.as_ref().map(|s| s.as_str())
    }

    #[getter]
    #[must_use]
    pub fn symbol(&self) -> &str {
        self.symbol.as_str()
    }

    #[getter]
    #[must_use]
    pub fn price(&self) -> &str {
        &self.price
    }

    #[getter]
    #[must_use]
    pub fn qty(&self) -> &str {
        &self.qty
    }

    #[getter]
    #[must_use]
    pub fn side(&self) -> BitgetOrderSide {
        self.side
    }

    #[getter]
    #[must_use]
    pub fn is_leverage(&self) -> &str {
        &self.is_leverage
    }

    #[getter]
    #[must_use]
    pub fn position_idx(&self) -> i32 {
        self.position_idx
    }

    #[getter]
    #[must_use]
    pub fn order_status(&self) -> BitgetOrderStatus {
        self.order_status
    }

    #[getter]
    #[must_use]
    pub fn cancel_type(&self) -> BitgetCancelType {
        self.cancel_type
    }

    #[getter]
    #[must_use]
    pub fn reject_reason(&self) -> &str {
        self.reject_reason.as_str()
    }

    #[getter]
    #[must_use]
    pub fn avg_price(&self) -> Option<&str> {
        self.avg_price.as_deref()
    }

    #[getter]
    #[must_use]
    pub fn leaves_qty(&self) -> &str {
        &self.leaves_qty
    }

    #[getter]
    #[must_use]
    pub fn leaves_value(&self) -> &str {
        &self.leaves_value
    }

    #[getter]
    #[must_use]
    pub fn cum_exec_qty(&self) -> &str {
        &self.cum_exec_qty
    }

    #[getter]
    #[must_use]
    pub fn cum_exec_value(&self) -> &str {
        &self.cum_exec_value
    }

    #[getter]
    #[must_use]
    pub fn cum_exec_fee(&self) -> &str {
        &self.cum_exec_fee
    }

    #[getter]
    #[must_use]
    pub fn time_in_force(&self) -> BitgetTimeInForce {
        self.time_in_force
    }

    #[getter]
    #[must_use]
    pub fn order_type(&self) -> BitgetOrderType {
        self.order_type
    }

    #[getter]
    #[must_use]
    pub fn stop_order_type(&self) -> BitgetStopOrderType {
        self.stop_order_type
    }

    #[getter]
    #[must_use]
    pub fn order_iv(&self) -> Option<&str> {
        self.order_iv.as_deref()
    }

    #[getter]
    #[must_use]
    pub fn trigger_price(&self) -> &str {
        &self.trigger_price
    }

    #[getter]
    #[must_use]
    pub fn take_profit(&self) -> &str {
        &self.take_profit
    }

    #[getter]
    #[must_use]
    pub fn stop_loss(&self) -> &str {
        &self.stop_loss
    }

    #[getter]
    #[must_use]
    pub fn tp_trigger_by(&self) -> BitgetTriggerType {
        self.tp_trigger_by
    }

    #[getter]
    #[must_use]
    pub fn sl_trigger_by(&self) -> BitgetTriggerType {
        self.sl_trigger_by
    }

    #[getter]
    #[must_use]
    pub fn trigger_direction(&self) -> BitgetTriggerDirection {
        self.trigger_direction
    }

    #[getter]
    #[must_use]
    pub fn trigger_by(&self) -> BitgetTriggerType {
        self.trigger_by
    }

    #[getter]
    #[must_use]
    pub fn last_price_on_created(&self) -> &str {
        &self.last_price_on_created
    }

    #[getter]
    #[must_use]
    pub fn reduce_only(&self) -> bool {
        self.reduce_only
    }

    #[getter]
    #[must_use]
    pub fn close_on_trigger(&self) -> bool {
        self.close_on_trigger
    }

    #[getter]
    #[must_use]
    #[expect(
        clippy::missing_panics_doc,
        reason = "serialization of a simple enum cannot fail"
    )]
    pub fn smp_type(&self) -> String {
        serde_json::to_string(&self.smp_type)
            .expect("Failed to serialize BitgetSmpType")
            .trim_matches('"')
            .to_string()
    }

    #[getter]
    #[must_use]
    pub fn smp_group(&self) -> i32 {
        self.smp_group
    }

    #[getter]
    #[must_use]
    pub fn smp_order_id(&self) -> &str {
        self.smp_order_id.as_str()
    }

    #[getter]
    #[must_use]
    pub fn tpsl_mode(&self) -> Option<BitgetTpSlMode> {
        self.tpsl_mode
    }

    #[getter]
    #[must_use]
    pub fn tp_limit_price(&self) -> &str {
        &self.tp_limit_price
    }

    #[getter]
    #[must_use]
    pub fn sl_limit_price(&self) -> &str {
        &self.sl_limit_price
    }

    #[getter]
    #[must_use]
    pub fn place_type(&self) -> &str {
        self.place_type.as_str()
    }

    #[getter]
    #[must_use]
    pub fn created_time(&self) -> &str {
        &self.created_time
    }

    #[getter]
    #[must_use]
    pub fn updated_time(&self) -> &str {
        &self.updated_time
    }
}

/// Response alias for open order queries.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
pub type BitgetOpenOrdersResponse = BitgetCursorListResponse<BitgetOrder>;
/// Response alias for order history queries with pagination.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
pub type BitgetOrderHistoryResponse = BitgetCursorListResponse<BitgetOrder>;

/// Payload returned after placing a single order.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetPlaceOrderResult {
    /// `null` when a one-way reduce-only order replaced an existing one.
    pub order_id: Option<Ustr>,
    #[serde(rename = "clientOid", default)]
    pub order_link_id: Option<Ustr>,
}

/// Response alias for order placement endpoints.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
pub type BitgetPlaceOrderResponse = BitgetResponse<BitgetPlaceOrderResult>;

/// Payload returned after cancelling a single order.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetCancelOrderResult {
    pub order_id: Option<Ustr>,
    #[serde(rename = "clientOid", default)]
    pub order_link_id: Option<Ustr>,
}

/// Response alias for order cancellation endpoints.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
pub type BitgetCancelOrderResponse = BitgetResponse<BitgetCancelOrderResult>;

/// Execution/Fill payload returned by `GET /api/v3/trade/fills`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetExecution {
    pub symbol: Ustr,
    pub order_id: Ustr,
    #[serde(rename = "clientOid", default)]
    pub order_link_id: Ustr,
    pub side: BitgetOrderSide,
    /// No Bitget field.
    #[serde(default)]
    pub order_price: String,
    /// No Bitget field.
    #[serde(default)]
    pub order_qty: String,
    /// No Bitget field.
    #[serde(default)]
    pub leaves_qty: String,
    /// No Bitget field.
    #[serde(default)]
    pub create_type: Option<BitgetCreateType>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub order_type: BitgetOrderType,
    /// No Bitget field.
    #[serde(default)]
    pub stop_order_type: Option<BitgetStopOrderType>,
    /// Signed sum of `feeDetail[].fee` (positive = cost).
    #[serde(
        rename = "feeDetail",
        default,
        deserialize_with = "deserialize_fee_detail_sum"
    )]
    pub exec_fee: String,
    pub exec_id: String,
    pub exec_price: String,
    pub exec_qty: String,
    /// Derived from `tradeSide` (liquidation, ADL, delivery, rebase or a plain trade).
    #[serde(
        rename = "tradeSide",
        default,
        deserialize_with = "deserialize_exec_type"
    )]
    pub exec_type: BitgetExecType,
    pub exec_value: String,
    /// Fill time (`createdTime`; the WebSocket `fill` topic sends `execTime`).
    #[serde(rename = "createdTime", alias = "execTime")]
    pub exec_time: String,
    /// Currency of the first `feeDetail` entry.
    #[serde(skip_deserializing)]
    pub fee_currency: Ustr,
    /// `tradeScope == "maker"`.
    #[serde(rename = "tradeScope", deserialize_with = "deserialize_is_maker")]
    pub is_maker: bool,
    /// No Bitget field.
    #[serde(default)]
    pub fee_rate: String,
    /// No Bitget field.
    #[serde(default)]
    pub trade_iv: String,
    /// No Bitget field.
    #[serde(default)]
    pub mark_iv: String,
    /// No Bitget field.
    #[serde(default)]
    pub mark_price: String,
    /// No Bitget field.
    #[serde(default)]
    pub index_price: String,
    /// No Bitget field.
    #[serde(default)]
    pub underlying_price: String,
    /// No Bitget field.
    #[serde(default)]
    pub block_trade_id: String,
    /// No Bitget field.
    #[serde(default)]
    pub closed_size: String,
    /// No Bitget field.
    #[serde(default)]
    pub seq: i64,
    /// Realized PnL of the fill (`execPnl`).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub exec_pnl: String,
    /// Product category (`USDT-FUTURES`, ...).
    #[serde(default)]
    pub category: Option<BitgetProductType>,
}

fn deserialize_exec_type<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BitgetExecType, D::Error> {
    let trade_side = deserialize_string_or_null(d)?;
    Ok(BitgetExecType::from_trade_side(&trade_side))
}

fn deserialize_is_maker<'de, D: serde::Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    Ok(deserialize_string_or_null(d)?.eq_ignore_ascii_case("maker"))
}

/// Response alias for trade history requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
pub type BitgetTradeHistoryResponse = BitgetCursorListResponse<BitgetExecution>;

/// Represents a position returned by the Bitget API.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/position-management>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetPosition {
    /// Bitget sends `holdMode` + `posSide` instead; see `parse::resolve_position_idx`.
    #[serde(default)]
    pub position_idx: BitgetPositionIdx,
    /// No Bitget field.
    #[serde(default, deserialize_with = "deserialize_i32_or_string")]
    pub risk_id: i32,
    /// No Bitget field.
    #[serde(default)]
    pub risk_limit_value: String,
    pub symbol: Ustr,
    /// Position side (`posSide`: `long`/`short`).
    #[serde(
        rename = "posSide",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub side: BitgetPositionSide,
    /// Position size (`total` = available + frozen, always positive).
    #[serde(rename = "total")]
    pub size: String,
    pub avg_price: String,
    /// No Bitget field.
    #[serde(default)]
    pub position_value: String,
    /// `0` cross, `1` isolated (from `marginMode`).
    #[serde(
        rename = "marginMode",
        default,
        deserialize_with = "deserialize_trade_mode"
    )]
    pub trade_mode: i32,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub position_status: BitgetPositionStatus,
    /// No Bitget field.
    #[serde(default, deserialize_with = "deserialize_i32_or_string")]
    pub auto_add_margin: i32,
    /// No Bitget field.
    #[serde(default, deserialize_with = "deserialize_i32_or_string")]
    pub adl_rank_indicator: i32,
    pub leverage: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub position_balance: String,
    pub mark_price: String,
    /// Liquidation price (`liquidationPrice`, `<= 0` when none).
    #[serde(
        rename = "liquidationPrice",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub liq_price: String,
    /// No Bitget field.
    #[serde(default)]
    pub bust_price: String,
    /// Maintenance margin rate (`mmr`).
    #[serde(
        rename = "mmr",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub position_mm: String,
    /// No Bitget field.
    #[serde(default, rename = "positionIM")]
    pub position_im: String,
    /// No Bitget field.
    #[serde(default)]
    pub tpsl_mode: BitgetTpSlMode,
    /// No Bitget field.
    #[serde(default)]
    pub take_profit: String,
    /// No Bitget field.
    #[serde(default)]
    pub stop_loss: String,
    /// No Bitget field.
    #[serde(default)]
    pub trailing_stop: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub unrealised_pnl: String,
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub cur_realised_pnl: String,
    /// No Bitget field.
    #[serde(default)]
    pub cum_realised_pnl: String,
    /// No Bitget field.
    #[serde(default = "default_position_seq")]
    pub seq: i64,
    /// No Bitget field.
    #[serde(default)]
    pub is_reduce_only: bool,
    /// No Bitget field.
    #[serde(default)]
    pub mmr_sys_updated_time: String,
    /// No Bitget field.
    #[serde(default)]
    pub leverage_sys_updated_time: String,
    pub created_time: String,
    pub updated_time: String,
    /// No Bitget field (`createdTime` is the open time).
    #[serde(default)]
    pub open_time: i64,
    /// Hold mode (`one_way_mode`/`hedge_mode`).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub hold_mode: String,
    /// Settlement coin of the position margin.
    #[serde(default)]
    pub margin_coin: Ustr,
    /// Product category (`USDT-FUTURES`, ...).
    #[serde(default)]
    pub category: Option<BitgetProductType>,
}

fn deserialize_trade_mode<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    Ok(i32::from(deserialize_string_or_null(d)? == "isolated"))
}

const fn default_position_seq() -> i64 {
    -1
}

/// Response alias for position list requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/position-management>
pub type BitgetPositionListResponse = BitgetCursorListResponse<BitgetPosition>;

/// Reason detail for set margin mode failures.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSetMarginModeReason {
    pub reason_code: String,
    pub reason_msg: String,
}

/// Result payload for set margin mode operation.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
///
/// Bitget answers with `"data": null`; no reasons are reported.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "serde_json::Value")]
pub struct BitgetSetMarginModeResult {
    #[serde(default)]
    pub reasons: Vec<BitgetSetMarginModeReason>,
}

impl From<serde_json::Value> for BitgetSetMarginModeResult {
    fn from(value: serde_json::Value) -> Self {
        let reasons = value
            .get("reasons")
            .and_then(|reasons| serde_json::from_value(reasons.clone()).ok())
            .unwrap_or_default();
        Self { reasons }
    }
}

/// Response alias for set margin mode requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
pub type BitgetSetMarginModeResponse = BitgetResponse<BitgetSetMarginModeResult>;

/// Empty result for set leverage operation.
///
/// Bitget answers with `"data": "success"` (or `null`); the payload is ignored.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(from = "serde_json::Value")]
pub struct BitgetSetLeverageResult {}

impl From<serde_json::Value> for BitgetSetLeverageResult {
    fn from(_: serde_json::Value) -> Self {
        Self {}
    }
}

/// Response alias for set leverage requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
pub type BitgetSetLeverageResponse = BitgetResponse<BitgetSetLeverageResult>;

/// Empty result for switch mode operation.
///
/// Bitget answers with `"data": "success"` (or `null`); the payload is ignored.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(from = "serde_json::Value")]
pub struct BitgetSwitchModeResult {}

impl From<serde_json::Value> for BitgetSwitchModeResult {
    fn from(_: serde_json::Value) -> Self {
        Self {}
    }
}

/// Response alias for switch mode requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
pub type BitgetSwitchModeResponse = BitgetResponse<BitgetSwitchModeResult>;

/// Empty result for set trading stop operation.
///
/// Bitget answers with `"data": "success"` (or `null`); the payload is ignored.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(from = "serde_json::Value")]
pub struct BitgetSetTradingStopResult {}

impl From<serde_json::Value> for BitgetSetTradingStopResult {
    fn from(_: serde_json::Value) -> Self {
        Self {}
    }
}

/// Response alias for set trading stop requests.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/position-management>
pub type BitgetSetTradingStopResponse = BitgetResponse<BitgetSetTradingStopResult>;

/// Result from manual borrow operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBorrowResult {
    pub coin: Ustr,
    pub amount: String,
}

/// Response alias for manual borrow requests.
///
/// # References
///
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
pub type BitgetBorrowResponse = BitgetResponse<BitgetBorrowResult>;

/// Result from no-convert repay operation.
///
/// Bitget answers with `"data": "success"` (or an object); a non-error response is a success.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "serde_json::Value")]
pub struct BitgetNoConvertRepayResult {
    pub result_status: BitgetRepayStatus,
}

impl From<serde_json::Value> for BitgetNoConvertRepayResult {
    fn from(value: serde_json::Value) -> Self {
        let result_status = value
            .get("resultStatus")
            .and_then(|status| serde_json::from_value(status.clone()).ok())
            .unwrap_or(BitgetRepayStatus::Success);
        Self { result_status }
    }
}

/// Response alias for no-convert repay requests.
///
/// # References
///
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
pub type BitgetNoConvertRepayResponse = BitgetResponse<BitgetNoConvertRepayResult>;

/// Result from a manual repay (with conversion) operation.
///
/// Bitget answers with `"data": "success"` (or an object); a non-error response is a success.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "serde_json::Value")]
pub struct BitgetRepayResult {
    pub result_status: BitgetRepayStatus,
}

impl From<serde_json::Value> for BitgetRepayResult {
    fn from(value: serde_json::Value) -> Self {
        let result_status = value
            .get("resultStatus")
            .and_then(|status| serde_json::from_value(status.clone()).ok())
            .unwrap_or(BitgetRepayStatus::Success);
        Self { result_status }
    }
}

/// Response alias for manual repay requests.
///
/// # References
///
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
pub type BitgetRepayResponse = BitgetResponse<BitgetRepayResult>;

/// API key permissions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
#[serde(rename_all = "PascalCase")]
pub struct BitgetApiKeyPermissions {
    #[serde(default)]
    pub contract_trade: Vec<String>,
    #[serde(default)]
    pub spot: Vec<String>,
    #[serde(default)]
    pub wallet: Vec<String>,
    #[serde(default)]
    pub options: Vec<String>,
    #[serde(default)]
    pub derivatives: Vec<String>,
    #[serde(default)]
    pub exchange: Vec<String>,
    #[serde(default)]
    pub copy_trading: Vec<String>,
    #[serde(default)]
    pub block_trade: Vec<String>,
    // Bitget ships this key uppercase (`"NFT"`); the struct-level PascalCase
    // rule would otherwise serialize it as `"Nft"` and silently drop values.
    #[serde(rename = "NFT", default)]
    pub nft: Vec<String>,
    #[serde(default)]
    pub affiliate: Vec<String>,
    // Newer permission buckets. Master-account responses populate them, sub-key
    // responses typically omit or return empty arrays - both cases deserialize
    // to an empty `Vec` via `serde(default)`.
    #[serde(default)]
    pub earn: Vec<String>,
    // Bitget uses `"FiatP2P"` - PascalCase rename would emit `"FiatP2p"`.
    #[serde(rename = "FiatP2P", default)]
    pub fiat_p2p: Vec<String>,
    #[serde(default)]
    pub fiat_bitget_pay: Vec<String>,
    #[serde(default)]
    pub fiat_bit_pay: Vec<String>,
    #[serde(default)]
    pub fiat_global_pay: Vec<String>,
    #[serde(default)]
    pub fiat_convert_broker: Vec<String>,
    #[serde(default)]
    pub bit_card: Vec<String>,
    // Bitget uses `"ByXPost"` - PascalCase rename would emit `"ByxPost"`.
    #[serde(rename = "ByXPost", default)]
    pub byx_post: Vec<String>,
}

/// Account details from API key info.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
#[serde(rename_all = "camelCase")]
pub struct BitgetAccountDetails {
    /// No Bitget field.
    #[serde(default)]
    pub id: String,
    /// No Bitget field.
    #[serde(default)]
    pub note: String,
    /// No Bitget field (the key is not echoed).
    #[serde(default)]
    pub api_key: SecretString,
    /// No Bitget field.
    #[serde(default)]
    pub read_only: u8,
    /// No Bitget field.
    #[serde(default)]
    pub secret: SecretString,
    /// No Bitget field.
    #[serde(rename = "type", default)]
    pub key_type: u8,
    /// Permission names (`permissions`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub permissions: Vec<String>,
    /// Whitelisted IPs (`ips`, comma separated; empty when unrestricted).
    #[serde(default, deserialize_with = "deserialize_ip_list")]
    pub ips: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_optional_u64")]
    pub user_id: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_u64")]
    pub inviter_id: Option<u64>,
    /// No Bitget field.
    #[serde(default)]
    pub vip_level: String,
    /// No Bitget field.
    #[serde(default)]
    pub mkt_maker_level: u8,
    /// No Bitget field.
    #[serde(default)]
    pub affiliate_id: Option<u64>,
    /// No Bitget field.
    #[serde(default)]
    pub rsa_public_key: String,
    /// Whether the key belongs to a master account (no `parentId`).
    #[serde(default)]
    pub is_master: bool,
    /// Parent (master) UID (`parentId`).
    #[serde(
        rename = "parentId",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub parent_uid: String,
    /// No Bitget field; Bitget v3 keys are UTA keys.
    #[serde(default = "default_uta")]
    pub uta: u8,
    /// No Bitget field.
    #[serde(default)]
    pub kyc_level: String,
    /// No Bitget field.
    #[serde(default)]
    pub kyc_region: String,
    /// No Bitget field.
    #[serde(default)]
    pub unified: Option<i32>,
    /// No Bitget field.
    #[serde(default)]
    pub deadline_day: i64,
    /// No Bitget field.
    #[serde(default)]
    pub expired_at: Option<String>,
    /// Registration time in milliseconds (`regisTime`).
    #[serde(
        rename = "regisTime",
        default,
        deserialize_with = "deserialize_string_or_null"
    )]
    pub created_at: String,
    /// Permission type (`permType`).
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub perm_type: String,
}

const fn default_uta() -> u8 {
    1
}

fn deserialize_ip_list<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    Ok(match Option::<serde_json::Value>::deserialize(d)? {
        Some(serde_json::Value::String(ips)) => ips
            .split(',')
            .map(str::trim)
            .filter(|ip| !ip.is_empty())
            .map(str::to_string)
            .collect(),
        Some(serde_json::Value::Array(ips)) => ips
            .into_iter()
            .filter_map(|ip| ip.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    })
}

fn deserialize_optional_u64<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<u64>, D::Error> {
    let raw = deserialize_string_or_null(d)?;
    if raw.is_empty() {
        return Ok(None);
    }
    raw.parse().map(Some).map_err(serde::de::Error::custom)
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl BitgetAccountDetails {
    #[getter]
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[getter]
    #[must_use]
    pub fn note(&self) -> &str {
        &self.note
    }

    #[getter]
    #[must_use]
    pub fn api_key(&self) -> &str {
        self.api_key.expose_secret()
    }

    #[getter]
    #[must_use]
    pub fn read_only(&self) -> u8 {
        self.read_only
    }

    #[getter]
    #[must_use]
    pub fn key_type(&self) -> u8 {
        self.key_type
    }

    #[getter]
    #[must_use]
    pub fn user_id(&self) -> Option<u64> {
        self.user_id
    }

    #[getter]
    #[must_use]
    pub fn inviter_id(&self) -> Option<u64> {
        self.inviter_id
    }

    #[getter]
    #[must_use]
    pub fn vip_level(&self) -> &str {
        &self.vip_level
    }

    #[getter]
    #[must_use]
    pub fn mkt_maker_level(&self) -> u8 {
        self.mkt_maker_level
    }

    #[getter]
    #[must_use]
    pub fn affiliate_id(&self) -> Option<u64> {
        self.affiliate_id
    }

    #[getter]
    #[must_use]
    pub fn rsa_public_key(&self) -> &str {
        &self.rsa_public_key
    }

    #[getter]
    #[must_use]
    pub fn is_master(&self) -> bool {
        self.is_master
    }

    #[getter]
    #[must_use]
    pub fn parent_uid(&self) -> &str {
        &self.parent_uid
    }

    #[getter]
    #[must_use]
    pub fn uta(&self) -> u8 {
        self.uta
    }

    #[getter]
    #[must_use]
    pub fn kyc_level(&self) -> &str {
        &self.kyc_level
    }

    #[getter]
    #[must_use]
    pub fn kyc_region(&self) -> &str {
        &self.kyc_region
    }

    #[getter]
    #[must_use]
    pub fn deadline_day(&self) -> i64 {
        self.deadline_day
    }

    #[getter]
    #[must_use]
    pub fn expired_at(&self) -> Option<&str> {
        self.expired_at.as_deref()
    }

    #[getter]
    #[must_use]
    pub fn created_at(&self) -> &str {
        &self.created_at
    }
}

/// Response alias for API key info requests.
pub type BitgetAccountDetailsResponse = BitgetResponse<BitgetAccountDetails>;

/// Basic information about a sub-account member.
///
/// `member_type`, `status`, and `account_mode` use raw integer codes whose valid
/// ranges differ per endpoint; values are kept as-is rather than mapped to Rust
/// enums, consistent with other venue-raw fields in this module.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSubMember {
    pub uid: String,
    pub username: String,
    pub member_type: i32,
    pub status: i32,
    pub account_mode: i32,
    #[serde(default)]
    pub remark: String,
}

/// Result payload for `GET /api/v3/user/sub-list`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSubMembersResult {
    #[serde(default)]
    pub sub_members: Vec<BitgetSubMember>,
}

/// Response alias for the non-paginated sub-UID list.
pub type BitgetSubMembersResponse = BitgetResponse<BitgetSubMembersResult>;

/// Result payload for cursor-paginated sub-account listings.
///
/// The inner array is named `subMembers` and the cursor field is `nextCursor`
/// (with `"0"` as the end-of-pages sentinel), so the standard
/// `BitgetCursorListResponse<T>` (which expects `list` / `nextPageCursor`)
/// cannot be reused here. Callers treat `"0"` or an empty string as the
/// termination sentinel.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSubMembersPagedResult {
    #[serde(default)]
    pub sub_members: Vec<BitgetSubMember>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

impl BitgetSubMembersPagedResult {
    /// Returns the cursor to use for the next page, or `None` when the final
    /// page has been fetched.
    ///
    /// Bitget signals end-of-pages either by omitting the cursor or returning
    /// `"0"`/`""`; both cases collapse to `None` here so callers can treat any
    /// non-`None` return value as a live cursor.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&str> {
        match self.next_cursor.as_deref() {
            None | Some("" | "0") => None,
            Some(cursor) => Some(cursor),
        }
    }

    /// Returns `true` when the result has more pages to fetch.
    #[must_use]
    pub fn has_more_pages(&self) -> bool {
        self.continuation_cursor().is_some()
    }
}

/// Response alias for paginated sub-UID list (`/api/v3/user/sub-list`).
pub type BitgetSubMembersPagedResponse = BitgetResponse<BitgetSubMembersPagedResult>;

/// Response alias for the escrow (fund-custodial) sub-account list
/// (`/api/v3/user/sub-list`); shares the paginated sub-member shape.
pub type BitgetEscrowSubMembersResponse = BitgetResponse<BitgetSubMembersPagedResult>;

/// Information about a single sub-account API key.
///
/// Deliberately not shared with [`BitgetAccountDetails`]: master-level fields
/// such as `is_master`, `parent_uid`, `uta`, and the KYC block are absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSubApiKeyInfo {
    pub id: String,
    #[serde(default)]
    pub ips: Vec<String>,
    pub api_key: SecretString,
    #[serde(default)]
    pub note: String,
    pub status: i32,
    #[serde(default)]
    pub expired_at: Option<String>,
    pub created_at: String,
    #[serde(rename = "type")]
    pub key_type: BitgetApiKeyType,
    #[serde(with = "masked_secret")]
    pub secret: Option<SecretString>,
    #[serde(with = "bool_or_int")]
    pub read_only: bool,
    #[serde(default)]
    pub deadline_day: Option<i64>,
    #[serde(default)]
    pub flag: String,
    pub permissions: BitgetApiKeyPermissions,
}

/// Result payload for `GET /api/v3/user/sub-api-list`.
///
/// The inner array field is named `result` (nested inside the outer
/// `retCode/retMsg/result` envelope) rather than the usual `list`, so the
/// standard `BitgetCursorListResponse<T>` cannot be reused here.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSubApiKeysResult {
    #[serde(rename = "result", default)]
    pub keys: Vec<BitgetSubApiKeyInfo>,
    #[serde(default)]
    pub next_page_cursor: Option<String>,
}

impl BitgetSubApiKeysResult {
    /// Returns the cursor to use for the next page, or `None` when the final
    /// page has been fetched.
    ///
    /// The end-of-pages sentinel on this endpoint is an empty string rather
    /// than `"0"`; both that and a missing cursor collapse to `None`.
    #[must_use]
    pub fn continuation_cursor(&self) -> Option<&str> {
        match self.next_page_cursor.as_deref() {
            None | Some("") => None,
            Some(cursor) => Some(cursor),
        }
    }

    /// Returns `true` when the result has more pages to fetch.
    #[must_use]
    pub fn has_more_pages(&self) -> bool {
        self.continuation_cursor().is_some()
    }
}

/// Response alias for sub-account API keys list.
pub type BitgetSubApiKeysResponse = BitgetResponse<BitgetSubApiKeysResult>;

/// Shared result payload for API-key update endpoints (sub or master).
///
/// `/api/v3/user/update-sub-api` and the master-key update (no Bitget endpoint) share the field
/// set; only the number of permission buckets populated inside `permissions`
/// differs. Because [`BitgetApiKeyPermissions`] covers the superset of both,
/// the two endpoints reuse a single DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetApiKeyUpdateResult {
    pub id: String,
    #[serde(default)]
    pub note: String,
    pub api_key: SecretString,
    #[serde(with = "bool_or_int")]
    pub read_only: bool,
    #[serde(with = "masked_secret")]
    pub secret: Option<SecretString>,
    pub permissions: BitgetApiKeyPermissions,
    #[serde(default)]
    pub ips: Vec<String>,
}

/// Response alias for `POST /api/v3/user/update-sub-api`.
pub type BitgetUpdateSubApiResponse = BitgetResponse<BitgetApiKeyUpdateResult>;

/// Response alias for the master API key update (no Bitget endpoint; kept for API parity).
pub type BitgetUpdateMasterApiResponse = BitgetResponse<BitgetApiKeyUpdateResult>;

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::common::testing::load_test_json;

    #[rstest]
    fn deserialize_spot_instrument_uses_enums() {
        let json = load_test_json("http_get_instruments_spot.json");
        let response: BitgetInstrumentSpotResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        assert_eq!(instrument.status, BitgetInstrumentStatus::Trading);
        assert_eq!(instrument.innovation, BitgetInnovationFlag::Other);
        assert_eq!(instrument.margin_trading, BitgetMarginTrading::Other);
        assert_eq!(instrument.price_filter().tick_size, "0.01");
        assert_eq!(instrument.lot_size_filter().base_precision, "0.000001");
    }

    #[rstest]
    fn deserialize_linear_instrument_status() {
        let json = load_test_json("http_get_instruments_linear.json");
        let response: BitgetInstrumentLinearResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        assert_eq!(instrument.status, BitgetInstrumentStatus::Trading);
        assert_eq!(
            instrument.contract_type(),
            BitgetContractType::LinearPerpetual
        );
        assert_eq!(instrument.price_filter().tick_size, "0.1");
        assert_eq!(instrument.lot_size_filter().qty_step, "0.0001");
        assert_eq!(instrument.funding_interval, 480);
        assert_eq!(instrument.settle_coin(), "USDT");
        assert_eq!(response.result.list.len(), 3);
    }

    #[rstest]
    fn deserialize_spot_instrument_with_xstock_fields() {
        let json = load_test_json("http_get_instruments_spot_xstocks.json");
        let response: BitgetInstrumentSpotResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        assert_eq!(instrument.symbol_id, None);
        assert_eq!(instrument.symbol_type, Some(BitgetSymbolType::Stock));
        assert_eq!(instrument.is_reality, "yes");
        assert_eq!(instrument.xstock_multiplier, None);
    }

    #[rstest]
    fn deserialize_linear_instrument_with_symbol_type_and_id() {
        let json = load_test_json("http_get_instruments_linear_symbol_type.json");
        let response: BitgetInstrumentLinearResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        assert_eq!(instrument.symbol_id, None);
        assert_eq!(instrument.symbol_type, Some(BitgetSymbolType::Stock));
        assert_eq!(instrument.buy_limit_price_ratio, "0.02");
    }

    #[derive(Deserialize)]
    struct SymbolTypeWrap {
        #[serde(rename = "symbolType")]
        t: BitgetSymbolType,
    }

    #[rstest]
    fn deserialize_symbol_type_falls_back_to_other_for_unknown() {
        let json = r#"{"symbolType": "newthing"}"#;
        let parsed: SymbolTypeWrap = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.t, BitgetSymbolType::Other);
    }

    #[rstest]
    fn deserialize_account_info_response() {
        let json = load_test_json("http_get_account_info.json");
        let response: BitgetAccountInfoResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(response.result.margin_mode, BitgetMarginMode::RegularMargin);
        assert_eq!(
            response.result.unified_margin_status,
            BitgetUnifiedMarginStatus::Hybrid
        );
        assert_eq!(
            response.result.hold_mode,
            Some(crate::common::enums::BitgetPositionMode::MergedSingle)
        );
        assert!(!response.result.is_master_trader);
        assert!(!response.result.spot_hedging_status);
        assert!(!response.result.dcp_status);
        assert_eq!(response.result.symbol_config_list[0].leverage, "5");
    }

    #[rstest]
    fn deserialize_account_info_without_deprecated_fields() {
        let json = r#"{
            "code": "00000",
            "msg": "success",
            "requestTime": 1753787749280,
            "data": {
                "accountMode": "unified",
                "accountLevel": "advanced",
                "holdMode": "hedge_mode"
            }
        }"#;
        let response: BitgetAccountInfoResponse = serde_json::from_str(json).unwrap();

        assert_eq!(
            response.result.margin_mode,
            BitgetMarginMode::PortfolioMargin
        );
        assert_eq!(
            response.result.unified_margin_status,
            BitgetUnifiedMarginStatus::Unified
        );
        assert!(!response.result.spot_hedging_status);
        assert_eq!(response.result.time_window, 0);
        assert_eq!(response.result.smp_group, 0);
        assert!(response.result.symbol_config_list.is_empty());
    }

    #[rstest]
    fn deserialize_order_response_maps_enums() {
        let json = load_test_json("http_get_orders_history.json");
        let response: BitgetOrderHistoryResponse = serde_json::from_str(&json).unwrap();
        let order = &response.result.list[0];

        assert_eq!(order.cancel_type, BitgetCancelType::Other);
        assert_eq!(order.tp_trigger_by, BitgetTriggerType::None);
        assert_eq!(order.sl_trigger_by, BitgetTriggerType::None);
        assert_eq!(order.tpsl_mode, None);
        assert_eq!(order.order_type, BitgetOrderType::Market);
        assert_eq!(order.order_status, BitgetOrderStatus::Filled);
        assert_eq!(order.time_in_force, BitgetTimeInForce::Ioc);
        assert_eq!(order.smp_type, BitgetSmpType::None);
        assert_eq!(order.smp_group, 0);
        assert!(order.reduce_only);
        assert_eq!(order.cum_exec_fee, "0.53523428");
        assert_eq!(order.pos_side, BitgetPositionSide::Buy);
        assert_eq!(order.hold_mode, "one_way_mode");
        assert!(response.result.next_page_cursor.is_some());
    }

    #[rstest]
    fn deserialize_order_response_maps_cancel_reasons() {
        let json = load_test_json("http_get_orders_history.json");
        let response: BitgetOrderHistoryResponse = serde_json::from_str(&json).unwrap();
        let reasons: Vec<BitgetCancelType> = response
            .result
            .list
            .iter()
            .filter(|order| order.order_status == BitgetOrderStatus::Canceled)
            .map(|order| order.cancel_type)
            .collect();

        assert!(reasons.contains(&BitgetCancelType::CancelByIocNotFull));
        assert!(reasons.contains(&BitgetCancelType::CancelByUser));
    }

    #[rstest]
    fn deserialize_open_order_with_null_tp_sl_fields() {
        let json = load_test_json("http_get_orders_realtime.json");
        let response: BitgetOpenOrdersResponse = serde_json::from_str(&json).unwrap();
        let order = &response.result.list[0];

        assert_eq!(order.order_status, BitgetOrderStatus::New);
        assert_eq!(order.time_in_force, BitgetTimeInForce::PostOnly);
        assert_eq!(order.take_profit, "");
        assert_eq!(order.cum_exec_fee, "0");
        assert!(!order.reduce_only);
    }

    #[rstest]
    fn deserialize_order_info_partially_filled_then_cancelled() {
        let json = load_test_json("http_get_order_partially_filled_rejected.json");
        let response: BitgetResponse<BitgetOrder> = serde_json::from_str(&json).unwrap();
        let order = &response.result;

        assert_eq!(order.order_status, BitgetOrderStatus::Canceled);
        assert_eq!(order.cancel_type, BitgetCancelType::CancelByIocNotFull);
        assert_eq!(order.cum_exec_qty, "3.71");
        assert_eq!(order.qty, "4.71");
        assert_eq!(order.pos_side, BitgetPositionSide::Sell);
    }

    #[rstest]
    fn deserialize_strategy_order_history() {
        let json = load_test_json("http_get_strategy_orders_history.json");
        let response: BitgetOrderHistoryResponse = serde_json::from_str(&json).unwrap();
        let order = &response.result.list[0];

        assert_eq!(order.stop_order_type, BitgetStopOrderType::Stop);
        assert_eq!(order.order_status, BitgetOrderStatus::Canceled);
        assert_eq!(order.trigger_by, BitgetTriggerType::MarkPrice);
        assert_eq!(order.trigger_price, "396.89");
        assert_eq!(order.order_type, BitgetOrderType::Market);
        assert_eq!(
            response.result.next_page_cursor.as_deref(),
            Some("1503521067782803459")
        );
    }

    #[rstest]
    fn deserialize_wallet_balance_without_optional_fields() {
        let json = r#"{
            "code": "00000",
            "msg": "success",
            "requestTime": 1746687063471,
            "data": {
                "accountEquity": "1000.00",
                "assets": [{
                    "coin": "USDT",
                    "equity": "1000.00",
                    "usdValue": "1000.00",
                    "balance": "1000.00",
                    "available": "1000.00",
                    "locked": "0"
                }]
            }
        }"#;

        let response: BitgetWalletBalanceResponse = serde_json::from_str(json)
            .expect("Failed to parse wallet balance without optional fields");

        assert_eq!(response.ret_code, 0);
        assert_eq!(response.result.coin[0].total_order_im, None);
        assert_eq!(response.result.coin[0].total_position_mm, None);
        assert_eq!(response.result.coin[0].borrow_amount, "");
    }

    #[rstest]
    fn deserialize_wallet_balance_from_docs() {
        let json = load_test_json("http_get_wallet_balance.json");

        let response: BitgetWalletBalanceResponse =
            serde_json::from_str(&json).expect("Failed to parse wallet balance");

        assert_eq!(response.ret_code, 0);
        assert_eq!(response.ret_msg, "success");

        let wallet = &response.result;
        assert_eq!(wallet.total_equity, "99923.24521669");
        assert_eq!(wallet.total_initial_margin, "91.05");
        assert_eq!(wallet.total_maintenance_margin, "9.26");
        assert_eq!(wallet.account_type, BitgetAccountType::Unified);

        let usdt = wallet.coin.iter().find(|c| c.coin == "USDT").unwrap();
        assert_eq!(usdt.wallet_balance, dec!(49998.28810184));
        assert_eq!(usdt.available_to_withdraw, "49998.28810184");
        assert_eq!(usdt.borrow_amount, "0");
        assert_eq!(usdt.spot_borrow, Decimal::ZERO);
    }

    #[rstest]
    #[case("http_get_wallet_balance_spot_short.json", dec!(-0.5), "0.5")]
    #[case("http_get_wallet_balance_with_spot_borrow.json", dec!(1.5), "0.5")]
    fn deserialize_wallet_balance_with_borrowed_coin(
        #[case] fixture: &str,
        #[case] balance: Decimal,
        #[case] debt: &str,
    ) {
        let response: BitgetWalletBalanceResponse =
            serde_json::from_str(&load_test_json(fixture)).unwrap();
        let eth = response
            .result
            .coin
            .iter()
            .find(|c| c.coin == "ETH")
            .unwrap();

        assert_eq!(eth.wallet_balance, balance);
        assert_eq!(eth.borrow_amount, debt);
    }

    #[rstest]
    fn deserialize_order_history_with_duplicate_row() {
        let response: BitgetOrderHistoryResponse = serde_json::from_str(&load_test_json(
            "http_get_orders_history_with_duplicate.json",
        ))
        .unwrap();
        let list = &response.result.list;

        assert_eq!(list[0].order_id, list[1].order_id);
    }

    #[rstest]
    fn deserialize_open_order_with_tp_sl() {
        let response: BitgetOpenOrdersResponse =
            serde_json::from_str(&load_test_json("http_get_orders_realtime_tp_sl.json")).unwrap();
        let order = &response.result.list[0];

        assert_eq!(order.take_profit, "400");
        assert_eq!(order.stop_loss, "300");
        assert_eq!(order.sl_trigger_by, BitgetTriggerType::MarkPrice);
    }

    #[rstest]
    fn deserialize_position_with_open_time() {
        let response: BitgetPositionListResponse =
            serde_json::from_str(&load_test_json("http_get_positions_with_open_time.json"))
                .unwrap();

        assert!(!response.result.list[0].created_time.is_empty());
    }

    #[rstest]
    fn deserialize_borrow_response() {
        let json = r#"{
            "code": "00000",
            "msg": "success",
            "requestTime": 1756197991955,
            "data": {
                "coin": "BTC",
                "amount": "0.01"
            }
        }"#;

        let response: BitgetBorrowResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.ret_code, 0);
        assert_eq!(response.ret_msg, "success");
        assert_eq!(response.result.coin, "BTC");
        assert_eq!(response.result.amount, "0.01");
    }

    #[rstest]
    fn deserialize_no_convert_repay_response() {
        let json = r#"{"code":"00000","msg":"success","requestTime":1234567890,"data":"success"}"#;

        let response: BitgetNoConvertRepayResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.ret_code, 0);
        assert_eq!(response.result.result_status, BitgetRepayStatus::Success);
    }

    #[rstest]
    fn deserialize_repay_response() {
        let json = r#"{"code":"00000","msg":"success","requestTime":1756295680801,"data":null}"#;

        let response: BitgetRepayResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.ret_code, 0);
        assert_eq!(response.result.result_status, BitgetRepayStatus::Success);
    }

    #[rstest]
    fn deserialize_position_without_conditional_fields() {
        let json = load_test_json("http_get_positions.json");

        let response: BitgetPositionListResponse =
            serde_json::from_str(&json).expect("Failed to parse position list");

        let position = &response.result.list[0];
        assert_eq!(position.side, BitgetPositionSide::Buy);
        assert_eq!(position.size, "0.02");
        assert_eq!(position.trade_mode, 0);
        assert_eq!(position.hold_mode, "one_way_mode");
        assert!(!position.is_reduce_only);
        assert_eq!(position.seq, -1);
        assert_eq!(position.open_time, 0);
        assert_eq!(position.position_status, BitgetPositionStatus::Normal);
    }

    #[rstest]
    fn deserialize_position_list_null_is_empty() {
        let json = load_test_json("http_get_positions_empty.json");

        let response: BitgetPositionListResponse = serde_json::from_str(&json).unwrap();

        assert!(response.result.list.is_empty());
    }

    #[rstest]
    fn deserialize_inverse_instrument_with_symbol_type_and_id() {
        let json = load_test_json("http_get_instruments_inverse_symbol_type.json");
        let response: BitgetInstrumentInverseResponse = serde_json::from_str(&json).unwrap();
        let instrument = &response.result.list[0];

        assert_eq!(instrument.symbol_id, None);
        assert_eq!(instrument.symbol_type, Some(BitgetSymbolType::Crypto));
        assert_eq!(
            instrument.contract_type(),
            BitgetContractType::InverseFutures
        );
        assert_eq!(instrument.funding_interval, 0);
        assert_eq!(instrument.settle_coin(), "BTC");
    }

    #[rstest]
    fn deserialize_executions_response() {
        let json = load_test_json("http_get_executions.json");
        let response: BitgetTradeHistoryResponse = serde_json::from_str(&json).unwrap();
        let execution = &response.result.list[0];

        assert_eq!(execution.exec_type, BitgetExecType::Trade);
        assert!(!execution.is_maker);
        assert_eq!(execution.exec_fee, "0.53523428");
        assert_eq!(execution.exec_time, "1794193218321");
        assert_eq!(execution.order_link_id, "tdcapflp93218");
    }

    #[rstest]
    fn deserialize_executions_null_list_is_empty() {
        let json = load_test_json("http_get_executions_empty.json");
        let response: BitgetTradeHistoryResponse = serde_json::from_str(&json).unwrap();

        assert!(response.result.list.is_empty());
        assert!(response.result.next_page_cursor.is_none());
    }

    #[rstest]
    fn deserialize_market_data_responses() {
        let book: BitgetOrderbookResponse =
            serde_json::from_str(&load_test_json("http_get_orderbook.json")).unwrap();
        assert_eq!(
            book.result.a[0],
            ["75228.9".to_string(), "2.6781".to_string()]
        );
        assert_eq!(book.result.ts, 1_794_192_836_072);

        let klines: BitgetKlinesResponse =
            serde_json::from_str(&load_test_json("http_get_klines_linear.json")).unwrap();
        assert_eq!(klines.result.list.len(), 3);
        assert_eq!(klines.result.list[0].open, "75225.3");

        let trades: BitgetTradesResponse =
            serde_json::from_str(&load_test_json("http_get_trades_recent.json")).unwrap();
        assert_eq!(trades.result.list[0].side, BitgetOrderSide::Sell);
        assert_eq!(trades.result.list[0].time, "1794192830666");

        let funding: BitgetFundingResponse =
            serde_json::from_str(&load_test_json("http_get_funding_history.json")).unwrap();
        assert_eq!(funding.result.list.len(), 3);

        let time: BitgetServerTimeResponse =
            serde_json::from_str(&load_test_json("http_get_server_time.json")).unwrap();
        assert_eq!(time.result.time_second_str(), "1794192837");

        let fee: BitgetFeeRateResponse =
            serde_json::from_str(&load_test_json("http_get_fee_rate.json")).unwrap();
        assert_eq!(fee.result.list[0].taker_fee_rate, "0.0004");

        let tickers: BitgetTickersLinearResponse =
            serde_json::from_str(&load_test_json("http_get_tickers_linear.json")).unwrap();
        assert_eq!(tickers.result.list[0].mark_price, "75230.7");
    }

    #[rstest]
    fn deserialize_permissions_renamed_buckets_preserve_values() {
        // Regression guard for `#[serde(rename = ...)]` on permission keys of the
        // parity-kept sub-account DTOs
        let json = r#"{
            "NFT": ["NFTQueryProductList"],
            "FiatP2P": ["P2PDeposit"],
            "ByXPost": ["PostContent"]
        }"#;
        let perms: BitgetApiKeyPermissions =
            serde_json::from_str(json).expect("parse renamed buckets");
        assert_eq!(perms.nft, vec!["NFTQueryProductList"]);
        assert_eq!(perms.fiat_p2p, vec!["P2PDeposit"]);
        assert_eq!(perms.byx_post, vec!["PostContent"]);
    }

    #[rstest]
    fn deserialize_account_details_response_with_current_docs_example() {
        let json = load_test_json("http_get_user_query_api.json");
        let response: BitgetAccountDetailsResponse =
            serde_json::from_str(&json).expect("parse account details");
        let debug = format!("{:?}", response.result);

        assert_eq!(response.result.user_id, Some(21_967_229_145));
        assert_eq!(response.result.inviter_id, None);
        assert_eq!(response.result.parent_uid, "9322381550");
        assert!(response.result.ips.is_empty());
        assert!(response.result.permissions.is_empty());
        assert_eq!(response.result.created_at, "1794152051000");
        assert!(debug.contains("api_key: <redacted>"));
        assert!(debug.contains("secret: <redacted>"));
    }
}
