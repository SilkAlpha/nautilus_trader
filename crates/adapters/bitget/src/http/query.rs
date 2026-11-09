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

//! Builder types for Bitget REST query parameters and filters.

use derive_builder::Builder;
#[cfg(test)]
use nautilus_core::string::secret::REDACTED;
use nautilus_core::string::secret::SecretString;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::common::{
    enums::{
        BitgetAccountType, BitgetBboSideType, BitgetExecType, BitgetInstrumentStatus,
        BitgetKlineInterval, BitgetMarginMode, BitgetMarketUnit, BitgetOpenOnly, BitgetOptionType,
        BitgetOrderFilter, BitgetOrderSide, BitgetOrderSmpType, BitgetOrderStatus, BitgetOrderType,
        BitgetPositionIdx, BitgetPositionMode, BitgetProductType, BitgetStopOrderType,
        BitgetTimeInForce, BitgetTpSlMode, BitgetTriggerDirection, BitgetTriggerType,
    },
    parse::opt_bool_as_int,
};

/// Serializes an optional reduce-only flag as Bitget `YES`/`NO`.
fn serialize_opt_yes_no<S: serde::Serializer>(
    value: &Option<bool>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(true) => s.serialize_str("YES"),
        Some(false) => s.serialize_str("NO"),
        None => s.serialize_none(),
    }
}

/// Serializes a hedge-mode position index as Bitget `posSide` (`long`/`short`).
fn serialize_pos_side<S: serde::Serializer>(
    value: &Option<BitgetPositionIdx>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match value.and_then(BitgetPositionIdx::as_pos_side) {
        Some(side) => side.serialize(s),
        None => s.serialize_none(),
    }
}

/// Returns `true` when no `posSide` is sent (one-way mode or unset).
fn is_one_way(value: &Option<BitgetPositionIdx>) -> bool {
    value.and_then(BitgetPositionIdx::as_pos_side).is_none()
}

/// Serializes a batch body as the JSON array Bitget expects, with `category` on every entry.
fn serialize_batch<S, T>(
    category: BitgetProductType,
    entries: &[T],
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: Serialize,
{
    use serde::ser::{Error, SerializeSeq};

    let mut seq = s.serialize_seq(Some(entries.len()))?;
    for entry in entries {
        let mut value = serde_json::to_value(entry).map_err(S::Error::custom)?;
        if let Some(map) = value.as_object_mut() {
            map.insert(
                "category".to_string(),
                serde_json::to_value(category).map_err(S::Error::custom)?,
            );
        }
        seq.serialize_element(&value)?;
    }
    seq.end()
}

/// Query parameters for `GET /api/v3/market/instruments`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetInstrumentsInfoParams {
    pub category: BitgetProductType,
    #[builder(setter(strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub status: Option<BitgetInstrumentStatus>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub limit: Option<u32>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub cursor: Option<String>,
}

/// Query parameters for `GET /api/v3/market/tickers`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetTickersParams {
    pub category: BitgetProductType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub exp_date: Option<String>,
}

/// Query parameters for `GET /api/v3/market/candles`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(setter(into, strip_option), default)]
pub struct BitgetKlinesParams {
    pub category: BitgetProductType,
    pub symbol: String,
    pub interval: BitgetKlineInterval,
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    pub start: Option<i64>,
    #[serde(rename = "endTime", skip_serializing_if = "Option::is_none")]
    pub end: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Price type (`market`, `mark`, `index`); Bitget serves mark/index candles on the same
    /// endpoint.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kline_type: Option<String>,
}

impl Default for BitgetKlinesParams {
    fn default() -> Self {
        Self {
            category: BitgetProductType::Linear,
            symbol: String::new(),
            interval: BitgetKlineInterval::Minute1,
            start: None,
            end: None,
            limit: None,
            kline_type: None,
        }
    }
}

/// Query parameters for `GET /api/v3/market/fills`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(setter(into, strip_option), default)]
pub struct BitgetTradesParams {
    pub category: BitgetProductType,
    pub symbol: String,
    /// Not a Bitget parameter; never sent.
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[serde(skip_serializing)]
    pub option_type: Option<BitgetOptionType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl Default for BitgetTradesParams {
    fn default() -> Self {
        Self {
            category: BitgetProductType::Linear,
            symbol: String::new(),
            base_coin: None,
            option_type: None,
            limit: None,
        }
    }
}

/// Query parameters for `GET /api/v3/market/history-fund-rate`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/derivatives>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(setter(into, strip_option), default)]
pub struct BitgetFundingParams {
    pub category: BitgetProductType,
    pub symbol: String,
    /// Not a Bitget parameter (history is paged by `cursor`); used to filter client side.
    #[serde(skip_serializing)]
    pub start_time: Option<i64>,
    /// Not a Bitget parameter (history is paged by `cursor`); used to filter client side.
    #[serde(skip_serializing)]
    pub end_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Page number (1-based, maximum 100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<u32>,
}

impl Default for BitgetFundingParams {
    fn default() -> Self {
        Self {
            category: BitgetProductType::Linear,
            symbol: String::new(),
            start_time: None,
            end_time: None,
            limit: None,
            cursor: None,
        }
    }
}

/// Query parameters for `GET /api/v3/market/orderbook`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/market/market-data>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[builder(setter(into, strip_option), default)]
pub struct BitgetOrderbookParams {
    pub category: BitgetProductType,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl Default for BitgetOrderbookParams {
    fn default() -> Self {
        Self {
            category: BitgetProductType::Linear,
            symbol: String::new(),
            limit: None,
        }
    }
}

/// Query parameters for the coin info query (no Bitget endpoint; kept for API parity).
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetCoinInfoParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub coin: Option<String>,
}

/// Query parameters for `GET /api/v3/account/all-fee-rate`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetFeeRateParams {
    pub category: BitgetProductType,
    #[builder(setter(strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
}

/// Query parameters for `GET /api/v3/account/assets`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWalletBalanceParams {
    /// Not a Bitget parameter; never sent.
    #[serde(skip_serializing)]
    pub account_type: BitgetAccountType,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub coin: Option<String>,
}

/// Query parameters for `GET /api/v3/position/current-position`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/position-management>
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetPositionListParams {
    pub category: BitgetProductType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub settle_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub limit: Option<u32>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub cursor: Option<String>,
}

/// Body parameters for `POST /api/v3/account/adjust-account-mode`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSetMarginModeParams {
    #[serde(rename = "mode")]
    pub set_margin_mode: BitgetMarginMode,
}

/// Body parameters for `POST /api/v3/account/set-leverage`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSetLeverageParams {
    pub category: BitgetProductType,
    pub symbol: String,
    /// Sent as `leverage`: Bitget sets one leverage per symbol in crossed margin.
    #[serde(rename = "leverage")]
    pub buy_leverage: String,
    /// Not sent: only isolated hedge-mode positions take separate long/short leverage.
    #[serde(skip_serializing)]
    pub sell_leverage: String,
}

/// Body parameters for `POST /api/v3/account/set-hold-mode`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/account/account-settings>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSwitchModeParams {
    /// Not a Bitget parameter (the hold mode is account wide); never sent.
    #[serde(skip_serializing)]
    pub category: BitgetProductType,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub coin: Option<String>,
    #[serde(rename = "holdMode")]
    pub mode: BitgetPositionMode,
}

/// Body parameters for `POST /api/v3/trade/place-strategy-order`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/position-management>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSetTradingStopParams {
    pub category: BitgetProductType,
    pub symbol: String,
    /// Sent as `posSide` in hedge mode only.
    #[serde(
        rename = "posSide",
        serialize_with = "serialize_pos_side_required",
        skip_serializing_if = "is_one_way_required"
    )]
    pub position_idx: BitgetPositionIdx,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[serde(skip_serializing)]
    pub trailing_stop: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_trigger_by: Option<BitgetTriggerType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_trigger_by: Option<BitgetTriggerType>,
    /// Not a Bitget parameter; never sent.
    #[serde(skip_serializing)]
    pub active_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tpsl_mode: Option<BitgetTpSlMode>,
    /// Not a Bitget parameter; never sent.
    #[serde(skip_serializing)]
    pub tp_size: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[serde(skip_serializing)]
    pub sl_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_limit_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_limit_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_order_type: Option<BitgetOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_order_type: Option<BitgetOrderType>,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde passes fields by reference
fn serialize_pos_side_required<S: serde::Serializer>(
    value: &BitgetPositionIdx,
    s: S,
) -> Result<S::Ok, S::Error> {
    serialize_pos_side(&Some(*value), s)
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde passes fields by reference
fn is_one_way_required(value: &BitgetPositionIdx) -> bool {
    value.as_pos_side().is_none()
}

/// Body parameters for `POST /api/v3/account/borrow`.
///
/// # References
///
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBorrowParams {
    pub coin: String,
    pub amount: String,
}

/// Body parameters for `POST /api/v3/account/repay`.
///
/// # References
///
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetNoConvertRepayParams {
    pub coin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub amount: Option<String>,
}

/// Body parameters for `POST /api/v3/account/repay`.
///
/// # References
///
/// - <https://www.bitget.com/docs/catalog/account/assets-balance>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetRepayParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub coin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub amount: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub repayment_type: Option<String>,
}

/// Order entry payload for `POST /api/v3/trade/place-batch`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBatchPlaceOrderEntry {
    pub symbol: String,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub is_leverage: Option<i32>,
    pub side: BitgetOrderSide,
    pub order_type: BitgetOrderType,
    pub qty: String,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub market_unit: Option<BitgetMarketUnit>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub trigger_direction: Option<BitgetTriggerDirection>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub order_filter: Option<BitgetOrderFilter>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub trigger_price: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub trigger_by: Option<BitgetTriggerType>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub order_iv: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<BitgetTimeInForce>,
    /// Sent as `posSide` in hedge mode only.
    #[builder(default)]
    #[serde(
        rename = "posSide",
        serialize_with = "serialize_pos_side",
        skip_serializing_if = "is_one_way"
    )]
    pub position_idx: Option<BitgetPositionIdx>,
    #[builder(default)]
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub order_link_id: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_trigger_by: Option<BitgetTriggerType>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_trigger_by: Option<BitgetTriggerType>,
    /// Sent as `YES`/`NO`.
    #[builder(default)]
    #[serde(
        serialize_with = "serialize_opt_yes_no",
        skip_serializing_if = "Option::is_none"
    )]
    pub reduce_only: Option<bool>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub close_on_trigger: Option<bool>,
    #[builder(default)]
    #[serde(rename = "stpMode", skip_serializing_if = "Option::is_none")]
    pub smp_type: Option<BitgetOrderSmpType>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub mmp: Option<bool>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub tpsl_mode: Option<BitgetTpSlMode>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_limit_price: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_limit_price: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_order_type: Option<BitgetOrderType>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_order_type: Option<BitgetOrderType>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub bbo_side_type: Option<BitgetBboSideType>,
    /// Bitget `bboLevel` (`queue1`, `queue5`, `counterparty1`, `counterparty5`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbo_level: Option<String>,
}

/// Body parameters for `POST /api/v3/trade/place-batch`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
///
/// Bitget takes a bare JSON array of entries, each carrying `category`.
#[derive(Clone, Debug, Deserialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBatchPlaceOrderParams {
    pub category: BitgetProductType,
    pub request: Vec<BitgetBatchPlaceOrderEntry>,
}

impl Serialize for BitgetBatchPlaceOrderParams {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        serialize_batch(self.category, &self.request, s)
    }
}

/// Native TP/SL and option-specific fields that map onto the `POST /api/v3/trade/place-order` entry.
///
/// Bundled to keep the `submit_order` signature manageable, and to give the demo HTTP path
/// access to the same fields the mainnet WS path supports via
/// [`crate::websocket::messages::BitgetWsPlaceOrderParams`]. All fields are optional; populated
/// fields are written onto the entry builder as-is. `tpsl_mode` defaults to `Full` upstream when
/// only `take_profit` / `stop_loss` are set without an explicit mode.
///
/// `tp_trigger_price` / `sl_trigger_price` are intentionally absent: the create-order entry does
/// not carry them (the mainnet WS Trade API does, via separate fields).
#[derive(Debug, Clone, Default)]
pub struct BitgetNativeTpSlParams {
    pub take_profit: Option<String>,
    pub stop_loss: Option<String>,
    pub tp_trigger_by: Option<BitgetTriggerType>,
    pub sl_trigger_by: Option<BitgetTriggerType>,
    pub tp_order_type: Option<BitgetOrderType>,
    pub sl_order_type: Option<BitgetOrderType>,
    pub tp_limit_price: Option<String>,
    pub sl_limit_price: Option<String>,
    pub tpsl_mode: Option<BitgetTpSlMode>,
    pub close_on_trigger: Option<bool>,
    pub order_iv: Option<String>,
    pub mmp: Option<bool>,
}

impl BitgetNativeTpSlParams {
    /// Returns `true` if no TP/SL or option-specific field is set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.take_profit.is_none()
            && self.stop_loss.is_none()
            && self.tp_trigger_by.is_none()
            && self.sl_trigger_by.is_none()
            && self.tp_order_type.is_none()
            && self.sl_order_type.is_none()
            && self.tp_limit_price.is_none()
            && self.sl_limit_price.is_none()
            && self.tpsl_mode.is_none()
            && self.close_on_trigger.is_none()
            && self.order_iv.is_none()
            && self.mmp.is_none()
    }
}

/// Body parameters for `POST /api/v3/trade/place-order`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetPlaceOrderParams {
    #[serde(flatten)]
    pub order: BitgetBatchPlaceOrderEntry,
    pub category: BitgetProductType,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub slippage_tolerance_type: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slippage_tolerance: Option<String>,
}

/// Body parameters for `POST /api/v3/trade/place-strategy-order` (conditional orders).
///
/// Bitget keeps trigger orders out of the regular order book endpoints: stop and if-touched
/// orders become `type = "trigger"` strategy orders.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetPlaceStrategyOrderParams {
    pub category: BitgetProductType,
    pub symbol: String,
    /// Strategy type (`trigger` or `tpsl`).
    #[serde(rename = "type")]
    pub strategy_type: BitgetStopOrderType,
    pub qty: String,
    pub side: BitgetOrderSide,
    #[serde(
        rename = "posSide",
        serialize_with = "serialize_pos_side",
        skip_serializing_if = "is_one_way"
    )]
    pub position_idx: Option<BitgetPositionIdx>,
    #[serde(
        serialize_with = "serialize_opt_yes_no",
        skip_serializing_if = "Option::is_none"
    )]
    pub reduce_only: Option<bool>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_by: Option<BitgetTriggerType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_order_type: Option<BitgetOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_order_price: Option<String>,
}

impl BitgetPlaceStrategyOrderParams {
    /// Builds a `trigger` strategy order from a regular order body carrying `trigger_price`.
    #[must_use]
    pub fn trigger_from(params: &BitgetPlaceOrderParams) -> Self {
        let order = &params.order;
        Self {
            category: params.category,
            symbol: order.symbol.clone(),
            strategy_type: BitgetStopOrderType::Stop,
            qty: order.qty.clone(),
            side: order.side,
            position_idx: order.position_idx,
            reduce_only: order.reduce_only,
            order_link_id: order.order_link_id.clone(),
            trigger_by: order.trigger_by,
            trigger_price: order.trigger_price.clone(),
            trigger_order_type: Some(order.order_type),
            trigger_order_price: order.price.clone(),
        }
    }
}

/// Body parameters for `POST /api/v3/trade/cancel-strategy-order`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetCancelStrategyOrderParams {
    pub category: BitgetProductType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
}

/// Body parameters for `POST /api/v3/trade/modify-strategy-order`.
///
/// The venue requires `qty` on every modify; trigger fields are unchanged when omitted.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetModifyStrategyOrderParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    pub order_link_id: Option<String>,
    pub qty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_order_price: Option<String>,
}

/// Amend entry for `POST /api/v3/trade/batch-modify-order`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBatchAmendOrderEntry {
    pub symbol: String,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub order_id: Option<String>,
    #[builder(default)]
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub order_link_id: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub order_iv: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub trigger_price: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub tpsl_mode: Option<BitgetTpSlMode>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_trigger_by: Option<BitgetTriggerType>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_trigger_by: Option<BitgetTriggerType>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub trigger_by: Option<BitgetTriggerType>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp_limit_price: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl_limit_price: Option<String>,
}

/// Body parameters for `POST /api/v3/trade/batch-modify-order`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
///
/// Bitget takes a bare JSON array of entries, each carrying `category`.
#[derive(Clone, Debug, Deserialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBatchAmendOrderParams {
    pub category: BitgetProductType,
    pub request: Vec<BitgetBatchAmendOrderEntry>,
}

impl Serialize for BitgetBatchAmendOrderParams {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        serialize_batch(self.category, &self.request, s)
    }
}

/// Body parameters for `POST /api/v3/trade/modify-order`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetAmendOrderParams {
    #[serde(flatten)]
    pub order: BitgetBatchAmendOrderEntry,
    pub category: BitgetProductType,
}

/// Cancel entry for `POST /api/v3/trade/cancel-batch`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBatchCancelOrderEntry {
    pub symbol: String,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub order_id: Option<String>,
    #[builder(default)]
    #[serde(rename = "clientOid", skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub order_link_id: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub order_filter: Option<BitgetOrderFilter>,
}

/// Body parameters for `POST /api/v3/trade/cancel-batch`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
///
/// Bitget takes a bare JSON array of entries, each carrying `category`.
#[derive(Clone, Debug, Deserialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBatchCancelOrderParams {
    pub category: BitgetProductType,
    pub request: Vec<BitgetBatchCancelOrderEntry>,
}

impl Serialize for BitgetBatchCancelOrderParams {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        serialize_batch(self.category, &self.request, s)
    }
}

/// Body parameters for `POST /api/v3/trade/cancel-order`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
pub struct BitgetCancelOrderParams {
    #[serde(flatten)]
    pub order: BitgetBatchCancelOrderEntry,
    pub category: BitgetProductType,
}

/// Body parameters for `POST /api/v3/trade/cancel-symbol-order`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetCancelAllOrdersParams {
    pub category: BitgetProductType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub settle_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_filter: Option<BitgetOrderFilter>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub stop_order_type: Option<BitgetStopOrderType>,
}

/// Query parameters for `GET /api/v3/trade/unfilled-orders`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetOpenOrdersParams {
    pub category: BitgetProductType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub settle_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_id: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_link_id: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub open_only: Option<BitgetOpenOnly>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_filter: Option<BitgetOrderFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub cursor: Option<String>,
}

/// Query parameters for `GET /api/v3/trade/history-orders`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetOrderHistoryParams {
    pub category: BitgetProductType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub settle_coin: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_id: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_link_id: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub open_only: Option<BitgetOpenOnly>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_filter: Option<BitgetOrderFilter>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_status: Option<BitgetOrderStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub start_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "endTime")]
    #[builder(setter(strip_option))]
    pub end_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub cursor: Option<String>,
}

/// Query parameters for `GET /api/v3/trade/fills`.
///
/// # References
/// - <https://www.bitget.com/docs/catalog/trading/order-management>
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetTradeHistoryParams {
    pub category: BitgetProductType,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub symbol: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub base_coin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub order_id: Option<String>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub order_link_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub start_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "endTime")]
    #[builder(setter(strip_option))]
    pub end_time: Option<i64>,
    /// Not a Bitget parameter; never sent.
    #[builder(setter(strip_option))]
    #[serde(skip_serializing)]
    pub exec_type: Option<BitgetExecType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub cursor: Option<String>,
}

/// Request-side permission patch for API-key update endpoints (`PascalCase`).
///
/// Kept separate from the response-side
/// [`crate::http::models::BitgetApiKeyPermissions`]: every field here is
/// `Option<Vec<String>>` with `skip_serializing_if`, so an unset bucket is
/// omitted from the request body entirely rather than being serialized as an
/// explicit empty array (which the venue treats as "clear all permissions").
///
/// The field set is the superset of the master and sub-account permission
/// tables. Buckets that only appear for master-account keys (`FiatP2P`,
/// `FiatBitgetPay`, `FiatBitPay`, `FiatConvertBroker`, `BitCard`, `ByXPost`)
/// must not be set when calling `POST /api/v3/user/update-sub-api`; callers are
/// expected to only populate the buckets documented for the endpoint being
/// invoked.
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "PascalCase")]
#[builder(default)]
#[builder(setter(into, strip_option))]
pub struct BitgetApiKeyPermissionUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_trade: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spot: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wallet: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derivatives: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earn: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affiliate: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_trade: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copy_trading: Option<Vec<String>>,
    // Bitget ships this key uppercase (`"NFT"`); the struct-level PascalCase
    // rule would otherwise emit `"Nft"` and the venue would ignore the field.
    #[serde(rename = "NFT", skip_serializing_if = "Option::is_none")]
    pub nft: Option<Vec<String>>,
    // Bitget uses `"FiatP2P"` - PascalCase rename would emit `"FiatP2p"`.
    #[serde(rename = "FiatP2P", skip_serializing_if = "Option::is_none")]
    pub fiat_p2p: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fiat_bitget_pay: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fiat_bit_pay: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fiat_convert_broker: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bit_card: Option<Vec<String>>,
    // Bitget uses `"ByXPost"` - PascalCase rename would emit `"ByxPost"`.
    #[serde(rename = "ByXPost", skip_serializing_if = "Option::is_none")]
    pub byx_post: Option<Vec<String>>,
}

/// Body parameters for `POST /api/v3/user/update-sub-api`.
///
/// `api_key` is only required when a master key is editing a sub-account key;
/// it is omitted when a sub key edits itself.
#[derive(Debug, Clone, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(setter(into))]
pub struct BitgetUpdateSubApiParams {
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub api_key: Option<SecretString>,
    // Bitget accepts `readOnly` as a 0/1 integer on the wire; the builder takes
    // a `bool` and `opt_bool_as_int` serializes it to match.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none", with = "opt_bool_as_int")]
    #[builder(setter(strip_option))]
    pub read_only: Option<bool>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub ips: Option<String>,
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub permissions: Option<BitgetApiKeyPermissionUpdate>,
}

impl Zeroize for BitgetUpdateSubApiParams {
    fn zeroize(&mut self) {
        self.api_key.zeroize();
    }
}

/// Body parameters for the master API key update (no Bitget endpoint; kept for API parity).
///
/// Can only modify the caller's own master key.
///
/// The official request parameter table lists only `readOnly` and
/// `permissions`; `ips` appears in the response but is not a request field,
/// so it is deliberately excluded here.
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetUpdateMasterApiParams {
    #[serde(skip_serializing_if = "Option::is_none", with = "opt_bool_as_int")]
    #[builder(setter(strip_option))]
    pub read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub permissions: Option<BitgetApiKeyPermissionUpdate>,
}

/// Query parameters for cursor-paginated sub-account listings.
///
/// Shared by `GET /api/v3/user/sub-list` and `GET /api/v3/user/sub-list`,
/// which take the same pagination shape (`pageSize` up to 100 plus
/// `nextCursor`). Bitget documents `pageSize` as a string, but the URL encoder
/// serializes any numeric value as text anyway, so `u32` is used on the Rust
/// side for compile-time type safety.
#[derive(Clone, Debug, Deserialize, Serialize, Default, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(default)]
#[builder(setter(into))]
pub struct BitgetSubMembersPageParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub page_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub next_cursor: Option<String>,
}

/// Query parameters for `GET /api/v3/user/sub-api-list`.
///
/// `sub_member_id` is required, so this struct intentionally does not carry
/// `#[builder(default)]`; the generated builder returns `Err` when the field
/// is missing, matching the convention used by `BitgetBorrowParams`.
#[derive(Clone, Debug, Deserialize, Serialize, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(setter(into))]
pub struct BitgetSubApiKeysParams {
    pub sub_member_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub cursor: Option<String>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use zeroize::Zeroize;

    use super::*;

    #[rstest]
    fn test_update_sub_api_preserves_wire_value_and_redacts_debug() {
        let mut params = BitgetUpdateSubApiParamsBuilder::default()
            .api_key("sub-api-key-secret".to_string())
            .read_only(true)
            .build()
            .unwrap();

        let value = serde_json::to_value(&params).unwrap();
        let debug = format!("{params:?}");

        assert_eq!(value["apiKey"], "sub-api-key-secret");
        assert_eq!(value["readOnly"], 1);
        assert!(debug.contains(REDACTED));
        assert!(!debug.contains("sub-api-key-secret"));

        params.zeroize();
        assert!(params.api_key.is_none());
    }

    #[rstest]
    fn test_amend_entry_builds_with_only_order_id() {
        let entry = BitgetBatchAmendOrderEntryBuilder::default()
            .symbol("ETHUSDT".to_string())
            .order_id("12345".to_string())
            .price(Some("1600.0".to_string()))
            .build()
            .expect("amend entry should build with only order_id");

        assert_eq!(entry.order_id.as_deref(), Some("12345"));
        assert_eq!(entry.order_link_id, None);
    }

    #[rstest]
    fn test_amend_entry_builds_with_only_order_link_id() {
        let entry = BitgetBatchAmendOrderEntryBuilder::default()
            .symbol("ETHUSDT".to_string())
            .order_link_id("link-1".to_string())
            .qty(Some("0.02".to_string()))
            .build()
            .expect("amend entry should build with only order_link_id");

        assert_eq!(entry.order_link_id.as_deref(), Some("link-1"));
        assert_eq!(entry.order_id, None);
    }

    #[rstest]
    fn test_amend_entry_builds_with_no_identifiers() {
        let entry = BitgetBatchAmendOrderEntryBuilder::default()
            .symbol("ETHUSDT".to_string())
            .price(Some("1600.0".to_string()))
            .build()
            .expect("amend entry optional identifiers default to None");

        assert_eq!(entry.order_id, None);
        assert_eq!(entry.order_link_id, None);
    }

    #[rstest]
    fn test_cancel_entry_builds_with_only_order_id() {
        let entry = BitgetBatchCancelOrderEntryBuilder::default()
            .symbol("ETHUSDT".to_string())
            .order_id("12345".to_string())
            .build()
            .expect("cancel entry should build with only order_id");

        assert_eq!(entry.order_id.as_deref(), Some("12345"));
        assert_eq!(entry.order_link_id, None);
    }

    #[rstest]
    fn test_cancel_entry_builds_with_only_order_link_id() {
        let entry = BitgetBatchCancelOrderEntryBuilder::default()
            .symbol("ETHUSDT".to_string())
            .order_link_id("link-1".to_string())
            .build()
            .expect("cancel entry should build with only order_link_id");

        assert_eq!(entry.order_link_id.as_deref(), Some("link-1"));
        assert_eq!(entry.order_id, None);
    }
}
