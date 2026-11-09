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

//! Enumerations that model Bitget string/int enums across HTTP and WebSocket payloads.

use std::fmt::Display;

use jiff::{Timestamp, civil::Date, tz::Offset};
use nautilus_model::enums::{AggressorSide, OrderSide, TriggerType};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{AsRefStr, EnumIter, EnumString};

/// Unified account mode values (`accountMode` of `GET /api/v3/account/settings`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BitgetUnifiedMarginStatus {
    /// Unified trading account.
    Unified,
    /// Hybrid (classic + unified) account.
    Hybrid,
    /// Account upgrade to unified in progress.
    Upgrading,
    /// Account mode switch in progress.
    Switching,
    #[serde(other)]
    Other,
}

/// Account level used by Bitget when switching risk profiles (`POST /api/v3/account/adjust-account-mode`).
#[derive(
    Clone,
    Copy,
    Debug,
    strum::Display,
    Eq,
    PartialEq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.bitget",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetMarginMode {
    #[serde(rename = "isolated")]
    IsolatedMargin,
    #[serde(rename = "basic")]
    RegularMargin,
    #[serde(rename = "advanced")]
    PortfolioMargin,
}

/// Position (hold) mode as used by `holdMode` in the v3 API.
#[derive(
    Clone,
    Copy,
    Debug,
    strum::Display,
    Eq,
    PartialEq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.bitget",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetPositionMode {
    /// One-way (merged single) position mode.
    #[serde(rename = "one_way_mode")]
    MergedSingle,
    /// Dual-side hedged position mode.
    #[serde(rename = "hedge_mode")]
    BothSides,
}

/// Position index values used for hedge mode payloads.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    strum::Display,
    Eq,
    PartialEq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize_repr,
    Deserialize_repr,
)]
#[repr(i32)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.bitget",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetPositionIdx {
    /// One-way mode position identifier.
    #[default]
    OneWay = 0,
    /// Buy side of a hedge-mode position.
    BuyHedge = 1,
    /// Sell side of a hedge-mode position.
    SellHedge = 2,
}

impl BitgetPositionIdx {
    /// Returns the Bitget `posSide` for this index (`None` in one-way mode, where it is not sent).
    #[must_use]
    pub const fn as_pos_side(self) -> Option<BitgetPositionSide> {
        match self {
            Self::OneWay => None,
            Self::BuyHedge => Some(BitgetPositionSide::Buy),
            Self::SellHedge => Some(BitgetPositionSide::Sell),
        }
    }
}

/// Account type enumeration (Bitget UTA has a single unified wallet; kept for API parity).
#[derive(
    Copy,
    Clone,
    Debug,
    strum::Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "UPPERCASE")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.bitget",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetAccountType {
    Unified,
    Fund,
}

/// API key authentication type of a Bitget API key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum BitgetApiKeyType {
    /// HMAC-SHA256 signed keys (the default).
    Hmac = 1,
    /// RSA-signed keys.
    Rsa = 2,
}

/// Environments supported by the Bitget API stack.
#[derive(
    Copy,
    Clone,
    Debug,
    strum::Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.bitget",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetEnvironment {
    /// Live trading environment.
    Mainnet,
    /// Demo (paper trading) environment.
    Demo,
    /// Bitget has no testnet; resolves to the demo (paper trading) environment.
    Testnet,
}

/// Product categories supported by the v3 API (`category` / `instType`).
#[derive(
    Copy,
    Clone,
    Debug,
    strum::Display,
    Default,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.bitget",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetProductType {
    #[default]
    #[serde(rename = "SPOT", alias = "spot")]
    Spot,
    #[serde(rename = "USDT-FUTURES", alias = "usdt-futures", alias = "linear")]
    Linear,
    #[serde(rename = "COIN-FUTURES", alias = "coin-futures", alias = "inverse")]
    Inverse,
    /// Bitget lists no options; kept for API parity and rejected by every request.
    #[serde(rename = "OPTION", alias = "option")]
    Option,
}

/// Spot margin trading enablement states (no Bitget instrument field; kept for API parity).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetMarginTrading {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "utaOnly")]
    UtaOnly,
    #[serde(rename = "both")]
    Both,
    #[serde(rename = "normalSpotOnly")]
    NormalSpotOnly,
    #[serde(other)]
    Other,
}

/// Innovation market flag for spot instruments (no Bitget instrument field; kept for API parity).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetInnovationFlag {
    #[serde(rename = "0")]
    Standard,
    #[serde(rename = "1")]
    Innovation,
    #[serde(other)]
    Other,
}

/// Instrument lifecycle status values (`status` of `/api/v3/market/instruments`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetInstrumentStatus {
    /// Listed, not yet open.
    #[serde(rename = "listed")]
    PreLaunch,
    /// No Bitget value; kept for parity.
    #[serde(rename = "PendingOpen")]
    PendingOpen,
    #[serde(rename = "online")]
    Trading,
    /// Opening positions restricted.
    #[serde(rename = "limit_open")]
    LimitOpen,
    /// Closing positions restricted.
    #[serde(rename = "limit_close")]
    LimitClose,
    /// API trading restricted.
    #[serde(rename = "restrictedAPI")]
    RestrictedApi,
    /// No Bitget value (delivery is reported by `deliveryStatus`); kept for parity.
    #[serde(rename = "Delivering")]
    Delivering,
    /// Delisted or under maintenance.
    #[serde(rename = "offline")]
    Closed,
    #[serde(other)]
    #[default]
    Other,
}

impl BitgetProductType {
    /// Returns the canonical lowercase identifier used for labels and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "linear",
            Self::Inverse => "inverse",
            Self::Option => "option",
        }
    }

    /// Returns the uppercase suffix used in instrument identifiers (e.g. `-PERP`).
    #[must_use]
    pub const fn suffix(self) -> &'static str {
        match self {
            Self::Spot => "-SPOT",
            // `-PERP` keeps the `<SYMBOL>-PERP.BITGET` ids the bitget_nt2 engine binds to
            Self::Linear => "-PERP",
            Self::Inverse => "-INVERSE",
            Self::Option => "-OPTION",
        }
    }

    /// Returns the product type identified by the suffix in the symbol string.
    #[must_use]
    pub fn from_suffix(symbol: &str) -> Option<Self> {
        if symbol.ends_with("-SPOT") {
            Some(Self::Spot)
        } else if symbol.ends_with("-PERP") {
            Some(Self::Linear)
        } else if symbol.ends_with("-INVERSE") {
            Some(Self::Inverse)
        } else if symbol.ends_with("-OPTION") {
            Some(Self::Option)
        } else {
            None
        }
    }

    /// Returns the REST `category` value (e.g. `USDT-FUTURES`).
    #[must_use]
    pub const fn as_category(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::Linear => "USDT-FUTURES",
            Self::Inverse => "COIN-FUTURES",
            Self::Option => "OPTION",
        }
    }

    /// Returns the lowercase WebSocket `instType` value (e.g. `usdt-futures`).
    #[must_use]
    pub const fn as_ws_inst_type(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "usdt-futures",
            Self::Inverse => "coin-futures",
            Self::Option => "option",
        }
    }

    /// Parses a REST `category` or WebSocket `instType` value (case-insensitive).
    #[must_use]
    pub fn from_category(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("SPOT") {
            Some(Self::Spot)
        } else if value.eq_ignore_ascii_case("USDT-FUTURES") {
            Some(Self::Linear)
        } else if value.eq_ignore_ascii_case("COIN-FUTURES") {
            Some(Self::Inverse)
        } else {
            None
        }
    }

    /// Returns `true` if the product is a spot instrument.
    #[must_use]
    pub fn is_spot(self) -> bool {
        matches!(self, Self::Spot)
    }

    /// Returns `true` if the product is a linear contract.
    #[must_use]
    pub fn is_linear(self) -> bool {
        matches!(self, Self::Linear)
    }

    /// Returns `true` if the product is an inverse contract.
    #[must_use]
    pub fn is_inverse(self) -> bool {
        matches!(self, Self::Inverse)
    }

    /// Returns `true` if the product is an option contract.
    #[must_use]
    pub fn is_option(self) -> bool {
        matches!(self, Self::Option)
    }
}

/// Contract type enumeration for linear and inverse derivatives.
///
/// Bitget has no `contractType` field; it is derived from `category` and `type`
/// (`perpetual`/`delivery`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum BitgetContractType {
    LinearPerpetual,
    LinearFutures,
    InversePerpetual,
    InverseFutures,
}

/// Option flavor values (Bitget lists no options; kept for API parity).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum BitgetOptionType {
    Call,
    Put,
}

/// Instrument type values (`instrumentType` of `/api/v3/market/instruments`).
///
/// New values may be added by the venue; unknown strings fall back to `Other` so deserialization
/// remains forward-compatible. `Innovation`, `Adventure`, `Xstocks` and `Mstocks` have no Bitget
/// value and are kept for API parity.
///
/// # References
/// - <https://www.bitget.com/docs/uta/enum>
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BitgetSymbolType {
    /// No Bitget value.
    Innovation,
    /// No Bitget value.
    Adventure,
    /// No Bitget value (Bitget spot rTokens are flagged by `isReality`).
    Xstocks,
    /// Commodity instruments.
    Commodity,
    /// Tokenized stock derivatives.
    Stock,
    /// Foreign exchange instruments.
    #[serde(rename = "fx")]
    Forex,
    /// Exchange-traded fund derivatives.
    #[serde(rename = "etf")]
    Etf,
    /// No Bitget value.
    Mstocks,
    /// Crypto instruments.
    Crypto,
    /// Precious metal instruments.
    Metal,
    /// Energy instruments.
    Energy,
    /// Index instruments.
    Index,
    /// Forward-compatible fallback for any value the venue adds later.
    #[serde(other)]
    #[default]
    Other,
}

impl BitgetSymbolType {
    /// Returns the exact recognized value used by Bitget, or `None` for an unknown value.
    #[must_use]
    pub(crate) const fn as_str(self) -> Option<&'static str> {
        match self {
            Self::Innovation => Some("innovation"),
            Self::Adventure => Some("adventure"),
            Self::Xstocks => Some("xstocks"),
            Self::Commodity => Some("commodity"),
            Self::Stock => Some("stock"),
            Self::Forex => Some("fx"),
            Self::Etf => Some("etf"),
            Self::Mstocks => Some("mstocks"),
            Self::Crypto => Some("crypto"),
            Self::Metal => Some("metal"),
            Self::Energy => Some("energy"),
            Self::Index => Some("index"),
            Self::Other => None,
        }
    }
}

/// Position side as represented in REST/WebSocket payloads (`posSide` / `holdSide`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetPositionSide {
    #[serde(rename = "")]
    #[default]
    Flat,
    #[serde(rename = "long")]
    Buy,
    #[serde(rename = "short")]
    Sell,
}

/// WebSocket trade request topics (`op:"trade"`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetWsOrderRequestOp {
    #[serde(rename = "place-order")]
    Create,
    #[serde(rename = "modify-order")]
    Amend,
    #[serde(rename = "cancel-order")]
    Cancel,
    #[serde(rename = "batch-place")]
    CreateBatch,
    #[serde(rename = "batch-modify")]
    AmendBatch,
    #[serde(rename = "batch-cancel")]
    CancelBatch,
}

/// Available kline intervals.
///
/// Bitget v3 documents `1m,3m,5m,15m,30m,1H,4H,6H,12H,1D`; `2H`, `1W` and `1M` are kept for
/// API parity and are rejected by the venue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetKlineInterval {
    #[serde(rename = "1m")]
    Minute1,
    #[serde(rename = "3m")]
    Minute3,
    #[serde(rename = "5m")]
    Minute5,
    #[serde(rename = "15m")]
    Minute15,
    #[serde(rename = "30m")]
    Minute30,
    #[serde(rename = "1H")]
    Hour1,
    #[serde(rename = "2H")]
    Hour2,
    #[serde(rename = "4H")]
    Hour4,
    #[serde(rename = "6H")]
    Hour6,
    #[serde(rename = "12H")]
    Hour12,
    #[serde(rename = "1D")]
    Day1,
    #[serde(rename = "1W")]
    Week1,
    #[serde(rename = "1M")]
    Month1,
}

impl BitgetKlineInterval {
    /// Returns the end time in milliseconds for a bar that starts at `start_ms`.
    ///
    /// For most intervals this is simply `start_ms + duration`. For monthly bars,
    /// this calculates the actual first millisecond of the next month to handle
    /// variable month lengths (28-31 days).
    #[must_use]
    pub fn bar_end_time_ms(&self, start_ms: i64) -> i64 {
        match self {
            Self::Month1 => {
                let start_dt = Offset::UTC.to_datetime(
                    Timestamp::from_millisecond(start_ms).unwrap_or(Timestamp::UNIX_EPOCH),
                );
                let (year, month) = if start_dt.month() == 12 {
                    (start_dt.year() + 1, 1)
                } else {
                    (start_dt.year(), start_dt.month() + 1)
                };
                Date::new(year, month, 1)
                    .and_then(|date| Offset::UTC.to_timestamp(date.at(0, 0, 0, 0)))
                    .map_or(start_ms + 2_678_400_000, Timestamp::as_millisecond)
            }
            _ => start_ms + self.duration_ms(),
        }
    }

    /// Returns the fixed duration of this interval in milliseconds.
    ///
    /// Note: For monthly bars, use [`Self::bar_end_time_ms`] instead as months have
    /// variable lengths (28-31 days).
    #[must_use]
    pub const fn duration_ms(&self) -> i64 {
        match self {
            Self::Minute1 => 60_000,
            Self::Minute3 => 180_000,
            Self::Minute5 => 300_000,
            Self::Minute15 => 900_000,
            Self::Minute30 => 1_800_000,
            Self::Hour1 => 3_600_000,
            Self::Hour2 => 7_200_000,
            Self::Hour4 => 14_400_000,
            Self::Hour6 => 21_600_000,
            Self::Hour12 => 43_200_000,
            Self::Day1 => 86_400_000,
            Self::Week1 => 604_800_000,
            Self::Month1 => 2_678_400_000, // 31 days - use bar_end_time_ms() for accurate calculation
        }
    }
}

impl Display for BitgetKlineInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Minute1 => "1m",
            Self::Minute3 => "3m",
            Self::Minute5 => "5m",
            Self::Minute15 => "15m",
            Self::Minute30 => "30m",
            Self::Hour1 => "1H",
            Self::Hour2 => "2H",
            Self::Hour4 => "4H",
            Self::Hour6 => "6H",
            Self::Hour12 => "12H",
            Self::Day1 => "1D",
            Self::Week1 => "1W",
            Self::Month1 => "1M",
        };
        write!(f, "{s}")
    }
}

/// Order status values returned by Bitget.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetOrderStatus {
    /// Order created (REST `live`).
    #[serde(rename = "live")]
    #[default]
    Created,
    #[serde(rename = "new")]
    New,
    /// Strategy (conditional) order failed to trigger or place.
    #[serde(rename = "failed")]
    Rejected,
    #[serde(rename = "partially_filled")]
    PartiallyFilled,
    /// No Bitget value: a partially filled cancel arrives as `cancelled` with `cumExecQty > 0`.
    #[serde(rename = "PartiallyFilledCanceled")]
    PartiallyFilledCanceled,
    #[serde(rename = "filled")]
    Filled,
    #[serde(rename = "cancelled", alias = "canceled")]
    Canceled,
    /// Strategy (conditional) order waiting for its trigger.
    #[serde(rename = "pending", alias = "submitting")]
    Untriggered,
    /// Strategy (conditional) order triggered.
    #[serde(rename = "success")]
    Triggered,
    /// No Bitget value; kept for parity.
    #[serde(rename = "Deactivated")]
    Deactivated,
}

/// Order side enumeration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetOrderSide {
    #[serde(rename = "")]
    #[default]
    Unknown,
    #[serde(rename = "buy")]
    Buy,
    #[serde(rename = "sell")]
    Sell,
}

impl From<BitgetOrderSide> for AggressorSide {
    fn from(value: BitgetOrderSide) -> Self {
        match value {
            BitgetOrderSide::Buy => Self::Buy,
            BitgetOrderSide::Sell => Self::Sell,
            BitgetOrderSide::Unknown => Self::NoAggressor,
        }
    }
}

impl From<BitgetOrderSide> for Option<OrderSide> {
    fn from(value: BitgetOrderSide) -> Self {
        match value {
            BitgetOrderSide::Buy => Some(OrderSide::Buy),
            BitgetOrderSide::Sell => Some(OrderSide::Sell),
            BitgetOrderSide::Unknown => None,
        }
    }
}

impl TryFrom<BitgetOrderSide> for OrderSide {
    type Error = anyhow::Error;

    fn try_from(value: BitgetOrderSide) -> Result<Self, Self::Error> {
        match value {
            BitgetOrderSide::Buy => Ok(Self::Buy),
            BitgetOrderSide::Sell => Ok(Self::Sell),
            BitgetOrderSide::Unknown => anyhow::bail!("Unspecified Bitget order side"),
        }
    }
}

impl From<OrderSide> for BitgetOrderSide {
    fn from(value: OrderSide) -> Self {
        match value {
            OrderSide::Buy => Self::Buy,
            OrderSide::Sell => Self::Sell,
        }
    }
}

impl From<BitgetTriggerType> for TriggerType {
    fn from(value: BitgetTriggerType) -> Self {
        match value {
            BitgetTriggerType::None => Self::Default,
            BitgetTriggerType::LastPrice => Self::LastPrice,
            BitgetTriggerType::IndexPrice => Self::IndexPrice,
            BitgetTriggerType::MarkPrice => Self::MarkPrice,
        }
    }
}

impl From<TriggerType> for BitgetTriggerType {
    fn from(value: TriggerType) -> Self {
        match value {
            TriggerType::Default | TriggerType::LastPrice => Self::LastPrice,
            TriggerType::IndexPrice => Self::IndexPrice,
            TriggerType::MarkPrice => Self::MarkPrice,
            _ => Self::LastPrice,
        }
    }
}

/// Resolves an optional Nautilus trigger type to a Bitget trigger type,
/// defaulting to `LastPrice` when absent.
pub fn resolve_trigger_type(trigger_type: Option<TriggerType>) -> BitgetTriggerType {
    trigger_type.map_or(BitgetTriggerType::LastPrice, BitgetTriggerType::from)
}

/// Order cancel reason values as returned by Bitget (`cancelReason`).
///
/// Variants without an explicit rename have no Bitget value and are kept for API parity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetCancelType {
    #[serde(rename = "normal_cancel")]
    CancelByUser,
    /// Post-only order would have taken liquidity.
    #[serde(rename = "post_only_fill_cancel")]
    CancelByPostOnly,
    #[serde(rename = "ioc_not_full_cancel")]
    CancelByIocNotFull,
    #[serde(rename = "fok_not_full_cancel")]
    CancelByFokNotFull,
    #[serde(rename = "trade_too_many_times_cancel")]
    CancelByTradeTooManyTimes,
    #[serde(rename = "trade_count_too_small")]
    CancelByTradeCountTooSmall,
    #[serde(rename = "trade_max_deal_size_cancel")]
    CancelByMaxDealSize,
    #[serde(rename = "back_contract_cancel")]
    CancelByBackContract,
    #[serde(rename = "trader_order_cancel")]
    CancelByTrader,
    #[serde(rename = "slippage_cancel")]
    CancelBySlippage,
    #[serde(rename = "penetrate_cancel")]
    CancelByPenetrate,
    #[serde(rename = "call_auction_cancel")]
    CancelByCallAuction,
    #[serde(rename = "limit_price_exceed_cancel")]
    CancelByLimitPriceExceed,
    #[serde(rename = "cancel_receiveWindow")]
    CancelByReceiveWindow,
    CancelByReduceOnly,
    CancelByPrepareLiq,
    #[serde(rename = "burst_cancel")]
    CancelAllBeforeLiq,
    #[serde(rename = "adl_cancel")]
    CancelByPrepareAdl,
    CancelAllBeforeAdl,
    CancelByPrepareLackOfMargin,
    CancelByPrepareOrderFilter,
    CancelByPrepareOrderMarginCheckFailed,
    CancelByPrepareOrderCommission,
    CancelByPrepareOrderRms,
    CancelByPrepareOrderOther,
    #[serde(rename = "delegated_risk_order_cancel")]
    CancelByRiskLimit,
    CancelOnDisconnect,
    CancelByStopOrdersExceeded,
    CancelByPzMarketClose,
    CancelByMarginCheckFailed,
    CancelByPzTakeover,
    CancelByAdmin,
    CancelBySettle,
    #[serde(rename = "tpsl_plan_cancel")]
    CancelByTpSlTsClear,
    #[serde(rename = "modify_limit_order")]
    CancelByAmendNotModified,
    CancelByPzCancel,
    #[serde(rename = "self_trade_cancel", alias = "stp_cancel")]
    CancelBySmp,
    #[serde(rename = "CancelByDCP")]
    CancelByDcp,
    CancelByRebalance,
    #[serde(rename = "CancelByOCOTpCanceledBySlTriggered")]
    CancelByOcoTpCanceledBySlTriggered,
    #[serde(rename = "CancelByOCOSlCanceledByTpTriggered")]
    CancelByOcoSlCanceledByTpTriggered,
    CancelByCannotAffordOrderCost,
    CancelByPmTrialMmOverEquity,
    CancelByAccountBlocking,
    #[serde(rename = "delivery_order_cancel")]
    CancelByDelivery,
    CancelByMmpTriggered,
    CancelByCrossSelfMuch,
    CancelByCrossReachMaxTradeNum,
    CancelByCrossSelfMatch,
    CancelBySelfMatchPrevention,
    #[serde(other)]
    #[default]
    Other,
}

/// Order creation origin values (no Bitget field; `delegateType` is the closest; kept for API parity).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum BitgetCreateType {
    CreateByUser,
    CreateByFutureSpread,
    CreateByAdminClosing,
    CreateBySettle,
    CreateByClosing,
    CreateByTakeProfit,
    CreateByStopLoss,
    CreateByTrailingStop,
    CreateByTrailingProfit,
    CreateByStopOrder,
    CreateByPartialTakeProfit,
    CreateByPartialStopLoss,
    CreateByAdl,
    #[serde(rename = "CreateByAdl_PassThrough")]
    CreateByAdlPassThrough,
    CreateByLiq,
    CreateByLiquidate,
    CreateByTakeover,
    #[serde(rename = "CreateByTakeOver_PassThrough")]
    CreateByTakeOverPassThrough,
    #[serde(rename = "CreateByBlock_PassThrough")]
    CreateByBlockPassThrough,
    #[serde(rename = "CreateByBlockTradeMovePosition_PassThrough")]
    CreateByBlockTradeMovePositionPassThrough,
    CreateByChaseOrder,
    #[serde(rename = "CreateByFGridBot")]
    CreateByFGridBot,
    #[serde(rename = "CloseByFGridBot")]
    CloseByFGridBot,
    #[serde(rename = "CreateByTWAP")]
    CreateByTwap,
    CreateByMartingaleBot,
    CloseByMartingaleBot,
    #[serde(rename = "CreateByIceBerg")]
    CreateByIceBerg,
    CreateByBboOrder,
    #[serde(rename = "CreateByTVSignal")]
    CreateByTvSignal,
    CreateByMmRateClose,
    CreateByArbitrage,
    CreateByDdh,
    CreateByTpsl,
    #[serde(other)]
    Other,
}

/// BBO side type values for Bitget order placement (`bboLevel` prefix: `queue`/`counterparty`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BitgetBboSideType {
    Queue,
    Counterparty,
}

/// Venue order type enumeration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetOrderType {
    #[serde(rename = "market")]
    Market,
    #[serde(rename = "limit")]
    Limit,
    #[serde(rename = "UNKNOWN")]
    #[default]
    Unknown,
}

/// Stop (strategy) order type classification (`type` of the strategy-order endpoints).
///
/// `TakeProfit`, `StopLoss` and the partial variants are all carried by Bitget `tpsl`
/// orders; variants without a lowercase value have no Bitget value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetStopOrderType {
    #[serde(rename = "")]
    #[default]
    None,
    #[serde(rename = "UNKNOWN")]
    Unknown,
    #[serde(rename = "TakeProfit")]
    TakeProfit,
    #[serde(rename = "StopLoss")]
    StopLoss,
    #[serde(rename = "trailing_stop")]
    TrailingStop,
    #[serde(rename = "trigger")]
    Stop,
    #[serde(rename = "PartialTakeProfit")]
    PartialTakeProfit,
    #[serde(rename = "PartialStopLoss")]
    PartialStopLoss,
    #[serde(rename = "tpsl")]
    TpslOrder,
    #[serde(rename = "oco")]
    OcoOrder,
    #[serde(rename = "MmRateClose")]
    MmRateClose,
    #[serde(rename = "BidirectionalTpslOrder")]
    BidirectionalTpslOrder,
}

/// Trigger type configuration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetTriggerType {
    #[serde(rename = "")]
    #[default]
    None,
    #[serde(rename = "market")]
    LastPrice,
    /// Bitget triggers only on `market` or `mark`; rejected by the venue.
    #[serde(rename = "index")]
    IndexPrice,
    #[serde(rename = "mark")]
    MarkPrice,
}

/// Trigger direction (Bitget infers it from the trigger price; never sent, kept for API parity).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetTriggerDirection {
    #[default]
    None = 0,
    RisesTo = 1,
    FallsTo = 2,
}

/// Take-profit/stop-loss mode for derivatives orders.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetTpSlMode {
    Full,
    Partial,
    #[serde(other)]
    #[default]
    Unknown,
}

/// Time-in-force enumeration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetTimeInForce {
    #[serde(rename = "gtc")]
    #[default]
    Gtc,
    #[serde(rename = "ioc")]
    Ioc,
    #[serde(rename = "fok")]
    Fok,
    #[serde(rename = "post_only")]
    PostOnly,
    #[serde(rename = "rpi")]
    Rpi,
}

/// Execution type values used in execution reports.
///
/// Bitget reports `execType` on orders (`normal`, `offset`, `reduce`, `liquidation`,
/// `delivery`) and encodes venue-generated fills in `tradeSide` (see [`Self::from_trade_side`]).
/// Variants without a lowercase value have no Bitget value and are kept for API parity.
///
/// Reference: <https://www.bitget.com/docs/uta/enum>.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetExecType {
    #[serde(rename = "normal", alias = "offset", alias = "reduce")]
    Trade,
    #[serde(rename = "adl")]
    AdlTrade,
    #[serde(rename = "Funding")]
    Funding,
    #[serde(rename = "liquidation")]
    BustTrade,
    #[serde(rename = "delivery")]
    Delivery,
    #[serde(rename = "Settle")]
    Settle,
    #[serde(rename = "BlockTrade")]
    BlockTrade,
    #[serde(rename = "MovePosition")]
    MovePosition,
    #[serde(rename = "FutureSpread")]
    FutureSpread,
    /// Retained for execution history recorded while the venue issued this type
    /// (2026-07-23 to 2026-08-20), before its replacement by the split settlement
    /// and dividend types.
    #[serde(rename = "CorporateAction")]
    CorporateAction,
    #[serde(rename = "ForwardSplitSettle")]
    ForwardSplitSettle,
    #[serde(rename = "ReverseSplitSettle")]
    ReverseSplitSettle,
    #[serde(rename = "Dividend")]
    Dividend,
    #[serde(other)]
    #[serde(rename = "UNKNOWN")]
    #[default]
    Unknown,
}

impl BitgetExecType {
    /// Derives the execution type from a Bitget `tradeSide` value.
    ///
    /// Liquidation (`burst_*`), auto-deleveraging (`dte_sys_adl_*`), delivery (`delivery_*`) and
    /// corporate-action rebases (`dte_rebase_*`) are venue generated; everything else is a trade.
    #[must_use]
    pub fn from_trade_side(trade_side: &str) -> Self {
        if trade_side.starts_with("burst_") {
            Self::BustTrade
        } else if trade_side.starts_with("dte_sys_adl_") {
            Self::AdlTrade
        } else if trade_side.starts_with("delivery_") {
            Self::Delivery
        } else if trade_side.starts_with("dte_rebase_") {
            Self::CorporateAction
        } else {
            Self::Trade
        }
    }

    /// Returns `true` if this execution was generated by the venue rather than the user.
    ///
    /// This covers auto-deleveraging (`AdlTrade`), liquidation takeovers (`BustTrade`),
    /// scheduled deliveries (`Delivery`), settlement (`Settle`), and corporate actions
    /// (`CorporateAction`, `ForwardSplitSettle`, `ReverseSplitSettle`, `Dividend`).
    #[must_use]
    pub const fn is_exchange_generated(&self) -> bool {
        matches!(
            self,
            Self::AdlTrade
                | Self::BustTrade
                | Self::Delivery
                | Self::Settle
                | Self::CorporateAction
                | Self::ForwardSplitSettle
                | Self::ReverseSplitSettle
                | Self::Dividend
        )
    }
}

/// Transaction types for wallet funding records (`type` of `/api/v3/account/financial-records`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetTransactionType {
    #[serde(rename = "TRANSFER_IN")]
    TransferIn,
    #[serde(rename = "TRANSFER_OUT")]
    TransferOut,
    #[serde(
        rename = "TRADE",
        alias = "OPEN_LONG",
        alias = "OPEN_SHORT",
        alias = "CLOSE_LONG",
        alias = "CLOSE_SHORT",
        alias = "BUY_DEAL",
        alias = "SELL_DEAL"
    )]
    Trade,
    /// Funding settlement.
    #[serde(
        rename = "SETTLEMENT",
        alias = "CONTRACT_MAIN_SETTLE_FEE_USER_IN",
        alias = "CONTRACT_MAIN_SETTLE_FEE_USER_OUT"
    )]
    Settlement,
    #[serde(rename = "DELIVERY")]
    Delivery,
    #[serde(rename = "LIQUIDATION")]
    Liquidation,
    #[serde(rename = "AIRDRP")]
    Airdrop,
    #[serde(rename = "ADL")]
    Adl,
    #[serde(rename = "DIVIDEND_SETTLEMENT")]
    DividendSettlement,
    #[serde(rename = "FORWARD_SPLIT_SETTLE")]
    ForwardSplitSettle,
    #[serde(rename = "REVERSE_SPLIT_SETTLE")]
    ReverseSplitSettle,
    #[serde(other)]
    Other,
}

/// Endpoint classifications used by the Bitget API.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum BitgetEndpointType {
    None,
    Asset,
    Market,
    Account,
    Trade,
    Position,
    User,
}

/// Filter for open orders query.
///
/// Selects `GET /api/v3/trade/unfilled-orders` (open) or `GET /api/v3/trade/history-orders`
/// (recent closed); never sent to the venue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetOpenOnly {
    /// Query open status orders only (New, PartiallyFilled).
    #[default]
    OpenOnly = 0,
    /// Query up to 500 recent closed orders (cancelled, rejected, filled).
    ClosedRecent = 1,
}

/// Order filter for querying specific order types.
///
/// `Order` selects the plain order endpoints, every other value the strategy-order endpoints
/// (`unfilled-strategy-orders` / `history-strategy-orders`); never sent to the venue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", eq, eq_int, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetOrderFilter {
    /// Active orders (default).
    #[default]
    Order,
    /// Conditional orders (futures and spot).
    StopOrder,
    /// Spot take-profit/stop-loss orders.
    #[serde(rename = "tpslOrder")]
    TpslOrder,
    /// Spot one-cancels-other orders.
    OcoOrder,
    /// Spot bidirectional TP/SL orders.
    BidirectionalTpslOrder,
}

/// Margin actions for spot margin trading operations.
#[derive(
    Clone,
    Copy,
    Debug,
    strum::Display,
    Eq,
    PartialEq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        hash,
        frozen,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.adapters.bitget",
        from_py_object,
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetMarginAction {
    /// Borrow funds for margin trading.
    Borrow,
    /// Repay borrowed funds.
    Repay,
    /// Query current borrowed amount.
    GetBorrowAmount,
}

/// Result status of repayment requests (Bitget returns no status; kept for API parity).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, strum::Display, Serialize, Deserialize)]
pub enum BitgetRepayStatus {
    /// The repayment is processing.
    #[serde(rename = "P")]
    #[strum(serialize = "P")]
    Processing,
    /// The repayment succeeded.
    #[serde(rename = "SU")]
    #[strum(serialize = "SU")]
    Success,
    /// The repayment failed.
    #[serde(rename = "FA")]
    #[strum(serialize = "FA")]
    Failed,
}

/// Position status enumeration (`positionStatus`; variants without a lowercase value have no
/// Bitget value).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum BitgetPositionStatus {
    #[serde(rename = "normal", alias = "opening")]
    #[default]
    Normal,
    /// Position closed (WebSocket `position` topic).
    #[serde(rename = "ended")]
    Ended,
    Liq,
    Adl,
    Settle,
    Delivering,
    #[serde(other)]
    Other,
}

/// Market unit for spot market orders (Bitget market buys are always quote-sized; never sent).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BitgetMarketUnit {
    #[serde(rename = "baseCoin")]
    BaseCoin,
    #[serde(rename = "quoteCoin")]
    QuoteCoin,
}

/// Self-match prevention type reported on an order or execution.
///
/// `Other` is a catch-all for values Bitget adds after this enum was written. Requests use
/// [`BitgetOrderSmpType`], which has no catch-all, so an unknown value cannot reach the venue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BitgetSmpType {
    #[default]
    None,
    CancelMaker,
    CancelTaker,
    CancelBoth,
    #[serde(other)]
    Other,
}

/// Self-match prevention type accepted on an order request.
///
/// Mirrors [`BitgetSmpType`] without its inbound catch-all, so every value this type can hold is one
/// Bitget accepts on an order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, AsRefStr, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BitgetOrderSmpType {
    None,
    CancelMaker,
    CancelTaker,
    CancelBoth,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::minute1(BitgetKlineInterval::Minute1, 60_000)]
    #[case::minute3(BitgetKlineInterval::Minute3, 180_000)]
    #[case::minute5(BitgetKlineInterval::Minute5, 300_000)]
    #[case::minute15(BitgetKlineInterval::Minute15, 900_000)]
    #[case::minute30(BitgetKlineInterval::Minute30, 1_800_000)]
    #[case::hour1(BitgetKlineInterval::Hour1, 3_600_000)]
    #[case::hour2(BitgetKlineInterval::Hour2, 7_200_000)]
    #[case::hour4(BitgetKlineInterval::Hour4, 14_400_000)]
    #[case::hour6(BitgetKlineInterval::Hour6, 21_600_000)]
    #[case::hour12(BitgetKlineInterval::Hour12, 43_200_000)]
    #[case::day1(BitgetKlineInterval::Day1, 86_400_000)]
    #[case::week1(BitgetKlineInterval::Week1, 604_800_000)]
    #[case::month1(BitgetKlineInterval::Month1, 2_678_400_000)]
    fn test_kline_interval_duration_ms(
        #[case] interval: BitgetKlineInterval,
        #[case] expected_ms: i64,
    ) {
        assert_eq!(interval.duration_ms(), expected_ms);
    }

    #[rstest]
    fn test_bar_end_time_ms_non_monthly_adds_duration() {
        let interval = BitgetKlineInterval::Minute1;
        let start_ms = 1704067200000i64;
        assert_eq!(interval.bar_end_time_ms(start_ms), start_ms + 60_000);
    }

    #[rstest]
    #[case::jan_31_days(1704067200000i64, 1706745600000i64)]
    #[case::feb_leap_year_29_days(1706745600000i64, 1709251200000i64)]
    #[case::apr_30_days(1711929600000i64, 1714521600000i64)]
    #[case::dec_to_next_year(1733011200000i64, 1735689600000i64)]
    fn test_bar_end_time_ms_monthly_variable_lengths(
        #[case] start_ms: i64,
        #[case] expected_end_ms: i64,
    ) {
        let interval = BitgetKlineInterval::Month1;
        assert_eq!(interval.bar_end_time_ms(start_ms), expected_end_ms);
    }

    #[rstest]
    #[case(BitgetSymbolType::Innovation, "innovation")]
    #[case(BitgetSymbolType::Adventure, "adventure")]
    #[case(BitgetSymbolType::Xstocks, "xstocks")]
    #[case(BitgetSymbolType::Commodity, "commodity")]
    #[case(BitgetSymbolType::Stock, "stock")]
    #[case(BitgetSymbolType::Forex, "fx")]
    #[case(BitgetSymbolType::Etf, "etf")]
    #[case(BitgetSymbolType::Mstocks, "mstocks")]
    #[case(BitgetSymbolType::Crypto, "crypto")]
    #[case(BitgetSymbolType::Metal, "metal")]
    #[case(BitgetSymbolType::Energy, "energy")]
    #[case(BitgetSymbolType::Index, "index")]
    fn test_symbol_type_round_trip(
        #[case] symbol_type: BitgetSymbolType,
        #[case] wire_value: &str,
    ) {
        let value = serde_json::Value::String(wire_value.to_string());

        assert_eq!(
            serde_json::from_value::<BitgetSymbolType>(value.clone()).unwrap(),
            symbol_type
        );
        assert_eq!(serde_json::to_value(symbol_type).unwrap(), value);
        assert_eq!(symbol_type.as_str(), Some(wire_value));
    }

    #[rstest]
    fn test_unknown_symbol_type_has_no_wire_value() {
        assert_eq!(BitgetSymbolType::Other.as_str(), None);
    }

    #[rstest]
    #[case(BitgetExecType::Trade, false)]
    #[case(BitgetExecType::AdlTrade, true)]
    #[case(BitgetExecType::BustTrade, true)]
    #[case(BitgetExecType::Delivery, true)]
    #[case(BitgetExecType::Settle, true)]
    #[case(BitgetExecType::Funding, false)]
    #[case(BitgetExecType::BlockTrade, false)]
    #[case(BitgetExecType::MovePosition, false)]
    #[case(BitgetExecType::FutureSpread, false)]
    #[case(BitgetExecType::CorporateAction, true)]
    #[case(BitgetExecType::ForwardSplitSettle, true)]
    #[case(BitgetExecType::ReverseSplitSettle, true)]
    #[case(BitgetExecType::Dividend, true)]
    #[case(BitgetExecType::Unknown, false)]
    fn test_exec_type_is_exchange_generated(
        #[case] exec_type: BitgetExecType,
        #[case] expected: bool,
    ) {
        assert_eq!(exec_type.is_exchange_generated(), expected);
    }

    #[rstest]
    #[case(BitgetExecType::Trade, "normal")]
    #[case(BitgetExecType::AdlTrade, "adl")]
    #[case(BitgetExecType::Funding, "Funding")]
    #[case(BitgetExecType::BustTrade, "liquidation")]
    #[case(BitgetExecType::Delivery, "delivery")]
    #[case(BitgetExecType::Settle, "Settle")]
    #[case(BitgetExecType::BlockTrade, "BlockTrade")]
    #[case(BitgetExecType::MovePosition, "MovePosition")]
    #[case(BitgetExecType::FutureSpread, "FutureSpread")]
    #[case(BitgetExecType::CorporateAction, "CorporateAction")]
    #[case(BitgetExecType::ForwardSplitSettle, "ForwardSplitSettle")]
    #[case(BitgetExecType::ReverseSplitSettle, "ReverseSplitSettle")]
    #[case(BitgetExecType::Dividend, "Dividend")]
    #[case(BitgetExecType::Unknown, "UNKNOWN")]
    fn test_exec_type_round_trip(#[case] exec_type: BitgetExecType, #[case] wire_value: &str) {
        let value = serde_json::Value::String(wire_value.to_string());

        assert_eq!(
            serde_json::from_value::<BitgetExecType>(value.clone()).unwrap(),
            exec_type
        );
        assert_eq!(serde_json::to_value(exec_type).unwrap(), value);
    }

    #[rstest]
    fn test_exec_type_unrecognized_value_deserializes_to_unknown() {
        let value = serde_json::Value::String("StockMerger".to_string());

        assert_eq!(
            serde_json::from_value::<BitgetExecType>(value).unwrap(),
            BitgetExecType::Unknown
        );
    }

    #[rstest]
    #[case("open", BitgetExecType::Trade)]
    #[case("sell_single", BitgetExecType::Trade)]
    #[case("burst_buy_single", BitgetExecType::BustTrade)]
    #[case("dte_sys_adl_sell_in_single_side_mode", BitgetExecType::AdlTrade)]
    #[case("delivery_close_long", BitgetExecType::Delivery)]
    #[case("dte_rebase_buy_in_single_side_mode", BitgetExecType::CorporateAction)]
    fn test_exec_type_from_trade_side(#[case] trade_side: &str, #[case] expected: BitgetExecType) {
        assert_eq!(BitgetExecType::from_trade_side(trade_side), expected);
    }

    #[rstest]
    #[case(BitgetAccountType::Unified, "UNIFIED")]
    #[case(BitgetAccountType::Fund, "FUND")]
    fn test_account_type_round_trip(
        #[case] account_type: BitgetAccountType,
        #[case] wire_value: &str,
    ) {
        let value = serde_json::Value::String(wire_value.to_string());

        assert_eq!(
            serde_json::from_value::<BitgetAccountType>(value.clone()).unwrap(),
            account_type
        );
        assert_eq!(serde_json::to_value(account_type).unwrap(), value);
    }

    #[rstest]
    #[case(BitgetTimeInForce::Gtc, "gtc")]
    #[case(BitgetTimeInForce::Ioc, "ioc")]
    #[case(BitgetTimeInForce::Fok, "fok")]
    #[case(BitgetTimeInForce::PostOnly, "post_only")]
    #[case(BitgetTimeInForce::Rpi, "rpi")]
    fn test_time_in_force_round_trip(#[case] tif: BitgetTimeInForce, #[case] wire_value: &str) {
        let value = serde_json::Value::String(wire_value.to_string());

        assert_eq!(
            serde_json::from_value::<BitgetTimeInForce>(value.clone()).unwrap(),
            tif
        );
        assert_eq!(serde_json::to_value(tif).unwrap(), value);
    }

    #[rstest]
    #[case("CreateByAdl_PassThrough", BitgetCreateType::CreateByAdlPassThrough)]
    #[case("CreateByLiq", BitgetCreateType::CreateByLiq)]
    #[case(
        "CreateByTakeOver_PassThrough",
        BitgetCreateType::CreateByTakeOverPassThrough
    )]
    #[case("CreateByTWAP", BitgetCreateType::CreateByTwap)]
    #[case("CreateByFutureSpread", BitgetCreateType::CreateByFutureSpread)]
    fn test_create_type_venue_wire_values(
        #[case] wire_value: &str,
        #[case] expected: BitgetCreateType,
    ) {
        let value = serde_json::Value::String(wire_value.to_string());

        assert_eq!(
            serde_json::from_value::<BitgetCreateType>(value.clone()).unwrap(),
            expected
        );
        assert_eq!(serde_json::to_value(expected).unwrap(), value);
    }

    #[rstest]
    #[case("normal_cancel", BitgetCancelType::CancelByUser)]
    #[case("post_only_fill_cancel", BitgetCancelType::CancelByPostOnly)]
    #[case("ioc_not_full_cancel", BitgetCancelType::CancelByIocNotFull)]
    #[case("self_trade_cancel", BitgetCancelType::CancelBySmp)]
    #[case("burst_cancel", BitgetCancelType::CancelAllBeforeLiq)]
    #[case("cancel_receiveWindow", BitgetCancelType::CancelByReceiveWindow)]
    fn test_cancel_type_venue_wire_values(
        #[case] wire_value: &str,
        #[case] expected: BitgetCancelType,
    ) {
        let value = serde_json::Value::String(wire_value.to_string());

        assert_eq!(
            serde_json::from_value::<BitgetCancelType>(value.clone()).unwrap(),
            expected
        );
        assert_eq!(serde_json::to_value(expected).unwrap(), value);
    }

    #[rstest]
    #[case(BitgetInstrumentStatus::PreLaunch, "listed")]
    #[case(BitgetInstrumentStatus::Trading, "online")]
    #[case(BitgetInstrumentStatus::LimitOpen, "limit_open")]
    #[case(BitgetInstrumentStatus::LimitClose, "limit_close")]
    #[case(BitgetInstrumentStatus::RestrictedApi, "restrictedAPI")]
    #[case(BitgetInstrumentStatus::Closed, "offline")]
    fn test_instrument_status_round_trip(
        #[case] status: BitgetInstrumentStatus,
        #[case] wire_value: &str,
    ) {
        let value = serde_json::Value::String(wire_value.to_string());

        assert_eq!(
            serde_json::from_value::<BitgetInstrumentStatus>(value.clone()).unwrap(),
            status
        );
        assert_eq!(serde_json::to_value(status).unwrap(), value);
    }

    #[rstest]
    fn test_normal_spot_only_margin_trading_round_trip() {
        let value = serde_json::Value::String("normalSpotOnly".to_string());

        assert_eq!(
            serde_json::from_value::<BitgetMarginTrading>(value.clone()).unwrap(),
            BitgetMarginTrading::NormalSpotOnly
        );
        assert_eq!(
            serde_json::to_value(BitgetMarginTrading::NormalSpotOnly).unwrap(),
            value
        );
    }

    #[rstest]
    #[case(BitgetPositionStatus::Normal, "normal")]
    #[case(BitgetPositionStatus::Ended, "ended")]
    #[case(BitgetPositionStatus::Liq, "Liq")]
    #[case(BitgetPositionStatus::Adl, "Adl")]
    fn test_position_status_round_trip(
        #[case] status: BitgetPositionStatus,
        #[case] wire_value: &str,
    ) {
        let value = serde_json::Value::String(wire_value.to_string());

        assert_eq!(
            serde_json::from_value::<BitgetPositionStatus>(value.clone()).unwrap(),
            status
        );
        assert_eq!(serde_json::to_value(status).unwrap(), value);
    }

    #[rstest]
    #[case(BitgetProductType::Spot, "SPOT", "spot")]
    #[case(BitgetProductType::Linear, "USDT-FUTURES", "usdt-futures")]
    #[case(BitgetProductType::Inverse, "COIN-FUTURES", "coin-futures")]
    fn test_product_type_category_forms(
        #[case] product_type: BitgetProductType,
        #[case] category: &str,
        #[case] inst_type: &str,
    ) {
        assert_eq!(product_type.as_category(), category);
        assert_eq!(product_type.as_ws_inst_type(), inst_type);
        assert_eq!(
            BitgetProductType::from_category(category),
            Some(product_type)
        );
        assert_eq!(
            BitgetProductType::from_category(inst_type),
            Some(product_type)
        );
        assert_eq!(
            serde_json::from_value::<BitgetProductType>(serde_json::json!(inst_type)).unwrap(),
            product_type
        );
        assert_eq!(serde_json::to_value(product_type).unwrap(), category);
    }

    #[rstest]
    fn test_linear_suffix_is_perp() {
        assert_eq!(BitgetProductType::Linear.suffix(), "-PERP");
        assert_eq!(
            BitgetProductType::from_suffix("TSLAUSDT-PERP"),
            Some(BitgetProductType::Linear)
        );
        assert_eq!(BitgetProductType::from_suffix("TSLAUSDT"), None);
    }

    #[rstest]
    #[case("live", BitgetOrderStatus::Created)]
    #[case("new", BitgetOrderStatus::New)]
    #[case("partially_filled", BitgetOrderStatus::PartiallyFilled)]
    #[case("filled", BitgetOrderStatus::Filled)]
    #[case("cancelled", BitgetOrderStatus::Canceled)]
    #[case("pending", BitgetOrderStatus::Untriggered)]
    fn test_order_status_wire_values(
        #[case] wire_value: &str,
        #[case] expected: BitgetOrderStatus,
    ) {
        assert_eq!(
            serde_json::from_value::<BitgetOrderStatus>(serde_json::json!(wire_value)).unwrap(),
            expected
        );
    }
}
