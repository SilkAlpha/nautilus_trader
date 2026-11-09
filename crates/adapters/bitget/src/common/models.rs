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

//! Shared data transfer objects used across Bitget HTTP and WebSocket payloads.

use serde::{Deserialize, Serialize};

use crate::common::{
    enums::BitgetProductType,
    parse::{deserialize_code, deserialize_null_default, deserialize_optional_cursor},
};

/// Generic wrapper that contains a list payload returned by Bitget.
///
/// Bitget returns most lists as a bare JSON array in `data` and some as `{"list": [...]}`;
/// both forms (and `null`) decode into this wrapper.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    bound(deserialize = "T: Deserialize<'de>"),
    from = "ListPayload<T>"
)]
pub struct BitgetList<T> {
    /// Collection returned by the endpoint (`null` when empty).
    pub list: Vec<T>,
}

impl<T> From<ListPayload<T>> for BitgetList<T> {
    fn from(payload: ListPayload<T>) -> Self {
        let page = BitgetCursorList::from(payload);
        Self { list: page.list }
    }
}

/// Wire forms of a Bitget list payload: a bare array, a `{list, cursor}` object, or `null`.
#[derive(Debug, Deserialize)]
#[serde(untagged, bound(deserialize = "T: Deserialize<'de>"))]
#[doc(hidden)]
pub enum ListPayload<T> {
    Array(Vec<T>),
    Page(PagePayload<T>),
    Null(()),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", bound(deserialize = "T: Deserialize<'de>"))]
#[doc(hidden)]
pub struct PagePayload<T> {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    list: Vec<T>,
    #[serde(default, deserialize_with = "deserialize_optional_cursor")]
    cursor: Option<String>,
    #[serde(default)]
    category: Option<BitgetProductType>,
}

impl<T> From<ListPayload<T>> for BitgetCursorList<T> {
    fn from(payload: ListPayload<T>) -> Self {
        match payload {
            ListPayload::Array(list) => Self {
                list,
                next_page_cursor: None,
                category: None,
            },
            ListPayload::Page(page) => Self {
                list: page.list,
                next_page_cursor: page.cursor,
                category: page.category,
            },
            ListPayload::Null(()) => Self {
                list: Vec::new(),
                next_page_cursor: None,
                category: None,
            },
        }
    }
}

/// Generic list result that also carries pagination cursor information.
///
/// Bitget returns `"list": null` for empty pages (e.g. `/api/v3/trade/fills`), decoded as empty.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    bound(deserialize = "T: Deserialize<'de>"),
    from = "ListPayload<T>"
)]
pub struct BitgetCursorList<T> {
    /// Collection returned by the endpoint (`null` when empty).
    pub list: Vec<T>,
    /// Pagination cursor for the next page, when provided (the smallest id of this page).
    #[serde(rename = "cursor")]
    pub next_page_cursor: Option<String>,
    /// Optional product category when the API includes it.
    #[serde(default)]
    pub category: Option<BitgetProductType>,
}

// Bitget instruments are flat; the filter structs below are built from the flat fields by the
// instrument models (`price_filter()`, `lot_size_filter()`, ...) so parsing mirrors the other venues.

/// Common leverage filter that describes leverage bounds and step.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeverageFilter {
    /// Minimum leverage supported.
    pub min_leverage: String,
    /// Maximum leverage supported.
    pub max_leverage: String,
    /// Step between successive leverage values.
    pub leverage_step: String,
}

/// Price filter for linear/inverse contracts describing min/max and tick.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearPriceFilter {
    /// Minimum allowed order price.
    pub min_price: String,
    /// Maximum allowed order price.
    pub max_price: String,
    /// Tick size for price increments.
    pub tick_size: String,
}

/// Price filter for spot instruments (only tick size is provided).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotPriceFilter {
    /// Tick size for price increments.
    pub tick_size: String,
}

/// Lot size filter for spot instruments.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotLotSizeFilter {
    /// Base asset precision.
    pub base_precision: String,
    /// Quote asset precision.
    pub quote_precision: String,
    /// Minimum order quantity.
    pub min_order_qty: String,
    /// Maximum order quantity.
    pub max_order_qty: String,
    /// Minimum order notional.
    pub min_order_amt: String,
    /// Maximum order notional.
    pub max_order_amt: String,
}

/// Lot size filter for derivatives instruments.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearLotSizeFilter {
    /// Maximum order quantity.
    pub max_order_qty: String,
    /// Minimum order quantity.
    pub min_order_qty: String,
    /// Quantity increment step.
    pub qty_step: String,
    /// Maximum order quantity for post-only orders.
    #[serde(default)]
    pub post_only_max_order_qty: Option<String>,
    /// Maximum order quantity for market orders.
    #[serde(default)]
    pub max_mkt_order_qty: Option<String>,
    /// Minimum notional value.
    #[serde(default)]
    pub min_notional_value: Option<String>,
}

/// Lot size filter for option instruments.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OptionLotSizeFilter {
    /// Maximum order quantity.
    pub max_order_qty: String,
    /// Minimum order quantity.
    pub min_order_qty: String,
    /// Quantity increment step.
    pub qty_step: String,
}

/// Trait for Bitget response types that contain `code` and `msg` fields.
pub trait BitgetResponseCheck {
    /// Returns the response code (0 = success, decoded from `"00000"`).
    fn ret_code(&self) -> i64;
    /// Returns the response message.
    fn ret_msg(&self) -> &str;
}

/// Lightweight struct for checking Bitget API errors without deserializing result.
///
/// Bitget error responses often have `result: null` which would fail deserialization
/// into typed response structs. This struct ignores the result field entirely.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetErrorCheck {
    /// Return code (0 = success, decoded from `"00000"`).
    #[serde(rename = "code", deserialize_with = "deserialize_code")]
    pub ret_code: i64,
    /// Textual message accompanying the response.
    #[serde(rename = "msg", default)]
    pub ret_msg: String,
}

/// Top-level response envelope returned by Bitget HTTP endpoints.
///
/// Bitget envelope: `{"code":"00000","msg":"success","requestTime":<ms>,"data":<payload>}`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetResponse<T> {
    /// Return code (0 = success, decoded from `"00000"`).
    #[serde(rename = "code", deserialize_with = "deserialize_code")]
    pub ret_code: i64,
    /// Textual message accompanying the response.
    #[serde(rename = "msg", default)]
    pub ret_msg: String,
    /// Actual payload returned by the endpoint.
    #[serde(rename = "data")]
    pub result: T,
    /// Additional metadata (Bitget sends none; kept for parity).
    #[serde(default)]
    pub ret_ext_info: Option<serde_json::Value>,
    /// Server time in milliseconds when the response was produced.
    #[serde(rename = "requestTime", default)]
    pub time: Option<i64>,
}

impl<T> BitgetResponseCheck for BitgetResponse<T> {
    fn ret_code(&self) -> i64 {
        self.ret_code
    }

    fn ret_msg(&self) -> &str {
        &self.ret_msg
    }
}

/// Convenience alias for responses that return a simple list.
pub type BitgetListResponse<T> = BitgetResponse<BitgetList<T>>;

/// Convenience alias for responses that return a cursor-based list.
pub type BitgetCursorListResponse<T> = BitgetResponse<BitgetCursorList<T>>;

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_cursor_list_null_is_empty() {
        let page: BitgetCursorList<String> =
            serde_json::from_str(r#"{"list":null,"cursor":null}"#).unwrap();
        assert!(page.list.is_empty());
        assert!(page.next_page_cursor.is_none());
    }

    #[rstest]
    fn test_cursor_list_numeric_cursor() {
        let page: BitgetCursorList<String> =
            serde_json::from_str(r#"{"list":["a"],"cursor":1235058132196622336}"#).unwrap();
        assert_eq!(page.list, vec!["a".to_string()]);
        assert_eq!(
            page.next_page_cursor.as_deref(),
            Some("1235058132196622336")
        );
    }

    #[rstest]
    fn test_list_missing_is_empty() {
        let page: BitgetList<String> = serde_json::from_str("{}").unwrap();
        assert!(page.list.is_empty());
    }

    #[rstest]
    #[case(r#"["a","b"]"#, 2)]
    #[case(r#"{"list":["a","b"]}"#, 2)]
    #[case("null", 0)]
    fn test_list_wire_forms(#[case] raw: &str, #[case] expected: usize) {
        let list: BitgetList<String> = serde_json::from_str(raw).unwrap();
        assert_eq!(list.list.len(), expected);
        let page: BitgetCursorList<String> = serde_json::from_str(raw).unwrap();
        assert_eq!(page.list.len(), expected);
    }

    #[rstest]
    #[case(r#"{"code":"00000","msg":"success","requestTime":1,"data":null}"#, 0)]
    #[case(
        r#"{"code":"25204","msg":"Order does not exist","requestTime":1,"data":null}"#,
        25204
    )]
    #[case(r#"{"code":0,"msg":"","data":null}"#, 0)]
    fn test_response_code_forms(#[case] raw: &str, #[case] expected: i64) {
        let response: BitgetResponse<Option<String>> = serde_json::from_str(raw).unwrap();
        assert_eq!(response.ret_code, expected);
        let check: BitgetErrorCheck = serde_json::from_str(raw).unwrap();
        assert_eq!(check.ret_code, expected);
    }
}
