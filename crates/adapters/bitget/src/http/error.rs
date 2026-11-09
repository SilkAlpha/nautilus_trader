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

//! Error structures and enumerations for the Bitget integration.
//!
//! The JSON error schema is described in the Bitget documentation under
//! *Error Codes* - <https://www.bitget.com/docs/uta/error-code/restapi>.
//! The types below mirror that structure and are reused across the entire
//! crate.

use nautilus_network::http::HttpClientError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Build error for query parameter validation.
#[derive(Debug, Clone, Error)]
pub enum BitgetBuildError {
    /// Missing required category.
    #[error("Missing required category")]
    MissingCategory,
    /// Missing required symbol.
    #[error("Missing required symbol")]
    MissingSymbol,
    /// Missing required interval.
    #[error("Missing required interval")]
    MissingInterval,
    /// Invalid limit value.
    #[error("Invalid limit: must be between 1 and 1000")]
    InvalidLimit,
    /// Invalid time range: `start` should be less than `end`.
    #[error("Invalid time range: start ({start}) must be less than end ({end})")]
    InvalidTimeRange { start: i64, end: i64 },
    /// Both orderId and clientOid specified.
    #[error("Cannot specify both 'orderId' and 'clientOid'")]
    BothOrderIds,
    /// Missing required order identifier.
    #[error("Missing required order identifier (orderId or clientOid)")]
    MissingOrderId,
}

/// Represents the JSON structure of an error response returned by the Bitget API.
///
/// # References
/// - <https://www.bitget.com/docs/uta/error-code/restapi>
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetErrorResponse {
    /// Error code returned by Bitget (`code`, a numeric string such as `"25204"`).
    #[serde(rename = "code", deserialize_with = "deserialize_code_i32")]
    pub ret_code: i32,
    /// A human-readable explanation of the error condition.
    #[serde(rename = "msg", default)]
    pub ret_msg: String,
    /// Extended error information (Bitget sends none; kept for parity).
    #[serde(default)]
    pub ret_ext_info: Option<serde_json::Value>,
}

fn deserialize_code_i32<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    crate::common::parse::deserialize_int_or_string(d)
}

/// A typed error enumeration for the Bitget HTTP client.
#[derive(Debug, Clone, Error)]
pub enum BitgetHttpError {
    /// Error variant when credentials are missing but the request is authenticated.
    #[error("Missing credentials for authenticated request")]
    MissingCredentials,
    /// Errors returned directly by Bitget (non-zero code).
    #[error("Bitget error {error_code}: {message}")]
    BitgetError { error_code: i32, message: String },
    /// Failure during JSON serialization/deserialization.
    #[error("JSON error: {0}")]
    JsonError(String),
    /// Parameter validation error.
    #[error("Parameter validation error: {0}")]
    ValidationError(String),
    /// Build error for query parameters.
    #[error("Build error: {0}")]
    BuildError(#[from] BitgetBuildError),
    /// Request was canceled, typically due to shutdown or disconnect.
    #[error("Request canceled: {0}")]
    Canceled(String),
    /// Generic network error (for retries, cancellations, etc).
    #[error("Network error: {0}")]
    NetworkError(String),
    /// Any unknown HTTP status or unexpected response from Bitget.
    #[error("Unexpected HTTP status code {status}: {body}")]
    UnexpectedStatus { status: u16, body: String },
}

/// Error raised while submitting a Bitget order.
#[derive(Debug, Error)]
pub enum BitgetSubmitOrderError {
    /// Bitget accepted the request envelope, but did not return an order ID.
    #[error("No order_id in response")]
    MissingOrderId,
    /// Bitget returned an order that is rejected with no fills.
    #[error("Order rejected: {reason}")]
    Rejected {
        /// Venue reject reason.
        reason: String,
    },
    /// Bitget returned an order ID, but the immediate order lookup failed.
    #[error("Order lookup failed after submission: {source}")]
    PostSubmitLookup {
        /// Source lookup error.
        #[source]
        source: anyhow::Error,
    },
}

/// Error raised after Bitget accepts a cancel request envelope.
#[derive(Debug, Error)]
pub enum BitgetCancelOrderError {
    /// Bitget accepted the request envelope, but did not return an order ID.
    #[error("No order_id in cancel response")]
    MissingOrderId,
    /// Bitget returned an order ID, but the immediate order lookup failed.
    #[error("Order lookup failed after cancellation: {source}")]
    PostCancelLookup {
        /// Source lookup error.
        #[source]
        source: anyhow::Error,
    },
}

/// Error raised after Bitget accepts a modify request envelope.
#[derive(Debug, Error)]
pub enum BitgetModifyOrderError {
    /// Bitget accepted the request envelope, but did not return an order ID.
    #[error("No order_id in amend response")]
    MissingOrderId,
    /// Bitget returned an order ID, but the immediate order lookup failed.
    #[error("Order lookup failed after amendment: {source}")]
    PostModifyLookup {
        /// Source lookup error.
        #[source]
        source: anyhow::Error,
    },
}

impl From<HttpClientError> for BitgetHttpError {
    fn from(error: HttpClientError) -> Self {
        Self::NetworkError(error.to_string())
    }
}

impl From<String> for BitgetHttpError {
    fn from(error: String) -> Self {
        Self::ValidationError(error)
    }
}

// Allow use of the `?` operator on `serde_json` results inside the HTTP
// client implementation by converting them into our typed error.
impl From<serde_json::Error> for BitgetHttpError {
    fn from(error: serde_json::Error) -> Self {
        Self::JsonError(error.to_string())
    }
}

impl From<BitgetErrorResponse> for BitgetHttpError {
    fn from(error: BitgetErrorResponse) -> Self {
        Self::BitgetError {
            error_code: error.ret_code,
            message: error.ret_msg,
        }
    }
}

/// Returns `true` when a Bitget order-mutation error leaves the outcome unknown (timeout or
/// service error); the order must then be queried by `clientOid`, never resent.
pub(crate) fn is_bitget_ambiguous_order_error_code(code: i64) -> bool {
    // 40010 "Request timed out", 40725 "service return an error", 45001 "Unknown error"
    matches!(code, 40010 | 40725 | 45001)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_bitget_build_error_display() {
        let error = BitgetBuildError::MissingSymbol;
        assert_eq!(error.to_string(), "Missing required symbol");

        let error = BitgetBuildError::InvalidLimit;
        assert_eq!(
            error.to_string(),
            "Invalid limit: must be between 1 and 1000"
        );

        let error = BitgetBuildError::InvalidTimeRange {
            start: 100,
            end: 50,
        };
        assert_eq!(
            error.to_string(),
            "Invalid time range: start (100) must be less than end (50)"
        );
    }

    #[rstest]
    fn test_bitget_http_error_from_error_response() {
        let error_response: BitgetErrorResponse = serde_json::from_str(
            r#"{"code":"25204","msg":"Order does not exist","requestTime":1794193099412,"data":null}"#,
        )
        .unwrap();

        let http_error: BitgetHttpError = error_response.into();
        assert_eq!(
            http_error.to_string(),
            "Bitget error 25204: Order does not exist"
        );
    }

    #[rstest]
    fn test_bitget_http_error_from_json_error() {
        let json_err = serde_json::from_str::<BitgetErrorResponse>("invalid json").unwrap_err();
        let http_error: BitgetHttpError = json_err.into();
        assert!(http_error.to_string().contains("JSON error"));
    }

    #[rstest]
    fn test_bitget_http_error_from_string() {
        let error_msg = "Invalid parameter value".to_string();
        let http_error: BitgetHttpError = error_msg.into();
        assert_eq!(
            http_error.to_string(),
            "Parameter validation error: Invalid parameter value"
        );
    }

    #[rstest]
    #[case(40010, true)]
    #[case(40725, true)]
    #[case(45001, true)]
    #[case(429, false)]
    #[case(25004, false)]
    #[case(25204, false)]
    #[case(25206, false)]
    #[case(22002, false)]
    #[case(40015, false)]
    fn test_order_error_ambiguity(#[case] code: i64, #[case] expected: bool) {
        assert_eq!(is_bitget_ambiguous_order_error_code(code), expected);
    }

    #[rstest]
    fn test_unexpected_status_error() {
        let error = BitgetHttpError::UnexpectedStatus {
            status: 502,
            body: "Server error".to_string(),
        };
        assert_eq!(
            error.to_string(),
            "Unexpected HTTP status code 502: Server error"
        );
    }

    #[rstest]
    fn test_bitget_submit_order_error_display() {
        let missing_order_id = BitgetSubmitOrderError::MissingOrderId;
        let rejected = BitgetSubmitOrderError::Rejected {
            reason: "post_only_fill_cancel".to_string(),
        };
        let post_submit_lookup = BitgetSubmitOrderError::PostSubmitLookup {
            source: anyhow::anyhow!("No order returned after submission"),
        };

        assert_eq!(missing_order_id.to_string(), "No order_id in response");
        assert_eq!(
            rejected.to_string(),
            "Order rejected: post_only_fill_cancel"
        );
        assert_eq!(
            post_submit_lookup.to_string(),
            "Order lookup failed after submission: No order returned after submission"
        );
    }
}
