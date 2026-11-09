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

//! Parsing functions for Bitget HTTP API responses.

use serde::de::DeserializeOwned;

use super::models::{
    BitgetInstrumentInverseResponse, BitgetInstrumentLinearResponse,
    BitgetInstrumentOptionResponse, BitgetInstrumentSpotResponse, BitgetKlinesResponse,
    BitgetServerTimeResponse, BitgetTickersLinearResponse, BitgetTickersOptionResponse,
    BitgetTickersSpotResponse, BitgetTradesResponse,
};
use crate::common::models::BitgetResponse;

fn parse_response<T: DeserializeOwned>(data: &[u8]) -> anyhow::Result<BitgetResponse<T>> {
    let response = serde_json::from_slice::<BitgetResponse<T>>(data)?;
    if response.ret_code != 0 {
        anyhow::bail!(
            "Bitget API error {}: {}",
            response.ret_code,
            response.ret_msg
        );
    }
    Ok(response)
}

/// Parses a Bitget server time response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_server_time_response(data: &[u8]) -> anyhow::Result<BitgetServerTimeResponse> {
    parse_response(data)
}

/// Parses a Bitget spot instruments response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_instruments_spot_response(
    data: &[u8],
) -> anyhow::Result<BitgetInstrumentSpotResponse> {
    parse_response(data)
}

/// Parses a Bitget linear instruments response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_instruments_linear_response(
    data: &[u8],
) -> anyhow::Result<BitgetInstrumentLinearResponse> {
    parse_response(data)
}

/// Parses a Bitget inverse instruments response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_instruments_inverse_response(
    data: &[u8],
) -> anyhow::Result<BitgetInstrumentInverseResponse> {
    parse_response(data)
}

/// Parses a Bitget option instruments response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_instruments_option_response(
    data: &[u8],
) -> anyhow::Result<BitgetInstrumentOptionResponse> {
    parse_response(data)
}

/// Parses a Bitget spot tickers response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_tickers_spot_response(data: &[u8]) -> anyhow::Result<BitgetTickersSpotResponse> {
    parse_response(data)
}

/// Parses a Bitget linear tickers response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_tickers_linear_response(data: &[u8]) -> anyhow::Result<BitgetTickersLinearResponse> {
    parse_response(data)
}

/// Parses a Bitget option tickers response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_tickers_option_response(data: &[u8]) -> anyhow::Result<BitgetTickersOptionResponse> {
    parse_response(data)
}

/// Parses a Bitget klines response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_klines_response(data: &[u8]) -> anyhow::Result<BitgetKlinesResponse> {
    parse_response(data)
}

/// Parses a Bitget trades response from raw JSON bytes.
///
/// # Errors
///
/// Returns an error if deserialization or validation fails.
pub fn parse_trades_response(data: &[u8]) -> anyhow::Result<BitgetTradesResponse> {
    parse_response(data)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::testing::load_test_json;

    #[rstest]
    fn test_parse_instruments_linear_response() {
        let json = load_test_json("http_get_instruments_linear.json");
        let result = parse_instruments_linear_response(json.as_bytes());
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.ret_code, 0);
        assert!(!response.result.list.is_empty());
    }

    #[rstest]
    fn test_parse_instruments_spot_response() {
        let json = load_test_json("http_get_instruments_spot.json");
        let result = parse_instruments_spot_response(json.as_bytes());
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.ret_code, 0);
        assert!(!response.result.list.is_empty());
    }

    #[rstest]
    fn test_parse_instruments_inverse_response() {
        let json = load_test_json("http_get_instruments_inverse.json");
        let result = parse_instruments_inverse_response(json.as_bytes());
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.ret_code, 0);
        assert!(!response.result.list.is_empty());
    }

    #[rstest]
    fn test_parse_response_rejects_error_envelope() {
        let json = load_test_json("http_get_fee_rate_not_found.json");
        let result = parse_instruments_option_response(json.as_bytes());

        assert_eq!(
            result.unwrap_err().to_string(),
            "Bitget API error 40404: Request URL NOT FOUND"
        );
    }

    #[rstest]
    fn test_parse_klines_response() {
        let json = load_test_json("http_get_klines_linear.json");
        let result = parse_klines_response(json.as_bytes());
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.ret_code, 0);
        assert!(!response.result.list.is_empty());
    }

    #[rstest]
    fn test_parse_trades_response() {
        let json = load_test_json("http_get_trades_recent.json");
        let result = parse_trades_response(json.as_bytes());
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.ret_code, 0);
        assert!(!response.result.list.is_empty());
    }
}
