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

//! Python bindings from `pyo3`.

pub mod config;
pub mod enums;
pub mod factories;
pub mod http;
pub mod params;
pub mod types;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_model::{
    data::ensure_rust_extractor_registered,
    enums::{BarAggregation, OrderSide},
};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::{
        consts::{BITGET, BITGET_CLIENT_ID, BITGET_VENUE},
        enums::{BitgetOrderSide, BitgetPositionIdx, BitgetPositionMode},
        parse::{bar_spec_to_bitget_interval, extract_raw_symbol, resolve_position_idx},
        symbol::BitgetSymbol,
    },
    config::{BitgetDataClientConfig, BitgetExecutionClientConfig},
    data_types::{BitgetLiquidation, register_bitget_custom_data},
    factories::{BitgetDataClientFactory, BitgetExecutionClientFactory},
};

/// Extracts the raw symbol from a Bitget symbol by removing the product type suffix.
///
/// # Examples
/// - `"ETHUSDT-LINEAR"` → `"ETHUSDT"`
/// - `"BTCUSDT-SPOT"` → `"BTCUSDT"`
/// - `"ETHUSDT"` → `"ETHUSDT"` (no suffix)
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.bitget")]
#[pyo3(name = "bitget_extract_raw_symbol")]
fn py_bitget_extract_raw_symbol(symbol: &str) -> &str {
    extract_raw_symbol(symbol)
}

/// Converts a Nautilus bar aggregation and step to a Bitget kline interval string.
///
/// # Errors
///
/// Returns an error if the aggregation type or step is not supported by Bitget.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.bitget")]
#[pyo3(name = "bitget_bar_spec_to_interval")]
fn py_bitget_bar_spec_to_interval(aggregation: BarAggregation, step: u64) -> PyResult<String> {
    let interval = bar_spec_to_bitget_interval(aggregation, step).map_err(to_pyvalue_err)?;
    Ok(interval.to_string())
}

/// Extracts the product type from a Bitget symbol.
///
/// # Examples
/// - `"ETHUSDT-LINEAR"` → `BitgetProductType.LINEAR`
/// - `"BTCUSDT-SPOT"` → `BitgetProductType.SPOT`
/// - `"BTCUSD-INVERSE"` → `BitgetProductType.INVERSE`
/// - `"ETH-26JUN26-16000-P-OPTION"` → `BitgetProductType.OPTION`
///
/// # Errors
///
/// Returns an error if the symbol does not contain a valid Bitget product type suffix.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.bitget")]
#[pyo3(name = "bitget_product_type_from_symbol")]
fn py_bitget_product_type_from_symbol(
    symbol: &str,
) -> PyResult<crate::common::enums::BitgetProductType> {
    let bitget_symbol = BitgetSymbol::new(symbol).map_err(to_pyvalue_err)?;
    Ok(bitget_symbol.product_type())
}

/// Resolves the Bitget `positionIdx` for an outgoing order.
///
/// Returns `None` when no position mode is configured. Otherwise returns the
/// hedge-mode index for the position being affected (long or short), accounting
/// for `is_reduce_only`. A `manual_override` always wins.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.bitget")]
#[pyo3(name = "bitget_resolve_position_idx")]
#[pyo3(signature = (position_mode, order_side, is_reduce_only, manual_override=None))]
fn py_bitget_resolve_position_idx(
    position_mode: Option<BitgetPositionMode>,
    order_side: OrderSide,
    is_reduce_only: bool,
    manual_override: Option<BitgetPositionIdx>,
) -> Option<BitgetPositionIdx> {
    let bitget_side = BitgetOrderSide::from(order_side);
    resolve_position_idx(position_mode, bitget_side, is_reduce_only, manual_override)
}

#[expect(clippy::needless_pass_by_value)]
fn extract_bitget_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<BitgetDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract BitgetDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_bitget_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<BitgetExecutionClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract BitgetExecutionClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_bitget_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<BitgetDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract BitgetDataClientConfig: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_bitget_exec_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<BitgetExecutionClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract BitgetExecutionClientConfig: {e}"
        ))),
    }
}

/// Exposed through `nautilus_trader.adapters.bitget`.
///
/// # Errors
///
/// Returns an error if any bindings fail to register with the Python module.
#[pymodule]
#[rustfmt::skip]
pub fn bitget(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(stringify!(BITGET), BITGET)?;
    m.add(stringify!(BITGET_CLIENT_ID), *BITGET_CLIENT_ID)?;
    m.add(stringify!(BITGET_VENUE), *BITGET_VENUE)?;
    m.add_class::<crate::common::enums::BitgetAccountType>()?;
    m.add_class::<crate::common::enums::BitgetCancelType>()?;
    m.add_class::<crate::common::enums::BitgetEnvironment>()?;
    m.add_class::<crate::common::enums::BitgetMarginAction>()?;
    m.add_class::<crate::common::enums::BitgetMarginMode>()?;
    m.add_class::<crate::common::enums::BitgetOpenOnly>()?;
    m.add_class::<crate::common::enums::BitgetOrderFilter>()?;
    m.add_class::<crate::common::enums::BitgetOrderSide>()?;
    m.add_class::<crate::common::enums::BitgetOrderStatus>()?;
    m.add_class::<crate::common::enums::BitgetOrderType>()?;
    m.add_class::<crate::common::enums::BitgetPositionIdx>()?;
    m.add_class::<crate::common::enums::BitgetPositionMode>()?;
    m.add_class::<crate::common::enums::BitgetProductType>()?;
    m.add_class::<crate::common::enums::BitgetStopOrderType>()?;
    m.add_class::<crate::common::enums::BitgetTimeInForce>()?;
    m.add_class::<crate::common::enums::BitgetTpSlMode>()?;
    m.add_class::<crate::common::enums::BitgetTriggerDirection>()?;
    m.add_class::<crate::common::enums::BitgetTriggerType>()?;
    m.add_class::<crate::http::client::BitgetHttpClient>()?;
    m.add_class::<crate::http::client::BitgetRawHttpClient>()?;
    m.add_class::<crate::http::models::BitgetServerTime>()?;
    m.add_class::<crate::http::models::BitgetAccountDetails>()?;
    m.add_class::<crate::http::models::BitgetApiKeyPermissions>()?;
    m.add_class::<crate::http::models::BitgetFeeRate>()?;
    m.add_class::<crate::http::models::BitgetOrder>()?;
    m.add_class::<crate::http::models::BitgetOrderCursorList>()?;
    m.add_class::<crate::http::models::BitgetTickerData>()?;
    m.add_class::<crate::common::types::BitgetMarginBorrowResult>()?;
    m.add_class::<crate::common::types::BitgetMarginRepayResult>()?;
    m.add_class::<crate::common::types::BitgetMarginStatusResult>()?;
    m.add_class::<params::BitgetTickersParams>()?;
    m.add_class::<params::BitgetNativeTpSlParams>()?;
    m.add_class::<BitgetLiquidation>()?;
    m.add_class::<BitgetDataClientConfig>()?;
    m.add_class::<BitgetDataClientFactory>()?;
    m.add_class::<BitgetExecutionClientConfig>()?;
    m.add_class::<BitgetExecutionClientFactory>()?;
    m.add_function(wrap_pyfunction!(py_bitget_extract_raw_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(py_bitget_bar_spec_to_interval, m)?)?;
    m.add_function(wrap_pyfunction!(py_bitget_product_type_from_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(py_bitget_resolve_position_idx, m)?)?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(BITGET.to_string(), extract_bitget_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Bitget data factory extractor: {e}"
        )));
    }

    if let Err(e) = registry
        .register_exec_factory_extractor(BITGET.to_string(), extract_bitget_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Bitget exec factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "BitgetDataClientConfig".to_string(),
        extract_bitget_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Bitget data config extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "BitgetExecutionClientConfig".to_string(),
        extract_bitget_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Bitget exec config extractor: {e}"
        )));
    }

    register_bitget_custom_data();
    let _result = ensure_rust_extractor_registered::<BitgetLiquidation>();

    Ok(())
}
