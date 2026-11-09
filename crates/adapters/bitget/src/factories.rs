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

//! Factory functions for creating Bitget clients and components.

use std::{any::Any, cell::RefCell, rc::Rc};

#[cfg(test)]
use nautilus_common::clock::VirtualClock;
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, TraderId},
};

use crate::{
    common::{
        consts::{BITGET, BITGET_VENUE},
        enums::BitgetProductType,
    },
    config::{BitgetDataClientConfig, BitgetExecutionClientConfig},
    data::BitgetDataClient,
    execution::BitgetExecutionClient,
};

impl ClientConfig for BitgetDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for BitgetExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Bitget data clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetDataClientFactory;

impl BitgetDataClientFactory {
    /// Creates a new [`BitgetDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for BitgetDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for BitgetDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let bitget_config = config
            .as_any()
            .downcast_ref::<BitgetDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for BitgetDataClientFactory. Expected BitgetDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let client = BitgetDataClient::new(client_id, bitget_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        BITGET
    }

    fn config_type(&self) -> &'static str {
        "BitgetDataClientConfig"
    }
}

/// Factory for creating Bitget execution clients.
#[derive(Debug, Default, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetExecutionClientFactory;

impl BitgetExecutionClientFactory {
    /// Creates a new [`BitgetExecutionClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for BitgetExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let bitget_config = config
            .as_any()
            .downcast_ref::<BitgetExecutionClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for BitgetExecutionClientFactory. Expected BitgetExecutionClientConfig, was {config:?}",
                )
            })?
            .clone();

        // Default to Linear if product_types is empty (matches execution client behavior)
        let product_types = if bitget_config.product_types.is_empty() {
            vec![BitgetProductType::Linear]
        } else {
            bitget_config.product_types.clone()
        };

        let has_derivatives = product_types.iter().any(|t| {
            matches!(
                t,
                BitgetProductType::Linear | BitgetProductType::Inverse | BitgetProductType::Option
            )
        });

        let account_type = if has_derivatives {
            AccountType::Margin
        } else {
            AccountType::Cash
        };

        // Bitget uses netting for derivatives, hedging for spot
        let oms_type = if has_derivatives {
            OmsType::Netting
        } else {
            OmsType::Hedging
        };

        let account_id = bitget_config
            .account_id
            .unwrap_or_else(|| AccountId::from("BITGET-001"));

        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            *BITGET_VENUE,
            oms_type,
            account_id,
            account_type,
            None, // base_currency
            cache,
        );

        let client = BitgetExecutionClient::new(core, bitget_config)?;

        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        BITGET
    }

    fn config_type(&self) -> &'static str {
        "BitgetExecutionClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        factories::{ClientConfig, ExecutionClientFactory},
    };
    use nautilus_model::identifiers::TraderId;
    use rstest::rstest;

    use super::*;
    use crate::{common::enums::BitgetProductType, config::BitgetExecutionClientConfig};

    #[rstest]
    fn test_bitget_execution_client_factory_creation() {
        let factory = BitgetExecutionClientFactory::new();
        assert_eq!(factory.name(), BITGET);
        assert_eq!(factory.config_type(), "BitgetExecutionClientConfig");
    }

    #[rstest]
    fn test_bitget_exec_client_config_implements_client_config() {
        let config = BitgetExecutionClientConfig::default();

        let boxed_config: Box<dyn ClientConfig> = Box::new(config);
        let downcasted = boxed_config
            .as_any()
            .downcast_ref::<BitgetExecutionClientConfig>();

        assert!(downcasted.is_some());
    }

    #[rstest]
    fn test_bitget_execution_client_factory_creates_client_for_spot() {
        let factory = BitgetExecutionClientFactory::new();
        let config = BitgetExecutionClientConfig {
            product_types: vec![BitgetProductType::Spot],
            api_key: Some("test_key".into()),
            api_secret: Some("test_secret".into()),
            api_passphrase: Some("test_passphrase".into()),
            ..Default::default()
        };

        let cache = Rc::new(RefCell::new(Cache::default()));

        let result = factory.create(
            TraderId::from("TRADER-001"),
            "BITGET-TEST",
            &config,
            cache.into(),
            Rc::new(RefCell::new(VirtualClock::new())),
        );
        assert!(result.is_ok());

        let client = result.unwrap();
        assert_eq!(client.client_id(), ClientId::from("BITGET-TEST"));
    }

    #[rstest]
    fn test_bitget_execution_client_factory_creates_client_for_derivatives() {
        let factory = BitgetExecutionClientFactory::new();
        let config = BitgetExecutionClientConfig {
            product_types: vec![BitgetProductType::Linear, BitgetProductType::Inverse],
            api_key: Some("test_key".into()),
            api_secret: Some("test_secret".into()),
            api_passphrase: Some("test_passphrase".into()),
            ..Default::default()
        };

        let cache = Rc::new(RefCell::new(Cache::default()));

        let result = factory.create(
            TraderId::from("TRADER-001"),
            "BITGET-DERIV",
            &config,
            cache.into(),
            Rc::new(RefCell::new(VirtualClock::new())),
        );
        result.unwrap();
    }

    #[rstest]
    fn test_bitget_execution_client_factory_rejects_wrong_config_type() {
        let factory = BitgetExecutionClientFactory::new();
        let wrong_config = BitgetDataClientConfig::default();

        let cache = Rc::new(RefCell::new(Cache::default()));

        let result = factory.create(
            TraderId::from("TRADER-001"),
            "BITGET-TEST",
            &wrong_config,
            cache.into(),
            Rc::new(RefCell::new(VirtualClock::new())),
        );
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Invalid config type")
        );
    }
}
