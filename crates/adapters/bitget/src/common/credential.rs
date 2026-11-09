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

//! Bitget API credential resolution, storage, and request signing.

#![allow(unused_assignments)] // Fields are used in methods, false positive from nightly

use std::fmt::Debug;

use aws_lc_rs::hmac;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use nautilus_core::{env::get_or_env_var_opt, string::secret::REDACTED};
use zeroize::ZeroizeOnDrop;

use crate::common::{consts::BITGET_WS_LOGIN_PATH, enums::BitgetEnvironment};

/// Returns the environment variable names for API credentials (key, secret, passphrase),
/// based on the trading environment.
///
/// Bitget has no testnet, so `Testnet` uses the demo variables.
#[must_use]
pub fn credential_env_vars(
    environment: BitgetEnvironment,
) -> (&'static str, &'static str, &'static str) {
    match environment {
        BitgetEnvironment::Demo | BitgetEnvironment::Testnet => (
            "BITGET_DEMO_API_KEY",
            "BITGET_DEMO_API_SECRET",
            "BITGET_DEMO_API_PASSPHRASE",
        ),
        BitgetEnvironment::Mainnet => (
            "BITGET_API_KEY",
            "BITGET_API_SECRET",
            "BITGET_API_PASSPHRASE",
        ),
    }
}

/// API credentials required for signing Bitget REST requests and the WebSocket login.
///
/// Bitget keys carry a passphrase chosen at key creation, sent as `ACCESS-PASSPHRASE`.
#[derive(Clone, ZeroizeOnDrop)]
pub struct Credential {
    api_key: Box<str>,
    api_secret: Box<[u8]>,
    api_passphrase: Box<str>,
}

impl Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Credential))
            .field("api_key", &REDACTED)
            .field("api_secret", &REDACTED)
            .field("api_passphrase", &REDACTED)
            .finish()
    }
}

impl Credential {
    /// Resolves credentials from provided values or environment variables.
    ///
    /// Each of `api_key`, `api_secret` and `api_passphrase` falls back to its environment
    /// variable; returns `None` unless all three resolve.
    #[must_use]
    pub fn resolve(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        environment: BitgetEnvironment,
    ) -> Option<Self> {
        let (key_var, secret_var, passphrase_var) = credential_env_vars(environment);
        Some(Self::new(
            get_or_env_var_opt(api_key, key_var)?,
            get_or_env_var_opt(api_secret, secret_var)?,
            get_or_env_var_opt(api_passphrase, passphrase_var)?,
        ))
    }

    /// Creates a new [`Credential`] instance from the API key, secret and passphrase.
    #[must_use]
    pub fn new(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        api_passphrase: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into().into_boxed_str(),
            api_secret: api_secret.into().into_bytes().into_boxed_slice(),
            api_passphrase: api_passphrase.into().into_boxed_str(),
        }
    }

    /// Returns the API key associated with this credential.
    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Returns the API passphrase associated with this credential.
    #[must_use]
    pub fn api_passphrase(&self) -> &str {
        &self.api_passphrase
    }

    /// Returns a masked version of the API key for logging purposes.
    ///
    /// Shows first 4 and last 4 characters with ellipsis in between.
    /// For keys shorter than 8 characters, shows asterisks only.
    #[must_use]
    pub fn api_key_masked(&self) -> String {
        nautilus_core::string::secret::mask_api_key(&self.api_key)
    }

    /// Produces the Bitget WebSocket login signature for the provided millisecond timestamp.
    ///
    /// Signs `timestamp + "GET" + "/user/verify"`; `timestamp` must be the value sent in the
    /// login payload (the venue accepts seconds or milliseconds; the adapter sends milliseconds).
    #[must_use]
    pub fn sign_websocket_auth(&self, timestamp: i64) -> String {
        self.sign_with_payload(&timestamp.to_string(), "GET", BITGET_WS_LOGIN_PATH, None)
    }

    /// Produces the Bitget `ACCESS-SIGN` value: `Base64(HMAC-SHA256(secret, prehash))` with
    /// `prehash = timestamp + METHOD + request_path + body`.
    ///
    /// `request_path` includes `?query` when present (unencoded, as sent). `payload` is the
    /// JSON body for POST requests; it must be byte-identical to the body sent.
    #[must_use]
    pub fn sign_with_payload(
        &self,
        timestamp: &str,
        method: &str,
        request_path: &str,
        payload: Option<&str>,
    ) -> String {
        let payload_len = payload.map_or(0usize, str::len);
        let mut message = String::with_capacity(
            timestamp.len() + method.len() + request_path.len() + payload_len,
        );

        message.push_str(timestamp);
        message.push_str(method);
        message.push_str(request_path);

        if let Some(payload) = payload {
            message.push_str(payload);
        }

        let key = hmac::Key::new(hmac::HMAC_SHA256, &self.api_secret);
        let tag = hmac::sign(&key, message.as_bytes());
        BASE64_STANDARD.encode(tag.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const API_KEY: &str = "test_api_key";
    const API_SECRET: &str = "test_secret";
    const API_PASSPHRASE: &str = "test_passphrase";
    const TIMESTAMP: &str = "16273667805456";

    // Expected values computed with the Python sample from the Bitget quick-start:
    // base64.b64encode(hmac.new(secret.encode(), prehash.encode(), 'sha256').digest())
    #[rstest]
    fn sign_with_payload_matches_reference_get() {
        let credential = Credential::new(API_KEY, API_SECRET, API_PASSPHRASE);
        let path = "/api/v3/account/fee-rate?category=SPOT&symbol=BTCUSDT";

        let signature = credential.sign_with_payload(TIMESTAMP, "GET", path, None);

        assert_eq!(signature, "4hhzzxsi/PNT7iNhqhouYxLYLCLxG7yrY3eMhmeYQdY=");
    }

    #[rstest]
    fn sign_with_payload_matches_reference_post() {
        let credential = Credential::new(API_KEY, API_SECRET, API_PASSPHRASE);
        let body = r#"{"category":"SPOT","symbol":"BTCUSDT","side":"buy","orderType":"limit","qty":"0.1","price":"30000","timeInForce":"gtc"}"#;

        let signature = credential.sign_with_payload(
            TIMESTAMP,
            "POST",
            "/api/v3/trade/place-order",
            Some(body),
        );

        assert_eq!(signature, "BHUr17oJOJ29n7ikT+CRfI38AQKsjqxBLepiXP99leI=");
    }

    #[rstest]
    fn sign_with_empty_payload_omits_tail() {
        let credential = Credential::new(API_KEY, API_SECRET, API_PASSPHRASE);

        let signature = credential.sign_with_payload(TIMESTAMP, "GET", "/api/v3/market/time", None);

        let expected =
            credential.sign_with_payload(TIMESTAMP, "GET", "/api/v3/market/time", Some(""));
        assert_eq!(signature, expected);
    }

    #[rstest]
    fn sign_websocket_auth_matches_reference() {
        let credential = Credential::new(API_KEY, API_SECRET, API_PASSPHRASE);

        let signature = credential.sign_websocket_auth(1_538_054_050);

        assert_eq!(signature, "ZKcxhODLTztM2iNiV/trAzhDaeE9+T9LNddmARBPvj8=");
    }

    #[rstest]
    fn test_debug_redacts_secret() {
        let credential = Credential::new(API_KEY, API_SECRET, API_PASSPHRASE);
        let dbg_out = format!("{credential:?}");

        assert_eq!(dbg_out.matches(REDACTED).count(), 3);
        assert!(!dbg_out.contains(API_KEY));
        assert!(!dbg_out.contains(API_SECRET));
        assert!(!dbg_out.contains(API_PASSPHRASE));
    }

    #[rstest]
    fn test_resolve_with_all_args() {
        let result = Credential::resolve(
            Some("my_key".to_string()),
            Some("my_secret".to_string()),
            Some("my_passphrase".to_string()),
            BitgetEnvironment::Mainnet,
        );

        let credential = result.unwrap();
        assert_eq!(credential.api_key(), "my_key");
        assert_eq!(credential.api_passphrase(), "my_passphrase");
    }

    #[rstest]
    fn test_resolve_with_no_args_no_env() {
        let (key_var, secret_var, passphrase_var) = credential_env_vars(BitgetEnvironment::Mainnet);
        if std::env::var(key_var).is_ok()
            || std::env::var(secret_var).is_ok()
            || std::env::var(passphrase_var).is_ok()
        {
            return;
        }

        let result = Credential::resolve(None, None, None, BitgetEnvironment::Mainnet);

        assert!(result.is_none());
    }
}
