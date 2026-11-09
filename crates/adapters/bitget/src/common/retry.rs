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

//! Bitget HTTP retry classification.

use crate::http::error::BitgetHttpError;

pub(crate) fn should_retry_http(error: &BitgetHttpError) -> bool {
    match error {
        BitgetHttpError::NetworkError(_) => true,
        BitgetHttpError::UnexpectedStatus { status, .. } => {
            is_retryable_status(*status) || *status >= 600
        }
        // 25004 "Operation too frequent", 40015 "System is abnormal, please try again later"
        BitgetHttpError::BitgetError { error_code, .. } => matches!(*error_code, 25004 | 40015),
        BitgetHttpError::MissingCredentials
        | BitgetHttpError::JsonError(_)
        | BitgetHttpError::ValidationError(_)
        | BitgetHttpError::BuildError(_)
        | BitgetHttpError::Canceled(_) => false,
    }
}

const fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500..=599)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(408, false)]
    #[case(425, false)]
    #[case(429, true)]
    #[case(500, true)]
    #[case(599, true)]
    #[case(600, true)]
    #[case(404, false)]
    fn http_status_classification(#[case] status: u16, #[case] expected: bool) {
        let error = BitgetHttpError::UnexpectedStatus {
            status,
            body: String::new(),
        };

        assert_eq!(should_retry_http(&error), expected);
    }

    #[rstest]
    #[case(25004, true)]
    #[case(40015, true)]
    #[case(25204, false)]
    #[case(40010, false)]
    fn http_error_code_classification(#[case] error_code: i32, #[case] expected: bool) {
        let error = BitgetHttpError::BitgetError {
            error_code,
            message: String::new(),
        };

        assert_eq!(should_retry_http(&error), expected);
    }

    #[rstest]
    #[case(BitgetHttpError::NetworkError(String::new()), true)]
    #[case(BitgetHttpError::MissingCredentials, false)]
    #[case(BitgetHttpError::Canceled(String::new()), false)]
    fn http_error_classification(#[case] error: BitgetHttpError, #[case] expected: bool) {
        assert_eq!(should_retry_http(&error), expected);
    }
}
