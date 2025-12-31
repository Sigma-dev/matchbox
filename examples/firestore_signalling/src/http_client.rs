use std::fmt;

use matchbox_socket::SignalingError;
use serde::Serialize;

// ============================================================================
// Platform-specific HTTP implementations
// ============================================================================

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{Headers, Request, RequestInit, RequestMode, Response};

    #[derive(Clone)]
    pub struct HttpClient;

    impl HttpClient {
        pub fn new() -> Self {
            Self
        }

        pub async fn fetch<B: Serialize>(
            &self,
            method: &str,
            url: &str,
            body: Option<&B>,
        ) -> Result<serde_json::Value, SignalingError> {
            let window = web_sys::window().ok_or_else(|| {
                SignalingError::UserImplementationError("no global window".to_string())
            })?;

            let init = RequestInit::new();
            init.set_method(method);
            init.set_mode(RequestMode::Cors);

            if let Some(b) = body {
                let headers = Headers::new().map_err(to_user_error)?;
                headers
                    .set("Content-Type", "application/json")
                    .map_err(to_user_error)?;
                init.set_headers(&headers);
                let body_str = serde_json::to_string(b).map_err(to_user_error)?;
                init.set_body(&JsValue::from_str(&body_str));
            }

            let request = Request::new_with_str_and_init(url, &init).map_err(to_user_error)?;
            let resp_value = JsFuture::from(window.fetch_with_request(&request))
                .await
                .map_err(to_user_error)?;
            let resp: Response = resp_value.dyn_into().map_err(to_user_error)?;

            if !resp.ok() {
                let status = resp.status();
                let status_text = resp.status_text();
                let error_text = match JsFuture::from(resp.text().map_err(to_user_error)?).await {
                    Ok(js_text) => js_text
                        .as_string()
                        .unwrap_or_else(|| "no error details".to_string()),
                    Err(_) => "failed to read error body".to_string(),
                };

                web_sys::console::error_1(
                    &format!("Firestore error {} {}: {}", status, status_text, error_text).into(),
                );

                return Err(SignalingError::UserImplementationError(format!(
                    "HTTP {} {}: {}",
                    status, status_text, error_text
                )));
            }

            let json = JsFuture::from(resp.json().map_err(to_user_error)?)
                .await
                .map_err(to_user_error)?;
            let value: serde_json::Value =
                serde_wasm_bindgen::from_value(json).map_err(to_user_error)?;
            Ok(value)
        }
    }

    pub fn log_error(msg: &str) {
        web_sys::console::error_1(&msg.into());
    }

    pub fn log_warn(msg: &str) {
        web_sys::console::warn_1(&msg.into());
    }

    fn to_user_error<E: fmt::Debug>(e: E) -> SignalingError {
        SignalingError::UserImplementationError(format!("{e:#?}"))
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::*;
    use tracing::{error, warn};

    #[derive(Clone)]
    pub struct HttpClient {
        client: reqwest::Client,
    }

    impl HttpClient {
        pub fn new() -> Self {
            Self {
                client: reqwest::Client::new(),
            }
        }

        pub async fn fetch<B: Serialize>(
            &self,
            method: &str,
            url: &str,
            body: Option<&B>,
        ) -> Result<serde_json::Value, SignalingError> {
            let mut request = match method {
                "GET" => self.client.get(url),
                "POST" => self.client.post(url),
                "PATCH" => self.client.patch(url),
                "PUT" => self.client.put(url),
                "DELETE" => self.client.delete(url),
                _ => {
                    return Err(SignalingError::UserImplementationError(format!(
                        "Unsupported HTTP method: {}",
                        method
                    )));
                }
            };

            if let Some(b) = body {
                request = request.json(b);
            }

            let response = request.send().await.map_err(to_user_error)?;

            if !response.status().is_success() {
                let status = response.status();
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "no error details".to_string());

                error!("Firestore error {}: {}", status, error_text);

                return Err(SignalingError::UserImplementationError(format!(
                    "HTTP {}: {}",
                    status, error_text
                )));
            }

            let value = response.json().await.map_err(to_user_error)?;
            Ok(value)
        }
    }

    pub fn log_error(msg: &str) {
        error!("{}", msg);
    }

    pub fn log_warn(msg: &str) {
        warn!("{}", msg);
    }

    fn to_user_error<E: fmt::Debug>(e: E) -> SignalingError {
        SignalingError::UserImplementationError(format!("{e:#?}"))
    }
}

// Export the platform-specific implementation
#[cfg(target_arch = "wasm32")]
pub use wasm::*;

#[cfg(not(target_arch = "wasm32"))]
pub use native::*;
