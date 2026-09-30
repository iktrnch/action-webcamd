//! Typed client for the legacy GoPro USB-webcam HTTP API.

use std::{net::Ipv4Addr, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;

const GOPRO_CONTROL_PORT: u16 = 80;
const GOPRO_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The fixed webcam settings supported by the first control milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WebcamConfiguration {
    resolution: WebcamResolution,
    fov: WebcamFov,
    udp_port: u16,
}

impl WebcamConfiguration {
    pub(crate) const INITIAL: Self = Self {
        resolution: WebcamResolution::P1080,
        fov: WebcamFov::Linear,
        udp_port: 8554,
    };

    pub(crate) const fn fov(self) -> WebcamFov {
        self.fov
    }

    pub(crate) const fn udp_port(self) -> u16 {
        self.udp_port
    }
}

/// GoPro's legacy webcam resolution values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WebcamResolution {
    P1080,
}

impl WebcamResolution {
    const fn api_value(self) -> u16 {
        match self {
            Self::P1080 => 1080,
        }
    }
}

/// GoPro's legacy webcam field-of-view values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WebcamFov {
    Linear,
}

impl WebcamFov {
    const fn api_value(self) -> u8 {
        match self {
            Self::Linear => 4,
        }
    }
}

/// A client tied to one GoPro USB-network address pair.
#[derive(Debug, Clone)]
pub(crate) struct GoProClient {
    client: Client,
    base_url: Url,
}

impl GoProClient {
    /// Creates a client that sends traffic through the selected GoPro USB
    /// network interface rather than an unrelated default route.
    pub(crate) fn new(host_address: Ipv4Addr, control_address: Ipv4Addr) -> Result<Self> {
        let client = Client::builder()
            .local_address(Some(host_address.into()))
            .no_proxy()
            .timeout(GOPRO_REQUEST_TIMEOUT)
            .build()
            .context("failed to build GoPro HTTP client")?;
        let base_url = Url::parse(&format!("http://{control_address}:{GOPRO_CONTROL_PORT}/"))
            .context("failed to construct GoPro control URL")?;

        Ok(Self { client, base_url })
    }

    #[cfg(test)]
    fn with_base_url(base_url: Url) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    /// Starts the UDP webcam stream with the requested resolution and port.
    pub(crate) async fn start_webcam(&self, configuration: WebcamConfiguration) -> Result<()> {
        self.request(
            "gp/gpWebcam/START",
            &[
                ("res", configuration.resolution.api_value().to_string()),
                ("port", configuration.udp_port.to_string()),
            ],
        )
        .await
        .context("GoPro webcam START request failed")
    }

    /// Applies the selected webcam field of view after webcam mode starts.
    pub(crate) async fn set_webcam_fov(&self, fov: WebcamFov) -> Result<()> {
        self.request(
            "gp/gpWebcam/SETTINGS",
            &[("fov", fov.api_value().to_string())],
        )
        .await
        .context("GoPro webcam FOV request failed")
    }

    /// Stops webcam mode and returns the camera to its normal USB-connected state.
    pub(crate) async fn stop_webcam(&self) -> Result<()> {
        self.request("gp/gpWebcam/STOP", &[])
            .await
            .context("GoPro webcam STOP request failed")
    }

    async fn request(&self, path: &str, query: &[(&'static str, String)]) -> Result<()> {
        let url = self.request_url(path, query)?;
        let response = self
            .client
            .get(url)
            .query(query)
            .send()
            .await
            .context("GoPro HTTP request failed")?;
        ensure_success_status(response.status())?;
        let response = response
            .json::<GoProResponse>()
            .await
            .context("GoPro returned an invalid control response")?;

        validate_response(response)
    }

    fn request_url(&self, path: &str, query: &[(&'static str, String)]) -> Result<Url> {
        let mut url = self
            .base_url
            .join(path)
            .with_context(|| format!("failed to construct GoPro request path {path}"))?;
        if !query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(query.iter().map(|(name, value)| (*name, value.as_str())));
        }
        Ok(url)
    }
}

fn ensure_success_status(status: StatusCode) -> Result<()> {
    if status.is_success() {
        Ok(())
    } else {
        bail!("GoPro HTTP response was unsuccessful: {status}")
    }
}

fn validate_response(response: GoProResponse) -> Result<()> {
    if response.error != 0 {
        bail!(
            "GoPro returned error {} with status {}",
            response.error,
            response.status
        );
    }
    Ok(())
}

/// Success and failure envelope returned by the legacy control API.
#[derive(Debug, Deserialize)]
struct GoProResponse {
    status: u32,
    error: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_the_legacy_webcam_urls_with_fixed_settings() {
        let base_url = Url::parse("http://172.27.187.51:80/").unwrap();
        let client = GoProClient::with_base_url(base_url);

        assert_eq!(
            client
                .request_url(
                    "gp/gpWebcam/START",
                    &[("res", "1080".to_owned()), ("port", "8554".to_owned())],
                )
                .unwrap()
                .as_str(),
            "http://172.27.187.51/gp/gpWebcam/START?res=1080&port=8554"
        );
        assert_eq!(
            client
                .request_url("gp/gpWebcam/SETTINGS", &[("fov", "4".to_owned())])
                .unwrap()
                .as_str(),
            "http://172.27.187.51/gp/gpWebcam/SETTINGS?fov=4"
        );
        assert_eq!(
            client
                .request_url("gp/gpWebcam/STOP", &[])
                .unwrap()
                .as_str(),
            "http://172.27.187.51/gp/gpWebcam/STOP"
        );
    }

    #[test]
    fn rejects_unsuccessful_http_and_gopro_error_responses() {
        assert!(ensure_success_status(StatusCode::SERVICE_UNAVAILABLE).is_err());
        let error = validate_response(GoProResponse {
            status: 0,
            error: 7,
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("error 7"));
    }

    #[test]
    fn rejects_malformed_control_responses() {
        assert!(serde_json::from_str::<GoProResponse>("not-json").is_err());
    }
}
