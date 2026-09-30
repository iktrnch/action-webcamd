use std::{
    fmt,
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};
use tokio_udev::Device;

use super::runtime::{
    attribute, attribute_or_property, closest_usb_driver, gopro_usb_parent, os_to_string,
    parse_hex_u16, property,
};

pub(super) const GOPRO_VENDOR_ID: u16 = 0x2672;
pub(super) const GOPRO_CONTROL_LAST_OCTET: u8 = 51;
pub(super) const GOPRO_CONTROL_PORT: u16 = 80;
pub(super) const GOPRO_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Describes where a camera observation originated so lifecycle logs can
/// distinguish initial discovery, live hotplug events, and recovery scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EventSource {
    Startup,
    Udev,
    Reconcile,
}

impl fmt::Display for EventSource {
    /// Formats the source as a stable lowercase value suitable for structured logs.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Startup => "startup",
            Self::Udev => "udev",
            Self::Reconcile => "reconcile",
        })
    }
}

/// An owned snapshot of the USB metadata used to identify one connected
/// GoPro and retain useful details after its sysfs entry disappears.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CameraIdentity {
    pub(super) usb_path: PathBuf,
    pub(super) physical_path: Option<String>,
    pub(super) vendor_id: u16,
    pub(super) product_id: Option<u16>,
    pub(super) manufacturer: Option<String>,
    pub(super) model: Option<String>,
    pub(super) serial: Option<String>,
    pub(super) bus_number: Option<u16>,
    pub(super) device_number: Option<u16>,
}

impl CameraIdentity {
    /// Builds a GoPro identity from a udev USB device, returning `None` when
    /// the device is missing a vendor ID or does not belong to GoPro.
    pub(super) fn from_device(device: &Device) -> Option<Self> {
        let vendor_id = attribute_or_property(device, "idVendor", "ID_VENDOR_ID")
            .and_then(|value| parse_hex_u16(&value))?;

        if vendor_id != GOPRO_VENDOR_ID {
            return None;
        }

        Some(Self {
            usb_path: device.syspath().to_path_buf(),
            physical_path: property(device, "ID_PATH"),
            vendor_id,
            product_id: attribute_or_property(device, "idProduct", "ID_MODEL_ID")
                .and_then(|value| parse_hex_u16(&value)),
            manufacturer: attribute_or_property(device, "manufacturer", "ID_VENDOR"),
            model: attribute_or_property(device, "product", "ID_MODEL"),
            serial: attribute_or_property(device, "serial", "ID_SERIAL_SHORT"),
            bus_number: attribute(device, "busnum").and_then(|value| value.parse().ok()),
            device_number: attribute(device, "devnum").and_then(|value| value.parse().ok()),
        })
    }

    /// Creates a minimal GoPro identity for tests that exercise missing
    /// optional udev metadata.
    #[cfg(test)]
    pub(super) fn unknown(path: PathBuf) -> Self {
        Self {
            usb_path: path,
            physical_path: None,
            vendor_id: GOPRO_VENDOR_ID,
            product_id: None,
            manufacturer: None,
            model: None,
            serial: None,
            bus_number: None,
            device_number: None,
        }
    }
}

/// An owned snapshot of a Linux network interface correlated to its parent
/// GoPro USB device through the udev device tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NetworkInterface {
    pub(super) sysfs_path: PathBuf,
    pub(super) name: String,
    pub(super) ifindex: u32,
    pub(super) mac_address: Option<String>,
    pub(super) driver: Option<String>,
    pub(super) usb_path: PathBuf,
}

impl NetworkInterface {
    /// Builds network-interface metadata when the device belongs to a GoPro
    /// USB ancestor, or returns `None` for unrelated interfaces.
    pub(super) fn from_device(device: &Device) -> Result<Option<Self>> {
        let Some(usb_parent) = gopro_usb_parent(device)? else {
            return Ok(None);
        };

        let driver = closest_usb_driver(device).or_else(|| property(device, "ID_NET_DRIVER"));

        Ok(Some(Self {
            sysfs_path: device.syspath().to_path_buf(),
            name: property(device, "INTERFACE").unwrap_or_else(|| os_to_string(device.sysname())),
            ifindex: property(device, "IFINDEX")
                .and_then(|value| value.parse().ok())
                .or_else(|| attribute(device, "ifindex").and_then(|value| value.parse().ok()))
                .context("GoPro network interface is missing its kernel index")?,
            mac_address: attribute(device, "address"),
            driver,
            usb_path: usb_parent.syspath().to_path_buf(),
        }))
    }
}

/// The host and GoPro control addresses derived from the selected USB network.
/// GoPro's legacy USB webcam API listens on the `.51` peer address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ControlEndpoint {
    pub(super) host_address: Ipv4Addr,
    pub(super) control_address: Ipv4Addr,
}

impl ControlEndpoint {
    pub(super) fn from_host_address(host_address: Ipv4Addr) -> Self {
        let [first, second, third, _] = host_address.octets();
        Self {
            host_address,
            control_address: Ipv4Addr::new(first, second, third, GOPRO_CONTROL_LAST_OCTET),
        }
    }

    pub(super) fn socket_address(self) -> SocketAddrV4 {
        SocketAddrV4::new(self.control_address, GOPRO_CONTROL_PORT)
    }
}

/// A normalized device observation produced from either udev monitoring or a
/// sysfs enumeration and consumed by the lifecycle state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Observation {
    UsbUpsert {
        camera: CameraIdentity,
        source: EventSource,
    },
    UsbRemoved {
        usb_path: PathBuf,
    },
    NetworkUpsert(NetworkInterface),
    NetworkRemoved {
        sysfs_path: PathBuf,
        name: Option<String>,
        ifindex: Option<u32>,
        usb_path: Option<PathBuf>,
    },
    NetworkAddressesChanged {
        ifindex: u32,
        addresses: Vec<Ipv4Addr>,
    },
    ApiProbeSucceeded {
        ifindex: u32,
        endpoint: ControlEndpoint,
    },
    ApiProbeFailed {
        ifindex: u32,
        endpoint: ControlEndpoint,
        error: String,
    },
}

/// The explicit lifecycle of the selected GoPro session. Network and API
/// readiness are distinct because a USB network interface can appear before
/// the kernel assigns its IPv4 address or the camera opens its control port.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum CameraState {
    #[default]
    Disconnected,
    DeviceDetected {
        camera: CameraIdentity,
    },
    WaitingForNetwork {
        camera: CameraIdentity,
    },
    WaitingForAddress {
        camera: CameraIdentity,
        network: NetworkInterface,
    },
    WaitingForApi {
        camera: CameraIdentity,
        network: NetworkInterface,
        endpoint: ControlEndpoint,
    },
    Ready {
        camera: CameraIdentity,
        network: NetworkInterface,
        endpoint: ControlEndpoint,
    },
}

impl CameraState {
    /// Returns the stable, data-free name used in transition records and logs.
    pub(super) fn kind(&self) -> CameraStateKind {
        match self {
            Self::Disconnected => CameraStateKind::Disconnected,
            Self::DeviceDetected { .. } => CameraStateKind::DeviceDetected,
            Self::WaitingForNetwork { .. } => CameraStateKind::WaitingForNetwork,
            Self::WaitingForAddress { .. } => CameraStateKind::WaitingForAddress,
            Self::WaitingForApi { .. } => CameraStateKind::WaitingForApi,
            Self::Ready { .. } => CameraStateKind::Ready,
        }
    }

    /// Returns the selected physical camera, if a session exists.
    pub(super) fn camera(&self) -> Option<&CameraIdentity> {
        match self {
            Self::Disconnected => None,
            Self::DeviceDetected { camera }
            | Self::WaitingForNetwork { camera }
            | Self::WaitingForAddress { camera, .. }
            | Self::WaitingForApi { camera, .. }
            | Self::Ready { camera, .. } => Some(camera),
        }
    }

    /// Returns the selected USB-network interface once it has been observed.
    pub(super) fn network(&self) -> Option<&NetworkInterface> {
        match self {
            Self::WaitingForAddress { network, .. }
            | Self::WaitingForApi { network, .. }
            | Self::Ready { network, .. } => Some(network),
            Self::Disconnected | Self::DeviceDetected { .. } | Self::WaitingForNetwork { .. } => {
                None
            }
        }
    }

    /// Returns the endpoint while its connectivity is being checked or has
    /// already been confirmed.
    pub(super) fn endpoint(&self) -> Option<ControlEndpoint> {
        match self {
            Self::WaitingForApi { endpoint, .. } | Self::Ready { endpoint, .. } => Some(*endpoint),
            Self::Disconnected
            | Self::DeviceDetected { .. }
            | Self::WaitingForNetwork { .. }
            | Self::WaitingForAddress { .. } => None,
        }
    }

    /// Refreshes USB metadata while preserving the current lifecycle phase.
    pub(super) fn replace_camera(&mut self, camera: CameraIdentity) {
        match self {
            Self::Disconnected => {}
            Self::DeviceDetected {
                camera: current_camera,
            }
            | Self::WaitingForNetwork {
                camera: current_camera,
            }
            | Self::WaitingForAddress {
                camera: current_camera,
                ..
            }
            | Self::WaitingForApi {
                camera: current_camera,
                ..
            }
            | Self::Ready {
                camera: current_camera,
                ..
            } => *current_camera = camera,
        }
    }

    /// Refreshes network metadata while preserving the address/API phase.
    pub(super) fn replace_network(&mut self, network: NetworkInterface) {
        match self {
            Self::WaitingForAddress {
                network: current_network,
                ..
            }
            | Self::WaitingForApi {
                network: current_network,
                ..
            }
            | Self::Ready {
                network: current_network,
                ..
            } => *current_network = network,
            Self::Disconnected | Self::DeviceDetected { .. } | Self::WaitingForNetwork { .. } => {}
        }
    }
}

/// Data-free state names make transition assertions and structured logs stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CameraStateKind {
    Disconnected,
    DeviceDetected,
    WaitingForNetwork,
    WaitingForAddress,
    WaitingForApi,
    Ready,
}

impl fmt::Display for CameraStateKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Disconnected => "disconnected",
            Self::DeviceDetected => "device_detected",
            Self::WaitingForNetwork => "waiting_for_network",
            Self::WaitingForAddress => "waiting_for_address",
            Self::WaitingForApi => "waiting_for_api",
            Self::Ready => "ready",
        })
    }
}

/// A semantic transition emitted by the lifecycle reducer for output control
/// and structured logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LifecycleEvent {
    StateTransition {
        from: CameraStateKind,
        to: CameraStateKind,
        camera: Option<CameraIdentity>,
        network: Option<NetworkInterface>,
        endpoint: Option<ControlEndpoint>,
        source: Option<EventSource>,
    },
    CameraIgnored(CameraIdentity),
    NetworkUpdated {
        previous: NetworkInterface,
        current: NetworkInterface,
    },
    ApiProbeFailed {
        endpoint: ControlEndpoint,
        error: String,
    },
}
