use std::{
    collections::HashSet,
    ffi::OsStr,
    fmt,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, future};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::{
    net::TcpSocket,
    signal::unix::{SignalKind, signal},
    time::timeout,
};
use tokio_udev::{AsyncMonitorSocket, Device, Enumerator, Event, EventType, MonitorBuilder};
use tracing::{debug, info, warn};

use crate::{
    network::AddressMonitor,
    virtual_camera::{CameraOutput, VirtualCamera},
};

const GOPRO_VENDOR_ID: u16 = 0x2672;
const GOPRO_CONTROL_LAST_OCTET: u8 = 51;
const GOPRO_CONTROL_PORT: u16 = 80;
const GOPRO_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Describes where a camera observation originated so lifecycle logs can
/// distinguish initial discovery, live hotplug events, and recovery scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventSource {
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
struct CameraIdentity {
    usb_path: PathBuf,
    physical_path: Option<String>,
    vendor_id: u16,
    product_id: Option<u16>,
    manufacturer: Option<String>,
    model: Option<String>,
    serial: Option<String>,
    bus_number: Option<u16>,
    device_number: Option<u16>,
}

impl CameraIdentity {
    /// Builds a GoPro identity from a udev USB device, returning `None` when
    /// the device is missing a vendor ID or does not belong to GoPro.
    fn from_device(device: &Device) -> Option<Self> {
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
    fn unknown(path: PathBuf) -> Self {
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
struct NetworkInterface {
    sysfs_path: PathBuf,
    name: String,
    ifindex: u32,
    mac_address: Option<String>,
    driver: Option<String>,
    usb_path: PathBuf,
}

impl NetworkInterface {
    /// Builds network-interface metadata when the device belongs to a GoPro
    /// USB ancestor, or returns `None` for unrelated interfaces.
    fn from_device(device: &Device) -> Result<Option<Self>> {
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
struct ControlEndpoint {
    host_address: Ipv4Addr,
    control_address: Ipv4Addr,
}

impl ControlEndpoint {
    fn from_host_address(host_address: Ipv4Addr) -> Self {
        let [first, second, third, _] = host_address.octets();
        Self {
            host_address,
            control_address: Ipv4Addr::new(first, second, third, GOPRO_CONTROL_LAST_OCTET),
        }
    }

    fn socket_address(self) -> SocketAddrV4 {
        SocketAddrV4::new(self.control_address, GOPRO_CONTROL_PORT)
    }
}

/// A normalized device observation produced from either udev monitoring or a
/// sysfs enumeration and consumed by the lifecycle state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Observation {
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
enum CameraState {
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
    fn kind(&self) -> CameraStateKind {
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
    fn camera(&self) -> Option<&CameraIdentity> {
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
    fn network(&self) -> Option<&NetworkInterface> {
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
    fn endpoint(&self) -> Option<ControlEndpoint> {
        match self {
            Self::WaitingForApi { endpoint, .. } | Self::Ready { endpoint, .. } => Some(*endpoint),
            Self::Disconnected
            | Self::DeviceDetected { .. }
            | Self::WaitingForNetwork { .. }
            | Self::WaitingForAddress { .. } => None,
        }
    }

    /// Refreshes USB metadata while preserving the current lifecycle phase.
    fn replace_camera(&mut self, camera: CameraIdentity) {
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
    fn replace_network(&mut self, network: NetworkInterface) {
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
enum CameraStateKind {
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
enum LifecycleEvent {
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

/// Reduces normalized observations into idempotent camera lifecycle events and
/// tracks additional GoPros that are ignored while one camera is active.
#[derive(Debug, Default)]
struct Lifecycle {
    state: CameraState,
    ignored: HashSet<PathBuf>,
}

type ApiProbeFuture = Pin<Box<dyn Future<Output = ApiProbeResult>>>;

#[derive(Debug)]
struct ApiProbeResult {
    ifindex: u32,
    endpoint: ControlEndpoint,
    result: Result<(), String>,
}

impl Lifecycle {
    /// Applies one normalized observation and returns only the semantic state
    /// transitions caused by it.
    fn apply(&mut self, observation: Observation) -> Vec<LifecycleEvent> {
        match observation {
            Observation::UsbUpsert { camera, source } => self.upsert_camera(camera, source),
            Observation::UsbRemoved { usb_path } => self.remove_camera(&usb_path),
            Observation::NetworkUpsert(network) => self.upsert_network(network),
            Observation::NetworkRemoved {
                sysfs_path,
                name,
                ifindex,
                usb_path,
            } => self.remove_network(&sysfs_path, name.as_deref(), ifindex, usb_path.as_deref()),
            Observation::NetworkAddressesChanged { ifindex, addresses } => {
                self.update_network_addresses(ifindex, addresses)
            }
            Observation::ApiProbeSucceeded { ifindex, endpoint } => {
                self.api_probe_succeeded(ifindex, endpoint)
            }
            Observation::ApiProbeFailed {
                ifindex,
                endpoint,
                error,
            } => self.api_probe_failed(ifindex, endpoint, error),
        }
    }

    /// Selects a newly observed GoPro, refreshes the active camera's metadata,
    /// or records an additional camera as ignored.
    fn upsert_camera(
        &mut self,
        camera: CameraIdentity,
        source: EventSource,
    ) -> Vec<LifecycleEvent> {
        if let Some(active_camera) = self.state.camera() {
            if active_camera.usb_path == camera.usb_path {
                self.state.replace_camera(camera);
                return Vec::new();
            }

            if self.ignored.insert(camera.usb_path.clone()) {
                return vec![LifecycleEvent::CameraIgnored(camera)];
            }

            return Vec::new();
        }

        self.ignored.remove(&camera.usb_path);
        vec![
            self.transition(
                CameraState::DeviceDetected {
                    camera: camera.clone(),
                },
                Some(source),
            ),
            self.transition(CameraState::WaitingForNetwork { camera }, Some(source)),
        ]
    }

    /// Ends the selected session when its physical USB device disappears.
    fn remove_camera(&mut self, usb_path: &Path) -> Vec<LifecycleEvent> {
        self.ignored.remove(usb_path);

        if self
            .state
            .camera()
            .is_none_or(|camera| camera.usb_path != usb_path)
        {
            return Vec::new();
        }

        vec![self.transition(CameraState::Disconnected, None)]
    }

    /// Records the selected GoPro USB network interface, then waits for its
    /// IPv4 address through netlink before any API check begins.
    fn upsert_network(&mut self, network: NetworkInterface) -> Vec<LifecycleEvent> {
        let Some(active_camera) = self.state.camera() else {
            return Vec::new();
        };

        if active_camera.usb_path != network.usb_path {
            return Vec::new();
        }
        let active_camera = active_camera.clone();

        match &self.state {
            CameraState::DeviceDetected { .. } | CameraState::WaitingForNetwork { .. } => {
                vec![self.transition(
                    CameraState::WaitingForAddress {
                        camera: active_camera,
                        network,
                    },
                    None,
                )]
            }
            CameraState::WaitingForAddress {
                network: previous, ..
            }
            | CameraState::WaitingForApi {
                network: previous, ..
            }
            | CameraState::Ready {
                network: previous, ..
            } => {
                if previous == &network {
                    return Vec::new();
                }
                let previous = previous.clone();
                let ifindex_changed = previous.ifindex != network.ifindex;
                self.state.replace_network(network.clone());
                let mut events = vec![LifecycleEvent::NetworkUpdated {
                    previous,
                    current: network.clone(),
                }];
                if ifindex_changed {
                    events.push(self.transition(
                        CameraState::WaitingForAddress {
                            camera: active_camera,
                            network,
                        },
                        None,
                    ));
                }
                events
            }
            CameraState::Disconnected => Vec::new(),
        }
    }

    /// Returns to waiting for the selected camera's USB network without
    /// stopping black output; physical USB removal remains the session boundary.
    fn remove_network(
        &mut self,
        sysfs_path: &Path,
        name: Option<&str>,
        ifindex: Option<u32>,
        usb_path: Option<&Path>,
    ) -> Vec<LifecycleEvent> {
        let Some(current) = self.state.network() else {
            return Vec::new();
        };

        let same_path = current.sysfs_path == sysfs_path;
        let same_ifindex = ifindex == Some(current.ifindex);
        let same_name_and_parent = name.is_some_and(|value| current.name == value)
            && usb_path.is_some_and(|value| current.usb_path == value);

        if !same_path && !same_ifindex && !same_name_and_parent {
            return Vec::new();
        }

        let Some(camera) = self.state.camera().cloned() else {
            return Vec::new();
        };
        vec![self.transition(CameraState::WaitingForNetwork { camera }, None)]
    }

    /// Replaces the active interface's IPv4 inventory and requests exactly one
    /// asynchronous TCP probe whenever a usable host address changes.
    fn update_network_addresses(
        &mut self,
        ifindex: u32,
        mut addresses: Vec<Ipv4Addr>,
    ) -> Vec<LifecycleEvent> {
        let Some(network) = self.state.network() else {
            return Vec::new();
        };
        if network.ifindex != ifindex {
            return Vec::new();
        }
        addresses.sort_unstable();
        let address = addresses.into_iter().next();
        let Some(camera) = self.state.camera().cloned() else {
            return Vec::new();
        };
        let network = network.clone();

        let Some(host_address) = address else {
            return match self.state.kind() {
                CameraStateKind::WaitingForAddress => Vec::new(),
                _ => {
                    vec![self.transition(CameraState::WaitingForAddress { camera, network }, None)]
                }
            };
        };
        let endpoint = ControlEndpoint::from_host_address(host_address);
        if self.state.endpoint() == Some(endpoint)
            && self.state.kind() == CameraStateKind::WaitingForApi
        {
            return Vec::new();
        }
        vec![self.transition(
            CameraState::WaitingForApi {
                camera,
                network,
                endpoint,
            },
            None,
        )]
    }

    /// Marks an in-state TCP probe as ready and ignores stale completions.
    fn api_probe_succeeded(
        &mut self,
        ifindex: u32,
        endpoint: ControlEndpoint,
    ) -> Vec<LifecycleEvent> {
        let CameraState::WaitingForApi {
            camera,
            network,
            endpoint: current_endpoint,
        } = &self.state
        else {
            return Vec::new();
        };
        if network.ifindex != ifindex || *current_endpoint != endpoint {
            return Vec::new();
        }
        vec![self.transition(
            CameraState::Ready {
                camera: camera.clone(),
                network: network.clone(),
                endpoint,
            },
            None,
        )]
    }

    /// Keeps API failures recoverable: the next relevant netlink address event
    /// will request a fresh probe, without an arbitrary retry loop.
    fn api_probe_failed(
        &mut self,
        ifindex: u32,
        endpoint: ControlEndpoint,
        error: String,
    ) -> Vec<LifecycleEvent> {
        let Some(network) = self.state.network() else {
            return Vec::new();
        };
        if self.state.kind() != CameraStateKind::WaitingForApi
            || network.ifindex != ifindex
            || self.state.endpoint() != Some(endpoint)
        {
            return Vec::new();
        }
        vec![LifecycleEvent::ApiProbeFailed { endpoint, error }]
    }

    /// Records a state transition after retaining snapshots useful after unplug.
    fn transition(&mut self, next: CameraState, source: Option<EventSource>) -> LifecycleEvent {
        let from = self.state.kind();
        let to = next.kind();
        let camera = next.camera().or_else(|| self.state.camera()).cloned();
        let network = next.network().or_else(|| self.state.network()).cloned();
        let endpoint = next.endpoint().or_else(|| self.state.endpoint());
        self.state = next;
        LifecycleEvent::StateTransition {
            from,
            to,
            camera,
            network,
            endpoint,
            source,
        }
    }

    /// Returns the selected physical camera while a session exists.
    fn selected_camera(&self) -> Option<&CameraIdentity> {
        self.state.camera()
    }

    /// Returns the selected network interface once udev has correlated it.
    fn selected_network(&self) -> Option<&NetworkInterface> {
        self.state.network()
    }

    /// Reports whether the daemon currently has a selected GoPro session.
    fn has_active_camera(&self) -> bool {
        self.selected_camera().is_some()
    }
}

/// Runs startup discovery and both udev event streams until shutdown or an
/// unrecoverable monitoring failure occurs.
pub(crate) async fn run(
    mut virtual_camera: VirtualCamera,
    mut producer_failures: UnboundedReceiver<anyhow::Error>,
) -> Result<()> {
    let mut usb_monitor = monitor_usb().context("failed to open USB udev monitor")?;
    let mut network_monitor = monitor_network().context("failed to open network udev monitor")?;
    let mut address_monitor = AddressMonitor::open()?;
    let mut lifecycle = Lifecycle::default();
    let mut api_probe = None;

    reconcile(
        &mut lifecycle,
        EventSource::Startup,
        &mut virtual_camera,
        &address_monitor,
        &mut api_probe,
    )
    .await
    .context("failed to enumerate devices during startup")?;

    info!(event = "hotplug_monitor_started", "hotplug monitor started");

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            result = &mut shutdown => {
                result?;
                info!(event = "daemon_stopping", "shutdown signal received");
                virtual_camera.stop().context("failed to stop virtual camera during shutdown")?;
                return Ok(());
            }
            failure = producer_failures.recv() => {
                let Some(error) = failure else {
                    bail!("virtual camera producer failure channel closed unexpectedly");
                };
                virtual_camera.stop().context("failed to stop failed virtual camera producer")?;
                return Err(error);
            }
            item = usb_monitor.next() => {
                match item {
                    Some(Ok(event)) => handle_usb_event(&mut lifecycle, &event, &mut virtual_camera, &address_monitor, &mut api_probe).await?,
                    Some(Err(error)) => {
                        warn!(%error, event = "udev_receive_error", subsystem = "usb", "failed to receive USB udev event; reconciling");
                        reconcile(&mut lifecycle, EventSource::Reconcile, &mut virtual_camera, &address_monitor, &mut api_probe).await?;
                    }
                    None => bail!("USB udev monitor ended unexpectedly"),
                }
            }
            item = network_monitor.next() => {
                match item {
                    Some(Ok(event)) => handle_network_event(&mut lifecycle, &event, &mut virtual_camera, &address_monitor, &mut api_probe).await?,
                    Some(Err(error)) => {
                        warn!(%error, event = "udev_receive_error", subsystem = "net", "failed to receive network udev event; reconciling");
                        reconcile(&mut lifecycle, EventSource::Reconcile, &mut virtual_camera, &address_monitor, &mut api_probe).await?;
                    }
                    None => bail!("network udev monitor ended unexpectedly"),
                }
            }
            change = address_monitor.next_change() => {
                let change = change?;
                if lifecycle.selected_network().is_some_and(|network| network.ifindex == change.ifindex) {
                    debug!(event = "gopro_ipv4_address_changed", ifindex = change.ifindex, address = %change.address, "refreshing GoPro USB-network addresses");
                    refresh_selected_addresses(&mut lifecycle, &mut virtual_camera, &address_monitor, &mut api_probe).await?;
                }
            }
            result = next_api_probe(&mut api_probe) => {
                api_probe = None;
                let observation = match result.result {
                    Ok(()) => Observation::ApiProbeSucceeded { ifindex: result.ifindex, endpoint: result.endpoint },
                    Err(error) => Observation::ApiProbeFailed { ifindex: result.ifindex, endpoint: result.endpoint, error },
                };
                let events = lifecycle.apply(observation);
                dispatch_runtime(&lifecycle, events, &mut virtual_camera, &mut api_probe)?;
            }
        }
    }
}

/// Opens an asynchronous udev monitor restricted to physical USB devices.
fn monitor_usb() -> Result<AsyncMonitorSocket> {
    let socket = MonitorBuilder::new()?
        .match_subsystem_devtype("usb", "usb_device")?
        .listen()?;
    Ok(AsyncMonitorSocket::new(socket)?)
}

/// Opens an asynchronous udev monitor for Linux network-interface events.
fn monitor_network() -> Result<AsyncMonitorSocket> {
    let socket = MonitorBuilder::new()?.match_subsystem("net")?.listen()?;
    Ok(AsyncMonitorSocket::new(socket)?)
}

/// Converts one USB udev event into a lifecycle observation and reconciles
/// remaining cameras after the active camera disconnects.
async fn handle_usb_event(
    lifecycle: &mut Lifecycle,
    event: &Event,
    virtual_camera: &mut VirtualCamera,
    address_monitor: &AddressMonitor,
    api_probe: &mut Option<ApiProbeFuture>,
) -> Result<()> {
    let usb_path = event.syspath().to_path_buf();
    let observation = match event.event_type() {
        EventType::Add | EventType::Bind | EventType::Change => {
            let Some(camera) = CameraIdentity::from_device(event) else {
                return Ok(());
            };
            Observation::UsbUpsert {
                camera,
                source: EventSource::Udev,
            }
        }
        EventType::Remove | EventType::Unbind => Observation::UsbRemoved { usb_path },
        EventType::Unknown => {
            debug!(path = %usb_path.display(), event = "unknown_udev_action", subsystem = "usb", "ignoring unknown udev action");
            return Ok(());
        }
    };

    let active_was_removed = matches!(&observation, Observation::UsbRemoved { usb_path } if lifecycle.selected_camera().is_some_and(|camera| camera.usb_path == *usb_path));
    let events = lifecycle.apply(observation);
    dispatch_runtime(lifecycle, events, virtual_camera, api_probe)?;

    if active_was_removed && !lifecycle.has_active_camera() {
        reconcile(
            lifecycle,
            EventSource::Reconcile,
            virtual_camera,
            address_monitor,
            api_probe,
        )
        .await?;
    }

    Ok(())
}

/// Converts one network udev event into lifecycle observations, ensuring the
/// owning USB camera is observed before its interface.
async fn handle_network_event(
    lifecycle: &mut Lifecycle,
    event: &Event,
    virtual_camera: &mut VirtualCamera,
    address_monitor: &AddressMonitor,
    api_probe: &mut Option<ApiProbeFuture>,
) -> Result<()> {
    let event_type = event.event_type();
    if matches!(
        event_type,
        EventType::Add | EventType::Bind | EventType::Change
    ) || property(event, "ACTION").as_deref() == Some("move")
    {
        let Some(network) = NetworkInterface::from_device(event)? else {
            return Ok(());
        };

        // Separate udev sockets do not guarantee cross-subsystem ordering.
        // Build the owning camera from the net device's parent before the
        // interface observation so a net event can safely arrive first.
        if let Some(parent) = gopro_usb_parent(event)?
            && let Some(camera) = CameraIdentity::from_device(&parent)
        {
            let events = lifecycle.apply(Observation::UsbUpsert {
                camera,
                source: EventSource::Udev,
            });
            dispatch_runtime(lifecycle, events, virtual_camera, api_probe)?;
        }
        let events = lifecycle.apply(Observation::NetworkUpsert(network));
        dispatch_runtime(lifecycle, events, virtual_camera, api_probe)?;
        refresh_selected_addresses(lifecycle, virtual_camera, address_monitor, api_probe).await?;
        return Ok(());
    }

    if matches!(event_type, EventType::Remove | EventType::Unbind) {
        let usb_path = gopro_usb_parent(event)?
            .map(|parent| parent.syspath().to_path_buf())
            .or_else(|| {
                lifecycle
                    .selected_camera()
                    .map(|camera| camera.usb_path.clone())
            });
        let observation = Observation::NetworkRemoved {
            sysfs_path: event.syspath().to_path_buf(),
            name: property(event, "INTERFACE").or_else(|| Some(os_to_string(event.sysname()))),
            ifindex: property(event, "IFINDEX").and_then(|value| value.parse().ok()),
            usb_path,
        };
        let events = lifecycle.apply(observation);
        dispatch_runtime(lifecycle, events, virtual_camera, api_probe)?;
        return Ok(());
    }

    debug!(path = %event.syspath().display(), event = "unknown_udev_action", subsystem = "net", "ignoring unknown udev action");

    Ok(())
}

/// Rebuilds lifecycle state from the current sysfs inventory to cover startup,
/// missed events, and promotion of an already-connected secondary camera.
async fn reconcile(
    lifecycle: &mut Lifecycle,
    source: EventSource,
    virtual_camera: &mut VirtualCamera,
    address_monitor: &AddressMonitor,
    api_probe: &mut Option<ApiProbeFuture>,
) -> Result<()> {
    let cameras = enumerate_usb_cameras()?;
    let networks = enumerate_gopro_networks()?;
    reconcile_inventory(lifecycle, source, cameras, networks, virtual_camera)?;
    if lifecycle.state.kind() != CameraStateKind::WaitingForApi {
        *api_probe = None;
    }
    refresh_selected_addresses(lifecycle, virtual_camera, address_monitor, api_probe).await
}

/// Reconciles the selected interface's address inventory after udev discovery
/// or an IPv4 netlink notification. This replaces timing-dependent sleeps.
async fn refresh_selected_addresses(
    lifecycle: &mut Lifecycle,
    virtual_camera: &mut VirtualCamera,
    address_monitor: &AddressMonitor,
    api_probe: &mut Option<ApiProbeFuture>,
) -> Result<()> {
    let Some(network) = lifecycle.selected_network() else {
        return Ok(());
    };
    let ifindex = network.ifindex;
    let addresses = address_monitor.addresses_for(ifindex).await?;
    let events = lifecycle.apply(Observation::NetworkAddressesChanged { ifindex, addresses });
    dispatch_runtime(lifecycle, events, virtual_camera, api_probe)
}

/// Dispatches state effects and keeps the single in-flight TCP probe aligned
/// with lifecycle state. Dropping the future cancels a stale connect attempt.
fn dispatch_runtime(
    lifecycle: &Lifecycle,
    events: Vec<LifecycleEvent>,
    camera_output: &mut impl CameraOutput,
    api_probe: &mut Option<ApiProbeFuture>,
) -> Result<()> {
    let request = api_probe_request(&events);
    let waiting_for_api = lifecycle.state.kind() == CameraStateKind::WaitingForApi;
    dispatch_all(events, camera_output)?;

    if let Some((ifindex, endpoint)) = request {
        *api_probe = Some(Box::pin(probe_control_endpoint(ifindex, endpoint)));
    } else if !waiting_for_api {
        *api_probe = None;
    }
    Ok(())
}

/// Extracts a newly requested probe from the transition that introduced its
/// endpoint; metadata refreshes and failed probes never create blind retries.
fn api_probe_request(events: &[LifecycleEvent]) -> Option<(u32, ControlEndpoint)> {
    events.iter().find_map(|event| match event {
        LifecycleEvent::StateTransition {
            to: CameraStateKind::WaitingForApi,
            network: Some(network),
            endpoint: Some(endpoint),
            ..
        } => Some((network.ifindex, *endpoint)),
        _ => None,
    })
}

/// Waits for the active probe, or forever when no probe is active so the
/// select loop remains event-driven without a periodic wake-up.
async fn next_api_probe(probe: &mut Option<ApiProbeFuture>) -> ApiProbeResult {
    match probe {
        Some(probe) => probe.await,
        None => future::pending().await,
    }
}

/// Verifies that the GoPro control TCP port accepts a connection. A timeout is
/// a bounded readiness result, not a retry loop; the kernel owns SYN retries.
async fn probe_control_endpoint(ifindex: u32, endpoint: ControlEndpoint) -> ApiProbeResult {
    let result = match TcpSocket::new_v4().and_then(|socket| {
        socket.bind(SocketAddr::new(IpAddr::V4(endpoint.host_address), 0))?;
        Ok(socket)
    }) {
        Ok(socket) => match timeout(
            GOPRO_CONNECT_TIMEOUT,
            socket.connect(SocketAddr::V4(endpoint.socket_address())),
        )
        .await
        {
            Ok(Ok(_stream)) => Ok(()),
            Ok(Err(error)) => Err(format!(
                "TCP connection to {}:{} failed: {error}",
                endpoint.control_address, GOPRO_CONTROL_PORT
            )),
            Err(_) => Err(format!(
                "TCP connection to {}:{} timed out after {} seconds",
                endpoint.control_address,
                GOPRO_CONTROL_PORT,
                GOPRO_CONNECT_TIMEOUT.as_secs()
            )),
        },
        Err(error) => Err(format!(
            "failed to bind TCP probe to GoPro host address {}: {error}",
            endpoint.host_address
        )),
    };
    ApiProbeResult {
        ifindex,
        endpoint,
        result,
    }
}

/// Applies one complete sysfs inventory in the same order used at daemon
/// startup and after monitor recovery. Keeping this separate from udev makes
/// already-connected-camera startup behavior unit-testable.
fn reconcile_inventory(
    lifecycle: &mut Lifecycle,
    source: EventSource,
    mut cameras: Vec<CameraIdentity>,
    mut networks: Vec<NetworkInterface>,
    camera_output: &mut impl CameraOutput,
) -> Result<()> {
    cameras.sort_by(|left, right| left.usb_path.cmp(&right.usb_path));

    let present_paths: HashSet<_> = cameras
        .iter()
        .map(|camera| camera.usb_path.clone())
        .collect();
    lifecycle
        .ignored
        .retain(|usb_path| present_paths.contains(usb_path));
    if let Some(active_path) = lifecycle
        .selected_camera()
        .map(|camera| camera.usb_path.clone())
        && !present_paths.contains(&active_path)
    {
        dispatch_all(
            lifecycle.apply(Observation::UsbRemoved {
                usb_path: active_path,
            }),
            camera_output,
        )?;
    }

    for camera in cameras {
        dispatch_all(
            lifecycle.apply(Observation::UsbUpsert { camera, source }),
            camera_output,
        )?;
    }

    networks.sort_by(|left, right| left.sysfs_path.cmp(&right.sysfs_path));
    let active_network_present = lifecycle.selected_network().map(|current| {
        networks
            .iter()
            .any(|network| network.sysfs_path == current.sysfs_path)
    });
    if active_network_present == Some(false)
        && let Some(current) = lifecycle.selected_network().cloned()
    {
        dispatch_all(
            lifecycle.apply(Observation::NetworkRemoved {
                sysfs_path: current.sysfs_path,
                name: Some(current.name),
                ifindex: Some(current.ifindex),
                usb_path: Some(current.usb_path),
            }),
            camera_output,
        )?;
    }

    for network in networks {
        dispatch_all(
            lifecycle.apply(Observation::NetworkUpsert(network)),
            camera_output,
        )?;
    }

    Ok(())
}

/// Enumerates currently connected GoPro USB devices from sysfs.
fn enumerate_usb_cameras() -> Result<Vec<CameraIdentity>> {
    let mut enumerator = Enumerator::new()?;
    enumerator.match_subsystem("usb")?;
    enumerator.match_attribute("idVendor", "2672")?;

    let devices = enumerator.scan_devices()?;
    Ok(devices
        .filter(|device| device.devtype() == Some(OsStr::new("usb_device")))
        .filter_map(|device| CameraIdentity::from_device(&device))
        .collect())
}

/// Enumerates initialized network interfaces whose udev ancestry contains a
/// GoPro USB device.
fn enumerate_gopro_networks() -> Result<Vec<NetworkInterface>> {
    let mut enumerator = Enumerator::new()?;
    enumerator.match_subsystem("net")?;
    enumerator.match_is_initialized()?;

    let mut networks = Vec::new();
    for device in enumerator.scan_devices()? {
        if let Some(network) = NetworkInterface::from_device(&device)? {
            networks.push(network);
        }
    }
    Ok(networks)
}

/// Finds and validates the physical GoPro USB ancestor of a udev device.
fn gopro_usb_parent(device: &Device) -> Result<Option<Device>> {
    let parent = device.parent_with_subsystem_devtype("usb", "usb_device")?;
    Ok(parent.filter(|device| CameraIdentity::from_device(device).is_some()))
}

/// Walks toward the USB root and returns the closest bound USB driver name.
fn closest_usb_driver(device: &Device) -> Option<String> {
    let mut parent = device.parent();
    while let Some(current) = parent {
        if current.subsystem() == Some(OsStr::new("usb"))
            && let Some(driver) = current.driver()
        {
            return Some(os_to_string(driver));
        }
        parent = current.parent();
    }
    None
}

/// Applies output side effects at state boundaries and logs every transition.
fn dispatch_all(events: Vec<LifecycleEvent>, camera_output: &mut impl CameraOutput) -> Result<()> {
    for event in events {
        match event {
            LifecycleEvent::StateTransition {
                from,
                to,
                camera,
                network,
                endpoint,
                source,
            } => {
                if to == CameraStateKind::DeviceDetected {
                    camera_output
                        .start()
                        .context("failed to start virtual camera for detected GoPro")?;
                }
                if to == CameraStateKind::Disconnected && from != CameraStateKind::Disconnected {
                    camera_output
                        .stop()
                        .context("failed to stop virtual camera for disconnected GoPro")?;
                }

                let camera = camera.as_ref();
                let network = network.as_ref();
                info!(
                    event = "camera_state_transition",
                    from = %from,
                    to = %to,
                    source = source.map(|value| value.to_string()).as_deref().unwrap_or("unknown"),
                    model = camera.and_then(|value| value.model.as_deref()).unwrap_or("unknown"),
                    manufacturer = camera.and_then(|value| value.manufacturer.as_deref()).unwrap_or("unknown"),
                    serial = camera.and_then(|value| value.serial.as_deref()).unwrap_or("unknown"),
                    usb_id = camera.map(usb_id).unwrap_or_else(|| "unknown".to_owned()),
                    usb_path = camera.map(|value| value.usb_path.display().to_string()).unwrap_or_else(|| "unknown".to_owned()),
                    interface = network.map(|value| value.name.as_str()).unwrap_or("unknown"),
                    ifindex = network.map_or_else(|| "unknown".to_owned(), |value| value.ifindex.to_string()),
                    host_address = endpoint.map(|value| value.host_address.to_string()).unwrap_or_else(|| "unknown".to_owned()),
                    control_address = endpoint.map(|value| value.control_address.to_string()).unwrap_or_else(|| "unknown".to_owned()),
                    "GoPro lifecycle state changed"
                );
            }
            LifecycleEvent::CameraIgnored(camera) => {
                warn!(
                    event = "camera_ignored",
                    reason = "active_camera_present",
                    model = camera.model.as_deref().unwrap_or("unknown"),
                    serial = camera.serial.as_deref().unwrap_or("unknown"),
                    usb_id = %usb_id(&camera),
                    usb_path = %camera.usb_path.display(),
                    "additional GoPro ignored while another camera is active"
                );
            }
            LifecycleEvent::NetworkUpdated { previous, current } => {
                info!(
                    event = "network_interface_updated",
                    previous_interface = %previous.name,
                    interface = %current.name,
                    ifindex = current.ifindex,
                    mac = current.mac_address.as_deref().unwrap_or("unknown"),
                    driver = current.driver.as_deref().unwrap_or("unknown"),
                    network_path = %current.sysfs_path.display(),
                    usb_path = %current.usb_path.display(),
                    "GoPro USB network interface metadata changed"
                );
            }
            LifecycleEvent::ApiProbeFailed { endpoint, error } => {
                warn!(
                    event = "gopro_api_probe_failed",
                    host_address = %endpoint.host_address,
                    control_address = %endpoint.control_address,
                    port = GOPRO_CONTROL_PORT,
                    %error,
                    "GoPro control API is not reachable yet; waiting for a network change"
                );
            }
        }
    }

    Ok(())
}

/// Formats a camera's USB vendor and product IDs for logs.
fn usb_id(camera: &CameraIdentity) -> String {
    camera.product_id.map_or_else(
        || format!("{:04x}:unknown", camera.vendor_id),
        |product_id| format!("{:04x}:{product_id:04x}", camera.vendor_id),
    )
}

/// Reads a sysfs attribute and falls back to the equivalent udev property.
fn attribute_or_property(
    device: &Device,
    attribute_name: &str,
    property_name: &str,
) -> Option<String> {
    attribute(device, attribute_name).or_else(|| property(device, property_name))
}

/// Reads a udev device's sysfs attribute into an owned, lossily decoded string.
fn attribute(device: &Device, name: &str) -> Option<String> {
    device.attribute_value(name).map(os_to_string)
}

/// Reads a udev property into an owned, lossily decoded string.
fn property(device: &Device, name: &str) -> Option<String> {
    device.property_value(name).map(os_to_string)
}

/// Converts Linux device metadata to owned UTF-8 without failing on invalid bytes.
fn os_to_string(value: &OsStr) -> String {
    value.to_string_lossy().into_owned()
}

/// Parses a USB identifier with or without a leading `0x` prefix.
fn parse_hex_u16(value: &str) -> Option<u16> {
    u16::from_str_radix(value.trim_start_matches("0x"), 16).ok()
}

/// Waits for either Ctrl-C or SIGTERM and reports signal-registration errors.
async fn shutdown_signal() -> Result<()> {
    let mut terminate =
        signal(SignalKind::terminate()).context("failed to register SIGTERM handler")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("failed to register Ctrl-C handler"),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct MockCameraOutput {
        starts: usize,
        stops: usize,
        fail_start: bool,
    }

    impl CameraOutput for MockCameraOutput {
        fn start(&mut self) -> Result<()> {
            if self.fail_start {
                bail!("synthetic start failure");
            }
            self.starts += 1;
            Ok(())
        }

        fn stop(&mut self) -> Result<()> {
            self.stops += 1;
            Ok(())
        }
    }

    /// Creates a complete synthetic GoPro identity for lifecycle tests.
    fn camera(path: &str, serial: Option<&str>) -> CameraIdentity {
        CameraIdentity {
            usb_path: PathBuf::from(path),
            physical_path: Some(format!("pci-test-{path}")),
            vendor_id: GOPRO_VENDOR_ID,
            product_id: Some(0x0059),
            manufacturer: Some("GoPro".to_owned()),
            model: Some("HERO12 Black".to_owned()),
            serial: serial.map(str::to_owned),
            bus_number: Some(1),
            device_number: Some(2),
        }
    }

    /// Creates a synthetic CDC-NCM interface owned by a test GoPro.
    fn network(path: &str, usb_path: &str, name: &str) -> NetworkInterface {
        NetworkInterface {
            sysfs_path: PathBuf::from(path),
            name: name.to_owned(),
            ifindex: 7,
            mac_address: Some("02:00:00:00:00:01".to_owned()),
            driver: Some("cdc_ncm".to_owned()),
            usb_path: PathBuf::from(usb_path),
        }
    }

    fn transition_kinds(events: &[LifecycleEvent]) -> Vec<(CameraStateKind, CameraStateKind)> {
        events
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::StateTransition { from, to, .. } => Some((*from, *to)),
                LifecycleEvent::CameraIgnored(_)
                | LifecycleEvent::NetworkUpdated { .. }
                | LifecycleEvent::ApiProbeFailed { .. } => None,
            })
            .collect()
    }

    /// Verifies black output starts at USB detection and stops only on session
    /// teardown, rather than at the intermediate network states.
    #[test]
    fn dispatch_starts_at_detection_and_stops_at_disconnection() {
        let identity = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");
        let mut output = MockCameraOutput::default();

        dispatch_all(
            vec![
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::Disconnected,
                    to: CameraStateKind::DeviceDetected,
                    camera: Some(identity.clone()),
                    network: None,
                    endpoint: None,
                    source: Some(EventSource::Startup),
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::DeviceDetected,
                    to: CameraStateKind::WaitingForNetwork,
                    camera: Some(identity.clone()),
                    network: None,
                    endpoint: None,
                    source: Some(EventSource::Startup),
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::WaitingForNetwork,
                    to: CameraStateKind::WaitingForAddress,
                    camera: Some(identity.clone()),
                    network: Some(interface.clone()),
                    endpoint: None,
                    source: None,
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::WaitingForAddress,
                    to: CameraStateKind::WaitingForApi,
                    camera: Some(identity.clone()),
                    network: Some(interface.clone()),
                    endpoint: Some(ControlEndpoint::from_host_address(Ipv4Addr::new(
                        172, 27, 187, 52,
                    ))),
                    source: None,
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::WaitingForApi,
                    to: CameraStateKind::Ready,
                    camera: Some(identity.clone()),
                    network: Some(interface.clone()),
                    endpoint: Some(ControlEndpoint::from_host_address(Ipv4Addr::new(
                        172, 27, 187, 52,
                    ))),
                    source: None,
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::Ready,
                    to: CameraStateKind::Disconnected,
                    camera: Some(identity),
                    network: Some(interface),
                    endpoint: Some(ControlEndpoint::from_host_address(Ipv4Addr::new(
                        172, 27, 187, 52,
                    ))),
                    source: None,
                },
            ],
            &mut output,
        )
        .unwrap();

        assert_eq!(output.starts, 1);
        assert_eq!(output.stops, 1);
    }

    /// Verifies a producer setup failure aborts lifecycle dispatch immediately.
    #[test]
    fn dispatch_propagates_virtual_camera_start_failure() {
        let mut output = MockCameraOutput {
            fail_start: true,
            ..MockCameraOutput::default()
        };

        let error = dispatch_all(
            vec![LifecycleEvent::StateTransition {
                from: CameraStateKind::Disconnected,
                to: CameraStateKind::DeviceDetected,
                camera: Some(camera("/sys/camera-a", None)),
                network: None,
                endpoint: None,
                source: Some(EventSource::Udev),
            }],
            &mut output,
        )
        .unwrap_err();

        assert!(error.to_string().contains("failed to start virtual camera"));
        assert_eq!(output.starts, 0);
    }

    /// Verifies the requested state sequence and exact black-output boundary.
    #[test]
    fn usb_network_address_and_api_observations_reach_ready() {
        let mut lifecycle = Lifecycle::default();
        let first = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");
        let mut output = MockCameraOutput::default();

        let detected = lifecycle.apply(Observation::UsbUpsert {
            camera: first,
            source: EventSource::Udev,
        });
        assert_eq!(
            transition_kinds(&detected),
            vec![
                (
                    CameraStateKind::Disconnected,
                    CameraStateKind::DeviceDetected
                ),
                (
                    CameraStateKind::DeviceDetected,
                    CameraStateKind::WaitingForNetwork
                ),
            ]
        );
        dispatch_all(detected, &mut output).unwrap();
        assert_eq!(output.starts, 1);
        assert_eq!(lifecycle.state.kind(), CameraStateKind::WaitingForNetwork);

        let waiting_for_address = lifecycle.apply(Observation::NetworkUpsert(interface));
        assert_eq!(
            transition_kinds(&waiting_for_address),
            vec![(
                CameraStateKind::WaitingForNetwork,
                CameraStateKind::WaitingForAddress
            )]
        );
        dispatch_all(waiting_for_address, &mut output).unwrap();
        let endpoint = ControlEndpoint::from_host_address(Ipv4Addr::new(172, 27, 187, 52));
        let waiting_for_api = lifecycle.apply(Observation::NetworkAddressesChanged {
            ifindex: 7,
            addresses: vec![endpoint.host_address],
        });
        assert_eq!(
            transition_kinds(&waiting_for_api),
            vec![(
                CameraStateKind::WaitingForAddress,
                CameraStateKind::WaitingForApi
            )]
        );
        dispatch_all(waiting_for_api, &mut output).unwrap();
        let ready = lifecycle.apply(Observation::ApiProbeSucceeded {
            ifindex: 7,
            endpoint,
        });
        assert_eq!(
            transition_kinds(&ready),
            vec![(CameraStateKind::WaitingForApi, CameraStateKind::Ready)]
        );
        dispatch_all(ready, &mut output).unwrap();
        assert_eq!(output.starts, 1);
        assert_eq!(output.stops, 0);
        assert_eq!(lifecycle.state.kind(), CameraStateKind::Ready);
    }

    /// Verifies address ownership, endpoint derivation, stale probe results,
    /// and API failure handling without depending on a real GoPro or netlink
    /// namespace in the unit test process.
    #[test]
    fn address_and_api_events_are_scoped_to_the_selected_interface() {
        let identity = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");
        let endpoint = ControlEndpoint::from_host_address(Ipv4Addr::new(172, 27, 187, 52));
        let mut lifecycle = Lifecycle::default();

        lifecycle.apply(Observation::UsbUpsert {
            camera: identity,
            source: EventSource::Startup,
        });
        lifecycle.apply(Observation::NetworkUpsert(interface));
        assert!(
            lifecycle
                .apply(Observation::NetworkAddressesChanged {
                    ifindex: 99,
                    addresses: vec![endpoint.host_address],
                })
                .is_empty()
        );
        assert_eq!(lifecycle.state.kind(), CameraStateKind::WaitingForAddress);

        assert_eq!(
            transition_kinds(&lifecycle.apply(Observation::NetworkAddressesChanged {
                ifindex: 7,
                addresses: vec![endpoint.host_address],
            })),
            vec![(
                CameraStateKind::WaitingForAddress,
                CameraStateKind::WaitingForApi
            )]
        );
        assert_eq!(lifecycle.state.endpoint(), Some(endpoint));
        assert_eq!(endpoint.control_address, Ipv4Addr::new(172, 27, 187, 51));

        let failure = lifecycle.apply(Observation::ApiProbeFailed {
            ifindex: 7,
            endpoint,
            error: "connection refused".to_owned(),
        });
        assert!(matches!(
            failure.as_slice(),
            [LifecycleEvent::ApiProbeFailed { .. }]
        ));
        assert_eq!(lifecycle.state.kind(), CameraStateKind::WaitingForApi);
        assert!(
            lifecycle
                .apply(Observation::ApiProbeSucceeded {
                    ifindex: 99,
                    endpoint,
                })
                .is_empty()
        );
        assert_eq!(lifecycle.state.kind(), CameraStateKind::WaitingForApi);

        lifecycle.apply(Observation::NetworkAddressesChanged {
            ifindex: 7,
            addresses: Vec::new(),
        });
        assert_eq!(lifecycle.state.kind(), CameraStateKind::WaitingForAddress);
        assert!(
            lifecycle
                .apply(Observation::ApiProbeSucceeded {
                    ifindex: 7,
                    endpoint,
                })
                .is_empty()
        );
    }

    /// Verifies repeated udev observations do not emit duplicate transitions.
    #[test]
    fn duplicate_events_are_idempotent() {
        let mut lifecycle = Lifecycle::default();
        let identity = camera("/sys/camera-a", None);
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");

        lifecycle.apply(Observation::UsbUpsert {
            camera: identity.clone(),
            source: EventSource::Startup,
        });
        assert!(
            lifecycle
                .apply(Observation::UsbUpsert {
                    camera: identity,
                    source: EventSource::Udev,
                })
                .is_empty()
        );
        lifecycle.apply(Observation::NetworkUpsert(interface.clone()));
        assert!(
            lifecycle
                .apply(Observation::NetworkUpsert(interface))
                .is_empty()
        );
    }

    /// Verifies a predictable interface rename is represented as one update.
    #[test]
    fn network_rename_is_an_update_not_a_detach() {
        let mut lifecycle = Lifecycle::default();
        let identity = camera("/sys/camera-a", None);
        let original = network("/sys/net/eth0", "/sys/camera-a", "eth0");
        let mut renamed = original.clone();
        renamed.sysfs_path = PathBuf::from("/sys/net/enx020000000001");
        renamed.name = "enx020000000001".to_owned();

        lifecycle.apply(Observation::UsbUpsert {
            camera: identity,
            source: EventSource::Startup,
        });
        lifecycle.apply(Observation::NetworkUpsert(original.clone()));

        assert_eq!(
            lifecycle.apply(Observation::NetworkUpsert(renamed.clone())),
            vec![LifecycleEvent::NetworkUpdated {
                previous: original,
                current: renamed,
            }]
        );
    }

    /// Verifies USB removal tears down every long-lived connected state.
    #[test]
    fn usb_removal_reaches_disconnected_from_every_connected_state() {
        let identity = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");
        let endpoint = ControlEndpoint::from_host_address(Ipv4Addr::new(172, 27, 187, 52));
        let states = [
            CameraState::DeviceDetected {
                camera: identity.clone(),
            },
            CameraState::WaitingForNetwork {
                camera: identity.clone(),
            },
            CameraState::WaitingForAddress {
                camera: identity.clone(),
                network: interface.clone(),
            },
            CameraState::WaitingForApi {
                camera: identity.clone(),
                network: interface.clone(),
                endpoint,
            },
            CameraState::Ready {
                camera: identity.clone(),
                network: interface.clone(),
                endpoint,
            },
        ];

        for state in states {
            let from = state.kind();
            let mut lifecycle = Lifecycle {
                state,
                ignored: HashSet::new(),
            };
            let events = lifecycle.apply(Observation::UsbRemoved {
                usb_path: identity.usb_path.clone(),
            });
            assert_eq!(
                transition_kinds(&events),
                vec![(from, CameraStateKind::Disconnected)]
            );
            assert_eq!(lifecycle.state.kind(), CameraStateKind::Disconnected);
        }
    }

    /// Verifies network loss preserves the physical camera session and returns
    /// it to the interface-waiting phase.
    #[test]
    fn network_removal_returns_to_waiting_for_network() {
        let identity = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");
        let endpoint = ControlEndpoint::from_host_address(Ipv4Addr::new(172, 27, 187, 52));
        let states = [
            CameraState::WaitingForAddress {
                camera: identity.clone(),
                network: interface.clone(),
            },
            CameraState::WaitingForApi {
                camera: identity.clone(),
                network: interface.clone(),
                endpoint,
            },
            CameraState::Ready {
                camera: identity.clone(),
                network: interface.clone(),
                endpoint,
            },
        ];

        for state in states {
            let from = state.kind();
            let mut lifecycle = Lifecycle {
                state,
                ignored: HashSet::new(),
            };
            let events = lifecycle.apply(Observation::NetworkRemoved {
                sysfs_path: interface.sysfs_path.clone(),
                name: Some(interface.name.clone()),
                ifindex: Some(interface.ifindex),
                usb_path: Some(identity.usb_path.clone()),
            });
            assert_eq!(
                transition_kinds(&events),
                vec![(from, CameraStateKind::WaitingForNetwork)]
            );
            assert_eq!(lifecycle.state.kind(), CameraStateKind::WaitingForNetwork);
        }
    }

    /// Verifies interfaces owned by other devices do not affect camera state.
    #[test]
    fn unrelated_network_is_ignored() {
        let mut lifecycle = Lifecycle::default();
        lifecycle.apply(Observation::UsbUpsert {
            camera: camera("/sys/camera-a", None),
            source: EventSource::Startup,
        });

        assert!(
            lifecycle
                .apply(Observation::NetworkUpsert(network(
                    "/sys/net/eth0",
                    "/sys/not-the-camera",
                    "eth0",
                )))
                .is_empty()
        );
        assert!(lifecycle.selected_network().is_none());
    }

    /// Verifies an early network observation does not prevent later attachment.
    #[test]
    fn network_before_usb_does_not_poison_later_attachment() {
        let mut lifecycle = Lifecycle::default();
        let identity = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");

        assert!(
            lifecycle
                .apply(Observation::NetworkUpsert(interface.clone()))
                .is_empty()
        );
        lifecycle.apply(Observation::UsbUpsert {
            camera: identity,
            source: EventSource::Udev,
        });
        assert_eq!(
            transition_kinds(&lifecycle.apply(Observation::NetworkUpsert(interface))),
            vec![(
                CameraStateKind::WaitingForNetwork,
                CameraStateKind::WaitingForAddress
            )]
        );
    }

    /// Verifies only one camera is active and a remaining camera can be promoted.
    #[test]
    fn second_camera_is_warned_once_and_can_be_promoted() {
        let mut lifecycle = Lifecycle::default();
        let first = camera("/sys/camera-a", Some("A"));
        let second = camera("/sys/camera-b", Some("B"));

        lifecycle.apply(Observation::UsbUpsert {
            camera: first.clone(),
            source: EventSource::Startup,
        });
        assert_eq!(
            lifecycle.apply(Observation::UsbUpsert {
                camera: second.clone(),
                source: EventSource::Startup,
            }),
            vec![LifecycleEvent::CameraIgnored(second.clone())]
        );
        assert!(
            lifecycle
                .apply(Observation::UsbUpsert {
                    camera: second.clone(),
                    source: EventSource::Udev,
                })
                .is_empty()
        );

        lifecycle.apply(Observation::UsbRemoved {
            usb_path: first.usb_path,
        });
        assert_eq!(
            transition_kinds(&lifecycle.apply(Observation::UsbUpsert {
                camera: second,
                source: EventSource::Reconcile,
            })),
            vec![
                (
                    CameraStateKind::Disconnected,
                    CameraStateKind::DeviceDetected
                ),
                (
                    CameraStateKind::DeviceDetected,
                    CameraStateKind::WaitingForNetwork
                ),
            ]
        );
    }

    /// Verifies absent optional USB metadata is valid lifecycle input.
    #[test]
    fn missing_optional_identifiers_are_supported() {
        let identity = CameraIdentity::unknown(PathBuf::from("/sys/camera-a"));
        let mut lifecycle = Lifecycle::default();

        assert_eq!(
            transition_kinds(&lifecycle.apply(Observation::UsbUpsert {
                camera: identity,
                source: EventSource::Startup,
            })),
            vec![
                (
                    CameraStateKind::Disconnected,
                    CameraStateKind::DeviceDetected
                ),
                (
                    CameraStateKind::DeviceDetected,
                    CameraStateKind::WaitingForNetwork
                ),
            ]
        );
    }

    /// Verifies loss of the selected USB network retains black output and a
    /// later network observation restores the address-waiting phase.
    #[test]
    fn network_loss_keeps_black_output_until_usb_disconnect() {
        let identity = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");
        let mut lifecycle = Lifecycle::default();
        let mut output = MockCameraOutput::default();

        dispatch_all(
            lifecycle.apply(Observation::UsbUpsert {
                camera: identity.clone(),
                source: EventSource::Startup,
            }),
            &mut output,
        )
        .unwrap();
        dispatch_all(
            lifecycle.apply(Observation::NetworkUpsert(interface.clone())),
            &mut output,
        )
        .unwrap();

        let disconnected = lifecycle.apply(Observation::NetworkRemoved {
            sysfs_path: interface.sysfs_path.clone(),
            name: Some(interface.name.clone()),
            ifindex: Some(interface.ifindex),
            usb_path: Some(identity.usb_path.clone()),
        });
        assert_eq!(
            transition_kinds(&disconnected),
            vec![(
                CameraStateKind::WaitingForAddress,
                CameraStateKind::WaitingForNetwork
            )]
        );
        dispatch_all(disconnected, &mut output).unwrap();
        assert_eq!((output.starts, output.stops), (1, 0));

        dispatch_all(
            lifecycle.apply(Observation::NetworkUpsert(interface)),
            &mut output,
        )
        .unwrap();
        assert_eq!((output.starts, output.stops), (1, 0));
        assert_eq!(lifecycle.state.kind(), CameraStateKind::WaitingForAddress);
    }

    /// Verifies a daemon restart reconciles an already-connected GoPro into
    /// address-waiting state without waiting for a future udev event.
    #[test]
    fn startup_inventory_with_camera_and_network_reaches_waiting_for_address() {
        let identity = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");
        let mut lifecycle = Lifecycle::default();
        let mut output = MockCameraOutput::default();

        reconcile_inventory(
            &mut lifecycle,
            EventSource::Startup,
            vec![identity],
            vec![interface],
            &mut output,
        )
        .unwrap();

        assert_eq!(lifecycle.state.kind(), CameraStateKind::WaitingForAddress);
        assert_eq!((output.starts, output.stops), (1, 0));
    }

    /// Verifies the USB identifier parser accepts kernel and prefixed forms.
    #[test]
    fn parses_usb_identifiers() {
        assert_eq!(parse_hex_u16("2672"), Some(GOPRO_VENDOR_ID));
        assert_eq!(parse_hex_u16("0x0059"), Some(0x0059));
        assert_eq!(parse_hex_u16("not-hex"), None);
    }
}
