use std::{
    collections::HashSet,
    ffi::OsStr,
    fmt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_udev::{AsyncMonitorSocket, Device, Enumerator, Event, EventType, MonitorBuilder};
use tracing::{debug, info, warn};

use crate::virtual_camera::{CameraOutput, VirtualCamera};

const GOPRO_VENDOR_ID: u16 = 0x2672;

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
    ifindex: Option<u32>,
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
                .or_else(|| attribute(device, "ifindex").and_then(|value| value.parse().ok())),
            mac_address: attribute(device, "address"),
            driver,
            usb_path: usb_parent.syspath().to_path_buf(),
        }))
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
}

/// The explicit lifecycle of the selected GoPro session.
///
/// `DeviceDetected` and `Ready` are deliberate, observable transition points;
/// the reducer immediately advances them to `WaitingForNetwork` and
/// `Streaming` respectively because this milestone has no asynchronous GoPro
/// configuration step between those states.
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
    Ready {
        camera: CameraIdentity,
        network: NetworkInterface,
    },
    Streaming {
        camera: CameraIdentity,
        network: NetworkInterface,
    },
}

impl CameraState {
    /// Returns the stable, data-free name used in transition records and logs.
    fn kind(&self) -> CameraStateKind {
        match self {
            Self::Disconnected => CameraStateKind::Disconnected,
            Self::DeviceDetected { .. } => CameraStateKind::DeviceDetected,
            Self::WaitingForNetwork { .. } => CameraStateKind::WaitingForNetwork,
            Self::Ready { .. } => CameraStateKind::Ready,
            Self::Streaming { .. } => CameraStateKind::Streaming,
        }
    }

    /// Returns the selected physical camera, if a session exists.
    fn camera(&self) -> Option<&CameraIdentity> {
        match self {
            Self::Disconnected => None,
            Self::DeviceDetected { camera }
            | Self::WaitingForNetwork { camera }
            | Self::Ready { camera, .. }
            | Self::Streaming { camera, .. } => Some(camera),
        }
    }

    /// Returns the selected USB-network interface once it has been observed.
    fn network(&self) -> Option<&NetworkInterface> {
        match self {
            Self::Ready { network, .. } | Self::Streaming { network, .. } => Some(network),
            Self::Disconnected | Self::DeviceDetected { .. } | Self::WaitingForNetwork { .. } => {
                None
            }
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
            | Self::Ready {
                camera: current_camera,
                ..
            }
            | Self::Streaming {
                camera: current_camera,
                ..
            } => *current_camera = camera,
        }
    }

    /// Refreshes network metadata while preserving the ready or streaming phase.
    fn replace_network(&mut self, network: NetworkInterface) {
        match self {
            Self::Ready {
                network: current_network,
                ..
            }
            | Self::Streaming {
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
    Ready,
    Streaming,
}

impl fmt::Display for CameraStateKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Disconnected => "disconnected",
            Self::DeviceDetected => "device_detected",
            Self::WaitingForNetwork => "waiting_for_network",
            Self::Ready => "ready",
            Self::Streaming => "streaming",
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
        source: Option<EventSource>,
    },
    CameraIgnored(CameraIdentity),
    NetworkUpdated {
        previous: NetworkInterface,
        current: NetworkInterface,
    },
}

/// Reduces normalized observations into idempotent camera lifecycle events and
/// tracks additional GoPros that are ignored while one camera is active.
#[derive(Debug, Default)]
struct Lifecycle {
    state: CameraState,
    ignored: HashSet<PathBuf>,
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

    /// Advances a selected camera to streaming when its USB network appears,
    /// or refreshes network metadata without restarting an active stream.
    fn upsert_network(&mut self, network: NetworkInterface) -> Vec<LifecycleEvent> {
        let Some(active_camera) = self.state.camera() else {
            return Vec::new();
        };

        if active_camera.usb_path != network.usb_path {
            return Vec::new();
        }

        match &self.state {
            CameraState::DeviceDetected { .. } | CameraState::WaitingForNetwork { .. } => {
                let camera = active_camera.clone();
                vec![
                    self.transition(
                        CameraState::Ready {
                            camera: camera.clone(),
                            network: network.clone(),
                        },
                        None,
                    ),
                    self.transition(CameraState::Streaming { camera, network }, None),
                ]
            }
            CameraState::Ready {
                network: previous, ..
            }
            | CameraState::Streaming {
                network: previous, ..
            } => {
                if previous == &network {
                    return Vec::new();
                }
                let previous = previous.clone();
                self.state.replace_network(network.clone());
                vec![LifecycleEvent::NetworkUpdated {
                    previous,
                    current: network,
                }]
            }
            CameraState::Disconnected => Vec::new(),
        }
    }

    /// Ends the selected session when its known USB-network interface vanishes.
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
        let same_ifindex = ifindex.is_some() && current.ifindex == ifindex;
        let same_name_and_parent = name.is_some_and(|value| current.name == value)
            && usb_path.is_some_and(|value| current.usb_path == value);

        if !same_path && !same_ifindex && !same_name_and_parent {
            return Vec::new();
        }

        vec![self.transition(CameraState::Disconnected, None)]
    }

    /// Records a state transition after retaining snapshots useful after unplug.
    fn transition(&mut self, next: CameraState, source: Option<EventSource>) -> LifecycleEvent {
        let from = self.state.kind();
        let to = next.kind();
        let camera = next.camera().or_else(|| self.state.camera()).cloned();
        let network = next.network().or_else(|| self.state.network()).cloned();
        self.state = next;
        LifecycleEvent::StateTransition {
            from,
            to,
            camera,
            network,
            source,
        }
    }

    /// Returns the selected physical camera while a session exists.
    fn selected_camera(&self) -> Option<&CameraIdentity> {
        self.state.camera()
    }

    /// Returns the selected network interface once it is ready or streaming.
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
    let mut lifecycle = Lifecycle::default();

    reconcile(&mut lifecycle, EventSource::Startup, &mut virtual_camera)
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
                    Some(Ok(event)) => handle_usb_event(&mut lifecycle, &event, &mut virtual_camera)?,
                    Some(Err(error)) => {
                        warn!(%error, event = "udev_receive_error", subsystem = "usb", "failed to receive USB udev event; reconciling");
                        reconcile(&mut lifecycle, EventSource::Reconcile, &mut virtual_camera)?;
                    }
                    None => bail!("USB udev monitor ended unexpectedly"),
                }
            }
            item = network_monitor.next() => {
                match item {
                    Some(Ok(event)) => handle_network_event(&mut lifecycle, &event, &mut virtual_camera)?,
                    Some(Err(error)) => {
                        warn!(%error, event = "udev_receive_error", subsystem = "net", "failed to receive network udev event; reconciling");
                        reconcile(&mut lifecycle, EventSource::Reconcile, &mut virtual_camera)?;
                    }
                    None => bail!("network udev monitor ended unexpectedly"),
                }
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
fn handle_usb_event(
    lifecycle: &mut Lifecycle,
    event: &Event,
    virtual_camera: &mut VirtualCamera,
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
    dispatch_all(lifecycle.apply(observation), virtual_camera)?;

    if active_was_removed && !lifecycle.has_active_camera() {
        reconcile(lifecycle, EventSource::Reconcile, virtual_camera)?;
    }

    Ok(())
}

/// Converts one network udev event into lifecycle observations, ensuring the
/// owning USB camera is observed before its interface.
fn handle_network_event(
    lifecycle: &mut Lifecycle,
    event: &Event,
    virtual_camera: &mut VirtualCamera,
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
            dispatch_all(
                lifecycle.apply(Observation::UsbUpsert {
                    camera,
                    source: EventSource::Udev,
                }),
                virtual_camera,
            )?;
        }
        dispatch_all(
            lifecycle.apply(Observation::NetworkUpsert(network)),
            virtual_camera,
        )?;
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
        dispatch_all(lifecycle.apply(observation), virtual_camera)?;
        return Ok(());
    }

    debug!(path = %event.syspath().display(), event = "unknown_udev_action", subsystem = "net", "ignoring unknown udev action");

    Ok(())
}

/// Rebuilds lifecycle state from the current sysfs inventory to cover startup,
/// missed events, and promotion of an already-connected secondary camera.
fn reconcile(
    lifecycle: &mut Lifecycle,
    source: EventSource,
    virtual_camera: &mut VirtualCamera,
) -> Result<()> {
    let cameras = enumerate_usb_cameras()?;
    let networks = enumerate_gopro_networks()?;
    reconcile_inventory(lifecycle, source, cameras, networks, virtual_camera)
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
                ifindex: current.ifindex,
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
                    ifindex = network.and_then(|value| value.ifindex).map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
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
                    ifindex = current.ifindex.map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
                    mac = current.mac_address.as_deref().unwrap_or("unknown"),
                    driver = current.driver.as_deref().unwrap_or("unknown"),
                    network_path = %current.sysfs_path.display(),
                    usb_path = %current.usb_path.display(),
                    "GoPro USB network interface metadata changed"
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
            ifindex: Some(7),
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
                LifecycleEvent::CameraIgnored(_) | LifecycleEvent::NetworkUpdated { .. } => None,
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
                    source: Some(EventSource::Startup),
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::DeviceDetected,
                    to: CameraStateKind::WaitingForNetwork,
                    camera: Some(identity.clone()),
                    network: None,
                    source: Some(EventSource::Startup),
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::WaitingForNetwork,
                    to: CameraStateKind::Ready,
                    camera: Some(identity.clone()),
                    network: Some(interface.clone()),
                    source: None,
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::Ready,
                    to: CameraStateKind::Streaming,
                    camera: Some(identity.clone()),
                    network: Some(interface.clone()),
                    source: None,
                },
                LifecycleEvent::StateTransition {
                    from: CameraStateKind::Streaming,
                    to: CameraStateKind::Disconnected,
                    camera: Some(identity),
                    network: Some(interface),
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
    fn usb_and_network_observations_reach_streaming() {
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

        let ready = lifecycle.apply(Observation::NetworkUpsert(interface));
        assert_eq!(
            transition_kinds(&ready),
            vec![
                (CameraStateKind::WaitingForNetwork, CameraStateKind::Ready),
                (CameraStateKind::Ready, CameraStateKind::Streaming),
            ]
        );
        dispatch_all(ready, &mut output).unwrap();
        assert_eq!(output.starts, 1);
        assert_eq!(output.stops, 0);
        assert_eq!(lifecycle.state.kind(), CameraStateKind::Streaming);
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
        let states = [
            CameraState::DeviceDetected {
                camera: identity.clone(),
            },
            CameraState::WaitingForNetwork {
                camera: identity.clone(),
            },
            CameraState::Ready {
                camera: identity.clone(),
                network: interface.clone(),
            },
            CameraState::Streaming {
                camera: identity.clone(),
                network: interface.clone(),
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

    /// Verifies loss of the selected interface ends either post-network phase.
    #[test]
    fn network_removal_reaches_disconnected_from_ready_and_streaming() {
        let identity = camera("/sys/camera-a", Some("C123"));
        let interface = network("/sys/net/enx1", "/sys/camera-a", "enx1");
        let states = [
            CameraState::Ready {
                camera: identity.clone(),
                network: interface.clone(),
            },
            CameraState::Streaming {
                camera: identity.clone(),
                network: interface.clone(),
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
                ifindex: interface.ifindex,
                usb_path: Some(identity.usb_path.clone()),
            });
            assert_eq!(
                transition_kinds(&events),
                vec![(from, CameraStateKind::Disconnected)]
            );
            assert_eq!(lifecycle.state.kind(), CameraStateKind::Disconnected);
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
            vec![
                (CameraStateKind::WaitingForNetwork, CameraStateKind::Ready),
                (CameraStateKind::Ready, CameraStateKind::Streaming),
            ]
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

    /// Verifies loss of the selected USB network tears down the session and a
    /// later USB/network observation creates a fresh black-frame session.
    #[test]
    fn network_loss_disconnects_and_network_reappearance_reconnects() {
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
            ifindex: interface.ifindex,
            usb_path: Some(identity.usb_path.clone()),
        });
        assert_eq!(
            transition_kinds(&disconnected),
            vec![(CameraStateKind::Streaming, CameraStateKind::Disconnected)]
        );
        dispatch_all(disconnected, &mut output).unwrap();
        assert_eq!((output.starts, output.stops), (1, 1));

        dispatch_all(
            lifecycle.apply(Observation::UsbUpsert {
                camera: identity,
                source: EventSource::Udev,
            }),
            &mut output,
        )
        .unwrap();
        dispatch_all(
            lifecycle.apply(Observation::NetworkUpsert(interface)),
            &mut output,
        )
        .unwrap();
        assert_eq!((output.starts, output.stops), (2, 1));
        assert_eq!(lifecycle.state.kind(), CameraStateKind::Streaming);
    }

    /// Verifies a daemon restart reconciles an already-connected GoPro into
    /// streaming state without waiting for a future udev event.
    #[test]
    fn startup_inventory_with_camera_and_network_reaches_streaming() {
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

        assert_eq!(lifecycle.state.kind(), CameraStateKind::Streaming);
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
