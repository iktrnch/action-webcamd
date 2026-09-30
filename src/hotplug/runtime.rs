use std::{
    collections::HashSet,
    ffi::OsStr,
    future,
    net::{IpAddr, SocketAddr},
    pin::Pin,
};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use tokio::{
    net::TcpSocket,
    signal::unix::{SignalKind, signal},
    sync::mpsc::UnboundedReceiver,
    time::timeout,
};
use tokio_udev::{AsyncMonitorSocket, Device, Enumerator, Event, EventType, MonitorBuilder};
use tracing::{debug, info, warn};

use super::{lifecycle::*, model::*};
use crate::{
    gopro::{GoProClient, WebcamConfiguration},
    network::AddressMonitor,
    stream::{StreamMonitor, StreamSocket},
    virtual_camera::{CameraOutput, VirtualCamera},
};

type WebcamStartFuture = Pin<Box<dyn Future<Output = WebcamStartResult>>>;

struct ActiveWebcam {
    endpoint: ControlEndpoint,
    client: GoProClient,
    stream: StreamMonitor,
}

struct WebcamStartResult {
    endpoint: ControlEndpoint,
    client: Option<GoProClient>,
    stream: Option<StreamSocket>,
    outcome: WebcamStartOutcome,
}

enum WebcamStartOutcome {
    Started,
    StartFailed(String),
    FovFailed(String),
}

pub(crate) async fn run(
    mut virtual_camera: VirtualCamera,
    mut producer_failures: UnboundedReceiver<anyhow::Error>,
) -> Result<()> {
    let mut usb_monitor = monitor_usb().context("failed to open USB udev monitor")?;
    let mut network_monitor = monitor_network().context("failed to open network udev monitor")?;
    let mut address_monitor = AddressMonitor::open()?;
    let mut lifecycle = Lifecycle::default();
    let mut api_probe = None;
    let mut webcam_start = None;
    let mut active_webcam = None;

    reconcile(
        &mut lifecycle,
        EventSource::Startup,
        &mut virtual_camera,
        &address_monitor,
        &mut api_probe,
        &mut webcam_start,
        &mut active_webcam,
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
                stop_active_webcam(&mut active_webcam).await;
                virtual_camera.stop().context("failed to stop virtual camera during shutdown")?;
                return Ok(());
            }
            failure = producer_failures.recv() => {
                let error = failure.unwrap_or_else(|| anyhow::anyhow!("virtual camera producer failure channel closed unexpectedly"));
                stop_active_webcam(&mut active_webcam).await;
                virtual_camera.stop().context("failed to stop failed virtual camera producer")?;
                return Err(error);
            }
            item = usb_monitor.next() => {
                match item {
                    Some(Ok(event)) => handle_usb_event(&mut lifecycle, &event, &mut virtual_camera, &address_monitor, &mut api_probe, &mut webcam_start, &mut active_webcam).await?,
                    Some(Err(error)) => {
                        warn!(%error, event = "udev_receive_error", subsystem = "usb", "failed to receive USB udev event; reconciling");
                        reconcile(&mut lifecycle, EventSource::Reconcile, &mut virtual_camera, &address_monitor, &mut api_probe, &mut webcam_start, &mut active_webcam).await?;
                    }
                    None => bail!("USB udev monitor ended unexpectedly"),
                }
            }
            item = network_monitor.next() => {
                match item {
                    Some(Ok(event)) => handle_network_event(&mut lifecycle, &event, &mut virtual_camera, &address_monitor, &mut api_probe, &mut webcam_start, &mut active_webcam).await?,
                    Some(Err(error)) => {
                        warn!(%error, event = "udev_receive_error", subsystem = "net", "failed to receive network udev event; reconciling");
                        reconcile(&mut lifecycle, EventSource::Reconcile, &mut virtual_camera, &address_monitor, &mut api_probe, &mut webcam_start, &mut active_webcam).await?;
                    }
                    None => bail!("network udev monitor ended unexpectedly"),
                }
            }
            change = address_monitor.next_change() => {
                let change = change?;
                if lifecycle.selected_network().is_some_and(|network| network.ifindex == change.ifindex) {
                    debug!(event = "gopro_ipv4_address_changed", ifindex = change.ifindex, address = %change.address, "refreshing GoPro USB-network addresses");
                    refresh_selected_addresses(&mut lifecycle, &mut virtual_camera, &address_monitor, &mut api_probe, &mut webcam_start, &mut active_webcam).await?;
                }
            }
            result = next_api_probe(&mut api_probe) => {
                api_probe = None;
                let observation = match result.result {
                    Ok(()) => Observation::ApiProbeSucceeded { ifindex: result.ifindex, endpoint: result.endpoint },
                    Err(error) => Observation::ApiProbeFailed { ifindex: result.ifindex, endpoint: result.endpoint, error },
                };
                let events = lifecycle.apply(observation);
                dispatch_runtime(&lifecycle, events, &mut virtual_camera, &mut api_probe, &mut webcam_start, &mut active_webcam).await?;
            }
            result = next_webcam_start(&mut webcam_start) => {
                webcam_start = None;
                apply_webcam_start_result(&lifecycle, result, &mut active_webcam).await;
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
    webcam_start: &mut Option<WebcamStartFuture>,
    active_webcam: &mut Option<ActiveWebcam>,
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
    dispatch_runtime(
        lifecycle,
        events,
        virtual_camera,
        api_probe,
        webcam_start,
        active_webcam,
    )
    .await?;

    if active_was_removed && !lifecycle.has_active_camera() {
        reconcile(
            lifecycle,
            EventSource::Reconcile,
            virtual_camera,
            address_monitor,
            api_probe,
            webcam_start,
            active_webcam,
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
    webcam_start: &mut Option<WebcamStartFuture>,
    active_webcam: &mut Option<ActiveWebcam>,
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
            dispatch_runtime(
                lifecycle,
                events,
                virtual_camera,
                api_probe,
                webcam_start,
                active_webcam,
            )
            .await?;
        }
        let events = lifecycle.apply(Observation::NetworkUpsert(network));
        dispatch_runtime(
            lifecycle,
            events,
            virtual_camera,
            api_probe,
            webcam_start,
            active_webcam,
        )
        .await?;
        refresh_selected_addresses(
            lifecycle,
            virtual_camera,
            address_monitor,
            api_probe,
            webcam_start,
            active_webcam,
        )
        .await?;
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
        dispatch_runtime(
            lifecycle,
            events,
            virtual_camera,
            api_probe,
            webcam_start,
            active_webcam,
        )
        .await?;
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
    webcam_start: &mut Option<WebcamStartFuture>,
    active_webcam: &mut Option<ActiveWebcam>,
) -> Result<()> {
    let cameras = enumerate_usb_cameras()?;
    let networks = enumerate_gopro_networks()?;
    reconcile_inventory(lifecycle, source, cameras, networks, virtual_camera)?;
    if lifecycle.state.kind() != CameraStateKind::WaitingForApi {
        *api_probe = None;
    }
    if lifecycle.state.kind() != CameraStateKind::Ready {
        *webcam_start = None;
        discard_active_webcam(active_webcam).await;
    }
    refresh_selected_addresses(
        lifecycle,
        virtual_camera,
        address_monitor,
        api_probe,
        webcam_start,
        active_webcam,
    )
    .await
}

/// Reconciles the selected interface's address inventory after udev discovery
/// or an IPv4 netlink notification. This replaces timing-dependent sleeps.
async fn refresh_selected_addresses(
    lifecycle: &mut Lifecycle,
    virtual_camera: &mut VirtualCamera,
    address_monitor: &AddressMonitor,
    api_probe: &mut Option<ApiProbeFuture>,
    webcam_start: &mut Option<WebcamStartFuture>,
    active_webcam: &mut Option<ActiveWebcam>,
) -> Result<()> {
    let Some(network) = lifecycle.selected_network() else {
        return Ok(());
    };
    let ifindex = network.ifindex;
    let addresses = address_monitor.addresses_for(ifindex).await?;
    let events = lifecycle.apply(Observation::NetworkAddressesChanged { ifindex, addresses });
    dispatch_runtime(
        lifecycle,
        events,
        virtual_camera,
        api_probe,
        webcam_start,
        active_webcam,
    )
    .await
}

/// Dispatches state effects and keeps the single in-flight TCP probe aligned
/// with lifecycle state. Dropping the future cancels a stale connect attempt.
async fn dispatch_runtime(
    lifecycle: &Lifecycle,
    events: Vec<LifecycleEvent>,
    camera_output: &mut impl CameraOutput,
    api_probe: &mut Option<ApiProbeFuture>,
    webcam_start: &mut Option<WebcamStartFuture>,
    active_webcam: &mut Option<ActiveWebcam>,
) -> Result<()> {
    let api_request = api_probe_request(&events);
    let webcam_request = webcam_start_request(&events);
    let leaving_ready = events.iter().any(|event| {
        matches!(
            event,
            LifecycleEvent::StateTransition { from: CameraStateKind::Ready, to, .. }
                if *to != CameraStateKind::Ready
        )
    });
    let waiting_for_api = lifecycle.state.kind() == CameraStateKind::WaitingForApi;
    dispatch_all(events, camera_output)?;

    if leaving_ready {
        *webcam_start = None;
        discard_active_webcam(active_webcam).await;
    }
    if let Some(endpoint) = webcam_request {
        discard_active_webcam(active_webcam).await;
        *webcam_start = Some(Box::pin(start_webcam_mode(endpoint)));
    }

    if let Some((ifindex, endpoint)) = api_request {
        *api_probe = Some(Box::pin(probe_control_endpoint(ifindex, endpoint)));
    } else if !waiting_for_api {
        *api_probe = None;
    }
    Ok(())
}

/// Extracts the readiness transition that should configure webcam mode once.
pub(super) fn webcam_start_request(events: &[LifecycleEvent]) -> Option<ControlEndpoint> {
    events.iter().find_map(|event| match event {
        LifecycleEvent::StateTransition {
            to: CameraStateKind::Ready,
            endpoint: Some(endpoint),
            ..
        } => Some(*endpoint),
        _ => None,
    })
}

/// Runs the ordered START then FOV configuration without blocking hotplug
/// monitoring. Dropping this future cancels it when the session goes stale.
async fn start_webcam_mode(endpoint: ControlEndpoint) -> WebcamStartResult {
    let client = match GoProClient::new(endpoint.host_address, endpoint.control_address) {
        Ok(client) => client,
        Err(error) => {
            return WebcamStartResult {
                endpoint,
                client: None,
                stream: None,
                outcome: WebcamStartOutcome::StartFailed(error.to_string()),
            };
        }
    };

    let configuration = WebcamConfiguration::INITIAL;
    let stream = match StreamSocket::bind(endpoint.host_address, configuration.udp_port()).await {
        Ok(stream) => stream,
        Err(error) => {
            return WebcamStartResult {
                endpoint,
                client: Some(client),
                stream: None,
                outcome: WebcamStartOutcome::StartFailed(error.to_string()),
            };
        }
    };

    if let Err(error) = client.start_webcam(configuration).await {
        return WebcamStartResult {
            endpoint,
            client: Some(client),
            stream: Some(stream),
            outcome: WebcamStartOutcome::StartFailed(error.to_string()),
        };
    }
    if let Err(error) = client.set_webcam_fov(configuration.fov()).await {
        return WebcamStartResult {
            endpoint,
            client: Some(client),
            stream: Some(stream),
            outcome: WebcamStartOutcome::FovFailed(error.to_string()),
        };
    }

    WebcamStartResult {
        endpoint,
        client: Some(client),
        stream: Some(stream),
        outcome: WebcamStartOutcome::Started,
    }
}

/// Applies only completions that still belong to the current ready session.
async fn apply_webcam_start_result(
    lifecycle: &Lifecycle,
    result: WebcamStartResult,
    active_webcam: &mut Option<ActiveWebcam>,
) {
    if lifecycle.state.kind() != CameraStateKind::Ready
        || lifecycle.state.endpoint() != Some(result.endpoint)
    {
        debug!(
            event = "gopro_webcam_start_stale",
            control_address = %result.endpoint.control_address,
            "ignoring webcam control completion for a stale session"
        );
        return;
    }

    match result.outcome {
        WebcamStartOutcome::Started => {
            let client = result
                .client
                .expect("successful webcam start retains its client");
            let stream = result
                .stream
                .expect("successful webcam start retains its UDP receiver");
            discard_active_webcam(active_webcam).await;
            *active_webcam = Some(ActiveWebcam {
                endpoint: result.endpoint,
                client,
                stream: stream.monitor(),
            });
            info!(
                event = "gopro_webcam_started",
                control_address = %result.endpoint.control_address,
                resolution = 1080,
                fov = "linear",
                udp_port = WebcamConfiguration::INITIAL.udp_port(),
                "GoPro entered webcam mode"
            );
        }
        WebcamStartOutcome::StartFailed(error) => {
            warn!(
                event = "gopro_webcam_start_failed",
                control_address = %result.endpoint.control_address,
                resolution = 1080,
                udp_port = WebcamConfiguration::INITIAL.udp_port(),
                %error,
                "GoPro webcam start failed; waiting for a future readiness transition"
            );
        }
        WebcamStartOutcome::FovFailed(error) => {
            if let Some(client) = result.client {
                let stream = result
                    .stream
                    .expect("started webcam retains its UDP receiver after an FOV failure");
                discard_active_webcam(active_webcam).await;
                *active_webcam = Some(ActiveWebcam {
                    endpoint: result.endpoint,
                    client,
                    stream: stream.monitor(),
                });
            }
            warn!(
                event = "gopro_webcam_fov_failed",
                control_address = %result.endpoint.control_address,
                fov = "linear",
                %error,
                "GoPro webcam started but Linear FOV was not applied"
            );
        }
    }
}

/// Waits for the active webcam configuration, or forever when none is active.
async fn next_webcam_start(start: &mut Option<WebcamStartFuture>) -> WebcamStartResult {
    match start {
        Some(start) => start.await,
        None => future::pending().await,
    }
}

/// Stops an active camera on process shutdown without turning a clean shutdown
/// into a daemon failure when the USB link has already disappeared.
async fn stop_active_webcam(active_webcam: &mut Option<ActiveWebcam>) {
    let Some(active) = active_webcam.take() else {
        return;
    };

    active.stream.stop().await;

    match active.client.stop_webcam().await {
        Ok(()) => info!(
            event = "gopro_webcam_stopped",
            control_address = %active.endpoint.control_address,
            "GoPro exited webcam mode"
        ),
        Err(error) => warn!(
            event = "gopro_webcam_stop_failed",
            control_address = %active.endpoint.control_address,
            %error,
            "failed to stop GoPro webcam mode during shutdown"
        ),
    }
}

/// Stops only stream reception when a hotplug transition invalidates an active
/// session. The control endpoint may already be unreachable, so HTTP STOP is
/// intentionally reserved for orderly daemon shutdown.
async fn discard_active_webcam(active_webcam: &mut Option<ActiveWebcam>) {
    if let Some(active) = active_webcam.take() {
        active.stream.stop().await;
    }
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
pub(super) fn reconcile_inventory(
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
pub(super) fn gopro_usb_parent(device: &Device) -> Result<Option<Device>> {
    let parent = device.parent_with_subsystem_devtype("usb", "usb_device")?;
    Ok(parent.filter(|device| CameraIdentity::from_device(device).is_some()))
}

/// Walks toward the USB root and returns the closest bound USB driver name.
pub(super) fn closest_usb_driver(device: &Device) -> Option<String> {
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
pub(super) fn dispatch_all(
    events: Vec<LifecycleEvent>,
    camera_output: &mut impl CameraOutput,
) -> Result<()> {
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
pub(super) fn attribute_or_property(
    device: &Device,
    attribute_name: &str,
    property_name: &str,
) -> Option<String> {
    attribute(device, attribute_name).or_else(|| property(device, property_name))
}

/// Reads a udev device's sysfs attribute into an owned, lossily decoded string.
pub(super) fn attribute(device: &Device, name: &str) -> Option<String> {
    device.attribute_value(name).map(os_to_string)
}

/// Reads a udev property into an owned, lossily decoded string.
pub(super) fn property(device: &Device, name: &str) -> Option<String> {
    device.property_value(name).map(os_to_string)
}

/// Converts Linux device metadata to owned UTF-8 without failing on invalid bytes.
pub(super) fn os_to_string(value: &OsStr) -> String {
    value.to_string_lossy().into_owned()
}

/// Parses a USB identifier with or without a leading `0x` prefix.
pub(super) fn parse_hex_u16(value: &str) -> Option<u16> {
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
