use std::{
    collections::HashSet,
    future::Future,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    pin::Pin,
};

use super::model::*;

#[derive(Debug, Default)]
pub(super) struct Lifecycle {
    pub(super) state: CameraState,
    pub(super) ignored: HashSet<PathBuf>,
}

pub(super) type ApiProbeFuture = Pin<Box<dyn Future<Output = ApiProbeResult>>>;

#[derive(Debug)]
pub(super) struct ApiProbeResult {
    pub(super) ifindex: u32,
    pub(super) endpoint: ControlEndpoint,
    pub(super) result: Result<(), String>,
}

impl Lifecycle {
    /// Applies one normalized observation and returns only the semantic state
    /// transitions caused by it.
    pub(super) fn apply(&mut self, observation: Observation) -> Vec<LifecycleEvent> {
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
    pub(super) fn selected_camera(&self) -> Option<&CameraIdentity> {
        self.state.camera()
    }

    /// Returns the selected network interface once udev has correlated it.
    pub(super) fn selected_network(&self) -> Option<&NetworkInterface> {
        self.state.network()
    }

    /// Reports whether the daemon currently has a selected GoPro session.
    pub(super) fn has_active_camera(&self) -> bool {
        self.selected_camera().is_some()
    }
}
