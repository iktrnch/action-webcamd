use super::{lifecycle::*, model::*, runtime::*};
use crate::virtual_camera::CameraOutput;
use anyhow::{Result, bail};
use std::{collections::HashSet, net::Ipv4Addr, path::PathBuf};

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
