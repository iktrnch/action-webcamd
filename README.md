# action-webcamd

Native Linux webcam support for GoPro cameras.

`action-webcamd` runs as a background service and turns a compatible GoPro connected over USB into a standard V4L2 webcam.

```text
GoPro
  │ USB
  ▼
USB network interface
  │
  ├── HTTP ──► webcam control
  │
  └── UDP MPEG-TS
          │
          ▼
        FFmpeg
      H.264 → YUV420
          │
          ▼
     v4l2loopback
          │
          ▼
      /dev/videoX
```

The daemon handles USB hotplug, network discovery, GoPro webcam control, stream reception and V4L2 output. FFmpeg is currently used only for H.264 decoding.

> This project is not affiliated with, endorsed by, or associated with GoPro, Inc. GoPro is a trademark or registered trademark of GoPro, Inc.

## Features

- Automatic GoPro USB detection through udev.
- Event-driven USB networking discovery through netlink.
- Automatic webcam start and shutdown.
- Automatic reconnect after unplugging and reconnecting the camera.
- Persistent `v4l2loopback` virtual camera.
- Native Rust UDP stream reception and V4L2 output.
- FFmpeg-based MPEG-TS / H.264 decoding.
- Graceful systemd shutdown.
- TOML configuration.

## Requirements

- Linux
- A GoPro supporting the USB webcam API
- FFmpeg
- `v4l2loopback`

The virtual camera must currently be created before starting the daemon:

```sh
sudo modprobe v4l2loopback \
    exclusive_caps=1 \
    card_label="Action Webcam" \
    video_nr=42
```

`action-webcamd` does not load or unload kernel modules itself.

## Configuration

Configuration is stored at:

```text
/etc/action-webcamd/config.toml
```

Default configuration:

```toml
[virtual_camera]
label = "Action Webcam"
```

The label must match the `card_label` used when creating the `v4l2loopback` device.

Configuration is loaded on startup. Restart the service after making changes:

```sh
sudo systemctl restart action-webcamd.service
```

## Running from source

```sh
cargo build --release
sudo ./target/release/action-webcamd
```

For logs:

```sh
RUST_LOG=info sudo ./target/release/action-webcamd
```

Once running, connect the GoPro over USB. The daemon will detect it, wait for its USB network interface, start webcam mode and begin feeding the virtual camera.

## systemd

The repository includes a systemd unit at:

```text
packaging/action-webcamd.service
```

For development, build the binary and restart your development service:

```sh
cargo build
sudo systemctl restart action-webcamd-dev.service
journalctl -fu action-webcamd-dev.service
```

## Current limitations

- Video output is fixed at **1920×1080 @ 30 FPS**.
- GoPro FOV is fixed to **Linear**.
- Only one GoPro is used at a time.
- FFmpeg is required for video decoding.
- `v4l2loopback` must be provisioned separately.
- Configuration changes require a daemon restart.

## Development

Run the checks with:

```sh
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

The project intentionally keeps GoPro-specific behaviour in userspace rather than implementing a custom kernel driver. `v4l2loopback` provides the generic virtual-camera interface while the Rust daemon owns device discovery, lifecycle, networking and stream delivery.
