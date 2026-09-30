# Linux GoPro Webcam

`linux-gopro-webcam-rs` is a native Rust daemon that exposes a supported GoPro
USB webcam stream through an administrator-provisioned `v4l2loopback` device.
It detects the camera, waits for its USB network interface and control API,
enables 1080p Linear webcam mode, receives MPEG-TS over UDP, and uses FFmpeg
only to decode H.264 into YUV420 frames. Rust owns the UDP socket and writes
the virtual V4L2 device directly.

## v0.1.0 prerequisites

- Linux with a GoPro that supports the legacy USB webcam API.
- `ffmpeg` available on `PATH`.
- The `v4l2loopback` kernel module, provisioned separately with exclusive
  capabilities and the exact card label `GoPro Webcam`.
- Read/write access for the user running the daemon to the resulting
  `/dev/videoX` device.

For example, provision one device before starting the daemon:

```sh
sudo modprobe v4l2loopback exclusive_caps=1 card_label='GoPro Webcam' video_nr=42
```

The daemon deliberately does not load or unload kernel modules itself.

## Run

```sh
cargo run --release
```

Set `RUST_LOG=info` to see lifecycle, decoder, and stream-health events:

```sh
RUST_LOG=info cargo run --release
```

Connect the GoPro by USB. On a successful session, the daemon binds the GoPro
USB-network UDP port, starts FFmpeg with MPEG-TS stdin and raw YUV420 stdout,
then sends decoded frames to the labelled `/dev/videoX` device. Select that
device in a V4L2-compatible application.

The virtual device begins with black frames when a GoPro is detected and
returns to black if no decoded frame is currently available. Disconnecting the
camera stops output while leaving the administrator-provisioned device intact.
An unexpected FFmpeg or UDP failure also returns to black and waits for a real
network or USB reconnect before attempting another webcam session. On normal
daemon shutdown, the local decoder stops and the daemon makes a best-effort
request for the GoPro to leave webcam mode.

## Verify on hardware

1. Run the daemon with `RUST_LOG=info` and connect the GoPro.
2. Confirm logs reach `gopro_webcam_started` and `gopro_stream_started`.
3. Open the labelled `/dev/videoX` in a V4L2 consumer and confirm live 1080p
   video.
4. Unplug and reconnect the GoPro; verify output returns to black while absent
   and resumes only after the new lifecycle session becomes ready.
