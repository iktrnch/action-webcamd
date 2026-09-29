# AGENTS.md

This repository implements a native Linux GoPro webcam service in Rust.

The goal is to provide Windows-like plug-and-play behaviour:

```text
GoPro connected
→ detect device
→ wait for USB networking
→ configure GoPro webcam mode
→ receive video stream
→ expose it through a V4L2 virtual camera
```

## Architecture

- Rust userspace daemon for device detection, networking, lifecycle, and settings.
- `v4l2loopback` provides the virtual `/dev/video*` device.
- GoPro control is performed through its HTTP API over the USB network interface.
- Video is received as a UDP MPEG-TS stream.
- FFmpeg may initially be used for decoding, with the option to replace the subprocess with libavcodec later.
- A CLI and optional GUI may communicate with the daemon through D-Bus.
- `references/gopro.sh` holds a reference script we are replacing with the Rust daemon. 

## Principles

- Prefer native Rust APIs over shell commands.
- Do not reimplement generic Linux kernel functionality unnecessarily.
- Keep device lifecycle event-driven; avoid arbitrary sleeps.
- Each milestone should leave the application in a working, testable state.
- Handle connect, disconnect, reconnect, and daemon restart cleanly.
