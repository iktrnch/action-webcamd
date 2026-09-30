use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{info, warn};
use v4l::{Device, Format, FourCC};
use v4l::{format::colorspace::Colorspace, format::quantization::Quantization};
use v4l::{video::Output, video::output::Parameters};

use crate::settings::{VIDEO_DEVICE_LABEL, VIDEO_FPS, VIDEO_HEIGHT, VIDEO_WIDTH};

const LOOPBACK_DRIVER: &str = "v4l2 loopback";
const VIDEO_CLASS: &str = "/sys/class/video4linux";
const YUV420_FOURCC_BYTES: &[u8; 4] = b"YU12";
const Y_BLACK: u8 = 16;
const UV_NEUTRAL: u8 = 128;

/// A bounded, latest-frame-wins handoff from the decoder to the V4L2 writer.
///
/// The output device has one fixed frame cadence. Retaining only the newest
/// decoded frame prevents an overloaded consumer from increasing video latency.
#[derive(Clone, Default)]
pub(crate) struct FrameSink {
    state: Arc<Mutex<FrameState>>,
}

#[derive(Default)]
struct FrameState {
    latest: Option<Vec<u8>>,
    reset_requested: bool,
}

/// One update consumed by the V4L2 producer on its fixed frame cadence.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FrameUpdate {
    New(Vec<u8>),
    Reset,
    Empty,
}

impl FrameSink {
    /// Replaces any unconsumed frame with one complete YU12 frame.
    pub(crate) fn publish(&self, frame: Vec<u8>) -> Result<()> {
        ensure!(
            frame.len() == frame_size(VIDEO_WIDTH, VIDEO_HEIGHT)?,
            "decoded frame has {} bytes; expected {} bytes for {}x{} YU12",
            frame.len(),
            frame_size(VIDEO_WIDTH, VIDEO_HEIGHT)?,
            VIDEO_WIDTH,
            VIDEO_HEIGHT,
        );
        let mut state = self.state.lock().expect("frame sink lock poisoned");
        state.latest = Some(frame);
        state.reset_requested = false;
        Ok(())
    }

    pub(crate) fn take(&self) -> FrameUpdate {
        let mut state = self.state.lock().expect("frame sink lock poisoned");
        if state.reset_requested {
            state.reset_requested = false;
            return FrameUpdate::Reset;
        }
        state
            .latest
            .take()
            .map_or(FrameUpdate::Empty, FrameUpdate::New)
    }

    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().expect("frame sink lock poisoned");
        state.latest = None;
        state.reset_requested = true;
    }
}

/// Receives physical-camera presence changes from the hotplug reducer.
pub(crate) trait CameraOutput {
    /// Starts producing placeholder frames for the active camera.
    fn start(&mut self) -> Result<()>;

    /// Stops producing frames while preserving the virtual device.
    fn stop(&mut self) -> Result<()>;
}

/// Owns the persistent virtual-device path and at most one frame producer.
pub(crate) struct VirtualCamera {
    device_path: PathBuf,
    producer: Option<Producer>,
    frames: FrameSink,
    failures: UnboundedSender<anyhow::Error>,
}

impl VirtualCamera {
    /// Finds the administrator-provisioned virtual device before hotplug discovery.
    pub(crate) fn prepare() -> Result<(Self, UnboundedReceiver<anyhow::Error>)> {
        let device_path = require_existing_device()?;
        let (failures, receiver) = unbounded_channel();

        info!(
            event = "virtual_camera_ready",
            label = VIDEO_DEVICE_LABEL,
            device = %device_path.display(),
            width = VIDEO_WIDTH,
            height = VIDEO_HEIGHT,
            fps = VIDEO_FPS,
            format = "YU12",
            "virtual GoPro camera ready"
        );

        Ok((
            Self {
                device_path,
                producer: None,
                frames: FrameSink::default(),
                failures,
            },
            receiver,
        ))
    }

    /// Returns the device node selected for the GoPro virtual camera.
    #[cfg(test)]
    fn device_path(&self) -> &Path {
        &self.device_path
    }

    /// Returns the stable frame handoff used by each active decoder session.
    pub(crate) fn frame_sink(&self) -> FrameSink {
        self.frames.clone()
    }
}

impl CameraOutput for VirtualCamera {
    fn start(&mut self) -> Result<()> {
        if self.producer.is_some() {
            return Ok(());
        }

        let device = configure_output(&self.device_path)?;
        let producer = Producer::spawn(
            device,
            self.device_path.clone(),
            self.failures.clone(),
            self.frames.clone(),
        )?;
        self.producer = Some(producer);

        info!(
            event = "virtual_camera_feed_started",
            device = %self.device_path.display(),
            width = VIDEO_WIDTH,
            height = VIDEO_HEIGHT,
            fps = VIDEO_FPS,
            format = "YU12",
            "started black-frame virtual camera feed"
        );
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        let Some(producer) = self.producer.take() else {
            return Ok(());
        };

        self.frames.clear();
        producer.stop()?;
        info!(
            event = "virtual_camera_feed_stopped",
            device = %self.device_path.display(),
            "stopped virtual camera feed; device remains available"
        );
        Ok(())
    }
}

impl Drop for VirtualCamera {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            warn!(%error, event = "virtual_camera_stop_failed", "failed to stop virtual camera during cleanup");
        }
    }
}

/// One cancellable OS thread that owns the V4L2 output file descriptor.
struct Producer {
    stop: SyncSender<()>,
    thread: JoinHandle<()>,
}

impl Producer {
    /// Starts the producer after format negotiation has already succeeded.
    fn spawn(
        device: Device,
        device_path: PathBuf,
        failures: UnboundedSender<anyhow::Error>,
        frames: FrameSink,
    ) -> Result<Self> {
        let (stop, receiver) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("gopro-black-frame-producer".to_owned())
            .spawn(move || {
                if let Err(error) = produce_frames(device, receiver, frames) {
                    let _ = failures.send(error.context(format!(
                        "virtual camera producer failed for {}",
                        device_path.display()
                    )));
                }
            })
            .context("failed to spawn virtual camera producer thread")?;

        Ok(Self { stop, thread })
    }

    /// Requests cancellation and waits for the producer to close its device.
    fn stop(self) -> Result<()> {
        let _ = self.stop.try_send(());
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("virtual camera producer thread panicked"))
    }
}

/// Locates one administrator-provisioned video node with the configured label.
fn require_existing_device() -> Result<PathBuf> {
    find_labelled_device(
        Path::new(VIDEO_CLASS),
        Path::new("/dev"),
        VIDEO_DEVICE_LABEL,
    )?
    .with_context(|| {
        format!(
            "no V4L2 device labelled {VIDEO_DEVICE_LABEL:?} was found; provision v4l2loopback separately with exclusive_caps=1 and grant this user read/write access to its /dev/videoX node"
        )
    })
}

/// Finds a unique video node by sysfs card label without opening module controls.
fn find_labelled_device(
    video_class: &Path,
    device_directory: &Path,
    label: &str,
) -> Result<Option<PathBuf>> {
    let entries = match fs::read_dir(video_class) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("failed to enumerate V4L2 devices"),
    };

    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry.context("failed to read a V4L2 sysfs entry")?;
        let Some(number) = parse_video_number(&entry.file_name()) else {
            continue;
        };
        let name_path = entry.path().join("name");
        let name = match fs::read_to_string(&name_path) {
            Ok(name) => name,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read {}", name_path.display()));
            }
        };
        if name.trim_end() == label {
            candidates.push(number);
        }
    }

    if candidates.is_empty() {
        return Ok(None);
    }
    candidates.sort_unstable();
    ensure!(
        candidates.len() == 1,
        "found multiple V4L2 devices labelled {label:?}: {}",
        candidates
            .iter()
            .map(|number| device_directory
                .join(format!("video{number}"))
                .display()
                .to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let number = candidates[0];
    let path = device_directory.join(format!("video{number}"));
    ensure!(
        path.exists(),
        "{} exists in sysfs but its device node is missing",
        path.display()
    );
    Ok(Some(path))
}

/// Parses `videoN` sysfs entry names without accepting other node classes.
fn parse_video_number(name: &std::ffi::OsStr) -> Option<i32> {
    let name = name.to_str()?;
    let number = name.strip_prefix("video")?;
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    number.parse().ok()
}

/// Opens the output node and verifies exact format and frame-rate negotiation.
fn configure_output(path: &Path) -> Result<Device> {
    let device = Device::with_path(path)
        .with_context(|| format!("failed to open virtual camera {}", path.display()))?;
    let capabilities = device
        .query_caps()
        .with_context(|| format!("failed to query virtual camera {}", path.display()))?;
    ensure!(
        capabilities.driver == LOOPBACK_DRIVER,
        "{} is handled by {:?}, not v4l2loopback",
        path.display(),
        capabilities.driver
    );
    ensure!(
        capabilities.card == VIDEO_DEVICE_LABEL,
        "{} has card label {:?}, expected {:?}",
        path.display(),
        capabilities.card,
        VIDEO_DEVICE_LABEL
    );

    let yuv420_fourcc = FourCC::new(YUV420_FOURCC_BYTES);
    let mut requested = Format::new(VIDEO_WIDTH, VIDEO_HEIGHT, yuv420_fourcc);
    requested.colorspace = Colorspace::Rec709;
    requested.quantization = Quantization::LimitedRange;
    let negotiated = device
        .set_format(&requested)
        .with_context(|| format!("failed to set YUV420 output format on {}", path.display()))?;
    let expected_frame_size = u64::from(VIDEO_WIDTH) * u64::from(VIDEO_HEIGHT) * 3 / 2;
    ensure!(
        negotiated.width == VIDEO_WIDTH
            && negotiated.height == VIDEO_HEIGHT
            && negotiated.fourcc == yuv420_fourcc
            && u64::from(negotiated.size) == expected_frame_size,
        "{} negotiated {}x{} {} with {} bytes instead of {}x{} YU12 with {} bytes",
        path.display(),
        negotiated.width,
        negotiated.height,
        negotiated.fourcc,
        negotiated.size,
        VIDEO_WIDTH,
        VIDEO_HEIGHT,
        expected_frame_size
    );

    let negotiated_params = device
        .set_params(&Parameters::with_fps(VIDEO_FPS))
        .with_context(|| format!("failed to set output frame rate on {}", path.display()))?;
    ensure!(
        negotiated_params.interval.numerator == 1
            && negotiated_params.interval.denominator == VIDEO_FPS,
        "{} negotiated frame interval {} instead of 1/{}",
        path.display(),
        negotiated_params.interval,
        VIDEO_FPS
    );

    Ok(device)
}

/// Writes black frames at fixed monotonic deadlines until cancellation.
fn produce_frames(mut device: Device, stop: Receiver<()>, frames: FrameSink) -> Result<()> {
    let black_frame = black_yuv420_frame(VIDEO_WIDTH, VIDEO_HEIGHT)?;
    let mut frame = black_frame.clone();
    let interval = Duration::from_nanos(1_000_000_000 / u64::from(VIDEO_FPS));
    let mut deadline = Instant::now();

    loop {
        match stop.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => return Ok(()),
            Err(TryRecvError::Empty) => {}
        }

        match frames.take() {
            FrameUpdate::New(decoded) => frame = decoded,
            FrameUpdate::Reset => frame.clone_from(&black_frame),
            FrameUpdate::Empty => {}
        }
        write_frame(&mut device, &frame).context("failed to write YUV frame")?;

        deadline += interval;
        let now = Instant::now();
        while deadline <= now {
            deadline += interval;
        }
        match stop.recv_timeout(deadline.saturating_duration_since(now)) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

/// Writes exactly one V4L2 frame, treating nonblocking backpressure as a drop.
fn write_frame(writer: &mut impl Write, frame: &[u8]) -> Result<()> {
    loop {
        match writer.write(frame) {
            Ok(written) if written == frame.len() => return Ok(()),
            Ok(written) => {
                bail!(
                    "V4L2 accepted a partial frame ({written} of {} bytes)",
                    frame.len()
                )
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}

/// Builds one limited-range planar YUV420 black frame.
fn black_yuv420_frame(width: u32, height: u32) -> Result<Vec<u8>> {
    ensure!(
        width > 0 && height > 0 && width.is_multiple_of(2) && height.is_multiple_of(2),
        "YUV420 dimensions must be non-zero and even"
    );
    let pixels = usize::try_from(u64::from(width) * u64::from(height))
        .context("video dimensions do not fit in memory")?;
    let chroma_samples = pixels / 4;
    let mut frame = vec![Y_BLACK; pixels];
    frame.resize(pixels + chroma_samples * 2, UV_NEUTRAL);
    Ok(frame)
}

fn frame_size(width: u32, height: u32) -> Result<usize> {
    ensure!(
        width > 0 && height > 0 && width.is_multiple_of(2) && height.is_multiple_of(2),
        "YUV420 dimensions must be non-zero and even"
    );
    usize::try_from(u64::from(width) * u64::from(height) * 3 / 2)
        .context("video dimensions do not fit in memory")
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsStr,
        fs, process,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

    struct VideoFixture {
        root: PathBuf,
        video_class: PathBuf,
        devices: PathBuf,
    }

    impl VideoFixture {
        fn new() -> Self {
            let suffix = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir()
                .join(format!("linux-gopro-webcam-rs-{}-{suffix}", process::id()));
            let video_class = root.join("sys/class/video4linux");
            let devices = root.join("dev");
            fs::create_dir_all(&video_class).unwrap();
            fs::create_dir(&devices).unwrap();
            Self {
                root,
                video_class,
                devices,
            }
        }

        fn add_video(&self, number: i32, label: &str, with_device_node: bool) {
            let name = format!("video{number}");
            let sysfs_node = self.video_class.join(&name);
            fs::create_dir(&sysfs_node).unwrap();
            fs::write(sysfs_node.join("name"), format!("{label}\n")).unwrap();
            if with_device_node {
                fs::write(self.devices.join(name), []).unwrap();
            }
        }
    }

    impl Drop for VideoFixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn black_frame_has_limited_range_yuv420_planes() {
        let frame = black_yuv420_frame(4, 2).unwrap();

        assert_eq!(frame.len(), 12);
        assert_eq!(&frame[..8], &[Y_BLACK; 8]);
        assert_eq!(&frame[8..], &[UV_NEUTRAL; 4]);
    }

    #[test]
    fn black_frame_rejects_invalid_dimensions() {
        assert!(black_yuv420_frame(0, 1080).is_err());
        assert!(black_yuv420_frame(1919, 1080).is_err());
        assert!(black_yuv420_frame(1920, 1079).is_err());
    }

    #[test]
    fn labelled_device_is_found_without_opening_module_control() {
        let fixture = VideoFixture::new();
        fixture.add_video(42, VIDEO_DEVICE_LABEL, true);

        let path = find_labelled_device(&fixture.video_class, &fixture.devices, VIDEO_DEVICE_LABEL)
            .unwrap();

        assert_eq!(path, Some(fixture.devices.join("video42")));
    }

    #[test]
    fn absent_label_returns_none() {
        let fixture = VideoFixture::new();
        fixture.add_video(42, "Other camera", true);

        assert_eq!(
            find_labelled_device(&fixture.video_class, &fixture.devices, VIDEO_DEVICE_LABEL,)
                .unwrap(),
            None
        );
    }

    #[test]
    fn duplicate_labels_are_rejected() {
        let fixture = VideoFixture::new();
        fixture.add_video(42, VIDEO_DEVICE_LABEL, true);
        fixture.add_video(43, VIDEO_DEVICE_LABEL, true);

        let error =
            find_labelled_device(&fixture.video_class, &fixture.devices, VIDEO_DEVICE_LABEL)
                .unwrap_err();

        assert!(error.to_string().contains("multiple V4L2 devices"));
    }

    #[test]
    fn labelled_sysfs_entry_without_device_node_is_rejected() {
        let fixture = VideoFixture::new();
        fixture.add_video(42, VIDEO_DEVICE_LABEL, false);

        let error =
            find_labelled_device(&fixture.video_class, &fixture.devices, VIDEO_DEVICE_LABEL)
                .unwrap_err();

        assert!(error.to_string().contains("device node is missing"));
    }

    #[test]
    fn video_numbers_only_parse_from_video_nodes() {
        assert_eq!(parse_video_number(OsStr::new("video42")), Some(42));
        assert_eq!(parse_video_number(OsStr::new("video")), None);
        assert_eq!(parse_video_number(OsStr::new("vbi42")), None);
        assert_eq!(parse_video_number(OsStr::new("video4x")), None);
    }

    #[test]
    fn frame_writer_rejects_partial_frames_but_allows_backpressure() {
        struct PartialWriter;
        impl Write for PartialWriter {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                Ok(buffer.len() - 1)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        struct BusyWriter;
        impl Write for BusyWriter {
            fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        assert!(write_frame(&mut PartialWriter, &[0; 4]).is_err());
        assert!(write_frame(&mut BusyWriter, &[0; 4]).is_ok());
    }

    #[test]
    fn frame_sink_retains_only_the_newest_complete_frame() {
        let sink = FrameSink::default();
        let first = vec![1; frame_size(VIDEO_WIDTH, VIDEO_HEIGHT).unwrap()];
        let latest = vec![2; frame_size(VIDEO_WIDTH, VIDEO_HEIGHT).unwrap()];

        sink.publish(first).unwrap();
        sink.publish(latest.clone()).unwrap();

        assert_eq!(sink.take(), FrameUpdate::New(latest));
        assert_eq!(sink.take(), FrameUpdate::Empty);
        sink.clear();
        assert_eq!(sink.take(), FrameUpdate::Reset);
        assert!(sink.publish(vec![0; 1]).is_err());
    }

    #[test]
    fn test_virtual_camera_retains_selected_path() {
        let (failures, _receiver) = unbounded_channel();
        let camera = VirtualCamera {
            device_path: PathBuf::from("/dev/video42"),
            producer: None,
            frames: FrameSink::default(),
            failures,
        };

        assert_eq!(camera.device_path(), Path::new("/dev/video42"));
    }
}
