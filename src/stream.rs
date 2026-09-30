//! Rust-owned GoPro UDP reception with an FFmpeg MPEG-TS/H.264 decoder.
//!
//! FFmpeg receives only MPEG-TS bytes on stdin and returns planar YUV420 on
//! stdout. Rust retains the USB-bound UDP socket, lifecycle, and V4L2 output.

use std::{
    collections::VecDeque,
    future,
    io::{BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::Duration as StdDuration,
};

use anyhow::{Context, Result, bail};
use tokio::{
    net::UdpSocket,
    sync::mpsc::UnboundedSender,
    task::JoinHandle as TokioJoinHandle,
    time::{self, Duration, Instant, MissedTickBehavior},
};
use tracing::{debug, info, warn};

use crate::{
    settings::{VIDEO_HEIGHT, VIDEO_WIDTH},
    virtual_camera::FrameSink,
};

const STREAM_STATISTICS_INTERVAL: Duration = Duration::from_secs(1);
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_UDP_DATAGRAM_SIZE: usize = 65_535;
const DECODER_PACKET_QUEUE: usize = 64;
const DECODER_DIAGNOSTIC_LINES: usize = 20;

/// A failure that ends one active stream session without ending the daemon.
#[derive(Debug)]
pub(crate) struct StreamFailure {
    pub(crate) session_id: u64,
    pub(crate) error: String,
}

/// A UDP socket opened before GoPro webcam mode is enabled.
pub(crate) struct StreamSocket {
    socket: UdpSocket,
    local_address: SocketAddrV4,
}

impl StreamSocket {
    /// Binds only the selected GoPro USB-network address.
    pub(crate) async fn bind(host_address: Ipv4Addr, port: u16) -> Result<Self> {
        let requested_address = SocketAddrV4::new(host_address, port);
        let socket = UdpSocket::bind(requested_address).await.with_context(|| {
            format!("failed to bind GoPro UDP stream receiver on {requested_address}")
        })?;
        let local_address = match socket
            .local_addr()
            .context("failed to inspect bound GoPro UDP stream receiver")?
        {
            SocketAddr::V4(address) => address,
            SocketAddr::V6(address) => {
                bail!("GoPro UDP stream receiver unexpectedly bound IPv6 address {address}")
            }
        };
        info!(event = "gopro_stream_receiver_opened", address = %local_address, "GoPro UDP stream receiver opened");
        Ok(Self {
            socket,
            local_address,
        })
    }

    /// Starts FFmpeg before GoPro START so an unavailable decoder cannot leave
    /// a camera streaming without a local consumer.
    pub(crate) fn prepare_decoder(
        self,
        frames: FrameSink,
        session_id: u64,
    ) -> Result<PreparedStream> {
        Ok(PreparedStream {
            socket: self.socket,
            local_address: self.local_address,
            decoder: FfmpegDecoder::spawn(frames)?,
            session_id,
        })
    }

    #[cfg(test)]
    fn local_address(&self) -> SocketAddrV4 {
        self.local_address
    }
}

/// Resources created before GoPro START and promoted only after it succeeds.
pub(crate) struct PreparedStream {
    socket: UdpSocket,
    local_address: SocketAddrV4,
    decoder: FfmpegDecoder,
    session_id: u64,
}

impl PreparedStream {
    pub(crate) fn monitor(self, failures: UnboundedSender<StreamFailure>) -> Result<StreamMonitor> {
        let stopping = self.decoder.stopping.clone();
        let frames = self.decoder.frame_sink();
        let decoder = self.decoder.activate(self.session_id, failures.clone())?;
        let task = tokio::spawn(receive(
            self.socket,
            self.local_address,
            decoder.packet_sender(),
            self.session_id,
            failures,
            stopping,
            frames,
        ));
        Ok(StreamMonitor {
            task: Some(task),
            decoder: Some(decoder),
        })
    }
}

/// Owns UDP reception and FFmpeg for one active webcam session.
pub(crate) struct StreamMonitor {
    task: Option<TokioJoinHandle<()>>,
    decoder: Option<FfmpegDecoder>,
}

impl StreamMonitor {
    /// Cancels UDP reception, terminates FFmpeg, and clears any stale frame.
    pub(crate) async fn stop(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                warn!(event = "gopro_stream_receiver_failed", %error, "GoPro UDP receiver task ended unexpectedly");
            }
        }
        if let Some(decoder) = self.decoder.take()
            && let Err(error) = decoder.stop()
        {
            warn!(event = "ffmpeg_decoder_stop_failed", %error, "failed to stop FFmpeg decoder");
        }
    }
}

impl Drop for StreamMonitor {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn receive(
    socket: UdpSocket,
    local_address: SocketAddrV4,
    packets: SyncSender<Vec<u8>>,
    session_id: u64,
    failures: UnboundedSender<StreamFailure>,
    stopping: Arc<AtomicBool>,
    frames: FrameSink,
) {
    let mut buffer = [0_u8; MAX_UDP_DATAGRAM_SIZE];
    let mut tracker = StreamTracker::default();
    let mut statistics_tick = time::interval(STREAM_STATISTICS_INTERVAL);
    statistics_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    statistics_tick.tick().await;
    loop {
        tokio::select! {
            received = socket.recv_from(&mut buffer) => match received {
                Ok((packet_bytes, peer_address)) => {
                    let now = Instant::now();
                    if tracker.record_packet(now, packet_bytes) {
                        info!(event = "gopro_stream_started", address = %local_address, source = %peer_address, "GoPro UDP stream started");
                    }
                    match packets.try_send(buffer[..packet_bytes].to_vec()) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => tracker.record_drop(),
                        Err(TrySendError::Disconnected(_)) => {
                            report_failure(&failures, session_id, &stopping, "FFmpeg MPEG-TS input pipe closed");
                            return;
                        }
                    }
                }
                Err(error) => {
                    warn!(event = "gopro_stream_receive_failed", address = %local_address, %error, "GoPro UDP stream receiver failed");
                    report_failure(&failures, session_id, &stopping, format!("GoPro UDP receive failed: {error}"));
                    return;
                }
            },
            _ = wait_for_idle_stream(tracker.last_packet()) => {
                if let Some(totals) = tracker.stop_if_idle(Instant::now()) {
                    frames.clear();
                    info!(event = "gopro_stream_stopped", address = %local_address, packets = totals.packets, bytes = totals.bytes, dropped_packets = totals.dropped_packets, idle_seconds = STREAM_IDLE_TIMEOUT.as_secs(), "GoPro UDP stream stopped after inactivity");
                }
            },
            _ = statistics_tick.tick() => {
                if let Some(statistics) = tracker.take_statistics(Instant::now()) {
                    info!(event = "gopro_stream_statistics", address = %local_address, interval_packets = statistics.interval_packets, interval_bytes = statistics.interval_bytes, interval_dropped_packets = statistics.interval_dropped_packets, packets = statistics.total_packets, bytes = statistics.total_bytes, dropped_packets = statistics.total_dropped_packets, bitrate_bps = statistics.bitrate_bps, "GoPro UDP stream statistics");
                }
            },
        }
    }
}

fn report_failure(
    failures: &UnboundedSender<StreamFailure>,
    session_id: u64,
    stopping: &AtomicBool,
    error: impl Into<String>,
) {
    if !stopping.load(Ordering::Relaxed) {
        let _ = failures.send(StreamFailure {
            session_id,
            error: error.into(),
        });
    }
}

async fn wait_for_idle_stream(last_packet: Option<Instant>) {
    match last_packet {
        Some(last_packet) => time::sleep_until(last_packet + STREAM_IDLE_TIMEOUT).await,
        None => future::pending().await,
    }
}

/// A child process restricted to MPEG-TS/H.264 decoding and YUV420 output.
struct FfmpegDecoder {
    packet_sender: Option<SyncSender<Vec<u8>>>,
    control_sender: Option<mpsc::Sender<()>>,
    status_receiver: Option<Receiver<ProcessExit>>,
    process_thread: Option<JoinHandle<()>>,
    worker_threads: Vec<JoinHandle<()>>,
    observer_thread: Option<JoinHandle<()>>,
    diagnostics: Arc<Mutex<VecDeque<String>>>,
    worker_error: Arc<Mutex<Option<String>>>,
    frames: FrameSink,
    stopping: Arc<AtomicBool>,
}

impl FfmpegDecoder {
    fn spawn(frames: FrameSink) -> Result<Self> {
        let mut child = ffmpeg_command()
            .spawn()
            .context("failed to start ffmpeg; install FFmpeg and ensure `ffmpeg` is on PATH")?;
        let stdin = child.stdin.take().context("FFmpeg stdin was not piped")?;
        let stdout = child.stdout.take().context("FFmpeg stdout was not piped")?;
        let stderr = child.stderr.take().context("FFmpeg stderr was not piped")?;
        let (packet_sender, packet_receiver) = mpsc::sync_channel(DECODER_PACKET_QUEUE);
        let (control_sender, control_receiver) = mpsc::channel();
        let (status_sender, status_receiver) = mpsc::channel();
        let diagnostics = Arc::new(Mutex::new(VecDeque::new()));
        let worker_error = Arc::new(Mutex::new(None));
        let stopping = Arc::new(AtomicBool::new(false));

        let input_error = worker_error.clone();
        let input = thread::Builder::new()
            .name("gopro-ffmpeg-input".to_owned())
            .spawn(move || write_packets(stdin, packet_receiver, input_error))
            .context("failed to spawn FFmpeg input worker")?;
        let output_error = worker_error.clone();
        let output_frames = frames.clone();
        let output = thread::Builder::new()
            .name("gopro-ffmpeg-output".to_owned())
            .spawn(move || read_frames(stdout, output_frames, output_error))
            .context("failed to spawn FFmpeg output worker")?;
        let stderr_diagnostics = diagnostics.clone();
        let stderr_worker = thread::Builder::new()
            .name("gopro-ffmpeg-stderr".to_owned())
            .spawn(move || collect_diagnostics(stderr, stderr_diagnostics))
            .context("failed to spawn FFmpeg diagnostic worker")?;
        let process = thread::Builder::new()
            .name("gopro-ffmpeg-process".to_owned())
            .spawn(move || supervise_process(child, control_receiver, status_sender))
            .context("failed to spawn FFmpeg process supervisor")?;

        Ok(Self {
            packet_sender: Some(packet_sender),
            control_sender: Some(control_sender),
            status_receiver: Some(status_receiver),
            process_thread: Some(process),
            worker_threads: vec![input, output, stderr_worker],
            observer_thread: None,
            diagnostics,
            worker_error,
            frames,
            stopping,
        })
    }

    fn packet_sender(&self) -> SyncSender<Vec<u8>> {
        self.packet_sender
            .as_ref()
            .expect("active decoder retains input sender")
            .clone()
    }

    fn frame_sink(&self) -> FrameSink {
        self.frames.clone()
    }

    fn activate(
        mut self,
        session_id: u64,
        failures: UnboundedSender<StreamFailure>,
    ) -> Result<Self> {
        let status_receiver = self
            .status_receiver
            .take()
            .expect("decoder status receiver is activated once");
        let diagnostics = self.diagnostics.clone();
        let worker_error = self.worker_error.clone();
        let stopping = self.stopping.clone();
        self.observer_thread = Some(
            thread::Builder::new()
                .name("gopro-ffmpeg-observer".to_owned())
                .spawn(move || {
                    let Ok(status) = status_receiver.recv() else {
                        return;
                    };
                    if stopping.load(Ordering::Relaxed) || matches!(status, ProcessExit::Stopped) {
                        return;
                    }
                    let _ = failures.send(StreamFailure {
                        session_id,
                        error: process_error(status, &worker_error, &diagnostics),
                    });
                })
                .context("failed to spawn FFmpeg failure observer")?,
        );
        Ok(self)
    }

    fn stop(mut self) -> Result<()> {
        self.request_stop();
        if let Some(thread) = self.process_thread.take() {
            join_thread(thread, "FFmpeg process supervisor")?;
        }
        for thread in self.worker_threads.drain(..) {
            join_thread(thread, "FFmpeg worker")?;
        }
        if let Some(thread) = self.observer_thread.take() {
            join_thread(thread, "FFmpeg failure observer")?;
        }
        self.frames.clear();
        Ok(())
    }

    fn request_stop(&mut self) {
        if self.stopping.swap(true, Ordering::Relaxed) {
            return;
        }
        self.packet_sender.take();
        if let Some(sender) = self.control_sender.take() {
            let _ = sender.send(());
        }
        self.frames.clear();
    }
}

impl Drop for FfmpegDecoder {
    fn drop(&mut self) {
        self.request_stop();
    }
}

fn ffmpeg_command() -> Command {
    let mut command = Command::new("ffmpeg");
    command
        .args([
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-probesize",
            "2048",
            "-analyzeduration",
            "0",
            "-threads",
            "1",
            "-flags",
            "low_delay",
            "-f",
            "mpegts",
            "-i",
            "pipe:0",
            "-map",
            "0:v:0",
            "-fps_mode",
            "passthrough",
            "-an",
            "-sn",
            "-dn",
            "-pix_fmt",
            "yuv420p",
            "-f",
            "rawvideo",
            "pipe:1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn write_packets(
    mut stdin: impl Write,
    packets: Receiver<Vec<u8>>,
    worker_error: Arc<Mutex<Option<String>>>,
) {
    while let Ok(packet) = packets.recv() {
        if let Err(error) = stdin.write_all(&packet) {
            record_worker_error(
                &worker_error,
                format!("failed to write MPEG-TS to FFmpeg: {error}"),
            );
            return;
        }
    }
}

fn read_frames(mut stdout: impl Read, frames: FrameSink, worker_error: Arc<Mutex<Option<String>>>) {
    let frame_size = usize::try_from(u64::from(VIDEO_WIDTH) * u64::from(VIDEO_HEIGHT) * 3 / 2)
        .expect("configured video size fits in memory");
    loop {
        let mut frame = vec![0; frame_size];
        match stdout.read_exact(&mut frame) {
            Ok(()) => {
                if let Err(error) = frames.publish(frame) {
                    record_worker_error(
                        &worker_error,
                        format!("FFmpeg produced an invalid YUV frame: {error:#}"),
                    );
                    return;
                }
            }
            Err(error) => {
                record_worker_error(
                    &worker_error,
                    format!("FFmpeg raw-video output ended: {error}"),
                );
                return;
            }
        }
    }
}

fn collect_diagnostics(stderr: impl Read, diagnostics: Arc<Mutex<VecDeque<String>>>) {
    for line in BufReader::new(stderr).lines().map_while(Result::ok) {
        debug!(event = "ffmpeg_diagnostic", message = %line, "FFmpeg diagnostic");
        let mut diagnostics = diagnostics
            .lock()
            .expect("FFmpeg diagnostics lock poisoned");
        if diagnostics.len() == DECODER_DIAGNOSTIC_LINES {
            diagnostics.pop_front();
        }
        diagnostics.push_back(line);
    }
}

fn record_worker_error(slot: &Mutex<Option<String>>, error: String) {
    let mut slot = slot.lock().expect("FFmpeg worker error lock poisoned");
    if slot.is_none() {
        *slot = Some(error);
    }
}

enum ProcessExit {
    Exited(ExitStatus),
    Failed(String),
    Stopped,
}

fn supervise_process(mut child: Child, control: Receiver<()>, status: mpsc::Sender<ProcessExit>) {
    loop {
        match child.try_wait() {
            Ok(Some(exit)) => {
                let _ = status.send(ProcessExit::Exited(exit));
                return;
            }
            Ok(None) => {}
            Err(error) => {
                let _ = status.send(ProcessExit::Failed(format!(
                    "failed to inspect FFmpeg process: {error}"
                )));
                return;
            }
        }
        match control.recv_timeout(StdDuration::from_millis(100)) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                let result = child.kill().and_then(|()| child.wait());
                let _ = status.send(match result {
                    Ok(_) => ProcessExit::Stopped,
                    Err(error) => {
                        ProcessExit::Failed(format!("failed to stop FFmpeg process: {error}"))
                    }
                });
                return;
            }
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

fn process_error(
    status: ProcessExit,
    worker_error: &Mutex<Option<String>>,
    diagnostics: &Mutex<VecDeque<String>>,
) -> String {
    let process = match status {
        ProcessExit::Exited(status) => format!("FFmpeg decoder exited with {status}"),
        ProcessExit::Failed(error) => error,
        ProcessExit::Stopped => return "FFmpeg decoder stopped".to_owned(),
    };
    let worker = worker_error
        .lock()
        .expect("FFmpeg worker error lock poisoned")
        .clone();
    let diagnostic = diagnostics
        .lock()
        .expect("FFmpeg diagnostics lock poisoned")
        .back()
        .cloned();
    [Some(process), worker, diagnostic]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("; ")
}

fn join_thread(thread: JoinHandle<()>, name: &str) -> Result<()> {
    thread
        .join()
        .map_err(|_| anyhow::anyhow!("{name} thread panicked"))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StreamTotals {
    packets: u64,
    bytes: u64,
    dropped_packets: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamStatistics {
    interval_packets: u64,
    interval_bytes: u64,
    interval_dropped_packets: u64,
    total_packets: u64,
    total_bytes: u64,
    total_dropped_packets: u64,
    bitrate_bps: u64,
}

#[derive(Debug, Default)]
struct StreamTracker {
    active: bool,
    last_packet: Option<Instant>,
    statistics_started: Option<Instant>,
    interval: StreamTotals,
    total: StreamTotals,
}

impl StreamTracker {
    fn record_packet(&mut self, now: Instant, packet_bytes: usize) -> bool {
        let started = !self.active;
        if started {
            self.active = true;
            self.statistics_started = Some(now);
            self.interval = StreamTotals::default();
            self.total = StreamTotals::default();
        }
        let packet_bytes = u64::try_from(packet_bytes).expect("UDP packet length fits in u64");
        self.last_packet = Some(now);
        self.interval.packets += 1;
        self.interval.bytes += packet_bytes;
        self.total.packets += 1;
        self.total.bytes += packet_bytes;
        started
    }
    fn record_drop(&mut self) {
        self.interval.dropped_packets += 1;
        self.total.dropped_packets += 1;
    }
    fn last_packet(&self) -> Option<Instant> {
        self.last_packet.filter(|_| self.active)
    }
    fn take_statistics(&mut self, now: Instant) -> Option<StreamStatistics> {
        if !self.active || self.interval.packets == 0 {
            return None;
        }
        let elapsed_nanos = now
            .saturating_duration_since(
                self.statistics_started
                    .expect("active stream has a statistics start"),
            )
            .as_nanos();
        let bitrate_bps = (u128::from(self.interval.bytes) * 8 * 1_000_000_000)
            .checked_div(elapsed_nanos)
            .unwrap_or(0)
            .try_into()
            .unwrap_or(u64::MAX);
        let statistics = StreamStatistics {
            interval_packets: self.interval.packets,
            interval_bytes: self.interval.bytes,
            interval_dropped_packets: self.interval.dropped_packets,
            total_packets: self.total.packets,
            total_bytes: self.total.bytes,
            total_dropped_packets: self.total.dropped_packets,
            bitrate_bps,
        };
        self.interval = StreamTotals::default();
        self.statistics_started = Some(now);
        Some(statistics)
    }
    fn stop_if_idle(&mut self, now: Instant) -> Option<StreamTotals> {
        let last_packet = self.last_packet()?;
        if now.saturating_duration_since(last_packet) < STREAM_IDLE_TIMEOUT {
            return None;
        }
        self.active = false;
        self.last_packet = None;
        self.statistics_started = None;
        self.interval = StreamTotals::default();
        Some(self.total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    use crate::virtual_camera::FrameUpdate;

    struct FragmentedReader {
        bytes: io::Cursor<Vec<u8>>,
        maximum_read: usize,
    }

    impl Read for FragmentedReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let limit = buffer.len().min(self.maximum_read);
            self.bytes.read(&mut buffer[..limit])
        }
    }

    #[test]
    fn ffmpeg_is_restricted_to_pipe_decoding_and_raw_yuv_output() {
        let command = ffmpeg_command();
        let arguments = command
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(arguments.windows(2).any(|pair| pair == ["-i", "pipe:0"]));
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["-fps_mode", "passthrough"])
        );
        let input = arguments
            .iter()
            .position(|argument| argument == "-i")
            .unwrap();
        assert!(
            arguments[..input]
                .windows(2)
                .any(|pair| pair == ["-threads", "1"])
        );
        assert!(
            arguments[..input]
                .windows(2)
                .any(|pair| pair == ["-flags", "low_delay"])
        );
        assert!(arguments.windows(2).any(|pair| pair == ["-f", "rawvideo"]));
        assert!(arguments.contains(&"yuv420p".to_owned()));
        assert!(
            !arguments
                .iter()
                .any(|argument| argument.contains("udp://") || argument.contains("v4l2"))
        );
    }

    #[test]
    fn tracks_packets_bitrate_drop_stop_and_restart() {
        let start = Instant::now();
        let mut tracker = StreamTracker::default();
        assert!(tracker.record_packet(start, 188));
        assert!(!tracker.record_packet(start + Duration::from_millis(500), 188));
        tracker.record_drop();
        assert_eq!(
            tracker.take_statistics(start + Duration::from_secs(1)),
            Some(StreamStatistics {
                interval_packets: 2,
                interval_bytes: 376,
                interval_dropped_packets: 1,
                total_packets: 2,
                total_bytes: 376,
                total_dropped_packets: 1,
                bitrate_bps: 3_008
            })
        );
        assert_eq!(
            tracker.stop_if_idle(start + Duration::from_millis(3_499)),
            None
        );
        assert_eq!(
            tracker.stop_if_idle(start + Duration::from_millis(3_500)),
            Some(StreamTotals {
                packets: 2,
                bytes: 376,
                dropped_packets: 1
            })
        );
        assert!(tracker.record_packet(start + Duration::from_secs(4), 200));
    }

    #[test]
    fn reassembles_fragmented_raw_frames_and_retains_the_latest_one() {
        let frame_bytes =
            usize::try_from(u64::from(VIDEO_WIDTH) * u64::from(VIDEO_HEIGHT) * 3 / 2).unwrap();
        let mut bytes = vec![1; frame_bytes];
        bytes.extend(vec![2; frame_bytes]);
        let sink = FrameSink::default();
        let errors = Arc::new(Mutex::new(None));

        read_frames(
            FragmentedReader {
                bytes: io::Cursor::new(bytes),
                maximum_read: 8_192,
            },
            sink.clone(),
            errors.clone(),
        );

        assert_eq!(sink.take(), FrameUpdate::New(vec![2; frame_bytes]));
        assert!(
            errors
                .lock()
                .unwrap()
                .as_deref()
                .is_some_and(|error| error.contains("output ended"))
        );
    }

    #[tokio::test]
    async fn retains_the_kernel_assigned_udp_port() {
        let socket = match StreamSocket::bind(Ipv4Addr::LOCALHOST, 0).await {
            Ok(socket) => socket,
            Err(error)
                if error
                    .downcast_ref::<io::Error>()
                    .is_some_and(|error| error.kind() == io::ErrorKind::PermissionDenied) =>
            {
                return;
            }
            Err(error) => panic!("failed to bind loopback UDP socket: {error:#}"),
        };
        assert_eq!(socket.local_address().ip(), &Ipv4Addr::LOCALHOST);
        assert_ne!(socket.local_address().port(), 0);
    }
}
