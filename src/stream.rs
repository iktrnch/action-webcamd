//! UDP reception and lightweight diagnostics for the GoPro webcam stream.
//!
//! This milestone deliberately treats every datagram as opaque payload. MPEG-TS
//! parsing and video decoding belong to later milestones; packet arrival and
//! throughput are enough to distinguish a GoPro-network problem from one in a
//! future decoder.

use std::{
    future,
    net::{Ipv4Addr, SocketAddrV4},
};

use anyhow::{Context, Result};
use tokio::{
    net::UdpSocket,
    task::JoinHandle,
    time::{self, Duration, Instant, MissedTickBehavior},
};
use tracing::{info, warn};

const STREAM_STATISTICS_INTERVAL: Duration = Duration::from_secs(1);
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_UDP_DATAGRAM_SIZE: usize = 65_535;

/// A UDP socket opened for the selected GoPro before webcam mode is enabled.
///
/// Keeping the socket separate from its monitor lets the GoPro control future
/// own it safely: a cancelled control future simply drops the socket, while a
/// successful one promotes it to an active monitor.
pub(crate) struct StreamSocket {
    socket: UdpSocket,
    local_address: SocketAddrV4,
}

impl StreamSocket {
    /// Binds only the selected GoPro USB-network address, preventing an
    /// unrelated local interface from becoming the stream diagnostic source.
    pub(crate) async fn bind(host_address: Ipv4Addr, port: u16) -> Result<Self> {
        let local_address = SocketAddrV4::new(host_address, port);
        let socket = UdpSocket::bind(local_address).await.with_context(|| {
            format!("failed to bind GoPro UDP stream receiver on {local_address}")
        })?;

        info!(
            event = "gopro_stream_receiver_opened",
            address = %local_address,
            "GoPro UDP stream receiver opened"
        );

        Ok(Self {
            socket,
            local_address,
        })
    }

    /// Starts reading an already-bound socket after webcam control succeeds.
    pub(crate) fn monitor(self) -> StreamMonitor {
        StreamMonitor {
            task: Some(tokio::spawn(receive(self.socket, self.local_address))),
        }
    }

    #[cfg(test)]
    fn local_address(&self) -> SocketAddrV4 {
        self.local_address
    }
}

/// Owns the stream-monitor task for one active webcam session.
pub(crate) struct StreamMonitor {
    task: Option<JoinHandle<()>>,
}

impl StreamMonitor {
    /// Cancels the receiver and waits for it to release UDP port ownership.
    pub(crate) async fn stop(mut self) {
        let Some(task) = self.task.take() else {
            return;
        };

        task.abort();
        if let Err(error) = task.await
            && !error.is_cancelled()
        {
            warn!(
                event = "gopro_stream_receiver_failed",
                %error,
                "GoPro UDP stream receiver task ended unexpectedly"
            );
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

/// Receives opaque UDP payloads and reports connection-level stream health.
async fn receive(socket: UdpSocket, local_address: SocketAddrV4) {
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
                        info!(
                            event = "gopro_stream_started",
                            address = %local_address,
                            source = %peer_address,
                            "GoPro UDP stream started"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        event = "gopro_stream_receive_failed",
                        address = %local_address,
                        %error,
                        "GoPro UDP stream receiver failed"
                    );
                    return;
                }
            },
            _ = wait_for_idle_stream(tracker.last_packet()) => {
                let now = Instant::now();
                if let Some(totals) = tracker.stop_if_idle(now) {
                    info!(
                        event = "gopro_stream_stopped",
                        address = %local_address,
                        packets = totals.packets,
                        bytes = totals.bytes,
                        idle_seconds = STREAM_IDLE_TIMEOUT.as_secs(),
                        "GoPro UDP stream stopped after inactivity"
                    );
                }
            },
            _ = statistics_tick.tick() => {
                if let Some(statistics) = tracker.take_statistics(Instant::now()) {
                    info!(
                        event = "gopro_stream_statistics",
                        address = %local_address,
                        interval_packets = statistics.interval_packets,
                        interval_bytes = statistics.interval_bytes,
                        packets = statistics.total_packets,
                        bytes = statistics.total_bytes,
                        bitrate_bps = statistics.bitrate_bps,
                        "GoPro UDP stream statistics"
                    );
                }
            },
        }
    }
}

/// Waits exactly until the active stream becomes idle, or forever before it
/// has received its first packet.
async fn wait_for_idle_stream(last_packet: Option<Instant>) {
    match last_packet {
        Some(last_packet) => time::sleep_until(last_packet + STREAM_IDLE_TIMEOUT).await,
        None => future::pending().await,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StreamTotals {
    packets: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamStatistics {
    interval_packets: u64,
    interval_bytes: u64,
    total_packets: u64,
    total_bytes: u64,
    bitrate_bps: u64,
}

/// Stateful accounting for one contiguous run of received UDP packets.
#[derive(Debug, Default)]
struct StreamTracker {
    active: bool,
    last_packet: Option<Instant>,
    statistics_started: Option<Instant>,
    interval: StreamTotals,
    total: StreamTotals,
}

impl StreamTracker {
    /// Accounts for one packet and returns whether it began a new stream run.
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

    fn last_packet(&self) -> Option<Instant> {
        self.last_packet.filter(|_| self.active)
    }

    /// Produces a completed non-empty reporting window while the stream is up.
    fn take_statistics(&mut self, now: Instant) -> Option<StreamStatistics> {
        if !self.active || self.interval.packets == 0 {
            return None;
        }

        let started = self
            .statistics_started
            .expect("active stream has a statistics start");
        let elapsed = now.saturating_duration_since(started);
        let elapsed_nanos = elapsed.as_nanos();
        let bitrate_bps = (u128::from(self.interval.bytes) * 8 * 1_000_000_000)
            .checked_div(elapsed_nanos)
            .unwrap_or(0)
            .try_into()
            .unwrap_or(u64::MAX);
        let statistics = StreamStatistics {
            interval_packets: self.interval.packets,
            interval_bytes: self.interval.bytes,
            total_packets: self.total.packets,
            total_bytes: self.total.bytes,
            bitrate_bps,
        };
        self.interval = StreamTotals::default();
        self.statistics_started = Some(now);
        Some(statistics)
    }

    /// Marks the current run stopped only after the configured idle threshold.
    fn stop_if_idle(&mut self, now: Instant) -> Option<StreamTotals> {
        let last_packet = self.last_packet()?;
        if !self.active || now.saturating_duration_since(last_packet) < STREAM_IDLE_TIMEOUT {
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

    #[test]
    fn tracks_packets_bitrate_stop_and_restart() {
        let start = Instant::now();
        let mut tracker = StreamTracker::default();

        assert!(tracker.record_packet(start, 188));
        assert!(!tracker.record_packet(start + Duration::from_millis(500), 188));
        assert_eq!(
            tracker.take_statistics(start + Duration::from_secs(1)),
            Some(StreamStatistics {
                interval_packets: 2,
                interval_bytes: 376,
                total_packets: 2,
                total_bytes: 376,
                bitrate_bps: 3_008,
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
            })
        );

        assert!(tracker.record_packet(start + Duration::from_secs(4), 200));
        assert_eq!(
            tracker.take_statistics(start + Duration::from_secs(5)),
            Some(StreamStatistics {
                interval_packets: 1,
                interval_bytes: 200,
                total_packets: 1,
                total_bytes: 200,
                bitrate_bps: 1_600,
            })
        );
    }

    #[tokio::test]
    async fn binds_the_requested_ipv4_address_and_port() {
        let socket = match StreamSocket::bind(Ipv4Addr::LOCALHOST, 0).await {
            Ok(socket) => socket,
            // The Codex sandbox prohibits network syscalls, while this test
            // deliberately covers a real kernel UDP bind on normal hosts.
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied) =>
            {
                return;
            }
            Err(error) => panic!("failed to bind loopback UDP socket: {error:#}"),
        };
        assert_eq!(socket.local_address().ip(), &Ipv4Addr::LOCALHOST);
        assert_ne!(socket.local_address().port(), 0);
    }
}
