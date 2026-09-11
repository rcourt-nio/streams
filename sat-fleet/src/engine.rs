use crate::config::GroundStationConfig;
use crate::satgen::SatelliteConfig;
use crate::{orbital, telemetry};
use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const CHANNELS: [&str; 10] = [
    "latitude_deg",
    "longitude_deg",
    "altitude_km",
    "elevation_deg",
    "signal_strength_dbm",
    "data_throughput_mbps",
    "battery_level_percent",
    "solar_panel_power_w",
    "temperature_c",
    "doppler_shift_khz",
];

#[derive(Debug, Default)]
pub struct EngineStats {
    pub enqueued: AtomicU64,
    pub sent: AtomicU64,
    pub failed_requests: AtomicU64,
    pub failed_points: AtomicU64,
    pub ticks: AtomicU64,
    pub skipped_ticks: AtomicU64,
    /// log.system write failures (maintained by the log thread, not the engine).
    pub log_failures: AtomicU64,
}

/// Counts points actually sent to (or rejected by) the Nominal ingest API,
/// as opposed to `enqueued`, which counts points buffered locally.
#[derive(Debug)]
pub struct StatsListener(pub Arc<EngineStats>);

impl nominal_streaming::listener::NominalStreamListener for StatsListener {
    fn on_error(&self, _error: &dyn std::error::Error, request: &WriteRequestNominal) {
        self.0.failed_requests.fetch_add(1, Ordering::Relaxed);
        self.0
            .failed_points
            .fetch_add(count_points(request), Ordering::Relaxed);
    }

    fn on_success(&self, request: &WriteRequestNominal) {
        self.0.sent.fetch_add(count_points(request), Ordering::Relaxed);
    }
}

fn count_points(request: &WriteRequestNominal) -> u64 {
    request
        .series
        .iter()
        .filter_map(|s| s.points.as_ref())
        .filter_map(|p| p.points_type.as_ref())
        .map(|pt| match pt {
            PointsType::DoublePoints(p) => p.points.len() as u64,
            PointsType::StringPoints(p) => p.points.len() as u64,
            PointsType::IntegerPoints(p) => p.points.len() as u64,
            PointsType::Uint64Points(p) => p.points.len() as u64,
            PointsType::StructPoints(p) => p.points.len() as u64,
            // We never send array points; counting them precisely isn't needed.
            PointsType::ArrayPoints(_) => 0,
        })
        .sum()
}

pub struct Engine {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Engine {
    pub fn start(
        stream: NominalDatasetStream,
        sats: Vec<SatelliteConfig>,
        ground_station: Option<GroundStationConfig>,
        rate_hz: f64,
        stats: Arc<EngineStats>,
    ) -> Engine {
        let running = Arc::new(AtomicBool::new(true));
        let flag = running.clone();
        let handle = std::thread::Builder::new()
            .name("fleet-engine".into())
            .spawn(move || run_loop(stream, sats, ground_station, rate_hz, stats, flag))
            .expect("failed to spawn engine thread");
        Engine {
            running,
            handle: Some(handle),
        }
    }

    /// Signals the tick loop to stop and blocks until the stream has been
    /// dropped (which flushes remaining buffered points). Call off the UI
    /// thread: the final flush can take a few seconds with a large backlog.
    pub fn stop(mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct SatRuntime {
    config: SatelliteConfig,
    descriptors: Vec<ChannelDescriptor>,
}

fn run_loop(
    stream: NominalDatasetStream,
    sats: Vec<SatelliteConfig>,
    ground_station: Option<GroundStationConfig>,
    rate_hz: f64,
    stats: Arc<EngineStats>,
    running: Arc<AtomicBool>,
) {
    let sats: Vec<SatRuntime> = sats
        .into_iter()
        .map(|config| SatRuntime {
            descriptors: CHANNELS
                .iter()
                .map(|ch| ChannelDescriptor::with_tags(*ch, [("satellite", config.name.as_str())]))
                .collect(),
            config,
        })
        .collect();

    let interval = Duration::from_secs_f64(1.0 / rate_hz);
    let mut next = Instant::now();

    while running.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now < next {
            std::thread::sleep(next - now);
        }

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let t = ts.as_secs_f64();

        for sat in &sats {
            let state = orbital::compute_state(&sat.config, ground_station.as_ref(), t);
            let telem = telemetry::compute(&sat.config, &state, t);
            let values = [
                state.latitude_deg,
                state.longitude_deg,
                state.altitude_km,
                state.elevation_deg,
                telem.signal_strength_dbm,
                telem.data_throughput_mbps,
                telem.battery_level_percent,
                telem.solar_panel_power_w,
                telem.temperature_c,
                telem.doppler_shift_khz,
            ];
            for (descriptor, value) in sat.descriptors.iter().zip(values) {
                stream.enqueue(
                    descriptor,
                    vec![DoublePoint {
                        timestamp: Some(ts.into_timestamp()),
                        value,
                    }],
                );
            }
            stats
                .enqueued
                .fetch_add(CHANNELS.len() as u64, Ordering::Relaxed);
        }
        stats.ticks.fetch_add(1, Ordering::Relaxed);

        next += interval;
        let now = Instant::now();
        if now > next {
            // Fell behind: skip missed ticks instead of bursting to catch up.
            let missed = ((now - next).as_secs_f64() / interval.as_secs_f64()).ceil() as u32;
            stats
                .skipped_ticks
                .fetch_add(missed as u64, Ordering::Relaxed);
            next += interval * missed;
        }
    }
    // stream drops here, flushing whatever is still buffered
}
