//! Throughput probe for the Nominal streaming pipeline.
//!
//! Mimics the sat-fleet engine's write pattern (N sats x 10 channels, tagged)
//! but with synthetic values under BENCH-* satellite tags so it never touches
//! real satellite series or assets.
//!
//! Usage: cargo run --release --example throughput -- [sats] [rate_hz] [secs] [delay_ms] [buffered] [dispatchers]

use nominal_streaming::listener::NominalStreamListener;
use nominal_streaming::prelude::*;
use nominal_streaming::stream::{NominalDatasetStreamBuilder, NominalStreamOpts};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHANNELS: [&str; 10] = [
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
struct Counter {
    sent: AtomicU64,
    failed: AtomicU64,
    requests: AtomicU64,
}

fn count_points(request: &WriteRequestNominal) -> u64 {
    request
        .series
        .iter()
        .filter_map(|s| s.points.as_ref())
        .filter_map(|p| p.points_type.as_ref())
        .map(|pt| match pt {
            PointsType::DoublePoints(p) => p.points.len() as u64,
            _ => 0,
        })
        .sum()
}

impl NominalStreamListener for Counter {
    fn on_error(&self, error: &dyn std::error::Error, request: &WriteRequestNominal) {
        eprintln!("request error: {error}");
        self.failed
            .fetch_add(count_points(request), Ordering::Relaxed);
    }

    fn on_success(&self, request: &WriteRequestNominal) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.sent
            .fetch_add(count_points(request), Ordering::Relaxed);
    }
}

fn arg(n: usize, default: f64) -> f64 {
    std::env::args()
        .nth(n)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let _ = dotenvy::from_path(concat!(env!("CARGO_MANIFEST_DIR"), "/.env"));
    let token = std::env::var("NOMINAL_TOKEN").expect("NOMINAL_TOKEN");
    let dataset = std::env::var("NOMINAL_DATASET").expect("NOMINAL_DATASET");
    let url = std::env::var("NOMINAL_URL").expect("NOMINAL_URL");

    let sats = arg(1, 50.0) as usize;
    let rate_hz = arg(2, 100.0);
    let secs = arg(3, 20.0) as u64;
    let delay_ms = arg(4, 100.0) as u64;
    let buffered = arg(5, 4.0) as usize;
    let dispatchers = arg(6, 8.0) as usize;
    let target = sats as f64 * CHANNELS.len() as f64 * rate_hz;

    println!(
        "probe: {sats} sats x {} ch @ {rate_hz} Hz = {target:.0} pts/s target, {secs}s | opts: delay={delay_ms}ms buffered={buffered} dispatchers={dispatchers}",
        CHANNELS.len(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();

    let counter = Arc::new(Counter::default());
    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(
            BearerToken::new(&token).unwrap(),
            ResourceIdentifier::new(&dataset).unwrap(),
            runtime.handle().clone(),
        )
        .with_options(NominalStreamOpts {
            base_api_url: url,
            max_request_delay: Duration::from_millis(delay_ms),
            max_buffered_requests: buffered,
            request_dispatcher_tasks: dispatchers,
            ..NominalStreamOpts::default()
        })
        .add_listener(counter.clone())
        .build();

    let descriptors: Vec<Vec<ChannelDescriptor>> = (0..sats)
        .map(|i| {
            let name = format!("BENCH-{i:05}");
            CHANNELS
                .iter()
                .map(|ch| ChannelDescriptor::with_tags(*ch, [("satellite", name.as_str())]))
                .collect()
        })
        .collect();

    let interval = Duration::from_secs_f64(1.0 / rate_hz);
    let start = Instant::now();
    let mut next = Instant::now();
    let mut enqueued: u64 = 0;
    let mut last_report = Instant::now();
    let mut last_sent: u64 = 0;

    while start.elapsed() < Duration::from_secs(secs) {
        let now = Instant::now();
        if now < next {
            std::thread::sleep(next - now);
        }
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let t = ts.as_secs_f64();
        for descs in &descriptors {
            for (k, cd) in descs.iter().enumerate() {
                stream.enqueue(
                    cd,
                    vec![DoublePoint {
                        timestamp: Some(ts.into_timestamp()),
                        value: (t * 0.1 + k as f64).sin(),
                    }],
                );
                enqueued += 1;
            }
        }
        next += interval;
        let now = Instant::now();
        if now > next {
            let missed = ((now - next).as_secs_f64() / interval.as_secs_f64()).ceil() as u32;
            next += interval * missed;
        }

        if last_report.elapsed() >= Duration::from_secs(2) {
            let sent = counter.sent.load(Ordering::Relaxed);
            let rate = (sent - last_sent) as f64 / last_report.elapsed().as_secs_f64();
            println!(
                "t={:>4.0}s enqueued={enqueued} sent={sent} backlog={} send_rate={rate:.0} pts/s",
                start.elapsed().as_secs_f64(),
                enqueued.saturating_sub(sent),
            );
            last_sent = sent;
            last_report = Instant::now();
        }
    }

    println!("draining...");
    let drain_start = Instant::now();
    drop(stream);
    let sent = counter.sent.load(Ordering::Relaxed);
    let failed = counter.failed.load(Ordering::Relaxed);
    let requests = counter.requests.load(Ordering::Relaxed);
    println!(
        "final: enqueued={enqueued} sent={sent} failed={failed} requests={requests} drain_time={:.1}s avg_send_rate={:.0} pts/s (target {target:.0})",
        drain_start.elapsed().as_secs_f64(),
        sent as f64 / start.elapsed().as_secs_f64(),
    );
}
