mod collectors;
mod config;
mod csv_sink;
#[cfg(feature = "nominal")]
mod nominal_sink;
mod noise;
mod orbital;
mod output;
mod pipeline;
mod scheduler;
mod telemetry;

use clap::Parser;
use collectors::Collector;
use collectors::satellite::SatelliteCollector;
use output::{ConsoleSink, OutputDispatcher, OutputSink};
use pipeline::{LogEntry, MetricPipeline, MetricSample};
use scheduler::ScheduledCollector;
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "sat-streams", about = "Satellite telemetry data generator")]
struct Args {
    /// Polling interval in milliseconds
    #[arg(short, long, default_value = "1000")]
    interval: u64,

    /// Pretty-print JSON output
    #[arg(short, long)]
    pretty: bool,

    /// Path to satellite constellation config file
    #[arg(short, long, default_value = "satellites.toml")]
    config: String,

    /// Only include specific satellites (comma-separated names)
    #[arg(long, value_delimiter = ',')]
    satellites: Vec<String>,

    /// Disable delta encoding (send full snapshots every cycle)
    #[arg(long)]
    no_delta: bool,

    /// Send a full snapshot every N cycles (default: 100)
    #[arg(long, default_value = "100")]
    snapshot_every: u64,

    /// Suppress console (stdout) output
    #[arg(long)]
    no_console: bool,

    /// Write CSV output to file
    #[arg(long)]
    csv: Option<String>,

    /// Nominal streaming auth token
    #[cfg(feature = "nominal")]
    #[arg(long)]
    nominal_token: Option<String>,

    /// Nominal dataset RID
    #[cfg(feature = "nominal")]
    #[arg(long)]
    nominal_dataset: Option<String>,

    /// Nominal API URL
    #[cfg(feature = "nominal")]
    #[arg(long)]
    nominal_url: Option<String>,

    /// Nominal Avro file fallback path
    #[cfg(feature = "nominal")]
    #[arg(long)]
    nominal_fallback: Option<String>,
}

fn main() {
    let args = Args::parse();

    // Load constellation config
    let config_str = std::fs::read_to_string(&args.config).unwrap_or_else(|e| {
        eprintln!("Failed to read config file '{}': {e}", args.config);
        std::process::exit(1);
    });
    let constellation: config::ConstellationConfig = toml::from_str(&config_str).unwrap_or_else(|e| {
        eprintln!("Failed to parse config file '{}': {e}", args.config);
        std::process::exit(1);
    });

    let filter: HashSet<String> = args
        .satellites
        .iter()
        .map(|s| s.to_lowercase())
        .collect();

    let all: Vec<Box<dyn Collector>> = constellation
        .satellites
        .iter()
        .filter(|s| filter.is_empty() || filter.contains(&s.name.to_lowercase()))
        .map(|s| {
            Box::new(SatelliteCollector::new(
                s.clone(),
                constellation.ground_station.clone(),
            )) as Box<dyn Collector>
        })
        .collect();

    if all.is_empty() {
        eprintln!("No satellites matched. Available:");
        for s in &constellation.satellites {
            eprintln!("  - {}", s.name);
        }
        std::process::exit(1);
    }

    let mut scheduled: Vec<ScheduledCollector> = all
        .into_iter()
        .map(|c| ScheduledCollector::new(c, args.interval))
        .collect();

    eprintln!("Satellites ({}):", scheduled.len());
    for sc in &scheduled {
        eprintln!("  {} (every {}ms)", sc.name(), sc.interval_ms());
    }
    if let Some(gs) = &constellation.ground_station {
        eprintln!(
            "Ground station: {} ({:.4}, {:.4})",
            gs.name, gs.latitude_deg, gs.longitude_deg
        );
    }
    eprintln!(
        "Interval: {}ms. Delta: {}. Press Ctrl+C to stop.",
        args.interval,
        if args.no_delta { "off" } else { "on" },
    );

    // Build sinks
    let mut sinks: Vec<Box<dyn OutputSink>> = Vec::new();

    if !args.no_console {
        sinks.push(Box::new(ConsoleSink::new(args.pretty)));
    }

    if let Some(csv_path) = &args.csv {
        match csv_sink::CsvSink::new(csv_path) {
            Ok(sink) => {
                eprintln!("CSV output: {csv_path}");
                sinks.push(Box::new(sink));
            }
            Err(e) => {
                eprintln!("Failed to create CSV file: {e}");
                std::process::exit(1);
            }
        }
    }

    #[cfg(feature = "nominal")]
    {
        if let (Some(token), Some(dataset)) = (&args.nominal_token, &args.nominal_dataset) {
            match build_nominal_sink(
                token,
                dataset,
                args.nominal_url.as_deref(),
                args.nominal_fallback.as_deref(),
            ) {
                Ok(sink) => {
                    eprintln!("Nominal streaming enabled");
                    sinks.push(Box::new(sink));
                }
                Err(e) => {
                    eprintln!("Failed to initialize Nominal sink: {e}");
                    std::process::exit(1);
                }
            }
        }
    }

    if sinks.is_empty() {
        eprintln!("Warning: no output sinks enabled");
    }

    // Channel + dispatcher
    let (tx, rx) = mpsc::sync_channel(256);
    let dispatcher = OutputDispatcher::spawn(rx, sinks);

    // Pipeline
    let mut pipeline = MetricPipeline::new(if args.no_delta { 1 } else { args.snapshot_every });

    // Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let running_flag = running.clone();
    ctrlc::set_handler(move || {
        running_flag.store(false, Ordering::SeqCst);
    })
    .expect("Failed to set Ctrl+C handler");

    let mut drop_count: u64 = 0;
    let mut last_drop_log = Instant::now();

    while running.load(Ordering::SeqCst) {
        let start = Instant::now();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();
        let timestamp_secs = now.as_secs_f64();

        let mut record = serde_json::Map::new();
        record.insert(
            "timestamp".into(),
            json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        );
        record.insert("interval_ms".into(), json!(args.interval));

        for sc in &mut scheduled {
            let result = sc.maybe_collect(timestamp_secs);
            if result.fresh {
                if let Some(v) = result.value {
                    record.insert(result.name.into(), v);
                }
            }
        }

        record.insert(
            "collect_ms".into(),
            json!(start.elapsed().as_secs_f64() * 1000.0),
        );

        let mut metric_record = pipeline.process(record);

        // Add common tagged channels: for each satellite-prefixed sample,
        // create a mirrored sample with the satellite name stripped from the
        // path and added as a {satellite: "name"} tag instead.
        let satellite_names: HashSet<&str> = scheduled.iter().map(|sc| sc.name()).collect();
        let tagged: Vec<MetricSample> = metric_record
            .samples
            .iter()
            .filter(|s| s.tags.is_none())
            .filter_map(|s| {
                let dot = s.path.find('.')?;
                let prefix = &s.path[..dot];
                if !satellite_names.contains(prefix) {
                    return None;
                }
                let field = &s.path[dot + 1..];
                Some(MetricSample {
                    path: field.to_string(),
                    tags: Some(BTreeMap::from([(
                        "satellite".to_string(),
                        prefix.to_string(),
                    )])),
                    value: s.value.clone(),
                    changed: s.changed,
                })
            })
            .collect();
        metric_record.samples.extend(tagged);

        // Inject drop warning as a log entry if we've been dropping
        if drop_count > 0 && last_drop_log.elapsed() >= Duration::from_secs(5) {
            let msg = format!("dropped {drop_count} records (dispatcher behind)");
            eprintln!("Warning: {msg}");
            let mut args = std::collections::HashMap::new();
            args.insert("dropped".into(), drop_count.to_string());
            metric_record.logs.push(LogEntry {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default(),
                channel: "log.system".into(),
                message: msg,
                args,
            });
            drop_count = 0;
            last_drop_log = Instant::now();
        }

        match tx.try_send(metric_record) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_)) => {
                drop_count += 1;
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                eprintln!("Dispatcher disconnected");
                break;
            }
        }

        thread::sleep(Duration::from_millis(args.interval));
    }

    // Clean shutdown
    drop(tx);
    dispatcher.join();
}

#[cfg(feature = "nominal")]
fn build_nominal_sink(
    token: &str,
    dataset: &str,
    url: Option<&str>,
    fallback: Option<&str>,
) -> Result<nominal_sink::NominalSink, Box<dyn std::error::Error>> {
    use nominal_streaming::prelude::{BearerToken, ResourceIdentifier};
    use nominal_streaming::stream::{NominalDatasetStreamBuilder, NominalStreamOpts};

    let bearer = BearerToken::new(token)?;
    let rid = ResourceIdentifier::new(dataset)?;

    let base_url = url.unwrap_or("https://api.gov.nominal.io/api");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("nominal-rt")
        .build()?;
    let handle = runtime.handle().clone();
    Box::leak(Box::new(runtime));

    let mut builder = NominalDatasetStreamBuilder::new().stream_to_core(bearer, rid, handle);

    if url.is_some() {
        builder = builder.with_options(NominalStreamOpts {
            base_api_url: base_url.to_string(),
            ..NominalStreamOpts::default()
        });
    }

    if let Some(path) = fallback {
        builder = builder.with_file_fallback(path);
    }

    let stream = builder.build();
    let log_writer = nominal_sink::LogWriter::new(base_url, dataset, token);

    Ok(nominal_sink::NominalSink::new(stream, Some(log_writer)))
}
