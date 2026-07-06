mod collectors;
mod csv_sink;
#[cfg(feature = "nominal")]
mod nominal_sink;
mod output;
mod pipeline;
mod scheduler;

use clap::Parser;
use collectors::{Collector, Tier};
use output::{ConsoleSink, OutputDispatcher, OutputSink};
use pipeline::{LogEntry, MetricPipeline};
use scheduler::ScheduledCollector;
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "usage-logger", about = "macOS system metrics logger")]
struct Args {
    /// Polling interval in milliseconds
    #[arg(short, long, default_value = "1000")]
    interval: u64,

    /// Pretty-print JSON output
    #[arg(short, long)]
    pretty: bool,

    /// Disable specific collectors (comma-separated: cpu,memory,disk,network,smc,battery,thermal_state,system)
    #[arg(long, value_delimiter = ',')]
    disable: Vec<String>,

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
    let disabled: HashSet<&str> = args.disable.iter().map(|s| s.as_str()).collect();

    let mut sys = sysinfo::System::new_all();
    sys.refresh_cpu_all();
    thread::sleep(Duration::from_millis(200));

    let all: Vec<Box<dyn Collector>> = vec![
        Box::new(collectors::system_info::SystemInfoCollector::new()),
        Box::new(collectors::cpu::CpuCollector::new()),
        Box::new(collectors::memory::MemoryCollector::new()),
        Box::new(collectors::disk::DiskCollector::new()),
        Box::new(collectors::network::NetworkCollector::new()),
        Box::new(collectors::smc::SmcCollector::new()),
        Box::new(collectors::battery::BatteryCollector::new()),
        Box::new(collectors::thermal::ThermalCollector::new()),
    ];

    let mut scheduled: Vec<ScheduledCollector> = all
        .into_iter()
        .filter(|c| !disabled.contains(c.name()))
        .map(|c| ScheduledCollector::new(c, args.interval))
        .collect();

    for sc in &scheduled {
        let tier = match sc.tier() {
            Tier::Fast => "fast",
            Tier::Medium => "medium",
            Tier::Slow => "slow",
        };
        eprintln!("  {} ({}): every {}ms", sc.name(), tier, sc.interval_ms());
    }
    eprintln!(
        "Base interval: {}ms. Delta: {}. Press Ctrl+C to stop.",
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
            match build_nominal_sink(token, dataset, args.nominal_url.as_deref(), args.nominal_fallback.as_deref()) {
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

    // Pipeline (replaces DeltaEncoder)
    let mut pipeline = MetricPipeline::new(if args.no_delta { 1 } else { args.snapshot_every });

    // Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let running_flag = running.clone();
    ctrlc::set_handler(move || {
        running_flag.store(false, Ordering::SeqCst);
    })
    .expect("Failed to set Ctrl+C handler");

    // CPU refresh timing
    let medium_interval = Duration::from_millis(200.max(args.interval));
    let mut last_cpu_refresh = Instant::now() - Duration::from_secs(999);

    let mut drop_count: u64 = 0;
    let mut last_drop_log = Instant::now();

    while running.load(Ordering::SeqCst) {
        let start = Instant::now();

        if last_cpu_refresh.elapsed() >= medium_interval {
            sys.refresh_cpu_all();
            last_cpu_refresh = Instant::now();
        }
        sys.refresh_memory();

        let mut record = serde_json::Map::new();
        record.insert(
            "timestamp".into(),
            json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        );
        record.insert("interval_ms".into(), json!(args.interval));

        for sc in &mut scheduled {
            let result = sc.maybe_collect(&sys);
            if result.fresh && let Some(v) = result.value {
                record.insert(result.name.into(), v);
            }
        }

        record.insert(
            "collect_ms".into(),
            json!(start.elapsed().as_secs_f64() * 1000.0),
        );

        let mut metric_record = pipeline.process(record);

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

    // Clean shutdown: drop sender so dispatcher drains and flushes
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

    // nominal-streaming's NominalCoreConsumer needs a tokio runtime handle
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("nominal-rt")
        .build()?;
    let handle = runtime.handle().clone();

    // Leak the runtime so it lives for the program's lifetime.
    // The stream's Drop impl flushes before the process exits.
    Box::leak(Box::new(runtime));

    let mut builder = NominalDatasetStreamBuilder::new()
        .stream_to_core(bearer, rid, handle);

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

    // Log writer uses the conjure REST endpoint: POST /storage/writer/v1/logs/{dataSourceRid}
    let log_writer = nominal_sink::LogWriter::new(base_url, dataset, token);

    Ok(nominal_sink::NominalSink::new(stream, Some(log_writer)))
}
