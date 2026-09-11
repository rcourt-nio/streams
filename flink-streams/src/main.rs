use clap::Parser;
use nominal_streaming::prelude::*;
use nominal_streaming::stream::{
    NominalDatasetStream, NominalDatasetStreamBuilder, NominalStreamOpts,
};
use nominal_streaming::types::ChannelDescriptor;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::f64::consts::TAU;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

#[derive(Parser)]
#[command(about = "Stream contrived lateness scenarios to Nominal for testing Flink allowed-lateness behaviour")]
struct Args {
    /// Nominal API token
    #[arg(long, env = "NOMINAL_TOKEN")]
    nominal_token: String,

    /// Nominal dataset resource identifier
    #[arg(long, env = "NOMINAL_DATASET")]
    nominal_dataset: String,

    /// Nominal API base URL
    #[arg(long, env = "NOMINAL_URL")]
    nominal_url: Option<String>,

    /// Comma-separated scenario names, or "all"
    #[arg(long, value_delimiter = ',', default_value = "all")]
    scenario: Vec<String>,

    /// Prefix for channel names (channel = prefix + scenario name)
    #[arg(long, default_value = "")]
    channel_prefix: String,

    /// Print the scenarios and exit
    #[arg(long)]
    list: bool,

    /// Suppress all console output
    #[arg(long)]
    no_console: bool,
}

// ---------------------------------------------------------------------------
// Scenario model
//
// Every scenario is a periodic data source. On data tick `n` (data time
// `t = n * period`) it returns zero or more events. Each event carries the
// point's timestamp offset from `t`, how long after the tick it is handed to
// the SDK, and its value. The runner turns those into scheduled deliveries.
// ---------------------------------------------------------------------------

struct Ev {
    /// Seconds after this tick's wall time at which the point is delivered.
    deliver_after: f64,
    /// Seconds added to the tick's wall time to form the point's timestamp.
    ts_offset: f64,
}

impl Ev {
    fn on_time() -> Self {
        Ev { deliver_after: 0.0, ts_offset: 0.0 }
    }
    fn late(deliver_after: f64) -> Self {
        Ev { deliver_after, ts_offset: 0.0 }
    }
}

/// Every scenario carries the same value: a sine wave with this period, so a
/// dropped point reads as a notch in the curve and a recovery fills it in.
const SINE_PERIOD_S: f64 = 5.0;

struct Scenario {
    name: &'static str,
    summary: &'static str,
    period: Duration,
    /// `n` is the tick index, `t` the tick's wall time in unix seconds. Ticks
    /// are aligned to multiples of the period, so `t % SINE_PERIOD_S` is the
    /// sine phase.
    emit: fn(n: u64, t: f64, rng: &mut Rng) -> Vec<Ev>,
}

/// Small deterministic xorshift so every run produces the same delivery
/// pattern for the chaos scenario.
struct Rng(u64);

impl Rng {
    fn next_f64(&mut self) -> f64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x >> 11) as f64 / (1u64 << 53) as f64
    }
}

const HZ5: Duration = Duration::from_millis(200);
const HZ50: Duration = Duration::from_millis(20);

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "sine-5hz",
        summary: "perfect sine, every point on time",
        period: HZ5,
        emit: |_, _, _| vec![Ev::on_time()],
    },
    Scenario {
        name: "sine-50hz",
        summary: "perfect sine, every point on time",
        period: HZ50,
        emit: |_, _, _| vec![Ev::on_time()],
    },
    Scenario {
        name: "constant-delay",
        summary: "every point delivered 6 s after its timestamp",
        period: HZ5,
        emit: |_, _, _| vec![Ev::late(6.0)],
    },
    Scenario {
        name: "skew-forward",
        summary: "every point stamped 2 s in the future, delivered on time",
        period: HZ5,
        emit: |_, _, _| vec![Ev { deliver_after: 0.0, ts_offset: 2.0 }],
    },
    Scenario {
        name: "tail-swap",
        summary: "the whole 1 -> 0 quarter of each wave is held and delivered 50 ms after the first point past the zero crossing",
        period: HZ5,
        emit: |_, t, _| {
            let step = HZ5.as_secs_f64();
            let per = (SINE_PERIOD_S / step).round() as i64; // samples per wave
            let k = ((t % SINE_PERIOD_S) / step).round() as i64 % per;
            // The 1 -> 0 quarter runs from just after the peak (per/4) to the
            // last sample before the midpoint crossing (per/2). Hold all of it
            // and release it just after the first sample past the crossing.
            let first = per / 4 + 1;
            let release = per / 2 + 1;
            if k >= first && k < release {
                vec![Ev::late((release - k) as f64 * step + 0.05)]
            } else {
                vec![Ev::on_time()]
            }
        },
    },
    Scenario {
        name: "chaos-sine",
        summary: "delivery delay random: 95% uniform 0-10 s, 5% uniform 10-15 s",
        period: HZ50,
        emit: |_, _, rng| {
            let u = rng.next_f64();
            let delay = if rng.next_f64() < 0.95 { u * 10.0 } else { 10.0 + u * 5.0 };
            vec![Ev::late(delay)]
        },
    },
    Scenario {
        name: "future-stamps",
        summary: "every 4th point stamped 3 s in the future, delivered on time",
        period: HZ5,
        emit: |n, _, _| match n % 4 {
            3 => vec![Ev { deliver_after: 0.0, ts_offset: 3.0 }],
            _ => vec![Ev::on_time()],
        },
    },
    Scenario {
        name: "pair-swap",
        summary: "pairs (t, t+0.2); the t+0.2 point is delivered 50 ms after the t+0.4 point",
        period: HZ5,
        emit: |n, _, _| match n % 2 {
            1 => vec![Ev::late(0.25)],
            _ => vec![Ev::on_time()],
        },
    },
];

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

struct Sched {
    deliver_at: Instant,
    seq: u64,
    point: DoublePoint,
}

// Min-heap on (deliver_at, seq) so equal-time deliveries keep enqueue order.
impl PartialEq for Sched {
    fn eq(&self, o: &Self) -> bool {
        self.deliver_at == o.deliver_at && self.seq == o.seq
    }
}
impl Eq for Sched {}
impl PartialOrd for Sched {
    fn partial_cmp(&self, o: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(o))
    }
}
impl Ord for Sched {
    fn cmp(&self, o: &Self) -> CmpOrdering {
        o.deliver_at
            .cmp(&self.deliver_at)
            .then_with(|| o.seq.cmp(&self.seq))
    }
}

fn to_timestamp(secs: f64) -> Timestamp {
    let whole = secs.floor();
    Timestamp {
        seconds: whole as i64,
        nanos: ((secs - whole) * 1e9).round() as i32,
    }
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn run_scenario(
    sc: &'static Scenario,
    channel: String,
    stream: Arc<NominalDatasetStream>,
    running: Arc<AtomicBool>,
    quiet: bool,
) {
    let ch = ChannelDescriptor::new(&channel);
    let period = sc.period.as_secs_f64();
    // Align the first tick to a multiple of the period in wall time so every
    // timestamp sits on the grid and the sine phase is predictable.
    let wall_now = unix_now();
    let start_wall = (wall_now / period).ceil() * period;
    let start = Instant::now() + Duration::from_secs_f64(start_wall - wall_now);
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut heap: BinaryHeap<Sched> = BinaryHeap::new();
    let mut n: u64 = 0;
    let mut seq: u64 = 0;
    let mut delivered: u64 = 0;

    while running.load(Ordering::SeqCst) {
        let now = Instant::now();

        // Generate every tick that has come due.
        loop {
            let tick_at = start + sc.period * n as u32;
            if tick_at > now {
                break;
            }
            let tick_wall = start_wall + n as f64 * period;
            for ev in (sc.emit)(n, tick_wall, &mut rng) {
                let ts = tick_wall + ev.ts_offset;
                heap.push(Sched {
                    deliver_at: tick_at + Duration::from_secs_f64(ev.deliver_after),
                    seq,
                    point: DoublePoint {
                        timestamp: Some(to_timestamp(ts)),
                        value: (TAU * ts / SINE_PERIOD_S).sin(),
                    },
                });
                seq += 1;
            }
            n += 1;
        }

        // Deliver everything due, in one enqueue so intra-batch order holds.
        let mut due: Vec<DoublePoint> = Vec::new();
        while heap.peek().is_some_and(|s| s.deliver_at <= now) {
            due.push(heap.pop().unwrap().point);
        }
        if !due.is_empty() {
            if !quiet {
                let lag: Vec<String> = due
                    .iter()
                    .map(|p| {
                        let ts = p.timestamp.as_ref().unwrap();
                        let ts_f = ts.seconds as f64 + ts.nanos as f64 / 1e9;
                        format!("{:+.2}s={:.2}", ts_f - unix_now(), p.value)
                    })
                    .collect();
                eprintln!("[{}] {} pt(s) (ts-now=value): {}", sc.name, due.len(), lag.join(" "));
            }
            delivered += due.len() as u64;
            stream.enqueue(&ch, due);
        }

        // Sleep until the next tick or delivery, capped so Ctrl-C stays snappy.
        let next_tick = start + sc.period * n as u32;
        let next = heap
            .peek()
            .map(|s| s.deliver_at.min(next_tick))
            .unwrap_or(next_tick);
        let wait = next.saturating_duration_since(Instant::now()).min(Duration::from_millis(200));
        thread::sleep(wait);
    }

    if !quiet {
        eprintln!("[{}] stopped after {} points ({} still scheduled, discarded)", sc.name, delivered, heap.len());
    }
}

fn build_stream(args: &Args) -> NominalDatasetStream {
    let bearer = BearerToken::new(&args.nominal_token).expect("invalid token");
    let rid = ResourceIdentifier::new(&args.nominal_dataset).expect("invalid dataset RID");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("nominal-rt")
        .build()
        .expect("failed to build tokio runtime");
    let handle = runtime.handle().clone();
    Box::leak(Box::new(runtime));

    let defaults = NominalStreamOpts::default();
    let opts = NominalStreamOpts {
        base_api_url: args
            .nominal_url
            .clone()
            .unwrap_or(defaults.base_api_url.clone()),
        // One dispatcher so requests leave in enqueue order. Several scenarios
        // depend on delivery order and the default 8 tasks can reorder them.
        request_dispatcher_tasks: 1,
        max_buffered_requests: 1,
        ..defaults
    };

    NominalDatasetStreamBuilder::new()
        .stream_to_core(bearer, rid, handle)
        .with_options(opts)
        .build()
}

fn main() {
    let args = Args::parse();

    if args.list {
        for sc in SCENARIOS {
            println!("{:<18} {} Hz  {}", sc.name, 1.0 / sc.period.as_secs_f64(), sc.summary);
        }
        return;
    }

    let selected: Vec<&'static Scenario> = if args.scenario.iter().any(|s| s == "all") {
        SCENARIOS.iter().collect()
    } else {
        args.scenario
            .iter()
            .map(|name| {
                SCENARIOS
                    .iter()
                    .find(|sc| sc.name == name)
                    .unwrap_or_else(|| {
                        eprintln!("unknown scenario '{name}'; run with --list to see the options");
                        std::process::exit(2);
                    })
            })
            .collect()
    };

    let quiet = args.no_console;
    if !quiet {
        eprintln!("connecting to Nominal...");
    }
    let stream = Arc::new(build_stream(&args));
    if !quiet {
        eprintln!(
            "connected; running {} scenario(s): {}. Ctrl-C to stop",
            selected.len(),
            selected.iter().map(|s| s.name).collect::<Vec<_>>().join(", ")
        );
    }

    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        ctrlc::set_handler(move || r.store(false, Ordering::SeqCst))
            .expect("failed to set Ctrl-C handler");
    }

    let handles: Vec<_> = selected
        .into_iter()
        .map(|sc| {
            let channel = format!("{}{}", args.channel_prefix, sc.name);
            let stream = stream.clone();
            let running = running.clone();
            thread::Builder::new()
                .name(sc.name.to_string())
                .spawn(move || run_scenario(sc, channel, stream, running, quiet))
                .expect("failed to spawn scenario thread")
        })
        .collect();

    for h in handles {
        let _ = h.join();
    }

    if !quiet {
        eprintln!("shutting down (flushing stream)...");
    }
    drop(stream);
    if !quiet {
        eprintln!("done.");
    }
}
