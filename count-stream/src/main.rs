use clap::Parser;
use nominal_streaming::prelude::*;
use nominal_streaming::stream::{
    NominalDatasetStream, NominalDatasetStreamBuilder, NominalStreamOpts,
};
use nominal_streaming::types::ChannelDescriptor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

#[derive(Parser)]
#[command(about = "Send mock counting data to Nominal streaming API")]
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

    /// Interval between sends in milliseconds
    #[arg(short, long, default_value = "1000")]
    interval: u64,

    /// Suppress all console output
    #[arg(long)]
    no_console: bool,
}

fn build_stream(args: &Args) -> NominalDatasetStream {
    let bearer = BearerToken::new(&args.nominal_token).expect("invalid token");
    let rid = ResourceIdentifier::new(&args.nominal_dataset).expect("invalid dataset RID");
    let base_url = args
        .nominal_url
        .as_deref()
        .unwrap_or("https://api.gov.nominal.io/api");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("nominal-rt")
        .build()
        .expect("failed to build tokio runtime");
    let handle = runtime.handle().clone();
    Box::leak(Box::new(runtime));

    let mut builder = NominalDatasetStreamBuilder::new().stream_to_core(bearer, rid, handle);

    if args.nominal_url.is_some() {
        builder = builder.with_options(NominalStreamOpts {
            base_api_url: base_url.to_string(),
            ..NominalStreamOpts::default()
        });
    }

    builder.build()
}

fn now_timestamp() -> Timestamp {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

/// Channels:
///   toggle        – alternates 0, 1, 0, 1, ...
///   count4        – cycles 1, 2, 3, 4, 1, 2, ...
///   count30       – cycles 1, 2, ..., 30, 1, 2, ...
///   flip-1        – alternates -1, 1, -1, 1, ...
///   flip-2        – alternates -2, 2, -2, 2, ...
///   flip-1-4      – alternates 1, 4, 1, 4, ...
///   flip-2-3      – alternates 2, 3, 2, 3, ...
///   flip-around-zero – tagged channel with tags "flip-1" and "flip-2"
///   flip-positive    – tagged channel with tags "flip-1-4" and "flip-2-3"
///   toggle-tagged – value = tag repeated 4× per tag (tags: 1,2,3,4)
///   count4-tagged – same
///   count30-tagged – same
fn main() {
    let args = Args::parse();
    let interval = Duration::from_millis(args.interval);

    let quiet = args.no_console;

    if !quiet { eprintln!("connecting to Nominal..."); }
    let stream = build_stream(&args);
    if !quiet { eprintln!("connected — sending every {}ms, Ctrl-C to stop", args.interval); }

    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        ctrlc::set_handler(move || r.store(false, Ordering::SeqCst))
            .expect("failed to set Ctrl-C handler");
    }

    // Plain channels
    let ch_toggle = ChannelDescriptor::new("toggle");
    let ch_count4 = ChannelDescriptor::new("count4");
    let ch_count30 = ChannelDescriptor::new("count30");
    let ch_flip1 = ChannelDescriptor::new("flip-1");
    let ch_flip2 = ChannelDescriptor::new("flip-2");
    let ch_flip14 = ChannelDescriptor::new("flip-1-4");
    let ch_flip23 = ChannelDescriptor::new("flip-2-3");

    let mut cycle: u64 = 0;

    while running.load(Ordering::SeqCst) {
        let ts = now_timestamp();

        // --- plain channels ---
        let toggle_val = (cycle % 2) as i64; // 0, 1, 0, 1 ...
        let count4_val = (cycle % 4) as i64 + 1; // 1, 2, 3, 4 ...
        let count30_val = (cycle % 30) as i64 + 1; // 1..30

        let flip1_val = if cycle % 2 == 0 { -1i64 } else { 1 };
        let flip2_val = if cycle % 2 == 0 { -2i64 } else { 2 };
        let flip14_val = if cycle % 2 == 0 { 1i64 } else { 4 };
        let flip23_val = if cycle % 2 == 0 { 2i64 } else { 3 };

        stream.enqueue(&ch_toggle, vec![IntegerPoint { timestamp: Some(ts), value: toggle_val }]);
        stream.enqueue(&ch_count4, vec![IntegerPoint { timestamp: Some(ts), value: count4_val }]);
        stream.enqueue(&ch_count30, vec![IntegerPoint { timestamp: Some(ts), value: count30_val }]);
        stream.enqueue(&ch_flip1, vec![IntegerPoint { timestamp: Some(ts), value: flip1_val }]);
        stream.enqueue(&ch_flip2, vec![IntegerPoint { timestamp: Some(ts), value: flip2_val }]);
        stream.enqueue(&ch_flip14, vec![IntegerPoint { timestamp: Some(ts), value: flip14_val }]);
        stream.enqueue(&ch_flip23, vec![IntegerPoint { timestamp: Some(ts), value: flip23_val }]);

        // --- flip-around-zero tagged: flip-1 and flip-2 as tags ---
        let cd_faz_1 = ChannelDescriptor::with_tags("flip-around-zero", [("tag", "flip-1")]);
        let cd_faz_2 = ChannelDescriptor::with_tags("flip-around-zero", [("tag", "flip-2")]);
        stream.enqueue(&cd_faz_1, vec![IntegerPoint { timestamp: Some(ts), value: flip1_val }]);
        stream.enqueue(&cd_faz_2, vec![IntegerPoint { timestamp: Some(ts), value: flip2_val }]);

        // --- flip-positive tagged: flip-1-4 and flip-2-3 as tags ---
        let cd_fp_14 = ChannelDescriptor::with_tags("flip-positive", [("tag", "flip-1-4")]);
        let cd_fp_23 = ChannelDescriptor::with_tags("flip-positive", [("tag", "flip-2-3")]);
        stream.enqueue(&cd_fp_14, vec![IntegerPoint { timestamp: Some(ts), value: flip14_val }]);
        stream.enqueue(&cd_fp_23, vec![IntegerPoint { timestamp: Some(ts), value: flip23_val }]);

        // --- flip-positive-ab tagged: same data, tags ordered so alpha sort is reversed ---
        let cd_fpab_23 = ChannelDescriptor::with_tags("flip-positive-ab", [("tag", "a-flip-2-3")]);
        let cd_fpab_14 = ChannelDescriptor::with_tags("flip-positive-ab", [("tag", "b-flip-1-4")]);
        stream.enqueue(&cd_fpab_23, vec![IntegerPoint { timestamp: Some(ts), value: flip23_val }]);
        stream.enqueue(&cd_fpab_14, vec![IntegerPoint { timestamp: Some(ts), value: flip14_val }]);

        // --- tagged channels ---
        // Each tagged channel repeats its value 4 times with tags "1","2","3","4".
        // The value matches the plain channel equivalent.
        for tag in 1..=4i64 {
            let tag_str = tag.to_string();

            let cd_toggle = ChannelDescriptor::with_tags("toggle-tagged", [("tag", tag_str.as_str())]);
            let cd_count4 = ChannelDescriptor::with_tags("count4-tagged", [("tag", tag_str.as_str())]);
            let cd_count30 = ChannelDescriptor::with_tags("count30-tagged", [("tag", tag_str.as_str())]);

            stream.enqueue(&cd_toggle, vec![IntegerPoint { timestamp: Some(ts), value: toggle_val }]);
            stream.enqueue(&cd_count4, vec![IntegerPoint { timestamp: Some(ts), value: count4_val }]);
            stream.enqueue(&cd_count30, vec![IntegerPoint { timestamp: Some(ts), value: count30_val }]);
        }

        if !quiet {
            eprintln!(
                "cycle {cycle}: toggle={toggle_val} count4={count4_val} count30={count30_val} flip1={flip1_val} flip2={flip2_val} flip14={flip14_val} flip23={flip23_val}"
            );
        }

        cycle += 1;
        thread::sleep(interval);
    }

    if !quiet { eprintln!("shutting down (flushing stream)..."); }
    drop(stream);
    if !quiet { eprintln!("done."); }
}
