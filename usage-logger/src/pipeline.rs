use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    Double(f64),
    Integer(i64),
    Text(String),
    Bool(bool),
    Null,
}

impl MetricValue {
    fn from_json(value: &Value) -> Self {
        match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    MetricValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    MetricValue::Double(f)
                } else {
                    MetricValue::Null
                }
            }
            Value::String(s) => MetricValue::Text(s.clone()),
            Value::Bool(b) => MetricValue::Bool(*b),
            Value::Null => MetricValue::Null,
            // Objects and arrays are not leaf values — should not reach here
            _ => MetricValue::Null,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricSample {
    pub path: String,
    pub tags: Option<BTreeMap<String, String>>,
    pub value: MetricValue,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DeltaKey {
    path: String,
    tags: Option<Vec<(String, String)>>,
}

impl DeltaKey {
    fn from_sample(sample: &MetricSample) -> Self {
        Self {
            path: sample.path.clone(),
            tags: sample
                .tags
                .as_ref()
                .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    #[allow(dead_code)] // used by NominalSink behind feature flag
    pub timestamp: Duration,
    pub channel: String,
    pub message: String,
    pub args: HashMap<String, String>,
}

pub struct MetricRecord {
    #[allow(dead_code)] // used by NominalSink behind feature flag
    pub timestamp: Duration,
    pub timestamp_iso: String,
    pub interval_ms: u64,
    pub collect_ms: f64,
    pub snapshot: bool,
    pub raw: Map<String, Value>,
    pub samples: Vec<MetricSample>,
    pub logs: Vec<LogEntry>,
}

pub struct MetricPipeline {
    previous: HashMap<DeltaKey, MetricValue>,
    cycle: u64,
    snapshot_every: u64,
}

impl MetricPipeline {
    pub fn new(snapshot_every: u64) -> Self {
        Self {
            previous: HashMap::new(),
            cycle: 0,
            snapshot_every,
        }
    }

    pub fn process(&mut self, raw: Map<String, Value>) -> MetricRecord {
        let is_snapshot = self.cycle.is_multiple_of(self.snapshot_every);
        let cycle = self.cycle;
        self.cycle += 1;

        let timestamp_iso = raw
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let interval_ms = raw
            .get("interval_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let collect_ms = raw
            .get("collect_ms")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        // Flatten the raw map into samples
        let mut samples = Vec::new();
        for (key, value) in &raw {
            // Skip metadata fields — they're stored directly on MetricRecord
            if key == "timestamp" || key == "interval_ms" || key == "collect_ms" {
                continue;
            }
            flatten(key, value, &mut samples);
        }

        // Delta detection: compare against previous values
        let mut changed_count = 0usize;
        for sample in &mut samples {
            if is_snapshot {
                sample.changed = true;
            } else {
                let key = DeltaKey::from_sample(sample);
                sample.changed = match self.previous.get(&key) {
                    Some(prev) => prev != &sample.value,
                    None => true, // new key = changed
                };
            }
            if sample.changed {
                changed_count += 1;
            }
        }

        // Update previous state
        for sample in &samples {
            self.previous
                .insert(DeltaKey::from_sample(sample), sample.value.clone());
        }

        // Generate pipeline log entries
        let mut logs = Vec::new();

        // Collect which top-level keys had fresh data
        let mut fresh_keys: Vec<&str> = Vec::new();
        for (key, _) in &raw {
            if key != "timestamp" && key != "interval_ms" && key != "collect_ms" {
                fresh_keys.push(key);
            }
        }

        let raw_json = serde_json::to_string(&raw).unwrap_or_default();

        let (kind, changed_info) = if is_snapshot {
            ("snapshot", format!("{}", samples.len()))
        } else {
            ("delta", format!("{changed_count}/{}", samples.len()))
        };

        let mut args = HashMap::new();
        args.insert("cycle".into(), cycle.to_string());
        args.insert("kind".into(), kind.into());
        args.insert("changed".into(), changed_info);
        args.insert("collectors".into(), fresh_keys.join(","));
        args.insert("collect_ms".into(), format!("{collect_ms:.1}"));
        args.insert("data".into(), raw_json.clone());

        logs.push(LogEntry {
            timestamp,
            channel: "log.system".into(),
            message: format!(
                "{kind} cycle={cycle} collectors=[{}] changed={} collect_ms={collect_ms:.1} {raw_json}",
                fresh_keys.join(","),
                args.get("changed").unwrap(),
            ),
            args,
        });

        MetricRecord {
            timestamp,
            timestamp_iso,
            interval_ms,
            collect_ms,
            snapshot: is_snapshot,
            raw,
            samples,
            logs,
        }
    }
}

fn flatten(prefix: &str, value: &Value, out: &mut Vec<MetricSample>) {
    match value {
        Value::Object(map) => {
            for (key, val) in map {
                let path = format!("{prefix}.{key}");
                flatten(&path, val, out);
            }
        }
        Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                // Existing: indexed path, no tags
                let path = format!("{prefix}.{i}");
                flatten(&path, val, out);

                // New: aggregated path with index tag (only for object elements)
                if val.is_object() {
                    let tags = BTreeMap::from([("index".to_string(), i.to_string())]);
                    flatten_tagged(prefix, val, &tags, out);
                }
            }
        }
        _ => {
            out.push(MetricSample {
                path: prefix.to_string(),
                tags: None,
                value: MetricValue::from_json(value),
                changed: true, // default; overwritten by delta detection
            });
        }
    }
}

/// Like [`flatten`], but attaches `tags` to every leaf sample produced.
/// Used to create aggregated channels where array elements share a channel name
/// and are distinguished by tags (e.g., `{index: "0"}`).
fn flatten_tagged(
    prefix: &str,
    value: &Value,
    tags: &BTreeMap<String, String>,
    out: &mut Vec<MetricSample>,
) {
    match value {
        Value::Object(map) => {
            for (key, val) in map {
                let path = format!("{prefix}.{key}");
                flatten_tagged(&path, val, tags, out);
            }
        }
        Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                let path = format!("{prefix}.{i}");
                flatten_tagged(&path, val, tags, out);
            }
        }
        _ => {
            out.push(MetricSample {
                path: prefix.to_string(),
                tags: Some(tags.clone()),
                value: MetricValue::from_json(value),
                changed: true,
            });
        }
    }
}
