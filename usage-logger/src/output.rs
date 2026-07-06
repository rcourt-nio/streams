use crate::pipeline::MetricRecord;
use serde_json::{Map, Value, json};
use std::error::Error;
use std::io::Write;
use std::sync::mpsc;
use std::thread;

pub trait OutputSink: Send {
    fn name(&self) -> &'static str;
    fn consume(&mut self, record: &MetricRecord) -> Result<(), Box<dyn Error>>;
    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

pub struct OutputDispatcher {
    _handle: thread::JoinHandle<()>,
}

impl OutputDispatcher {
    pub fn spawn(
        receiver: mpsc::Receiver<MetricRecord>,
        mut sinks: Vec<Box<dyn OutputSink>>,
    ) -> Self {
        let handle = thread::spawn(move || {
            while let Ok(record) = receiver.recv() {
                for sink in &mut sinks {
                    if let Err(e) = sink.consume(&record) {
                        eprintln!("[{}] error: {e}", sink.name());
                    }
                }
            }
            // Channel closed — flush all sinks
            for sink in &mut sinks {
                if let Err(e) = sink.flush() {
                    eprintln!("[{}] flush error: {e}", sink.name());
                }
            }
        });

        Self { _handle: handle }
    }

    pub fn join(self) {
        let _ = self._handle.join();
    }
}

pub struct ConsoleSink {
    pretty: bool,
}

impl ConsoleSink {
    pub fn new(pretty: bool) -> Self {
        Self { pretty }
    }

    fn build_output(&self, record: &MetricRecord) -> Value {
        if record.snapshot {
            // Full snapshot: emit the entire raw map
            let mut out = record.raw.clone();
            out.insert("_snapshot".into(), Value::Bool(true));
            return Value::Object(out);
        }

        // Delta: rebuild partial JSON from changed samples only
        // Collect the set of top-level keys that have at least one changed sample
        let mut changed_top_keys = std::collections::HashSet::new();
        for sample in &record.samples {
            if sample.tags.is_none() && sample.changed {
                // Top-level key is the first segment of the dotted path
                if let Some(top) = sample.path.split('.').next() {
                    changed_top_keys.insert(top.to_string());
                }
            }
        }

        let mut out = Map::new();
        // Always include metadata
        out.insert(
            "timestamp".into(),
            json!(record.timestamp_iso),
        );
        out.insert("interval_ms".into(), json!(record.interval_ms));
        out.insert("collect_ms".into(), json!(record.collect_ms));

        // Include only top-level keys with changes, using diff_value for object fields
        for key in &changed_top_keys {
            if let Some(value) = record.raw.get(key.as_str()) {
                // For objects, only include changed leaf fields
                let filtered = self.filter_changed(key, value, record);
                out.insert(key.clone(), filtered);
            }
        }

        Value::Object(out)
    }

    /// For objects, returns only the sub-fields that have changed samples.
    /// For primitives/arrays, returns the full value.
    fn filter_changed(&self, prefix: &str, value: &Value, record: &MetricRecord) -> Value {
        match value {
            Value::Object(map) => {
                let mut filtered = Map::new();
                for (k, v) in map {
                    let path = format!("{prefix}.{k}");
                    if self.has_changed_descendant(&path, record) {
                        filtered.insert(k.clone(), self.filter_changed(&path, v, record));
                    }
                }
                Value::Object(filtered)
            }
            Value::Array(arr) => {
                // For arrays, include elements that have changed descendants
                let mut filtered = Vec::new();
                let mut any_changed = false;
                for (i, v) in arr.iter().enumerate() {
                    let path = format!("{prefix}.{i}");
                    if self.has_changed_descendant(&path, record) {
                        any_changed = true;
                    }
                    filtered.push(v.clone());
                }
                if any_changed {
                    // Return full array if any element changed (matching old behavior)
                    Value::Array(filtered)
                } else {
                    Value::Array(vec![])
                }
            }
            _ => value.clone(),
        }
    }

    fn has_changed_descendant(&self, prefix: &str, record: &MetricRecord) -> bool {
        record.samples.iter().any(|s| {
            s.tags.is_none()
                && s.changed
                && (s.path == prefix || s.path.starts_with(&format!("{prefix}.")))
        })
    }
}

impl OutputSink for ConsoleSink {
    fn name(&self) -> &'static str {
        "console"
    }

    fn consume(&mut self, record: &MetricRecord) -> Result<(), Box<dyn Error>> {
        let output = self.build_output(record);
        let mut stdout = std::io::stdout().lock();
        if self.pretty {
            serde_json::to_writer_pretty(&mut stdout, &output)?;
        } else {
            serde_json::to_writer(&mut stdout, &output)?;
        }
        stdout.write_all(b"\n")?;

        // Print log entries as separate lines
        for log in &record.logs {
            writeln!(stdout, "[{}] {}", log.channel, log.message)?;
        }

        stdout.flush()?;
        Ok(())
    }
}
