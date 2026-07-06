use crate::output::OutputSink;
use crate::pipeline::{MetricRecord, MetricValue};
use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStream;
use nominal_streaming::types::ChannelDescriptor;
use std::error::Error;

pub struct LogWriter {
    client: reqwest::blocking::Client,
    url: String,
    token: String,
}

impl LogWriter {
    pub fn new(base_url: &str, dataset_rid: &str, token: &str) -> Self {
        let url = format!("{base_url}/storage/writer/v1/logs/{dataset_rid}");
        Self {
            client: reqwest::blocking::Client::new(),
            url,
            token: token.to_string(),
        }
    }

    fn write_logs(&self, record: &MetricRecord) -> Result<(), Box<dyn Error>> {
        if record.logs.is_empty() {
            return Ok(());
        }

        for log in &record.logs {
            let secs = log.timestamp.as_secs() as i64;
            let nanos = log.timestamp.subsec_nanos() as i64;

            let body = serde_json::json!({
                "logs": [{
                    "timestamp": { "seconds": secs, "nanos": nanos, "picos": null },
                    "value": { "message": log.message, "args": log.args },
                }],
                "channel": log.channel,
            });

            let resp = self
                .client
                .post(&self.url)
                .header("Authorization", format!("Bearer {}", self.token))
                .json(&body)
                .send()?;

            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().unwrap_or_default();
                return Err(format!("log write failed ({status}): {text}").into());
            }
        }

        Ok(())
    }
}

pub struct NominalSink {
    stream: NominalDatasetStream,
    log_writer: Option<LogWriter>,
}

impl NominalSink {
    pub fn new(stream: NominalDatasetStream, log_writer: Option<LogWriter>) -> Self {
        Self { stream, log_writer }
    }
}

impl OutputSink for NominalSink {
    fn name(&self) -> &'static str {
        "nominal"
    }

    fn consume(&mut self, record: &MetricRecord) -> Result<(), Box<dyn Error>> {
        let ts = record.timestamp;

        // Push collect_ms (excluded from samples as metadata)
        self.stream.enqueue(
            &ChannelDescriptor::new("collect_ms"),
            vec![DoublePoint {
                timestamp: Some(ts.into_timestamp()),
                value: record.collect_ms,
            }],
        );

        // Push changed metric samples via streaming library
        for sample in &record.samples {
            if !sample.changed {
                continue;
            }

            let cd = match &sample.tags {
                Some(tags) => ChannelDescriptor::with_tags(
                    &sample.path,
                    tags.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                ),
                None => ChannelDescriptor::new(&sample.path),
            };
            match &sample.value {
                MetricValue::Double(v) => {
                    self.stream.enqueue(
                        &cd,
                        vec![DoublePoint {
                            timestamp: Some(ts.into_timestamp()),
                            value: *v,
                        }],
                    );
                }
                MetricValue::Integer(v) => {
                    self.stream.enqueue(
                        &cd,
                        vec![IntegerPoint {
                            timestamp: Some(ts.into_timestamp()),
                            value: *v,
                        }],
                    );
                }
                MetricValue::Bool(v) => {
                    self.stream.enqueue(
                        &cd,
                        vec![DoublePoint {
                            timestamp: Some(ts.into_timestamp()),
                            value: if *v { 1.0 } else { 0.0 },
                        }],
                    );
                }
                MetricValue::Text(v) => {
                    self.stream.enqueue(
                        &cd,
                        vec![StringPoint {
                            timestamp: Some(ts.into_timestamp()),
                            value: v.clone(),
                        }],
                    );
                }
                MetricValue::Null => {}
            }
        }

        // Push log entries via REST API (LogPoints type for log table UI)
        if let Some(log_writer) = &self.log_writer {
            if let Err(e) = log_writer.write_logs(record) {
                eprintln!("[nominal] log write error: {e}");
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
