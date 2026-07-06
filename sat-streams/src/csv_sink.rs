use crate::output::OutputSink;
use crate::pipeline::{MetricRecord, MetricValue};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};

pub struct CsvSink {
    writer: BufWriter<File>,
    columns: Vec<String>,
    last_values: HashMap<String, String>,
    header_written: bool,
}

impl CsvSink {
    pub fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            columns: Vec::new(),
            last_values: HashMap::new(),
            header_written: false,
        })
    }

    fn format_value(value: &MetricValue) -> String {
        match value {
            MetricValue::Double(v) => v.to_string(),
            MetricValue::Integer(v) => v.to_string(),
            MetricValue::Text(v) => escape_csv(v),
            MetricValue::Bool(v) => if *v { "true" } else { "false" }.to_string(),
            MetricValue::Null => String::new(),
        }
    }
}

impl OutputSink for CsvSink {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn consume(&mut self, record: &MetricRecord) -> Result<(), Box<dyn Error>> {
        if !self.header_written {
            self.columns = record
                .samples
                .iter()
                .filter(|s| s.tags.is_none())
                .map(|s| s.path.clone())
                .collect();

            write!(self.writer, "timestamp,interval_ms,collect_ms,")?;
            let header: Vec<&str> = self.columns.iter().map(|s| s.as_str()).collect();
            write!(self.writer, "{}", header.join(","))?;
            writeln!(self.writer, ",log")?;
            self.header_written = true;
        }

        for sample in &record.samples {
            if sample.tags.is_some() {
                continue;
            }
            self.last_values
                .insert(sample.path.clone(), Self::format_value(&sample.value));
        }

        write!(
            self.writer,
            "{},{},{},",
            escape_csv(&record.timestamp_iso),
            record.interval_ms,
            record.collect_ms,
        )?;

        let values: Vec<String> = self
            .columns
            .iter()
            .map(|col| {
                self.last_values
                    .get(col)
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        write!(self.writer, "{}", values.join(","))?;

        let log_msg: String = record
            .logs
            .iter()
            .map(|l| format!("[{}] {}", l.channel, l.message))
            .collect::<Vec<_>>()
            .join("; ");
        writeln!(self.writer, ",{}", escape_csv(&log_msg))?;

        Ok(())
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.writer.flush()?;
        Ok(())
    }
}

fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
