use crate::collectors::{Collector, Tier};
use serde_json::Value;
use std::time::{Duration, Instant};

pub struct CollectResult {
    pub name: &'static str,
    pub value: Option<Value>,
    pub fresh: bool,
}

pub struct ScheduledCollector {
    collector: Box<dyn Collector>,
    interval_ms: u64,
    last_run: Instant,
    cached: Option<Value>,
}

impl ScheduledCollector {
    pub fn new(collector: Box<dyn Collector>, base_interval: u64) -> Self {
        let interval_ms = match collector.tier() {
            Tier::Fast => base_interval,
            Tier::Medium => 200.max(base_interval),
            Tier::Slow => 5000.max(base_interval),
        };
        Self {
            collector,
            interval_ms,
            last_run: Instant::now() - Duration::from_secs(999),
            cached: None,
        }
    }

    pub fn maybe_collect(&mut self, sys: &sysinfo::System) -> CollectResult {
        let name = self.collector.name();
        let due = self.last_run.elapsed() >= Duration::from_millis(self.interval_ms);
        if due {
            self.cached = self.collector.collect(sys);
            self.last_run = Instant::now();
        }
        CollectResult {
            name,
            value: self.cached.clone(),
            fresh: due,
        }
    }

    pub fn name(&self) -> &'static str {
        self.collector.name()
    }

    pub fn tier(&self) -> Tier {
        self.collector.tier()
    }

    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }
}
