use super::Collector;
use serde_json::{Value, json};
use sysinfo::System;

pub struct SystemInfoCollector;

impl SystemInfoCollector {
    pub fn new() -> Self {
        eprintln!(
            "System: {} {} ({}) on {}",
            System::name().unwrap_or_default(),
            System::os_version().unwrap_or_default(),
            System::cpu_arch(),
            System::host_name().unwrap_or_default(),
        );
        Self
    }
}

impl Collector for SystemInfoCollector {
    fn name(&self) -> &'static str {
        "system"
    }

    fn tier(&self) -> super::Tier {
        super::Tier::Slow
    }

    fn collect(&mut self, _sys: &System) -> Option<Value> {
        Some(json!({
            "uptime_secs": System::uptime(),
        }))
    }
}
