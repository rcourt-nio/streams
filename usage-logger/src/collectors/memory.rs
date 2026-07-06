use super::Collector;
use serde_json::{Value, json};
use sysinfo::System;

pub struct MemoryCollector;

impl MemoryCollector {
    pub fn new() -> Self {
        Self
    }
}

impl Collector for MemoryCollector {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn tier(&self) -> super::Tier {
        super::Tier::Fast
    }

    fn collect(&mut self, sys: &System) -> Option<Value> {
        Some(json!({
            "total_bytes": sys.total_memory(),
            "used_bytes": sys.used_memory(),
            "available_bytes": sys.available_memory(),
            "free_bytes": sys.free_memory(),
            "swap_total_bytes": sys.total_swap(),
            "swap_used_bytes": sys.used_swap(),
            "swap_free_bytes": sys.free_swap(),
        }))
    }
}
