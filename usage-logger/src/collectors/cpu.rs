use super::Collector;
use serde_json::{Value, json};
use sysinfo::System;

pub struct CpuCollector;

impl CpuCollector {
    pub fn new() -> Self {
        Self
    }
}

impl Collector for CpuCollector {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn tier(&self) -> super::Tier {
        super::Tier::Medium
    }

    fn collect(&mut self, sys: &System) -> Option<Value> {
        let cpus = sys.cpus();
        let per_core: Vec<Value> = cpus
            .iter()
            .enumerate()
            .map(|(i, cpu)| {
                json!({
                    "core": i,
                    "usage_percent": cpu.cpu_usage(),
                })
            })
            .collect();

        let load_avg = System::load_average();

        Some(json!({
            "global_usage_percent": sys.global_cpu_usage(),
            "physical_cores": sys.physical_core_count(),
            "per_core": per_core,
            "load_average": {
                "one": load_avg.one,
                "five": load_avg.five,
                "fifteen": load_avg.fifteen,
            },
        }))
    }
}
