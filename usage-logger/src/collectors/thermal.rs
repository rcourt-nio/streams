use super::Collector;
use serde_json::{Value, json};

pub struct ThermalCollector;

impl ThermalCollector {
    pub fn new() -> Self {
        Self
    }
}

impl Collector for ThermalCollector {
    fn name(&self) -> &'static str {
        "thermal_state"
    }

    fn tier(&self) -> super::Tier {
        super::Tier::Fast
    }

    fn collect(&mut self, _sys: &sysinfo::System) -> Option<Value> {
        use objc2_foundation::NSProcessInfo;

        let process_info = NSProcessInfo::processInfo();
        let state = process_info.thermalState();

        let label = match state.0 {
            0 => "nominal",
            1 => "fair",
            2 => "serious",
            3 => "critical",
            _ => "unknown",
        };

        Some(json!(label))
    }
}
