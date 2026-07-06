use super::Collector;
use serde_json::{Value, json};
use sysinfo::Disks;

pub struct DiskCollector {
    disks: Disks,
}

impl DiskCollector {
    pub fn new() -> Self {
        Self {
            disks: Disks::new_with_refreshed_list(),
        }
    }
}

impl Collector for DiskCollector {
    fn name(&self) -> &'static str {
        "disk"
    }

    fn tier(&self) -> super::Tier {
        super::Tier::Slow
    }

    fn collect(&mut self, _sys: &sysinfo::System) -> Option<Value> {
        self.disks.refresh(true);
        let entries: Vec<Value> = self
            .disks
            .iter()
            .map(|d| {
                json!({
                    "total_bytes": d.total_space(),
                    "available_bytes": d.available_space(),
                })
            })
            .collect();

        Some(Value::Array(entries))
    }
}
