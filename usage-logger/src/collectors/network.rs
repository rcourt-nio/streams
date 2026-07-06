use super::Collector;
use serde_json::{Value, json};
use sysinfo::Networks;

pub struct NetworkCollector {
    networks: Networks,
}

impl NetworkCollector {
    pub fn new() -> Self {
        Self {
            networks: Networks::new_with_refreshed_list(),
        }
    }
}

impl Collector for NetworkCollector {
    fn name(&self) -> &'static str {
        "network"
    }

    fn tier(&self) -> super::Tier {
        super::Tier::Medium
    }

    fn collect(&mut self, _sys: &sysinfo::System) -> Option<Value> {
        self.networks.refresh(true);

        let mut received_bytes: u64 = 0;
        let mut transmitted_bytes: u64 = 0;
        let mut received_packets: u64 = 0;
        let mut transmitted_packets: u64 = 0;
        let mut errors_in: u64 = 0;
        let mut errors_out: u64 = 0;

        for data in self.networks.list().values() {
            received_bytes += data.received();
            transmitted_bytes += data.transmitted();
            received_packets += data.packets_received();
            transmitted_packets += data.packets_transmitted();
            errors_in += data.errors_on_received();
            errors_out += data.errors_on_transmitted();
        }

        Some(json!({
            "received_bytes": received_bytes,
            "transmitted_bytes": transmitted_bytes,
            "received_packets": received_packets,
            "transmitted_packets": transmitted_packets,
            "errors_in": errors_in,
            "errors_out": errors_out,
        }))
    }
}
