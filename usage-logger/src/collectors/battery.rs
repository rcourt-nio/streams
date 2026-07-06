use super::Collector;
use serde_json::{Value, json};

pub struct BatteryCollector {
    manager: Option<battery::Manager>,
}

impl BatteryCollector {
    pub fn new() -> Self {
        match battery::Manager::new() {
            Ok(manager) => Self { manager: Some(manager) },
            Err(e) => {
                eprintln!("Warning: Battery monitoring failed ({e}). Battery metrics disabled.");
                Self { manager: None }
            }
        }
    }
}

impl Collector for BatteryCollector {
    fn name(&self) -> &'static str {
        "battery"
    }

    fn tier(&self) -> super::Tier {
        super::Tier::Slow
    }

    fn collect(&mut self, _sys: &sysinfo::System) -> Option<Value> {
        let manager = self.manager.as_mut()?;
        let batteries: Vec<Value> = manager
            .batteries()
            .ok()?
            .filter_map(|b| b.ok())
            .map(|b| {
                use battery::units::*;

                json!({
                    "state": format!("{:?}", b.state()),
                    "state_of_charge_percent": b.state_of_charge().get::<ratio::percent>(),
                    "energy_wh": b.energy().get::<energy::watt_hour>(),
                    "energy_full_wh": b.energy_full().get::<energy::watt_hour>(),
                    "energy_full_design_wh": b.energy_full_design().get::<energy::watt_hour>(),
                    "state_of_health_percent": b.state_of_health().get::<ratio::percent>(),
                    "cycle_count": b.cycle_count(),
                    "voltage_v": b.voltage().get::<electric_potential::volt>(),
                    "energy_rate_w": b.energy_rate().get::<power::watt>(),
                    "temperature_celsius": b.temperature().map(|t| t.get::<thermodynamic_temperature::degree_celsius>()),
                    "time_to_full_secs": b.time_to_full().map(|t| t.get::<time::second>()),
                    "time_to_empty_secs": b.time_to_empty().map(|t| t.get::<time::second>()),
                })
            })
            .collect();

        if batteries.is_empty() {
            None
        } else {
            Some(Value::Array(batteries))
        }
    }
}
