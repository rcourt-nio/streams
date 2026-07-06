use super::Collector;
use serde_json::{Value, json};

pub struct SmcCollector {
    smc: Option<macsmc::Smc>,
}

impl SmcCollector {
    pub fn new() -> Self {
        match macsmc::Smc::connect() {
            Ok(smc) => Self { smc: Some(smc) },
            Err(e) => {
                eprintln!("Warning: SMC access failed ({e}). Try running with sudo. Temperature, fan, and power metrics disabled.");
                Self { smc: None }
            }
        }
    }
}

impl Collector for SmcCollector {
    fn name(&self) -> &'static str {
        "smc"
    }

    fn tier(&self) -> super::Tier {
        super::Tier::Slow
    }

    fn collect(&mut self, _sys: &sysinfo::System) -> Option<Value> {
        let smc = self.smc.as_mut()?;

        let mut result = serde_json::Map::new();

        // CPU Temperatures
        if let Ok(temps) = smc.cpu_temperature() {
            result.insert("cpu_temperatures".into(), json!({
                "proximity": *temps.proximity,
                "die": *temps.die,
                "graphics": *temps.graphics,
                "system_agent": *temps.system_agent,
            }));
        }

        // GPU Temperatures
        if let Ok(temps) = smc.gpu_temperature() {
            result.insert("gpu_temperatures".into(), json!({
                "proximity": *temps.proximity,
                "die": *temps.die,
            }));
        }

        // Other Temperatures
        if let Ok(temps) = smc.other_temperatures() {
            result.insert("other_temperatures".into(), json!({
                "memory_bank_proximity": *temps.memory_bank_proximity,
                "mainboard_proximity": *temps.mainboard_proximity,
                "pch_die": *temps.platform_controller_hub_die,
                "airport": *temps.airport,
                "airflow_left": *temps.airflow_left,
                "airflow_right": *temps.airflow_right,
                "thunderbolt_left": *temps.thunderbolt_left,
                "thunderbolt_right": *temps.thunderbolt_right,
                "heatpipe_1": *temps.heatpipe_1,
                "heatpipe_2": *temps.heatpipe_2,
                "palm_rest_1": *temps.palm_rest_1,
                "palm_rest_2": *temps.palm_rest_2,
            }));
        }

        // Fans
        if let Ok(fans) = smc.fans() {
            let fan_list: Vec<Value> = fans
                .enumerate()
                .filter_map(|(i, fan)| {
                    let fan = fan.ok()?;
                    Some(json!({
                        "id": i,
                        "actual_rpm": *fan.actual,
                        "min_rpm": *fan.min,
                        "max_rpm": *fan.max,
                        "target_rpm": *fan.target,
                        "mode": format!("{:?}", fan.mode),
                    }))
                })
                .collect();
            if !fan_list.is_empty() {
                result.insert("fans".into(), Value::Array(fan_list));
            }
        }

        // Power
        let mut power = serde_json::Map::new();
        if let Ok(v) = smc.cpu_power() {
            power.insert("cpu_core_watts".into(), json!(*v.core));
            power.insert("cpu_dram_watts".into(), json!(*v.dram));
            power.insert("cpu_gfx_watts".into(), json!(*v.gfx));
            power.insert("cpu_total_watts".into(), json!(*v.total));
        }
        if let Ok(v) = smc.gpu_power() { power.insert("gpu_watts".into(), json!(*v)); }
        if let Ok(v) = smc.power_system_total() { power.insert("system_total_watts".into(), json!(*v)); }
        if let Ok(v) = smc.power_dc_in() { power.insert("dc_in_watts".into(), json!(*v)); }
        if !power.is_empty() {
            result.insert("power".into(), Value::Object(power));
        }

        // Battery info from SMC
        if let Ok(info) = smc.battery_info() {
            result.insert("battery_smc".into(), json!({
                "battery_powered": info.battery_powered,
                "charging": info.charging,
                "ac_present": info.ac_present,
                "health_ok": info.health_ok,
                "temperature_max": *info.temperature_max,
                "temperature_1": *info.temperature_1,
                "temperature_2": *info.temperature_2,
            }));
        }

        if result.is_empty() {
            None
        } else {
            Some(Value::Object(result))
        }
    }
}
