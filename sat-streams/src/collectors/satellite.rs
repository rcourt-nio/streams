use crate::collectors::{Collector, Tier};
use crate::config::{GroundStationConfig, SatelliteConfig};
use crate::orbital;
use crate::telemetry;
use serde_json::{Value, json};

pub struct SatelliteCollector {
    config: SatelliteConfig,
    ground_station: Option<GroundStationConfig>,
    name: &'static str,
}

impl SatelliteCollector {
    pub fn new(config: SatelliteConfig, ground_station: Option<GroundStationConfig>) -> Self {
        // Sanitize name for use as JSON key: lowercase, replace spaces/hyphens with underscores
        let sanitized: String = config
            .name
            .to_lowercase()
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect();
        let name: &'static str = Box::leak(sanitized.into_boxed_str());
        Self {
            config,
            ground_station,
            name,
        }
    }
}

impl Collector for SatelliteCollector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn tier(&self) -> Tier {
        Tier::Fast
    }

    fn collect(&mut self, timestamp_secs: f64) -> Option<Value> {
        let state = orbital::compute_state(
            &self.config,
            self.ground_station.as_ref(),
            timestamp_secs,
        );
        let telem = telemetry::compute(&self.config, &state, timestamp_secs);

        let category = match self.config.category {
            crate::config::SatelliteCategory::Communications => "communications",
            crate::config::SatelliteCategory::EarthObservation => "earth_observation",
            crate::config::SatelliteCategory::Navigation => "navigation",
            crate::config::SatelliteCategory::Weather => "weather",
            crate::config::SatelliteCategory::Science => "science",
        };

        Some(json!({
            "norad_id": self.config.norad_id,
            "name": self.config.name,
            "category": category,
            // Position
            "latitude_deg": round(state.latitude_deg, 6),
            "longitude_deg": round(state.longitude_deg, 6),
            "altitude_km": round(state.altitude_km, 3),
            "x_ecef_km": round(state.x_ecef_km, 3),
            "y_ecef_km": round(state.y_ecef_km, 3),
            "z_ecef_km": round(state.z_ecef_km, 3),
            "elevation_deg": round(state.elevation_deg, 3),
            "range_km": round(state.range_km, 3),
            "is_sunlit": state.is_sunlit,
            // Attitude (principal axes, degrees)
            "heading_deg": round(state.heading_deg, 3),
            "pitch_deg": round(telem.pitch_deg, 3),
            "roll_deg": round(telem.roll_deg, 3),
            // Numerical telemetry
            "signal_strength_dbm": round(telem.signal_strength_dbm, 2),
            "battery_level_percent": round(telem.battery_level_percent, 2),
            "solar_panel_power_w": round(telem.solar_panel_power_w, 2),
            "temperature_c": round(telem.temperature_c, 2),
            "data_throughput_mbps": round(telem.data_throughput_mbps, 3),
            "doppler_shift_khz": round(telem.doppler_shift_khz, 3),
            // Enum telemetry
            "operational_status": telem.operational_status,
            "communication_band": telem.communication_band,
            "power_mode": telem.power_mode,
        }))
    }
}

fn round(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}
