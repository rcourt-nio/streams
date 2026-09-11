use anyhow::{bail, Result};
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Deserialize, Clone)]
pub struct FleetConfig {
    pub common_label: String,
    pub ground_station: Option<GroundStationConfig>,
    #[serde(default)]
    pub orbits: OrbitWeights,
    pub presets: Vec<Preset>,
}

#[derive(Deserialize, Clone)]
pub struct GroundStationConfig {
    pub name: String,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    pub altitude_m: f64,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(default)]
pub struct OrbitWeights {
    pub leo: f64,
    pub meo: f64,
    pub geo: f64,
    pub heo: f64,
}

impl Default for OrbitWeights {
    fn default() -> Self {
        Self {
            leo: 0.70,
            meo: 0.15,
            geo: 0.10,
            heo: 0.05,
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct Preset {
    /// Batch identifier; stored as the `batch` property on every asset.
    pub name: String,
    /// Batch-specific label applied to the batch's assets.
    pub label: String,
    /// Prefix for generated satellite names, e.g. "RC1K" -> "RC1K-00042".
    pub sat_prefix: String,
    pub count: u32,
    pub rate_hz: f64,
}

impl Preset {
    pub fn points_per_sec(&self) -> f64 {
        self.count as f64 * crate::engine::CHANNELS.len() as f64 * self.rate_hz
    }
}

/// Sustainable ceiling measured to staging is ~150-180k pts/s
/// (examples/throughput.rs); presets are capped below it so streams never
/// build unbounded backlog.
pub const MAX_POINTS_PER_SEC: f64 = 100_000.0;

impl FleetConfig {
    pub fn validate(&self) -> Result<()> {
        if self.presets.is_empty() {
            bail!("fleet config has no presets");
        }
        let mut names = HashSet::new();
        let mut prefixes = HashSet::new();
        let mut labels = HashSet::new();
        for p in &self.presets {
            if p.count == 0 {
                bail!("preset '{}' has count 0", p.name);
            }
            if p.rate_hz <= 0.0 {
                bail!("preset '{}' has non-positive rate_hz", p.name);
            }
            if p.points_per_sec() > MAX_POINTS_PER_SEC {
                bail!(
                    "preset '{}' targets {:.0} pts/s, above the {:.0} pts/s cap ({} sats x {} channels x {} Hz)",
                    p.name,
                    p.points_per_sec(),
                    MAX_POINTS_PER_SEC,
                    p.count,
                    crate::engine::CHANNELS.len(),
                    p.rate_hz,
                );
            }
            if !names.insert(&p.name) {
                bail!("duplicate preset name '{}'", p.name);
            }
            if !prefixes.insert(&p.sat_prefix) {
                bail!("duplicate sat_prefix '{}'", p.sat_prefix);
            }
            if !labels.insert(&p.label) {
                bail!("duplicate preset label '{}'", p.label);
            }
        }
        let w = self.orbits;
        if w.leo + w.meo + w.geo + w.heo <= 0.0 {
            bail!("orbit weights must sum to a positive value");
        }
        Ok(())
    }
}
