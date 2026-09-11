use crate::config::{OrbitWeights, Preset};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SatelliteCategory {
    Communications,
    EarthObservation,
    Navigation,
    Weather,
    Science,
}

impl SatelliteCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            SatelliteCategory::Communications => "communications",
            SatelliteCategory::EarthObservation => "earth_observation",
            SatelliteCategory::Navigation => "navigation",
            SatelliteCategory::Weather => "weather",
            SatelliteCategory::Science => "science",
        }
    }
}

#[derive(Clone)]
pub struct SatelliteConfig {
    pub name: String,
    pub norad_id: u32,
    pub category: SatelliteCategory,
    pub semi_major_axis_km: f64,
    pub eccentricity: f64,
    pub inclination_deg: f64,
    pub raan_deg: f64,
    pub arg_perigee_deg: f64,
    pub mean_anomaly_epoch_deg: f64,
    pub epoch_unix: f64,
}

const EPOCH_UNIX: f64 = 1_700_000_000.0;

/// FNV-1a: stable across builds and platforms, unlike DefaultHasher. Orbital
/// parameters must stay pinned to satellite names forever, since the assets
/// they map to persist in Nominal.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Deterministic value in [0, 1) derived from the satellite name and a key.
fn unit(name: &str, key: &str) -> f64 {
    let h = fnv1a64(format!("{name}:{key}").as_bytes());
    (h >> 11) as f64 / (1u64 << 53) as f64
}

fn in_range(name: &str, key: &str, lo: f64, hi: f64) -> f64 {
    lo + unit(name, key) * (hi - lo)
}

pub fn generate(preset: &Preset, weights: &OrbitWeights) -> Vec<SatelliteConfig> {
    (1..=preset.count)
        .map(|i| build_sat(format!("{}-{:05}", preset.sat_prefix, i), weights))
        .collect()
}

fn build_sat(name: String, w: &OrbitWeights) -> SatelliteConfig {
    let norad_id = 100_000 + (fnv1a64(name.as_bytes()) % 900_000) as u32;

    let total = w.leo + w.meo + w.geo + w.heo;
    let shell = unit(&name, "shell") * total;

    let (semi_major_axis_km, eccentricity, inclination_deg, arg_perigee_deg) = if shell < w.leo {
        // LEO: 400-1200 km altitude, common inclination families
        let inclinations = [51.6, 97.8, 45.0, 28.5, 63.4, 87.4];
        let inc = inclinations[(fnv1a64(format!("{name}:inc").as_bytes()) % 6) as usize];
        (
            in_range(&name, "sma", 6771.0, 7571.0),
            in_range(&name, "ecc", 0.0001, 0.02),
            inc,
            in_range(&name, "argp", 0.0, 360.0),
        )
    } else if shell < w.leo + w.meo {
        // MEO: GPS-like shells
        (
            in_range(&name, "sma", 24000.0, 29600.0),
            in_range(&name, "ecc", 0.0001, 0.01),
            in_range(&name, "inc", 50.0, 65.0),
            in_range(&name, "argp", 0.0, 360.0),
        )
    } else if shell < w.leo + w.meo + w.geo {
        // GEO belt: slots spread by mean anomaly
        (
            in_range(&name, "sma", 42154.0, 42174.0),
            0.0001,
            in_range(&name, "inc", 0.0, 0.1),
            0.0,
        )
    } else {
        // HEO: Molniya-type
        (
            in_range(&name, "sma", 26354.0, 26754.0),
            in_range(&name, "ecc", 0.70, 0.74),
            63.4,
            270.0,
        )
    };

    let cat = unit(&name, "cat");
    let category = if cat < 0.30 {
        SatelliteCategory::Communications
    } else if cat < 0.55 {
        SatelliteCategory::EarthObservation
    } else if cat < 0.75 {
        SatelliteCategory::Science
    } else if cat < 0.90 {
        SatelliteCategory::Weather
    } else {
        SatelliteCategory::Navigation
    };

    SatelliteConfig {
        norad_id,
        category,
        semi_major_axis_km,
        eccentricity,
        inclination_deg,
        raan_deg: in_range(&name, "raan", 0.0, 360.0),
        arg_perigee_deg,
        mean_anomaly_epoch_deg: in_range(&name, "ma", 0.0, 360.0),
        epoch_unix: EPOCH_UNIX,
        name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{OrbitWeights, Preset};

    fn preset(count: u32) -> Preset {
        Preset {
            name: "test-batch".into(),
            label: "test-label".into(),
            sat_prefix: "RCT".into(),
            count,
            rate_hz: 1.0,
        }
    }

    #[test]
    fn generation_is_deterministic_and_prefix_stable() {
        let w = OrbitWeights::default();
        let a = generate(&preset(1000), &w);
        let b = generate(&preset(1000), &w);
        assert_eq!(a.len(), 1000);
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.name, y.name);
            assert_eq!(x.norad_id, y.norad_id);
            assert_eq!(x.semi_major_axis_km, y.semi_major_axis_km);
            assert_eq!(x.inclination_deg, y.inclination_deg);
        }
        // Smaller batches are strict prefixes of larger ones.
        let small = generate(&preset(10), &w);
        for (x, y) in small.iter().zip(&a) {
            assert_eq!(x.name, y.name);
            assert_eq!(x.semi_major_axis_km, y.semi_major_axis_km);
        }
        assert_eq!(a[0].name, "RCT-00001");
        assert_eq!(a[999].name, "RCT-01000");
    }

    #[test]
    #[ignore = "timing check; run with --release -- --ignored"]
    fn tick_compute_budget_10k() {
        let w = OrbitWeights::default();
        let sats = generate(&preset(10_000), &w);
        let gs = crate::config::GroundStationConfig {
            name: "GSFC".into(),
            latitude_deg: 38.9977,
            longitude_deg: -76.8489,
            altitude_m: 53.0,
        };
        let start = std::time::Instant::now();
        let mut acc = 0.0;
        for s in &sats {
            let state = crate::orbital::compute_state(s, Some(&gs), 1_753_600_000.0);
            let telem = crate::telemetry::compute(s, &state, 1_753_600_000.0);
            acc += state.latitude_deg + telem.temperature_c;
        }
        let elapsed = start.elapsed();
        println!("10k sats full tick compute: {elapsed:?} (acc={acc:.1})");
        assert!(elapsed.as_millis() < 500, "tick compute too slow: {elapsed:?}");
    }

    #[test]
    fn orbits_are_physically_sane() {
        let w = OrbitWeights::default();
        let sats = generate(&preset(2000), &w);
        let mut leo = 0;
        for s in &sats {
            assert!(s.semi_major_axis_km > 6640.0, "{} below LEO", s.name);
            assert!(s.semi_major_axis_km < 42200.0, "{} above GEO", s.name);
            assert!(s.eccentricity >= 0.0 && s.eccentricity < 0.75);
            // Perigee stays above the atmosphere.
            let perigee = s.semi_major_axis_km * (1.0 - s.eccentricity);
            assert!(perigee > 6550.0, "{} perigee too low: {perigee}", s.name);
            if s.semi_major_axis_km < 8000.0 {
                leo += 1;
            }
        }
        // ~70% LEO by default weights; allow generous tolerance.
        assert!((0.6..0.8).contains(&(leo as f64 / sats.len() as f64)));
    }
}
