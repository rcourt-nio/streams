use crate::noise::layered_noise;
use crate::orbital::OrbitalState;
use crate::satgen::{SatelliteCategory, SatelliteConfig};

pub struct TelemetryOutput {
    pub signal_strength_dbm: f64,
    pub battery_level_percent: f64,
    pub solar_panel_power_w: f64,
    pub temperature_c: f64,
    pub data_throughput_mbps: f64,
    pub doppler_shift_khz: f64,
}

pub fn compute(
    config: &SatelliteConfig,
    state: &OrbitalState,
    timestamp_secs: f64,
) -> TelemetryOutput {
    let id = config.norad_id;

    // --- Signal Strength (dBm) ---
    // Free-space path loss model
    let carrier_ghz: f64 = match config.category {
        SatelliteCategory::Communications => 12.0,  // Ku-band
        SatelliteCategory::Navigation => 1.575,      // L1
        SatelliteCategory::Weather => 8.0,           // X-band
        SatelliteCategory::EarthObservation => 8.2,  // X-band
        SatelliteCategory::Science => 2.25,          // S-band
    };
    let signal_strength_dbm = if state.elevation_deg > 0.0 && state.range_km > 0.0 {
        let fspl = 20.0 * state.range_km.log10() + 20.0 * carrier_ghz.log10() + 92.45;
        let tx_power_dbm = 40.0; // ~10W
        let antenna_gain = 35.0 + 5.0 * (state.elevation_deg / 90.0); // Higher gain at higher elevation
        let noise = layered_noise(id, timestamp_secs, "signal", &[(30.0, 1.5), (120.0, 1.0)]);
        tx_power_dbm - fspl + antenna_gain + noise
    } else {
        -120.0 + layered_noise(id, timestamp_secs, "signal_floor", &[(60.0, 2.0)])
    };

    // --- Battery Level (%) ---
    // Periodic sawtooth based on orbital phase and eclipse fraction
    let a = config.semi_major_axis_km;
    let period_secs = 2.0 * std::f64::consts::PI * (a * a * a / 398600.4418).sqrt();
    let orbital_phase = ((timestamp_secs - config.epoch_unix) % period_secs) / period_secs;

    // Approximate eclipse fraction (longer for LEO, shorter for higher orbits)
    let eclipse_frac = if a > 42000.0 {
        0.04 // GEO: very short eclipses, only near equinox
    } else {
        (6371.0 / a).asin().sin() * 0.5 // Rough geometric estimate
    };

    let charge_rate = 0.8; // %/min charging
    let discharge_rate = 0.4; // %/min discharging
    let eclipse_minutes = eclipse_frac * period_secs / 60.0;
    let sunlit_minutes = (1.0 - eclipse_frac) * period_secs / 60.0;
    let discharge_per_orbit = discharge_rate * eclipse_minutes;
    let charge_per_orbit = charge_rate * sunlit_minutes;

    // Steady state: battery swings between [min, max] where range = discharge_per_orbit
    let swing = discharge_per_orbit.min(charge_per_orbit).min(30.0);
    let battery_base = if state.is_sunlit {
        // Charging: ramp up during sunlit portion
        let sunlit_phase = if orbital_phase > eclipse_frac {
            (orbital_phase - eclipse_frac) / (1.0 - eclipse_frac)
        } else {
            orbital_phase / (1.0 - eclipse_frac)
        };
        100.0 - swing + swing * sunlit_phase
    } else {
        // Discharging: ramp down during eclipse
        let eclipse_phase = if eclipse_frac > 0.0 {
            orbital_phase / eclipse_frac
        } else {
            0.0
        };
        100.0 - swing * eclipse_phase
    };
    let battery_noise = layered_noise(id, timestamp_secs, "battery", &[(60.0, 0.3), (300.0, 0.2)]);
    let battery_level_percent = (battery_base + battery_noise).clamp(5.0, 100.0);

    // --- Solar Panel Power (W) ---
    let max_power = match config.category {
        SatelliteCategory::Communications => 3000.0,
        SatelliteCategory::Navigation => 1500.0,
        SatelliteCategory::Weather => 1200.0,
        SatelliteCategory::EarthObservation => 800.0,
        SatelliteCategory::Science => 600.0,
    };
    let solar_panel_power_w = if state.is_sunlit {
        let sun_factor = 0.7 + 0.3 * (state.elevation_deg.to_radians().sin().abs());
        let noise = layered_noise(id, timestamp_secs, "solar", &[(15.0, 0.02), (60.0, 0.01)]);
        (max_power * sun_factor * (1.0 + noise)).max(0.0)
    } else {
        0.0
    };

    // --- Temperature (C) ---
    let t_hot = match config.category {
        SatelliteCategory::Communications => 55.0,
        SatelliteCategory::Science => 35.0,
        _ => 45.0,
    };
    let t_cold = match config.category {
        SatelliteCategory::Communications => -25.0,
        SatelliteCategory::Science => -60.0,
        _ => -40.0,
    };
    let sunlit_factor = if state.is_sunlit { 0.8 } else { 0.2 };
    let temp_noise = layered_noise(id, timestamp_secs, "temp", &[(60.0, 1.5), (300.0, 1.0)]);
    let temperature_c = t_cold + (t_hot - t_cold) * sunlit_factor + temp_noise;

    // --- Data Throughput (Mbps) ---
    let max_throughput = match config.category {
        SatelliteCategory::Communications => 150.0,
        SatelliteCategory::EarthObservation => 800.0,
        SatelliteCategory::Navigation => 2.0,
        SatelliteCategory::Weather => 20.0,
        SatelliteCategory::Science => 10.0,
    };
    let data_throughput_mbps = if state.elevation_deg > 2.0 {
        let signal_factor = sigmoid((signal_strength_dbm + 90.0) / 10.0);
        let noise = layered_noise(id, timestamp_secs, "throughput", &[(10.0, 0.05), (60.0, 0.03)]);
        (max_throughput * signal_factor * (1.0 + noise)).max(0.0)
    } else {
        0.0
    };

    // --- Doppler Shift (kHz) ---
    let carrier_freq_hz = carrier_ghz * 1e9;
    let c_km_s = 299792.458;
    let doppler_shift_khz = -(state.range_rate_km_s / c_km_s) * carrier_freq_hz / 1000.0;

    TelemetryOutput {
        signal_strength_dbm,
        battery_level_percent,
        solar_panel_power_w,
        temperature_c,
        data_throughput_mbps,
        doppler_shift_khz,
    }
}

fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}
