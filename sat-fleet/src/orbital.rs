use crate::config::GroundStationConfig;
use crate::satgen::SatelliteConfig;

const MU_EARTH: f64 = 398600.4418; // km^3/s^2
const R_EARTH: f64 = 6371.0; // km mean radius (eclipse shadow model only)

// WGS84 ellipsoid: lat/lon/alt outputs are geodetic coordinates on this
// datum so they can back WGS84 position inputs directly.
const WGS84_A: f64 = 6378.137; // km semi-major axis
const WGS84_F: f64 = 1.0 / 298.257223563; // flattening
const WGS84_B: f64 = WGS84_A * (1.0 - WGS84_F); // km semi-minor axis
const WGS84_E2: f64 = WGS84_F * (2.0 - WGS84_F); // first eccentricity squared
const EARTH_ROTATION_RATE: f64 = 7.2921159e-5; // rad/s
#[allow(dead_code)]
const J2000_UNIX: f64 = 946728000.0; // Unix timestamp of J2000.0 epoch
const AU_KM: f64 = 149_597_870.7;

// Kept identical to sat-streams' orbital model; sat-fleet only emits a
// subset of the state, hence the unused fields.
#[allow(dead_code)]
pub struct OrbitalState {
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    pub altitude_km: f64,
    pub x_ecef_km: f64,
    pub y_ecef_km: f64,
    pub z_ecef_km: f64,
    pub velocity_eci: [f64; 3],
    pub is_sunlit: bool,
    pub elevation_deg: f64,
    pub range_km: f64,
    pub range_rate_km_s: f64,
    pub heading_deg: f64,
}

fn solve_kepler(mean_anomaly: f64, eccentricity: f64) -> f64 {
    let mut e_anomaly = mean_anomaly;
    for _ in 0..50 {
        let delta = (e_anomaly - eccentricity * e_anomaly.sin() - mean_anomaly)
            / (1.0 - eccentricity * e_anomaly.cos());
        e_anomaly -= delta;
        if delta.abs() < 1e-12 {
            break;
        }
    }
    e_anomaly
}

fn gmst_rad(unix_secs: f64) -> f64 {
    let jd = unix_secs / 86400.0 + 2440587.5;
    let d = jd - 2451545.0;
    let gmst_deg = 280.46061837 + 360.98564736629 * d;
    (gmst_deg % 360.0).to_radians()
}

fn sun_position_eci(unix_secs: f64) -> [f64; 3] {
    let jd = unix_secs / 86400.0 + 2440587.5;
    let d = jd - 2451545.0;
    let mean_longitude = (280.460 + 0.9856474 * d) % 360.0;
    let mean_anomaly = ((357.528 + 0.9856003 * d) % 360.0).to_radians();
    let ecliptic_lon =
        (mean_longitude + 1.915 * mean_anomaly.sin() + 0.020 * (2.0 * mean_anomaly).sin())
            .to_radians();
    let obliquity = 23.439_f64.to_radians();
    let r = AU_KM;
    [
        r * ecliptic_lon.cos(),
        r * ecliptic_lon.sin() * obliquity.cos(),
        r * ecliptic_lon.sin() * obliquity.sin(),
    ]
}

fn geodetic_to_ecef(lat_rad: f64, lon_rad: f64, alt_km: f64) -> [f64; 3] {
    let n = WGS84_A / (1.0 - WGS84_E2 * lat_rad.sin() * lat_rad.sin()).sqrt();
    [
        (n + alt_km) * lat_rad.cos() * lon_rad.cos(),
        (n + alt_km) * lat_rad.cos() * lon_rad.sin(),
        (n * (1.0 - WGS84_E2) + alt_km) * lat_rad.sin(),
    ]
}

/// ECEF -> WGS84 geodetic (Bowring's method, single iteration: sub-meter
/// accuracy at satellite altitudes).
fn ecef_to_geodetic(x: f64, y: f64, z: f64) -> (f64, f64, f64) {
    let p = (x * x + y * y).sqrt();
    let lon_rad = y.atan2(x);

    let e2_prime = (WGS84_A * WGS84_A - WGS84_B * WGS84_B) / (WGS84_B * WGS84_B);
    let theta = (z * WGS84_A).atan2(p * WGS84_B);
    let (sin_t, cos_t) = theta.sin_cos();
    let lat_rad = (z + e2_prime * WGS84_B * sin_t * sin_t * sin_t)
        .atan2(p - WGS84_E2 * WGS84_A * cos_t * cos_t * cos_t);

    let n = WGS84_A / (1.0 - WGS84_E2 * lat_rad.sin() * lat_rad.sin()).sqrt();
    // Stable for all latitudes (avoids p/cos(lat) blowup near the poles).
    let alt_km = if lat_rad.cos().abs() > 1e-6 {
        p / lat_rad.cos() - n
    } else {
        z.abs() / lat_rad.sin().abs() - n * (1.0 - WGS84_E2)
    };

    (lat_rad, lon_rad, alt_km)
}

pub fn compute_state(
    config: &SatelliteConfig,
    ground_station: Option<&GroundStationConfig>,
    timestamp_secs: f64,
) -> OrbitalState {
    let a = config.semi_major_axis_km;
    let e = config.eccentricity;
    let i = config.inclination_deg.to_radians();
    let raan = config.raan_deg.to_radians();
    let omega = config.arg_perigee_deg.to_radians();
    let m0 = config.mean_anomaly_epoch_deg.to_radians();

    // Mean motion (rad/s)
    let n = (MU_EARTH / (a * a * a)).sqrt();

    // Mean anomaly at current time
    let dt = timestamp_secs - config.epoch_unix;
    let mean_anomaly = (m0 + n * dt) % (2.0 * std::f64::consts::PI);

    // Solve Kepler's equation
    let ea = solve_kepler(mean_anomaly, e);

    // True anomaly
    let nu = 2.0
        * ((1.0 + e).sqrt() * (ea / 2.0).sin())
            .atan2((1.0 - e).sqrt() * (ea / 2.0).cos());

    // Orbital radius
    let r = a * (1.0 - e * ea.cos());

    // Position in perifocal frame
    let x_pf = r * nu.cos();
    let y_pf = r * nu.sin();

    // Velocity in perifocal frame
    let p = a * (1.0 - e * e);
    let h = (MU_EARTH * p).sqrt();
    let vx_pf = -(MU_EARTH / h) * nu.sin();
    let vy_pf = (MU_EARTH / h) * (e + nu.cos());

    // Rotation matrix perifocal -> ECI (3-1-3: RAAN, inclination, arg_perigee)
    let cos_raan = raan.cos();
    let sin_raan = raan.sin();
    let cos_i = i.cos();
    let sin_i = i.sin();
    let cos_w = omega.cos();
    let sin_w = omega.sin();

    let r11 = cos_raan * cos_w - sin_raan * sin_w * cos_i;
    let r12 = -cos_raan * sin_w - sin_raan * cos_w * cos_i;
    let r21 = sin_raan * cos_w + cos_raan * sin_w * cos_i;
    let r22 = -sin_raan * sin_w + cos_raan * cos_w * cos_i;
    let r31 = sin_w * sin_i;
    let r32 = cos_w * sin_i;

    let x_eci = r11 * x_pf + r12 * y_pf;
    let y_eci = r21 * x_pf + r22 * y_pf;
    let z_eci = r31 * x_pf + r32 * y_pf;

    let vx_eci = r11 * vx_pf + r12 * vy_pf;
    let vy_eci = r21 * vx_pf + r22 * vy_pf;
    let vz_eci = r31 * vx_pf + r32 * vy_pf;

    // ECI -> ECEF (rotate by GMST)
    let theta = gmst_rad(timestamp_secs);
    let cos_t = theta.cos();
    let sin_t = theta.sin();

    let x_ecef = x_eci * cos_t + y_eci * sin_t;
    let y_ecef = -x_eci * sin_t + y_eci * cos_t;
    let z_ecef = z_eci;

    // ECEF -> WGS84 geodetic
    let (lat_rad, lon_rad, alt_km) = ecef_to_geodetic(x_ecef, y_ecef, z_ecef);

    let latitude_deg = lat_rad.to_degrees();
    let mut longitude_deg = lon_rad.to_degrees();
    if longitude_deg > 180.0 {
        longitude_deg -= 360.0;
    }
    if longitude_deg < -180.0 {
        longitude_deg += 360.0;
    }

    // Eclipse detection (cylindrical shadow model)
    let sun_eci = sun_position_eci(timestamp_secs);
    let sat_eci = [x_eci, y_eci, z_eci];
    let is_sunlit = !is_eclipsed(&sat_eci, &sun_eci);

    // Velocity in ECEF: V_ecef = R_z(theta) * V_eci - omega x r_ecef
    let vx_ecef = vx_eci * cos_t + vy_eci * sin_t + EARTH_ROTATION_RATE * y_ecef;
    let vy_ecef = -vx_eci * sin_t + vy_eci * cos_t - EARTH_ROTATION_RATE * x_ecef;
    let vz_ecef = vz_eci;

    // Heading: azimuth of ground-track velocity at the sub-satellite point.
    // Project ECEF velocity onto local ENU basis at (lat, lon), then atan2(E, N).
    let sin_lat = lat_rad.sin();
    let cos_lat = lat_rad.cos();
    let sin_lon = lon_rad.sin();
    let cos_lon = lon_rad.cos();
    let v_east = -sin_lon * vx_ecef + cos_lon * vy_ecef;
    let v_north =
        -sin_lat * cos_lon * vx_ecef - sin_lat * sin_lon * vy_ecef + cos_lat * vz_ecef;
    let mut heading_deg = v_east.atan2(v_north).to_degrees();
    if heading_deg < 0.0 {
        heading_deg += 360.0;
    }

    // Ground station calculations
    let (elevation_deg, range_km, range_rate_km_s) = if let Some(gs) = ground_station {
        let gs_ecef = geodetic_to_ecef(
            gs.latitude_deg.to_radians(),
            gs.longitude_deg.to_radians(),
            gs.altitude_m / 1000.0,
        );

        let dx = x_ecef - gs_ecef[0];
        let dy = y_ecef - gs_ecef[1];
        let dz = z_ecef - gs_ecef[2];
        let range = (dx * dx + dy * dy + dz * dz).sqrt();

        // Up vector at ground station (normalized ECEF position)
        let gs_r = (gs_ecef[0] * gs_ecef[0] + gs_ecef[1] * gs_ecef[1] + gs_ecef[2] * gs_ecef[2])
            .sqrt();
        let up = [gs_ecef[0] / gs_r, gs_ecef[1] / gs_r, gs_ecef[2] / gs_r];

        // Elevation = asin(dot(range_unit, up))
        let range_unit = [dx / range, dy / range, dz / range];
        let sin_el = range_unit[0] * up[0] + range_unit[1] * up[1] + range_unit[2] * up[2];
        let elevation = sin_el.asin().to_degrees();

        // Range rate: project satellite velocity (ECEF) onto range direction
        let range_rate =
            vx_ecef * range_unit[0] + vy_ecef * range_unit[1] + vz_ecef * range_unit[2];

        (elevation, range, range_rate)
    } else {
        (0.0, 0.0, 0.0)
    };

    OrbitalState {
        latitude_deg,
        longitude_deg,
        altitude_km: alt_km,
        x_ecef_km: x_ecef,
        y_ecef_km: y_ecef,
        z_ecef_km: z_ecef,
        velocity_eci: [vx_eci, vy_eci, vz_eci],
        is_sunlit,
        elevation_deg,
        range_km,
        range_rate_km_s,
        heading_deg,
    }
}

fn is_eclipsed(sat_eci: &[f64; 3], sun_eci: &[f64; 3]) -> bool {
    // Cylindrical shadow model
    // Project satellite onto sun direction. If projection is negative (sat is between earth and sun side)
    // and perpendicular distance < R_earth, satellite is in shadow.
    let sun_dist = (sun_eci[0] * sun_eci[0] + sun_eci[1] * sun_eci[1] + sun_eci[2] * sun_eci[2])
        .sqrt();
    let sun_dir = [
        sun_eci[0] / sun_dist,
        sun_eci[1] / sun_dist,
        sun_eci[2] / sun_dist,
    ];

    // Projection of satellite position onto sun direction
    let proj = sat_eci[0] * sun_dir[0] + sat_eci[1] * sun_dir[1] + sat_eci[2] * sun_dir[2];

    // Satellite is on the anti-sun side of Earth
    if proj >= 0.0 {
        return false; // On the sun side, not eclipsed
    }

    // Perpendicular distance from satellite to sun-earth line
    let perp_x = sat_eci[0] - proj * sun_dir[0];
    let perp_y = sat_eci[1] - proj * sun_dir[1];
    let perp_z = sat_eci[2] - proj * sun_dir[2];
    let perp_dist = (perp_x * perp_x + perp_y * perp_y + perp_z * perp_z).sqrt();

    perp_dist < R_EARTH
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wgs84_roundtrip() {
        for (lat_deg, lon_deg, alt_km) in [
            (0.0, 0.0, 400.0),
            (38.9977, -76.8489, 0.053), // GSFC
            (51.6, 120.0, 420.0),
            (-63.4, -170.0, 39000.0),
            (89.5, 45.0, 800.0), // near-polar stability
            (-89.9, 10.0, 800.0),
        ] {
            let [x, y, z] = geodetic_to_ecef(
                (lat_deg as f64).to_radians(),
                (lon_deg as f64).to_radians(),
                alt_km,
            );
            let (lat2, lon2, alt2) = ecef_to_geodetic(x, y, z);
            assert!(
                (lat2.to_degrees() - lat_deg).abs() < 1e-6,
                "lat mismatch at ({lat_deg}, {lon_deg}, {alt_km}): {}",
                lat2.to_degrees()
            );
            assert!((lon2.to_degrees() - lon_deg).abs() < 1e-9);
            assert!(
                (alt2 - alt_km).abs() < 1e-3,
                "alt mismatch at ({lat_deg}, {lon_deg}, {alt_km}): {alt2}"
            );
        }
    }

    #[test]
    fn geodetic_latitude_differs_from_geocentric() {
        // At 45 deg the geodetic/geocentric difference is ~0.19 deg; make
        // sure we're producing geodetic (WGS84), not spherical, latitude.
        let [x, y, z] = geodetic_to_ecef(45f64.to_radians(), 0.0, 500.0);
        let geocentric = z.atan2((x * x + y * y).sqrt()).to_degrees();
        let (lat, _, _) = ecef_to_geodetic(x, y, z);
        assert!((lat.to_degrees() - 45.0).abs() < 1e-6);
        assert!((geocentric - 45.0).abs() > 0.1);
    }
}
