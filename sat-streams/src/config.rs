use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct ConstellationConfig {
    pub ground_station: Option<GroundStationConfig>,
    pub satellites: Vec<SatelliteConfig>,
}

#[derive(Deserialize, Clone)]
pub struct GroundStationConfig {
    pub name: String,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    pub altitude_m: f64,
}

#[derive(Deserialize, Clone)]
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

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SatelliteCategory {
    Communications,
    EarthObservation,
    Navigation,
    Weather,
    Science,
}
