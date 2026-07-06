use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

fn hash_to_f64(norad_id: u32, bucket: i64, channel: &str) -> f64 {
    let mut hasher = DefaultHasher::new();
    norad_id.hash(&mut hasher);
    bucket.hash(&mut hasher);
    channel.hash(&mut hasher);
    let h = hasher.finish();
    // Map to [0.0, 1.0)
    (h as f64) / (u64::MAX as f64)
}

/// Returns a smooth deterministic noise value in [0.0, 1.0) for the given parameters.
/// Uses cosine interpolation between adjacent time buckets for continuity.
pub fn deterministic_noise(norad_id: u32, time_secs: f64, channel: &str, bucket_secs: f64) -> f64 {
    let t = time_secs / bucket_secs;
    let bucket = t.floor() as i64;
    let frac = t.fract();

    let n0 = hash_to_f64(norad_id, bucket, channel);
    let n1 = hash_to_f64(norad_id, bucket + 1, channel);

    // Cosine interpolation for C1 smoothness
    let blend = (1.0 - (frac * std::f64::consts::PI).cos()) / 2.0;
    n0 * (1.0 - blend) + n1 * blend
}

/// Multi-octave noise: layers multiple bucket sizes for richer variation.
/// `octaves` is a slice of (bucket_secs, amplitude) pairs.
/// Returns a value centered around 0.0 with range depending on amplitudes.
pub fn layered_noise(
    norad_id: u32,
    time_secs: f64,
    channel: &str,
    octaves: &[(f64, f64)],
) -> f64 {
    let mut total = 0.0;
    for (i, (bucket_secs, amplitude)) in octaves.iter().enumerate() {
        // Use a different channel suffix per octave to decorrelate
        let octave_channel = if i == 0 {
            channel.to_string()
        } else {
            format!("{channel}.oct{i}")
        };
        let n = deterministic_noise(norad_id, time_secs, &octave_channel, *bucket_secs);
        // Center around 0: map [0,1) to [-0.5, 0.5) then scale
        total += (n - 0.5) * 2.0 * amplitude;
    }
    total
}
