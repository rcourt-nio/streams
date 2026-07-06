pub mod satellite;

use serde_json::Value;

#[derive(Clone, Copy)]
pub enum Tier {
    Fast,
    Medium,
    Slow,
}

pub trait Collector {
    fn name(&self) -> &'static str;
    fn tier(&self) -> Tier;
    fn collect(&mut self, timestamp_secs: f64) -> Option<Value>;
}
