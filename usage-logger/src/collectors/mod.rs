pub mod battery;
pub mod cpu;
pub mod disk;
pub mod memory;
pub mod network;
pub mod smc;
pub mod system_info;
pub mod thermal;

use serde_json::Value;

/// Tier determines how frequently a collector runs.
/// Fast = every cycle, Medium = ~200ms, Slow = ~5s
#[derive(Clone, Copy)]
pub enum Tier {
    Fast,
    Medium,
    Slow,
}

pub trait Collector {
    fn name(&self) -> &'static str;
    fn tier(&self) -> Tier;
    fn collect(&mut self, sys: &sysinfo::System) -> Option<Value>;
}
