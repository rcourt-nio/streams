mod config;
mod engine;
mod nominal;
mod noise;
mod orbital;
mod satgen;
mod telemetry;
mod tui;

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "sat-fleet",
    about = "Large-scale satellite telemetry generator with Nominal asset/run provisioning"
)]
struct Args {
    /// Path to the fleet presets config file
    #[arg(short, long, default_value = "fleet.toml")]
    config: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Load .env from the crate directory first (so `cargo run` works from
    // anywhere), then fall back to the standard cwd search.
    let manifest_env = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".env");
    if dotenvy::from_path(&manifest_env).is_err() {
        let _ = dotenvy::dotenv();
    }
    let env = nominal::EnvConfig::from_env()?;

    let mut config_path = PathBuf::from(&args.config);
    if !config_path.exists() && args.config == "fleet.toml" {
        config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fleet.toml");
    }
    let config_str = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config file {}", config_path.display()))?;
    let fleet: config::FleetConfig =
        toml::from_str(&config_str).context("failed to parse fleet config")?;
    fleet.validate()?;

    let api = nominal::NominalApi::new(&env)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("nominal-rt")
        .build()?;

    tui::run(fleet, env, api, runtime.handle().clone())
}
