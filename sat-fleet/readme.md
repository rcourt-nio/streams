# sat-fleet

Large-scale satellite telemetry generator for Nominal. Evolves `sat-streams`
to thousands of satellites, with each satellite provisioned as its own
**asset** in Nominal and a **run** created per streaming session.

## Setup

```bash
cp .env.example .env   # then fill in the values
```

- `NOMINAL_TOKEN` — API token (same one as sat-streams works)
- `NOMINAL_DATASET` — RID of the streaming dataset all batches write into
- `NOMINAL_URL` — e.g. `https://api-staging.gov.nominal.io/api`

## Run

```bash
cargo run --release
```

The TUI flow:

1. **Select** a batch preset (`fleet.toml` defines them: 10 @ 100 Hz,
   50 @ 100 Hz, 100 @ 50 Hz, 500 @ 20 Hz, 1k @ 10 Hz, 5k @ 2 Hz,
   10k @ 1 Hz).
   - `enter` — provision assets, create a run, start streaming
   - `p` — provision (upsert) assets only, no streaming
   - `d` — toggle debug mode: the chosen preset streams at 1 Hz instead of
     its configured rate (same batch, same assets; the run is titled
     `[debug]` and gets a `debug=true` property)
2. **Provisioning** searches existing assets for the batch (by the `batch`
   property) and creates only what's missing — idempotent, re-run safe.
3. **Streaming** dashboard shows session config, points sent this session,
   and per-second / per-minute send rates.
   - `n` — cut over to a new run (ends the current run, keeps streaming)
   - `s` — stop streaming and end the run
   - `q` — stop, end the run, quit

## Throughput

Measured ceiling from a laptop to staging is roughly 150-180k pts/s
(`examples/throughput.rs` reproduces the measurement). All presets are
capped at 100k pts/s, which sustains cleanly with steady sub-second
backlog; keep new presets at or below `count x 10 channels x rate_hz =
100,000`.

## Data model

- One shared streaming dataset; every point is written to a common channel
  name tagged with `satellite=<name>`.
- Channels: `latitude_deg`, `longitude_deg`, `altitude_km`, `elevation_deg`,
  `signal_strength_dbm`, `data_throughput_mbps`, `battery_level_percent`,
  `solar_panel_power_w`, `temperature_c`, `doppler_shift_khz`, plus
  `log.system` (lifecycle events + a stats entry every 10s).
- Each satellite is an asset whose data scope filters the dataset by its
  `satellite` tag. Assets carry the common `RC sats` label, a per-batch
  label, and `batch` / `norad_id` / `category` properties.
- Batches never share assets: satellite names are prefixed per preset
  (`RC10K-00042`), and orbital parameters derive deterministically from the
  name, so re-runs always map to the same assets.
- Each batch also gets one `<PREFIX> logs` asset whose data scope is the
  dataset unfiltered: `log.system` is written without tags (the logs API has
  no tag support), so it isn't inside any satellite's tag-filtered data
  scope and would otherwise be invisible from runs.
- Each streaming session creates a run (open-ended, closed on stop/cutover)
  referencing all of the batch's assets, including the logs asset.
