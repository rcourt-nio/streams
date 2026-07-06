# usage-logger

macOS system metrics logger with optional streaming to [Nominal](https://nominal.io).

## Build

```bash
# Default (console output only)
cargo build --release

# With Nominal streaming support
cargo build --release --features nominal
```

## Usage

```bash
# Basic usage — JSON to stdout every second
cargo run --release

# Pretty-print at 200ms interval
cargo run --release -- --pretty --interval 200

# Disable specific collectors
cargo run --release -- --disable smc,battery

# Full snapshots every cycle (no delta encoding)
cargo run --release -- --no-delta
```

### CLI flags

| Flag | Description |
|---|---|
| `-i, --interval <ms>` | Polling interval in milliseconds (default: 1000) |
| `-p, --pretty` | Pretty-print JSON output |
| `--disable <list>` | Comma-separated collectors to disable: `cpu,memory,disk,network,smc,battery,thermal_state,system` |
| `--no-delta` | Disable delta encoding (full snapshot every cycle) |
| `--snapshot-every <N>` | Full snapshot every N cycles (default: 100) |
| `--no-console` | Suppress stdout output (for headless/production streaming) |
| `--csv <PATH>` | Write CSV output to file (complete rows every cycle) |

### CSV output

```bash
# CSV only (no console)
cargo run --release -- --csv metrics.csv --no-console --interval 200

# CSV + console together
cargo run --release -- --csv metrics.csv --pretty --interval 500
```

Each sample path becomes a column. Unchanged values carry forward so every row is complete. Log entries go in a `log` column.

### Nominal streaming (requires `--features nominal`)

Copy `.env.example` to `.env` and fill in your Nominal credentials (API key, dataset RID, API URL), then:

```bash
source .env

# Stream to Nominal (no console output)
cargo run --release --features nominal -- \
  --nominal-token "$NOMINAL_TOKEN" \
  --nominal-dataset "$NOMINAL_DATASET" \
  --nominal-url "$NOMINAL_URL" \
  --no-console --interval 10

# Stream to Nominal + CSV + console
cargo run --release --features nominal -- \
  --nominal-token "$NOMINAL_TOKEN" \
  --nominal-dataset "$NOMINAL_DATASET" \
  --nominal-url "$NOMINAL_URL" \
  --csv metrics.csv --pretty --interval 200
```

| Flag | Description |
|---|---|
| `--nominal-token <TOKEN>` | Nominal API key or JWT |
| `--nominal-dataset <RID>` | Nominal dataset RID |
| `--nominal-url <URL>` | Nominal API URL (optional, defaults to production) |
| `--nominal-fallback <PATH>` | Avro file fallback path for network failures (optional) |

## Architecture

```
Collection Loop (sync, main thread)
    |
    +-- builds serde_json::Map (raw record)
    +-- MetricPipeline: flattens to Vec<MetricSample> with changed: bool
    |   (delta detection happens once, shared by all sinks)
    |
    +-- tx.try_send(MetricRecord) -- non-blocking, drops if full
                |
        mpsc::sync_channel(256)
                |
    OutputDispatcher (dedicated thread)
        |
        +-> ConsoleSink  -- partial JSON from changed samples
        +-> CsvSink     -- complete rows, unchanged values carry forward
        +-> NominalSink  -- typed channel pushes (doubles, integers, strings)
```

- **Delta detection is upstream**: each `MetricSample` carries a `changed` flag. Both sinks skip unchanged values.
- **Log entries**: pipeline events (snapshot/delta info, drop warnings) are streamed as string channels alongside numeric metrics.
- **Bounded channel with `try_send`**: collection never blocks. Drops are counted and logged periodically.
- **Nominal behind a feature flag**: `--features nominal` pulls in tokio + nominal-streaming. Without it, the binary stays small.