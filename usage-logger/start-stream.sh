#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/.env"

exec cargo run --release --features nominal -- \
  --nominal-token "$NOMINAL_TOKEN" \
  --nominal-dataset "$NOMINAL_DATASET" \
  --nominal-url "$NOMINAL_URL" \
  --no-console --interval 500
