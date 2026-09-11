#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/.env"

# Pass scenario names as args, e.g. ./start-stream.sh constant-delay swap-4-5
# No args runs every scenario. `--list` prints them.
SCENARIOS=("$@")
if [ ${#SCENARIOS[@]} -eq 0 ]; then SCENARIOS=(all); fi

exec cargo run --release -- \
  --nominal-token "$NOMINAL_TOKEN" \
  --nominal-dataset "$NOMINAL_DATASET" \
  --nominal-url "$NOMINAL_URL" \
  --scenario "$(IFS=,; echo "${SCENARIOS[*]}")"
