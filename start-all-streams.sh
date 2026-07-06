#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

printf '\033]0;%s\007' "Streaming Scripts"

STREAMS=(
  "count-stream"
  "sat-streams"
  "usage-logger"
)

pids=()

cleanup() {
  echo ""
  echo "Stopping streams..."
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for stream in "${STREAMS[@]}"; do
  echo "Starting $stream..."
  (cd "$SCRIPT_DIR/$stream" && ./start-stream.sh 2>&1 | sed "s/^/[$stream] /") &
  pids+=($!)
done

echo "All streams started (pids: ${pids[*]}). Press Ctrl-C to stop."
wait
