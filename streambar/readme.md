# StreamBar

Native macOS menu bar app for starting and stopping the streams in this repo:
Counters (`count-stream`), SATs (`sat-streams`), MacBook Metrics
(`usage-logger`), and Flink Scenarios (`flink-streams`). Fleet streams are intentionally not included.

## Build & run

```bash
./build.sh
open StreamBar.app
```

Single Swift file compiled with `swiftc` — no Xcode project. Rebuild any time
with `build.sh` (it quits a running instance first).

Launch-at-login is enabled automatically on first run (via `SMAppService`)
and can be toggled with the "Launch at Login" menu item or in System
Settings → General → Login Items. Only the app auto-starts — streams stay
stopped until you start them.

## What it does

- Menu bar icon gets a green background whenever any stream is active. If only
  some streams are running, the active count is shown next to the icon; if a
  timed run is active, the shortest remaining countdown is shown instead.
- Each stream has a submenu with Start/Stop and "Run for 5/10/30 minutes /
  1 hour" — timed runs stop automatically at the deadline.
- Start All / Stop All / Run All For apply to all streams at once.
- Streams found running outside the app (e.g. the old tmux session) are
  flagged "(running outside StreamBar)" in the menu so you notice
  double-streaming. StreamBar only manages processes it started.
- Quitting StreamBar stops all streams it started.

## How streams are launched

Rather than the `start-stream.sh` scripts (which wrap the binary in
`cargo run`, making clean shutdown unreliable), StreamBar sources each
stream's `.env`, runs `cargo build --release` to pick up any source changes,
then `exec`s the release binary directly with the same flags the scripts use.
That way the managed process IS the stream binary and SIGTERM stops it
cleanly.

Logs go to `~/Library/Logs/StreamBar/<stream>.log`. "Open Logs in Terminal"
opens a Ghostty window cd'd there (falls back to Terminal.app if Ghostty
isn't installed).

Stream definitions (paths, display names, flags) live at the top of
`StreamBar.swift` — the repo path is hardcoded to this checkout.
