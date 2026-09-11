# flink-streams

Contrived lateness scenarios for testing Flink allowed-lateness behaviour end to
end (reorder buffer, idle flush, dual-stream seam UX). One binary, one channel
per scenario, all scenarios can run at once into a single dataset.

Every scenario is a periodic data source. Each data tick produces points with a
timestamp offset (for future stamps) and a delivery delay (for late arrivals).
Ticks are aligned to the wall clock so timestamps sit on clean multiples of the
period. A scheduler holds points until their delivery time and hands them to the
SDK in order. Points that must arrive out of order are enqueued in the same request or
50 ms apart with a single dispatcher task, so the SDK cannot reorder them.

Every scenario carries the same value: a sine wave with a 5 s period, so a
dropped point reads as a notch in the curve and a recovery fills it in. All
scenarios run at 5 Hz except the 50 Hz sine and chaos.

```bash
cp .env.example .env            # fill in token / dataset / url
./start-stream.sh               # all scenarios
./start-stream.sh tail-swap pair-swap
cargo run --release -- --list   # print scenarios
```

| Channel | Rate | Behaviour |
|---|---|---|
| `sine-5hz` | 5 Hz | Perfect sine, every point on time. Control. |
| `sine-50hz` | 50 Hz | Perfect sine, every point on time. Control. |
| `constant-delay` | 5 Hz | Every point delivered 6 s after its timestamp. |
| `skew-forward` | 5 Hz | Every point stamped 2 s in the future, delivered on time. |
| `tail-swap` | 5 Hz | The whole 1 → 0 quarter of each 5 s wave (6 points, peak to zero crossing) is held and delivered together 50 ms after the first point past the crossing, so the segment arrives 0.2–1.2 s out of order. The other three quarters are on time. |
| `chaos-sine` | 50 Hz | Delivery delay random: 95% uniform 0–10 s, 5% uniform 10–15 s. |
| `future-stamps` | 5 Hz | Every 4th point stamped 3 s in the future, delivered on time. |
| `pair-swap` | 5 Hz | Pairs (t, t+0.2). The t+0.2 point is delivered 50 ms after the t+0.4 point, so it is 0.2 s out of order every time. |

Flags: `--scenario a,b,c` or `all`, `--channel-prefix` to namespace channels,
`--no-console` to silence the per-delivery log. The log line shows each
delivered point as `ts-now=value`, so `-0.25s=0.38` means a point stamped 0.25 s in
the past with value 0.38.

The chaos scenario uses a fixed-seed xorshift so a run is reproducible.
