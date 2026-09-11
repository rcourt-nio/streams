# Flink test streams

Eight channels streamed from a laptop, listed in chart row order. Every channel carries the same signal, a sine wave with a **5 s period** and amplitude 1, sampled on exact multiples of the period so the phase is identical everywhere. A perfect render of any channel matches `sine-5hz`.

The channels differ only in *when* points arrive and *what timestamp* they carry. Values are never wrong, only absent or late. A dropped point shows as a notch or a wide step, and a late recovery fills it in.

---

**Row 1 · `sine-50hz`** · 50 Hz
Every point on time. Reference for `chaos-sine`.

**Row 2 · `sine-5hz`** · 5 Hz
Every point on time. Reference for the 5 Hz channels.

**Row 3 · `chaos-sine`** · 50 Hz
Delivery delay is random per point: 95% uniform 0 to 10 s, 5% uniform 10 to 15 s. Same seed each run, so the pattern repeats.
*Look for:* arrival order is essentially random. Low allowed lateness keeps only the occasional point that happens to be newest. 10 s keeps about 95%, 15 s keeps everything. The rendered wave settles about 15 s behind now.

**Row 4 · `constant-delay`** · 5 Hz
Every point is delivered 6 s after its timestamp. Nothing is out of order.
*Look for:* a perfect wave that ends 6 s behind now. A settled line at now − 5 s cuts through data that has not arrived yet.

**Row 5 · `skew-forward`** · 5 Hz
Every point is stamped 2 s in the future and delivered on time. Nothing is out of order.
*Look for:* a perfect wave running 2 s ahead of wall clock. Harmless with a data-driven cutoff, never late under a wall-clock one.

**Row 6 · `pair-swap`** · 5 Hz
Points come in pairs (t, t+0.2). The t+0.2 point is delivered 50 ms after the t+0.4 point, so it is 0.2 s out of order.
*Look for:* below 0.2 s allowed lateness half the points are dropped and the wave is effectively 2.5 Hz. At 0.2 s or above it is perfect.

**Row 7 · `tail-swap`** · 5 Hz
The whole 1 → 0 quarter of each wave (6 points, peak to zero crossing) is held and delivered together 50 ms after the first point past the crossing. It arrives 0.2 to 1.2 s out of order.
*Look for:* below 1.2 s allowed lateness some or all of that quarter is missing and the wave drops straight from peak to zero. Above it, the quarter fills in about 1.3 s late.

**Row 8 · `future-stamps`** · 5 Hz
Every 4th point is stamped 3 s in the future and delivered on time. Its value is correct for the timestamp it carries.
*Look for:* each future point pushes the cutoff 3 s ahead, so the next three on-time points count as late. Below 3 s allowed lateness only the future points survive and the wave renders at 1.25 Hz with 0.8 s steps. Above 3 s it is perfect.

---

## Reading the charts

- **Steps wider than 0.2 s** on a 5 Hz channel mean points were dropped between the surviving ones.
- **A segment appearing late** and filling a gap means the reorder buffer released it, either because a later point moved the cutoff past it or because the idle flush fired.
- **Compare against the reference** of the same rate to see what is missing rather than what is wrong.

Source: `_pvt/streams/flink-streams`. Run `./start-stream.sh` for all channels, or pass channel names for a subset.
