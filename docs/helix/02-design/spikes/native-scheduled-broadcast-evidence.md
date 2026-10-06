# Scheduled scattered broadcast publication

2026-10-05; SPIKE-001 bounded native experiment. No final layout approval.

## Workload

`native_scheduled_broadcast.py` reuses the private n1 10M-node canonical table at version **8**, after the preceding three disjoint scattered updates. Four disjoint sets of 300k IDs use the modular range 600k..1.8M over the same bijective selector, each spanning all 100 contiguous key-range segments. Producer payload generation is outside the clock. Batches accumulate for 30s and arrive on a fixed 30s schedule, modeling **10k changes/s** for two minutes. A single independent staging driver can overlap the serial publisher; clocks never reset for queueing. Existing dbw-aidev-cus data-gateway serverless Photon PRO 2X-Small compute/settings remain unchanged.

Every batch retains the broadcast prior/post checks, total and distinct typed identity cardinality, all prior canonical fields, exact property/retained carriers, journal origin/presence/decoded old-new values and total event count. Each publication captures actual canonical/journal versions and appends its manifest only after validation. Full carrier text is not replaced by digest comparison. Whole-entity fixture ordering remains synthetic, not a native Truss feed claim.

## Results

| Position | Changes | Queue delay s | Processing s | Oldest freshness s | Canonical/journal versions |
| --- | ---: | ---: | ---: | ---: | --- |
| 4 | 300,000 | 0.010 | 29.29 | 59.30 | 9 / 4 |
| 5 | 300,000 | 0.002 | 30.23 | 60.24 | 10 / 5 |
| 6 | 300,000 | 0.237 | 28.98 | 59.22 | 11 / 6 |
| 7 | 300,000 | 0.011 | 31.73 | 61.74 | 12 / 7 |

**Two of four batches miss 60s.** Queue remains small, but this schedule does not pass the freshness gate. Four samples do not establish population p95 or sustained capacity. Near-equal processing/interarrival periods leave little practical headroom; overlapping staging by itself does not guarantee that staging time disappears from the clock.

Atomic wall durations were **22.04, 23.47, 22.31, 24.47s**. Aggregate script reads were **17.44, 17.06, 18.86, 18.75GB** (decimal); rows scanned **44.2M, 45.1M, 46.0M, 46.9M**. Disk-cache percentage was **91%** for all four; zero spill reported. These are aggregate metrics, not isolated child-query costs. Post canonical detail: **507 files, 8,361,260,310 bytes**. Forty-two recorded statements succeeded and the harness completed. Raw evidence: `SPIKE-001-table-layout/out/native/ashlar_scheduled_broadcast_20261005_n4/`.

## Integrity scope

All four fixed-vector changed/current/journal guard readbacks passed independently after the schedule, covering 1.2M changed rows at their respective publications. The final snapshot compared **all 8.8M untouched rows across all 13 canonical fields** against before version 8, with zero mismatches; this full untouched-set check is final-state evidence, not an exhaustive untouched comparison at every intermediate vector. Final canonical identities are 10M distinct typed keys; the four batches have 1.2M distinct journal event keys. Post-run verification is excluded from freshness.

## Disposition

Next test **250k changes every 25s**, keeping 10k/s and the full 10M scattered identity domain. Reducing accumulation time may improve oldest freshness; measure queue and complete publication cost rather than assume it does. Then test singleton reads concurrently with the selected schedule. Passing a bounded schedule will still require a longer run and production payload/source evidence.

The 1B-node/5B-edge gate, cold/low-cache ingest, 100k/s burst, concurrent reader p95, actual Truss feed, replay/deletes/recovery, external graph protocols and attributable cloud cost remain open. The current broad scan volume must not be extrapolated into billion-scale admission. The overall goal stays active.
