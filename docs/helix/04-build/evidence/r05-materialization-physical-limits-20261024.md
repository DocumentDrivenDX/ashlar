---
ddx:
  id: ashlar.evidence.r05-materialization-physical-limits-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.toolkit-delivery-plan
      kind: informed_by
    - id: US-003
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: TD-007
      kind: informed_by
    - id: TD-008
      kind: informed_by
    - id: ashlar.evidence.r05-end-to-end-budget-design-20261023
      kind: informed_by
---

# R05 materialization limits and supported path

This note reconciles existing local measurements for the requested 1B-node,
assumed-5B-edge synthetic Delta materialization with its two point reads, two
persisted-data scans and one bounded graph-engine action. The full run remains
held. The “few minutes” aspiration is not a numeric SLO; five minutes below is
only a concrete comparison.

## Why writes project to 16.50 minutes

At the retained 80% planning rate, the 1B nodes take 268.877 seconds, using the
18-core sample's 45M-node write in 9.67956 seconds. The latest completed 225M
edge insert took 25.962255 seconds; at the same margin, 5B edges take 721.174
seconds. Their sum is 990.050 seconds, or 16.50 minutes. The edge write is
about 12.02 minutes and dominates. Attempt 02's task and footer reads completed,
but its supervisor rejected process-group cleanup certainty, so this is a
retained write observation used for arithmetic, not a qualified full case.

At a five-minute total, nodes alone consume 268.877 seconds and leave 31.123
seconds for 5B edge writes. That requires about 160.7M edges/second, roughly
23.2 times the current 80%-rate projection, before startup, table creation,
lookups, scans, graph action, validation or cleanup. Current evidence does not
support this improvement on the measured 18-core path. Even the earlier
18.05-minute write projection excludes all of those end-to-end phases; the
target-size scan and graph-action costs have not been measured.

## Storage and memory limits

The 270M-row calibration wrote 3.062GB active ZSTD Parquet, about 11.34 bytes
per row; row-linear arithmetic gives 68.05GB for six billion synthetic rows.
This is an active-data estimate only. It excludes Delta logs, temporary
staging, retained versions, local shuffle/spill files and graph-engine output;
the fixture also uses repetitive payloads, `source_position=id`, and
decimal-string delivery IDs rather than accepted-feed values.

Delta bytes are disk usage, not a requirement to hold all rows in RAM. The
calibration observed 4.13GB JVM heap used after its sample with a 24GB maximum,
but that checkpoint is neither a peak-RSS measure nor evidence for a 6B-row
run. No target-scale resident-memory, shuffle, spill or GraphFrames peak is
known, so staying within 96GB RAM is unproven. Keep writes partition-streamed;
pass pinned Delta DataFrames directly to GraphFrames, keep keys transient, do
not cache or collect full frames, and do not persist a second graph copy. This
is the TD-008 path that avoids duplicate materialization; it does not speed up
the measured Delta writes, and its canonical-scale cost is unknown.

## Improvements supported by evidence

- Keep the `ashlar-delta/0.4-xxhash64` carrier and its full source-qualified
  identity. `lookup_hash` is a pruning aid, never identity; it already
  replaces the much larger SHA-256 text profile and is part of the current
  rate, not a future optimization. No evidence supports dropping it.
- Keep sequential identity row order for this synthetic fixture; this is not
  a general production ordering guarantee. Hash-range sorting took 68.56
  seconds and 7.68GB at 270M rows; sorting full identity within ranges took
  58.24 seconds and 4.25GB. The baseline was 38.98 seconds and 3.06GB. Range
  sorting did produce one-file candidate scans for four probes, but did not
  improve the write/storage tradeoff. Candidate file counts do not establish
  runtime pruning.
- Do not promote the one Parquet-statistics pair: removing regular statistics
  for `source_delivery_id` saved only 238,615 bytes (0.0093%) in that pair; its
  timing difference is not repeatable evidence. The SNAPPY attempt stopped
  before a completed insert, so it supplies no faster or smaller alternative.
- `source_position` is nullable. The current synthetic fixture fills it with
  `id`; its sample column chunk accounts for about 1.02 compressed bytes per
  row. A separately named fixture may use null only where it represents an
  absent source position. Multiplying the observed column attribution by six
  billion gives 6.1GB (about 9%) as conditional upper arithmetic from this
  column's measured chunk bytes only, not an expected or measured net saving;
  nulls may change encoding and compression, so realized savings are unknown.
  Even that illustration is far below the 3.3x write improvement needed for a
  five-minute total. Preserve actual accepted positions and opaque delivery-ID
  strings.

The only supported end-to-end improvement is avoiding a second persisted graph
copy through TD-008's direct pinned-frame path. No layout, codec or carrier
change is shown to bring this full workload to a few minutes while preserving
the selected Delta profile. A next performance change needs a specific
writer-side hypothesis and a single focused comparison before it can change
this assessment; do not start a larger run or tuning matrix. Keep the 1B/5B
trial held for explicit review of the 16.50-minute projection, complete
resource/storage bounds, source-derived payload limits, target-scale scans,
GraphFrames action and cleanup.
