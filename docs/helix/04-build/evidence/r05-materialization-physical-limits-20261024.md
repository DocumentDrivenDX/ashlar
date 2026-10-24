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
held. The owner's stated target is “a few minutes or the design failed”; no
numeric pass/fail minute threshold was supplied. The target remains in force;
the projections below assess available evidence and do not waive or redefine it.

## Projections from the completed paired writer comparison

The completed 45M-node/225M-edge paired comparison is the current basis for
planning. Scaling rows linearly to 1B/5B multiplies the sample by 22.2222; the
80%-throughput planning rate then multiplies elapsed time by 1.25. On that
basis, the sequential write-only projection is 16.31 minutes and the
concurrent node/edge write-only projection is 14.18 minutes. These are
extrapolations from one same-JVM comparison, with sequential always run first
and concurrent second; they are not target-scale measurements or a qualified
end-to-end bound.

The paired receipt also reports mode totals, which include table setup, writes
and compact validation in the timed interval. Linear 80%-rate extrapolation
gives 30.68 minutes for sequential mode and 28.21 minutes for concurrent
mode. This scaled sample total is still not the requested target workload: it
does not measure two point lookups, the two target-scale persisted-data scans,
the GraphFrames export/action, or Spark startup and cleanup. Those phases and
their target-scale costs remain unknown. The concurrent write-only estimate
being below 15 minutes therefore does not establish a sub-15-minute complete
run, and current evidence does not support a few-minutes total.

For historical context, an earlier incomplete attempt's 45M-node and 225M-edge
write observations produced a 16.50-minute write-only projection at the same
80% planning rate. Its task and footer reads completed, but the supervisor
rejected process-group cleanup certainty. Retain that observation as history;
the completed paired comparison above supersedes it as the latest writer
comparison and does not qualify a full-scale run.

## Storage and memory limits

The completed paired sample reports 3,041,334,560 bytes of active Delta data
for 270M rows. Straight-line arithmetic gives 67,585,212,444 bytes (about
67.6 decimal GB) for six billion synthetic rows. An earlier 270M-row
calibration reported 3.062GB, about 11.34 bytes per row, implying 68.05GB at
six billion rows; that remains a historical estimate from a different sample.
Both are active-data estimates only. They exclude Delta logs, temporary
staging, retained versions, local shuffle/spill files and graph-engine output.
The fixtures use repetitive payloads, `source_position=id`, and
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
  This is not a measured saving or evidence for a few-minutes run. Preserve
  actual accepted positions and opaque delivery-ID strings.

The only supported end-to-end improvement is avoiding a second persisted graph
copy through TD-008's direct pinned-frame path. No layout, codec or carrier
change is shown to bring this full workload to a few minutes while preserving
the selected Delta profile. A next performance change needs a specific
writer-side hypothesis and a single focused comparison before it can change
this assessment; do not start a larger run or tuning matrix. Keep the 1B/5B
trial held for explicit review of the 28.21-minute concurrent mode-total
projection, complete resource/storage bounds, source-derived payload limits,
target-scale scans, GraphFrames action and cleanup.
