---
ddx:
  id: ashlar.evidence.r05-production-hardware-napkin-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: TD-008
      kind: informed_by
    - id: ashlar.evidence.r05-paired-writer-second-20261024
      kind: informed_by
    - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.r05-redesign-napkin-math-20261029
      kind: informed_by
---

# R05 production-hardware napkin — 2026-10-31

This is a capacity-sizing sensitivity from the existing 18-core local sample,
not a production benchmark or authorization to run the held 1B-node/5B-edge
workload. It keeps the selected `.4-xxhash64` profile unchanged. No scale run,
cloud pricing, or tuning matrix was used.

## Core requirement

The paired R05 sample wrote 45M nodes plus 225M edges in 30.620 seconds; its
measured mode total was 60.924 seconds including compact validation. The target
row factor is `1B/45M = 5B/225M = 22.2222`. Applying the existing 80%-throughput
planning factor gives a 28.206-minute projection for that measured bundle on
the same 18-core envelope. The projection is linear in rows and inversely
linear in cores only for this napkin; scaling beyond 18 cores is unmeasured.

| Executor cores | Projected measured bundle | Five-minute budget left for omitted work |
| ---: | ---: | ---: |
| 18 (measured local envelope) | 28.21 min | none; exceeds budget by 23.21 min |
| 102 (ideal minimum) | 5.00 min | none |
| 128 | 3.98 min | 1.02 min |
| 204 (half-budget reservation) | 2.49 min | 2.51 min |
| 256 (planning candidate) | 1.98 min | 3.02 min |

Formula: `cores = 18 × 28.2056 min / allowed measured-bundle minutes`. Thus
102 aggregate executor cores is the arithmetic minimum for the already-measured
bundle alone and leaves no time for the second scan, two point reads,
GraphFrames export/action, startup, cleanup, or contention. A more realistic
napkin candidate is 256 executor cores (driver additional): it allocates about
two minutes to the measured bundle and leaves about three minutes for those
unmeasured phases. This is a planning envelope, not a demonstrated five-minute
result. If the omitted work needs more than three minutes, or scaling efficiency
falls below the linear assumption, this candidate misses the target.

The independent edge-only sample recorded 310.684 executor CPU-seconds for
225M edges. Linear row scaling gives about 6,904 executor CPU-seconds for 5B
edges. At 80% core utilization, that edge write alone implies roughly
`6,904 / (300 × 0.8) = 28.8`, or 29 allocated cores at 80% utilization to
fit five minutes. This
CPU-only lower bound excludes node writing, scans, point reads, graph action,
and all fixed overhead; it does not replace the 102-core measured-bundle
arithmetic.

## Storage path requirement

The paired sample persisted 3,041,334,560 bytes for 270M rows. Its linear
1B/5B projection is 67,585,212,444 bytes, about 67.6 decimal GB (rounded to
68GB below). To transfer 68GB in 300 seconds requires about 227MB/s for the
write alone. If the full workload writes once and reads the full persisted
tables for two scans, the path must sustain at least `3 × 68GB / 300s`, about
680MB/s of aggregate useful data movement. If the GraphFrames action causes
another full-table read, the sensitivity becomes `4 × 68GB / 300s`, about
907MB/s. The actual graph input projection and bytes read were not measured;
the latter is only a conservative whole-table sensitivity. Point-read bytes,
Delta logs, staging, shuffle, spill, retries, and concurrent users add further
load. These are effective throughput requirements at the storage/engine
boundary, not a vendor volume's advertised peak bandwidth.

For a five-minute attempt, a plausible hardware candidate therefore starts at
256 executor cores and a storage path capable of at least 1.0GB/s sustained
aggregate read/write throughput under the intended concurrency. That 1.0GB/s
is a rounded planning target above the 907MB/s whole-table sensitivity; it is
not validated headroom. The existing 18-core local target remains useful for
correctness and mechanism checks, but its measured envelope projects to 28.21
minutes for the measured bundle and cannot substantiate a few-minute claim.

This napkin does not select a cloud SKU or estimate cost. It does not establish
executor-memory, shuffle-space, peak-RSS, storage-capacity, or cleanup needs;
the prior full-scale packet's 128GiB host-memory and 512GiB free-disk values
remain minimum admission assumptions, not evidence that the proposed
256-core/1GB/s envelope fits a specific machine. A target-scale run remains
held pending owner selection of platform/resources and a separately reviewed
end-to-end plan with measured scans, point reads, graph action, and resource
high-water evidence.
