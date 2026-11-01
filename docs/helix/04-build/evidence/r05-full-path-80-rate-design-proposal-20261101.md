---
ddx:
  id: ashlar.evidence.r05-full-path-80-rate-design-proposal-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-003
      kind: governed_by
    - id: ashlar.evidence.r05-release-scale-review-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-parallel-range-layout-design-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-72partition-run-reconciliation-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-few-minute-target-napkin-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.r05-production-hardware-envelope-20261101
      kind: informed_by
---

# R05 full-path projection at 80% measured throughput

## Decision

Keep the local 1B-node/5B-edge attempt held. The best available local sample
does not support the five-minute working reference or the 15-minute review
trigger for the required full path. The 256-vCPU production candidate has a
conditional 2.53-minute arithmetic sensitivity, but its scale-out and storage
rate are unmeasured; it is not a credible completion estimate. No new
algorithmic change has a defensible speedup estimate from current evidence.
The only measured improvement is the already reviewed 72-partition write
geometry, and it does not close the local gap.

## Measured basis and local projection

The reconciled 45M-node/225M-edge 72-partition worker reported 28.306671 seconds
for writes and 14.565078 seconds for one combined persisted witness, with a
48.464554-second setup/write/witness interval. The attempt remains held because
the supervisor's global task-attribution check rejected unrelated untagged
metadata stages; cleanup is pending independent review. The phase receipts show
both tagged write stages had 72 tasks and semantic row/value/endpoint checks
passed. This one sample is useful for sensitivity math only.

At `1B / 45M / 0.8 = 27.7778`, the measured interval projects to 22.437 minutes:
13.105 minutes for writes, 6.743 minutes for the combined witness, and 2.589
minutes for the unseparated setup/metadata residual. This includes one combined
persisted pass. The required full path has two distinct persisted scans and one
transient GraphFrames mapping/action over the pinned Delta frames. If the second
scan and graph input each cost only as much as the measured combined scan, the
local lower-bound projection is:

`setup + writes + 3 × scan = 2.589 + 13.105 + 3 × 6.743 = 35.923 minutes`.

The three scan-equivalents are the measured combined witness, the second
distinct validation scan, and the GraphFrames input read. The latter two are
unmeasured; pricing each at the combined-witness rate is an optimistic
assumption because GraphFrames can add shuffle and working-set cost. Point
lookups, startup, final checks and cleanup also remain outside this estimate.
The 35.923-minute floor is 2.40 times the 15-minute review trigger and 7.18 times
the provisional five-minute reference. Local completion within those limits is
therefore infeasible under measured throughput; no additional local partition
run is proposed.

The candidate wrote 3,116,356,172 active bytes at sample scale. Row-linear
projection gives 69.25 GB active output at target: about 10.54 GB for nodes and
58.71 GB for edges. Writing once, reading for two persisted scans and reading
for the graph action moves at least 277.01 GB before Delta logs, shuffle, spill,
or graph working data. The 80%-discounted candidate rates project each full
combined scan at 6.743 minutes on the local shape.

## Production sensitivity

The current range-layout design's 256 object and 1,024 edge partitions imply
about 41 MB and 57 MB per output task at the projected row-linear sizes. Those
counts remove the known 18-task ceiling for a 256-vCPU candidate; they reduce
neither row work nor bytes. Ideal inverse-linear scaling of the 35.923-minute
local floor gives 3.368 minutes at 192 vCPU and 2.526 minutes at 256 vCPU.
These figures assume every measured and extrapolated phase scales inversely
with cores, including setup and scans, and omit fixed overhead. The source
sample used `local[18]`; no 192/256-vCPU runtime or task balance has been
measured.

The four-pass minimum byte movement requires 923 MB/s of useful throughput to
fit 300 seconds, or about 1.15 GB/s of admitted sustained capacity at 80%
useful utilization. To support the 2.526-minute 256-vCPU sensitivity, it would
need about 1.83 GB/s useful, or 2.28 GB/s admitted at 80%. Graph shuffle and
Delta overhead increase these requirements. The existing 1.0 GB/s hardware
napkin is a sizing candidate, not an admitted runtime or demonstrated path; at
80% useful delivery it moves 277 GB in about 5.78 minutes before other work.
Production might fit the five-minute reference with a higher admitted I/O path,
but current evidence cannot establish that outcome.

## Design disposition

Retain `.4-xxhash64`, its complete values and formulas, and the 256/1,024 range
counts only as the production geometry hypothesis. The 72-partition sample
improved one write interval from the retained 18-partition 35.227 seconds to
28.307 seconds (1.24× measured write throughput) and reduced the setup/write/
witness interval from 66.278 to 48.465 seconds (1.37×). It increased active
bytes by 2.47%, and its supervisor result is held. This evidence does not
support another claimed speedup or justify removing either required scan.

The identity-range proposal changes the carrier and was explicitly rejected
for this release; its optimistic estimate still exceeds 15 minutes before
omitted phases. No codec, column, or algorithm change is selected. Any future
production screen needs one admitted runtime/resource/cost envelope and must
measure both scans, both point lookups, the pinned transient GraphFrames action,
startup, final checks and cleanup with full Delta semantics intact. It must stop
for review if the complete estimate exceeds 15 minutes. No compute was
provisioned and no native or large local write was run for this proposal.
