---
ddx:
  id: ashlar.evidence.r05-parallel-range-layout-design-20261101
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
    - id: TD-007
      kind: informed_by
    - id: TD-008
      kind: informed_by
    - id: ashlar.evidence.r05-paired-writer-second-20261024
      kind: informed_by
    - id: ashlar.evidence.r05-production-hardware-envelope-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
      kind: informed_by
---

# R05 parallel range layout design — 2026-11-01

## Decision

Keep the 1B-node/assumed-5B-edge full workload held. The current full-size
packet fixes `local[18]`, 18 shuffle partitions and SQL `range(N)` sources with
no explicit partition count. The measured 45M/225M calibration emitted 18
files per table. The packet therefore hard-caps each role at 18 input tasks;
moving that same packet to a larger host would not use its extra cores. A
larger production candidate must change both its runtime parallelism and its
range partition plan. This is a concrete parallelism and file-geometry limit,
though current measurements do not prove that it is the dominant elapsed-time
bottleneck.

The production design candidate is explicit per-role `range` partitioning
sized from the admitted executor capacity and target output bytes, while
preserving the `ashlar-delta/0.4-xxhash64` carrier and its complete values,
formulas, ZSTD codec, and all identity, row, endpoint and graph witnesses.
This adds task granularity on a larger executor; it does not reduce the O(N+E)
work or bytes written. It does not speed a local[18] machine past 18 concurrent
tasks.

## Local and production targets

The existing local host has 128 GiB RAM and 18 physical cores. Its 80%-rate
estimate for setup, writes and one combined witness is already 28.206 minutes;
the full two-scan, lookup and GraphFrames workload adds unmeasured work. Extra
RAM alone does not remove the 18-core ceiling or establish that data fits in
memory. The local full-scale attempt remains held because the measured bundle
alone exceeds 15 minutes. No current evidence supports a local algorithmic
change that preserves the selected profile and reaches a few minutes. The
separate identity-range proposal removes a large fixed-width column but still
projects 18.20 minutes for its measured bundle, before the omitted workload;
it is not selected here ([identity-range proposal](r05-identity-range-layout-proposal-20261031.md)).

The 192–256-vCPU production candidate is a separate target. Explicit range
counts can remove the current packet's 18-task ceiling there, but ideal
division of the 18-core measured bundle is only a sensitivity calculation.
The new runtime, scale-out, phase costs and complete workload remain
unmeasured. Do not use production arithmetic as evidence that a local run will
finish in a few minutes.

## Evidence and arithmetic

The paired 45M/225M sample wrote 471,896,757 object bytes and 2,569,437,803
edge bytes. A separate 18-core calibration of the same 45M/225M range shape
emitted 18 ZSTD files per table. Linear row scaling of the paired sample bytes
to 1B/5B gives about 10.4866 GB for objects and 57.0986 GB for edges, 67.585
GB total. The file-count observation is recorded in the [18-core
calibration](current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-18core-calibration-20261021.json).
If the same
18-file geometry persisted at target, that is approximately 583 MB per object
file and 3.17 GB per edge file. The measured 64 MiB file-size hint is not a
promise, but these per-file estimates show that 18 tasks cannot both use a
256-core executor well and approach the intended file scale.

For an admitted 256-vCPU candidate, use 256 object range partitions and 1,024
edge range partitions as a first geometry hypothesis: four edge tasks per
core. Linear bytes imply about 41.0 MB per object partition and 55.8 MB per
edge partition. This yields about 1,280 output files before retries or any
maintenance. It is a sizing calculation only; local Delta file sizing,
production runtime behavior, partition balance, task overhead and spill are
unmeasured. A different executor shape must derive its counts from that
shape rather than reuse these constants.

The current 80%-throughput planning estimate for setup, concurrent writes and
one combined witness is 28.206 minutes on the measured 18-core envelope. Ideal
linear division of that entire bundle gives 3.97 minutes at 128 vCPU, 2.64
minutes at 192 vCPU and 1.98 minutes at 256 vCPU. The 192-vCPU case has 12.36
minutes of arithmetic room under the 15-minute review trigger; 256 vCPU has
13.02 minutes. These are not end-to-end projections: setup and witness phases
were not timed separately, scale-out is unmeasured, and the second persisted
scan, two point lookups, GraphFrames action, startup, final checks and cleanup
are omitted. Therefore they cannot admit a full run under the user's rule that
any complete projection above 15 minutes receives review. The edge-only
writer's 6,904 projected CPU-seconds also requires about 34 seconds at 256
cores under an 80%-utilization assumption, before node work or any read/action.
This CPU estimate does not establish a speedup or storage ceiling.

## Next decision

Do not run a local 1B/5B load: its current 80%-rate measured bundle already
projects above 15 minutes, before omitted phases. Do not run a 72-partition
local pair on the assumption it will improve wall time; that adds scheduling
choices without adding cores, and no current profile identifies task
granularity as the local bottleneck. An optional small local partition
discriminator under the existing owner request and recorded resource limits
could answer only whether task/file geometry changes at 18 cores, not whether
a full local run meets a few-minute goal.

For production, first admit one exact 192–256-vCPU runtime and total resource
and cost envelope. A subsequent diagnostic should measure one changed
partition plan and capture per-role time, task duration/CPU, GC, spill,
output-file count/size, active bytes and all exact witnesses. It must also
measure both scans, two point reads, the pinned GraphFrames action, startup,
checks and cleanup before computing a complete estimate. The full 1B/5B run
stays held until that conservative estimate is at most 15 minutes and its
compute/cost limits fit the owner's existing authorization. If the estimate
exceeds 15 minutes, review it with the owner before a run. A few-minute
result remains a goal, not an acceptance claim.

## Limits

The 18-task ceiling follows directly from the pinned SQL `range` source and
observed 18 output files. It establishes available parallelism, not the cause
of current under-throughput. No evidence here shows whether added task
granularity helps local elapsed time, whether 256-vCPU scale-out approaches
ideal linear division, or whether disk, shuffle, serialization or GraphFrames
limits dominate. Keep the six-role publication vector, native Delta types,
source identity and multiplicity intact. This synthetic calculation does not
qualify real source payload entropy, managed Unity Catalog, publication/ACK,
production authority or memory sufficiency.
