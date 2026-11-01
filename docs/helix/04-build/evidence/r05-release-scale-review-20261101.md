---
ddx:
  id: ashlar.evidence.r05-release-scale-review-20261101
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
    - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.r05-redesign-napkin-math-20261029
      kind: informed_by
    - id: ashlar.evidence.r05-production-hardware-napkin-20261031
      kind: informed_by
    - id: ashlar.evidence.r05-materialization-physical-limits-20261024
      kind: informed_by
    - id: ashlar.evidence.r05-identity-range-layout-proposal-20261031
      kind: informed_by
---

# R05 release-scale design review — 2026-11-01

## Decision

Keep the 1B-node/assumed-5B-edge run held. Existing measurements do not support
the selected full `.4-xxhash64` Delta carrier meeting the owner's provisional
five-minute (300-second) working target. This is the target used by the existing
80%-throughput sizing napkin, not a newly approved service-level objective. The
owner's “Hold and redesign first” decision remains in force; this review neither
changes that decision nor authorizes a native or cloud trial.

The detailed evidence is already recorded in the linked R05 packet, redesign
napkin, and production-hardware napkin. Their 45M/225M sample projects the
measured setup/write/combined-witness bundle to 28.206 minutes at the 80%
planning rate. The combined witness is one persisted pass; the second distinct
scan, point reads, GraphFrames action, startup, final validation, and cleanup
remain unmeasured. The separate edge attribution shows substantial executor
CPU work, but it does not establish CPU, compression, or storage as the causal
limit. The roughly 68 GB active-data estimate is a row-linear synthetic
projection, not a disk-capacity or source-entropy bound.

The underlying [storage/CPU attribution](r05-storage-throughput-reassessment-20261023/README.md)
and [paired statistics result](parquet-field-statistics-paired-20261023/README.md)
are retained as evidence, not adopted optimizations.

## Hardware and scaling limits

The existing [production-hardware napkin](r05-production-hardware-napkin-20261031.md)
uses 256 executor cores and about 1 GB/s sustained useful I/O as a conditional
sizing candidate for the five-minute working target. It allocates roughly two
minutes to the already measured bundle and three minutes to omitted work. This
arithmetic assumes linear scaling beyond the measured 18-core/18-partition run.
No evidence verifies that the selected runtime exposes enough useful parallel
tasks, that more executors avoid stage and scheduling overhead, or that writes,
two scans, and GraphFrames can share that storage rate under the required
concurrency. The sizing napkin does not require a selected compute shape or
spend envelope; those are needed before executing a compute-specific screen or
trial in the existing Databricks workspace `dbw-aidev-cus`.

The full-path risks that can break the estimate are:

- **Task parallelism and imbalance:** the retained sample used 18 partitions
  and showed limited task-duration spread. That does not establish suitable
  partition sizing, output-file geometry, or balanced work for a target-scale
  run on a many-core executor set.
- **Data movement and shuffle:** two full-table scans already consume multiple
  passes over the projected data. GraphFrames may add another input pass and
  shuffle; the current canonical action has no scale plan, scan-byte, shuffle,
  or runtime evidence. A no-copy transient view avoids a second persisted graph
  materialization, but does not remove these costs.
- **Memory and temporary space:** the 128 GiB host and 24 GiB JVM settings in
  the sample are not peak-memory evidence. Executor memory, shuffle space,
  spill, and GraphFrames working-set requirements at target scale remain
  unknown. Active Parquet bytes cannot be used as an in-memory requirement.
- **Concurrency and fixed work:** concurrent writes improved one same-JVM
  sample, with concurrent mode run second. Startup, DDL, source staging,
  validation and cleanup do not necessarily shrink with more executor cores.

These are design risks inferred from the documented workload and missing
measurements, not observed target-scale failures. The hardware napkin gives a
reasonable screening envelope; it does not admit a dedicated compute shape,
runtime, or cost envelope for the existing workspace, or make the full workload
likely to finish within five minutes.

At the existing 80%-of-measured-throughput planning rate, the paired sample's
setup/write/combined-witness bundle projects to 28.206 minutes. That is 1.88x
the 15-minute review trigger and 5.64x the provisional five-minute working
target before omitted work. The separate storage/edge-attribution packet's
18.05-minute figure is a write-only projection from its own calibration; these
different scopes are not additive. Neither projection is a target-scale result.

## Scaling diagnosis and one redesign candidate

The measurements do not identify CPU, compression, or storage as the causal
limit. They do expose a concrete parallelism ceiling in the current trial shape:
the 225M-edge attribution completed one 18-task write stage, while the held
1B/5B supervisor pins `local[18]` and 18 shuffle partitions. At the planning
target this provides no more than 18 input tasks per role, even if a future
admitted compute shape has hundreds of cores. The observed stage used
310.684 executor CPU-seconds over a 25.485-second span (about 68% of the
18-core theoretical capacity); this is consistent with several possible
limits and does not prove CPU starvation. The dominant unavoidable algorithmic
work is row-wise carrier generation/Parquet writing followed by at least two
full persisted-table scans and a GraphFrames input pass: O(N+E) work per pass
over an estimated 68.05 GB active carrier. At a five-minute target, three
whole-carrier passes imply about 680 MB/s useful aggregate I/O; a fourth
GraphFrames pass implies about 907 MB/s. These are lower-bound sensitivities,
not measured bandwidth requirements for a particular runtime.

The single redesign candidate is to make generator and writer partition counts
explicit and scale them with the admitted executor shape while leaving the
`.4-xxhash64` schema, ZSTD, values, formulas, and all witnesses unchanged. For
the 256-core sizing candidate only, start with 256 node partitions and 1,024
edge partitions (four edge tasks per core); row-linear bytes imply roughly
40 MiB per node partition and 56 MiB per edge partition. This is compatible
with the selected 64 MiB target-file-size hint as a starting point, not a
promised file size. It removes the known 18-task ceiling and gives a reviewable
file-geometry hypothesis; it does not establish faster writes, linear scaling,
or adequate memory/storage.

The bounded mechanism discriminator, if the local Spark slot is admitted, is
one 45M-node/225M-edge candidate run on the existing pinned 18-core runtime
with 72 explicit partitions (four tasks per core), changing no other setting.
Compare it to the retained 18-partition baseline; do not rerun the baseline.
Bound startup, writes, exact row/value/endpoint witnesses and owned cleanup to
900 seconds and 32 GiB retained attempt data; one candidate, no retries, no
cloud compute. Record stage task count and duration distribution, executor CPU/GC,
spill, write time, output files/bytes and exact semantic equality. Stop at
either bound or any mismatch. This checks whether extra task granularity
improves or harms the measured local mechanism; it cannot validate 256-core
scaling. Carry the partition plan to a production-runtime screen only if the
candidate preserves all witnesses and materially improves write time without
unacceptable file or task overhead. Otherwise retain the current plan and
redesign around the measured bottleneck. A later 256-core screen requires separately admitted
compute/runtime and cost bounds; the 1B/5B run remains held.

## Smallest useful next discriminator

No additional writer micro-optimization is supported by current evidence. The
per-column statistics pair showed one lower write time, but its write-only
projection remained above the 15-minute review trigger; one pair does not
establish a stable gain. The identity-range layout proposal changes the
physical profile and is separately unapproved. A smaller local repetition would
not decide either question.

After the owner admits a dedicated Databricks compute shape and runtime, with a
bounded resource and cost envelope for workspace `dbw-aidev-cus`, the lowest-cost
useful screen is one full-path workload unit on that compute: 1M nodes and 5M
edges, written once with the selected carrier, followed by both persisted
scans, the two exact lookups, and one bounded GraphFrames action. Capture
per-phase wall time, useful bytes, task parallelism, spill and peak
executor/driver memory. This is a falsification screen for the admitted compute
and operation path only; it cannot qualify linear scale-up to 1B/5B. If it does
not meet the per-unit budget implied by the five-minute working target, redesign
the hardware or operation plan before considering a larger pilot. If it passes,
a separately reviewed, resource-bounded representative pilot is still required
before any full run.

No such compute-specific screen is proposed for execution in this review: the
dedicated compute shape, runtime and cost envelope have not been admitted, and
a single-unit result would not resolve target-scale shuffle, memory, or skew
uncertainty by itself. Preserve the current hold until those inputs and a
bounded screen are reviewed; the five-minute working target remains the sizing
reference throughout.
