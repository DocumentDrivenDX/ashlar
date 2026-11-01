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
