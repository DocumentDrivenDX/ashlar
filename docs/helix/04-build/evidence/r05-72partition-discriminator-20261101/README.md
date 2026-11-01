---
ddx:
  id: ashlar.evidence.r05-72partition-discriminator-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-003
      kind: governed_by
    - id: TD-008
      kind: informed_by
    - id: ashlar.evidence.r05-release-scale-review-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
      kind: informed_by
---

# R05 single 72-partition writer discriminator — 2026-11-01

**Status: one bounded local candidate completed; worker witnesses passed, while
the supervisor held the outcome on a global event-attribution false negative.**
See [the run reconciliation](r05-72partition-run-reconciliation-20261101.md)
for exact receipts, comparison, projection and the future validator correction.
The packet implements the 45M-node/225M-edge local candidate scoped by the
reviewed R05 release-scale design. It uses the existing `.4-xxhash64` UMF
carrier, schema, values, ZSTD settings and deterministic row formulas. The only
workload change is explicit Spark SQL `range(0, N, 1, 72)` slicing and 72 shuffle
partitions on the existing `local[18]` host. It does not rerun the retained
18-partition baseline, execute GraphFrames, change the carrier profile, or use
cloud compute.

The candidate writes each of the two current tables once, sequentially, into
fresh private Delta paths. It checks the admitted Spark/PySpark 4.2.0 and Delta
4.4.1 runtime, exact loaded Delta JAR origins, table schemas/protocol/UUIDs,
range slice count, persisted row/value summaries, and independent edge endpoint
arithmetic. Spark event logs retain write stage task count, per-stage duration,
executor CPU/GC, shuffle and spill metrics. Owned tables and temporary roots are
removed only after child process quiescence and complete validation; uncertainty
preserves the attempt root.

The supervisor bounds the one attempt to 900 seconds and 32 GiB total attempt
root, with a 780-second worker limit, 16 GiB active-Parquet cap and 64 GiB free
space floor. It makes no automatic retry and does not disable predictive
optimization. The candidate runtime receipt binds the observed R19 Python 3.12.14
installation and the exact restored Delta JARs. GraphFrames Python/JVM packages
are outside this writer-only profile; GraphFrames export remains a separate
qualification gate.

The one-run authorization capped the attempt at 900 seconds and 32 GiB. The
preserved attempt root remains for Sol review because the supervisor did not
complete cleanup after its attribution guard fired; no cleanup or repeat run
is authorized here. Results compare this writer mechanism with retained local
baseline evidence only. They cannot establish repeatability, 256-core scaling,
1B/5B capacity, Unity Catalog qualification, or production support.
