---
ddx:
  id: ashlar.evidence.r05-edge-writer-profile-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.r05-paired-writer-second-20261024
      kind: informed_by
    - id: ashlar.evidence.r05-materialization-physical-limits-20261024
      kind: informed_by
    - id: TD-008
      kind: informed_by
---

# R05 single edge-writer profile packet — 2026-10-24

**Status: prepared and held; no Spark execution has occurred.** This packet is
one bounded local diagnostic for the uncertainty left by the completed
45M-node/225M-edge comparison: whether the dominant edge write is limited by
expression CPU, garbage collection, Delta/Parquet serialization, or local I/O.
The prior successful attempt measured 27.114 seconds for the sequential edge
write and 30.618 seconds for the edge role during concurrent writes. It has no
retained Spark event log. The separate first paired attempt failed before a
valid comparison and is not used as a performance baseline.

## Scope and measurements

The worker builds the same digest-pinned `.4-xxhash64` generator plan and
creates one fresh `edge_current` Delta table at the existing 45M-node and
225M-edge profile. It performs exactly one sequential edge INSERT. It keeps
the same UMF-produced schema, layout, writer settings, Delta protocol, source
identity fields, endpoint formulas, `lookup_hash`, and predictive-optimization
state. It makes no node table and runs no paired schedule, throughput matrix,
GraphFrames action, source lifecycle, publication, ACK, Unity Catalog, cloud,
or 1B/5B trial.

Spark event logging is enabled for this local session. The bounded parser
recurses through Spark 4.2 event-log v2 rolling directories such as
`spark-events/eventlog_v2_local-*/events_*`, ignores companion `appstatus_*`
files, refuses symlinks, and limits total bytes, event records and line size.
It records task counts and executor
run/CPU time, JVM GC time, input/shuffle bytes,
memory/disk spill, and output bytes/records when Spark reports them, by stage,
with Spark SQL execution and job-group attribution. A profile is usable only
when at least one `edge_write` stage has tasks and nonzero executor runtime and
CPU metrics; an empty or incomplete event log blocks successful cleanup. A single compact
post-write aggregate checks 225M rows, ID range and exact decimal sum,
delivery IDs, lookup hashes, and every endpoint against the independent
formula. Expected witness values are 225,000,000 rows, IDs 0–224,999,999, sum
25,312,499,887,500,000 and zero bad rows. It does not prove exact multiset
uniqueness. `validationWallSeconds` times schema/protocol validation, opening
the pinned Delta version and the compact witness together, separately from the
INSERT wall time.

The worker attempts one JDK 21 `JFR.start` profile recording immediately
before the INSERT and `JFR.stop` immediately after it, with `settings=profile`,
the selected JDK's `jcmd`, a 64 MiB recording cap, and 15-second bounded
attach commands. The raw JFR file is retained with its size and SHA-256. If
the attach command is unavailable or denied, the run can still qualify as an
event-log profile, but the result must say JFR was unavailable; it cannot make
CPU-stack claims. JFR sampling overhead is part of the measured writer time.

## Bounds and cleanup

The exact one-attempt limits are 780 seconds for the child, 900 seconds from
supervisor admission through owned cleanup and receipts, 16 GiB active Delta
Parquet, 32 GiB for the private attempt root, and at least 64 GiB free on the
attempt-parent filesystem before launch and during the run. The event log is
capped at 512 MiB, JFR at 64 MiB, and diagnostics at 16 MiB. The runtime is
the reviewed local Spark 4.2.0 / Delta 4.4.1 / JDK 21.0.13.0.2 R14 cohort on
`local[18]`, with 24 GiB JVM max heap. No job retries are authorized. The
existing local Spark slot lock and fresh no-Spark-process check are required.

The supervisor terminates and reaps the worker process group at a limit or
surveillance failure. Cleanup is permitted only after worker-group quiescence
and exact checks of the private root identity, table path identity, Delta UUID,
and current version. It removes only the receipt-owned edge table and named
scratch directories. If checks or cleanup are uncertain, it preserves the
root and records the reason. Event logs, JFR, worker output and result receipts
remain for review. Predictive optimization is not disabled.

## Review decision

The decision-changing metric is the edge INSERT's task-level executor CPU and
GC time relative to task wall time, together with the JFR execution samples
and output bytes. High CPU samples concentrated in xxhash/projection indicate
expression cost; high GC time indicates allocation pressure; low CPU with
material output bytes and little spill is consistent with serialization or
storage limits. These are diagnostic interpretations, not predetermined
outcomes. Spark task metrics do not expose Parquet compression CPU or physical
device utilization directly, and JFR sampling is not a storage benchmark.

There is no production change in this packet. Any later optimization must
preserve the full carrier semantics and receive its own single focused review.
This diagnostic does not authorize or support an extrapolation that the
1B/5B workload will meet the owner's “few minutes” target.

The executable packet is [r05_edge_writer_profile.py](r05_edge_writer_profile.py).
The fixed limits and write semantics are pinned in
[packet-profile.json](packet-profile.json). A review-pins command checks the
source closure and exact reviewed local runtime without starting Spark:

```text
python3 r05_edge_writer_profile.py --print-review-pins
```

The prepared request records packet SHA-256
`cd813edc78e5ee14721984d86f6dd1ceb6eb6e157009dfc892bb6dba4fe5172e`, source
closure SHA-256
`e359eeb71f4864dbd572673e156df6cb6d6173740a6c370053c0077ecfcf3d4b`, and
reviewed runtime pin SHA-256
`504ad0a1958e343ae0b4b5c7787b40f1e86063519b86006aa6435ec3b4117316`. The
runtime hashes bind Spark 4.2.0, Delta 4.4.1, GraphFrames 0.12.3, JDK
21.0.13.0.2, six JARs and the selected Python package closure. The selector
rechecks those live paths and versions at admission; the checked-in runtime
receipt remains a candidate, not a substitute for that check.

The adjacent `authorization-request.json` is a request for one attempt, not an
approval. The executable accepts only a separate mode-0600 authorization with
the exact `approved` status, packet/source/runtime hashes, caps and nonempty
owner decision. An authorized invocation requires explicit absolute paths for
the authorization file, attempt parent and pre-existing native slot lock.
