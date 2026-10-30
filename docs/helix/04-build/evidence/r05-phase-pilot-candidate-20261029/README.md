---
ddx:
  id: ashlar.evidence.r05-phase-pilot-candidate-20261029
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
    kind: governed_by
  - id: ashlar.evidence.r05-redesign-napkin-math-20261029
    kind: informed_by
  - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
    kind: informed_by
---

# R05 phase-resolved local pilot candidate — 2026-10-29

## Decision and scope

This is an inert candidate scope for one local 45M-node/225M-edge diagnostic
pilot. It does not authorize execution and contains no Spark invocation. Keep
the `.4-xxhash64` UMF-generated schemas, Delta protocol, ZSTD settings,
`local[18]` runtime, 18 shuffle partitions, deterministic range generator,
source/value oracle and endpoint formula from the existing paired R05 packet.
Use one pair of fresh tables and the concurrent node/edge write schedule. Do
not run the sequential comparison mode, change layout or codec, or add a
concurrency matrix.

After writes, the pilot must open the existing
`PinnedCanonicalGraphFramesActionFixture` against the frozen synthetic
generator/source/oracle descriptor and the two owned table paths. The holder
must acquire its exclusive private-root lock, observe each Delta UUID, head
version, schema and protocol twice, pin those exact versions, and prove the
active-file inventory is retained and available before returning frames. Every
scan, point read and engine frame must use those versions. Renew the same lease
and recheck both table identities, runtime/JAR identity, lock and retained-file
inventory immediately before and after the GraphFrames action, then close the
lease with its closing renewal. This pilot has no source progress, ACK,
publication manifest, managed Unity Catalog or production authority. Its
diagnostic purpose is to separate setup,
each role write, point reads, persisted scans, GraphFrames construction/action,
final reconciliation and owned cleanup. Preserve physical bytes, file
geometry, query plans, task CPU/GC, spill, scan input bytes and Spark job/stage
attribution. Nested jobs/stages are attributed to their enclosing phase and
must not be added again to elapsed wall time.

## One-shot resource envelope

The following candidate limits reuse the paired runner's host, scratch, slot,
and disk controls. They are containment bounds for a future exact authorization,
not evidence that the current host meets admission or that a full run is
approved. Predictive optimization remains enabled.

| Resource or phase | Candidate limit |
| --- | ---: |
| Attempts / worker processes | 1 / 1 isolated worker |
| Runtime | Spark 4.2.0, Delta 4.4.1, GraphFrames 0.12.3, `local[18]`, 24 GiB JVM maximum |
| Physical RAM admission / sampled worker RSS | 128 GiB minimum / 96 GiB maximum |
| Free disk and inodes at admission | 64 GiB / 1,000 |
| Private attempt root | 32 GiB maximum |
| Active Parquet across both tables | 16 GiB maximum |
| End-to-end attempt | 900 seconds from supervised child start through cleanup and receipt close |
| Worker execution | 780 seconds maximum |
| Admission, DDL and setup | 60 seconds cumulative |
| Concurrent node and edge writes | 300 seconds cumulative; diagnostic only, not a throughput pass threshold |
| Two exact point reads | 60 seconds cumulative |
| Distinct row/value scan | 120 seconds |
| Distinct endpoint-formula scan | 120 seconds |
| Pinned-frame GraphFrames construction and one-hop action | 120 seconds; at most 10,000 complete occurrences |
| Final reconciliation and owned cleanup | 120 seconds cumulative |

The phase ceilings sum to 900 seconds. A parent deadline also stops the
attempt. Any overrun, unexpected write/job/retry, semantic mismatch, graph
result overflow, changed table identity, failed renewal, unavailable retained
file, or uncertain cleanup fails closed. Stop the workload and close the holder
in `finally`; if close/renewal cannot prove unchanged custody, preserve the
tables, attempt root and partial evidence for inspection. After a successful
holder close and worker-process-group quiescence, the supervisor must recheck
the exact ownership receipt, filesystem identities and Delta UUIDs before
removing only the two owned tables and scratch paths. Preserve the receipt and
phase evidence, then verify removal. Never clean up on a changed or uncertain
identity.

## Required phase record

Record one monotonic start/end pair and the relevant Spark job-group/stage IDs
for each phase:

1. Runtime and carrier admission, UMF DDL, and table creation.
2. Concurrent `object_current` and `edge_current` writes, with independent role
   timings and one enclosing write-wall time.
3. One exact full-identity point read for node `(source_system, 1, 0)` and one
   for edge `(source_system, 7, 0)`, each compared as a complete carrier row
   with the frozen oracle and each retaining its executed plan.
4. A persisted row/value/range/sum scan over both exact pinned table versions.
5. A separate persisted edge endpoint type/range/formula scan over the exact
   pinned edge version. Do not count the earlier paired runner's combined
   witness as this second scan.
6. Direct pinned DataFrame mapping, GraphFrames construction, and one bounded
   one-hop seed action. Preserve source-qualified struct node IDs and separate
   edge identity; do not materialize a second graph copy or collect full frames.
   Open the synthetic holder before using the pinned frames; verify retained
   file inventory and renew immediately before the action, then renew and
   compare the same UUID/version/schema/protocol/runtime/lock/file inventory
   after the action. Close through the holder even on action failure.
7. Final witness and limit reconciliation, then ownership-verified table
   cleanup and receipt close. Cleanup follows successful holder close and
   process-group quiescence; custody or ownership uncertainty preserves the
   attempt for review.

Use the existing generator and frozen oracle from
[`r05-full-delta4-scale-packet-20261023`](../r05-full-delta4-scale-packet-20261023/README.md),
`_point_lookup`, `_row_value_scan`, `_endpoint_scan`, `_graph_action`, and the
UMF-generated carrier plan. The 45M/225M endpoint expectations are five edge
occurrences per node and `source_id = floor(id/5)`,
`target_id = pmod(source_id + floor(pmod(id,5)/2), 45M)`. For the selected seed
0, the independently frozen result has five distinct edge IDs, including the
parallel pair at IDs 0 and 1.

## Open implementation gaps

This is a reviewed execution scope, not an executable runner. The existing
paired worker runs sequential and concurrent output pairs and combines row,
value and endpoint witnesses into one action. The full-scale worker has fixed
1B/5B assumptions and the full-scale supervisor's packet digest binds its own
source closure. Neither can be called as-is for this pilot.

A future implementation must add a separately named one-shot worker and
supervisor packet, bind the exact source/runtime/oracle digests and these phase
limits, reuse the paired supervisor's local admission/slot/scratch/RSS/cleanup
controls, and test its gates without starting Spark. It must add explicit
parent/child phase accounting so concurrent write-role times do not inflate
wall time and nested Spark stages are not double-counted. The two existing
point-read helper and GraphFrames helper currently encode the full-scale row
domain; the implementation must parameterize them for 45M/225M without
changing their current callers. No approval file, runtime pins, slot lock or
attempt directory is supplied here.

The pilot outcome must remain diagnostic. It cannot qualify accepted-source
entropy, managed publication, Unity Catalog, production authority, full-scale
capacity, or a target-scale completion time. Any next optimization is a
separate one-variable proposal based on measured bottleneck evidence. The
1B/5B run remains held under the R05 packet's owner decision and review gate.
