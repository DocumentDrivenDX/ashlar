# Canonical `.4` GraphFrames holder candidate — 2026-10-23

This is an offline implementation candidate for the synthetic canonical-carrier
benchmark described by TD-008. It does not create or modify tables, start Spark,
run a GraphFrames action, or qualify a scale workload.

`PinnedCanonicalSyntheticDeltaFixture` is separate from the existing tiny
three-node/two-edge compatibility fixture. It binds a descriptor digest to the
declared generator/source hashes, row counts, endpoint formula, selected seed and
oracle rows; the digest also freezes the generated `.4` carrier resource bytes,
UMF revision and runtime *selection*. It reads the exact generated object/edge
schemas, observes two distinct native Delta UUIDs, versions, schemas and protocol
twice at open, and provides `VERSION AS OF` frame handles under a finite renewable
metadata lease. Focused action tests cover bad descriptors, schema refusal,
duplicate UUIDs, changed versions, expiry, version selection, no carrier-row
collection, the separate R05 identity discriminator, and writer-7 protocol
admission; 32/32 passed. The
separately reviewed inert supervisor suite passed
11/11 focused tests, including admission and failure-cleanup controls.

These checks are metadata correspondence only. The generator/source/oracle
declarations have not been independently audited or executed by this candidate;
only Spark's version is observed, while Delta, GraphFrames and JDK values are
recorded as selected versions. It does not establish retained data-file
availability, exclusive use, or an actual carrier read. Its lazy frame handles
do not authorize a GraphFrames action and do not claim source progress, protected
ACK, publication, Unity Catalog or production authority. The existing tiny
compatibility smoke remains the actual execution evidence for this current-runtime
synthetic lane.

The canonical action path selects a separately versioned `.4` writer-7
protocol. It matches native `DESCRIBE DETAIL` reader/writer/features with the
latest protocol action replayed from the complete bounded local Delta commit
history. The writer-2 contract remains unchanged for the legacy compatibility
smoke. This gate has mocked coverage only and does not qualify a materialized
`.4` table.

Before enabling the canonical action, a separately reviewed fixture runner must
verify generator/source/oracle correspondence, attest the actual runtime/JARs,
hold retained-file and exclusive-use custody through closing checks, and run the
bounded functional action. A full 1B/5B run remains separately gated by the
approved scale budget and R05 evidence.

The [action-readiness packet](action-readiness-packet.md) defines the minimum
freeze, custody, runtime admission and exact next implementation slice. It is a
design packet only; no action has been run or admitted by it. A separate
[R05 identity discriminator](action-discriminator-r1/review-packet.md)
adds the required cross-source overlapping-ID, self-loop, isolated-node and
parallel-edge cases without changing the existing fixture or profile. Its
source/oracle and manifest/anchor passed Sol's independent static review; the
32-test result is mocked
and inert, and does not establish a native GraphFrames pass.

The offline follow-up now supplies a deterministic three-object/two-parallel-
edge source, a separately authored two-occurrence oracle, same-lock writer and
cleanup helpers, and the existing review-gated capped one-hop entrypoint. The
exact source and independent oracle are in the [action fixture](action-fixture/README.md).
No tables have been materialized and no Spark or GraphFrames runtime has been
started. The final action descriptor is separately bound to its reviewed
manifest anchor. The distinct hash-pinned native supervisor and worker have
passed independent review as inert preparation, documented in
[native-supervisor](native-supervisor/review-packet.md). Execution remains
disabled because exact runtime paths are unset; a fresh exclusive-slot check
and separate user authorization are also required. The invocation packet keeps
its pending status and cannot admit a run until it is immutably re-frozen with
review status and the exact runtime/accounting inputs. No wrapper authorization
is implied by the action anchor, and no native attempt has run.

A distinct R05 five-node/four-edge local execution packet is prepared under
[`r05-native-action-r1`](r05-native-action-r1/README.md). Its worker uses the
UMF-backed R05 materializer and the reviewed R05 manifest anchor, while its
supervisor validates the 5/4 persisted witnesses and three-row seed bag. The
packet is fail-closed: R05-specific owned-table cleanup is not implemented or
qualified, runtime executable/JAR paths are unset, and no wrapper review or
one-attempt authorization exists. No Spark or GraphFrames process was started.
