---
ddx:
  id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: governed_by
  - id: TD-008
    kind: governed_by
  - id: ashlar.release-closure-20261029
    kind: informed_by
---

# R05 one-shot 1B/5B local run packet — 2026-10-29

**Status: held for owner review; no run authorization.** This packet narrows
the existing R05 proposal to one local synthetic workload: materialize
1,000,000,000 `.4-xxhash64` nodes and 5,000,000,000 edges in two Delta tables,
perform exactly two point lookups and two persisted-table scans, then construct
the transient GraphFrames view from those persisted tables and run one bounded
engine action. That direct, version-pinned mapping is the requested export to
the graph engine for this trial; it avoids writing a second persistent graph
copy. The packet does not claim a separate file export. It is not a platform or
CI matrix, and it provides no source,
publication, ACK, Unity Catalog, production, or generalized capacity claim.
Delta schemas, columns, protocol, identities, values, and edge multiplicity
remain the selected full carrier.

## Measured baseline and decision

The completed R05 paired comparison wrote 45M nodes plus 225M edges (270M
rows) on Spark 4.2.0, Delta 4.4.1, `local[18]`, with 128 GiB physical RAM and a
24 GiB JVM maximum. Its sequential write was 35.227 s; the concurrent node/edge
write was 30.620 s. The corresponding write-plus-setup-and-compact-validation
times were 66.278 s and 60.924 s. Both modes produced 3,041,334,560 active
Delta bytes and matched their recorded row/value/endpoint witnesses. Concurrent
ran second in the same JVM, so warmup may favor it; this is one planning sample.

Scale factor is `1,000M / 45M = 5,000M / 225M = 22.2222`. At 80% of each
sample's measured throughput, target seconds are `sample_seconds × 22.2222 / .8`:

| Measure | Sample | 1B/5B at 80% measured rate |
| --- | ---: | ---: |
| Sequential writes | 35.227 s | 978.528 s / 16.309 min |
| Concurrent node/edge writes | 30.620 s | 850.556 s / 14.176 min |
| Sequential setup + writes + compact validation | 66.278 s | 1,841.056 s / 30.684 min |
| Concurrent setup + writes + compact validation | 60.924 s | 1,692.333 s / 28.206 min |

Use the concurrent write sample as the planning baseline for the one-run
candidate, while retaining its second-in-JVM bias. The **80% throughput target**
is writes completed within 850.556 seconds. A result beyond that value fails
the performance target even if it is inside the hard write cap. The proposed
20-minute cumulative write cap is fail-fast containment, not a pass threshold.
The prior sample's 28.206-minute setup/write/compact-validation projection
already leaves only 1.794 minutes inside a 30-minute whole-run cap for the
additional point reads, second scan, graph action, full final checks, and
cleanup. Their durations are unmeasured, so the total cannot be predicted
precisely; current evidence makes completion within 30 minutes unlikely.
Treat 30 minutes as a fail-fast stop bound, not a credible predicted completion
time. Owner review is required before any run because setup, writes and compact
validation project to 28.206 minutes, with the requested lookups, second scan,
graph action, final checks and cleanup still to add. The owner’s “few minutes”
goal has no numeric threshold and cannot be marked passed from this packet.

Technical pass requires exactly 1B object rows and 5B edge rows; zero invalid
endpoint type/range/formula witnesses; both complete point rows matching their
frozen carrier oracles; one complete bounded GraphFrames occurrence bag;
unchanged schema/protocol and exact values/multiplicity; all limits met; and
verified owned cleanup. A write time over 850.556 s is a throughput failure.
Any semantic mismatch, exceeded hard limit, incomplete graph result, or
uncertain cleanup is a failed/held attempt. Passing the 30-minute ceiling alone
is never acceptance.

Active-table arithmetic from the paired sample is
`3,041,334,560 × 22.2222 = 67,585,212,444 bytes` (67.585 decimal GB / 62.944
GiB). This estimates active Parquet only. Logs, shuffle/spill, retained files,
Spark event logs, and GraphFrames working data are excluded.

## One run and its evidence

The existing entry point is
[`r05_full_delta4_supervisor.py`](r05-full-delta4-scale-packet-20261023/r05_full_delta4_supervisor.py).
After separate owner review, fresh host/runtime/slot admission and a closed
approval bound to the reviewed packet/source/runtime digests and selecting the
parallel node/edge schedule, its sole mutating invocation is:

```sh
python3 docs/helix/04-build/evidence/r05-full-delta4-scale-packet-20261023/r05_full_delta4_supervisor.py \
  --authorization <owner-approved-one-run.json> \
  --runtime-pins <reviewed-local-runtime-pins.json> \
  --attempt-parent <private-parent-on-admitted-volume> \
  --native-slot-lock <fresh-exclusive-local-slot-lock>
```

Those files and paths are placeholders, not prepared credentials or an
authorization. The current runner requires one `parallel-node-edge` schedule
selection, exact node/edge counts, one attempt, and matching packet/source/
runtime digests. The proposed approval selects `parallel-node-edge` to match
the measured throughput basis. There is no retry, codec/layout variant, shared/default
Databricks cluster, or cloud spend.

The single attempt sequence is:

1. Admit the exact pinned local runtime, UMF-generated DDL, `.4-xxhash64`
   schemas and Delta protocol; create the two private Delta tables and generate
   each role once from its deterministic range plan.
2. Record exact row counts, table UUID/version/schema/protocol and active
   bytes. Look up node `(r05-full-scale-source, type_id=1, id=0)` and edge
   `(r05-full-scale-source, rel_type_id=7, id=0)` with full identity predicates;
   retain full returned carrier rows and query plans.
3. Run two separately timed scans over persisted Delta data: (a) row counts,
   identity ranges and full selected-carrier value/presence witnesses; (b)
   independent edge endpoint range/type/formula validation. Record each scan's
   plan, files and bytes read, task/stage time, spill, and GC.
4. Read the exact pinned Delta versions, map their DataFrames to transient
   source-qualified GraphFrames keys, and perform exactly one bounded one-hop
   seed action. Require the complete expected edge-occurrence bag, including
   separate parallel-edge identities, at no more than 10,000 results. This is
   an in-memory engine mapping/action, not a separately persisted graph export.
5. Reconcile semantic witnesses and limits, then remove only the two
   receipt-owned table paths after verifying filesystem identity and Delta
   UUIDs. Preserve the attempt evidence and verify cleanup.

**Runner conformance is implemented in the held worker and supervisor.** The
row/value scan and endpoint formula scan are separate timed actions over the
exact pinned persisted Delta frames; the supervisor requires both phase
records and the endpoint scan's independent plan and complete zero-invalid
witness. The graph path directly maps the same pinned Delta frames into
transient GraphFrames keys and performs one bounded action, which is the
requested transient export for this trial. No durable graph-engine export is
included. The concurrent write ceiling is 1,200 s, allowing the 850.556 s
80%-rate target to be measured while preserving the distinct performance
threshold. The supervisor samples RSS for the isolated worker process group
and stops above 96 GiB. The held worker packet digest includes this packet,
and the source digest includes the worker, supervisor, generator, focused
conformance test, and this packet. These source changes make the entry point
eligible for fresh review; they do not authorize a run. The packet remains
held for owner review because the measured setup/write/compact-validation
projection is 28.206 minutes and the requested reads, scans, graph action,
final checks and cleanup add unmeasured work.

## Bounds and stop rules

The existing proposal supplies these candidate bounds; they require fresh
admission and closed approval. They do not imply the machine currently has the
recorded capacity.

| Resource or phase | Bound / stop rule |
| --- | --- |
| Host | Dedicated local host; 18 Spark workers, 24 GiB JVM maximum, 128 GiB physical-RAM eligibility. The paired sample observed 128 GiB, but fresh host admission is required. The supervisor samples total RSS for the isolated worker process group and terminates above 96 GiB; the sampling interval is one second, so the ceiling is enforced at observed samples. |
| Disk admission | At least 512 GiB free and 1,000 free inodes on the exact attempt filesystem. This exceeds estimated active bytes by design; require fresh measurement. |
| Attempt root | 256 GiB maximum, including tables, logs, scratch, events, and result. Stop/cancel at the enforced cap; preserve partial evidence on uncertainty. |
| Active Parquet | 128 GiB maximum across the two tables, versus 62.944 GiB row-linear estimate. |
| Total time | One attempt; 1,800 s from child start through owned-table cleanup and receipt close. No retry. |
| Admission and DDL | 60 s cumulative. Stop before inserts if exceeded. |
| Writes | Node ≤360 s; edge ≤840 s; concurrent writes together ≤1,200 s. The 850.556 s 80%-rate threshold remains the performance target; 1,200 s is the containment cap. |
| Point lookups | Two total, ≤60 s cumulative. |
| Persisted scans | Two distinct logical scans, each ≤120 s; nested Spark jobs/stages are charged to the parent scan. |
| GraphFrames | One direct pinned-frame mapping/action ≤120 s; seed result ≤10,000 complete occurrences. Overflow refuses. No full-frame collection, GraphX conversion, cache, or second persisted graph copy. |
| Final reconciliation and cleanup | ≤120 s cumulative. Any uncertain ownership, UUID, quiescence or cleanup outcome fails closed and preserves data/evidence for review. |

Stop before launch if the required owner review has not occurred, runtime/source
digests differ, the selected volume or free space fails admission, the local
Spark slot is occupied, or the action plan contains an unexpected cartesian/cross
join. During the single run stop on any phase or byte cap, a sampled RSS above
96 GiB, failed semantic witness, graph result overflow, unexpected extra
write/job/retry, or 30-minute deadline. Preserve failed or partial receipts;
never retry an uncertain write or delete an unverified path.

## Scaling claim and limits

The deterministic generator evaluates a constant-size expression per row, so
generation/write work is proportional to `N + E`. Each of the two persisted
scans is one full linear pass, and the transient GraphFrames construction is
linear in input rows before the bounded one-hop action. This avoids all-pairs
work and duplicate materialization; it explains why row-volume arithmetic is a
useful first estimate. It is not an asymptotic proof supplied by a single
measurement. Delta file/transaction metadata, skew, shuffle, memory pressure,
spill and graph-engine planning can make the target degrade superlinearly or
hit a resource wall. Record actual phase plans and metrics; qualify only this
synthetic 1B/5B execution, and do not claim general workload scaling from it.

The completed 45M/225M comparison verified counts, ID ranges, exact decimal
sums, and endpoint formulas, but explicitly omitted exact distinct counting,
point reads, full scans, GraphFrames and publication. The 1B/5B generator is a
constant-payload synthetic fixture (`source_position=id`, decimal-string
delivery ID); its result cannot stand in for accepted-source entropy or
production identity/ACK custody. Source-derived wide values, production
authority, managed Delta behavior, and external-engine persistence remain
outside its evidence.
