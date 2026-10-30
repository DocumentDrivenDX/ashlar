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

**Owner decision (2026-10-29): “Hold and redesign first.” No 1B/5B run is
authorized.** This packet narrows
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
| Sequential setup + writes + one combined row/value and endpoint-formula scan | 66.278 s | 1,841.056 s / 30.684 min |
| Concurrent setup + writes + one combined row/value and endpoint-formula scan | 60.924 s | 1,692.333 s / 28.206 min |

The sample's active bytes work out to 10.4866 bytes per node row
(`471,896,757 / 45M`) and 11.4197 bytes per edge row
(`2,569,437,803 / 225M`), or 11.2642 bytes per row weighted across the
270M-row pair. Linear target arithmetic is about 10.49 GB for 1B node rows and
57.10 GB for 5B edge rows, 67.585 GB total active Parquet. This excludes logs,
staging, retained files, shuffle/spill and graph-engine working data.

For one workload unit of 1M nodes plus 5M edges, the measured concurrent write
took `30.620 / 45 = 0.6804 s`; the setup/write/combined-scan mode took
`60.924 / 45 = 1.3539 s`. At 80% of the measured rate, these become 0.8506 s
and 1.6923 s per unit. The target contains 1,000 such units, or 22.222 times
the 45-unit sample; the resulting write-only and measured-mode estimates are
14.176 and 28.206 minutes.
These are planning extrapolations from one sample, not target-scale measurements.

### Design-target checkpoint (napkin math)

For this checkpoint only, interpret “a few minutes” as a **provisional five-minute
design target** (300 s); the phrase has not been assigned a numeric threshold by
the owner and this is not an approved pass/fail criterion. The 3,041,334,560
sample active bytes over the 30.620 s concurrent write imply 99.33 decimal MB/s
observed, or 79.46 MB/s at the 80% planning rate. Scaling active bytes by
22.2222 gives 67.585 GB, which would require 225.28 MB/s end to end to fit in
300 s. That is 2.84× the 80%-discounted write rate (and excludes setup,
validation and graph work).

The fuller measured phase is 60.924 s for setup, writes and one combined scan.
Its 80%-discounted linear projection is 1,692.3 s (28.206 min), or 5.64× the
entire provisional 300 s budget before any omitted phases. To meet 300 s by
linear extrapolation, that complete measured phase would need to take no more
than `300 × .8 / 22.2222 = 10.8 s` on the 45M/225M sample; once omitted phases
are budgeted, the gate is lower: `(300 − omitted_phase_seconds) × .8 / 22.2222`.
The 10.8 s figure is a necessary upper bound for the measured phase, not a
claim that the sample by itself proves target-scale behavior.

Do no expensive optimization or full run yet. First profile a smaller,
representative sample that separately measures writes, persisted scans, and
GraphFrames overhead, so the dominant phase and its scaling behavior are
visible. Require that profiled path to meet an agreed budget in a full-path
local pilot before reconsidering 1B/5B. A single warm, second-in-JVM sample does
not establish a physical I/O bottleneck, a performance proof, or a reliable
full-scale runtime. The 1B/5B attempt remains held, and the 15-minute
owner-review constraint remains in force.

Use the concurrent write sample as the planning baseline for the one-run
candidate, while retaining its second-in-JVM bias. The **80% throughput target**
is writes completed within 850.556 seconds. A result beyond that value fails
the performance target even if it is inside the hard write cap. The proposed
20-minute cumulative write cap is fail-fast containment, not a pass threshold.
The paired worker's timed 60.924 s mode total includes setup, concurrent writes,
and one combined persisted row/value plus endpoint-formula scan:
`r05_paired_writer_packet.py` calls `_row_value_scan(...,
include_endpoint_formula=True)` before recording `modeTotalSeconds`. The
28.206-minute projection therefore already includes one pass that checks both
row/value and endpoint-formula witnesses. The requested two separately timed
scans need one additional full scan beyond this measured baseline; its target
cost is unknown. Do not count the measured combined pass again as an additional
scan. The 30.304 s sample residual after writes also includes table setup and
metadata, so it does not isolate scan time. Scaling that entire residual at 80%
gives a 14.03-minute planning anchor, not an endpoint-scan measurement.

The linear 80%-rate projection for setup/write/combined-scan is 28.206 minutes.
Express the end-to-end estimate as **28.206 minutes + X**, where X includes any
target-scale deviation in that projected phase (for example, warmup/cache
effects, Delta metadata/file geometry, spill or skew) plus the additional
separate scan, two point lookups, graph action, startup, final checks and
cleanup. These effects and phases are unmeasured; the 28.206-minute projection
is not a fixed measured portion, so X and the total remain unknown. The first
combined pass is already represented in 28.206 minutes; only the requested
second pass belongs among the additional phases in X.
At a 15-minute total, the measured portion alone needs at least 1.88× speedup
(46.8% elapsed-time reduction); at five minutes it needs at least 5.64×
(82.3% reduction), before remaining phases. A 30-minute envelope leaves 1.79
minutes for them; a 45-minute envelope leaves 16.79 minutes. Treat 45 minutes
only as a provisional planning envelope for redesign, not an acceptance target
or evidence that the remaining phases fit. The “few minutes” goal has no
numeric threshold and cannot be marked passed from this packet.
Treat 30 minutes as a fail-fast stop bound, not a credible predicted completion
time. The owner chose **“Hold and redesign first”** after review of the
28.206-minute setup/write/combined-scan projection and the at-least-5.64×
speedup needed to fit that measured portion into a five-minute total, before the
remaining workload. The full run stays held; this packet grants no run
authorization.

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
eligible for fresh review; they do not authorize a run. The owner decision is
to hold and redesign first: the measured setup/write/combined-scan projection
is 28.206 minutes, while the remaining point reads, second scan, graph action,
startup, final checks and cleanup add unmeasured work.

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
