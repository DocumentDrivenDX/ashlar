# R05 full Delta 4 synthetic scale trial packet (2026-10-23)

**Status: proposal only; held; not execution-ready.** This is an inert, local-only plan for one
1,000,000,000-node / 5,000,000,000-edge `.4-xxhash64` trial. It has not started
Spark, created a fixture, consumed cloud compute, or deleted data. The 16.50-minute
write estimate is above the 15-minute owner-review trigger and is not permission
to run. The offline generator creates only an in-memory SQL plan. A proposed
worker and isolated local process supervisor now exist and passed an independent
source review plus seven focused inert checks. Neither has been used for native
execution; the full attempt remains held.

## Decision and scope

Use the selected full `.4-xxhash64` Delta carrier and a single deterministic
generation pass per role. Run exactly two exact point lookups (one node, one
edge), exactly two logical persisted-table scans (full row/count/value
reconciliation and independent endpoint-formula validation), and one bounded
actual GraphFrames action over the persisted Delta frames. Each logical scan
is one separately timed semantic operation; Spark may implement an operation
with multiple jobs/stages, and those nested jobs are recorded under that
operation rather than counted as additional logical scans. The GraphFrames
action may itself scan the Delta inputs and can create several Spark jobs; it
remains a distinct graph-engine operation, not a third explicit validation
scan. Construct the GraphFrames view directly
from the two pinned Delta DataFrames; do not materialize a second graph copy or
collect full tables. The bounded action must return the complete expected
source-qualified edge-occurrence bag with edge IDs, preserving parallel edges.
No publication, ACK, Unity Catalog, production-source, or production-authority
claim is in scope.

The expected endpoint oracle is independent arithmetic, not a result computed
from the generated table: for edge ordinal `e` in `[0, 5B)`,
`source_id = floor(e / 5)` and
`target_id = (source_id + floor((e mod 5) / 2)) mod 1B`; both endpoint types
are BIGINT `1`. This yields exactly 5B edges with valid endpoints in
`[0, 1B)`. The full persisted scan must count invalid endpoints as well as
valid ones. The independent oracle is in `oracle.json`; its script only checks
this arithmetic and the estimate, without reading or generating row data.

## Measured basis and full-pipeline budget

The selected write-only estimate uses the R05 reassessment's completed
225M-edge ZSTD observation (25.962255 seconds) and the 18-core calibration's
45M-node observation (9.67956 seconds), each conservatively discounted to 80%
of measured throughput. It is **990.050 seconds / 16.50 minutes for writes
alone**. The older calibration-only projection was 18.05 minutes. The SNAPPY
candidate stopped at its output cap before INSERT completion and contributes no
timing. The 68.050 decimal GB active-Parquet size is row-linear arithmetic from
the 270M-row sample; it excludes Delta logs, temporary/staging files, event
logs, output, and any retained versions.

## Optional concurrent node/edge write candidate

The default remains sequential. The worker now has an opt-in
`writeSchedule: "parallel-node-edge"` authorization value that submits the
independent node and edge INSERTs together to the same local Spark session.
Both exact Delta table writes and their carrier schemas/protocols remain
unchanged. No publication vector is produced by this synthetic trial; later
consumers must continue to wait until both tables independently validate.
Failure or phase cancellation cancels the shared Spark job group and joins both
writer threads before the worker returns. The supervisor still owns the child
process boundary and cleanup after quiescence. This mode has an 840-second
phase cap within the existing 1,200-second write wall cap, and is bound by the
packet digest and explicit owner authorization. It is a candidate only; no
concurrent write has run.

## Held paired-writer discriminator

An executable but **held** local packet implements one sequential-then-parallel
comparison at exactly 45,000,000 nodes / 225,000,000 edges. Both output pairs
use the same digest-pinned, deterministic UMF-generated range SELECT/INSERT
plan, first sequentially and then in parallel, without a persisted source copy.
Each pair receives exact `.4-xxhash64` schema/layout/protocol checks and one
combined per-table row/value/endpoint aggregate pass. It does not run GraphFrames
or add a second count or endpoint scan. Results include total write wall time,
per-role write durations, and each mode's DDL/write/validation wall time. The
comparison retains the measured generator-to-Delta path; it remains only a
bounded scheduling discriminator, not full materialization qualification or a
target-scale projection. CPU, peak RSS and temporary-disk high water are not
claimed by this packet. Treat a result as a candidate only if the parallel
mode's total is at least 10% faster and all validation and cleanup checks pass.
The sequential run always precedes the parallel run in the same Spark JVM, so
JIT/codegen and cache warming can favor the second mode; this one-pair design
intentionally has no reverse-order matrix. Compare with the retained independent
sequential sample for context, and do not infer qualified full-scale throughput.

The [paired worker](r05_paired_writer_packet.py) is inert on import. The
[dedicated supervisor](r05_paired_writer_supervisor.py) requires a separate
closed authorization bound to the packet, source and runtime digests, plus a
reviewed local runtime and exclusive pre-existing slot lock. The lock only
coordinates with other callers that acquire that exact file; R02's retained
runner has no shared lock protocol, so the owner must schedule those local runs
sequentially and capture a fresh process inventory before each. It enforces one
attempt, a 780-second worker cap, 900 seconds aggregate, 32 GiB private-root
cap, 16 GiB active Delta cap and 64 GiB fresh free-space floor. It samples
these caps once per second during execution. At timeout or surveillance
failure it terminates and reaps the child process group. Cleanup is attempted
only after successful worker completion and process-group quiescence; the supervisor independently
checks all four output paths, their filesystem identities, Delta UUIDs, current
versions and writer protocols, then removes only those receipt-owned paths and
its recorded scratch. Any mismatch or incomplete receipt preserves the attempt
root. No authorization file is included, and the packet has not started Spark
or run a paired comparison. The worker cap is 780 seconds; cleanup stops by
second 860 to reserve the final 40 seconds for the supervisor receipt and slot
release, within the 900-second aggregate cap. Sol must review the exact packet
before execution; the authorization records the owner's already-directed
sample-based local performance work, not a new permission prompt. R02 and this
trial must be scheduled sequentially with a fresh Spark process inventory before
each run because R02 does not use the paired supervisor's lock. To prepare an
inert approval, run `r05_paired_writer_supervisor.py --print-review-pins
--runtime-pins <runtime-pins.json>` and copy its `packetSha256`,
`sourcePinsSha256` and `runtimePinsSha256` into the closed authorization with
`status: approved`, `attempts: 1`, `nodeRows: 45000000`, `edgeRows: 225000000`,
`totalSeconds: 900`, `workerSeconds: 780`, `rootBytes: 34359738368`,
`activeDeltaBytes: 17179869184`, `minimumFreeBytes: 68719476736`,
`predictiveOptimizationDisabled: false`, and an `ownerDecision` recording the
existing user direction. No approval file is checked in. A paired result cannot
establish canonical-scale throughput or target-scale memory/storage safety; the full 1B/5B attempt remains
separately held. Even a perfect concurrency win cannot meet the few-minute
aspiration by itself because the node write alone projects to about 269 seconds.

No target-size durations exist for startup, table DDL, two point reads, either
scan, GraphFrames action, footer/protocol checks, or cleanup. Do not manufacture
estimates for these phases. The proposed hard ceiling is 30 minutes end-to-end:
the 16.50-minute write estimate receives a 20-minute cumulative write cap (about
21% headroom), leaving at most 10 minutes for every other operation. This is a
fail-fast ceiling, not a claim that the end-to-end goal of “a few minutes” is
met. Abort the attempt when any phase cap or the aggregate 30-minute deadline
is reached. Record nested Spark jobs once under their parent operation and
attribute overlapping GraphFrames work by SQL execution and stage IDs.

## Proposed resource and cleanup bounds

These are proposed admission limits, not measured demand guarantees:

| Resource | Proposed cap / admission rule |
| --- | --- |
| Host/runtime | Dedicated local host only; at most 18 Spark local workers, 24 GiB JVM max heap, 128 GiB physical RAM. No shared/default Databricks cluster. |
| Disk | Require at least 512 GiB free on the exact filesystem before launch. Total private attempt root cap 256 GiB, including both tables, Delta logs, Spark scratch, event logs, GraphFrames output, and receipts. |
| Active Parquet | 128 GiB cap across the two tables; request cancellation when observed usage exceeds it. 68.05 GB is only the current estimate. |
| Runtime | One attempt, 1,800 seconds end-to-end maximum from child start through owned-table cleanup and receipt close. The proposed supervisor hard-terminates and reaps the child process group at limits; no retry. |
| Admission, startup and both table DDLs | 60 seconds cumulative; abort before writes if exceeded. |
| Node write | 360 seconds maximum; abort on timeout. |
| Edge write | 840 seconds maximum; abort on timeout. Both writes together are capped at 1,200 seconds. |
| Two point lookups | 60 seconds cumulative; abort before scans if exceeded. |
| Two logical persisted scans | 120 seconds each, 240 seconds cumulative; abort on either timeout. Multiple Spark jobs/stages within a logical scan count toward its wall cap. |
| GraphFrames operation | 120 seconds including frame construction and the single action; abort on timeout. |
| Final validation and cleanup | 120 seconds cumulative; abort on timeout and report any uncertain cleanup without deleting unverified paths. |
| Graph result | One seed, one hop, at most 10,000 output occurrences plus one overflow sentinel; refuse and retain evidence if cap is exceeded. No table collection. |
| Cleanup | Delete only the two exact private Delta table paths after verifying ownership receipt, filesystem identity, and Delta UUIDs; preserve logs and final receipt. On uncertain ownership, refuse deletion and stop. |

The machine observations (18 cores, 128 GiB RAM, 2.2 TiB free disk) are from
the historical calibration receipt, not fresh admission evidence. Before any
future authorization, recapture current host, free disk/inodes, process/slot
inventory, runtime and JAR digests, and the selected UMF bridge identity. The
exclusive native-slot check and owner clearance must be fresh immediately
before a separate authorization.

## Execution sequence and acceptance

1. Resolve the missing exact preconditions below and obtain an explicit owner
   decision after reviewing the 16.50-minute write-only estimate and the full
   pipeline's unknown scan/GraphFrames/cleanup cost. Keep this packet held until
   that separate decision is recorded.
2. Capture read-only native-slot/process/filesystem evidence. Confirm the
   proposed private attempt path is absent and predictive optimization remains
   enabled. Pin runtime, JAR, source, carrier and UMF realization digests.
3. Run the tiny source-qualified discriminator first under its own review and
   authorization. It must pass overlapping IDs across two sources, an isolated
   node, self-loop and parallel-edge identity, and refusal on custody/version
   drift. This does not replace full-size acceptance.
4. In one supervised attempt, create both tables using the admitted UMF
   generated `.4` DDL and verify writer-7 protocol, exact selected layout,
   schema, versions and locations before writing. Generate and write each role
   once; do not retry or add a codec/layout matrix.
5. Execute exactly two point reads with exact full identity predicates; retain
   complete rows and plans. Execute exactly two scans: (a) persisted row counts
   and carrier-presence/value witness, (b) independently computed endpoint
   validity counts. Record plans, input files/bytes, task/stage CPU and wall
   time, spill and GC. Treat each as one logical persisted scan even if Spark
   emits several jobs or stages; include all those jobs in that scan's cap.
6. Pin both Delta table UUID/version/schema/protocol tuples, construct the
   actual GraphFrames view directly from those persisted frames, and execute
   exactly one source-qualified bounded seed action. Preserve the bounded
   result bag, plan, SQL/stage identifiers, scan and shuffle metrics. Do not
   use a graph export artifact as a substitute for action evidence.
7. Reconcile exact counts (1B/5B), full endpoint validity (zero invalid), both
   point results, full `.4` value/multiplicity checks, actual graph result and
   all resource caps. Clean only receipt-owned paths; record cleanup outcome
   and final filesystem inventory. Keep all run results under build evidence.

Acceptance requires all semantic checks and exact count witnesses to pass, the
GraphFrames action to execute on actual pinned Delta frames, and every cap and
cleanup check to pass. A timeout, overflow, failed admission, or uncertain
cleanup is a failed/held attempt; no retry is implied.

For this deterministic synthetic trial, TD-008 permits deriving identity
uniqueness from the frozen `range` generator and reconciling persisted counts,
ID ranges, row values, and endpoint/formula witnesses; it expressly excludes a
second full-graph uniqueness join. The worker retains a linear persisted
count/range/sum witness and states that it is not itself a mathematical distinct
proof. This benchmark derivation does not qualify production publisher
enforcement or a production multiplicity claim.

## Exact unresolved preconditions

- The checked-in `canonical_graphframes_materializer.py` intentionally refuses
  before Spark or filesystem access. It only covers the tiny five-node/four-edge
  discriminator and explicitly requires admitted UMF `.4` DDL plus writer-7
  verification. The separate candidate generator and full trial driver are
  present and independently reviewed as inert source, but have no native
  execution evidence.
- The UMF 0.5.6 candidate bridge currently has one-table local DDL/write
  acceptance only. A two-role offline bridge admission is recorded in
  `offline-generation-check.json`; it does not qualify two-role native DDL,
  writes, or scale.
- The R05 GraphFrames R4 packet is for the tiny discriminator, uses a 300-second
  / 512 MiB action bound, and is blocked pending fresh slot evidence and a
  separate owner authorization. It is not the full-scale runner or approval.
- There are no target-size scan, point-read, GraphFrames, footer-validation,
  cleanup, or total-wall measurements. These remain unknown until an admitted
  trial; the 30-minute total and phase caps are proposed fail-fast bounds, not
  estimated operation durations or evidence that the few-minute goal is met.
- Fresh host capacity, inodes, slot ownership, actual runtime/JAR identity,
  dedicated output location and exact UMF-generated DDL must be revalidated.
- `r05_full_delta4_supervisor.py` is the proposed one-attempt local entrypoint.
  It prepares a mode-0700 attempt root and Spark scratch before child start,
  checks the reviewed Python/JDK/JAR and source pins, requires an owner approval
  bound to the final packet/runtime/source digests, and launches one isolated
  child process group. It watches aggregate time, phase state and byte caps,
  hard-kills/reaps on breach, and delegates success cleanup to a second bounded
  process only after the Spark child group exits. On uncertainty it preserves
  the attempt. Live capacity walks tolerate a vanished file only under the
  private Spark scratch trees, where Spark may atomically rename shuffle files;
  table and receipt walks remain strict. It also requires a pre-existing
  private shared-slot lock file and a fresh local Spark process scan; no
  repository-wide slot-lock protocol is
  established, so this remains a deployment precondition. No owner authorization
  artifact exists.
- The candidate event-log parser has not yet been exercised against a full R05
  event log. The retained edge-write log confirms SQL execution IDs map to jobs
  through `spark.sql.execution.id`, then to stage IDs through each job's stage
  list. The candidate parser maps stages to the phase-named
  `spark.jobGroup.id` set by the worker; it marks attribution incomplete if a
  job group is absent or a task stage is ambiguous. In the retained attempt-03
  event stream all 7 job records lack `spark.jobGroup.id`, so the parser reports
  phase attribution incomplete there. The sample predates worker phase groups;
  propagation from this worker is not yet confirmed on the selected runtime.
  The parser also bounds each JSONL record to 256 KiB, the stream to 1M events,
  SQL descriptions to 512 bytes, and SQL/job/stage/link counts; limit overflow
  is explicitly incomplete, and zero task-stage records cannot pass. SQL
  execution IDs/descriptions and task sums are recorded, but no per-scan
  physical operator attribution is claimed.

Related governing evidence: [R05 budget and gates](../r05-end-to-end-budget-design-20261023/README.md),
[storage throughput reassessment](../r05-storage-throughput-reassessment-20261023/README.md),
[18-core calibration](../current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-18core-calibration-20261021.json),
[writer-7 one-table acceptance](../current-upstream-release-20261023/umf-0.5.6-delta4-one-table-acceptance-20261023/README.md),
[GraphFrames R4 candidate](../canonical-graphframes-holder-candidate-20261023/r05-native-action-r4/README.md),
and [TD-008](../../../02-design/technical-designs/TD-008-pinned-delta-graphframes-scale-action.md).

The [offline proposal calculator](proposal_check.py) is deliberately inert: it
prints the sample-derived write arithmetic and independently checks endpoint
formula boundary cases. The [parameterized `.4` generator](full_delta4_generator.py)
builds both role INSERT plans and obtains each CREATE TABLE statement from the
pinned `LocalUmfDelta4Bridge`; it checks the generated UMF schemas against the
selected `.4` carrier and checks endpoint boundary cases. It imports no Spark
runtime and executes neither DDL nor INSERT. Sol ran its exact two-role plan
through the pinned UMF bridge in the retained Python 3.12.14 / mini-racer
0.14.1 runtime; both proposals passed offline admission
([receipt](offline-generation-check.json)). This is a materialization
prerequisite. The [candidate worker](full_delta4_trial.py) and [proposed
supervisor](r05_full_delta4_supervisor.py) compose the admitted plan with one
write per role, two point reads, two persisted logical scans, and one actual
GraphFrames edge-bag action. The supervisor has hard process-group timeout and
resource surveillance, phase evidence, exact ownership checks, and post-exit
success cleanup. It is still a proposal: no Spark or scale run has started, no
owner authorization exists, and a shared-slot coordination protocol is not
established. The supervisor now captures capped private child diagnostics
(32 MiB each) and enables an uncompressed, rolling Spark event log (512 MiB
aggregate cap); after child exit it records a compact per-stage sum of task
wall/CPU time, GC, input bytes, shuffle bytes, and spills, plus event-log size
and digest. These are proposed capture paths only: no event log has been
produced for the full attempt. SQL-to-stage lineage exists in the retained
edge-write log, but phase-group propagation is not yet confirmed on this
runtime. Row-level and per-operator scan attribution remain open. The synthetic
identity derivation follows TD-008 and does not establish production publisher
enforcement. The selected write-only estimate remains 990.050 seconds / 16.50
minutes, above the 15-minute review trigger; full execution remains held for
owner review.
