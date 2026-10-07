# Publication and maintenance policy candidate

Draft physical policy under CONTRACT-001, CONTRACT-002, CONTRACT-003 and ADR-001.
No producer wire format, UMF binding or new support claim is selected here.

## Evidence and limiting resource

The [r148 calculator](publication_capacity_r148.py) consumes hashed authoritative
r133 publisher, r140 maintenance and r142 reader evidence. Its
[output](out/publication-capacity-r148.json) is deterministic queue sensitivity,
not a native sustained-rate measurement. The r133 100k batch processes in
52.947 s and completes input-to-manifest in 52.952 s on existing compute.
At a modeled 10k/s arrival rate, 100k records arrive every 10 s. Holding that
single service sample constant implies 1,889 changed entities/s per serialized
writer, utilization 5.295 and 42.947 s of additional waiting per batch.
A twelve-batch model reaches 472.415 s of queue wait. These are consequences of
the stated model, not observed production capacity or safe parallelism factors.

For uniform modeled arrivals, first-batch per-record age p95 is 62.447 s;
oldest age is 62.947 s. Neither is a measured service-time p95. At 100k/s the
first-batch modeled p95 is only 53.897 s, but subsequent queue growth is worse:
51.947 s per batch. A passing first burst batch cannot admit burst recovery.
No batch-size or concurrency change is justified by this arithmetic alone.

The separate E23 clone maintenance cost is 13.340 s. Adding that sample to
publication service produces a hypothetical 66.287 s per batch; amortizing it
over ten batches still gives 54.281 s mean service. Costs are from separate runs,
not a measured combined pipeline. Maintenance reduces affected read files from
18 to 3, yet the warm repeat p95 remains 101 ms engine / 363.10 ms caller.
Immediate post-maintenance reads include remote data and regress. Mandatory
cleanup per batch is therefore not the candidate freshness solution.

## Physical policy

1. A logical publisher prepares immutable input, applies the exact eligible
   current-state changes, appends exact source/history evidence, validates and
   commits a manifest of exact Delta versions. Only that manifest advances
   consumer progress. Readers never substitute the current physical head.
2. Property-only updates reuse adjacency only with the owned structural proof
   and inherited baseline. Endpoint/identity/schema changes require separately
   validated rebuilt projections. Keep original IDs, typed endpoints, property
   maps and exact retained/source text throughout both paths.
3. Run physical maintenance outside the logical publication critical path on a
   serialized maintenance lane. This is a candidate scheduling separation, not
   evidence of conflict-free concurrent writers. Maintenance never advances
   source progress and never edits an existing manifest or pagination boundary.
4. Existing readers remain pinned to their original vector. A validated
   maintenance snapshot may become visible through a new immutable maintenance
   manifest with unchanged logical progress, linked to its logical parent.
   Receipt/publication-ID rules must distinguish this event from a new accepted
   source batch; this new manifest mode needs executable protocol qualification.
   Until then, publish maintained versions only through the existing evidenced
   logical publication path. Do not silently repoint the old manifest.
5. Admit a maintenance run only after recording its exact base versions,
   intended files/bytes, time budget, current backlog and estimated interference.
   Backlog has priority. The evidence does not justify a numeric file threshold
   or frequency yet; retain these as tuning parameters to measure. Stop admission
   of additional work when the bounded run is in doubt; inspect its owned handle
   and commits rather than submitting a duplicate mutation.
6. Before exposing rewritten versions verify full rewritten-carrier equality,
   global identity, endpoints where structural rows change and inherited history.
   Untouched-row custody may rely on verified immutable Delta files and stable
   schema only in the closed owned fixture lineage. The failed 20M wide digest
   scan remains failed evidence, not a verification shortcut for unknown writers.
7. Retain versions and immutable files required by active publications and
   cursors. No VACUUM/retention reduction is authorized by this policy. Recovery
   refuses an expired boundary; it does not continue at a newer snapshot.

## Next native experiment and stop bounds

Prioritize publisher service and queue behavior over further clustering screens.
Use existing authorized warehouse and an owned isolated clone of E23, together
with isolated source/journal/manifest outputs. Preserve canonical r139-b1. Reuse
an immutable recorded hot-set input and exact wire serializer; do not re-run the
historical canonical publisher harness against new state.

Run three serialized complete 100k property batches with modeled release times
10 s apart, without maintenance, then measure processing, wait, per-record age
and final drain time. Three batches are a bounded queue diagnostic, not sustained
10k/s or 100k/s admission. Actual source arrival is still unmeasured. Cap total
new rows at 300k current updates plus their exact source/history rows; cap the
experiment controller at ten minutes and each statement at 180 s. No whole-graph
payload digest or new compute. Record actual bytes scanned/written and preserve
owned handles before any mutation. Validate each manifest and exact changed
carriers; verify inherited structural baseline and untouched custody within the
closed lineage. Failures preserve incomplete outputs for recovery inspection.

Only if the diagnostic shows bounded processing with queue reduction should the
next iteration compare batched statement overhead, safe stage parallelism or
maintenance interference. Independent feed parallelism requires demonstrated
identity authority, fencing and atomic publication; dividing by the modeled
utilization and starting six writers would not establish correctness or capacity.
The 1B/5B scale and all agreed gates remain open. UC Delta remains selected.


### r149 native three-batch queue diagnostic

[Final native audit](out/native/ashlar_queue_r149/audited-summary.json) executes
three serialized 100k complete synthetic property batches on isolated E23/R17/J19
shallow clones, existing warehouse 2439e1f2e37ac563, runtime
19.8.x-aarch64-photon-scala2.13. Releases are modeled at 10/20/30 s (10k/s), not
real source arrivals. No readers, maintenance, resizing or new compute.

| Batch | Processing seconds | Queue wait seconds | Modeled record-age p95 seconds |
| --- | --- | --- | --- |
| 1 | 50.139 | 0.0001 | 59.639 |
| 2 | 54.654 | 40.507 | 104.661 |
| 3 | 49.974 | 85.567 | 145.041 |

Combined 300k uniform modeled per-record freshness p95 is 144.041 s. The oldest
record in batch 1 is 60.139 s old. Distinguish these quantiles from service p95,
which three samples do not establish. Input drained at 165.541 s after controller
epoch, 135.541 s after final modeled input completion. First-batch near-pass does
not admit sustained 10k/s or 100k/s burst; queue growth is actually observed under
the modeled release schedule, not just the prior fixed-cost arithmetic.

Exact20 affected fields and token changes, exact raw bytes/digests/origin/UTC
instant and property journal pass per batch. All20M unique IDs and owned structural
reuse proofs pass; independent full20M structural parity passes after the clock.
Final19.9M untouched physical custody passes under stable schema/immutable-file
assumptions, not fresh wide-payload equality or unknown-writer evidence. The second
and third batches count a full structural baseline scan inside processing; batch1
baseline is before the epoch. This extra proof cost is explicit, not attributed to
layout degradation. Prepared input is outside clock; real source staging remains
unmeasured. Source/history appends and validation are parallel separate lanes,
while logical publication is serialized. Carrier entropy remains synthetic.

Append pairs take13.696/10.670/10.596 s; raw/journal validation pairs take
18.358/19.221/11.707 s; MERGE caller3.953/3.767/3.540 s. Costs across preparation,
publication and final audit:122.642 GB recorded read_bytes and6.856 GB remote
writes (decimal). These counters are not distinct storage footprint or billing.
No whole-graph wide digest scan was repeated. Each statement is bounded180 s;
controller stops admitting SQL after600 s, not a guaranteed global process kill;
connector socket/retry bounds and durable submission correlation remain explicit.

[Cleanup](out/native/ashlar_queue_r149/cleanup/summary.json) UUID/version-checks
and drops all seven owned tables. Canonical E23/R17/J19/r139-b1 remains unchanged;
no VACUUM. The evidence prioritizes reducing publisher validation/capture service
cost with equivalent preservation proof over further singleton key changes.
Next compare one bounded precomputed-wire validation path against exact wire
checks, counting materialization inside publication; do not promote it without
measured end-to-end benefit and an explicit proof-strength comparison. The prior
r139 materialization regression remains relevant failed-candidate evidence.
UC Delta remains selected; sustained/burst/concurrent/cold/billion gates stay open.


### r150 exact wire witness screen: rejected

[Final native audit](out/native/ashlar_wire_validation_r150/audited-summary.json)
compares three alternating exact raw-wire validation pairs using immutable stage
r139 v0 and published source_record v17/r139-b1. An isolated100k-row Zstd wire
witness preserves complete20-field-plus-old_json UTC/microsecond encoding; full
symmetric exact witness comparison passes, as do100k unique delivery memberships
and all six exact raw UTF8/digest/origin validations. No canonical, raw, journal,
MERGE or publication write occurs; only one temporary witness is created.

Control caller5.220/5.030/6.216 s; stored5.436/4.844/5.107 s. Engine control
4.750/4.579/5.207 s; stored4.911/4.227/4.651 s. Control91files versus stored95;
control all remote0, first stored19.902MB remote, other stored remote0. Actual
witness8files673.355MB/reader3/writer7; source-stage and witness physical layouts
are different. Do not attribute timing differences to serialization alone.

Materialization7.473 s plus stored validation yields a hypothetical12.317–12.909 s
single-use cost versus5.030–6.216 s direct checks. Independent exact witness
verification12.774 s raises that sensitivity to25.091–25.683 s. These sums are
separate samples, not a measured publication. Digest does not replace exact raw
UTF8 comparison. No demonstrated benefit: reject this validation witness path;
retain the earlier r139 whole-publication materialization regression as evidence.

Total recorded reads11.515GB/remote writes0.673GB, not billable amounts or distinct
storage footprint. Each statement90 s; socket/retry bounded and submissions
correlated. [Cleanup](out/native/ashlar_wire_validation_r150/cleanup/summary.json)
UUID/version-checks and drops the witness; canonical E23/R17/J19/r139-b1 unchanged,
no VACUUM. All native metric IDs are final and result-uncached; three cached-data
pairs do not admit service/freshness/concurrency/billion targets.

Next screen explicit source-feed/epoch bounds in the raw validation query using
its existing source_feed/source_epoch/delivery_id statistics. Such bounds must be
proved from immutable stage membership and retain missing-row and exact-origin
refusals. This targets raw-file pruning without copying complete wire payloads or
weakening preservation proof. If it fails, do not keep adding per-batch copies.
UC Delta remains selected and the full performance goal remains active.


### r151 explicit raw origin bounds: pruning candidate

[Final native audit](out/native/ashlar_origin_validation_r151/audited-summary.json)
compares three alternating100k exact raw validations over stage r139 v0/source
records v17. A stage-origin GROUP BY proves one non-null feed/epoch and100k rows;
static feed/epoch filters supplement the existing apply_batch_id predicate.
Because every stage row has that exact origin and the join already requires it,
these filters preserve the logical match set. They do not replace delivery-ID,
missing-row, wire UTF8/digest/origin/UTC instant validation or global raw-membership
checks. Six exact comparisons pass. Two deliberately incorrect static origin
bounds each produce100k failures, confirming that missing rows are not suppressed.

Control caller6.148/4.885/5.408 s versus bounded4.919/4.470/5.095 s; engine
5.631/4.567/4.738 versus4.440/4.083/4.480 s. File reads91→12 in every pair;
read bytes1.353→1.323GB (decimal), all remote0/result-uncached. File-count gains
are much larger than byte savings; no assumption that payload serialization or
publication bottlenecks are solved. Three cached-data pairs do not establish
service p95, sustained rate, cold reads or timing causality. Stage-origin proof
cost is separate and must count inside any integrated publication comparison.

Read-only existing warehouse2439e1f2e37ac563; no materialization, mutations,
maintenance or compute changes. Canonical anchor E23/r139-b1 remains unchanged.
All phases plus refusal controls record8.057GB reads/zero remote writes, not
billing. Statements90 s, refusal controls30 s, bounded connector retries/socket
and durable correlation. Initial audit encountered finalization lag for two
FINISHED refusal-control IDs; [finalization note](out/native/ashlar_origin_validation_r151/audit-finalization-note.json)
records same-ID history refresh without SQL replay. Final histories all final.

The [executed bounded query](out/native/ashlar_origin_validation_r151/bounded-query.sql)
is a scoped owned candidate, not a generic consumer SQL API. Next incorporate
origin-proof validation into an owned query builder, rejecting unverified,
non-singleton or null origins, then count origin proof and exact validation in
one isolated publisher comparison. Preserve multi-origin fallback and real source
semantics; never assume all future batches share one epoch. This is the first
useful raw-validation pruning change in this sequence, but no agreed full gate is
admitted. UC Delta remains selected and the full goal remains active.


### r152 owned origin guard and integrated publication

The owned [raw validation helper](raw_validation_queries.py) consumes the exact
successful immutable stage0 origin query and requires one nonempty feed/epoch,
exact expected count and a native query ID. [Guard evidence](out/raw-validation-guard-check.json)
matches the executed r151 SQL after equivalent literal spelling and rejects11
adversarial query/version/result/count/null/multi-origin/different-stage cases.
The helper is inside a trusted controller; its records/dataclass are not an
untrusted authentication boundary or generic producer fencing. Unsupported
origin grouping is refused by this scoped helper; retain the prior unbounded-by-
origin exact validation as an explicit multi-origin fallback in future integration.

[Integrated native audit](out/native/ashlar_queue_r152/audited-summary.json) runs
one100k synthetic property publication on isolated E23/R17/J19 shallow clones,
existing warehouse2439e1f2e37ac563/runtime19.8.x-aarch64-photon-scala2.13.
Modeled10k/s release window is10 s; no actual source/rate or simultaneous readers.
Origin proof costs0.337 s inside the clock. Processing50.378 s, complete-input
queue wait0.00027 s, modeled record-age p95 59.878 s and oldest age60.378 s.
The first r149 batch took50.139 s: this single sequential comparison does not
show an end-to-end gain or establish a service/freshness p95 distribution.
A one-batch modeled near-pass cannot admit sustained10k/s or100k/s bursts.

Raw exact validation caller13.652 s/engine13.111,5files/1.474GB; journal symmetric
validation17.749/16.999 s,10files/2.885GB, keeps the parallel validation critical
path at17.753 s. MERGE4.182/3.775 s,40files/359.516MB. Reduced raw file scans do
not solve the journal validation cost. Exact20 affected fields/token patch,
raw origin/digest/UTF8/UTC instant and exact property journal pass, alongside20M
unique IDs and owned structural-reuse proof. Independent full20M structural
oracle and19.9M untouched physical custody pass after the clock under stable
schema/immutable-file assumptions. No fresh wide-payload equality claim for all
untouched rows, real producer completeness/fencing/ACK or billion admission.

Preparation/publication/final audit totals44.998GB recorded reads/2.285GB remote
writes, not billing or distinct storage footprint. Controller admits SQL for at
most600 s; per-statement180 s/socket/retry bounds remain explicit. Final history
IDs are all final. [Cleanup](out/native/ashlar_queue_r152/cleanup/summary.json)
UUID/version-checks and removes all five owned tables; canonical E23/R17/J19 and
r139-b1 remain unchanged. No new compute or VACUUM.

Keep guarded origin bounds as a scoped pruning option, with no claimed throughput
improvement. Next compare a single-pass exact journal comparison against the
symmetric EXCEPT query, preserving all columns, UTF8 equality, missing/null flags,
row multiplicity and membership refusals. Measure its own duplicate-key checks;
no digest substitution or moved validation clock. UC Delta remains selected and
all full-goal sustained/read/concurrent/cold/billion gates remain open.
