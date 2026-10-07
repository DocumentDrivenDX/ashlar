# Publication and maintenance policy candidate

Draft physical policy under CONTRACT-001, CONTRACT-002, CONTRACT-003 and ADR-001.
No producer wire format, UMF binding or new support claim is selected here.

## Current40M-edge maintenance candidate

Retain the proposed unpartitioned lookup-hash liquid-clustered baseline with full native identity checks and exact carriers;64MiB remains a tuning target, not a file-size guarantee. The [matched second-batch comparison](second-layout-comparison-r340.md) favors LC ingest cost. The [bounded maintenance qualification](lc-prefix-maintenance-disposition-r353.md) proves one prefix rewrite33.8s/2.328GB with all39.98M complete carriers preserved and actual same-SID versions4/5. All-table ordinary attempts remain canceled evidence with costs retained; no cheap per-batch whole maintenance assumption is justified.

Admit separately scheduled maintenance only against a validated base vector, actual coverage and stated resource/interference budget; source backlog retains priority. Select range width by candidate file/byte coverage. This pilot's49files/2.338GB and1/16prefix are not a billion-scale threshold or cadence. Require actual commit custody and preservation before maintained versions are exposed; no existing manifest is repointed. The maintenance-manifest/concurrent-writer protocol remains unqualified beyond controlled fixture receipts. Keep raw/journal history and active cursor/version retention intact.

Matched uncached warm second-pass reads remain104ms engine/416ms caller after maintenance, versus101ms/435ms before. Lower sampled bytes/files do not establish causal latency improvement. Warm100ms/250ms, sustained10k/s/100k/s burst, controlled cold and1B/5B remain open; the optional target calibration question has not changed the current budget. The completed [RPC diagnostic](connector-rpc-disposition-r358.md) observes one direct-result RPC per cached point and no polling/fetch overhead to remove. The [bounded maintenance publication design](maintenance-publication-design-r359.md) now specifies admission, complete multi-commit custody, immutable successor vectors, recovery and retention. Twelve local altered-receipt controls pass; real fencing/schema revisions/pin registry and native maintenance-manifest execution remain unqualified. UMF and native graph-engine feature admission remain deferred.

## Earlier evidence and limiting resource

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

## Complete intermediate publication evidence

The [r281 audited publisher](out/native/ashlar_mixed_publish_r281/audited-summary.json)
operates on the fully qualified8M-node/40M-edge baseline with90k updates/10k
deletes. Complete unchanged/current/forward/raw/history digests and full global
identity/deletion/endpoint checks pass before the exact descriptor readback.
Apply-only49.209s differs from full ready-input processing922.120s; the latter
misses60s freshness. Source preparation/transfer/staging and clone reference
preflight are separately recorded, never claimed as measured arrival freshness.
Reported372.920GB reads versus354.291MB writes and zero spill expose the full
inherited-row validation cost. This does not justify omitting custody validation.

The source-bound [r284 model](out/publication-capacity-r284.json) repeats this one
sample only. At modeled10k/s,100k entities arrive every10s: apply-only utilization
4.921 and an added39.209s wait per subsequent batch; full audit utilization92.212.
These are queue sensitivities, not measured sustained throughput, parallel writer
admission or service p95. Neither a passing apply-only first-batch age nor a
faster future validator would by itself admit10k/s or100k/s burst recovery.

Next qualify an incremental-custody publication protocol against this full sweep:
exact eligible change set, complete affected-role field/origin digests, mutation
predicate and commit receipts, immutable inherited snapshots, and explicit refusal
of intervening/unknown writer changes. Keep full sweeps as independent qualification
and audit evidence. The later r292/r332 incremental-custody runs now have scoped controlled-fixture evidence; production source/concurrent-writer qualification remains open;
no existing descriptor or correctness claim changes. Native0.3 forward rows include
entity_version; this mixed run updates that field even for property-only changes,
so structural endpoint stability alone does not prove unchanged forward carriers.

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


### r153 exact journal multiset aggregation: rejected for latency

[Native audit](out/native/ashlar_journal_validation_r153/audited-summary.json)
compares three alternating100k journal validation pairs over immutable stage
r139 v0 and property journal v19/r139-b1. Candidate unions normalized expected
and actual rows with signed multiplicities, groups by all21 fields and refuses
nonzero balances. Every text field uses hex UTF8 bytes; timestamps, booleans,
numerics and nulls remain native. No digest or approximate equality substitutes
for full values. Zero imbalance means exact multiset equality within this bounded
200k-row profile; nonzero result counts differ from EXCEPT counts but both refuse.

Both paths pass exact100k equality. Eleven tiny typed native controls are refused
by both: duplicate/missing row, old-value null, missing flag, Unicode lexical
normalization difference, new value, native cursor, source epoch, microsecond
instant, event ordinal and property ID. Expected stage membership/uniqueness and
source authority remain separate publisher responsibilities; equal duplicated
expected/actual bags alone would not prove those invariants. This helper is owned
synthetic query code, not a generic SQL/authentication or source-contract API.

Control caller7.156/6.346/6.653 s versus candidate9.283/9.416/9.071 s; engine
6.076/5.560/5.894 versus8.504/8.727/8.403 s. Files24→12; reads2.602GB→
1.301–1.305GB; spill0 throughout. Remote bytes control0/0/40,076; candidate
18.409MB/0/3.927MB, so not strict all-warm paired admission. All result caches
disabled, final native IDs and balanced order. Do not infer internal CPU/shuffle
causality without a physical profile or extrapolate these pairs to service p95.
The lower scan volume does not produce a latency benefit here. Reject candidate
for publisher promotion; preserve the existing symmetric exact validator.

Existing warehouse2439e1f2e37ac563, same published synthetic fixture; no tables,
new compute, maintenance or publication writes. All comparison/refusal phases
record11.717GB reads/zero remote writes, not billing. Statements90 s and durable
submission/socket/retry limits remain explicit. No cleanup needed. A local syntax
error was corrected before any SQL submission; the native run was not replayed.

Next screen a full outer comparison keyed by unique journal delivery/property/
event identity, counting uniqueness proof and all21 exact fields. Multiplicity
refusal must survive; otherwise retain symmetric EXCEPT. Bound it to the same
read-only100k corpus before another integrated publication. The r15250 s service
and r149 queue-growth results remain unmet sustained capacity evidence; optimizing
scan counts alone does not meet the full goal. UC Delta remains selected.


### r154 keyed exact journal comparison: rejected

[Native audit](out/native/ashlar_journal_validation_r154/audited-summary.json)
compares three alternating100k journal pairs over stage r139 v0 and property
journal v19/r139-b1. Candidate full-outer joins feed/epoch/delivery/property/event
keys, compares all21 columns with UTF8-normalized texts and includes per-side
window counts for duplicate refusal. This is an owned unique-event synthetic
profile; it does not select native Truss event identity. The source-profile key
and authority requirements of CONTRACT-003 remain unqualified for real feeds.

All exact equality checks and11 typed native corruption refusals pass on both
paths. Candidate also refuses equal duplicated expected/actual bags: intentionally
stricter than multiset equality because this profile requires unique events.
Missing-row detection uses the non-null window count, not nullable value columns.
Nulls, missing flags, Unicode lexical differences, native cursor/origin, exact
microseconds, event ordinal/property identity and changed values stay distinguishable.

Control caller6.618/6.673/6.589 s versus candidate11.389/11.497/11.999 s; engine
5.941/5.924/5.870 versus10.635/10.658/11.318 s. Files24→12, reads2.602–2.606GB→
1.301–1.305GB, spill0. Remote bytes control0/0/18.443MB versus candidate1.969MB/
0/3.961MB. Balanced order, final native IDs, result cache disabled; this is not
strict all-warm service-p95 or causal internal-profile evidence. The included
uniqueness checks halve scans but do not reduce latency. Reject publisher
promotion and retain symmetric exact EXCEPT validation.

Read-only existing warehouse2439e1f2e37ac563; no resources/tables or publication
changed. All phases11.719GB recorded reads/zero remote writes, not billing.
Statement90 s and bounded submission/socket/retry controls. No cleanup needed.
Preserve both r153/r154 failed alternatives rather than claiming fewer scans are
faster or changing the exact preservation obligation.

Next test three independent physical write lanes: current apply, raw append and
journal append, followed by the same complete exact checks and manifest barrier.
Only one logical publisher may own the fixture; consumers remain pinned to the
old manifest until all new vector members pass. A failed lane may leave unpublished
physical versions and requires inspection/recovery, never progress advancement or
blind mutation replay. Do not advertise native multi-table transactions or real
fencing from this experiment. Count all checks, history collection and manifest
confirmation in publication time. Keep the isolated100k/current20M bound and
existing compute; this addresses elapsed service without weakening validation.
The full goal and all sustained/read/concurrent/cold/billion gates remain open.


### r155 independent write overlap: bounded candidate, modest improvement

[Native audit](out/native/ashlar_queue_r155/audited-summary.json) runs one100k
property publication over isolated E23/R17/J19 shallow clones on existing
warehouse2439e1f2e37ac563/runtime19.8.x-aarch64-photon-scala2.13. Current MERGE,
raw append and journal append run through three separate client lanes; logical
publisher remains serialized. Exact pre-apply intent precedes all writes, and
all existing raw/history/current/identity/structural checks precede the manifest.
No native multi-table transaction, real feed fencing or recovery admission.

Processing48.761 s, modeled10 s input window, oldest record58.763 s and uniform
record-age p95 58.263 s. The prior r152 serial-apply publication took50.378 s;
this is one sequential comparison with different cache/load conditions and an
extra observer, not a causal win or measured freshness/service p95 distribution.
At100k batches, holding this sample constant still implies only about2,051
changed entities/s; sustained10k/s and100k/s burst requirements remain unproved.
Do not substitute first-batch latency for queue/rate admission.

Overlapped write barrier15.161 s: raw15.156, journal11.690 and MERGE7.826 s caller.
Prior serial-apply MERGE4.182 s: overlap also slows individual operations. Exact
raw/journal validation barrier17.541 s remains dominant; the saved clock time is
modest. Full20 affected fields/UTF8 exact values/raw digest and origin/native
cursor/property journal/20M IDs and owned structural proof pass. Independent
full20M structural parity and19.9M unchanged physical custody pass after the
publication clock under stable schema/immutable-file assumptions.

A [read-only observer](out/native/ashlar_queue_r155/observer/summary.json) completes
before manifest submission and sees manifest count0,100k applied current rows and
100k rows retained at old pinned version0. This proves one unpublished physical
window and old snapshot retention, not full consumer concurrency or a failed-
lane recovery protocol. The final audit includes an explicit scope correction to
its inherited no-concurrent-reader wording; actual observer IDs/costs are retained.
All saved native IDs are final. Total preparation/publication/observer/final audit
48.941GB recorded reads/2.286GB remote writes, not distinct storage or billing.
Controller600 s admission deadline/per-statement180 s and connector submission
bounds remain explicit; no new compute/resize or whole-graph payload digest.

[Cleanup](out/native/ashlar_queue_r155/cleanup/summary.json) UUID/version-checks and
drops all five owned tables; canonical E23/R17/J19/r139-b1 remains unchanged.
Keep write overlap as an experimental option, not an admitted publisher default.
Next prove failed-lane manifest refusal on a much smaller isolated fixture, then
consider overlap of independent validation lanes without weakening any checks.
Do not repeat the20M custody scan for a tiny protocol control. Resource/rate,
singleton, cold/concurrent reads, external-runtime and1B/5B obligations stay open;
UC Delta architecture remains selected.


### r156 failed raw-lane publication refusal

[Final native audit](out/native/ashlar_failure_r156/audited-summary.json) uses a
1,000-edge property slice with isolated current/raw/journal/manifest/stage tables,
existing warehouse2439e1f2e37ac563. Three separate clients complete native MERGE,
raw append and journal append. Raw records deliberately append one whitespace byte
to each full wire payload and store the matching SHA256 of that corrupt value.
JSON parsing and digest consistency alone therefore cannot establish exact source
bytes. Exact origin-bounded raw validation reports all1,000 mismatches.

Current20-field output and property journal equality pass. Controller validation
refuses the new manifest before any submission; saved records contain no
publish-new statement. The complete old manifest remains byte-for-byte equal,
and all1,000 old pinned carriers match immutable stage r139 v0 with UTF8-normalized
strings. New physical rows stay unmanifested. CONTRACT-003 now links this evidence
and explicitly distinguishes validation refusal from crash/ambiguous transport,
durable receipt, recovery and producer-fence qualification.

This is a protocol control, not a performance screen, full graph or endpoint-
closure/billion admission. Existing preserved typed endpoints/current carriers
are used; no20M structural/custody scan is repeated. Source subset extraction and
old snapshot verification still read original large files: recorded reads18.007GB,
remote writes26.948MB across preparation/control/audit, not billing or distinct
storage footprint. Initial local syntax correction preceded all SQL; native
mutations were not replayed. Statements60 s, final old-snapshot check30 s,
connector socket/retry and durable correlation retained; all native IDs final.

Unpublished versions were recorded/inspected, then [cleanup](out/native/ashlar_failure_r156/cleanup/summary.json)
UUID/version-checks and drops all five owned tables. This is cleanup, not durable
recovery. Canonical E23/R17/J19/r139-b1 stays unchanged; no new compute or VACUUM.
Next test overlap of exact current/output validation with raw/journal validation
on the existing100k fixture, keeping all publication checks and costs inside the
barrier. Full throughput/singleton/cold/concurrent/external-runtime/1B/5B targets
remain open, and UC Delta architecture remains selected.


### r157 three validation lanes: scheduling tuning plateau

[Native final audit](out/native/ashlar_queue_r157/audited-summary.json) runs one
100k synthetic property publication on isolated E23/R17/J19 clones and existing
warehouse2439e1f2e37ac563/runtime19.8.x-aarch64-photon-scala2.13. Three write lanes
are followed by three exact validation lanes: current global identities/output,
raw full-wire checks and symmetric property journal checks. All native IDs, owned
query/lineage proofs and manifest checks are finalized inside publication timing;
no checks are omitted or deferred from the existing publisher barrier.

Processing47.981 s; uniform10 s modeled input window gives oldest57.983 s and
record-age p95 57.483 s. Previous r15548.761 s, r15250.378 s. These are separate
single batches, not a controlled sustained/service-p95 comparison or proof of
10k/s capacity. They show a scheduling plateau rather than the required roughly
fivefold service improvement. Raw validation20.840 s, journal19.396 s, current
identities4.247 s and exact output7.884 s; three-lane barrier20.845 s. Additional
concurrency hides work but also slows individual checks. No causal CPU/shuffle
claim without further profiling. Do not promote this variation as a default.

All20 affected fields/UTF8 values, exact raw origin/digest/UTC instant/native
cursor and property journal,20M global IDs and owned structural reuse pass.
Independent20M structural parity and19.9M unchanged physical custody pass after
timing, under stable schema/immutable Delta-file assumptions, not fresh wide
payload comparison for untouched rows or real feed authority/fencing/ACK. No
reader workload/controlled-cold/external runtime/billion admission. Preparation,
publication/final audit totals48.733GB recorded reads/2.286GB remote writes,
not billing. Controller600 s SQL-admission deadline/per-statement180 s and bounded
connector submission remain explicit. [Cleanup](out/native/ashlar_queue_r157/cleanup/summary.json)
UUID/version-checks and drops all five owned tables; canonical E23/R17/J19/r139-b1
unchanged; no new compute/resize/VACUUM.

Stop this sequence of small scheduling/validator substitutions. Next return to
physical design: screen a fixed64-bucket hash partition plus within-bucket Z-order
against the current hash-clustered candidate. Historical four-bucket partition
publication evidence motivates the comparison but uses a different10M fixture;
it cannot establish a current20M winner or a universal billion-scale bucket count.
First use100k complete existing carriers to verify bucket derivation, native DDL,
exact values and directory pruning with actual file sizes; this is a prerequisite
pilot, not scale admission. Keep logical Truss identity/typed endpoints unchanged
and treat bucket as derived physical metadata only. Count copy/Z-order/write and
point-read costs separately, preserve source proofs and reject bucket/hash drift.
Advance to the existing20M corpus only after the pilot and a justified byte/time
bound; avoid another unbounded whole-graph wire-digest query. UC Delta remains
selected and the full agreed performance/correctness goal stays active.


### r158 native64-bucket physical layout pilot

[Native audit](out/native/ashlar_bucket_screen_r158/audited-summary.json) copies
100k complete immutable stage r139 v0 carriers into hash-LC and64-bucket partition
alternatives, same Zstd/64MiB configured target/identity+eligibility statistics.
Partition metadata is first60 SHA256 bits modulo64 (safe signed64 conversion),
not logical identity. Every point lookup still filters full hash and native
source/relationship/id tuple. The [experimental DDL](sql/bucket64-layout-candidate.sql)
records the shape; selected canonical DDL remains hash-clustered.

Full20-field UTF8-normalized carrier equality,100k unique IDs, bucket/hash drift,
64 bucket membership and lowercase64-hex shape pass. Native boundary vectors
zero/max60-bit/leading-prefix values match Python modulo. Bucket populations
1,474–1,653. Both OPTIMIZE LC and partition ZORDER commands are no-ops with zero
rewritten bytes; both remain v0. This pilot establishes directory pruning, not
actual Z-order/file-resizing effectiveness. CTAS LC11.146 s versus partition6.814 s;
no claim that isolated copy timings predict producer ingest or maintenance.

Thirty alternating exact full-carrier lookups per layout: LC p95engine120ms/
caller394.16ms, partition93ms/381.61ms. Both read1file; p95bytes92.655MB versus
5.606MB; remote0/result-uncached. Data were warmed by full equality scans, so these
are not cold-data admission. Partition passes the engine screen only; both caller
gates fail. Actual LC4files335.574MB versus partition64files336.286MB. Configured
64MiB target did not constrain actual copy file sizes or make OPTIMIZE rewrite.

Both reader3/writer7 but feature bundles differ: LC clustering/domainMetadata/
rowTracking plus common DV/v2Checkpoint; partition lacks rowTracking in this
pilot. This is an explicit whole-layout comparison, not causal isolation of
partitioning alone or external-reader compatibility. Match feature settings in
future ingestion controls. Extra physical bucket also requires a bucket-aware
apply source/assignment policy; historical20-column UPDATE SET * helpers must
not silently target this21-column table without adaptation and drift checks.

All pilot/control phases6.733GB reads/0.672GB remote writes, not billing. Existing
warehouse2439e1f2e37ac563, statements90 s/bucket controls15 s, bounded connector
correlation/retries. [Cleanup](out/native/ashlar_bucket_screen_r158/cleanup/summary.json)
UUID/version-checks and drops both owned tables; canonical E23/R17/J19/r139-b1
unchanged; no new compute, resize or VACUUM. No full20M/billion, incremental MERGE,
publisher freshness, reader concurrency or graph-engine admission.

Next validate a bucket-aware update template and matching native feature settings
on the100k fixture before full20M copy. Preserve all carrier/origin/projection
proofs; then bound larger copy/maintenance/parity bytes and statement/controller
costs explicitly.64 is a pilot bucket count, not a chosen1B/5B partition strategy.
The smaller point-read bytes justify continuing this physical comparison, while
hash-LC remains the canonical candidate and all full-goal obligations stay open.


### r159 matched rowTracking and bucket-aware updates

[Native audit](out/native/ashlar_bucket_screen_r159/audited-summary.json) creates
two100k full-carrier layouts with rowTracking explicitly enabled, common DV/
Zstd/64MiB target/identity+eligibility statistics. LC retains its inherent
clustering/domainMetadata features. [Owned bucket apply](bucket_apply_queries.py)
derives source bucket from exact hash, matches bucket plus full native tuple and
prior entity-version/batch eligibility, then assigns all20 logical fields
explicitly. Bucket stays derived physical metadata. [Helper guard](out/bucket-apply-guard-check.json)
matches executed SQL byte-for-byte and refuses unsupported matched placement.
This remains trusted synthetic update-only code, not generic inserts or fencing.

Initial100k exact20-field UTF8 copies/unique IDs pass. Both100k property MERGEs
pass full intended/output values and post-update global membership; bucket drift
and lowercase hash shape checks pass. Both native details confirm rowTracking
true; zero/max60-bit and leading-prefix native bucket vectors match Python.
One stage0 generates explicit64-hex-character property105 replacement values,
much shorter than previous4–8KiB opaque values. Exact semantic replacement is
intentional, but this is compact-update correctness and physical screening,
not wide producer ingest or an equal-payload comparison with r158/r157.

LC MERGE caller3.019 s versus partition5.605 s. Thirty alternating exact point
reads: LC p95engine103ms/caller365.51ms/9.656MB; partition76ms/367.39ms/0.166MB,
1file each/remote0/result-uncached. Both caller gates fail; compact payloads and
prior full validation warm data, so no cold/full-scale or sustained admission.
Different layouts remain different physical bundles; no causal directory-only
or billing inference. No optimization/Z-order is run in this update pass.

The original harness completed all60 reads then stopped because canonical head
was25 instead of23. [Read-only recovery](bucket_updates_recover_r159.py) records
contiguous OPTIMIZE24/25 lineage and verifies old r139-b1 still pinsE23/R17/J19,
with20M pinned edges readable. No MERGE/copy or read workload was replayed. This
experiment performs no canonical mutation; external maintenance changed its head,
and its resource interference is an additional timing confound. Distinguish
current physical head25 from the immutable published snapshot23. Do not silently
repoint the manifest or assert the head stayed unchanged.

Pilot/preparation/control/lineage audit records5.592GB reads/1.030GB remote writes,
not billing or distinct storage footprint. Existing warehouse2439e1f2e37ac563;
statements90 s/lineage30 s/bucket controls15 s and bounded connector submissions.
All saved native IDs final. [Cleanup](out/native/ashlar_bucket_screen_r159/cleanup/summary.json)
UUID/version-checks and removes the two owned tables and stage; no VACUUM/new
compute/resize. PublishedE23/R17/J19/r139-b1 remains intact.

The update-path prerequisite is now evidenced, but retain hash-LC as canonical
candidate. Next compare equal-size wide property replacements and retain the
same rowTracking/eligibility/bucket checks before a full20M copy. Observe actual
canonical head/lineage and source snapshot rather than hard-coding unchanged
physical head; keep explicit published versions.64 buckets remain experimental,
not the chosen1B/5B layout. All full-goal obligations remain open.


### r160 same-length wide bucket updates

[Native audit](out/native/ashlar_bucket_screen_r160/audited-summary.json) repeats
100k full-carrier LC/64-bucket updates with high-entropy property105 replacements
matching every old length; native length-drift count0. Both rowTracking-enabled
layouts receive the same immutable input. Initial copies, full intended/output
20 fields/UTF8 and global unique membership pass; bucket/hash/shape/boundary
checks pass. This removes the compact-value caveat from r159 while retaining an
isolated hot-set scope, not a20M corpus or producer publication/rate claim.

MERGE caller LC3.585 s/partition6.609 s. Thirty alternating exact lookup p95:
LCengine105ms/caller355.15ms,6files/326.928MB; partition81ms/350.51ms,
1file/5.471MB. Remote0/result-uncached, data warmed by exact output validation.
Both caller gates fail. Partition passes this engine screen only. One sequential
MERGE pair cannot establish causal/general write performance or service p95.
Different inherent LC/partition features remain explicit despite matching
rowTracking/DV/compression/statistics. No OPTIMIZE/Z-order in this pass.

Post-update LC6files326.711MB versus partition64files327.464MB. Copy callers
10.857/6.040 s are setup costs. All pilot/control phases18.475GB reads/1.990GB
remote writes, not billing or distinct storage footprint. Existing warehouse
2439e1f2e37ac563; statements90 s/controls15 s, bounded durable submissions.
All saved native query metrics final. Head is observed independently of the
manifest; r139-b1 still pinsE23, source stage139 v0 stays immutable. No canonical
mutation, compute change or VACUUM. [Cleanup](out/native/ashlar_bucket_screen_r160/cleanup/summary.json)
UUID/version-checks and drops both layouts plus stage.

The [20M comparison plan](bucket-scale-plan.md) now states row/write/read/time
bounds and explicit initial wide-copy proof limits. Next execute that physical
comparison on the existing20M fixture within those bounds. It measures file
counts, lookup/ingest tradeoffs and maintenance, not billion admission; full20M
wide-copy parity remains separate if only scoped checks pass. No chosen64-bucket
production layout or successful full performance gate is inferred. Hash-LC stays
canonical candidate; UC Delta stays selected and the full goal remains active.


### r181–r182: post-update singleton comparison

All60 complete-carrier lookups pass against immutable stage0,30 identical SHA-ranked updated keys per layout, alternating query order and disabling result caching. LC clone1 p95 is179ms engine/433.354ms caller,8files/403.803MB; bucket9 is228ms/487.591ms,2files/160.850MB. Remote reads occur in1/30 LC and9/30 bucket queries; this is neither a controlled cold test nor a fully warm cohort. Both observed distributions miss the provisional100ms engine/250ms caller targets. Different physical histories and sequential samples preclude a causal partition claim. Fewer bucket files did not imply lower observed latency.

All native query IDs are successful and final. The worker's post-query cost assertion stopped after15.753GB reads against15GB; zero writes/spill. The audit reconstructs completed comparisons without replay and preserves the cost failure. No new canonical publication, sustained-rate, production source authority or billion-scale admission. Evidence: `out/native/ashlar_bucket_post_update_reads_r181/audited-summary.json`.

#### Design consequence: budget maintenance separately from publication

Keep canonical current tables clustered by lookup_hash with full native identity predicates. The experimental64-bucket derivative is not a default serving table: r176 wrote5.254GB versus LC326.515MB for the same100k replacements (about16.1 times), while this post-update sample did not improve latency. Budget derivative refresh, copied rows and ZORDER as separate work; a delta commit alone does not establish freshness or sustained throughput.

Proposed maintenance controller inputs are observed candidate-file count/bytes per singleton, DV/copy metrics, queue age and last successful maintenance version. Admit one serialized maintenance task outside publication only when measured read degradation and an explicit byte/time allowance justify it. Do not mandate full OPTIMIZE after each batch. Record physical maintenance lineage independently of logical publication; advertise a new reader snapshot only after the existing publication validation/barrier rules succeed. Thresholds and intervals remain unknown until repeatable workload evidence, rather than fixed production constants inferred from this fixture.

Future experiment admissions must estimate oracle reads plus per-key candidate bytes, check cumulative metrics between small query batches, and stop before another batch exceeds the remaining allowance. The old end-of-phase assertion was not an in-flight spending cap. No wide preservation scan should be repeated merely for a read test; retain the proved version pins. Next measure bounded concurrent singleton reads and caller overhead, then revisit pipeline queueing with the maintenance budget explicit.


### r183–r184: two-client point reads and caller controls

Two independent persistent clients execute ten synchronized pairs of exact20-field LC1 reads using20 distinct updated identities and ten SELECT1 control pairs. Owned UUID/version is revalidated; expected values come from the already recorded independent immutable stage, avoiding another oracle scan. Result caching is disabled. Native query-start/execution-end windows overlap for all10 point and all10 control pairs; this does not prove simultaneous task execution or saturation. Polling outside timed calls adds idle gaps, so this is a burst-pair experiment, not steady load.

Point p95: engine197ms, caller443.288ms, compilation162ms,8files/403.803MB; remote reads0/20. SELECT1 p95: engine25ms, compilation26ms, caller149.983ms. Caller-minus-native-total residual p95 is124.784ms for points and97.532ms for controls; these are per-query differences, not subtraction of unrelated percentiles or a pure network measurement. Both point gates remain failed. Capacity-wait duration is absent from native histories; original summary incorrectly defaulted it to zero, and the separate audit explicitly records unknown while preserving the original evidence.

All native IDs succeeded and finalized. Total7.700GB reads/0 writes/0 spill fits10GB; the controller reserves1GB before each next pair and checks cumulative final telemetry between pairs. The reserve is an estimate, not an in-flight spending cap. No canonical publication, maintenance, resize or new compute occurred. Evidence: `out/native/ashlar_lc_concurrent_r183/audited-summary.json`.

Design implication: retain canonical hash LC, but do not advertise the provisional warm SLO as achieved. Candidate-file amplification persists for updated rows even with no remote reads. Caller/server residual and compilation consume significant budget; an in-region application measurement and reduced candidate bytes are separate qualification work. Native task saturation and capacity queueing remain unmeasured. Next investigate whether publication hot-file shape can reduce singleton amplification without adding mandatory full-table maintenance to every batch; measure that change together with raw/journal/current publication timing, not MERGE throughput alone.


### r185–r186: diagnose update hash-range overlap before rewriting

Two full20M narrow-column live-file groups pass at owned LC0/LC1 with2.522GB reads/0 writes/0 spill, within10GB. Live file count changes532 to522. The six new files contain exactly100k updated rows; five span99.9783–99.9959% of the256-bit hash domain and one spans49.0083%. Each of the30 prior sampled keys intersects18 pre-update live-file extrema and5–6 new hot-file extrema. Hot compressed candidate footprint is312.789–326.515MB; baseline footprint370.872–418.363MB. These are reconstructed full-precision live-row min/max extents and compressed file sizes, not stored Delta statistics, actual candidate counts or bytes scanned. DV filtering and stored-statistics truncation can change those relationships. The separate measured reader remains8files p95; do not substitute the reconstructed23–24 range overlaps for that measurement.

This identifies a specific testable write-shape hypothesis: random parallel hot output creates overlapping hash ranges even though the table has CLUSTER BY(lookup_hash). It does not establish that all read latency is attributable to that overlap. Evidence: `out/native/ashlar_lc_file_ranges_r185/summary.json`.

A no-mutation native EXPLAIN probe accepts `REPARTITION_BY_RANGE(6,lookup_hash)` on the20-column input and shows a Photon `rangepartitioning(...,6)` exchange. The MERGE EXPLAIN displays a command wrapper without the final runtime writer exchanges, so it does not prove range-aligned Delta output. Native version remains1; all probe histories finalize with0 read/write/spill. Evidence: `out/native/ashlar_lc_range_plan_r186/`. Spark documents the partitioning hint in its [primary performance documentation](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html); actual Databricks writer behavior must be observed rather than inferred from that documentation.

#### Next controlled range-input update

Use one new owned shallow clone of exact canonical E23, matching LC/DV/rowTracking/compression/statistics and the already proved immutable stage0. Preserve the existing baseline/update result as control. Replace only the source SELECT with the6-range hint; full native tuple, lookup hash, old entity_version15 and r139-b1 eligibility stay unchanged. No insert/delete clauses, schema change, new source authority or source publication are introduced. Do not mutate LC1 or canonical tables and do not replay an uncertain MERGE.

Before execution revalidate canonical/stage IDs and versions, and reference the existing exact intended-stage evidence by immutable pins. Bound the new run to one100k MERGE,90s statement/600s controller admission,25GB reported reads and2GB reported remote writes, with at least8GB reserved for post-update narrow custody and changed-carrier proof. These are admission estimates, not in-flight spending caps. Close results before metric collection; check cumulative final metrics between phases. Stop before reader admission if the write shape or preservation check fails. Record native history/rows-copied/DVs/output bytes, and compare all20 changed fields using binary text/native typed values. Prove19.9M untouched-row physical custody only if the history shows zero copied rows and immutable unchanged files; otherwise a wider proof needs a separate justified budget.

Measure fresh live-file extents on the100k output and a small exact singleton cohort only after correctness. A hint that disappears or leaves nearly global hot ranges is a failed optimization hypothesis, not permission to claim improved maintenance. Even successful range shaping must be integrated with raw/journal/current validation and immutable publication to qualify freshness; a standalone MERGE cannot admit10k/s,100k/s burst, cold/service or1B/5B targets. Canonical hash LC remains the proposal, UC Delta remains selected, and UMF binding remains deferred.


### r187–r188: range-aligned update produces narrower hot files

The fresh owned shallow LC clone of immutable canonical E23 applies the same independently proved stage0 and eligibility/native tuple with only `REPARTITION_BY_RANGE(6,lookup_hash)` added to the source SELECT. Native MERGE version1 updates100k, copies0, inserts0/deletes0. All20 changed fields match;19.9M unchanged full identities/file paths/row positions pass symmetric custody, and20M globally unique IDs remain. Clone0/MERGE1 is the complete closed lineage. Audit verifies hash LC, DV/rowTracking and reader3/writer7. Canonical manifest remainsr139-b1/E23 and unchanged other vector pins; this owned derivative is unpublished.

Six new hot files cover15.9644–18.0914% hash-domain spans; each of the20 tested hashes intersects exactly one reconstructed new live-file range. This contrasts with five nearly global hot files in the prior update. Full-precision live extrema remain distinct from stored Delta statistics. Twenty exact uncached-result singleton reads have no remote reads: p95 engine104ms/caller365.700ms,3files/139.231MB. Prior same20-key control is179ms/433.354ms,8files/403.803MB. This operational improvement is measured, but the runs are sequential, not randomized causal evidence; validation/polling can warm data and change idle timing. Both provisional warm gates still fail, and the caller gap remains material.

MERGE caller6.903s, reported remote write326.548MB,0 copied rows; prior LC control4.110s/326.515MB. The new run's native metadata time5.573s is retained rather than treating6.903s as a pure shuffle penalty. Whole clone/update/exact/custody/range/read run uses7.770GB reported reads/326.548MB writes/0 spill, within25GB/2GB admission, with telemetry checks/reserves between phases. All native IDs succeeded/finalized, no mutation replay or full-table OPTIMIZE. Existing intended-stage proof is inherited by immutable pins rather than rereading all20M payloads. Evidence: `out/native/ashlar_lc_range_update_r187/audited-summary.json`.

Design consequence: range-aligned hot output is a viable physical tuning candidate alongside canonical hash LC, avoiding a required full-table rewrite for this measured update. Six ranges is an experiment setting, not a production constant or1B/5B partition count. Spark input hints can still be overridden at other scales/writer paths; qualify actual output extents rather than presence of a hint. Preserve all source/native eligibility and20-field semantics. Before adopting the writer setting, repeat with new bounded batches and integrate raw/journal/current validation and the existing publication barrier to measure its freshness effect. Standalone MERGE/read evidence admits neither sustained10k/s nor100k/s recovery, cold/production service p95, real source authority/fencing or billion scale. UMF binding remains deferred.


### r189–r190: integrated writer prepared; validation submission unresolved

`PropertyApply` now optionally generates a bounded owned range-input hint, with defaultNone preserving historical SQL. Integer1–64 is the synthetic screen bound, not a production range count. Native tuple/hash/eligibility and intended/output checks remain unchanged; proof composition still matches the exact generated apply statement. Seven focused tests pass: historical native SQL equivalence, proof refusal for missing/mutated evidence, unchanged semantics after removing only the hint, and invalid range arguments. This changes the experiment helper, not a production producer or UMF binding.

The integrated publisher adapts the isolated r157 three write/validation lanes using a fresh same-length high-entropy property105 input. Its65GB reported-read/3GB reported-write/600s admission/180s statement bounds include phase telemetry and preserve no-replay rules. Prior integrated r157 used48.733GB reads/2.286GB writes; the larger read allowance reserves audit custody work. Bounds are admission checks, not in-flight monetary caps. Input preparation stays outside the publication clock; telemetry inside the publisher would be included in measured freshness. No new compute/resize/canonical mutation is introduced.

Preparation creates three shallow clone0 tables and an empty owned manifest0, verifies100k unique immutable stage0 members, and passes full20M structural baseline. The SQL driver then reportsUNKNOWN during intended-value query submission after10.039s, with no returned query ID, before any publisher raw/journal/current write. Two bounded unique-tag history searches find no matching native statement. Absence is not proof of server nonexecution; validation result and any unobserved work remain unknown. No failed validation or successful integrated publication is inferred. The original controller summary remains preparing and the explicit transport audit records the actual stopped state.

A fresh read-only audit verifies all four owned table heads still0 and the owned manifest empty; canonical r139-b1 still pinsE23. Known native preparation/audit totals2.411GB reads/663.725MB writes/0 spill, excluding unavailable lost-submission work rather than pricing it as zero. Prepared stage and clones remain retained. No setup, validation or write is replayed. Evidence: `out/native/ashlar_queue_r189/transport-audited-summary.json`, originalUNKNOWN statement/inflight tag and correlation searches beside it.

Next resume from these verified owned0/stage0 identities, not by restarting the preparation script. Scope intended-baseline reads to the exact predecessor entity_version15/r139-b1 while keeping the full outer comparison against every source member, so missing/ineligible targets still fail. Qualify that altered query with counterexample controls and native evidence before feeding it to the composed property proof; do not silently replace its expected SQL. Use a bounded submission timeout suitable for validation rather than widening all singleton settings. After that prerequisite, execute the range-input raw/journal/current publication once and preserve original failure timing separately. Integrated freshness, sustained/burst, production source authority, cold/service and1B/5B remain unproved; UC Delta/hash LC remain selected/proposed respectively.


### r191–r194: scoped validation qualified; resumed publication correct, shape not reproducible

The optional predecessor scope filters the intended baseline to exact previous entity_version/batch while retaining every source member in the full outer comparison. DefaultFalse preserves historical SQL. Eight focused tests pass. Native100k baseline returns0; wrong version/predecessor each rejects100k, missing target rejects1, and single retained-text/endpoint/entity-version corruptions each reject1. Separate100k unique source membership remains required. Controls use4.997GB reads/0 writes. This is owned synthetic validation, not source authority or a general schema adapter.

The resumed publisher verifies the original native creation statement ID for every owned clone0/manifest0/stage0, verifies the empty manifest and source membership, and performs no repeated preparation. Only publication clients opt into60s socket bounds; existing singleton clients retain10s. Source history and the r189UNKNOWN/no-ID evidence remain preserved. Raw/current/journal writes execute once with range-input6; exact wire/origin, all20 changed fields, exact property journal,20M identities, inherited structural baseline/closed lineage and full20M post-structural parity pass. The separate audit also passes19.9M unchanged identity/file/row-position custody. An owned manifest pins edge/raw/journal1, N0/tombstone0/adjacency1 and is verified by readback. Canonical r139-b1/E23 remains unchanged.

Measured complete-input-to-manifest processing59.883s includes phase telemetry. Modeled uniform10k/s arrivals over10s yield record-age p9569.386s/oldest69.886s, failing60s. One finite pre-staged batch is neither a service-time p95 nor sustained/burst evidence. Three write-lane wall16.367s and validation wall19.495s are retained; changing scope, timeout, source values and telemetry prevents attributing the result solely to range shaping. Resumed run plus custody15.940GB reported reads/1.622GB writes fits30GB/3GB; original preparation/audit2.411GB reads/663.725MB writes and controls4.997GB reads are separate. Unknown lost-submission work is still unavailable. No duplicate write, maintenance, new compute or resize.

The critical output qualification failed: the integrated MERGE SQL contains the6-range hint, but actual output is16 hot files, each spanning99.9014–99.9959% of the hash domain. A separate full100k live range group uses only7.416MB reads/0 writes. This does not reproduce the standalone six narrow files in r187. Both executions materialize source (2.010s standalone versus4.079s integrated); this observation does not establish why the final writer differs. Full-precision live ranges are not stored Delta statistics. Correctness and manifest publication passed, but the input hint does not guarantee production output layout, and no prior104ms standalone reader result transfers to this publication.

Keep canonical UC Delta/hash LC and range shaping experimental. Next isolate writer/materialization/optimized-write behavior on an owned clone with verified output ranges before integrating another publisher. Do not promote the hint based on EXPLAIN or rerun expensive full-wide oracles: retain exact/current/custody obligations and identify a reproducible writer-output contract. Caller latency, sustained10k/s,100k/s recovery, controlled cold, real producer authority/fencing and1B/5B remain unproved; UMF binding remains deferred.

Evidence: `out/native/ashlar_predecessor_controls_r191/summary.json`, `out/native/ashlar_queue_resume_r192/audited-summary.json`, `out/native/ashlar_publication_shape_r194/summary.json`.


### r195–r196: both source layouts yield narrow output when written alone

The two actual MERGE statements in standalone r187 and integrated r192 share the same6-range hint/native tuple/hash/old eligibility; physical source layout differs (8 prior stage files versus4 fresh stage files), and integrated execution overlaps raw/journal writes. Two new sequential owned E23 shallow clones now apply those same immutable stages under one60s-socket/90s-statement session, with no raw/journal interference. No previous mutation is replayed; each clone has independent CREATE0/MERGE1 lineage.

Fresh four-file stage produces6 disjoint live hash ranges spanning15.4513–17.9289%; prior eight-file stage also produces6 disjoint ranges spanning13.7096–18.6737%. MERGE callers5.980/5.706s. Both100k full20-field intended/output comparisons pass,19.9M unchanged identity/file/row-position custody passes, and20M globally unique identities remain. Both histories show0 copied/inserted/deleted rows and100k updates. Hash LC/DV/rowTracking/reader3/writer7 are audited. An independent full100k cross-stage check proves identical native identity, typed endpoints and hash cohort; payload values/epoch/delivery/timestamps and source layouts still differ.

The four-file source alone is therefore insufficient to explain the earlier16 broad integrated files. Repeated isolated narrow output is associated with execution conditions, but this is not randomized causal evidence that concurrency causes the difference, nor a stable future writer contract. Actual task exchanges remain unobserved. Input hints and file counts still cannot be advertised as guaranteed layout. No new singleton, cold, publication, source authority or billion admission follows.

Two-arm run12.388GB reads/653.106MB writes/0 spill plus cohort audit29.790MB reads/0 writes fits25GB/2GB and2GB audit allowances. All native IDs succeed/finalize; no new compute, resize, OPTIMIZE, canonical mutation or manifest change. Evidence: `out/native/ashlar_lc_writer_isolation_r195/audited-summary.json`.

#### Next scheduling comparison, retaining the full publication barrier

Use fresh owned E23/R17/J19 clones and an empty owned manifest, referencing the proved immutable fresh stage0. Verify creation histories/IDs and exact input membership before writes. Keep scoped intended/native tuple/eligibility and6-range input unchanged. Allow raw and journal writes to overlap each other, wait for both terminal success, then execute current MERGE alone; retain parallel current/raw/journal validation and manifest readback only after all checks pass. This tests a concrete scheduling choice against r192's three overlapping writes, without changing compute or table architecture.

Admit one100k synthetic batch at30GB reported reads/3GB reported writes/600s admission,180s statement and60s publication socket; reserve custody/output-shape audit within the bound. Record actual file ranges before transferring any standalone singleton evidence. Keep preparation outside and all runtime telemetry inside the measured publication clock; disclose scheduling/telemetry differences instead of subtracting overhead or claiming a service p95. Serialization may worsen freshness, so require both output shape and complete publication timing, not just MERGE success. Preserve old manifests, unknown transport-work costs and source authority gaps. Do not add a mandatory full-table rewrite or a new production range count. UC Delta/hash LC remain selected/proposed; full warm/cold/sustained/burst/1B/5B obligations and UMF binding remain open.


### r197–r200: serial current emission restores narrow shape, freshness worsens

Fresh owned E23/R17/J19 shallow clones plus empty manifest use the same pinned stage189 v0/native creation statement, same100k property105 input, scoped intended validation,6-range source hint and shared compute. Raw/journal writes overlap; both return before current MERGE starts. Native query-start/execution-end windows confirm0ms current/raw and current/journal overlap, compared with11,349ms for each in r192. Windows establish ordering, not task/resource causation. No setup or mutation on the earlier published owned clones is replayed.

Exact raw bytes/wire/origin, full20 changed fields, exact journal,20M global identities, full20M inherited/post structural parity, closed current lineage and19.9M unchanged physical custody pass. Owned manifest pins edge/raw/journal1 and originalN0/T0/adjacency1; readback passes. Canonical r139-b1 and its vector remain unchanged. All saved native IDs succeed/finalize; no maintenance, resize or new compute.

Actual current output is6 hot files spanning15.6558–17.7793% of the live hash domain, rather than r192's16 nearly global ranges. The output group covers exactly100k rows and reads4.787MB,0 writes. Same-stage isolated r195 and serial-current integrated r197 both yield narrow files; the three-overlap r192 result remains broad. This association supports an optional isolated-emission candidate, not a deterministic writer contract or universal causal claim. Live extrema are not stored Delta statistics; no standalone104ms point result transfers without a new read measurement.

Write phase raw/journal13.755s plus current6.608s gives20.362s total versus overlapped16.367s; validation17.951s versus19.495s. Complete-input-to-manifest controller62.468s gives modeled uniform10k/s record-age p9571.973s/oldest72.473s, versus69.386s prior. Both fail60s and neither finite pre-staged sample admits sustained/burst throughput or service-time p95. Phase telemetry remains inside the clock. Therefore do not make serialization a required freshness path based solely on better file shape. Keep current/auxiliary scheduling configurable and verify physical output for each qualified profile; semantic validation/manifest barrier remains mandatory.

Run plus custody15.564GB reported reads/1.622GB writes, shape audit4.787MB reads,0 spill, fit30GB/3GB and2GB shape bounds. Native windows and complete timing comparison are local saved-ID evidence, not another workload. Evidence: `out/native/ashlar_queue_serial_r197/audited-summary.json`, `out/native/ashlar_publication_shape_r199/summary.json`, `out/native/ashlar_queue_serial_r197/schedule-comparison.json`.

Next measure a small exact singleton cohort at this verified serial-current publication, using the same immutable expected values and a bounded per-query read reserve. Then consolidate the candidate read/ingest tradeoff and investigate validation/caller costs from the existing profiles before spending on another complete publisher. Do not infer1B/5B feasibility from these20M hot-set runs or replace UC Delta because metrics miss targets. Source authority/fencing, controlled cold, sustained/burst/concurrent service, external native-engine and full-scale gates remain open; UMF binding remains deferred.


### r201–r203: matched publication reads show a read/freshness tradeoff

The initial read controller stops before any oracle/singleton query because its obsolete latest-head==published1 check sees overlap head3. The owned UUID matches; recorded Predictive Optimization Job history shows an OPTIMIZE commit under runtime19.9.x with0 added/removed bytes/files. All three submitted metadata statements succeed/finalize with0 reads/writes/spill. This controller failure is preserved, not treated as a failed native read. The corrected reader verifies the original version1 MERGE statement ID and exact private manifest vector, observes later history separately, and explicitly reads VERSION AS OF1. It also checks the immutable stage oracle UUID. Background maintenance is not a new logical publication and its work/cache/cost is not attributed to this worker; no predictive settings are changed.

Eighty complete20-field exact reads pass: same20 SHA-ranked updated identities, each layout in two passes, alternating order reversed on repeat, persistent SQL client/result cache false. No remote reads occur. Prior validation/background work and telemetry idle gaps make this a scoped warm-cache cohort, not controlled cold, sustained load or production service p95. Query distributions and field equality cover the same keys; manifests remain unchanged and final physical heads are recorded independently.

| Measured p95 | Overlapping writes pass0/pass1 | Serial current pass0/pass1 |
| --- | --- | --- |
| Engine ms |129 /101 |95 /96 |
| Caller ms |416.120 /410.620 |348.750 /340.264 |
| Compilation ms |187 /179 |138 /150 |
| Files read |18 /18 |3 /3 |
| Read MB |414.851 /414.851 |142.939 /142.939 |
| Per-query caller minus native total ms |112.987 /141.620 |107.750 /104.481 |

The serial-current cohort passes the100ms engine screen in both measured passes; caller250ms still fails, as do both overlap engine screens. Do not add unrelated p95s or interpret the caller-minus-native-total residual as network alone. Narrow output demonstrably reduces candidate bytes/files for these published snapshots, but compiler/caller overhead remains material. Same-stage sequential timing on shared compute is operational evidence, not randomized causation or a full graph workload.

Combine with recorded publisher clocks: overlap modeled record-age p9569.386s, serial71.973s; both fail60s. Serialization is therefore an optional read tuning profile, not a successful full ingest/read solution or mandatory freshness path. Canonical UC Delta/hash LC remain selected/proposed and performance misses do not reopen architecture. No engine or read metric is transferred from unmeasured physical maintenance heads to older manifests.

Total21.024GB read/0 writes/0 spill fits25GB with3GB reserves before every four-key/two-layout batch. All native IDs succeed/finalize; no write, resize, new compute or mutation replay. Evidence: `out/native/ashlar_publication_reads_r202/summary.json`, with the separate stopped preflight audit at `out/native/ashlar_publication_reads_r201/audited-summary.json`.

Next analyze the existing validation and compiler/caller phase evidence before another full publisher; prepare a controlled improvement that preserves exact raw/current/journal and manifest checks. Native published-version selection must tolerate physical-head maintenance and refuse expired snapshots rather than silently repoint. Production caller, controlled cold, sustained10k/s/100k/s recovery, diverse/skewed and1B/5B workloads, real producer authority/fencing and external-runtime gates remain open; UMF binding remains deferred.


### r204–r209: attribute remaining limits; shared telemetry qualified without an SLO win

The saved-evidence calculator measures each serial reader query's components separately. Compilation p95138/150ms and caller-minus-native-total107.750/104.481ms accompany engine95/96ms and caller348.750/340.264ms. Component percentiles must not be added. Per-query subtraction of compilation gives a202.730–206.265ms p95 sensitivity only if all other timings remain unchanged; it is not an implemented optimization, an achievable plan-cache claim or a revised caller gate. Caller reduction needed to250ms is90.264–98.750ms p95. File-size tuning alone cannot be assumed to deliver that reduction.

Unioning overlapping local SQL-call intervals covers43.506s of the59.883s overlap controller and45.995s of62.468s serial;16.377/16.473s remain outside those intervals. These gaps include history/telemetry, Python work and controller waits, assuming no local clock jump; they are not measured removable overhead and cannot be subtracted to advertise passing freshness. Evidence: `out/performance-attribution-r204.json`, with hashed exact source artifacts.

Read-only compute inventory at2026-10-07T06:23:06Z still returns one RUNNING data-gateway warehouse2439e1f2e37ac563,2XSmall/serverless,min/max1,auto-stop10m, no listed classic clusters. No configuration/provisioning is changed. This snapshot does not grant arbitrary job-compute access or measure an in-region caller.

The shared history collector gathers all lane IDs in a bounded paginated scan, refuses unknown/duplicate/client-failed/native-failed evidence, distinguishes pending history/final metrics from terminal failure, and retains nonfinal metrics explicitly when completion-only mode is requested. Native GET-only validation resolves38 real publication IDs with final histories in one page/722.667ms; no SQL workload or mutation occurs. Fourteen focused helper/proof tests pass. Final-cost publication collection continues requiring is_final=True; completion-only mode is not used to price costs or advance this publisher.

A new owned-clone publication keeps the same immutable stage, scoped intended/6-range current input, serial-current schedule and exact raw/current/journal/structure/manifest barrier. Only repeated per-lane telemetry scans become shared scans, with pending-only polling. All20 changed fields, exact raw wire/origin and journal,20M structural/identity,19.9M unchanged physical custody and manifest readback pass. Native output remains6 narrow files (14.8741–19.7827% live hash-domain spans), without full-table maintenance. Canonical r139-b1 unchanged.

Observed processing64.647s gives modeled record-age p9574.150s, missing60s and worse than prior71.973s. Timed write phase24.055s versus20.362s and validation19.442s versus17.951s also run slower. Shared collection inside the actual publisher clock is9.475s across three phases, each needing two attempts. This is a tested instrumentation efficiency change, not a demonstrated end-to-end freshness improvement or causal resource claim. The final38-record collection follows post-structural parity outside the publisher clock; the original batch-presence flag mislabeled it inside. Audited metadata corrects that classification while preserving raw summary and executed worker source; the script's future flag is corrected too.

Publication plus custody16.008GB reported reads/1.622GB writes, output-shape4.786MB reads/0 writes/0 spill, fits30GB/3GB and2GB shape bounds. All native IDs succeed/finalize; no duplicate mutation, new compute, resize or altered semantic checks. Evidence: `out/native/ashlar_history_collector_r206/summary.json`, `out/native/ashlar_queue_shared_history_r207/audited-summary.json`, `out/native/ashlar_publication_shape_r209/summary.json`.

Next pursue a bounded transport/compilation comparison using the same published snapshot and exact full-field oracle before another complete publisher. Preserve original250ms caller/60s freshness and1B/5B goals; no counterfactual or narrow warm cohort admits service, controlled cold, sustained10k/s/100k/s recovery or production source fencing. UC Delta/hash LC remain selected/proposed, graph-engine support remains scoped, and UMF binding deferred.

### Role isolation integration

The [r373 isolation design](role-write-isolation-design-r373.md) proposes stable canonical UUIDs under a single admitted writer lane. Mandatory per-batch clones are not admitted by measured preparation costs. Authority tokens guard descriptors only; takeover also requires demonstrated prior-worker termination and terminal owned native handles. Unexplained role commits refuse successor publication. The real worker fence remains unqualified; no performance or production profile is approved.

### Complete LC publication reference

The [r376–r377 bridge](lc-full-vector-disposition-r377.md) qualifies complete39.98M20-field equivalence to the previously published experimental range32 state. A private reference now bindsN6/LC5/R2/J2/A3/T2 without advancing source progress. Retain this independent full-sweep cost separately from subsequent incremental publisher clocks; production writer ownership and integrated freshness remain unqualified.
