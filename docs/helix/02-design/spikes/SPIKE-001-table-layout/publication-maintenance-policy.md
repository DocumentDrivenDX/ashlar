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
