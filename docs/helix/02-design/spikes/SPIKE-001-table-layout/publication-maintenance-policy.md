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
