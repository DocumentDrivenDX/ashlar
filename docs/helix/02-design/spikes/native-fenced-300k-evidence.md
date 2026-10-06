# Three 300k/30s fenced publications

2026-10-05; SPIKE-001 native experiment on unchanged existing dbw-aidev-cus warehouse. Three 300k property batches across ten domains model 10k/s at 30s arrivals. Stages are prebuilt and no readers run concurrently. Both fence phases, prior full-carrier checks, changed-row broadcast checks, journal cardinality and descriptor publication remain in the clock. Native ID ranges 30,001..60,000, 60,001..90,000 and 90,001..120,000 avoid prior deletes. Version-10 stage carriers preserve prior property changes as exact old journal values.

| Batch | Processing s | Queue s | Oldest freshness s |
| --- | ---: | ---: | ---: |
| 1 | 34.665 | 0.010 | 64.675 |
| 2 | 30.664 | 4.676 | 65.339 |
| 3 | 33.230 | 5.340 | 68.570 |

All three miss 60s and exceed their 30s arrival interval. Queue grows to 5.34s. These are three observations, not a population p95 or long-run steady state. Hint/batch changes improve queue growth compared with the previous experiment but differ in workload and table state; no matched causal attribution or sustained-rate admission is made.

Atomic apply caller is about 22.97–23.01s; hinted changed-field check 6.645/3.121/4.623s; guarded publication 2.924/3.004/3.413s. Version/journal checks add the remaining time. Thus hinted validation alone does not establish the gate. Source capture/staging, exhaustive post-run verifier and concurrent read load are excluded, making this favorable evidence still insufficient for production.

All publications complete at canonical 11..13 / journal 9..11, fixed adjacency 2, degree 1, tombstones 1 and nodes 3. Each stage validates 300k distinct keys; timed changed checks preserve all retained fields and intended property/origin/version updates. Previous source progress (including scheduled position four) is retained. Exact planned vectors require exclusive synthetic writer and actual version checks.

Harness `SPIKE-001-table-layout/native_fenced_300k.py`; terminal native statements, summary and caller clock breakdown under `out/native/ashlar_fenced_scheduled_20261005_r16/`. Separate `verify_fenced_300k.py` checks 900k journals, 9,119,981 untouched full carriers and descriptors; its terminal result is required before broad preservation claims. The original process completed; no unknown write was restarted.

Next test integrating post-state validation into the atomic apply with identical predicates, explicit broadcast and cardinality semantics, avoiding a separate canonical validation scan only if the transactional check itself proves the same invariants. Post-write pruning/maintenance and concurrent latency must also be measured. Full-scale resources remain subject to pending cost bounds. Goal active; UMF deferred.

## Terminal preservation result

Separate verifier passes: 10,019,981 distinct typed identities, all **9,119,981 untouched rows exact across 17 fields**, **900k exact journal values/metadata including source-time text** with unique full event origins, and all three descriptor vectors/progress/revisions. Changed carriers are checked at each timed publication. These final untouched checks do not claim exhaustive untouched verification at intermediate versions. The correctness result does not alter failed rate/freshness admission.
