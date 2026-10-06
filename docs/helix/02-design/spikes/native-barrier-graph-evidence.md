# Integrated pending-barrier graph publication

2026-10-05; SPIKE-001 native experiment, unchanged existing dbw-aidev-cus warehouse. Two disjoint prebuilt 300k stages (r17 positions 2/3) use a new 30s arrival clock. Failed r17 schedule remains failed, not resumed. No concurrent readers or source staging in this favorable scope.

Both data transactions enforce epoch/owner/no pending, set pending atomically with canonical/journal/receipt and shared exact post-state checks, then persist immutable batch receipts without guessed physical versions. Strict complete non-null row_commit_version aggregates resolve actual commits. Guarded descriptor insertion and pending clear commit together. These are cooperative writer guards, not table-level protection against bypass clients or permissions/lease enforcement.

| Position | Processing s | Queue s | Oldest freshness s | Edge/journal versions |
| --- | ---: | ---: | ---: | --- |
| 2 | 45.568 | 0.010 | 75.578 | 15/16 |
| 3 | 42.242 | 15.579 | 87.820 | 16/17 |

Both miss 60s and exceed 30s arrivals. Two observations are not a stable population p95. Atomic data caller 39.644/36.318s; actual resolution queries total 1.141s each; publish-and-clear 4.779s each. Removing version guesses fixes correctness but does not admit throughput. Exact attribution between layout, validation, native write and contention requires further controlled evidence; do not label the difference from prior trials a causal barrier cost.

All transactions and publications complete. Source progress is retained. Final exhaustive verifier separately checks all 600k changed/journal fields, 9,419,981 untouched 17-field carriers, typed uniqueness, origin uniqueness, complete descriptors and cleared pending state. Its terminal summary is required before full preservation admission; its cost is excluded from the failed publication clock.

Harnesses `SPIKE-001-table-layout/native_barrier_graph_batches.py`, `verify_barrier_graph.py`; raw clock, statements and summary under `out/native/ashlar_barrier_graph_20261005_r23/`, verifier under `out/native/ashlar_barrier_graph_verify_20261005_r23/`. No unknown writes were restarted or committed data replayed.

Next inspect actual physical validation plans and post-write file behavior to identify measurable layout/projection improvements while preserving native bags, exact values, typed endpoints and journal origins. Count maintenance and concurrent reads in any subsequent admission trial. Larger/in-region/isolation resources remain under pending cost bounds; full 1B-node/5B-edge scale, caller latency, sustained/burst and external engines remain open. Goal active; UMF deferred.

## Terminal preservation

Verifier passes: all **600k changed rows/journals** match the complete shared exact predicate at the final published vector, all **9,419,981 untouched rows preserve 17 fields**, all 10,019,981 typed keys and 600k event origins are unique, both descriptors read back exact vector/progress/revisions, both durable receipts exist, and pending is cleared with expected sequence four. Final unchanged checks are not exhaustive intermediate-state checks. Correctness passes for this bounded workload; failed timing stays failed.
