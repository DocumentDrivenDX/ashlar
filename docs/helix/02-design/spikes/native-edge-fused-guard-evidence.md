# Shared transactional edge post-state guard

2026-10-05; SPIKE-001 native predicate controls on unchanged dbw-aidev-cus warehouse. `edge_fused_guard.py` supplies the exact post-state expression for the proposed fused transaction; `test_edge_fused_guard.py` evaluates it against virtual native rows at pinned canonical version 13.

Thirteen controls pass: clean state accepted; missing canonical, duplicate canonical, null wrong bag, wrong endpoint, lost retained bag, wrong derived hash, wrong entity version, missing journal, duplicate journal, wrong old token, false old presence and wrong native source-time text rejected. Count and distinct typed-identity guards supplement null-safe field/value comparisons. Journal epoch/version/kind/property/operation/presence/revision/origin/time and decoded exact scalar values are included. These are actual SQL evaluations of the shared predicate, not a separately reimplemented boolean oracle. They cover the selected cases, not every possible invalid input or a formal correctness proof.

Raw terminal control evidence `SPIKE-001-table-layout/out/native/ashlar_edge_fused_guard_20261005_r17/`. No graph mutations from controls. The integrated `native_fenced_fused300k.py` requires this passed result before setup, moves post-canonical/journal checks inside BEGIN ATOMIC before receipt insertion, keeps exact journal origin cardinality and actual fence writes, and omits the redundant separate post-change query. Stages use new disjoint ranges 120,001..210,000 across ten domains from version 13; all earlier source progress is retained.

The three 300k/30s native run is live under `out/native/ashlar_fenced_scheduled_20261005_r17/`. No throughput, freshness, source capture, maintenance or concurrent-reader result is yet admitted. Stages remain prebuilt outside the clock and native source semantics are synthetic. Observe the original process/statement handles; terminal summary and full preservation verification are required before a release claim. Goal active; UMF deferred.

## Verification continuation

All three r17 stages validate exactly 300k distinct typed keys; original process remains live under observation. `verify_fenced_fused300k.py` is prepared for a terminal summary only: 900k exact property journals/origins/source-time values, final 9,119,981 untouched 17-field carriers, typed canonical uniqueness and all descriptor vectors/progress/revisions. It reuses fixed actual published snapshots. No timing or preservation result is admitted before those terminal controls complete.
