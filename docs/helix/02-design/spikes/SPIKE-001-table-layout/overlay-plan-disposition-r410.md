# Qualified overlay lookup plans r408–r410

CONTRACT-001–003 and proposed ADR-001 govern the selected UC Delta design. This read-only experiment uses the specific independently verified immutable E5+overlay1 pair, compared with direct E6 and the prior guarded query. No Contract/production-reader revision, table write, pointer or source ACK occurs. UMF remains deferred.

## Preconditions and scope

Complete prior100k overlay digests, unique typed keys, source/history custody, positive version2 markers and all100k E5 bootstrap-version1 predecessors are bound through the audited source SHA chain. Direct/current reference E6 is the corresponding accepted publication. The simplified query omits per-read conflict fingerprinting only under this particular conflict-free immutable-pair qualification. It does not admit arbitrary conflicting data or implement a real publisher fence. A future accepted publication must prove conflicting identity/version absence before readers rely on this optimization. Exact duplicates, if allowed, must have identical accepted carrier content/origin; incoming semantic no-op classification is still the producer/publisher responsibility.

## Native evidence

Six EXPLAIN FORMATTED plans cover an unchanged key and a new updated key for direct, guarded and simpler winner queries. Every plan reports full Photon support. Guarded plans have3shuffle sink operators/6aggregate operators; the simpler max_by winner has1shuffle/2aggregates. Direct scans have0shuffle/aggregate operators. This is static native plan evidence, not measured per-task attribution or a proof of every future adaptive plan.

Eight previously tested identities include old deletion, new deletion, old/new updates and unchanged keys. Two interleaved passes across three query families return all48 complete20field carriers/absence exactly as independently reconstructed. Result caching is disabled; all48 queries report no cached result and no remote read bytes. Metadata identity/head checks remain exact; selected versions are E5/O1/referenceE6. All60submitted native statements succeed/finalize. Total51.816s/8,057,034,092reported read bytes/0write/spill, within10GB/180s bounds.

| Family,16queries | Caller p95 | Engine p95 | Compile p95 |
| --- | ---: | ---: | ---: |
| Direct E6 |436.1ms |95ms |183ms |
| Guarded winner |1058.9ms |305ms |635ms |
| Qualified-pair max_by |768.3ms |226ms |467ms |

Repeat-pass8query p95 is direct436.1ms/95ms, guarded874.4ms/305ms and qualified754.9ms/226ms caller/engine. These are tiny ordered warm-data subcohorts; direct engine95ms is a bounded screen, not reversal of larger112ms evidence or broad/service/scale admission. All caller250ms gates remain failed. The simpler overlay still fails engine100ms and caller250ms; no cold/concurrency/steady/burst/compaction/1B/5B evidence is added.

The result supports simplifying a qualified read plan, but one aggregation/shuffle and extra file access remain. Before testing overlay compaction, inspect a direct overlay-override/fallback shape for this specific one-row-per-key pair: overlay wins for every affected key because version2 exceeds the proven base version1; missing overlay falls back to E5; deletion prevents fallback. It must not be generalized to unqualified/out-of-order/duplicate/multiple-version overlays. Test all three cases and full-key/hash residuals, compare native plans, preserve old pins, and require an explicit pair proof. Keep a small8-key/2pass/10GB/180s comparison; no full-width reference join or extra batch is needed.

Canonical in-place LC current tables remain the existing qualified architecture candidate. The append overlay remains experimental and unadmitted. Its earlier8.0s append excludes source admission, other roles, validation/publication and compaction costs. External graph consumers still need qualified materialized/logical-current releases and native feature compatibility; Fabric remains bounded. No physical unique-row Contract compliance, new graph-engine claim or production policy/delegation follows from these query plans.

Evidence: out/native/ashlar_overlay_plan_points_r409/audited-summary.json and its six complete plan files. overlay_plan_audit_r410.py independently checks source chain, plan bytes/operator counts, native-final query text, every full returned carrier and percentile calculations.
