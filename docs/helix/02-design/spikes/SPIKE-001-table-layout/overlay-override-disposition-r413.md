# Overlay override/fallback query r411–r413

CONTRACT-001–003 and proposed ADR-001 govern the selected UC Delta design. This is a read-only private experiment, not a production reader or Contract revision. It reuses the independently qualified E5+overlay1 dominance proof: every unique overlay key has version2 and an exact matched E5 predecessor1. Other E5 keys fall back unchanged. Overlay deletion suppresses fallback. No out-of-order, conflicting or multiple-version overlay is admitted by this special-case query.

The query selects live rows from the exact-key overlay, then selects the exact-key base only when no overlay row exists. Hash and full source/relationship/id predicates remain in both branches. Positive versions, non-null flags, key uniqueness, exact21field digests and complete source/predecessor custody are prerequisites from the earlier audit. Arbitrary inputs must still use a qualified version/conflict protocol; skipping those conditions would permit loss. Native source/current/history/projection tables and old pins are unchanged. No UMF binding or source ACK.

Six EXPLAIN FORMATTED plans (unchanged and new updated keys, three families) are fully supported by Photon. Override/fallback has0shuffle/aggregate operators; max_by has1shuffle/2aggregates. Direct scans have0shuffle/aggregate. Static plan shape is not a guarantee of lower elapsed time.

Eight same stratified identities cover old/new deletions, old/new updates and unchanged fallback. Two rotated passes across direct E6, override and qualified max_by return all48 actual20field carriers/absence exactly. Result caching is disabled; every point reports no cached result or remote reads. All60native statements succeed/finalize.45.897s and8,118,574,908read bytes/0write/spill are within12GB/180s bounds. All costs are reported counters, not price or retained inventory.

|16query family | Caller p95 | Engine p95 | Compile p95 |
| --- | ---: | ---: | ---: |
| Direct E6 |392.1ms |98ms |168ms |
| Override/fallback |929.3ms |272ms |531ms |
| Qualified max_by |791.0ms |207ms |483ms |

Repeat8query p95 is override727.3ms/264ms versus max_by791.0ms/207ms caller/engine. Small phase variability does not establish a causal service winner; retain max_by as the simpler general qualified-pair candidate rather than replacing it on the repeat caller number alone. Both overlay variants fail100ms engine/250ms caller. Tiny direct engine98ms remains a bounded screen, not broad/1B performance admission; caller still fails. Cold, concurrent, complete publication freshness, steady/burst, compaction and1B/5B gates remain open.

The override removes shuffles but does not close the latency gap. Stop adding query shapes for now. Next test physical file shaping on the single80.37MB overlay file with a16MiB target and bounded OPTIMIZE FULL, preserving every21field row and both old/new pins. Compare the same max_by query and exact singleton keys before/after; maintenance/setup/validation/telemetry must remain disclosed. Do not silently substitute a new head or schedule compaction per batch. A target file size is not a maximum.

Evidence: out/native/ashlar_overlay_override_points_r412/audited-summary.json and six native plan files. overlay_override_audit_r413.py checks source proof, actual generated override SQL/helper digest, every complete returned carrier and native-final plan/query text. The runner's inherited code inventory includes the earlier unused guarded helper; the audit additionally binds the actual override helper against captured SQL.
