# Native result-cache and statement transport disposition

Native result-cache hits reduce measured engine time, but do not meet the provisional250ms caller target on the existing2XSmall warehouse. The persistent SQL connector remains the measured read transport candidate; this small statement-API comparison is slower. No table, compute, publication or architecture change was made.

The cohort is eight identical pinned-version5 identities:4second-deleted,2second-updated,2unchanged. Exact full20fields or absence are independently reconstructed for every request, with full hash/source/type/id predicates. Stable SQL/parameters/run-key tags repeat three times with cache enabled in the same connector session, then once disabled. The session-only setting is verified and restored false before closure.

| Connector phase | Native cache hits | Caller p95 ms | Engine p95 ms | Compile p95 ms |
| --- | ---: | ---: | ---: | ---: |
| Enabled first8 |0|626.269|108|197|
| Enabled repeat8 |8|352.125|3|175|
| Enabled third8 |8|342.203|3|223|
| Disabled control8 |0|392.452|94|177|

All38 statements are native-final, all32exact point results pass,16observed hits. Wall23.422s, read1,520,962,436 bytes/write0/spill0. The16hits combined p95caller352.125/engine3ms still misses250ms. Cache-hit results do not establish physical pruning, cold latency or production hit rate across an arbitrary1B-key working set. Warm data without result hits and warm repeated-result behavior remain separately reported; engine3ms must not replace the prior uncached104ms engine evidence.

The statement API uses the same eight keys and pinned version in two passes through a shared SDK HTTP client, with STRING named parameter declarations. Native per-statement session/cache behavior is observed, not assumed equivalent to the persistent connector session.

| Statement phase | Native cache hits | Caller p95 ms | Engine p95 ms | Compile p95 ms |
| --- | ---: | ---: | ---: | ---: |
| First8 |0|1,823.968|344|1,082|
| Repeat8 |8|647.732|122|147|

All16 native statements are final and complete results match. Wall14.838s, read760,481,218 bytes/write0/spill0. Query/session initialization, cache and transport conditions differ; this is a descriptive comparison, not causal isolation of a network component. In particular the first-pass1.824s is not controlled cold-data service latency. SDK UUID custody inherits the immediately preceding controlled connector/pinned fixture; no general unknown-writer identity or policy-delegation claim.

Combined new workload:2,281,443,654 bytes read, zero writes/spill,48point requests plus6connector configuration/detail statements. Native-final raw receipts and offline full-field/parameter/cohort/cache-count/percentile audit retained. No live-head mutation or cache invalidation/recovery test, service percentile, freshness/sustained/burst or billion admission.

## Design implication and next work

Keep LC/full native identity/pinned reads and separately admitted bounded maintenance as the proposed baseline. Result caching may accelerate repeated immutable requests but cannot currently be relied on to meet250ms, and its snapshot/policy/invalidation guarantees would need explicit production qualification. Do not replace the native tuple predicate or consume a physical head to reduce planning overhead.

Next inspect the installed connector's polling/direct-result behavior and compare any justified bounded transport change with this same pinned full-field cohort. Avoid another large table copy until a smaller experiment identifies a plausible benefit. The optional250ms-versus500ms owner calibration remains unanswered;250ms is retained. Delta architecture is selected regardless of measured misses. Native graph feature interoperability,1B/5B, actual producer fencing, permanent history/publication throughput and deferred UMF remain open as previously recorded.

Evidence: `out/native/ashlar_result_cache_r354/audited-summary.json`, `out/native/ashlar_statement_cache_r356/audited-summary.json`; auditor `native_cache_receipt_audit_r355.py`.
