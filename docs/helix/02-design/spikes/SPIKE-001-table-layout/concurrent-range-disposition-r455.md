# Concurrent disjoint-range read comparison (r452–r455)

Goal: test whether concurrency reduces elapsed time for exact predecessor validation while measuring read amplification and contention. Immutable prepared0 and E6, four disjoint1/64ranges [10,14),[50,54),[90,94),[d0,d4), existing SQLwarehouse2439e1f2e37ac563/channel2026.39. Bounds15GBread/0write/1GBspill/240s for corrected run. No compute changes or canonical mutations.

Initial SDK run r452 returns correct values but reuses four result-cache entries in its concurrent phase. Its1.366s concurrent observation is invalid for performance admission. All8native statements are terminal/final and4,807,925,050read bytes/0write/spill are charged. Preserve the original stopped summary and offline terminal disposition; no same-handle replay.

Corrected r454 uses a separate persistent SQL connection per task, use_cached_result=false checked in each session, bounded sockets/retries and unique durable correlation. Sequential-first then four concurrent tasks; all16native statements including8cache checks are successful/final. Each phase exactly matches independent generated range counts1530,1564,1565,1585 with zero full20field predecessor mismatches (6244 total). No result-cache hits. Offline r455 checks exact native query text, results, cache status and costs for both runs.

| Phase | Cohort wall seconds | Read bytes | Query caller range ms | Engine range ms |
|---|---:|---:|---:|---:|
| Sequential |14.045|4,804,341,688|1736–2152|1169–1573|
| Concurrent4 |3.863|4,803,975,559|2583–3110|1928–2486|

Cohort clock includes connection/cache-check setup. Corrected total phase21.746s,9,608,317,247read bytes/0write/spill. Sequential has65.27MB remote reads, concurrent zero; fixed order/cache conditions prevent causal claims. Per-query engine time increases under concurrency even though cohort elapsed falls. Native metrics do not provide a waiting-at-capacity duration; do not claim absence of queueing from that missing field. These are four ranges only, not service p95, full-batch admission, actual MERGE, publication or billion-scale evidence.

Next justified step: full64range read-only validation of all100k predecessors with at most4persistent workers, exact20field checks/independent counts, strict total-cost/elapsed limits, and source preparation charged separately. Compare complete wall/bytes against r44739.279s/33.261GBfull-parent baseline, explicitly retaining different dates/cache/workload projection. Abort remaining unsent ranges on cost/time limits; inspect live handles without replay. Do not admit parallel same-table MERGE from read-only concurrency; conflicts/atomic guards/fresh input/six-role publication still need separate evidence.

UC Delta/unpartitioned identity hash LC remains proposed physical candidate. Real Truss producer authority, cross-role writer fencing, external adapter profiles and UMF deferred binding remain unchanged. All sustained/burst freshness, caller latency/cold/concurrency service and1B/5B requirements remain open.
