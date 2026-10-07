# Fourth-batch direct read disposition r440–r442

CONTRACT-001–003 and proposed ADR-001 govern direct pinned reads after the successful full fourth100k publication. On the existing warehouse,8fresh fourth identities (2deleted/6updated) receive2rotated passes through before E6/after E7. All32actual20field carriers or deletion outcomes independently match FourthChanges. All37native statements are final/successful. Result cache is disabled, every point reports uncached, and final head7 remains unchanged.2,457,477,442reported read bytes/0write/spill over37.236s fit20GB/180s bounds.

|Repeat-pass8queries |Caller p95|Engine p95|Compile p95|Remote queries|
| --- | ---: | ---: | ---: | ---: |
|Before E6|569.54ms|141ms|325ms|2|
|After E7|603.60ms|336ms|172ms|3|

Across both passes (16per family), before caller/engine p95 is929.42ms/373ms; after664.54ms/368ms. These are small ordered cohorts after validation, not a controlled cold-data or service-tail test. Before/after remote conditions differ, so no causal ingestion-induced latency penalty is established. The250mscaller target remains missed, while the warm100msengine target cannot be admitted from this mixed-remote repeat cohort. Lower compilation after E7 did not solve caller latency.

Native telemetry reports median files4before/6after. Before point reads sum1,016,154,232bytes versus1,441,323,210after, with113,248,079/74,109,634remote bytes respectively. All after repeat queries access6files; the3remote after repeats include only199,033/217,076/1,789,958remote bytes yet engine154/336/148ms. The other5after repeats are zero-remote with engine79–103ms; selecting that subgroup is descriptive and is not a warm service percentile. File count, byte counters and remote metadata/DV/data contributions need further plan evidence rather than a presumed physical explanation. Data-skipping still needs improvement under repeated scattered writes.

Keep direct hash+complete-tuple Delta lookup independent of Fabric. Qualified inline guarded ingest is retained because the preceding whole processing test removes redundant before-carrier I/O while preserving all required semantics; it still misses freshness. Next inspect current MERGE source/target join and direct read physical operators, with source narrowing/join hints assessed read-only on the qualified immutable pair before a fifth mutation. Use the measured4→6file observation to scope maintenance; do not run another unbounded whole40MOPTIMIZE. Native caller/cold/concurrency, sustained/burst and actual1B/5B gates remain open, without reopening UC Delta or claiming new external-engine support.
