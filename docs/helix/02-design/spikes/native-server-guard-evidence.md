# Server-side traversal guard comparison

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.

One SQL script executes vertex existence, root degree and expansion estimates,
then selects paths only in the admitted branch. Node version 2, structural
adjacency version 0 and its derived degree version 0 are pinned. The experimental
work metric remains first-edge count plus multiplicity-preserving second-edge
expansions; it is not physical rows scanned. This uses the previous regular/
20,001-edge hub/isolated-node fixture and adds a missing vertex case.

Twenty measured repetitions per shape exclude repetition zero. Every regular
query preserves all 25 distinct ordered edge-ID paths and reports 30 expansions.
Every hub returns `QUERY_BUDGET_EXCEEDED` with 120,006 expansions. Isolates return
an OK empty marker; missing vertices return `NOT_FOUND`. Marker rows have null
path fields and must not be counted as graph paths.

| Shape | Caller p95 | Parent execution p95 | Parent compilation p95 |
| --- | ---: | ---: | ---: |
| Regular admitted traversal | 2566ms | 2437ms | 29ms |
| Hub refusal | 1884ms | 1752ms | 28ms |
| Isolated vertex | 1778ms | 1647ms | 25ms |
| Missing vertex | 876ms | 748ms | 26ms |

All measurements have zero result-cache hits and zero remote bytes. All 85
statements succeed. Complete metrics were refreshed by existing IDs without
repeating queries. Parent script execution includes its internal statement
processing; these counters must not be compared as pure scan time with a
standalone SELECT or used to infer internal interpreter costs causally.

The native branch checks preserve refusal and multiplicity but fail <=2s regular
caller admission. Reducing caller round trips by scripting does not close the
gap in this candidate. The previous client control's 2.14s was one observation,
not paired p95, so no causal speedup/slowdown estimate is asserted.

Next compare one relational admission query followed by the admitted traversal,
retaining exact snapshot dependencies and the same logical budget. Physical scan
limits, derived-degree maintenance/publication, generic relationship/type policy,
external engines, concurrent writers and billion-edge behavior remain unproved.
The guard fixture is structural and is not a full canonical graph release.

Harness: `SPIKE-001-table-layout/native_guard_script.py`. Evidence:
`out/native/ashlar_guard_script_20261005_m2/`, including verified results, scope,
statement records, query history and grouped summary.
