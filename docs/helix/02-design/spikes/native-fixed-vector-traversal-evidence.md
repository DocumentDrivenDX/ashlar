# Fixed-vector forward, reverse and two-hop evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.

The harness reads `synthetic-graph-release-1` from the owned manifest, then
uses its exact edge and adjacency versions in every query. No independent
latest-table selection occurs. Thirty measured roots per shape exclude
repetition zero. The fixture has regular degree five, including parallel edges,
with 10M vertices and 1M edges. All forward/reverse queries return five identical
independent edge IDs across layouts; all two-hop queries return the same 25
ordered edge-ID path pairs. Parallel paths are preserved rather than reduced to
distinct target vertices.

| Query | Narrow adjacency engine p95 | Narrow caller p95 | Canonical engine p95 | Canonical caller p95 |
| --- | ---: | ---: | ---: | ---: |
| Forward | 107ms | 440ms | 195ms | 461ms |
| Reverse | 108ms | 349ms | 193ms | 503ms |
| Two-hop | 394ms | 704ms | 364ms | 744ms |

All measured queries have zero result-cache hits and zero remote bytes. The
two-hop latency observations meet <=2s in this bounded shape. Both forward
paths and narrow reverse meet <=500ms list screening; canonical reverse misses
it at 503ms. These observations do not establish production p95, concurrent
publication, cold I/O, high degree or billion-edge behavior.

**Work admission is separate.** Although there are only 25 returned path pairs,
median history telemetry reports 2,000,000 rows scanned for adjacency two-hop
and 2,061,580 for canonical two-hop. Median rows read are 963,142 and 1,000,005
respectively; median read bytes are 15,015,395 and 20,876,540. A small output does
not prove <=100k candidate-edge work or enforce a traversal budget. No budget
guard is implemented here. A production query must define the work metric,
check expansion bounds and reject oversized work explicitly; physical scanning
must also remain observable. Do not substitute output cardinality for the gate.

Canonical outgoing/reverse file counters again report zero files despite
nonzero bytes; this ambiguous counter is not evidence of a zero-file path.
The narrow adjacency remains a small, one-file table. Multi-file pruning and
degree-skewed admission require separate evidence before selecting settings.

All 188 statements succeed, with complete telemetry refreshed by existing IDs
without repeating the workload. Harness:
`SPIKE-001-table-layout/native_graph_traversal.py`. Evidence:
`out/native/ashlar_graph_traversal_20261005_l3/`, including the consumed vector,
scope, statement records, query history and grouped summary. Explicit graph
descriptor validation is recorded in `native-graph-vector-evidence.md`.
