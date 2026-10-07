# Scoped maintenance disposition: r299–r304

Governing design: ADR-001 and CONTRACT-003; publication/read boundaries remain
CONTRACT-001/002. The owner-selected UC Delta architecture and exact Truss fields
remain fixed; UMF binding is deferred.

Private maintained_edge_r287 UUID780781cb-9c42-434b-857c-dcddc663e735 was rechecked
at version2 after the prior canceled whole-table maintenance. Native preflight
found56overlapping files/2.266GB for lookup_hash below the first-sixteenth boundary.
The range-scoped FULL statement succeeded under the90-second cancel guard.
Actual versions3and4 share statement01f1c254-7f6f-1d09-9771-eed3b5836964: version3
removed55files/2,256,803,279bytes and added40files/2,251,985,927bytes; version4
added/removed zero files. The original controller stopped on its incorrect
one-commit assumption; same-handle recovery validated both commits and resumed
only reads, preserving the stopped summary. No mutation was replayed.

Eight uncached complete-field digest queries compare all39.99M carriers in400
bounded identity groups at versions2and4. Counts, every20-field multiset digest
and UUID match, underSHA256collision resistance. Maintenance/audit total is
112,064,179,420read bytes/2,251,985,927write bytes/zero reported spill. This is
maintenance equivalence to version2, not new production fencing or independently
qualified real source correctness. Schema/native global source requirements and
permanent raw/journal history are not replaced by a maintenance receipt.

## Matched singleton result

The identical32stratified keys, repeated twice, all pass exact20-field/absence
checks at version4. Native histories for all68queries are final, and the64points
are uncached. Metadata and preceding full scans warm data; one point reports
remote bytes, which is not a controlled cold-data cohort.

| Metric | Earlier post-change r282 | Scoped maintained r301 |
| --- | --- | --- |
| Caller p95 |430.197ms|385.014ms|
| Engine p95 |119ms|111ms|
| Compile p95 |177ms|170ms|
| Read bytes p95 |189,352,260|188,283,489|
| Read files p95 |12|3|

Inside the maintained hash predicate there are only4queries (2keys twice):
caller355.954ms/engine91ms/read135,662,939bytesp95. Outside there are60queries:
caller385.014ms/engine111ms/read188,283,489bytesp95. Small scope cohorts do not
prove a service tail, and ordered different cache states prevent causal speedup
attribution. Rewriting files overlapping the predicate can also affect keys
outside it. Overall100ms engine and250ms caller targets remain missed.

## Layout consequence and next work

Reducing file opens alone has not materially reduced this cohort's read bytes.
The remaining work should target per-singleton bytes and metadata/client costs,
with repeated cohorts, rather than treating a lower file count as success.
A whole-graph64MiB/reclustering result remains unqualified: this experiment
maintains one range and still carries wide-range data outside the chosen scope.
The100k publication139.786seconds remains an independent freshness miss; this
maintenance did not run a subsequent batch or establish sustained10k/s.

Use complete live-file range evidence to choose a next maintenance scope or
smaller target-file comparison, and include that rewrite's ingest/retained cost.
Do not extrapolate from the two favorable maintained keys to1Bnodes/5Bedges.
Controlled cold, concurrent reads, repeated batches, real producer fencing and
direct graph-tool reader features remain open. No production descriptor was
changed and no source ACK, retention cleanup or compute resizing occurred.
