# Snapshot-pinned traversal budget and degree-skew evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_budget_20261005_m1`.

The structural adjacency fixture retains the existing 1M edges and adds 20,001
independent synthetic edges from node 200001 to existing nodes 1–20001. This
is a guard-test fixture, not a complete canonical edge publication. A degree
table is derived from exactly the pinned adjacency version; both versions are
recorded. Vertex existence uses the fixed canonical node snapshot.

The experimental work definition is **first-edge count plus second-edge
expansions preserving first-edge multiplicity**, not rows physically scanned.
Before fetching paths, the client checks first degree and sums the degrees of
every first edge's typed target at the same snapshot. DECIMAL(38,0) arithmetic
avoids narrowing the aggregate. Over 100k returns `QUERY_BUDGET_EXCEEDED`, with
no frontier/second-edge fetch. An admitted request fetches first edges and
second edges restricted to the explicit frontier; indexed client assembly
preserves each ordered edge-ID pair and verifies the expansion estimate.

| Root | Outcome | Candidate expansions | Returned paths | Caller wall |
| --- | --- | ---: | ---: | ---: |
| 1, regular | OK | 30 | 25 | 2139ms |
| 200001, high degree | QUERY_BUDGET_EXCEEDED | 120006 | No traversal executed | 1223ms |
| 9999999, isolate | OK | 0 | 0 | 1171ms |

The high-degree estimate counts 20,001 first edges plus 100,005 second-edge
expansions. Statement records prove no frontier or second fetch was submitted
for that root. The regular root retains parallel-edge multiplicity; the isolate
is distinguished from a missing vertex by explicit node lookup.

All 20 statements succeed, including the structured refusal path; query metrics
were refreshed by existing IDs. Indexed assembly was improved after the native
probe to avoid a first-by-second Cartesian client loop. It was verified locally
against the exact recorded frontier and second rows: identical ordered paths,
25 results. No cloud query was rerun for that local assembly change.

This proves bounded refusal in the scoped static fixture. It does **not** cap
physical file scans or establish the final meaning of the provisional work
gate. One regular request takes 2.14s, exceeding the 2s target in this probe;
single samples are not p95 admission. Degree maintenance under insert/delete/
endpoint changes, transactional publication of derived degree, generic type/
relationship filters, query policy, external engines and billion-edge cost
remain unproved. A mutable degree cache must not be substituted for the pinned
snapshot used here. The candidate needs fewer coordinator round trips and
independent physical-I/O evidence before acceptance.

Harness: `SPIKE-001-table-layout/native_traversal_budget.py`. Evidence:
`out/native/ashlar_budget_20261005_m1/`, including completed scope/results,
statement records and refreshed query history.
