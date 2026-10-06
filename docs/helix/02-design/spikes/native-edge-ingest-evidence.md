# Canonical edge property publication evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_edge_ingest_20261005_l1`.

The full CONTRACT-003 edge fields are seeded from the retained 1M-edge fixture.
Canonical clustering is source-system/relationship/id with a 16MiB target.
The full property journal and manifest schema are used. Experimental catalog-
managed edge/journal/receipt writes commit atomically. Narrow adjacency contains
source-system, relationship, independent edge id and typed endpoints, clustered
by source-system/relationship/source-type/source-id.

One prebuilt 200k-edge batch accumulates over 20s, modeling 10k changes/s.
Only property 201 changes, from varied 512-character SHA hex values to varied
2KB values. Endpoints stay fixed, so adjacency is pinned at its existing version
rather than rewritten. Staging, exact prior-state checks, canonical/retained/
endpoint/version validation, journal count and decoded old/new checks, version
discovery and manifest publication remain timed. Producer generation is outside
the arrival clock. No concurrent reader load is added.

| Component | Caller wall seconds | Engine seconds |
| --- | ---: | ---: |
| Stage | 5.70 | 5.24 |
| Atomic apply and validation | 17.91 | 17.40 |
| Manifest publication | 0.89 | 0.68 |

Total ready-to-publication processing is 25.37s, including version discovery.
Oldest-change freshness is 45.37s including the full accumulation window;
newest-change freshness is 25.37s. The single batch stays below 60s, but its
processing exceeds the 20s arrival interval. This is not steady-stream p95 or
combined node-plus-edge throughput admission.

All transaction checks pass. The published canonical/adjacency version pair
hydrates 200,000 changed edges with zero exact property, retained-content,
typed endpoint or version mismatches. Journal total and distinct feed/epoch/
position/ordinal keys both equal 200,000. Independent edge IDs are retained;
parallel edges remain separate even though their endpoint pairs coincide.
All 18 statements succeed; telemetry was refreshed by existing IDs without
repeating operations. Post-publication hydration is outside the freshness clock.

The fixture has one source, relationship and endpoint node type. It uses a
synthetic edge-only feed with one property event per entity and fixture-derived
ordinals; this does not establish mixed node/edge source-event ordering or
native Truss reconstruction. The descriptor covers this edge publication's
edge/journal/adjacency tables. A complete graph release still needs the explicit
fixed node-table version in its vector and typed endpoint validation there.
It must not inherit a complete-graph claim from this scoped edge-only result.

ID-first canonical edges plus reusable narrow adjacency are viable candidates
for fixed-endpoint property updates. Endpoint mutations, adjacency rewrite
cost, insert/delete/replay, recovery, degree skew, reader load, external engines,
long-run/burst rates and billion-edge/cost admission remain unproved. No endpoint
mutation semantics are inferred from the unavailable native feed contract.

Harness: `SPIKE-001-table-layout/native_edge_ingest.py`. Evidence:
`out/native/ashlar_edge_ingest_20261005_l1/`, including completed summary,
statement records and refreshed query history.
