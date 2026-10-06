# Explicit node/edge graph version-vector evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.

The edge-only publication test did not pin its referenced node table. This
experiment validates an explicit graph descriptor across existing immutable
table versions before adding a new descriptor to the owned synthetic manifest.
It does not edit the prior descriptor or mutate graph data.

| Qualified table | Version |
| --- | ---: |
| client_dev.ashlar_scale_20261005_i1.object_current | 2 |
| client_dev.ashlar_edge_ingest_20261005_l1.edge_current | 2 |
| client_dev.ashlar_edge_ingest_20261005_l1.adjacency | 0 |
| client_dev.ashlar_edge_ingest_20261005_l1.property_journal | 1 |

Validation at exactly these versions proves:

- 10M node rows and 10M distinct injective node keys.
- 1M edge rows and 1M distinct injective edge keys.
- Zero unresolved typed source or target endpoints.
- Zero missing edges or typed-endpoint mismatches in the narrow adjacency copy.
- 200k parallel endpoint pairs retain two distinct edge identities each.
- 9.8M isolated nodes remain in the vertex population.

The appended `synthetic-graph-release-1` descriptor contains all four fully
qualified table/version pairs, synthetic feed progress, revision and validation
report. Readback checks the exact vector and report. All ten statements succeed;
metrics were refreshed by existing IDs without repeating publication.

This closes the fixed node-table reference gap for this synthetic snapshot.
It is not a source-transaction reconstruction, interruption/recovery, fencing,
policy, revision-barrier, generic catalog restriction, external-engine or
billion-node admission. The fixture defines one source/node type/self-relationship;
typed endpoint existence does not prove an actual Truss catalog's restrictions.
Property bags, retained content and source semantics remain subject to their
independent fidelity evidence. Node payloads reuse the fixed 1M pool ten times.
No source identity renumbering or cross-engine completeness claim is inferred.

Harness: `SPIKE-001-table-layout/native_graph_release.py`. Evidence:
`out/native/ashlar_graph_release_20261005_l2/`, including the completed summary,
statement records and refreshed query history. Native traversal controls consume
this exact descriptor rather than independently selecting latest table versions.
