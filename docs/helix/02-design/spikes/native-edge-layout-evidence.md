# Canonical edge identity and adjacency layout evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_edges_20261005_j1`.

Two canonical edge tables execute the full CONTRACT-003 edge fields, with
16MiB targets and the same identity/endpoint statistics. One clusters by
relationship/source/target, matching the initial candidate. The other clusters
by source-system/relationship/id. A third narrow adjacency table contains only
source-system, relationship, independent edge id and typed endpoints, clustered
by relationship/source/target. No property bag is silently substituted for the
canonical carrier.

The fixture has 1M independent synthetic edge IDs, one source/relationship and
one node type. A modular permutation maps IDs to 200k source nodes, preventing
edge-ID order from accidentally matching source order. Every source has five
edges, including two parallel edges to the same target. Both canonical tables
retain identical complete records; typed endpoints resolve against the retained
canonical node fixture; all 200k parallel pairs survive. This is not production
ID renumbering or a representative hub/degree distribution.

| Table | Active files | Active bytes |
| --- | ---: | ---: |
| Endpoint-clustered canonical | 9 | 271,165,325 |
| ID-clustered canonical | 18 | 270,148,828 |
| Narrow adjacency | 1 | 6,655,819 |

File targets are not exact output sizes: the endpoint layout produces fewer,
larger files. The comparison changes clustering and resulting file organization,
not solely the key while holding file sizes constant. Canonical properties use
eight SHA-256 hex chunks per edge, and retained unknown content is preserved.

Thirty measurements per shape exclude repetition zero. Every singleton returns
the same complete carrier across canonical layouts, and every outgoing query
returns the same five independent edge IDs and typed endpoints across all three
tables. Result cache hits and remote bytes are zero for all measured requests.

| Query | Engine p95 | Caller p95 | Median files read | Median bytes read |
| --- | ---: | ---: | ---: | ---: |
| Endpoint canonical, edge-ID lookup | 93ms | 351ms | 9 | 273,698,063 |
| ID canonical, edge-ID lookup | 79ms | 353ms | 1 | 15,535,382 |
| Endpoint canonical, outgoing | 193ms | 473ms | 1 | 1,008,974 |
| ID canonical, outgoing | 204ms | 435ms | 0 reported | 10,398,419 |
| Narrow adjacency, outgoing | 102ms | 326ms | 1 | 7,033,786 |

ID-clustered singleton reads prune 17 of 18 files; endpoint-clustered singleton
reads prune none. The approximately 17.6x byte difference matters even though
both small warm fixtures meet engine screening. Both miss the 250ms caller
gate. Outgoing queries have five results, below the list cap, but this bounded
shape does not prove high-degree or two-hop limits.

The ID-canonical outgoing history reports zero read files while nonzero bytes
are read, with zero pruned files. Treat that file counter as incomplete or
ambiguous for this shape, not proof of a zero-file access path. Byte and timing
results are recorded as observed. No engine-internal explanation is inferred.

The evidence supports an **ID-first canonical edge candidate plus selective
narrow adjacency**, preserving full canonical properties, independent IDs and
typed endpoints. It does not approve a production choice: source/type/relationship
distributions, cold I/O, hubs, reverse/two-hop queries, edge publication freshness,
snapshot synchronization, external readers, total duplication cost and 5B-edge
metadata scale remain unproved. The narrow table has only one file, so its scale
pruning behavior is not established. Full retained/old-version storage is not
included in the active-byte comparison.

All 173 statements succeeded; execution metrics were refreshed by existing IDs
without rerunning. Harness: `SPIKE-001-table-layout/native_edge_layout.py`.
Evidence: `out/native/ashlar_edges_20261005_j1/`, including completed run,
statement records, query history and grouped summary.
