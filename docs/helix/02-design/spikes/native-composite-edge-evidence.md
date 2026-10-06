# Composite edge identity and typed adjacency baseline

2026-10-05; SPIKE-001 experiment, not layout/compatibility approval.

## Existing resource boundary

A current read-only inventory reports only data-gateway running: serverless PRO 2X-Small, min/max one cluster; no all-purpose clusters. No alternate running compute exists for read/write isolation. Raw inventory: `out/native/compute-inventory-20261005-p4/inventory.json`. This does not prove an isolated resource would meet latency, and no resource was started or resized. The resource/cost bound remains pending for new isolated/in-region or larger tests.

## Native candidate and invariants

`native_composite_edges.py` copies a fixed 1M-edge carrier pool into ten synthetic source/relationship domains: five sources, relationships 7/8 with same-type endpoints 1/2, native edge IDs 1..1M in each domain. Source/relationship/type/feed labels are fixture definitions, not a mapping from a real native catalog. The 17-column canonical table adds one SHA256 pruning column over exact source_system,rel_type_id,id. Native tuple predicates remain mandatory; hash is not graph identity or an endpoint key. [Experimental DDL](SPIKE-001-table-layout/sql/composite-edge-candidate.sql) stays separate from normative CONTRACT-003.

Narrow adjacency carries seven structural fields, clustered by source_system,rel_type_id,source_type,source_id. Fixed snapshots: node p1r1 **3**, edge q1 **2**, adjacency q1 **0**. All 10M typed edge identities are distinct, despite only 1M distinct native IDs. Every derived hash validates. All 10M property/retained/order carriers and endpoint IDs match the fixed source pool. Both typed endpoint anti-joins report zero unresolved rows against the fixed mixed-domain nodes. All 10M adjacency records match canonical endpoints. All **2M parallel endpoint pairs** retain both independent edge identities. Native relationship-catalog restrictions are not proved by fixture typing.

## Layout and measured reads

Existing dbw-aidev-cus data-gateway warehouse, unchanged settings. Population **36.22s**, edge OPTIMIZE FULL **12.05s**, adjacency OPTIMIZE **3.25s**, outside read timing. Canonical: **128 files / 3,042,696,020 bytes**. Adjacency: **8 files / 81,044,287 bytes**. This is 10M nodes/10M edges, not the owner-selected 1B-node/5B-edge scale or ratio.

Fifty-one domain/key lookups repeat across three phases; rep zero excluded leaves 50 per phase. Every projected native identity/typed endpoint/property/retained carrier matches expectations and repeats exactly. Every measured point lookup reads **one file**, zero remote bytes/result-cache hits; read-byte p95 **28,598,130**.

| Phase | Engine p95 ms | Caller p95 ms |
| --- | ---: | ---: |
| Prime | 79 | 347.4 |
| Repeat | 77 | 379.9 |
| Repeat2 | 75 | 333.5 |

All pass the bounded warm engine screen; all fail 250ms caller latency. The lookup is the ten-column endpoint/carrier projection recorded in the harness, not an asserted timing for every possible full-row API projection.

Twenty measured outgoing-list pairs (rep zero excluded) return exactly the same five ordered edge records from adjacency and canonical at fixed versions. Adjacency engine p95 **109ms**, caller **412.7ms**, median one file, read-byte p95 **13,602,853**. Canonical engine **328ms**, caller **609.1ms**, read-byte p95 **114,205,972**. Canonical reports zero files read despite nonzero bytes; do not infer zero I/O or use that field to claim no scanning. Both groups have zero remote bytes/result-cache hits. Five returned edges are not a physical work cap, hub admission or general graph latency guarantee.

All 213 statements complete. Query metrics refresh uses the same completed IDs without reruns. Raw run: `out/native/ashlar_composite_edges_20261005_q1/`.

## Disposition

Retain canonical independent edge identity plus separate narrow typed adjacency as the experiment candidate. Next test multi-domain edge property publication with exact property journal origin keys, fixed node/edge/adjacency vectors and pinned readers. Then structural changes must maintain adjacency/degree and validate endpoints in the same graph release; this static result does not establish those operations.

Caller latency, warm engine latency under publication load, sustained/burst ingest, maintenance policy, full scale, native source ordering/catalog/recovery and PuppyGraph/GraphFrames/Fabric execution remain open. Native exact bags and retained content remain canonical; UMF binding stays deferred. The complete table-layout goal remains active.
