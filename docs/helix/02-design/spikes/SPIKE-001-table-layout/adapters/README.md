# Proposed graph adapter surfaces

All mappings reference CONTRACT-003 and one immutable export publication.
No external engine has executed these mappings. `puppygraph-model.json` is a
model fragment; the live-version-specific Delta catalog connection must be
supplied and validated independently. `fabric-projection.json` is an Ashlar
mapping plan, not a Microsoft deployment API payload.

The small export fixture retains original logical identities and exact bags.
Only group_value is exposed in the sample PuppyGraph/Fabric property map. Other
properties, retained content, history and source rules remain in the canonical
source; every release must list that projection residual. Missing versus null
uses group_present plus scalar value. No unsupported decimal is cast to float.

Fabric export scope must bound both nodes and edges; filtered edges must resolve
to exported nodes. Cross-boundary edges require a recorded residual; no automatic
cross-scope graph federation is implied. Engine refresh, policy and source
progress are independently evidenced release metadata.

GraphFrames vertices come from node tables, preserving A0. Edge identity is an
attribute so endpoint-parallel rows remain distinguishable. Raw carrier strings
are kept alongside typed selected attributes; native scalar interpretation is
bounded by the adapter profile.

## 0.2 publication export evidence

[Native r67 export](../out/native/ashlar_layout_v02_export_20261006_r67/export.json) reads the actual r66 publication manifest and its pinned object/edge versions. Native IDs are exported as decimal strings to avoid consumer JSON-number precision loss. Injective graph keys preserve typed repeated IDs, two parallel edges, one self-loop and one isolated vertex. Exact JSON strings retain the integer above 2^53, explicit null and unknown content. The export declares history and other canonical-table residuals. This is native SQL plus Python conformance, not external-engine execution.

The proposed GraphFrames helper now requires a release table-version vector, rejects missing versions and duplicate edge IDs, and reads prepared adapter tables with `VERSION AS OF`. Those projection tables require their own release versions; canonical versions cannot substitute for projection versions. The helper remains unexecuted in Spark/GraphFrames. PuppyGraph model fragments and Fabric mapping plans remain earlier examples pending 0.2 release integration.

## Concrete typed release plan

Run `python3 adapters/build_v02_release.py` from the spike directory. The [0.2 mapping plan](release-r66/mapping-plan.json) and four checksummed local row files derive from the native pinned r67 export: two node tables and two edge tables split by relationship and endpoint types. Native numeric identities remain decimal strings in `native_id`; graph `id` is injective. Relationship identity survives the label split through `rel_type_id` and `edge_key`; consumers must include both endpoint-type labels when querying relationship 7. No canonical table duplication per type is required: these are consumer release projections.

Each engine release must materialize immutable tables from this fixed vector, verify counts/checksums and endpoint closure, then activate all mappings together. A failed or incomplete refresh keeps the prior release active. Local files are input evidence, not deployed Delta/OneLake tables. PuppyGraph catalog connection, Delta-feature compatibility, credentials and refresh semantics remain unqualified. Fabric edge-key uniqueness is publisher validated; no default edge-key constraint is assumed.

[Fabric limitations](https://learn.microsoft.com/fabric/graph/limitations) document approximately 2B elements and bounded strings; [performance guidance](https://learn.microsoft.com/en-us/fabric/graph/monitor-graph-performance) warns of unstable performance above 500M elements. The plan proposes 100M elements per release as an unproved conservative planning budget. Full 1B-node/5B-edge canonical scope is excluded. The local validator rejects strings over 65,534 UTF-8 bytes without truncation; any omitted carrier must instead be an explicit residual with a pinned canonical reference. No property JSON parsing or decimal-to-float conversion occurs.

## Native release materialization

[r68 evidence](../out/native/ashlar_layout_v02_release_20261006_r68/summary.json) records four actual Delta projections, all version 0, with exhaustive exported-field parity against the local pinned release. Counts are 2/1 node rows and 2/1 edge rows. `create_v02_release_graph` accepts these exact columns and projection-version vector; its Python syntax is checked, but Spark/GraphFrames execution is pending. The existing spike Python runtime contains neither pyspark nor graphframes. No dependency installation or new compute was started. The tables have version evidence, not immutability enforced by permissions or cross-engine activation evidence.

## Portable native singleton query builder

[native-read.ts](native-read.ts) builds native Databricks singleton SQL from an explicit publication vector. It validates table/kind/version and signed-int64 decimal identity strings, passes identity values as named string parameters, computes the derived hash in SQL and retains full native identity predicates. Bun boundary checks refuse numeric overflow/noncanonical IDs, invalid versions, missing vector entries and invalid table/kind mappings. No value is interpolated into SQL.

[r75 native execution](../out/native/ashlar_native_read_builder_20261006_r75/summary.json) passes four generated read-only queries against the 0.3 pinned publication: same numeric ID across two node types, one edge and a hostile-looking source string returning no row. Exact bags survive. The builder is browser-compatible TypeScript; Bun is only the fixture runner. No backend authorization or latency claim is inferred. SDK transport now accepts explicit parameter records while preserving the prior no-parameter path.


The native builder now also produces bounded typed adjacency queries using pinned forward/reverse table versions and keyset ordering by relationship type/edge ID. [r76](../out/native/ashlar_native_adjacency_builder_20261006_r76/summary.json) passes two forward pages (parallel edges then self-loop), reverse typed endpoint lookup and isolate lookup. Caller service code must bind continuation to the same publication, table, direction and endpoint; a raw cursor is not authority. JavaScript numeric identities are rejected before possible rounding; exact decimal strings are required. Bun adversarial checks and browser-target bundling pass; no real-browser runtime execution or new latency/scale claim.
