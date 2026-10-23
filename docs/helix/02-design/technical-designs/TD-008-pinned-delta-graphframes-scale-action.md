---
ddx:
  id: TD-008
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: US-003
    kind: informed_by
  - id: FEAT-003
    kind: informed_by
  - id: ADR-001
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: TD-007
    kind: informed_by
---

# Pinned Delta GraphFrames scale action

## Scope and technical approach

This is one proposed implementation slice for US-003-AC5 and the GraphFrames part of QUERY-03. The existing `read_graph_release` and immutable JSON export collect complete rows in Python and deliberately cap the `ashlar-delta/0.4-xxhash64` release at 10,000 nodes and 10,000 edges. Keep those APIs and their finite evidence unchanged. A full 1B-node/assumed 5B-edge algorithmic trial needs a separately named pinned-Delta profile that passes Spark DataFrames directly to an actual GraphFrames action. It does not qualify Truss acceptance, managed Unity Catalog publication, production authorization or full-width UMF carrier throughput.

The first slice is a development benchmark over a known synthetic fixture. Freeze the generator revision, parameters, source digest, expected row counts and endpoint formula before writing. Its fixture holder observes and records the two current-table Delta UUIDs, versions, schemas and protocols after the physical load, pins those exact versions for engine reads, and reopens/renews the same identities before and after the action. It owns a bounded immutable use interval; an altered table head, schema, protocol or fixture selection refuses. This fixture has no Truss source, publication manifest, protected ACK or six-role installation registry. Its result must label source progress and ACK as absent, not infer them from row counts or file metadata. It cannot claim production or effective-user-policy authority.

A later managed-publication variant must instead reuse the existing resolver, source, pin and effective-policy holders for one selected immutable descriptor, its complete four data-role UUID/version vector and separate six-role installation registry. It must preserve the original source progress and protected ACK, renew all holders after engine execution, and enforce `authorize_graph_row` through a separately qualified native DataFrame policy predicate or equivalent whole-dataset authorization capability. That variant must read qualified Unity Catalog table names with `VERSION AS OF` under Unity Catalog governance; it must not recover physical locations through `DESCRIBE DETAIL` and read files around that boundary. `CONTRACT-003` and `CONTRACT-004` govern the managed variant. It is not a prerequisite or a claim of the first synthetic component benchmark. A shared public command or receipt format requires a corresponding Contract update before implementation.

Read `object_current` and `edge_current` at their exact pinned Delta versions in Spark, selecting transient GraphFrames keys. Vertex `id` is a struct with canonical field names `(source_system STRING, type_id BIGINT, id BIGINT)`. Edge `src` and `dst` use those same field names, aliasing their corresponding source-qualified endpoint columns; preserve a separate struct of `(source_system STRING, rel_type_id BIGINT, id BIGINT)` for independent edge identity. Reject null and out-of-range signed-64 components and cross-profile table/schema mismatch. Retain parallel edges as distinct occurrences. These keys exist in the query plan only; do not add another high-cardinality stored column, use `lookup_hash` as identity, convert the full graph to GraphX, collect full frames, or create a second persisted graph copy. The signed 64-bit lookup hash in TD-007 remains only a pruning accelerator for native singleton reads.

Construct `GraphFrame(vertices, edges)` only after exact runtime/JAR/profile admission. One bounded one-hop action from a selected seed must return a small edge-distinct occurrence bag with its frozen fixture and runtime identity; compare it with an independently frozen fixture oracle. A result limit that truncates a larger answer must refuse or report incomplete, never claim exactness. The [2026-10-21 compatibility receipt](../../04-build/evidence/current-upstream-release-20261021/local-current/pinned-delta-graphframes-smoke-20261021.json) records a tiny synthetic Spark 4.2.0/Delta 4.4.1/GraphFrames 0.12.3 struct-key and one-hop pass. This qualifies only that fixture/runtime slice: source progress, protected ACK, publication, Unity Catalog, scale and production authority remain absent. It removes the runtime-compatibility prerequisite for this tiny synthetic profile, not the publication-bound managed variant or full-scale trial. Keep failure behavior closed rather than silently using the Spark 3 export worker.

## Component changes and verification

| Owner | Bounded change |
| --- | --- |
| `src/ashlar_host/pinned_delta_graphframes.py` | Reuse the synthetic holder's two version-pinned Spark frames and map canonical `.4` current carriers into transient GraphFrames views. Preserve payload columns, source-qualified struct keys and independent edge identity. Keep bounded result collection inside the action only; do not route through `GraphRelease` JSON or change its cap. |
| Synthetic fixture holder | Keep the tiny compatibility fixture separate. For a canonical `.4` benchmark, freeze generator revision/bytes, source bytes, carrier profile, exact runtime, row counts, endpoint formula, seed and independent oracle before writing. Observe both current-table UUID/version/schema/protocol identities, read their exact versions, and renew the same identities before/after the action. Establish retained-file availability and exclusive use for the bounded interval. This synthetic holder provides no source, publication, ACK or production authority. |
| Test/evidence owner | The small Spark 4.2/Delta 4.4 GraphFrames smoke with struct IDs, parallel edges, isolated node and selected one-hop answer is recorded in the linked receipt. Preserve its changed-version/expired-custody refusals and runtime/JAR evidence; test the shared carrier mapping inertly before the future full-scale trial. |

The full local algorithmic trial has one physical 1B/5B load, two exact point lookups, two scans of the persisted representation for row and endpoint-formula witnesses, and one actual bounded GraphFrames action. Freeze the deterministic source/endpoint formula and expected occurrence bag before effects; derive uniqueness and valid endpoint range from the generator and verify persisted counts and formula violations or transaction-log row counts without a second full-graph uniqueness join. Those fixture proofs do not relax enforcement for a production publisher. Record physical bytes, elapsed time and engine result for each action. This is a constant number of O(N+E) passes, not a low-latency or publication/ACK throughput claim. A small native smoke is implementation verification; the full trial remains held under R05's sample-based owner review and resource limits.

## Risks and rollback

GraphFrames may reject struct keys or the selected Spark 4.2 runtime. Refuse the new profile on that result and retain the existing bounded export unchanged. An unbounded motif or `collect()` would overload the driver; enforce the selected seed/result cap before action and inspect the physical plan for accidental cross joins or full-result materialization. If the trial exceeds its approved time or resource ceiling, terminate it and retain the original attempt and partial evidence without converting them into a support claim. No existing table migration or engine activation is part of this slice.
