---
ddx:
  id: SPIKE-001
  type: spike
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-002
    kind: informed_by
  - id: FEAT-003
    kind: informed_by
  - id: FEAT-004
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: ADR-001
    kind: informed_by
---

# SPIKE-001: Delta graph table layout

## Question and constraints

Select physical access patterns for a planning graph of 1B nodes with more edges,
low-latency native singleton lookup and explicit mapping to PuppyGraph,
GraphFrames and Microsoft Fabric Graph. Canonical storage remains Unity Catalog
managed Delta under [ADR-001](../adr/ADR-001-delta-canonical-and-serving-layout.md)
and [CONTRACT-003](../contracts/CONTRACT-003-delta-graph-tables.md).
Singleton lookup must work independently of Fabric or another graph service.
Performance measurements guide tuning and capacity planning; they do not gate
this storage architecture or substitute for semantic correctness.

Keep Truss-style property meaning, original source/type/entity identities,
independent edge identity, exact values and uninterpreted retained content.
Property history, raw source custody, tombstones and publication roles are
independent obligations. A rebuildable serving projection cannot replace its
canonical source or turn an unavailable projection into an empty graph.

## Comparison variables

| Variable | Compare and preserve |
| --- | --- |
| Physical organization | Identity-hash liquid clustering against explicitly qualified partition/Z-order alternatives; complete typed tuple predicates remain mandatory |
| Property access | Canonical JSON-text bags, promoted exact scalar columns and typed serving tables; preserve absent/null/value and numerical meaning |
| Edge access | Independent canonical edges plus directional adjacency/degree where admitted; preserve typed endpoints, parallel edges, self-loops and isolates |
| File layout and maintenance | File targets, statistics and overlap/read-amplification triggers; account for ingest, maintenance and retained storage together |
| External mappings | Publication-derived immutable releases with reversible identity/value correspondence and explicit per-engine capability/loss reports |

Start with the contract's unpartitioned identity-oriented liquid clustering and
64MiB tuning target; neither fixes actual file geometry. Hashes are pruning fields,
never uniqueness or endpoint authority. Use bound predicates over the complete
source/type/id tuple. Do not transfer PostgreSQL index, constraint or partition
semantics to Delta by analogy.

Preserve predictive optimization. Readability depends on observed optimization
configuration and retained snapshot guarantees. Maintenance may not repoint an
existing publication; exposing another snapshot requires a separately validated
publication. Generated DDL and missing Delta semantics belong to UMF's explicit
versioned model/extensions rather than a second Ashlar emitter.

## Measurements and bounded execution

Record engine/version/settings, source/model/layout pins, physical file statistics,
query plan and caller plus engine timing. Singleton checks compare complete original
carriers; filtered list and one/two-hop/count checks use independent typed bag
oracles, not row counts alone. Publication timing ends at the complete readable
immutable boundary, separately from remote source ACK and graph-engine refresh.
Include ingest and maintenance work in a whole-workload report.

Reuse retained performance evidence. New functional checks use tiny synthetic
fixtures on local Spark, Sail or DuckDB; this study authorizes no renewed scale
benchmark. Any future Databricks execution requires an explicitly dedicated Ashlar
endpoint and agreed resource/cost limits. Shared/default compute is excluded.
A bounded local case does not establish 1B-node capacity or a production latency
SLO; measurements name their exact workload and environment.

## Correctness and assurance

Every variant must preserve exact schema/value/identity and raw/current/history
correspondence, complete immutable native vectors, replay conflicts, retention and
source-specific progress. Preserve old readable publications independently of
later writes and physical maintenance. Apply the publication/recovery guards and
PUB-F1–F4/PUB-L1 assurance in
[TD-001](../technical-designs/TD-001-publication-recovery.md); physical tuning is
not evidence that those guards hold. Cross-engine support requires actual
execution of the claimed subset against an independent release oracle.

Entrypoints receive explicit bounded configuration; retain attributable safe
stage diagnostics and source/runtime fingerprints. Keep compiler, source
interpretation, publisher and native transport ownership distinct. Report
unsupported mappings and unavailable evidence without inventing native authority.

## Historical evidence

The complete original study, provisional targets and dated experiment observations
are retained byte-for-byte in the
[history custody record](../../04-build/evidence/table-layout-study-history-20261009/custody.json)
and its lossless archive. Original SQL, runners and raw receipts retain their
paths and profiles. Historical measurements qualify only their recorded scope;
they do not approve subsequent edits, production access or scale admission.
