# End-to-end toolkit workflow

Use the [toolkit delivery plan](toolkit-delivery-plan.md) for implementation order,
parallel work and acceptance criteria.

## Required workflow

An engineer can set up Ashlar on Unity Catalog managed Delta, submit original
UMF definitions, ingest typed objects and relationships, publish a complete
immutable snapshot, and query it through native singleton reads, Weft and
qualified graph-engine releases. Additional sources use the same documented
source-adapter boundary. Real Truss uses its accepted catalog and complete feed.

Schema additions and evolution preserve stable identity and original metadata.
Create, update and delete preserve exact values, history, tombstones and source
progress. Replay is idempotent; conflicting content refuses. An interrupted or
uncertain publication retains the previous readable snapshot until recovery
verifies the complete new state. Source acknowledgement follows the required
durable publication handoff. Independent sources retain separate identity,
epoch and checkpoint scopes.

## Boundaries

UMF owns schema interpretation and generated DDL; Weft owns query compilation.
Adapters retain original versions and bytes and disclose unsupported semantics.
Graph releases preserve isolated nodes, independent parallel edges, typed
endpoints and publication lineage. Authorization, retention and source fencing
are explicit host obligations. Predictive optimization follows the selected
workspace configuration. Functional checks stay small; prior performance
measurements inform operations without gating the Delta architecture.

See [the runnable guide](../../../examples/end-to-end/README.md),
[publication contract](../02-design/contracts/CONTRACT-001-publication-boundary.md),
[table contract](../02-design/contracts/CONTRACT-003-delta-graph-tables.md), and
[resolver contract](../02-design/contracts/CONTRACT-004-publication-resolver.md).
Historical execution notes are retained in [original evidence](evidence/documentation-history-20261009/end-to-end-plan.original.txt);
its provenance file records the original relative-link base and byte digest.
