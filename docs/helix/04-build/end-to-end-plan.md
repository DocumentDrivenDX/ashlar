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

## Trusted-writer carrier correction — 2026-10-09

Local authority validation previously split a scalar `trusted_writers='alice'`
into characters and could admit owner `a`. The validator now requires exact
built-in list/tuple/set/frozenset containers, captures one tuple, and requires
plain nonempty string identities before constructing membership. Regression
controls also refuse an executable list subclass that changes identities between
iterations and an unhashable string subclass; the former is refused without
calling its iterator. Supported collections retain whole-identity behavior.

Six authority tests, three effective-grants tests and two mocked registry tests
pass on Python 3.9.6. Astra independently rechecks the authority tests and reports
no remaining defect in this change. [Source-pinned local evidence](evidence/authority-writer-carriers-20261009.json)
records commands and qualification. This does not establish native Unity Catalog
permissions, the separate owner/grant-input boundaries, authenticated ordinary
actors or production publication authority; those obligations remain open.

## Owner and grant plain-data correction — 2026-10-09

A follow-on control demonstrated that a string subclass with spoofed equality
and hashing could impersonate an admitted owner or MODIFY principal. Owner,
permission keys and required permission fields now require exact built-in string
values. Grant inventories require built-in lists or tuples; records require
built-in dictionaries and are copied before validation.

Astra identified that tuple membership in the initial exact-container check still
called type equality: a custom metaclass could impersonate an admitted container.
The corrected checks compare type identity with `is`. Regression controls refuse
foreign writer and grant containers without equality or iterator callbacks.

Nine authority tests, three effective-grants tests and two mocked registry tests
pass on Python 3.9.6 with unchanged captured sources. The new
[source-pinned receipt](evidence/authority-plain-carriers-reviewed-20261009.json)
retains all commands and results. Earlier writer-only and owner/grant receipts
remain historical checkpoints; their source pins do not describe this revision.
No complete import closure, concurrent inventory snapshot, native inventory
authentication, Unity Catalog execution or full backend acceptance is established.

Astra ultra independently reran all nine authority tests on the corrected
revision and found no remaining actionable defect within local plain-data
validation. The review does not qualify the native obligations above.
