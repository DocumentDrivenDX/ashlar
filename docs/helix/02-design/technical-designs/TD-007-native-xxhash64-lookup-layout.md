---
ddx:
  id: TD-007
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
  - id: ADR-001
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: TD-006
    kind: informed_by
---

# Native 64-bit lookup layout candidate

## Decision to qualify

Author a **new** managed-current carrier profile, provisionally
`ashlar-delta/0.4-xxhash64`, with `lookup_hash BIGINT NOT NULL` on
`object_current` and `edge_current`. Keep the existing `ashlar-delta/0.3`
SHA-256 hex carrier, its original snapshots and their receipts intact. The
64-bit value accelerates lookup and is never semantic identity, uniqueness,
endpoint authority, a source digest or a publication digest. A signed negative
hash is valid. Independent edge ID remains part of the exact edge key.

Spark SQL computes `xxhash64(CAST(source_system AS STRING),
CAST(type_id AS BIGINT), CAST(id AS BIGINT))` for objects and substitutes
`rel_type_id` for edges. The built-in uses seed 42. Both publisher and reader
use that exact function, argument order and typed casts; source/type/ID MUST be
non-NULL and numeric values in signed 64-bit range. Compute the hash in the
native INSERT selection after parsing and casting original values. Compute
read-side equality with bound parameters in native SQL. Do not use a Python
hash, an implicit type coercion or a text/JSON concatenation. The stored
value must equal native recomputation at publication close.

Every singleton read and affected-key MERGE retains exact
`source_system`, `type_id` or `rel_type_id`, and `id` predicates. Every
publisher duplicate check compares the full tuple. A hash collision can add
candidate work but cannot select or overwrite another entity. Return no
partial result if row/hash validation or closing publication custody fails.
For 6B distinct keys under an ideal uniform-hash birthday model, a 64-bit
space has about 0.976 expected collision pairs and a 62.3% probability of at
least one pair; a 128-bit space has about 5.29e-20 expected pairs. These are
planning approximations, not a measured xxHash distribution or a correctness
bound. Exact-key checks make collision handling mandatory in either layout.
Do not add hash-only joins to Weft compilation or graph traversal. Weft owns
query compilation and its original artifact; Ashlar's publication-bound
singleton adapter owns this physical pruning predicate. If a selected Weft
target consumes this physical layout, version that target and qualify its
original compiled artifact rather than repairing its SQL in Ashlar. Select
the publication adapter by the exact table profile.

## Ownership and migration

The owner approved implementing this **new profile** on 2026-10-21. This does
not approve a full-size run or an in-place rewrite of existing tables. Start
the fresh functional trial with new tables under the selected profile; existing
SHA-256 tables and publications retain their original identities and bytes.

UMF authors the new physical field type, nullability, cluster/statistics
declarations and generated managed Delta DDL in a separately versioned model
and resource index using the current public UMF generator. A reusable UMF code
change is needed only if an actual generator gap is demonstrated. Do not mutate
the historical eight-role or current 0.10 six-role source assets. Ashlar adds
only profile-selected native publisher and
singleton/managed reader behavior, complete table UUID/version vector checks,
and a refusal when old/new table profiles or hash types are crossed. Existing
source, accepted-document, raw event, request, attempt and manifest SHA-256
digests retain their exact original bytes and meanings. Migration of existing
tables is a separate publication with new native identities and source custody;
changing the column type in place does not qualify it.

Audit the other high-cardinality columns before freezing the new UMF model:
`source_cursor_json`, `source_delivery_id`, `apply_batch_id`,
`logical_key_json`, `props_json`, `retained_json`, `schema_revision`,
`root_id`, `source_feed`, `source_epoch`, nullable `source_position`, and edge
`order_key`, plus
the table's data-skipping statistics list. For each, identify the exact
consumer or durable source/publication obligation and measured physical bytes.
Remove redundant statistics on columns not used for pruning; do not add a
second hash or pre-encoded full-key string. Preserve a column when exact
identity, presence, values, retained extensions, provenance, source cursor,
replay or ACK custody requires it. Any field removal or normalization needs a
separate versioned model and read/write proof; the lookup-hash optimization
alone must not silently discard source authority.
The [bounded column-footer audit](../../04-build/evidence/current-upstream-release-20261021/local-current/canonical-carrier-column-footer-audit-20261021.json)
at 1M nodes/5M edges attributes 35.33 MB and 176.60 MB compressed to legacy
SHA-256 hex `lookup_hash` (91.8% and 88.4% of those table files). Other
fixture columns were much smaller or constant. This justifies replacing the
lookup accelerator first; it does not establish production cardinalities for
the other fields. In the new layout, keep data-skipping statistics to the
hash and exact identity fields unless a separate query need is measured.

## Bounded acceptance before adoption

Use one identical, finite canonical node/edge corpus for the SHA-256 and
xxhash64 layouts, including a negative signed hash, non-ASCII source, maximum
signed IDs, parallel edges and an exact absent key. Check generated schema and
protocol, full-key singleton results, duplicate refusal, exact replay and
four-role publication vector with separate six-role installation inventory.
Force a same-hash/different-full-key candidate in an inert read-port control
to prove that hash equality cannot authorize identity; do not claim that
manufacturing an invalid native row is a valid publisher test. Measure active
bytes, point-read file pruning and elapsed time on the same runtime/data before
claiming benefit. Preserve the original sample and do not infer a 1B/5B pass
from a small comparison. The full run remains held under the owner's
15-minute estimate review rule.

## Open points

The exact UMF extension/profile name and generated resource revision are
assigned when UMF authors the carrier. The selected Databricks runtime must
confirm the built-in SQL behavior, schema type, signed output, clustering and
data-skipping support. This draft does not approve a migration or assert a
storage, latency or collision-free guarantee.
