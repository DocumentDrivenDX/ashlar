# Ashlar Delta schema candidate0.3

Reviewable extraction of the proposed [CONTRACT-003](../../docs/helix/02-design/contracts/CONTRACT-003-delta-graph-tables.md)
DDL and [table design handoff](../../docs/helix/02-design/spikes/SPIKE-001-table-layout/table-design-handoff.md).
Unity Catalog managed Delta is selected; this package remains a candidate and
has not been deployed. No runtime library, automatic installer or migration is
included. The owner stopped further tests; no cloud or Spark run is required here.

## Deployment selection

Create/select a fresh, explicitly reviewed UC catalog/schema with the intended
managed-storage and access policy. Replace the two quoted target placeholders
in each selected file before executing it. These are manual SQL templates,
not runtime parameters or an authorization boundary. Do not reuse an existing
schema as a migration, and do not substitute untrusted caller input.

Run01-baseline.sql for the six baseline tables. Select02-forward-adjacency.sql
for the first traversal projection;03-reverse-adjacency.sql and04-degree-summary.sql
are optional workload choices.05-typed-examples.sql contains illustrative types,
not mandatory production tables.06-coordination-candidate.sql belongs only to
a selected publisher protocol; its tables do not implement fencing by themselves.
The source CREATE definitions are preserved exactly, including column order,
nullability, cluster keys, statistics and properties. manifest.json records
source and file hashes plus table membership. No IF NOT EXISTS masks schema drift.

Native singleton templates require a separately validated publication descriptor,
qualified table identity/version and effective authorization context. They retain
the exact typed key alongside the hash and do not depend on Fabric. SQL templates
alone neither enforce caller policy nor validate uniqueness or publication.

## Enforcement responsibilities

| Rule | Enforcement in this package | Required application responsibility |
| --- | --- | --- |
| Required column presence/nullability | Delta NOT NULL declarations | Validate source decoding and classify failures |
| Typed object/edge key uniqueness | Not enforced by DDL | Reject duplicate accepted current identities |
| Endpoint type/existence; parallel edge identity | Not enforced by DDL | Validate final prospective batch and independent edge IDs |
| Exact properties and retained unknown content | STRING carriers | Preserve lexical text, missing/null and unsupported content |
| Raw delivery replay/conflict | Key columns and original payload/digest | Compare exact retained delivery; serialize/fence writer |
| Property event replay/order | Origin/version/event columns | Idempotent event identity and source profile checks |
| Deletes and stale resurrection | Tombstone representation | Apply CONTRACT-001 version/refusal policy |
| Degree direction/nonnegative counts | No native CHECK constraint | Validate direction/count and projection coverage |
| Multi-table publication | Descriptor representation only | Validate all role pins before visible immutable descriptor |
| Authentication and row/property policy | Not provided | Enforce CONTRACT-002 through UC/execution adapter |
| Retention and recovery | Not provided | Preserve active pins, replay evidence and tombstones |

US-002 AC1–AC7 remain traceable through the source contract/handoff. Native0.3
CREATE evidence belongs to its original private schema; this extracted package
has no new native execution claim. Later source statistics edits and synthetic
CTAS growth evidence do not establish canonical constraints or production support.
Performance observations inform tuning; they do not gate the UC Delta choice.
1B nodes/5B edges remains a planning scale, not admitted runtime capacity.

## Change and rollback

Additive nullable fields require declared revision compatibility; identity,
property type, hash recipe or endpoint changes require reviewed migration.
Never alter the raw payload to fit a projection. Retain old publication pins
through migration and select a validated prior descriptor for reader rollback.
This package does not DROP, VACUUM, rewrite or advance production progress.
Runtime, producer authority, production fencing and UMF binding remain open.
