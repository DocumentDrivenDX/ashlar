---
ddx:
  id: ashlar.evidence.managed-a1-authenticated-discovery-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-authenticated-discovery-20261023
      kind: informed_by
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-002
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
---

# Managed A1 authenticated read-only discovery addendum

Observed 2026-10-24 using the approved authenticated Databricks CLI profile.
This addendum records workspace metadata only. No schema, cluster, job or
warehouse was created, started, changed or deleted; no compute ran and no spend
was initiated by this discovery. Authentication and token details are omitted.

## Read-only observations

- `databricks clusters list` returned an empty list. The node-type metadata
  returned both `Standard_D4ds_v5` and `Standard_D4s_v5`, each with four cores,
  16,384 MiB memory and `is_deprecated=false`.
- `databricks clusters spark-versions` advertised
  `19.x-scala2.13` (labelled Spark 4.2.0, Scala 2.13) and
  `17.3.x-scala2.13` (labelled Spark 4.0.0, Scala 2.13), along with ML and
  Photon variants. These are selectable metadata identifiers, not evidence
  that a runtime/build is allowed by policy or runs Ashlar. Exact maintenance
  build, Java, Delta, Python, glibc/loader, Unity Catalog behavior and native
  table compatibility remain unqualified.
- `databricks cluster-policies list` returned exactly one policy,
  Personal Compute `001654FA4AA08D1A`. Its live definition fixes
  `cluster_type=all-purpose`, `num_workers=0`, and `runtime_engine=STANDARD`;
  allows `Standard_D4ds_v5` (also its default); leaves `spark_version`
  unrestricted with default `auto:latest-ml`; allows data security modes
  `DATA_SECURITY_MODE_AUTO`, `DATA_SECURITY_MODE_STANDARD`, `USER_ISOLATION`,
  `DATA_SECURITY_MODE_DEDICATED`, `SINGLE_USER`, `LEGACY_SINGLE_USER`, and
  `LEGACY_SINGLE_USER_STANDARD` with AUTO as default; and has a 4,320-minute
  default auto-termination value that is optional/unlimited. This is policy
  metadata, not proof of a runnable cluster/access-mode/runtime combination.
  Because `cluster_type` is fixed to all-purpose, this policy cannot provide
  TD-009's proposed Jobs-cluster path as-is. `databricks cluster-policies
  get-permissions 001654FA4AA08D1A` was denied: the caller lacks Manage
  permission on the policy, so its ACL and entitlement details were not
  established.
- `databricks catalogs get client_dev` returned a managed, isolated catalog
  owned by `db-aidev-admins`, with predictive optimization inherited as
  `ENABLE`. `databricks grants get CATALOG client_dev` showed
  `db-aidev-users` has `CREATE_SCHEMA`, `USE_CATALOG`, and other catalog
  privileges. The authenticated principal's group listing included
  `db-aidev-users`. This establishes catalog-level CREATE_SCHEMA metadata;
  no new schema was made, so resulting schema ownership and drop authority
  remain unverified. Object cleanup rights also remain unverified.
- `databricks warehouses list` returned the shared `data-gateway` warehouse
  stopped and the prior `umf-typed-sdk-qualification-20261016` warehouse
  stopped. Neither is a dedicated admitted endpoint; neither was started.
  `databricks jobs list` returned existing jobs, which does not establish
  general job create/run/cancel authority. No cluster existed to inspect for
  cluster permissions. `databricks schemas list client_dev` returned existing
  schemas. A grants read for `client_dev.default` failed because that schema
  does not exist.
- No billing usage or actual DBU consumption was queried. The retained public
  retail rate estimate is not a conservative workspace invoice bound and does
  not settle billing increments, startup/idle time or ancillary charges.

## Admission result

The cheapest candidate visible in metadata remains one non-Photon
D4ds_v5 all-purpose node for one A1 slot, but it is a separate design variant
from TD-009's Jobs proposal and is not admitted for execution. Before schema
creation or compute start, obtain a reviewed/adopted design path and evidence of
policy/cluster/job entitlements, create/cancel/terminate permissions, schema and
object cleanup authority, quota, an allowed selected runtime identifier and
Unity Catalog access mode, and a reliable whole-trial cost bound including
cleanup below the $100 hard ceiling. After an admitted, bounded compute start,
read back the exact runtime/build and native compatibility; before A1, verify
managed DDL and table features on the selected runtime. Preserve
predictive optimization. The $10 preference remains a tracking target; no
claim is made that the retail estimate enforces either limit. Existing R03,
source custody, external live pin/ACK and managed campaign prerequisites in the
[A1 constraints](managed-a1-correctness-trial-constraints.md) also remain open.

This is advertised metadata and permission discovery only. It does not qualify
the runtime or satisfy the prerequisites for schema/compute effects, so the
no-launch status remains.
