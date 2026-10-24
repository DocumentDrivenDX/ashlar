---
ddx:
  id: ashlar.evidence.managed-a1-authenticated-discovery-20261024-b
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
    - id: ashlar.evidence.managed-a1-authenticated-discovery-20261024
      kind: informed_by
---

# Managed A1 authenticated discovery refresh: compute and permissions

Observed 2026-10-24 with the authenticated `aidev-cus` CLI profile. This
refresh is limited to read-only workspace metadata and permission checks. No
cluster, job, schema or warehouse was created, changed or started; no compute
ran and no spend was initiated by these checks.

Queries used `databricks clusters list`, `clusters list-node-types`,
`clusters spark-versions`, `cluster-policies list/get/get-permissions`,
`jobs list`, `current-user me`, `catalogs get client_dev`,
`grants get CATALOG client_dev`, `warehouses list`, `schemas list client_dev`,
`account budgets list`, and `account billable-usage download 2026-10 2026-10`,
all with the `aidev-cus` profile. Only `get-permissions` on the Personal
Compute policy and the two account billing reads failed; the captured outputs
were permission denied and `Not Found`, respectively.

## Observations

- The CLI profile validated against the workspace. `clusters list` returned no
  clusters. Node metadata still lists `Standard_D4ds_v5` as nondeprecated with
  four cores and 16,384 MiB memory. `clusters spark-versions` lists
  `19.x-scala2.13`, described as Spark 4.2.0 and Scala 2.13. These are
  selectable metadata, not an admitted or executed runtime/build.
- `cluster-policies list/get` returned exactly the existing Personal Compute
  policy `001654FA4AA08D1A`. Its definition fixes `cluster_type=all-purpose`,
  `num_workers=0`, `runtime_engine=STANDARD`, and single-node profile; permits
  D4ds_v5; leaves `spark_version` unrestricted with `auto:latest-ml` as its
  default; allows several UC access modes with `DATA_SECURITY_MODE_AUTO` as
  the default; and leaves 4,320-minute auto-termination optional. This does
  not demonstrate that this caller can use the policy for the proposed A1
  shape or that any runtime/access-mode combination is allowed. The
  read-only `get-permissions` request was denied because the caller lacks
  Manage permission on this policy.
- `jobs list` returned existing jobs, but no check established general job
  create, run, cancel or attach-to-existing-cluster permissions. No cluster
  exists for a read of cluster-specific permissions. A Jobs task using an
  all-purpose cluster therefore remains unadmitted. The policy's fixed
  `cluster_type=all-purpose` also remains incompatible with TD-009's proposed
  Jobs-cluster creation path as-is.
- `current-user me` confirms the caller belongs to `db-aidev-users`.
  `catalogs get client_dev` and `grants get CATALOG client_dev` confirm the
  managed isolated catalog, `CREATE_SCHEMA`/`USE_CATALOG` through that group,
  and inherited predictive optimization `ENABLE`. User entitlement detail
  and schema/object drop authority remain unknown; the read-only user detail
  endpoint is admin-only. No schema was created.
- `warehouses list` returned only the stopped shared `data-gateway` and the
  stopped prior qualification warehouse. Neither is a dedicated admitted
  endpoint, and neither was started. Account budget listing and billable-usage
  download returned `Not Found` from this workspace-scoped CLI profile. This
  does not establish that no account budget or usage data exists; no current
  actual usage, DBU consumption, invoice increment, or reliable full-trial
  cost bound was obtained.

## Admission result

The existing metadata continues to expose a D4ds_v5 all-purpose candidate,
but it does not establish policy permission, exact runtime/build and UC mode,
cluster or Jobs lifecycle authority, cleanup rights, quota, or cost. The $10
preference and $100 hard total ceiling in the [A1 constraints](managed-a1-correctness-trial-constraints.md)
remain unadmitted: no account-level usage/budget result or conservative total
bound including startup, idle time, cleanup and ancillary meters was
available. Continue to preserve predictive optimization. No A1 execution,
schema, publication, source mutation or ACK is authorized by this discovery;
the R03/source-holder and managed-runtime requirements in the constraints
remain open.
