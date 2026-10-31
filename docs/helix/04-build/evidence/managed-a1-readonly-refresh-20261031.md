---
ddx:
  id: ashlar.evidence.managed-a1-readonly-refresh-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# A1 `aidev-cus` read-only refresh — 2026-10-31

The previously authorized `aidev-cus` SSO login was refreshed. Read-only CLI
calls confirmed that the selected workspace exposes the `client_dev` managed
catalog. Its effective predictive optimization setting is `ENABLE`, inherited
from the metastore. The authenticated principal belongs to `db-aidev-users`;
the catalog's effective grants to that group include `CREATE_SCHEMA`,
`CREATE_TABLE`, `MODIFY`, `SELECT`, `USE_CATALOG`, and `USE_SCHEMA`.

`clusters list` returned no active or stopped clusters visible to this
principal. `cluster-policies list` returned only `Personal Compute` (ID
`001654FA4AA08D1A`). That policy fixes `cluster_type` to `all-purpose`,
`num_workers` to zero, and the runtime engine to `STANDARD`, with default node
type `Standard_D4ds_v5`. This does not establish that a dedicated Jobs cluster
can be created or that its selected Spark/Delta runtime, Unity Catalog access,
retention holder, and total cost satisfy the release gate.

No schema, cluster, job, table, or paid compute was created or started. The
production-target trial remains held pending the separately admitted source,
retention, runtime and bounded Jobs-compute plan.
