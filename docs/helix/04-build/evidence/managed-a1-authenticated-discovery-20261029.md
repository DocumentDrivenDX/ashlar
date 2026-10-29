---
ddx:
  id: ashlar.evidence.managed-a1-authenticated-discovery-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
    - id: ashlar.evidence.managed-a1-compute-policy-refresh-20261029
      kind: informed_by
---

# Managed A1 authenticated read-only discovery

Observed 2026-10-29 through the already-authorized Databricks CLI profile
`aidev-cus`. These are metadata observations only. They do not admit a trial,
qualify execution or authorize resource creation.

## Commands and observations

- `databricks auth describe -p aidev-cus -o json` — success; profile resolves
  to Azure workspace `https://adb-7405607548213398.18.azuredatabricks.net`,
  workspace ID `7405607548213398`, principal
  `erik.labianca@synaptiq.ai`. Authentication uses secure OS-keyring storage;
  no token or credential material is included here.
- `databricks current-user me -p aidev-cus -o json` — success; active principal
  is `erik.labianca@synaptiq.ai`. Returned group membership includes direct
  `db-aidev-users` and `db-aidev-admins` and indirect `admins`. Membership is
  recorded as identity context, not as proof of an entitlement or permission.
- `databricks clusters spark-versions -p aidev-cus -o json` — success. The
  workspace advertises `19.x-scala2.13` (Spark 4.2.0), plus 18.x/18.2.x,
  17.3.x, 16.4.x, 15.4.x, 14.3.x and 13.3.x families. These wildcard runtime
  keys are not exact maintenance-build identifiers and do not establish the
  Job Compute policy allows a selected build or that Ashlar is compatible.
- `databricks cluster-policies get 0016F2019AE68DFD -p aidev-cus -o json` —
  success. Policy `Job Compute` is the default Job policy, with
  `cluster_type=job`; runtime defaults to `auto:latest-lts` and is configurable;
  node type defaults to `Standard_DS3_v2` and is configurable; worker count
  defaults to 4 and is configurable. It forbids
  `spark_conf.spark.databricks.cluster.profile`, instance pools and driver
  instance pools. Availability defaults to `SPOT_WITH_FALLBACK_AZURE`; spot
  bid max price is fixed at 100. A requested one-worker configuration is
  metadata-permitted by the unlimited worker rule, but no proposed full
  configuration was submitted or validated.
- `databricks cluster-policies get-permissions 0016F2019AE68DFD -p aidev-cus
  -o json` — success. The returned ACL lists inherited `CAN_USE` for group
  `admins`. The identity response lists the principal as an indirect `admins`
  member. This does not prove effective policy use or cluster-create authority
  for the principal; no cluster was submitted.
- `databricks clusters list-node-types -p aidev-cus -o json` — success; locally
  filtered the read-only result to `Standard_D4ds_v5` and `Standard_DS3_v2`.
  Both are listed, non-deprecated, four-core general-purpose node types. Node
  availability metadata does not prove the proposed policy/node/runtime/access
  mode tuple is launchable or cost-admitted.
- `databricks catalogs get client_dev -p aidev-cus -o json` — success. Catalog
  is a managed catalog, isolated, owner group `db-aidev-admins`, and comment
  identifies fake rehearsal data. Predictive optimization is `INHERIT`, with
  effective value `ENABLE` inherited from the metastore. No setting was
  changed.
- `databricks grants get-effective catalog client_dev --principal
  erik.labianca@synaptiq.ai -p aidev-cus -o json` — success. The response lists
  `ALL_PRIVILEGES` for `db-aidev-admins`; for `db-aidev-users` it lists
  `CREATE_SCHEMA`, `USE_CATALOG`, `USE_SCHEMA`, and the returned table,
  function, volume and execution privileges. `db-aidev-users` is direct group
  membership in the identity response. The output is catalog grant metadata;
  it does not establish schema ownership/drop authority for a future schema or
  cleanup permission for a specific object.
- CLI help was read for `databricks jobs get-permissions` and
  `databricks account billable-usage download`. No job-permission request was
  made because there is no admitted target job; Jobs create/run/cancel
  permissions and workspace quota remain unproven. The billing command is an
  account-level month-range usage-log download, not a scoped live trial meter;
  it was not run. No current DBU-consumption or all-in cost meter was observed.

## Admission result and limits

The read-only facts improve the Jobs candidate metadata: the target policy,
advertised runtime families, node types, catalog state and catalog grants are
now confirmed for the selected profile. They do not establish an exact
maintenance runtime identifier, UC access mode, effective principal policy
use, Jobs creation/run/cancel or termination permissions, quotas, schema
ownership/drop cleanup, actual DBU consumption, billing increments, ancillary
costs or a conservative total below the user's $100 ceiling (with $10
preference). Public retail arithmetic in the admission packet remains
conditional, not a cost cap.

No schema, job, cluster, warehouse or table was created; no query or job was
run; no compute was started; no predictive optimization setting was changed;
and no cloud spend was incurred by this discovery. The existing source/ACK
holder, accepted seed and independent B-source gates remain separate blockers.
Do not launch from this metadata receipt. Reopen admission only after the
remaining runtime/access-mode, entitlement, cleanup, cost and source-custody
evidence is independently attached and reviewed.
