---
ddx:
  id: ashlar.evidence.managed-a1-fresh-readonly-admission-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-authenticated-discovery-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-jobs-cost-bound-review-20261029
      kind: informed_by
    - id: TD-009
      kind: informed_by
---

# Managed A1 fresh read-only admission metadata

Observed 2026-10-29 through the authenticated `aidev-cus` profile at
`https://adb-7405607548213398.18.azuredatabricks.net` (workspace
`7405607548213398`). The current principal ID is `3755468182769617`, with direct
groups `db-aidev-fdle`, `db-aidev-users`, `users`, `db-aidev-admins`, and indirect
group `admins`. These were metadata reads only; no resource or compute was
created, started, run, cancelled or changed.

## Compute and grants

`databricks cluster-policies get 0016F2019AE68DFD` confirms the default `Job
Compute` policy fixes `cluster_type=job`, forbids the single-node profile and
instance pools, leaves `spark_version`, `node_type_id`, `num_workers` and Azure
availability configurable, and defaults to `auto:latest-lts`, `Standard_DS3_v2`,
four workers and `SPOT_WITH_FALLBACK_AZURE`. It contains no
`data_security_mode` rule. Its policy ACL grants inherited `CAN_USE` to
`admins`; the caller is an indirect `admins` member, which still does not prove
effective policy use or cluster-create entitlement. No full cluster
configuration was submitted, so no exact runtime/access-mode tuple was
validated.

The current workspace runtime list includes `19.x-scala2.13` (Spark 4.2.0) and
19 Photon/ML variants. These are wildcard keys, not exact maintenance builds.
The policy's unlimited runtime rule does not establish a specific selected
build or Ashlar compatibility. `client_dev` remains a managed catalog with
`INHERIT` and effective predictive optimization `ENABLE`. A fresh effective
grant read for direct group `db-aidev-users` returns `CREATE_SCHEMA`,
`USE_CATALOG`, `USE_SCHEMA`, `CREATE_TABLE`, `MODIFY`, `SELECT`, and other listed
table/function/volume privileges. This does not establish cleanup rights for a
future schema or child objects.

## Jobs, quota and usage

`databricks jobs list` shows two existing `ashlar-scale-20261016-*` Jobs:
`401862977233947` and `972181838495126`. Their sole `graph-layout` runs are,
respectively, run `458676000224358` (success, 92,309 ms) and run
`499115352760373` (failed timeout, 186,408 ms). Both run and task
`cluster_instance` fields are null, and both current Job definitions have no
`job_clusters`, `job_cluster_key`, `existing_cluster_id`, or inline `new_cluster`.
Their relationship, if any, to the A1 correctness campaign and any attributable
spend is unresolved; these records cannot establish compute cost or be
classified as A1 evidence. `databricks clusters list` returned no visible
clusters at the time of the read.

The CLI's `jobs get-permissions` command requires a `JOB_ID`; no target Job has
been admitted or identified for this trial, so it cannot establish future-job
create/run/cancel permissions. The current Jobs list does not establish
workspace quota. The account billable-usage API
read for October 2026 returned `Not Found`, leaving attributable prior spend
and actual usage unavailable. No billing meter or conservative all-in cost
bound was obtained.

## Commands and disposition

Read-only CLI commands used with `--profile aidev-cus`: `auth describe`,
`current-user me`, `cluster-policies get`, `cluster-policies get-permissions`,
`clusters spark-versions`, `catalogs get client_dev`,
`grants get-effective catalog client_dev --principal db-aidev-users`, `jobs
list`, `jobs get`, `jobs list-runs`, `jobs get-run`, `clusters list`, and
`account billable-usage download 2026-10 2026-10`. Personal-data billing output
was not requested; the API returned `Not Found`.

The fresh reads do not close exact build, access-mode, effective create/run/
cancel/terminate, quota, live-holder or total-cost gates. Existing
`ashlar-scale` Job metadata is a new unresolved attribution question for the
campaign-spend check. A1 remains unadmitted. No inference is made about the
separate `ashlar-dev` workspace.
