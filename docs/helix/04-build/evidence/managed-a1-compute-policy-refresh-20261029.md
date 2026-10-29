---
ddx:
  id: ashlar.evidence.managed-a1-compute-policy-refresh-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-permission-admission-20261024
      kind: informed_by
---

# Managed A1 compute policy refresh

Read-only authenticated discovery on 2026-10-29 used the `aidev-cus` profile
at `adb-7405607548213398.18.azuredatabricks.net`. The separate `ashlar-dev`
workspace supplied by the owner is accessible, but its catalog list does not
contain `client_dev`. The selected `aidev-cus` workspace does contain the
owner-designated catalog; its inherited predictive optimization is `ENABLE`.

The current caller, `erik.labianca@synaptiq.ai`, is an indirect member of
`admins` and direct member of `db-aidev-admins` and `db-aidev-users` according
to `databricks current-user me`. The `Job Compute` policy
`0016F2019AE68DFD` is visible. Its read-only policy ACL gives `admins`
inherited `CAN_USE`; the caller's reported membership therefore supports a
candidate policy-use path that the earlier Personal Compute receipt could not
establish. The Jobs policy fixes `cluster_type=job`, forbids instance pools,
defaults to four workers and `Standard_DS3_v2`, and leaves runtime, node type
and worker count configurable. It forbids
`spark_conf.spark.databricks.cluster.profile`; therefore a zero-worker
Spark trial is not admitted by this policy observation. The
[compute configuration reference](https://learn.microsoft.com/en-us/azure/databricks/compute/configure)
says Spark commands fail with zero workers outside single-node compute. Both
`Standard_DS3_v2` and `Standard_D4ds_v5`
appear as available four-core node types. Workspace runtime metadata lists
`19.x-scala2.13` (Spark 4.2.0) as well as 17.3 LTS Spark 4.0.0 and 16.4
LTS Spark 3.5.2 variants; this read did not establish an exact maintenance
build or runtime compatibility with Ashlar.
The [Azure Databricks Runtime 19 release notes](https://learn.microsoft.com/azure/databricks/release-notes/runtime/19)
describe Ubuntu 24.04.4, Python 3.12.3 and Delta Lake 4.2.0 for that runtime;
these published defaults do not replace actual cluster build, native wheel,
Delta feature and UC readback.

These facts make a dedicated Jobs-cluster candidate more concrete than the
previous all-purpose-only observation. They do not prove cluster creation,
the selected UC access mode, actual package/native compatibility, task
permissions, billing rates, or a complete cost bound. `current-user me`
returned no entitlements. No schema, job, cluster, warehouse, table, source
mutation, publication or ACK was created or started in this refresh. The
historical empty owned schema probe was already cleaned up.

For the next A1 admission, select one policy-conformant dedicated Jobs cluster
with explicit node/worker/runtime/access-mode settings and no retries; keep
the $100 total ceiling and $10 preference, including startup, idle and cleanup.
Read back the actual runtime and cost meters before any publication. Preserve
predictive optimization. The Truss source/ACK holder and latest package cohort
must pass their separate gates before an A1 launch. Neither a shared default
cluster nor the stopped shared SQL warehouse is selected.
