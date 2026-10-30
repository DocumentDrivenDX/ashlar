---
ddx:
  id: ashlar.evidence.managed-a1-ashlar-dev-readonly-discovery-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
      kind: informed_by
    - id: TD-009
      kind: informed_by
---

# Managed A1 ashlar-dev read-only discovery

Observed 2026-10-29 with the authenticated `ashlar-dev` Databricks CLI profile.
This is metadata-only evidence; no cloud object, job, or compute was created or
started.

The profile resolves to Azure workspace host
`https://adb-7405619521761591.11.azuredatabricks.net`, workspace ID
`7405619521761591`. `databricks catalogs list` returned no `client_dev` catalog;
the managed catalog named `dev` has predictive optimization `INHERIT` with
effective value `ENABLE`. The effective catalog grant read returned
`CREATE_SCHEMA` and `USE_CATALOG` for `account users`; this is scoped to `dev`
and says nothing about a `client_dev` schema.

`databricks cluster-policies list` returned three policies and no dedicated Jobs
policy. The two classic policies are both `cluster_type=all-purpose` and fixed
at zero workers with `spark.databricks.cluster.profile=singleNode`; the default
Personal Compute policy allows `auto:latest-ml`, while Personal Compute
(capped) allows only `15.4.x-scala2.12` and its CPU-ML variant, fixes Photon,
and permits only `Standard_D8ds_v5` or `Standard_D16ds_v5`. Dev Serverless is
serverless compute. These results provide no policy-conformant dedicated Jobs
cluster route for the packet's A1 trial. The policy-permissions reads for both
classic policies failed with “does not have Manage permissions”; effective
`CAN_USE` remains unproven.

`databricks jobs list` returned one unrelated job,
`tablespec-northwind-demo-us039` (ID `1016486615934960`); no A1 job was listed.
This does not establish create/run/cancel permissions or workspace quota. No
trial-attributable usage or cost meter was read, so prior spend and the total
cost ceiling remain unproven. Runtime listing advertised `19.x-scala2.13`
(Spark 4.2.0), but no observed policy admits it here; wildcard runtime metadata
does not establish an exact maintenance build. No access mode was selected or
validated.

## Disposition

The `ashlar-dev` workspace did not expose the packet's `client_dev` catalog to
this principal, and its observed classic policies do not provide dedicated Jobs compute. A1
remains unadmitted on this workspace. No conclusion about the different
`aidev-cus` workspace is drawn from these reads.
