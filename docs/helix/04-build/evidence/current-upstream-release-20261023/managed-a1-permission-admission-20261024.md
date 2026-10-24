---
ddx:
  id: ashlar.evidence.managed-a1-permission-admission-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-authenticated-discovery-20261024-b
      kind: informed_by
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
---

# Managed A1 permission admission receipt

Observed 2026-10-24 through the authenticated `aidev-cus` CLI using the
escalated host session. This receipt combines prior read-only admission
discovery with one bounded empty-schema authority probe. No job, cluster or
warehouse was created or started; no paid compute ran. The temporary schema
was deleted and its absence confirmed. See the [schema authority probe
receipt](managed-a1-schema-authority-probe-20261024.md).

## Results

- Authenticated `current-user me` returned the active caller and direct
  memberships in `db-aidev-users`, `db-aidev-fdle`, and `users`; its response
  contained no entitlements. The admin-only `users get` request was denied, so
  `allow-cluster-create` remains unproven.
- `catalogs get client_dev` returned a managed, isolated catalog with
  predictive optimization inherited as `ENABLE`. `grants get CATALOG
  client_dev` showed `db-aidev-users` with `CREATE_SCHEMA` and `USE_CATALOG`
  (and other catalog privileges). Group membership plus these grants confirms
  catalog-level schema-creation authority. A separate probe then created, read,
  and deleted an empty uniquely named schema under this catalog; its owner was
  the caller and the final absence check passed. This confirms authority for an
  empty owned schema only. No table or other child object was created, so
  object-drop and non-empty-schema cleanup remain unproven.
- The schema probe's `create` response reported effective predictive
  optimization `DISABLE`; an immediate `get` readback reported `ENABLE`. The
  schema property remained `INHERIT`, and no manual toggle was issued. Preserve
  this discrepancy as unresolved evidence for any later managed trial.
- `cluster-policies get 001654FA4AA08D1A` returned the Personal Compute policy:
  fixed `cluster_type=all-purpose`, single-node profile, zero workers and
  Standard engine; `Standard_D4ds_v5` is allowed; Spark version is unrestricted;
  UC access modes are allowlisted with AUTO as the default. This is policy
  definition metadata, not proof that this caller can use the policy or that a
  particular runtime/access-mode combination is allowed.
- `cluster-policies get-permissions 001654FA4AA08D1A` returned an explicit
  denial because the caller lacks Manage permission on the policy. The
  caller's or its groups' `CAN_USE` access is consequently unproven; the
  denial does not establish that `CAN_USE` is absent. The workspace runtime
  list includes selectable metadata such as `19.x-scala2.13`, but does not
  prove policy permission, an exact maintenance build, or compatibility.
- `clusters list` returned no clusters. `warehouses list` showed only the
  stopped shared `data-gateway` warehouse and the stopped prior qualification
  warehouse; neither is a dedicated active endpoint. These checks did not
  establish cluster/job create, run, cancel, or termination permissions.
  No user or group permission change was requested or made.

## Conditional public-price illustration

Microsoft's [Azure Databricks pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/)
lists Premium All-Purpose Compute at `$0.55` per DBU-hour and lists D4ds v5
at 2 DBUs. The retained [Central US VM retail observation](managed-fixed-jobs-retail-observation.json)
is `$0.255` per D4ds_v5 VM-hour. At those public rates, one node-hour is
`$0.255 + (2 × $0.55) = $1.355`; linear 40-minute arithmetic is about `$0.90`
per slot and `$7.23` for eight slots, before ancillary usage. Adding the
assumed campaign-wide `$5` ancillary reserve yields `$12.23`, above the `$10`
preference. These are conditional list-price calculations, not measured
workspace DBU consumption, an account rate, invoice treatment, or a bound. The
reserve is an assumption, not a maximum. The prior workspace-scoped account
budget and billable-usage reads returned `Not Found`; no account budget or
actual-usage evidence was obtained ([authenticated discovery refresh](managed-a1-authenticated-discovery-20261024-b.md)).

## Admission result

The dedicated A1 path remains unadmitted. Catalog-level `CREATE_SCHEMA` and
`USE_CATALOG`, plus create/read/delete authority for an empty owned schema, are
confirmed. Policy `CAN_USE`, cluster-create entitlement, exact runtime/build and
Unity Catalog access-mode compatibility, task and cluster lifecycle permissions,
quota, and table/object cleanup authority remain unresolved. The all-purpose
price illustration is not a whole-trial bound.
Live source/pin custody through independent verification, protected ACK,
reconciliation and closing also remains a gate.

The owner limits remain **$100 hard total** and **$10 preferred total**. No
reliable conservative total including startup, idle time, ancillary usage and
cleanup has been established. The probe schema was deleted. Trial-schema
creation or compute remains held pending Sol's review of admission and
source-holder custody. TD-009 remains a draft; this receipt changes neither
its proposal nor launch authority.
