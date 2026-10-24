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
escalated host session. This was read-only admission discovery. No schema, job,
cluster or warehouse was created or started; no paid compute ran.

## Results

- The caller read Personal Compute policy `001654FA4AA08D1A`, whose fixed
  all-purpose, zero-worker, Standard-engine shape and D4ds_v5 allowance are
  retained in the [authenticated discovery refresh](managed-a1-authenticated-discovery-20261024-b.md).
- `cluster-policies get-permissions` was denied because the caller lacks
  Manage permission on that policy. The policy's `CAN_USE` access for this
  caller or its groups is therefore unproven; denial is not evidence that
  `CAN_USE` is absent.
- `current-user me` returned the caller identity but no entitlements.
  `allow-cluster-create` is unproven. No user or group permission change was
  requested or made.
- Prior read-only catalog metadata shows `CREATE_SCHEMA` and `USE_CATALOG` on
  `client_dev` through `db-aidev-users`, with predictive optimization inherited
  as `ENABLE`. This does not prove resulting schema ownership, object-drop
  authority, or cleanup rights. Predictive optimization remains enabled.

## Admission result

The dedicated A1 path remains unadmitted. The policy permission and cluster
creation entitlement are unresolved, as are exact runtime/build and Unity
Catalog access-mode compatibility, task and cluster lifecycle permissions,
quota, and schema/object cleanup authority. The all-purpose price observation
is not a whole-trial bound. Live source/pin custody through independent
verification, protected ACK, reconciliation and closing also remains a gate.

The owner limits remain **$100 hard total** and **$10 preferred total**. No
reliable conservative total including startup, idle time, ancillary usage and
cleanup has been established. Do not create the temporary schema or start
compute until Sol reviews admission and source-holder custody is available.
TD-009 remains a draft; this receipt changes neither its proposal nor launch
authority.
