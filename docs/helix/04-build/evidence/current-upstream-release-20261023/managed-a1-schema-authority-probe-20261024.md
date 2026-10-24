---
ddx:
  id: ashlar.evidence.managed-a1-schema-authority-probe-20261024
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
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
---

# Managed A1 empty-schema authority probe

Observed 2026-10-24 through the authenticated `aidev-cus` CLI after renewed
SSO. This was a single, bounded Unity Catalog schema-permission probe. It did
not create or start a job, cluster, warehouse or paid compute, and it did not
create a table or mutate source state.

## Procedure and result

The target `client_dev.ashlar_a1_auth_probe_c7f8b6` was confirmed absent before
the probe. The CLI created the empty schema; Unity Catalog returned schema ID
`7fe0131f-eceb-43e6-aec4-66da49daceea` and owner
`erik.labianca@synaptiq.ai`. An immediate schema read returned the created
object. The caller deleted it without `--force`, then confirmed the schema was
absent. No probe object remains.

This demonstrates create, read and delete authority for an empty schema owned by
the caller in `client_dev` under the observed identity and session. It does not
demonstrate table or other child-object deletion, non-empty schema cleanup,
cluster or Jobs authority, or general A1 launch readiness.

## Predictive optimization observation

The schema property remained `INHERIT`; no manual predictive-optimization
toggle was issued. The create response reported effective predictive
optimization `DISABLE`, while the immediate readback reported `ENABLE`. Record
this disagreement as unresolved; it is not evidence that optimization was
intentionally disabled or that the initial response can be disregarded.

## Admission boundary

The probe resolves only empty-schema create/read/delete authority. Policy
`CAN_USE`, cluster-create entitlement, exact runtime/build and Unity Catalog
access-mode compatibility, Jobs and cluster lifecycle permissions, quota,
object cleanup, live source/pin custody, and a conservative whole-trial cost
bound remain open. The $100 hard total ceiling and $10 preferred total remain
in force. This probe does not amend TD-009 or authorize compute or the A1 trial.
