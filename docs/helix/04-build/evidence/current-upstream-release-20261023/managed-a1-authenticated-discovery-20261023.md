---
ddx:
  id: ashlar.evidence.managed-a1-authenticated-discovery-20261023
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
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-002
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
---

# Managed A1 authenticated read-only discovery

Observed 2026-10-23 through the approved SSO `aidev-cus` profile, which was
valid outside the sandbox. The authenticated workspace host was
`adb-7405607548213398.18.azuredatabricks.net`; the authenticated principal was
`erik.labianca@synaptiq.ai`. This records discovery only. No schema, job or
compute mutation was made, and no trial was launched.

The approved `client_dev` catalog has effective predictive optimization
`ENABLE` inherited from the metastore. The workspace advertised
`19.x-scala2.13`; `Standard_D4ds_v5` was available with four cores and 16 GiB.
The only accessible compute policy was Personal Compute
`001654FA4AA08D1A`. Its verified policy fields fix `cluster_type` to
`all-purpose` and `num_workers` to `0`, require the Standard engine, include
`Standard_D4ds_v5` among allowlisted node types, and leave `spark_version`
unrestricted. These are policy metadata only: the proposed dedicated one-node
Jobs cluster under TD-009 is not proven available and cannot use this policy
as-is. A separately reviewed policy variant or other admitted Jobs-compute
path, its exact node/runtime/access-mode compatibility, and the applicable
principal entitlements and create/cancel/termination permissions remain
unconfirmed. The read-only `users get` result did not expose entitlement detail
because that detail is admin-only. No schema or compute was created; no trial
was launched. Using an all-purpose cluster would be a separately reviewed
design and cost variant, distinct from TD-009's Jobs proposal.

An effective catalog-grant response for the authenticated principal included
`CREATE_SCHEMA` and `USE_CATALOG` through `db-aidev-users`, plus other catalog
privileges; membership in `db-aidev-users` was confirmed. Schema ownership and
authority to drop the trial schema or its objects remain unconfirmed. No
clusters were listed.
The only running SQL endpoint was the shared data-gateway; the older UMF
qualification warehouse was stopped. No dedicated Ashlar SQL endpoint was
available in this discovery.

## Remaining gates

This discovery does not admit execution. Actual R03 Truss migration/resume,
genesis publication, verification, ACK and seed readback remain prerequisites.
TD-009's offline, non-authoritative Spark reopen and bounded source-prefix
comparison slices are implemented and covered by fake-session tests. No native
managed Spark/Delta execution has run. Managed A1 campaign integration and the
external live source/read/pin holder and ACK custody interval remain missing.
The advertised runtime has no native Ashlar/Weft/UMF or managed Delta qualification;
the exact runtime build, access mode at execution, realized table protocol and
features still require bounded native admission. Current account billing,
DBU consumption, billing increments and a conservative total cost bound remain
unavailable, so the $10 preference and $100 hard ceiling are not yet admitted.

See the [offline trial constraints](managed-a1-correctness-trial-constraints.md)
for the conditional compute shape, bounded A1 sequence and cleanup requirements.
