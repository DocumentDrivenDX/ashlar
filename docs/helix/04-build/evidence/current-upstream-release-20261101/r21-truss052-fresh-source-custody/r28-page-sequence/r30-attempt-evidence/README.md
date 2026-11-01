---
ddx:
  id: ashlar.evidence.r30-truss052-selector-mismatch-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: TD-004
      kind: informed_by
---

# R30 local Truss selector refusal

The R30 worker stopped before a graph publication because the local source
selector required a four-key `LocalDeltaTransport` registry. The actual
selected `.4-xxhash64` transport is the exact six-role installation registry:
`object_current`, `edge_current`, `tombstone`, `whole_source_history`,
`attempts`, and `manifest`. The publication vector and native graph row
readback still contain only the four data roles. R30 used the immutable R24
source copy; it did not repeat acceptance, consumer enrollment, or the genesis
group.

Read-only inspection found two metadata-only Delta log versions in each of the
six roles, zero Parquet files, and no operations, plan, publication artifact,
or source plan rows in SQLite. The attempt retains a publication-intent file
and exact public source-page copy. Its failure receipt reports no Truss ACK.
The supervisor reports an empty worker process group and an absent, stopped
owned PostgreSQL cluster. These installation reservations and metadata logs
remain; the R30 Delta root is not reusable. Exact file and log hashes are in
the [reconciliation receipt](r30-readonly-reconciliation.json), reproducible
with [the read-only reconciler](reconcile_r30_readonly.py).

The selector fix requires exactly the six known installation targets, freezes
their path and UUID bindings, and separately freezes only the four data-role
targets for the publication vector. It refuses both four-role-only transport
maps and arbitrary extra roles. The focused selector suite passes 13/13. A
fresh R31 scope uses a new output and supervisor root; its actual worker and
supervisor effect-free preflight passed. R31 remains local-development scope,
unreviewed and undispatched. No cloud, ACK, or production authority is claimed.
