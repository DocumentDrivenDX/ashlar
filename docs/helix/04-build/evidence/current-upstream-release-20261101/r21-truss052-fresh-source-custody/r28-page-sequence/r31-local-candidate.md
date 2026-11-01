---
ddx:
  id: ashlar.evidence.r31-truss052-six-role-selector-candidate-20261101
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
    - id: ashlar.evidence.r30-truss052-selector-mismatch-20261101
      kind: informed_by
---

# R31 fresh-root local candidate

R31 retains the stopped R24 local Truss source and uses new output and
supervisor roots under `/private/tmp/ashlar-r31-truss052-page-sequence-20261101`.
Its pending scope is [`r31-host-scope.json`](r31-host-scope.json). The R30
installation root is not reused. The candidate changes only the selected
transport admission: exactly six known `.4` installation roles are required,
and only the four graph data roles bind into the publication vector. All six
target paths and UUIDs are frozen at selection and rechecked during native
readback.

The focused source-custody selector suite passed 13/13. The actual worker and
supervisor preflight passed against the R31 scope and current source bytes; it
created neither R31 output nor supervisor root and opened no database, Spark,
Delta, cloud, or ACK path. The [preflight receipt](r31-effectfree-preflight-receipt.json)
binds the exact scope, worker, and supervisor hashes. R31 is pending independent
review and has not been dispatched. Its scope is one bounded local-development
attempt with the existing 900-second, 2 GiB process-RSS, and 512 MiB attempt-root
limits; no production source authority is asserted.
