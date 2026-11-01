---
ddx:
  id: ashlar.evidence.r32-truss052-report-revision-mismatch-20261101
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

# R32 local Truss report revision mismatch

R32 started and stopped its isolated PostgreSQL copy and Spark runtime, read the
retained public installation/report/document and selected the same first feed
page, then stopped in `select_truss_local_dev_source_custody_anchor`. The worker
validated the report's exact retained R24 digest and the Truss .52 report
contract (`revision` is JSON text), then constructed
`CatalogRegistration.revision` as an integer. The selector compared the JSON
string directly with that integer and rejected it. The fix compares the report
revision with the string form of the public registration revision; it does not
relax any registration, document, page, cursor or request binding.

The R32 Delta root has only installation reservations and v0/v1 metadata in
each of its six roles, with zero Parquet files. SQLite contains one
installation record and zero operations, plans, publication artifacts or
source plans. The source page was read but not ACKed. A publication-intent and
source-page copy were written locally before the selector refused; those files
are not a Delta data publication. The supervisor records that the PostgreSQL
cluster and process group stopped, while its effects flag remains conservative
because six-role installation metadata exists. See the [reconciliation
receipt](r32-readonly-reconciliation.json).

R33 uses a fresh output and supervisor namespace, retains the immutable R24
source and the unchanged worker, and binds the corrected selector. The pending
scope is [`r33-host-scope.json`](../r33-host-scope.json). No R33 native attempt
was run.
