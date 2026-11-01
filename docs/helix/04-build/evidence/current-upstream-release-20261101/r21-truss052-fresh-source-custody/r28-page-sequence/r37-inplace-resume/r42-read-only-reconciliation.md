---
ddx:
  id: ashlar.evidence.r42-read-only-reconciliation-20261101
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

# R42 committed-page readback reconciliation

R42 committed the seven-change seed publication to the retained local Delta
destination, then stopped before source ACK when Ashlar's native row-bag check
reported a mismatch. The mismatch was in timestamp decoding, not in stored
Delta values. The original R42 failure receipt remains immutable and records
`source_ack_attempted: false`; its durable `publication-readback-pending` record
also says `acknowledgement: not_attempted`. No Truss call, destination write,
republication, or ACK occurred during this reconciliation.

The bounded Spark 4.2 `versionAsOf` audit read the committed vector from
`/private/tmp/ashlar-r36-truss052-page-sequence-20261101/delta`: object_current
v2, edge_current v2, whole_source_history v2 and tombstone v1. Under
`spark.sql.session.timeZone=UTC`, Spark `unix_micros(published_at)` returned
`1793544477944899` for every object and edge row. That is the manifest and
publisher-plan instant, `2026-11-01T14:47:57.944899Z`. The initial Python reader
returned `1793526477944899`, exactly five hours earlier. It collected Spark
TIMESTAMP into a naive Python `datetime`, then `_native_cell` labeled that value
UTC; PySpark's host-local conversion had already shifted the wall clock.

The four objects, three edges, seven history events and empty tombstone are
present at the committed versions. Reconstructing expected object/edge rows
from the immutable SQLite effect plan and using the Spark-verified UTC
microseconds yields exact row-bag matches: object_current
`04858ed3279b2b6f15311b9ab73e27bc2d225b60c15a4b7706b8917bca07555e`,
edge_current `de2569fd0cafc1995666ae02119916022f5df06c7873f876b590497a80416821`.
The history bag is `3f8b27096a8e421055bb88a9f6f9956c9bd7d2052bed24c68561dbd21696825e`;
tombstone is empty. The main SQLite journal passes `PRAGMA integrity_check`
with 15 operations, two plans, two source plans and two applied publication
artifacts. The retained Delta root now includes R42's committed versions and
must not be treated as the old R36 baseline.

Ashlar's local native reader now projects each TIMESTAMP column to Spark
`unix_micros` before collection and treats that projection as BIGINT. The
focused regression models a host-local naive datetime and verifies the UTC
instant survives as exact epoch microseconds. The focused local-development
custody tests also pass. This fixes observation only; it neither edits nor
replays the committed publication and does not authorize ACK.

Recovery must reopen the exact retained PGDATA and Delta paths because the
SQLite journal and target registry bind those absolute paths. It should verify
the pending receipt and original `publication-intent` hashes, then restore the
already-applied artifact with `restore_applied_truss_publication` in a live
`NativeDriver`. That helper reconstructs the source plan and `policy.active`
from the existing journal/artifact and does not execute SQL effects. In that
same process, mint `ExpectedNativePostState` from the restored active request
and `following` rows, rebuild the anchor only from the newly observed exact
public `.52` registration, consumer, runtime status and feed page, and reopen
the six-role transport at its bound R36 targets. Read the exact pinned vector
with `read_local_native_delta_publication_observation`, then require
`bind_local_native_publication` to match request, manifest, vector, destination
receipt, intent and every role row bag. No call to `publish_truss_transaction`
or continuation-page publisher is permitted during this recovery.

Only after that binding succeeds should the worker write a separate
`publication-readback-complete-<publication-id>.json` using the private
O_EXCL/no-follow, fsync pattern. It must reference the immutable pending
receipt SHA and all native vector/log/row-bag evidence; it must not overwrite or
delete the pending receipt. Then start a fresh same-session Truss `.52`
source-custody lifecycle: obtain fresh registrar `CatalogRuntimeStatus`, open
the selected source handle, reread the exact selected page on that held session,
renew, invoke one protected ACK with the retained destination receipt, reconcile
in a new read-only transaction on that same session, and close only after a
matched outcome. If any binding or reconciliation differs, stop without ACK
and preserve both markers. This recovery has not been executed; the source
cursor was not read during this audit.

Machine-readable details and hashes are in
[r42-read-only-reconciliation.json](r42-read-only-reconciliation.json).
The timestamp reader change is in
[`truss_source_custody_selection.py`](../../../../../../../../src/ashlar_host/truss_source_custody_selection.py)
with its focused test at
[`test_truss_local_native_timestamp_readback.py`](../../../../../../../../tests/test_truss_local_native_timestamp_readback.py).
