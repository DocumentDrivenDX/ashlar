---
ddx:
  id: ashlar.evidence.r29-truss052-native-attempt-reconciliation-20261101
  type: evidence
  activity: build
  status: recorded
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: informed_by
    - id: TD-004
      kind: informed_by
    - id: ashlar.evidence.r21-truss052-fresh-source-custody-20261101
      kind: informed_by
---

# R29 Truss 0.0.52 stopped-attempt reconciliation

This read-only reconciliation preserves the R29 attempt after the worker stopped
at `NativeDriver.__init__` with `PermissionError: Explicit registered ordinary
source session owner required`. The cause is a product profile-classification
omission: `TRUSS_CURRENT_052_CANDIDATE_GRAPH_PROFILE` was imported but omitted
from the constructor's Truss and typed-current allowlists. The native
`TrussFeedSessions` owner was consequently classified as an ordinary outbox
session. The source/public API itself had already been read successfully.

The retained source reconciliation binds the selected page digest
`942a325702683382c99bd92c69a1af6478cf04879fee0f9d8db4cd4e80f8a9a0` and prior
cursor `9afcb17ae6f64d065c0bc11f25c2f111c43b95ade0bd20a354a568001a91ad4a`.
It is a zero-change prerequisite page: two catalog prerequisites, one revision,
no source registrations or reservations, and no entity changes. The source
receipt says `acknowledgement_attempted:false`; the failure receipt says
`source_ack_attempted:false`. No ACK receipt exists. Acceptance, consumer
enrollment, and genesis application were all retained and not replayed.

The Delta inspection is read-only. All six roles have exactly versions 0 and 1;
v0 creates each table and v1 is `SET TBLPROPERTIES` for the local installation
reservation. Each log has zero AddFile and RemoveFile actions, and every role has
zero Parquet files. The immutable SQLite read shows one `local_installation`
row and zero rows in `local_operation`, `local_plan`,
`local_publication_artifact`, and `local_source_plan`. Thus R29 left local table
installation/reservation metadata only; it did not publish graph/page data or
create a publication receipt. The R29 Delta root is preserved and must not be
reused by a follow-up attempt.

The supervisor receipt records worker exit 1 after 16.43 seconds, peak supervised
RSS 1,272,512,512 bytes, no remaining process group, and absent PostgreSQL
cluster after cleanup. Its `effects_may_remain:true` is conservatively preserved;
this read-only reconciliation separates observed Delta metadata effects from
process custody and does not rewrite that historical receipt. A future bounded
attempt may use the retained R24 stopped Truss source as its source input only
after fresh public preflight, with a new output/Delta root and without replaying
acceptance, consumer enrollment, or the committed genesis.

[`r29-readonly-reconciliation.json`](r29-readonly-reconciliation.json) contains
the six log hashes/actions, SQLite row counts, receipt hashes, exact source page
binding, and supervisor disposition. Copied original receipts and the failure
trace are retained in this directory; the original native attempt remains at
`/private/tmp/ashlar-r29-truss052-page-sequence-loopback-20261101` and its
supervisor directory. No PostgreSQL, Spark, Delta writer, cloud, Truss mutation,
or ACK call was made during reconciliation.
