---
ddx:
  id: ashlar.evidence.r33-truss052-selector-reconciliation-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: TD-004
      kind: informed_by
    - id: ashlar.evidence.r32-truss052-report-revision-mismatch-20261101
      kind: informed_by
---

# R33 public-document selector reconciliation — 2026-11-01

## Outcome

R33 stopped before Spark/Delta publication or ACK because the local-development
source selector required `artifact.bytesBase64` inside Truss's accepted catalog
report. The actual Truss 0.0.52 public report contains only `artifact.identity`
and `artifact.sha256`; the public `accepted_document_in_transaction` result
carries the document bytes separately. The selector now requires that exact
public `AcceptedDocument`, compares its document ID/revision, artifact identity
and digest, registration ID and catalog revision with the report/registration,
and recomputes SHA-256 over `content`.

The read-only reconciliation ran public status, report, accepted-document and
feed-page reads on the retained stopped R33 clone. The public report SHA is
`466a3c0f4f0223499f9f1ba2e9c4bf6a73d74ddcbbc3b9275c89276e6efd502c`; its
selected document has revision `1`, identity
`ashlar-local-dev-typed-graph-r21-20261101`, and model SHA
`b908c0dd6a0da65c864c0debd0c793d8ab06a933546f34050480909c5c9349f2`. All
public AcceptedDocument/report comparisons passed. The page bytes, prior and
next cursor bytes, consumer registration, request digest/publication ID and
source-page wrapper hash match the retained R33 intent. R33 did not attempt ACK
and has no publication receipt.

The exact selector passed an effect-free call using the public typed report
projection, AcceptedDocument, runtime status and FeedPage, a FeedConsumer built
from the public enrollment identity and exact page prior cursor, and a temporary
six-role typed transport with empty local directories. Truss 0.0.52 has no
read-only consumer lookup. This check did not start Spark, read or write Delta,
open source custody, publish, or ACK.

## Evidence and next boundary

- [`r33-public-source-reconciliation-v3.json`](r33-attempt-evidence/r33-public-source-reconciliation-v3.json) records the complete readback and selector pass; SHA-256 `a5b60ecaed48e688ae159566684e8a8f8d9632dc1194bdbcec77fc5c07b74354`.
- [`r24-public-report-document-projection-v3.json`](r33-attempt-evidence/r24-public-report-document-projection-v3.json) is the compact live-shaped report fixture, with source-report SHA retained; SHA-256 `62ff140371e3c6999b7aef3af44192a08d0edb74f3842246b3271a7ce8a95807`.
- [`r34-host-scope.json`](r34-host-scope.json) points to fresh output/supervisor roots, binds the corrected worker and selector bytes, and remains `pending_sol_review`.
- [`r34-effectfree-preflight-receipt.json`](r34-effectfree-preflight-receipt.json) passed the actual supervisor/worker structural preflight with no output root, PostgreSQL, Spark, Delta write, source ACK or cloud effect.

R34 has not been dispatched. Native Spark, the actual six-role Delta registry,
publication readback, and protected ACK remain unexercised by this correction.
All authority in this packet is local-development authority; no production claim
is made.
