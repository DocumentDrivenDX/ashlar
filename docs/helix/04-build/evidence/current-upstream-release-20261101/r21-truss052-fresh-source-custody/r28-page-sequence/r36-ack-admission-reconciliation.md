---
ddx:
  id: ashlar.evidence.r36-protected-ack-admission-reconciliation-20261101
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
    - id: CONTRACT-003
      kind: governed_by
---

# R36 protected ACK continuation admission reconciliation

R36 completed and independently verified one local-development genesis
publication, then protected-ACKed that exact page through Truss 0.0.52 source
custody. The retained ACK reconciliation is `matched`, binds publication
`truss-graph-cbf69ea847e30930ec586b2a3d508745f0430da3e9e1628f6b81149b9d58230b`,
page SHA-256 `942a325702683382c99bd92c69a1af6478cf04879fee0f9d8db4cd4e80f8a9a0`,
and advanced checkpoint SHA-256
`8e370e1497197b93029ff7f5287d9102802a098fbc4ea02b1e6bf20339841f9f`. Native
Delta readback binds all four data roles at version 1 with zero rows; the
receipt and vector identities match the Truss ACK receipt.

Continuation stopped before reading page two with `Original durable Truss ACK
attempt required`. A read-only SQLite census of the retained destination
journal found six committed operation rows, one publication artifact, one
source plan, and a single `local_plan` row for `effects:<request-digest>`; there
was no `truss-ack:<request-digest>` row. This is expected for R36's protected
ACK path: `Truss052SourceCustodySession` performs the ACK outside
`NativeDriver.acknowledge`, so the older inline-ACK intent writer is not called.
The continuation gate incorrectly required that inline-only intent even though
the later flow verifies the protected receipt through Truss's public
reconciliation API.

The narrow correction in `truss_ack_journal.py` admits the no-local-intent case
only for a typed `TrussFeedSessions` instance explicitly configured for
destination receipts with a nonempty destination identity. It still verifies
the exact retained native committed attempt, request and manifest, source
admission, and manifest. The existing continuation flow then checks the
advanced checkpoint and performs fresh public Truss receipt reconciliation
before any next-page read. A plain ACK session still requires the exact local
durable intent; mismatching or present-but-different rows remain rejected. No
intent is synthesized, and this change does not itself ACK or claim source
authority.

R36 failure receipt SHA-256 is
`2d54dc548b821c1e7a56e2182341d2d7f23963eda688d1ebeca349b20561a303`; the
supervisor receipt SHA-256 is
`4b85fa6125c68f284befd85e84b88073737df428f5446cfdf175edee7cfbec5e`. The
supervisor reports an empty process group and PostgreSQL cleanup `absent`, with
`effects_may_remain: true` for the retained database/Delta publication. R36
remains preserved at `/private/tmp/ashlar-r36-truss052-page-sequence-20261101`;
no page-two publication or ACK exists. No retry or mutation was performed for
this reconciliation.

Validation: focused `test_truss_ack_journal` passed 2/2. The added test proves
receipt-backed admission does not create a local ACK-intent row while plain
sessions continue to require one.
