---
ddx:
  id: ashlar.evidence.a1-managed-truss052-port-adapter-20261102
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: ashlar.evidence.a1-managed-release-bridge-candidate-20261102
      kind: follows
---

# Managed A1 Truss 0.0.52 source/ACK gate — 2026-11-02

## Finding and fail-closed behavior

The public Truss 0.0.52 `Truss052SourceCustodySession` supports a selected
source-custody handle, renew observations, one-shot receipt ACK, fresh exact
reconciliation and close on one retained connection. Its current adapter does
not expose an independently verified remaining lifetime for that handle. Its
`statement_timeout_ms` bounds individual PostgreSQL statements, not the
source-custody lifetime or a whole ACK-plus-reconciliation operation.

`src/ashlar_host/managed_a1_truss052_ports.py` therefore refuses both source
open and ACK/reconciliation. It does not construct a Truss session, obtain a
connection, or attempt ACK. A supplied host monotonic deadline is not presented
as source-custody proof. The source `open` checks the original context identity
before inspecting other inputs or creating any session. This gate remains
unavailable until an independently verified live source-custody lifetime is
admitted and the existing Truss transaction setup is qualified.

Sol's read-only review also found that `Truss052SourceCustodySession._transaction`
executes `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` and then
`SET TRANSACTION READ ONLY` before adopting the requested mode. A selected
PostgreSQL server with `default_transaction_read_only=on` can reject this
sequence before the desired write-mode override. No change was made to that
Truss path in this candidate. A same-transaction `READ WRITE` first-statement
correction and focused native PostgreSQL proof remain required.

The separate owner would still need an issuer-bound A-side
`TrussSourceCustodySelection` matched to exact A/B source state, request,
manifest, receipt and vector. The current A1 genesis selection does not itself
admit that publication-bound source selection.

## Focused checks

Local Python 3.12 verification passed:

`PYTHONPATH=src:tests python3.12 -m unittest -q
tests.test_managed_a1_truss052_ports tests.test_truss052_source_custody
tests.test_managed_a1_release_bridge` — 27 tests.

The three gate tests verify wrong-context refusal before Truss session
construction, matching-context refusal before connection/session creation
when live custody proof is absent, and refusal by both ACK methods. The
existing Truss source-custody and bridge suites passed unchanged. No live
PostgreSQL, Truss, UC, cloud, source mutation or ACK action occurred.
`git diff --check` and Python compilation passed.

## Remaining live inputs

- Independently verified remaining-lifetime/custody proof tied to the exact
  original Truss .52 source handle, with renewal/readback semantics that cover
  the bounded ACK and reconciliation window.
- Reviewed same-transaction Truss PostgreSQL `READ WRITE` setup, with a focused
  native PostgreSQL check under the selected server defaults.
- Issued A-side publication-bound Truss source selection and current/retained
  source registration, page, cursor and ACK-scope observations.
- Complete retained managed `/0.2` A/B prefix, six-role table owner, native
  readback/source-oracle and pin-lifetime owner.
- Independent terminal durable verifier observation and approved read-only
  Databricks/serverless/runtime/cost inputs under the existing `$100` cap.

Until these inputs exist and are reviewed, the source and ACK ports remain
unavailable for managed A1.
