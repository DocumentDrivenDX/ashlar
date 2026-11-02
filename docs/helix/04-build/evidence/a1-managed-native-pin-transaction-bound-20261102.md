---
ddx:
  id: ashlar.evidence.a1-managed-native-pin-transaction-bound-20261102
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
    - id: CONTRACT-004
      kind: governed_by
    - id: ashlar.evidence.a1-managed-release-bridge-candidate-20261102
      kind: follows
    - id: ashlar.evidence.a1-managed-native-adapter-prerequisite-gap-20261101
      kind: informed_by
---

# A1 native pin lifetime and owner boundary — 2026-11-02

## Native pin transaction bound

The bounded lifetime is opt-in. `PostgresPins.open_live()` and
`ManagedOperationalReadPinPort` preserve existing behavior unless the A1 owner
explicitly selects a transaction timeout between 31 and 900 seconds. The bridge
requires this selection to cover its configured 900-second holder interval;
ordinary callers of `open_live()` do not receive a newly shortened timeout.

For an opted-in handle, the same PostgreSQL transaction sets both
`idle_in_transaction_session_timeout` and (where supported)
`transaction_timeout` to the selected duration. Before reporting remaining
lifetime, the handle reads back both effective settings and the transaction's
server-side age on that same connection. It subtracts the full host round-trip
time from both bounds and reports their minimum. Missing or unsupported
`transaction_timeout`, mismatched effective settings, malformed readback or
expired coverage refuses. The managed A1 bridge checks the capability at open
and after pre-ACK renewal, separately from the managed read interval clock.

The transaction timeout bounds the entire PostgreSQL transaction; the idle
timeout also bounds a stalled holder. These settings do not reserve Delta files
or replace managed exact-version readability observations. The managed pin SQL
allowlist admits only the fixed `set_config` and readback statements, with the
timeout constrained to the selected 31–900 second range. No PostgreSQL or cloud
connection was opened, so effective server settings and connection behavior
remain unqualified.

## Truss and verifier owner investigation

The repository already contains `Truss052SourceCustodySession`, which uses the
public pinned Truss 0.0.52 API on one retained PostgreSQL connection for source
custody open, renew, selected-page reread, one-shot ACK, fresh exact
reconciliation, and close. It is a real adapter implementation, but it is not
composed into the managed A1 bridge. Its required
`TrussSourceCustodySelection` must come from the issued source anchor and
publication binder; that binder requires the original managed receipt-reader
observation. The A1 genesis helper currently returns source observations and
the original A page, but it is read-only, closes its sessions, and does not
produce the live publication-bound selection or source-holder interval. The
.52 session also has per-operation statement timeouts, not a separately
admitted source-holder interval lifetime. A synthetic adapter around it would
not close these gaps.

No read-only Jobs owner currently observes terminal task status together with
the independently retained durable verifier result. A task report, run ID, or
caller-supplied completion object cannot substitute. No source session,
protected ACK, verifier query, UC read, or mutation was executed.

## Focused local checks

The following local Python 3.12 suites passed:

`PYTHONPATH=src:tests python3.12 -m unittest tests.test_pins
tests.test_native_pin_port tests.test_managed_pin_reader
tests.test_managed_a1_release_bridge -v` — 43 tests passed. They check
opt-in scope, effective-setting/transaction-age readback, conservative
remaining-lifetime math, and refusal of unsupported or expired bounds. They
use local executors and protocol doubles; they do not prove a real PostgreSQL
deployment, Truss, Databricks, Unity Catalog, or ACK authority.

No local PostgreSQL server was available for a live check (`pg_isready` reported
`/tmp:5432 - no response`). The server-side timeout statements and readback
therefore remain unqualified on an actual database.

The adjacent managed receipt/pin composition suite also passed:
`PYTHONPATH=src:tests python3.12 -m unittest
tests.test_managed_current_spark_receipt_read -v` — 46 tests passed. Its local
executor verifies timeout setup and independent pin lifetime readback through
the managed composite.

## External inputs still required

- A separately admitted managed `client_dev` serverless task and run-as
  principal, exact environment/dependency versions, UC mode/grants, and a
  per-run plus cleanup cost bound under the existing $100 cap.
- A selected fresh six-role managed target owner and the exact four-role
  publication vector, with native full-row reads compared to an independently
  rebuilt A/B source oracle at open, pre-ACK and close.
- The original published Truss .52 A/B registration and cursor observations,
  first retained A page, publication-bound `TrussSourceCustodySelection`, and
  an independently admitted same-session source/ACK owner with a reviewed
  bounded renewal interval.
- A read-only independent Jobs/run observer that proves terminal durable
  verifier success for the exact request, manifest, receipt and vector before
  opening the source/read/pin interval.
- The approved PostgreSQL pin endpoint, ordinary managed pin-reader identity,
  exact four-role pin registration and source-bound authorization owner, so the
  new server-enforced transaction bound can be checked on the selected target.

Until these owner inputs are admitted and independently reviewed, the A1
production trial and source ACK remain unqualified. No cloud, source, UC, pin,
or ACK operation was attempted; no spend was incurred.
