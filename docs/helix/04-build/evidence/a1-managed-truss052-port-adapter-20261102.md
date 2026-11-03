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
reconciliation and close on one retained connection. The public implementation
uses a session advisory lock as the custody witness. The lock has no TTL: it
survives transaction boundaries on the original PostgreSQL session and is
released when that connection ends. Renew, ACK, reconciliation and close check
the lock, backend and selected row in server-side SQL. A successful renewal is
therefore a point-in-time server observation of active custody, not a remaining
lifetime measurement. The Ashlar session wrapper now validates the complete
renew observation against the original page and runtime status.

The wrapper's `statement_timeout_ms` bounds individual SQL statements, not an
entire ACK-plus-reconciliation sequence. The managed A1 bridge currently
requires a `remaining_lifetime_seconds()` method from every owner, so it cannot
yet compose the Truss session-lock semantics honestly. It must instead admit
the source owner by its server-checked same-session lock witness and impose a
real operation deadline across ACK and reconciliation; a host stopwatch or
synthetic lease must not stand in for either property.

Inspection of the pinned public API found that Truss `.52`'s private host
runner accepts an absolute `operation_deadline`, and its cancellation path
uses `connection.cancel_safe()` then joins the cancellation timer before
cleanup. However, the public `acknowledge_feed_under_source_custody_in_transaction`,
`reconcile_feed_ack_under_source_custody_in_transaction`, renew and close
methods call the runner without forwarding such a deadline. The public session
also commits each transaction outside that runner's deadline. A configured
`operation_timeout_ms` is therefore per SDK operation and does not enforce one
aggregate ACK plus reconciliation budget. Ashlar must not reach into Truss
private runner state or claim the bridge's after-return elapsed check is a hard
deadline. Until an approved adapter API can bound cancellation and transaction
commit for the aggregate interval, these ports remain fail-closed.

`src/ashlar_host/managed_a1_truss052_ports.py` therefore refuses both source
open and ACK/reconciliation. It does not construct a Truss session, obtain a
connection, or attempt ACK. A supplied host monotonic deadline is not presented
as source-custody proof. The source `open` checks the original context identity
before inspecting other inputs or creating any session. The next implementation
can use the existing server-checked session lock as active custody, but must
retain one connection and check exact renewal immediately before ACK, then rely
on Truss's lock checks for ACK, reconciliation and close. It must bound the
full effect/reconciliation interval rather than claim a source lease.

Sol's read-only review also found that `Truss052SourceCustodySession._transaction`
previously separated isolation from access mode before the host adapter adopted
the requested transaction. The Ashlar-owned wrapper now makes its first
statement explicit and atomic: `SET TRANSACTION ISOLATION LEVEL READ COMMITTED,
READ WRITE` for writes, or the same statement with `READ ONLY` for reads. This
is a fail-closed change to Ashlar's adapter; no upstream Truss code changed.
Focused tests assert this exact mode statement is first. A selected PostgreSQL
server with `default_transaction_read_only=on` still needs a live PG18 proof
that the selected principal can override the default in this deployment.

PostgreSQL 18 documents `READ WRITE` and isolation as modes in one `SET
TRANSACTION` command, and exposes both `default_transaction_read_only` and
`transaction_read_only` for per-session default/current state. See the official
[SET TRANSACTION reference](https://www.postgresql.org/docs/18/sql-set-transaction.html)
and [client connection defaults](https://www.postgresql.org/docs/18/runtime-config-client.html).

## Narrow PG18 qualification plan

No local PG server is available (`pg_isready` reported `/tmp:5432 - no
response`), so this plan has not run. Use a dedicated disposable PostgreSQL
18.6 instance and a scratch database only; do not install Truss schema or
connect to a shared cluster.

1. As a local setup administrator, create a probe table and a non-superuser
   probe login. Grant only connect, schema usage, and insert/select on that
   table. Set `default_transaction_read_only=on` for the probe role and confirm
   the setting on a fresh connection.
2. Connect with psycopg `autocommit=False`. Capture the server log or SQL trace
   to confirm the first statement is exactly
   `SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE`. On that same
   transaction, read back `current_setting('transaction_read_only')` and
   `current_setting('transaction_isolation')`; require `off` and
   `read committed`. Apply the local statement timeout, invoke Truss .52
   `adopt_transaction(access_mode='read_write')`, insert one probe row, then
   roll back. Confirm the probe table has no row and the connection is idle.
3. Repeat with the first statement ending in `READ ONLY`; require the setting
   to read `on` and a probe insert to fail with SQLSTATE `25006`. Roll back and
   confirm idle state. Repeat a control connection with the role default off.
4. Retain PostgreSQL exact version, effective role/database settings, first
   statement, readback rows, SQLSTATE, transaction-status transitions, rollback
   result, and logs with secrets removed. A server in recovery or a principal
   unable to switch access mode must refuse.

This proves only PostgreSQL transaction setup used by the Ashlar adapter. It
does not open Truss source custody, ACK a receipt, or qualify the separate
Truss live-lifetime requirement.

## Minimal live-lifetime/renewal proof

The pinned Truss `.52` API does not need an invented lease field: a session
advisory lock is held until disconnect, and every effect/reconciliation/close
SQL operation verifies it on the same backend. The intended A1 source custody
proof is the original held session plus successful exact server renewal
immediately before ACK, with no reconnect path. If that session is lost, all
subsequent operations must fail closed. The 30-second A1 window is an operation
deadline covering ACK and reconciliation; it is not source lifetime.

This remains unimplemented at the managed bridge seam. The current Truss host
has per-operation `operation_timeout_ms`, but the public source APIs do not
forward a dynamic absolute deadline and transaction commits occur outside the
runner's timed callback. A concrete aggregate deadline requires an admitted
public adapter capability that can cancel and join the whole ACK/reconciliation
sequence, including commits; if it cannot, source ACK remains unavailable.
Keep managed read-handle lifetime and native pin transaction lifetime as
separate admissions; the session lock says nothing about either.

The separate owner would still need an issuer-bound A-side
`TrussSourceCustodySelection` matched to exact A/B source state, request,
manifest, receipt and vector. The current A1 genesis selection does not itself
admit that publication-bound source selection.

## Focused checks

Local Python 3.12 verification passed:

`PYTHONPATH=src:tests python3.12 -m unittest -q
tests.test_managed_a1_truss052_ports tests.test_truss052_source_custody
tests.test_managed_a1_release_bridge` — 27 tests.

The source-session test asserts the combined access mode is in the first
statement for both read/write and read-only transactions.

The three gate tests verify wrong-context refusal before Truss session
construction, matching-context refusal before connection/session creation
when live custody proof is absent, and refusal by both ACK methods. The
existing Truss source-custody and bridge suites passed unchanged. No live
PostgreSQL, Truss, UC, cloud, source mutation or ACK action occurred.
`git diff --check` and Python compilation passed.

## Remaining live inputs

- Concrete Truss `.52` source and protected ACK adapters in the managed bridge,
  retaining the original connection and enforcing one aggregate ACK plus
  reconciliation deadline, including transaction commits, while verifying the
  exact server-side session lock. The pinned Truss public API currently does
  not expose this aggregate deadline.
- Focused native PostgreSQL 18 proof for the Ashlar-owned first-statement
  `READ WRITE` setup under the selected role's effective read-only default.
- Issued A-side publication-bound Truss source selection and current/retained
  source registration, page, cursor and ACK-scope observations.
- Complete retained managed `/0.2` A/B prefix, six-role table owner, native
  readback/source-oracle and pin-lifetime owner.
- Independent terminal durable verifier observation and approved read-only
  Databricks/serverless/runtime/cost inputs under the existing `$100` cap.

Until these inputs exist and are reviewed, the source and ACK ports remain
unavailable for managed A1. The PostgreSQL 18 native proof also remains
outstanding; local fake-port tests do not qualify production custody or ACK.
