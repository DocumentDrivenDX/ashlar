# Truss 0.0.49 lifecycle attempt r2 — PostgreSQL start failure

This record supplements the Sol-reviewed offline candidate in
`../truss049-selected32ec-head0506fb7-preflight/`. The approved single-use
attempt r2 exited 1 after 182.055 seconds while Truss `LocalPostgres`
entered PostgreSQL 18.6.1 against the retained 0.0.48 PGDATA. PostgreSQL
logged `shmat(...) failed: Operation not permitted`, then shut down. The
evidence points to host sandbox denial of System V shared-memory attachment;
it does not establish a PostgreSQL data or Truss migration defect.

The attempt made a verified cold copy before startup. The retained source
PGDATA log then changed from the copied 9,654-byte file (SHA-256
`e3108663…`) to 9,936 bytes (SHA-256 `e11d51d0…`), containing the failed
startup and shutdown lines. Thus a local PostgreSQL filesystem effect is
confirmed. The worker did not reach `postgres-started.json`, the pre-migration
read, or migration apply. No Truss source mutation, feed action, publication,
destination write, ACK, seed, Spark, or cloud operation started. Other PGDATA
file differences were not audited, so the log append is the only specific
data-directory change claimed here.

The supervisor reports no postmaster PID and `cleanup_status=postmaster_pid_absent`,
but its process-group inventory failed with `PermissionError`. This does not
prove all worker descendants were reaped. The spent r2 attempt is retained as
failure evidence and must not be retried or cleaned up in place.

A future supervised run requires a new private attempt root and one-use
attempt, a fresh cold backup of the current retained PGDATA, and a complete
preflight tied to the exact worker, supervisor, source, runtime, bridge and
native manifests. It must run in an explicitly approved execution environment
that permits PostgreSQL System V shared memory and permits bounded process
inventory/reaping. The operator must issue a new hash-bound approval for that
exact packet; the prior approval covered only r2 and is spent. Any escalated
execution authorization and process-custody review must be completed before
that new run. No native retry is authorized by this evidence record.

The failed process-group inventory and absence of a postmaster PID leave
process custody unresolved. Preserve the private attempt artifacts and source
log as retained evidence until the owning operator establishes custody.
