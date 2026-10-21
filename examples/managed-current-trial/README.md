# Managed current functional trial worker

This directory contains the source for one Databricks Python notebook task. The
worker expects a caller-owned dedicated serverless Jobs task with one concurrent
run, zero retries, a finite timeout, and an approved cumulative cost estimate.
It does not create a job, retrieve source pages, create credentials, or ACK Truss.
The worker's configuration validation checks submitted intent; it does not prove
the Jobs definition enforces it. Before any paid run, independently inspect the
actual Jobs task through the Jobs API for its unscheduled serverless compute,
single-task concurrency, zero retries and finite timeout, and verify the
cumulative DBU/USD estimate including terminal cleanup against the owner's
approved limits.

The task runs one page per invocation. The external source holder supplies the
original Truss 0.0.44 page plus its digest-bound accepted UMF 0.10 catalog bundle
through private Databricks secret references. The widget values are secret scope
and key names only. The private configuration carries a frozen schedule digest,
step number and stage ID; the task echoes these in its receipt. Raw values never
go to notebook output or logs. After the
task succeeds, the holder must retain the returned destination receipt, reopen
the exact Unity Catalog manifest and all four data-role UUID/version pairs, then
ACK the original page. Only after that ACK can it fetch and deliver another
source page. A lost task response remains unresolved and must be reconciled from
the original task/run and UC state; the worker does not retry it.

Before execution, the private configuration must contain the exact selected
serverless runtime, public Spark version/build string when available, runtime
tag, principal, catalog/schema, native reader protocol, frozen source revision
vector, total DBU and dollar estimate, cleanup reserve, and timeout. The
estimate includes startup, driver, task work, and cleanup. The preferred total
is at most $10; a higher estimate needs an explicit owner rationale and must
remain below the $100 hard ceiling. The worker refuses an estimate that omits
cleanup reserve or disables predictive optimization. Public runtime observations
are captured within the functional task; no observer job is needed.

Set `create_schema` only on the first invocation, after selecting a unique new
schema in `client_dev`. Later pages reuse that same schema with
`create_schema: false`. Each publication uses the current six-role managed
installer and returns a destination receipt; the external holder owns ACK and
source cursor custody. Query qualification is a separate read stage over the
positive published state and must use the unchanged original compiler SQL and
parameters, the exact managed Backend03 and Backend02 artifacts, independent
source-derived results, and opening/closing publication custody. The worker
alone does not claim Weft query completion.

`ManagedCurrentReadOwner` implements that query stage over the same injected
Spark task. Construct it from a fresh `ManagedCurrentTargetOwner`, the retained
destination receipt, the task's opening/closing runtime identity, and a
separately digest-admitted `ManagedCurrentSourceOracle`. Admit that oracle with
the exact retained source bundle plus the frozen schedule digest and step; the
admission binds its feed, epoch, cursor, catalog/model hashes, and source-page
identity to that bundle. The read owner reconstructs the original Truss/UMF
admission and requires the durable manifest's progress entry to match the same
page before compiling or executing queries. It reopens the durable
manifest and four-role data vector against the separate six-role installation
inventory, then provides `NativeBackend`, a full-vector manifest pin, read-only
Weft execution, and runtime/table/protocol renewals around buffered results.
Use `run_backend03_case` once for the positive grouped/HAVING request and once
for the independently compiled negative HAVING request. Build the Backend02
`EngineRead` with the original method `ashlar-spark4-pages` and page size 2,
create `backend02_target`, and call `run_backend02_two_pages`; it returns two
case records and never returns the opaque continuation cursor.

The operator retains four separate case assets:
`managed-current-uc-case-backend03-grouped-having-v0.4.50.json`,
`managed-current-uc-case-backend03-negative-having-v0.4.50.json`,
`managed-current-uc-case-backend02-page1-v0.4.50.json`, and
`managed-current-uc-case-backend02-continuation-v0.4.50.json`. The case helper
stores canonical request/artifact JSON beside their SHA-256 values, exact
ordered-parameter hash, independent expected/actual rows, runtime identity,
four-role publication vector, and separate six-role inventory. Page records
contain only the SHA-256 of the opaque continuation. A parity asset must compare
the SQL and ordered-parameter hashes for these exact requests with the retained
prior local packet; none of these functional results closes the separate 1B/5B
release gates.

Retain all task results, exact request digests, native manifest/vector receipts,
query artifacts/results, terminal run state, and actual cost evidence before
calling `cleanup_managed_current_trial_schema`. Cleanup requires the exact
six-role UUID/owner inventory. It deletes only those six tables and the now-
empty schema, then verifies schema absence; an unknown or replaced object
blocks cleanup. Do not use this task for a scale trial.
