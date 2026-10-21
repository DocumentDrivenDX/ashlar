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

This campaign worker is for the positive graph page after the catalog page has
been published and ACKed. It publishes that positive page and runs all four
selected Weft cases in the same task: grouped HAVING `> 0`, negative HAVING
`> 2`, and the original Backend02 page-size-2 read with its continuation. The
fresh campaign explicitly selects physical carrier profile
`ashlar-delta/0.4-xxhash64`, including BIGINT `lookup_hash` columns; Truss
catalog evidence and UMF source pins remain their separately admitted profiles.
external source holder supplies the original Truss page plus its digest-bound
accepted UMF 0.10 catalog bundle through private secret references. A separate
private secret carries the digest-bound source oracle for that same page.
Widget values are secret scope/key names only. The private configuration
carries a frozen schedule digest, step number and stage ID; actual Jobs run/task
IDs are also recorded in every case. Raw values never go to notebook output or
logs. After the task succeeds, the holder must retain the returned destination
receipt, reopen
the exact Unity Catalog manifest and all four data-role UUID/version pairs, then
ACK the original page. Only after that ACK can it fetch and deliver another
source page. A lost task response remains unresolved and must be reconciled from
the original task/run and UC state; the worker does not retry it.

Before schema creation, the task rechecks the selected runtime name, principal,
native protocol selection, observed Spark version, and a public Spark build
string or runtime tag. It admits the original source bundle and source oracle
before creating a new schema. After confirming the schema is owned by this
trial, it selects the catalog/schema and requires the exact runtime namespace
observation. The target tables are then checked against actual Delta protocol
metadata. If Spark version or build values are unknown before launch, the
configuration leaves those fields unset; the first functional task captures
nonempty public values and holds that exact runtime tuple through publication
and all four reads. Any supplied Spark version, build string, or runtime tag is
checked against the observation. The private configuration must contain the
selected serverless runtime, principal, catalog/schema, native reader protocol,
the exact carrier profile, frozen source revision vector, total DBU and dollar
estimate, cleanup reserve, and timeout. The
estimate includes startup, driver, task work, and cleanup. The preferred total
is at most $10; a higher estimate needs an explicit owner rationale and must
remain below the $100 hard ceiling. The worker refuses an estimate that omits
cleanup reserve or disables predictive optimization. Public runtime observations
are captured within the functional task; no observer job is needed.

Set `create_schema` only on the first catalog publication, after selecting a
unique new schema in `client_dev`; the positive-page campaign uses that schema
with `create_schema: false`. The positive-page task re-admits its exact Truss
bundle and source oracle before publication, then creates a fresh
`ManagedCurrentReadOwner` from the returned destination receipt and the same
task's opening/closing runtime identity. Oracle admission binds feed, epoch,
cursor, catalog/model hashes, and page identity to the retained bundle and
frozen schedule. Before query execution, the owner reconstructs the original
Truss/UMF admission and requires durable manifest progress to match that page.
It reopens the manifest and four-role data vector against the separate six-role
installation inventory, then provides `NativeBackend`, a full-vector manifest
pin, read-only Weft execution, and runtime/table/protocol renewals around each
buffered result. The campaign runs `run_backend03_case` for positive and
negative requests, then builds the Backend02 `EngineRead` with the original
method `ashlar-spark4-pages` and page size 2 and calls
`run_backend02_two_pages`. The resulting case records include exact compiled
requests and artifacts; opaque continuation bytes stay in memory and only
their digests enter the result.

The operator retains four separate case assets from this single positive-page
task:
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
