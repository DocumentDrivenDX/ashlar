# R61 fresh Truss / UMF local qualification packet

Status: the fresh Truss setup stage and one-shot outer supervisor are coded but
have not been dispatched. No PostgreSQL, Truss source, Delta, ACK, Spark or
GraphFrames action has been run. This packet does not qualify a release.

The next local qualification is intended to establish one fresh-baseline path
with Truss 0.0.53, UMF SDK 0.10.2 and Core 0.10.6 while retaining the declared
UMF model version 0.10.0. Exact artifacts and the Ashlar adapter are still
unbound. The runner therefore has only an effect-free preflight; it refuses
execution until the candidate pins have been supplied and independently
reviewed.

## Bounded scope

The supervisor enforces one attempt, at most 900 seconds including cleanup,
4 GiB aggregate process-group RSS and 512 MiB disposable output. It captures at
most 64 MiB of worker logs, terminates and reaps the process group, and checks
the exact run-owned PostgreSQL pidfile/socket after cleanup. A worker completion
receipt is required to assert owner closure and state reconciliation. These
mechanisms have not yet had a native run; the exact candidate bindings and
independent review are still missing.
The estimated duration is 8–12 minutes. It uses a new PostgreSQL data directory
and Delta root, never the retained R59 state.
The callable first stage reuses Ashlar's public Truss setup/read helpers to
accept the retained UMF 0.10 model in a fresh installation, register a consumer,
capture the exact report/catalog/source/page bytes, and write its receipt only
after `LocalPostgres` closes. The helper has no CLI entry point and is not
called by preflight.
The intended workflow is public Truss source and consumer setup, one seven-op
seed, one continuation and protected ACK, complete four-role publication
vector plus a separate six-role installation inventory, original-owner graph
release plus custody, then one GraphFrames action
against those exact bytes using the reviewed Ashlar candidate wheel. All
owners must close before final graph bytes are exposed. No retries, broad
matrices, cloud, scale, tag, release or push are in scope.

The GraphFrames environment is a separate runtime. Its prior R59 configuration
(Python 3.11, JDK 17, Spark 3.5.3, GraphFrames 0.12.3, Delta 3.2.1, local[1],
512 MiB driver heap) is a planning reference only; it is not evidence for this
candidate. Candidate compatibility and all hashes remain required inputs.

## Readiness

Run `python tools/r61_fresh_cohort_runner.py --preflight-only` or invoke the
supervisor with the exact Python 3.11.17 pinned under `supervisor_runtime` in
the scope. Both preflights are effect-free. The supervisor's `--run-once` requires all exact
runtime/artifact/worker hashes, an accepted upstream review state, a matching
external dispatch receipt and a fresh output root. It launches only the bound
worker with a closed environment. The CLI remains unavailable in practice
because the candidate worker and runtime artifacts have not been supplied.
The separate GraphFrames process must consume owner-captured canonical release
and custody bytes with the exact new Ashlar wheel; it does not need to scan the
new Delta tables.

This packet uses HELIX `04-build` runtime-handoff mode. The governing boundaries
are the Ashlar product requirements, publication/consumer/table/resolver
contracts and consumer conformance; evidence must preserve source authority,
identity, presence, multiplicity, complete publication vectors and ACK
custody. See the scope JSON for planned completion receipts and explicit
non-claims.
