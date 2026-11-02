# R61 fresh Truss / UMF local qualification packet

Status: the fresh Truss setup stage is coded but has not been dispatched. No PostgreSQL,
Truss source, Delta, ACK, Spark or GraphFrames action has been run. This packet
does not qualify a release.

The next local qualification is intended to establish one fresh-baseline path
with Truss 0.0.53, UMF SDK 0.10.2 and Core 0.10.6 while retaining the declared
UMF model version 0.10.0. Exact artifacts and the Ashlar adapter are still
unbound. The runner therefore has only an effect-free preflight; it refuses
execution until the candidate pins have been supplied and independently
reviewed.

## Bounded scope

Planned bounds are one local attempt, at most 900 seconds including cleanup,
4 GiB aggregate RSS and 512 MiB disposable output; they are not yet enforced.
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

Run `python tools/r61_fresh_cohort_runner.py --preflight-only` from this source
tree. The preflight checks the pinned source base, clean source hashes, packet shape, distinct
fresh paths, and required artifact bindings without creating those paths or
importing/starting external runtimes. It reports `blocked_pending_candidate`.
There is deliberately no dispatch command in this skeleton. After exact pins
and APIs are verified, the prepared first-stage function plus bounded runner and scope must be reviewed
before any native action.

This packet uses HELIX `04-build` runtime-handoff mode. The governing boundaries
are the Ashlar product requirements, publication/consumer/table/resolver
contracts and consumer conformance; evidence must preserve source authority,
identity, presence, multiplicity, complete publication vectors and ACK
custody. See the scope JSON for planned completion receipts and explicit
non-claims.
