# Truss 0.0.49 current-cohort refresh proposal

Status: **effect-free proposal; not an execution packet**. This refresh proposes
replacing the held R5 cohort's Weft 0.4.52 pin with the public Weft 0.4.53
release, and rebinding its source snapshot to the selected Ashlar source revision
`6986e8327c366b76ba09ffed6b2fe17a3f484cc7`. It preserves the public Truss
0.0.49, UMF core 0.10.0 and UMF access 0.1.0.dev7 pins. No UMF bridge IIFE
rebuild is indicated: `src/` and `sql/` are unchanged from the R5 source
objects; the source `pyproject.toml` pin changes from 0.4.48 to 0.4.53. R5's
isolated runtime was separately installed with Weft 0.4.52. The bridge-readiness
pin still must be regenerated for the selected Ashlar commit and
`pyproject.toml` object.

The selected Weft wheel is the published macOS arm64 artifact from commit
`726a840e956e25217d6aeea2674fdfcf3f342a24`, SHA-256
`4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c`. The
[focused Ashlar adoption receipt](../weft053-ashlar-adoption-20261023/README.md)
shows one page-compiler fixture with byte-identical output to 0.4.52. It does
not qualify native SQL execution, the selected Truss lifecycle runtime, or
Unity Catalog. R5's preflight and runtime receipt remain historical 0.4.52
evidence and cannot be transferred to this cohort.

## Preserved lineage

The refresh retains the existing source identity, origin digest, registration,
feed receipt, consumer registration and genesis checkpoint exactly as recorded
in [R5 preparation](../truss049-r5-readiness-20261023/preparation.json).
It points to that packet's retained PGDATA reference without copying or moving
the directory. These are lineage references, not current authority: a future
operator must reobserve the accepted source/report/catalog view, feed and
consumer checkpoint, and verify that the same PGDATA is stopped and unchanged
before any migration or write. If the observation is incomplete or differs,
stop and reconcile the original lineage; do not register replacement roles or
create a new consumer.

## Required before any future preflight

1. Build a fresh isolated CPython 3.12.14 runtime from exact local wheel hashes
   for Truss 0.0.49, UMF core 0.10.0/access dev7, Weft 0.4.53, Spark 4.2.0 and
   Delta 4.4.1. R4's Weft 0.4.52 runtime is not reusable as current-cohort
   qualification. Verify wheel origins, installed RECORD payloads, imports and
   `pip check`; do not install into or overlay the old environment.
2. Rebind the worker, supervisor, preparation and bridge-readiness pin to this
   selected Ashlar revision, current Weft wheel and new runtime receipt. Keep
   the existing UMF source-role bytes, bridge IIFE and semantic assets only
   after their hashes are independently checked against the preserved R5
   receipts. Generate new static pins and a new unique attempt identity; do not
   reuse the old exact-approval payload.
3. Run only focused offline compiler/typed-model checks against the selected
   runtime. The 0.4.53 page fixture receipt is relevant compatibility evidence,
   not a substitute for installed-runtime provenance.
4. After packet review and a clear local resource slot, a separately bounded
   effect-free supervisor preflight may recheck current source custody, stopped
   PGDATA, process/SysV gates and exact approval-payload parity. Any changed or
   unknown lineage refuses. A fresh explicit owner decision is required before
   a supervised lifecycle execution; this proposal itself authorizes no such
   action.

If later prepared and separately approved, the campaign remains single-use: one bounded 0.49 migration, then reuse of
the existing accepted source and consumer for the planned genesis ACK, approved
seven-operation seed, actual continuation and protected ACK. Preserve the
original latest-only migration, complete source/consumer receipts and ACK
reconciliation sequence from R5; do not retry uncertain operations or silently
replay the seed. Resource limits remain the R5 limits (600-second child,
1-GiB campaign data including retained PGDATA, backup, attempt files and worker
logs, 16-MiB per-worker log, one attempt, local Spark only); runtime and source
snapshot remain excluded as in the original packet.

No runtime was built or installed for this proposal. No refresh preflight,
PostgreSQL, Spark, migration, backup, seed, feed, publication, ACK, approval
file or start marker was created. The R5 packet remains unchanged and
unapproved. See
[`refresh-proposal.json`](refresh-proposal.json) for frozen pins and preserved
lineage references, and [`check_refresh_proposal.py`](check_refresh_proposal.py)
for the local read-only consistency check. `artifacts.sha256` freezes the three
packet files for review.
