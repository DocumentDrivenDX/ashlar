# Truss 0.0.49 current-cohort custody proposal

Status: **static binding only; no executable packet or preflight**. This note
binds the selected Ashlar source objects at `6986e832` to the fresh isolated
CPython 3.12.14/macOS arm64 runtime containing Truss 0.0.49, UMF core 0.10.0,
UMF access dev7, Weft 0.4.53, Spark 4.2.0 and Delta 4.4.1. The source selection
and Weft change are recorded in the [R5 refresh proposal](../truss049-r5-current-cohort-refresh-20261023/README.md);
the fresh runtime package integrity is recorded in the
[current-cohort runtime receipt](../truss049-current-cohort-runtime-20261023/README.md).

This proposal preserves the existing accepted source, feed registration,
consumer, roles, genesis checkpoint, seed-request digest and a reference to the
retained PGDATA. Those are historical lineage pins, not current observations.
R5's Weft 0.4.52 runtime, preflight, bridge readiness and approval payload are
stale for this cohort and cannot be transferred. The retained PGDATA value is
not copied into this packet.

This preliminary proposal stops before worker/supervisor rebinding. The later
[R6 static readiness packet](../truss049-current-cohort-readiness-20261023/README.md)
records those rebased tools, bridge pin and fresh attempt identity. After independent
review and a free local slot, only then may an operator run the exact effect-free
`python run_supervisor.py --preflight` command from that regenerated private
packet. The effect-free supervisor preflight checks the original stopped PGDATA,
current process/SysV gates and exact approval-payload parity. It does not read
the current PostgreSQL catalog, source, feed or consumer checkpoint. Those
source/feed/consumer identities must be reobserved only after a separately
authorized lifecycle start and before migration; any changed, incomplete or
unknown state stops the lifecycle before migration. A passing preflight is not execution authorization; a separate explicit owner
decision is required before `--execute`.

No PostgreSQL or Spark process, lifecycle worker, migration, backup, seed,
source/feed mutation, publication, ACK, execution-approval file or started
marker was created for this proposal. No current lineage state is inferred.
The exact pins, preserved lineage references, preflight conditions and future
resource bounds are in [`custody-proposal.json`](custody-proposal.json).
Run [`check_custody_proposal.py`](check_custody_proposal.py) for the read-only
consistency check. `artifacts.sha256` covers this proposal packet.
