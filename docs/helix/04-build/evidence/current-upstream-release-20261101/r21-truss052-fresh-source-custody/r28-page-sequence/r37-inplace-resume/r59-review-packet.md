# R59 protected ACK recovery review packet

## Decision boundary

R59 is an ACK-only local-development recovery for the already committed R55 page. It does not submit a Truss group or write Delta. The worker is withheld from native dispatch until Sol reviews the scope and worker.

R58 has durable native readback for publication `truss-graph-678967ac3deeafa90b514ae8a5a1b05f78b0b32e5f89135f4c67205fede001ea`: 6 objects, 3 edges, 0 tombstones, and 9 whole-source history rows. The R58 held-ACK receipt states `source_ack_attempted=false`; the continuation result records `deferred_not_attempted`. Its durable deferred-ACK intent, R55 source page, original publication intent, R55 pending readback receipt, and R58 native-readback/page-selection receipts are individually hash-bound in [r59-host-scope.json](r59-host-scope.json).

The current stopped source binding is PGDATA tree SHA-256 `90636c7b1dae157477eb72587286408f45c6f8560e0240d29083b9f6f0599ef1`, 1,265 files / 65,790,675 bytes, with no `postmaster.pid`. `pg_controldata` reports shut down, system identifier `7703148769689564565`, checkpoint `0/1F69548`, next XID `0:862`.

The read-only Delta reconciliation reports SQLite `integrity_check=ok`, no WAL/SHM sidecars, 23 local operations, and 3 each of local plans, source plans, and publication artifacts. The R55 request has exactly one source plan and one applied artifact. Current six Delta heads match the R58 readback: attempts v16, edge_current v2, manifest v4, object_current v3, tombstone v1, and whole_source_history v3. Native readback binds all four graph roles, their table UUID/version vectors, row counts, and row-bag hashes.

The exact reconciliation is [r59-pre-ack-read-only-reconciliation.json](r59-pre-ack-read-only-reconciliation.json). PostgreSQL source receipt and live feed SQL have not been read in this executor. The prior sandbox startup failed on shared-memory permission; R59 therefore makes source SQL a mandatory supervised pre-ACK gate and does not claim source reconciliation is already complete.

## R59 execution path

The native path opens the exact stopped local PostgreSQL source under the supervisor, then performs bounded explicit read-only SQL checks before invoking the protected ACK owner. It requires one complete `truss.request_receipt` for the exact R55 request key, namespace derived from the retained Truss writer context, writer XID 847, exact request ID digest, and the matching `runtime_object_attempt` row. It checks the original group input's two-create shape, exact accepted document bytes, request identity, and the source receipt's `truss-object-group-receipt/0.1` / `truss-group-result/0.1.0` semantic result with exactly two results. The source attempt's output is correlated to R58's fresh FeedPage, R55 group receipt, and persisted two-operation page correspondence; the prepared group input bytes are not asserted to equal the frozen semantic request JSON.

R59 then calls the established R49 `_public_recovery_operation` and Ashlar's `recover_committed_truss_publication_and_ack`. That path freshly reads the selected public FeedPage, independently re-reads all native Delta roles against the retained receipt/vector, writes the durable readback marker, opens the protected `.52` custody session, rereads the held page, attempts at most one ACK, and reconciles the outcome. Afterward R59 checks the source consumer checkpoint in a new read-only SQL transaction and freshly reads the public empty interval. It requires the fresh page prior cursor to equal the matched R55 next cursor; it does not assume the empty interval's next frontier must equal its prior cursor.

Any mismatch before the protected ACK attempt stops without ACK. If an ACK attempt marker exists, the failure receipt records that the outcome may be unknown; the worker has no retry or source ACK replay path. No Truss group call, publication continuation, Delta write, cloud resource, or seed/ACK replay is present in the R59 worker.

## Effect-free verification

The selected runtime's actual `parse_resume_truss_configuration` is exercised with a scratch config: its `{path, sha256}` selector resolves the applied R55 intent, which is the publication now committed by R58 and the source session required by the driver. Preflight confirms exactly one R55 source plan and applied artifact. R49 remains only the prior acknowledged checkpoint used by the owner when selecting the R55 page. The R49 recovery operation also passes its effect-free input-shape sentinel for the R58 pending receipt and R55 page. These checks do not open PostgreSQL or Spark.

[Worker preflight](r59-worker-effect-free-preflight.json) and [supervisor preflight](r59-supervisor-effect-free-preflight.json) pass with no PostgreSQL open, Spark start, Delta write, or ACK. The focused [supervisor test](test_r59_ack_supervisor.py) exercises the reviewed/dispatchable scope handshake with the native dispatch function mocked; it confirms the worker preflight runs and the supervisor reaches only the mock dispatch call without creating either native root.

## Bound files and limits

The scope binds the R58/R55 receipts, all six current Delta heads, the current stopped PGDATA digest, the R49 checkpoint `506d67a8dc1f8af88beb73bcd2b899afda8e8bebc62604561b4246a934e88c94`, the selected Truss 0.0.52 runtime, and exact helper/supervisor hashes. R59 is limited to one attempt, 900 seconds, 2 GiB RSS, 512 MiB aggregate local bytes, no cloud, and no automatic retries.

No native R59 dispatch has occurred. Sol review must approve the exact scope/worker/supervisor and the pre-ACK SQL contract before the parent runs the supervised attempt.
