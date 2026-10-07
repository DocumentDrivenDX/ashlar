# Bounded20M physical comparison plan

Draft spike under CONTRACT-003/ADR-001; UC Delta remains selected and hash-LC
remains canonical candidate. r158–r160 qualify100k carrier/bucket/update mechanics,
not20M physical-copy preservation or billion admission.

Use immutable canonical E23 and published r139-b1 as input, observing current
physical head independently (currently25 via OPTIMIZE). One owned shallow LC
clone and one owned64-bucket partition copy; explicit rowTracking/DV/Zstd/
identity+eligibility statistics on both. Bucket source derivation/assignments must
use the executed owned helper.64 is an experiment count, not a selected1B/5B value.
Preserve Truss IDs, full tuple predicates, exact text, typed endpoints and history;
no source authority, UMF binding or graph-reader feature admission is added.

Bounds before any mutation:

- Exactly20M source edges and one100k same-size high-entropy update per layout;
  no node/history expansion or real source feed, no new/changed compute.
- Current source roughly34.3GB compressed. Partition copy plus a possible full
  Z-order rewrite roughly68.6GB; stage and updates add roughly1.3GB. Cap estimated
  new writes at75GB with an admission check between phases. These are planning
  bounds, not an enforced cap on one in-flight native operation or billing.
- Controller ten-minute SQL-admission deadline, each native statement180 s;
  durable per-client query correlation and bounded socket/retry configuration.
  Inspect same server handles/commits on timeout; never restart a write on an
  observation timeout. Stop admitting later phases if measured costs or time
  invalidate the bound. Record measured costs even when a phase fails.
- Initial copy/Z-order expected data scans plus narrow structural checks/update
  validation should fit an estimated150GB read budget. No whole20M wire/digest
  EXCEPT query: prior180 s failure/96GB scan remains failed evidence. Abort
  admission if actual phase metrics invalidate this estimate.

Record exact schema, table UUIDs/versions/protocol/features, file counts/bytes,
actual maintenance rewrite/no-op, bucket/hash invariants, global IDs and complete
20M structural tuple parity. Inspect100k complete changed carriers after update
and unchanged physical-row custody within each owned layout. Initial wide-copy
parity across all20M strings is a separate unresolved obligation: exact sampled
carriers/structural parity/copy SQL alone must not be advertised as exhaustive
wide-copy proof. Design a bounded follow-up only after timing and cost evidence
justify it; the goal cannot complete while that proof is missing.

Collect matched full-carrier point reads and runtime compilation/engine/caller/
file/remote metrics, separating affected from untouched keys. Copy/maintenance/
initial validation/input staging remain separate costs; no publisher freshness
or sustained-rate claim follows from a single MERGE. New files are not controlled
cold simply because they are new: preceding reads may warm them. External
maintenance/load can confound timings; capture actual head/lineage and preserve
all pinned descriptors. Keep PuppyGraph/GraphFrames/Fabric qualification and
real producer completeness/fencing/recovery separate and open.

Finalize evidence and identity/version-check cleanup of only owned tables; no
VACUUM/retention change or canonical mutation. Commit/push the completed iteration.
