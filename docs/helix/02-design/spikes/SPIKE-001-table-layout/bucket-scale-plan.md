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


### r161: full 20M bucket build reached the bounded timeout

The existing 2XSmall warehouse attempted a full 20-million-edge, 20-logical-field copy of canonical E23 into the 64-bucket physical candidate, with row tracking, Zstd, 64MiB target files and the same eligibility statistics. The source was the pinned 34.28GB/532-file snapshot, independent of the observed canonical head. The CTAS terminated with a native 180-second timeout (query `01f1c206-2c29-1449-a308-ac76ec5e0126`, final FAILED). Native metrics report 20M rows read, 34.784GB read, 29.014GB remote read, 16.961GB remote write work and zero spill. These failed-write metrics are not committed table size or ingest throughput.

No bucket table was registered. The shallow clone was UUID/version verified and dropped; no stage was created. No ZORDER, full structural parity, updates or singleton comparison ran. The source-copy wide-value obligation remains unproved. Uncommitted storage cleanup was not independently proven; no VACUUM or write retry was issued. Canonical publication r139-b1 still pins E23. Evidence is in `out/native/ashlar_bucket_screen_r161/{query-history,failed-summary}.json` and its inspection/cleanup subdirectories. The intended worker's unexecuted success-summary qualification inherited the 100k pilot scope; it produced no summary, and this terminal failure record defines r161's actual scope.

UC managed Delta remains selected. This bounded failure measures build cost on the existing small warehouse, not a reason to change architecture or a billion-scale result. Next: resume design around the canonical hash-clustered tables and explicitly separate optional bucket build/maintenance costs; a future full bucket comparison needs a longer controlled build window or resumable owned staging before read results can be compared.


### r162/r163: choose hash-range chunks, reject ID chunks

Five disjoint ID ranges each contained4M E23 edges but each read all532 files with zero file pruning (641.436MB narrow bytes per query). Four disjoint lexicographic hash ranges [0,4),[4,8),[8,c),[c,g) covered exactly20M edges and each read143–150 files/pruned382–389. Their total narrow read was1.297GB versus3.207GB for ID chunks. Final native IDs/metrics are preserved under `out/native/ashlar_bucket_chunk_r162` and `r163`. Counts and narrow pruning do not predict full-carrier write duration or prove preservation.

The next owned build uses these four immutable E23 ranges and exact20-column SELECTs plus derived bucket. Create an empty partitioned table, then admit one append at a time; require exact expected range count and inspect Delta history after every commit. Stop on terminal failure or ambiguity and inspect the same query/transaction; never append a range again blindly. Record UUID and per-chunk versions durably. Retain partial tables for inspection, exclude them from publication and readers, then clean only after ownership/version verification.

Limit each statement to180s and controller admissions to600s. Inspect final native metrics between chunks. Proposed copy-run admission bounds are45GB read and40GB write; the prior failed16.961GB write work remains recorded separately, so combined historical-plus-new expected work remains below75GB before any Z-order. Bounds control admission, not an in-flight operation or billing. No Z-order in this build iteration. Validate20M identities/bucket invariant and record file layout after successful copy; full20-field UTF8 parity, updated/untouched read comparison, and later maintenance remain distinct required phases. No billion-scale, freshness or generic producer claim follows from a resumable synthetic copy.
