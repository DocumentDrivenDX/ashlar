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
parity across all20M strings was a separate obligation, now resolved for E23/owned4 by r167–r169: exact sampled
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


### r164–r166: full20M owned bucket copy completed in four commits

The hash-range build succeeded on the unchanged existing 2XSmall warehouse. Four immutable E23 ranges produced4,999,762/5,003,588/4,999,471/4,997,179 rows in50.060/52.974/55.814/57.196 caller seconds. Owned table `client_dev.ashlar_entropy_20261006_r86.bucket_part_r164`, UUID `99831c85-f437-4e6f-8b8b-eaec9ab300d2`, is retained at version4:528 files/33,157,013,686 bytes. Each of the four exact native append IDs matches its Delta WRITE history and output row count; versions0–4 contain only the empty CREATE plus those appends. Reader3/writer7 with rowTracking/DV, Zstd,64MiB target and identity/eligibility statistics are recorded in the saved detail.

Full20M count and global distinct-ID cardinality pass; all derived buckets match the first60 SHA256 bits modulo64, valid lowercase64-hex hashes pass, and64 bucket counts sum to20M. Total build/recovery/audit reads38.199GB and remote writes33.157GB fit the45GB/40GB admission plan. The previous terminal failed r161 CTAS write work16.961GB remains separate evidence: combined historical write work50.118GB before any maintenance. No spill/latency/ingest conclusion is inferred merely from these totals; exact per-query metrics are retained. Canonical r139-b1 publication still pins E23/R17/J19 and its other original tables.

Two controller defects are preserved: r164 compared a nested result row to a flat expected list after a successful first append; r165 lacked the columns local and stopped before submitting another append. r166 checked the owned UUID, native version1 and exact expected count before appending only the remaining three ranges. No successful write was replayed. This is commit-inspection evidence for the owned experiment, not durable real-producer recovery/fencing admission.

The complete20-column SELECT plus derived bucket has now been executed at20M scale, but full20-field byte-exact parity is still required. No ZORDER, update comparison, singleton measurement, cold/service/rate or1B/5B admission follows. Keep this owned version for the next bounded exact-value validation and matched physical comparison; cleanup requires the same UUID and latest-version checks. Evidence: `out/native/ashlar_bucket_build_r166/audited-summary.json`, with r164/r165 failures and same-ID histories alongside it.


## r167–r169: full20M exact copy preservation proved

[Audited parity](out/native/ashlar_bucket_parity_r169/audited-summary.json) proves all20 logical edge fields across the complete20M-row owned bucket copy at version4 equal canonical E23. All11 text columns use null-safe UTF8 binary equality, including property/retained JSON and raw cursor text; numeric and timestamp columns use native typed equality. Four disjoint hash ranges cover4,999,762/5,003,588/4,999,471/4,997,179 edges with zero mismatches. Both snapshots independently have20M nonnull globally unique IDs. The first range uses a full outer join; the remaining inner joins require the exact expected joined count, which together with uniqueness proves exhaustive one-to-one membership, while all logical identity components are still compared. This is direct field equality, not sampled or digest-only parity.

The four comparisons take95.086/95.119/90.552/90.434 caller seconds. Total validation/recovery/audit reads89.987GB, remote writes0 and spill0 remain within the100GB read admission bound. All native IDs are terminal FINISHED with finalized uncached metrics. One unchanged and25 changed native constant controls pass, including NFC/NFD byte differences, JSON whitespace, retained/cursor integers above2^53, null and a timestamp1us change; two additional membership/duplicate counterexamples show why table counts alone are insufficient.

The r167/r168 controllers stopped after successful results while waiting for the last query's metric-final flag; no successful comparison was replayed. Installed connector source shows fetchall retains the active result until the next execute or cursor close. Explicit public cursor close/new cursor on the same session in r169 allowed final metrics to be inspected before further admission. Native result_fetch telemetry was5,875/61,659ms on the retained-result runs and302/303ms with explicit closure. This supports the lifecycle explanation but is not randomized causal isolation or caller fetch latency, and gives no singleton-SLO improvement claim. Preserve the stopped summaries and same-ID recovery evidence.

This resolves the initial full-wide-copy obligation specifically for owned UUID99831c85-f437-4e6f-8b8b-eaec9ab300d2/version4 and E23. The candidate is retained for the next physical comparison; r139-b1 publication is unchanged. It does not prove a later maintenance/update version, alter canonical DDL/constraints, select64 as a1B/5B bucket count, or admit source authority/fencing, external engines, cold reads or sustained freshness. Next: bound and run partition-local ZORDER, then matched update/singleton comparisons. UMF binding and existing graph-engine limits remain unchanged.


## r172 maintenance admission: four partition-local ZORDER groups

After initial/repeated matched30-key cohorts r170/r171, maintain only the owned version4 full20M bucket copy. Read current UUID/version and33.157GB size before writing; four disjoint partition predicates [0,16),[16,32),[32,48),[48,64) keep each attempted rewrite near8.3GB. Use ZORDER BY lookup_hash without altering source/canonical tables, permissions, compute, or retention. Record actual no-op/rewrite, command output, native ID, Delta history, version and costs after each phase; stop on ambiguity/terminal failure and inspect same IDs/commits before further writes. No blind replay.

Admit at most36GB new reported remote writes and45GB reads for maintenance;180s per statement and600s controller admission deadline. The earlier75GB planning cap would not cover the failed16.961GB CTAS plus33.157GB successful copy plus a full33.157GB rewrite. Explicitly revise the cumulative failed-build/successful-copy/maintenance planning ceiling to100GB, justified by the now-measured owned table size (expected combined83.275GB;36GB maintenance ceiling keeps this below86.118GB). This controls later-phase admission, not an in-flight statement or a billing cap. No additional copies or compute are admitted.

The64MiB target property is already recorded on the owned table. Azure documentation qualifies it as best effort, with only OPTIMIZE respecting targetFileSize on UC managed tables/SQL warehouses; CTAS/INSERT are automatically optimized. Measure actual file sizes/counts rather than assert the target was achieved. [Control data file size](https://learn.microsoft.com/en-us/azure/databricks/tables/tune-file-size) (updated2026-09-11), [OPTIMIZE syntax](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-optimize) (consulted2026-10-07) support partition predicates; no liquid-clustered source ZORDER is attempted.

Within maintenance bounds, check20M global IDs and bucket invariants. Complete20-field affected100k and all20M structural/full-wide post-maintenance checks are a separate validation phase if files changed; the maintenance cost cap does not silently include another wide scan. Record matched point reads at the final owned version, separating first-touch/remote and repeated warm samples. Scalar SLOs and cold-data, sustained-ingest/freshness, graph consumers and1B/5B admission stay open.

## r170–r175: full20M bucket maintenance and point-read comparison

[Comparison evidence](out/native/ashlar_bucket_full_reads_r175/comparison-summary.json) records four alternating30-key/60-query cohorts on canonical E23 and the owned full20M bucket copy. Every read returns the full20 carriers exactly. Before maintenance, bucket4 initial/repeated all-cohort caller p95 is818.284/493.483ms and engine p95 is593/232ms, with28/5 remote queries and2 files at p95. Canonical E23 has18 files at p95 and a different DV/hot-file history; this is an operational comparison, not causal isolation of partitioning.

Four partition-local ZORDER commands actually rewrite all528 input files/33,157,013,686 compressed bytes into448 files/33,156,068,142 bytes at owned8. Each16-bucket group takes75.903/77.901/79.137/79.839s. The64MiB best-effort target yields roughly74MB mean output files, not guaranteed64MiB files. Native commit IDs, before/after versions and64-partition coverage are audited in [maintenance evidence](out/native/ashlar_bucket_zorder_r172/audited-summary.json). The planned36GB new-write limit holds; the measured table footprint justified recording the100GB revised cumulative failed-build/copy/maintenance planning ceiling before mutation. OPTIMIZE history reports zero read bytes here despite rewriting33GB; input file footprint is recorded separately, and actual physical maintenance reads/read amplification are not independently measured. The reported0.939GB reads are check queries, not total maintenance IO.

After ZORDER, bucket8 initial/repeated all-cohort caller p95 is783.702/416.783ms and engine p95 is539/151ms, with29/2 remote queries and1 file/77.75MB at p95. Applying the same no-remote subset definition to every phase gives before-repeat n25 engine107/caller358.030ms and after-repeat n28 engine99/caller416.783ms. This is a scoped warm-engine observation near the100ms target; the caller250ms target remains missed. Remote samples also contain cached bytes, so neither initial phase proves a controlled cold-data gate. Sequential cache/load and canonical compilation variation prevent attributing timing differences solely to ZORDER. All240 queries are exact with finalized uncached native metrics; combined reader-cohort reads56.606GB, writes0 and spill0.

Input4 has full20M20-field parity. Output8 has20M global IDs/bucket integrity and120 exact after-maintenance point reads, but its exhaustive wide-value preservation obligation remains open. The candidate is retained unpublished at the same UUID for matched100k high-entropy updates and bounded full-wide final-state validation. Canonical r139-b1 pins remain unchanged; no DDL choice, source authority, graph-engine activation, sustained/burst publication or1B/5B admission changes.
