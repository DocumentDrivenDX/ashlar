# Native reader/publisher compute comparison candidate

Canonical UC Delta and CONTRACT-003 remain fixed. Prior r103/r106 evidence
misses provisional warm budgets. Parameterization did not reduce latency.
This plan qualifies resource separation, not a replacement architecture.

## Concrete resource scope

Publisher: existing `data-gateway` warehouse `2439e1f2e37ac563`, serverless
2X-Small, min/max one cluster. Reader: a distinct existing authorized warehouse
or one disposable serverless 2X-Small warehouse, min/max one cluster, ten-minute
auto-stop. Cap the experiment at twenty minutes of added running compute;
stop/delete only the owned disposable resource after collecting final histories.
Do not resize, stop or change the existing shared warehouse. Dollar attribution
is unavailable; this is a resource/time bound, not a dollar cap. Workspace
inventory currently has only the publisher warehouse and no classic clusters.

## Required workload and measurements

1. Bind the reader to the exact r103 publication E16 plus its independent staged
   full20-field oracle. Revalidate manifest and table histories before any write.
2. Warm the same 30-key opaque-token cohort on both reader targets; record warmup
   separately. Disable result cache and read the setting back in every session.
3. Run one causal 100k large-token publication, raw and journal appends on separate
   publisher connections, then canonical MERGE, independent token/instant/origin
   validation and manifest readback. Derive next versions from actual histories;
   abort unexpected lineage. Pre-staged finite input is not sustained-rate evidence.
4. Run simultaneous closed-loop full-field singleton readers on shared and isolated
   compute against the same old publication throughout publication. Each reader
   owns its connection; compare every returned row with the independent stage.
5. Record exact target IDs, statement IDs, final uncached histories, caller, server,
   compilation, execution, queue timestamps, bytes/files/remote I/O and overlap.
   Compare both readers during the same publisher window and their own idle
   baselines. Two readers add load relative to r103; disclose that difference.
6. Compare fresh published rows on both targets against the new input stage.
   No architecture/SLO admission from this cohort; separate cold, sustained, burst,
   skew and 1B/5B qualification remains required.

## Transport readiness and recovery

`Client(..., warehouse_id=...)` and `DriverClient(..., warehouse_id=...)` now
route explicitly and record target IDs. Default remains the existing warehouse.
Offline routing checks verify REST request and Bolt-independent SQL driver paths
without authenticating or provisioning. No isolation run has executed yet.

On observation timeout inspect the existing statement handle; never replay writes.
Any cleanup must account for live queries before removing owned compute. Preserve
the completed source stages, manifest vectors and evidence; no VACUUM or retention
change. The existing source authority/fencing gaps remain explicit.

## Prepared exact-reader lane

`isolation_reader.py` loads the recorded independent r103 stage oracle, validates
30 complete20-field rows and the pinned manifest vector, and provides a bounded
closed-loop reader with explicit full identity predicates and native decimal
string parameters. Each compute target must own its driver connection on the
thread running that lane. A preservation failure sets the shared stop event and
propagates; missing, extra or changed carriers cannot become successful timings.
Read-count and admission-time bounds do not interrupt an already running query;
the session statement timeout bounds execution separately.

Four offline failure-control tests pass, including missing/extra/corrupt data,
shared stop propagation and stop/count limits. These prove lane guard behavior,
not compute isolation performance. Live preflight and a dual-reader publisher
controller still need integration and execution. Temporary compute authorization
is pending; no second warehouse has been provisioned.

`isolation_controller.py` now coordinates both exact reader lanes: each owns
its client on its worker thread, preflights independently and completes an idle
cohort before the coordinator releases load and invokes the supplied publisher
callback once. Distinct target IDs and evidence directories are mandatory.
Reader or publisher failure sets stop; final histories and connection closure
run on each lane. Post-publication reads run only after publisher success.
Four offline controls verify preflight failure prevents publication, publisher
failure does not retry, successful target routing/closure and same-compute refusal.
No provisioning, live comparison or production publisher callback is included.
The next integration must adapt the causal100k publisher with fresh lineage
checks, invoke stop checks between phases and audit actual overlap from statement
intervals. Callback completion alone does not prove sustained ingest or reader SLOs.

## Integrated r107 runner

`isolation_publication_r107.py` supplies the causal100k synthetic publisher to
the dual-reader controller. It preserves the existing raw/journal/independent
microsecond wire/exact-current/global-ID checks, advances entity10 to11 and
checks newly published rows on both targets against the staged oracle. Readers
use an explicit parameterized publication-ID binding. Historical scripts remain
unchanged. No provisioning or automatic retry occurs in this runner.

Actual `--preflight-only` evidence under
`out/native/ashlar_isolation_r107_preflight/` passes complete lineage and descriptor
checks at edge16/raw12/journal14. All eleven statements are SELECT/DESCRIBE/session
SET; no staging or data writes occurred. Query-history metrics initially lagged;
the same IDs were refreshed and all are now final, without query resubmission.
Eight reader/controller guard tests and runner syntax checks pass. Full publisher
writes and dual-compute performance remain unexecuted pending resource approval.
This preflight is not source-authority, performance or billion-scale admission.


### r183–r184: two-client point reads and caller controls

Two independent persistent clients execute ten synchronized pairs of exact20-field LC1 reads using20 distinct updated identities and ten SELECT1 control pairs. Owned UUID/version is revalidated; expected values come from the already recorded independent immutable stage, avoiding another oracle scan. Result caching is disabled. Native query-start/execution-end windows overlap for all10 point and all10 control pairs; this does not prove simultaneous task execution or saturation. Polling outside timed calls adds idle gaps, so this is a burst-pair experiment, not steady load.

Point p95: engine197ms, caller443.288ms, compilation162ms,8files/403.803MB; remote reads0/20. SELECT1 p95: engine25ms, compilation26ms, caller149.983ms. Caller-minus-native-total residual p95 is124.784ms for points and97.532ms for controls; these are per-query differences, not subtraction of unrelated percentiles or a pure network measurement. Both point gates remain failed. Capacity-wait duration is absent from native histories; original summary incorrectly defaulted it to zero, and the separate audit explicitly records unknown while preserving the original evidence.

All native IDs succeeded and finalized. Total7.700GB reads/0 writes/0 spill fits10GB; the controller reserves1GB before each next pair and checks cumulative final telemetry between pairs. The reserve is an estimate, not an in-flight spending cap. No canonical publication, maintenance, resize or new compute occurred. Evidence: `out/native/ashlar_lc_concurrent_r183/audited-summary.json`.

Design implication: retain canonical hash LC, but do not advertise the provisional warm SLO as achieved. Candidate-file amplification persists for updated rows even with no remote reads. Caller/server residual and compilation consume significant budget; an in-region application measurement and reduced candidate bytes are separate qualification work. Native task saturation and capacity queueing remain unmeasured. Next investigate whether publication hot-file shape can reduce singleton amplification without adding mandatory full-table maintenance to every batch; measure that change together with raw/journal/current publication timing, not MERGE throughput alone.


### r204–r209: attribute remaining limits; shared telemetry qualified without an SLO win

The saved-evidence calculator measures each serial reader query's components separately. Compilation p95138/150ms and caller-minus-native-total107.750/104.481ms accompany engine95/96ms and caller348.750/340.264ms. Component percentiles must not be added. Per-query subtraction of compilation gives a202.730–206.265ms p95 sensitivity only if all other timings remain unchanged; it is not an implemented optimization, an achievable plan-cache claim or a revised caller gate. Caller reduction needed to250ms is90.264–98.750ms p95. File-size tuning alone cannot be assumed to deliver that reduction.

Unioning overlapping local SQL-call intervals covers43.506s of the59.883s overlap controller and45.995s of62.468s serial;16.377/16.473s remain outside those intervals. These gaps include history/telemetry, Python work and controller waits, assuming no local clock jump; they are not measured removable overhead and cannot be subtracted to advertise passing freshness. Evidence: `out/performance-attribution-r204.json`, with hashed exact source artifacts.

Read-only compute inventory at2026-10-07T06:23:06Z still returns one RUNNING data-gateway warehouse2439e1f2e37ac563,2XSmall/serverless,min/max1,auto-stop10m, no listed classic clusters. No configuration/provisioning is changed. This snapshot does not grant arbitrary job-compute access or measure an in-region caller.

The shared history collector gathers all lane IDs in a bounded paginated scan, refuses unknown/duplicate/client-failed/native-failed evidence, distinguishes pending history/final metrics from terminal failure, and retains nonfinal metrics explicitly when completion-only mode is requested. Native GET-only validation resolves38 real publication IDs with final histories in one page/722.667ms; no SQL workload or mutation occurs. Fourteen focused helper/proof tests pass. Final-cost publication collection continues requiring is_final=True; completion-only mode is not used to price costs or advance this publisher.

A new owned-clone publication keeps the same immutable stage, scoped intended/6-range current input, serial-current schedule and exact raw/current/journal/structure/manifest barrier. Only repeated per-lane telemetry scans become shared scans, with pending-only polling. All20 changed fields, exact raw wire/origin and journal,20M structural/identity,19.9M unchanged physical custody and manifest readback pass. Native output remains6 narrow files (14.8741–19.7827% live hash-domain spans), without full-table maintenance. Canonical r139-b1 unchanged.

Observed processing64.647s gives modeled record-age p9574.150s, missing60s and worse than prior71.973s. Timed write phase24.055s versus20.362s and validation19.442s versus17.951s also run slower. Shared collection inside the actual publisher clock is9.475s across three phases, each needing two attempts. This is a tested instrumentation efficiency change, not a demonstrated end-to-end freshness improvement or causal resource claim. The final38-record collection follows post-structural parity outside the publisher clock; the original batch-presence flag mislabeled it inside. Audited metadata corrects that classification while preserving raw summary and executed worker source; the script's future flag is corrected too.

Publication plus custody16.008GB reported reads/1.622GB writes, output-shape4.786MB reads/0 writes/0 spill, fits30GB/3GB and2GB shape bounds. All native IDs succeed/finalize; no duplicate mutation, new compute, resize or altered semantic checks. Evidence: `out/native/ashlar_history_collector_r206/summary.json`, `out/native/ashlar_queue_shared_history_r207/audited-summary.json`, `out/native/ashlar_publication_shape_r209/summary.json`.

Next pursue a bounded transport/compilation comparison using the same published snapshot and exact full-field oracle before another complete publisher. Preserve original250ms caller/60s freshness and1B/5B goals; no counterfactual or narrow warm cohort admits service, controlled cold, sustained10k/s/100k/s recovery or production source fencing. UC Delta/hash LC remain selected/proposed, graph-engine support remains scoped, and UMF binding deferred.
