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
