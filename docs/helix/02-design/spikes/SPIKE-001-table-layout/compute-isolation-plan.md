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
