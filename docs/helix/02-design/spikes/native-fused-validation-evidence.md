# Fused scattered-update validation evidence

2026-10-05; SPIKE-001 experiment, no approval or scale admission.

The previous scattered 200k-change run on 10M nodes missed freshness at 110.96s. This intervention combines prior cardinality/full-carrier checks in one query, and changed canonical/journal exact-value checks in one query. A separate cheap journal count rejects extra events outside the staged identity set. LEFT JOIN and null-safe comparisons explicitly detect missing canonical/journal rows. All 13 prior canonical columns are checked; post checks preserve unchanged columns and verify new properties, entity version, ordering and journal identity/presence/exact decoded values.

## Native results

Harness `native_fused_validation.py` reuses the isolated n1 canonical table. Before version **6**, 427 files, 6,646,989,936 bytes. It changes a disjoint 200k IDs selected by `200000<=pmod((id-1)*104729,10000000)<400000`, again spanning all 100 contiguous 100k-ID segments. Prior n1 changes remain present; this is a sequential comparison on the same shared existing PRO 2X-Small warehouse, not a clean randomized paired experiment. The batch carries the same ~2KB varied property payload model, with producer construction excluded and staging/publication checks/version discovery/manifest included.

One 20-second accumulation at modeled 10k/s completed with **93.46s oldest freshness / 73.46s newest freshness**, still failing the 60s gate. Atomic wall time **67.51s**, reported execution **67.17s**; aggregate read bytes **17,544,353,514**, rows scanned **42,456,708**, cache percentage **63%**, no spill reported. Seventeen recorded statements succeeded. Intermediate MERGE history reports **5.03s execution**, 200k updates, eight files / 214,303,685 bytes added, zero copied/inserted/deleted rows and 419 baseline deletion vectors updated. Post table detail: 435 files, 6,861,293,621 bytes. Aggregate scripting metrics must not be attributed solely to MERGE or treated as child-statement timings.

Publication captures canonical version **7** and journal version **2**. Independent post-publication queries passed: all 200k changed canonical/journal rows; all 9.8M untouched rows compared across every canonical column against version 6; 10M distinct canonical identity tuples; 200k unique journal event keys. These checks ran outside freshness timing. Raw evidence: `SPIKE-001-table-layout/out/native/ashlar_fused_validation_20261005_n2/`.

## Adversarial guard controls and plans

`test_fused_validation_guards.py` uses the actual post-guard expression extracted through AST from the harness, against two-row virtual SQL fixtures. Eleven controls pass: valid state accepted; missing canonical, missing journal, duplicate canonical, duplicate journal, retained-content loss, bad old token, bad new token, null token, wrong presence and wrong property rejected. It tests the relational guard expression, not transactional fault recovery or complete production conformance. The first local assertion used lowercase boolean text while the connector returns `False`; that attempt's output is retained. Normalization was corrected and the complete controls executed in `ashlar_fused_guard_controls_20261005_n2r1/`; no writes were retried.

Read-only `EXPLAIN FORMATTED` plans are retained in `ashlar_fused_validation_plans_20261005_n2/`. Both prior and post plans show Photon shuffled hash outer joins, shuffling canonical and stage inputs. The plans are from post-run state and remain adaptive initial plans, not reconstructed final runtime profiles. This is evidence for the next intervention, not proof of isolated shuffle cost.

## Next intervention and limits

Test inner matching with a broadcast staged set and **both total and distinct typed-identity cardinality checks**. Merely switching LEFT to INNER would weaken missing-row detection; distinct identity count must also prevent duplicates compensating for missing rows. The post journal lookup and exact-value checks must remain complete. Measure the resulting publication clock; do not assume a join hint succeeds or preserves performance at larger batches.

One batch does not prove sustained/p95 throughput, burst admission, concurrent reader performance, external graph protocols or 1B-node/5B-edge scale. The warehouse, static synthetic whole-entity source profile, catalogManaged Beta transaction dependency, native Truss feed/recovery unknowns and missing attributable cloud cost remain qualified as in the baseline. The full table-layout goal remains active.
