# Broadcast matching for scattered publication

2026-10-05; SPIKE-001 experiment, not final layout or sustained-throughput admission.

## Intervention and correctness

`native_broadcast_validation.py` replaces canonical LEFT joins in the prior/post checks with inner matching of the staged identity set, using BROADCAST(s), and BROADCAST(j) for the bounded journal side of the post check. Missing canonical identities are rejected through **both total and distinct typed-identity cardinality** checks. Distinct count prevents missing identities being masked by compensating duplicates. Exact prior checks cover all 13 canonical columns; post checks preserve stable columns and verify new property text, entity version, source ordering, journal identity/presence and decoded exact old/new values. A separate journal count rejects extra events outside the stage. No hashes substitute for carrier equality.

Before writes, twelve native virtual-fixture controls passed using the actual post-guard expression: valid state accepted; missing canonical, missing journal, duplicate canonical, duplicate journal, retained-content loss, bad old/new tokens, null token, wrong presence, wrong property, and a balanced missing-plus-duplicate canonical case rejected. These test guard logic on two rows, not complete production recovery. Evidence: `out/native/ashlar_broadcast_guard_controls_20261005_n3/`.

## Measured native batch

Existing dbw-aidev-cus data-gateway serverless Photon PRO 2X-Small warehouse; no compute/global-preview changes. The private n1 canonical fixture is reused at version **7**: 10M nodes, 435 files, 6,861,293,621 bytes. Selection `400000<=pmod((id-1)*104729,10000000)<600000` updates a disjoint 200k IDs across all 100 key-range segments. New property 103 has ~2KB deterministic varied text; full retained content stays exact. One batch models 20-second accumulation at 10k changes/s. Producer construction is excluded; staging, atomic checks/writes, actual-version discovery and manifest insert are timed.

Oldest freshness **45.55s**, newest **25.55s**, processing **25.54s**. This passes the provisional 60-second freshness screen for **one batch**. Atomic wall **19.40s**, reported execution **19.09s**. Aggregate read bytes **16,330,188,010**, rows scanned **42,800,000**, disk-cache percentage **94%**, remote read bytes **933,908,418**, no spill reported. The preceding fused run had 63% cache residency and different prior mutations; sequential timing is not a paired causal estimate of the join intervention alone. Aggregate rows/bytes remain large despite the latency improvement.

Intermediate MERGE history: **4.455s**, 200k updates, zero copied/inserted/deleted rows, 419 deletion vectors updated, eight files / 214,309,059 bytes added. Post table detail: **443 files, 7,075,602,680 bytes**. Published vector: canonical **8**, journal **3**. All seventeen recorded statements succeeded; the harness completed. Raw evidence: `out/native/ashlar_broadcast_validation_20261005_n3/`.

Independent post-publication checks pass for all 200k changed canonical/journal rows, all 9.8M untouched rows across all 13 canonical fields against version 7, 10M distinct typed canonical keys and 200k distinct journal event keys. These verification queries are outside freshness timing.

## Plans and next gate

Read-only post-run EXPLAIN plans in `out/native/ashlar_broadcast_validation_plans_20261005_n3/` show Photon broadcast inner matching and a broadcast left-outer journal join. They are post-run adaptive initial plans, not historical final runtime profiles. The plans support further measurement, not a guarantee about every runtime or batch size.

The 25.54-second serial processing duration exceeds the modeled 20-second interarrival period. Therefore one passing batch cannot admit sustained 10k/s. Next test overlapping staging and modestly larger batches under a fixed schedule, preserving the original arrival clock and every integrity check; stop on bounded backlog. Then measure reads during those publications and burst behavior. Do not admit billion-node scale from high-cache 10M results: broad scattered validations still scan substantial data.

CatalogManaged Beta dependency, synthetic whole-entity feed semantics, native Truss adapter/replay/deletion/recovery gaps, external graph integrations and missing attributable billing remain explicit. No 1B-node/5B-edge, cold-cache, concurrent-read or population-p95 claim follows. The full goal remains active.
