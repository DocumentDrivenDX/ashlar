# Scheduled fenced multi-domain publication

2026-10-05; SPIKE-001 native experiment on unchanged existing dbw-aidev-cus data-gateway PRO 2X-Small compute.

Four disjoint 200k-edge property batches arrive every 20 seconds (provisional 10k/s). Prior full-carrier checks, native data/journal/receipt writes, version checks, changed canonical/journal-count verification and guarded descriptor publication all occur inside the measured processing clock. Stages are prebuilt outside the clock; no simultaneous readers. This is a favorable pipeline boundary, excluding source capture/staging and separate exhaustive post-run verification. It still fails rate/freshness.

| Batch | Processing s | Queue s | Oldest freshness s |
| --- | ---: | ---: | ---: |
| 1 | 30.611 | 0.010 | 50.621 |
| 2 | 33.064 | 10.622 | 63.686 |
| 3 | 33.498 | 23.687 | 77.185 |
| 4 | 33.641 | 37.186 | 90.826 |

Three of four oldest-freshness observations exceed 60s. Every processing interval exceeds 20s arrivals; queue grows to 37.19s. These are four consecutive fixture batches, not a stable population p95. Do not admit sustained throughput from the initial passing batch or ignore descriptor/validation overhead.

Data atomic caller durations 18.761,19.595,19.896,20.093s; corresponding engine 18.431,19.242,19.545,19.743s. Guarded descriptor caller 2.940,2.951,3.230,3.257s. Last descriptor engine telemetry was initially missing and must not become zero. Remaining clock includes native version and canonical/journal cardinality checks; stages remain excluded. This is not a controlled marginal fencing-cost comparison.

All four publications complete, advancing canonical edges 7..10 and journal 5..8, with adjacency 2, degree 1, tombstones 1 and nodes 3 fixed. Each changed canonical batch validates all preserved fields, new bags/version/origin and 200k distinct keys. The timed checks validate 200k unique journal ordinals per batch, but full journal token semantics and exhaustive final untouched parity require the separate verifier's terminal result. Publication IDs are r13-1..4 in the private catalog-managed manifest; complete receipt vectors drive each.

Original r13 preparation stopped before publication because its second band included a prior deleted edge (199,999 rows). Original evidence retained. Corrected r13r1 uses native-ID bands 1,3,4,6 across ten domains, excluding all earlier deleted keys; all stages contain exactly 200k distinct keys. Harness `SPIKE-001-table-layout/native_fenced_scheduled.py`; raw terminal evidence `out/native/ashlar_fenced_scheduled_20261005_r13r1/`. Separate verifier `verify_fenced_scheduled.py`, evidence `out/native/ashlar_fenced_scheduled_verify_20261005_r13r1/`.

## Disposition

This existing-compute candidate does not meet sustained 10k/s or freshness with integrated checks/fencing, even without concurrent readers. Preserve failure evidence. Next profile avoidable repeated validation/version/descriptor scans and test a revised integrated batching clock; post-write lookup pruning and realistic multi-key concurrent latency also need verification. Resource bounds remain pending for new compute/in-region/billion-scale runs. Exact carriers, UMF deferral and external graph limitations remain governing requirements. Goal active.

## Separate preservation result

The verifier passes: all 10,019,981 typed identities remain distinct; all **9,219,981 untouched rows preserve all 17 fields**; all **800k journal values and metadata** match stage expectations with unique full origin keys; all four descriptor vectors, scheduled progress and revisions read back exactly. Changed canonical preservation is checked at each timed publication. This strengthens correctness evidence without changing the failed timing result. Exhaustive unchanged checks are final-state checks, not intermediate-release checks.

## Clock attribution

Own recorded caller intervals show changed canonical validation takes **7.360–8.830s** per batch, native version checks total roughly **0.925–1.442s**, and journal cardinality **0.619–0.753s**. Raw breakdown: `out/native/ashlar_fenced_scheduled_20261005_r13r1/clock-breakdown.json`. The post-change join lacks the explicit staged-side broadcast hint used in prior checks. A controlled hint/fusion comparison can retain all exact predicates and cardinality semantics; removing the check would weaken publication and is not admitted. Even eliminating this check leaves data apply plus fenced publication at roughly **21.70–23.35s**, already above 20s arrivals; metadata/check tuning alone cannot establish this rate with the observed atomic timings. This sum is arithmetic over observed caller intervals, not a tested optimized pipeline. Larger batches or overlapping phases require new actual rate evidence, not this projection.
