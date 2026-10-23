---
ddx:
  id: ashlar.evidence.r05-codec-snappy-edge-pair-plan-20261023
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
  - id: TD-007
    kind: informed_by
  - id: TD-008
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: ashlar.evidence.r05-end-to-end-budget-design-20261023
    kind: informed_by
---

# One-pair SNAPPY edge-write discriminator and disposition

**Status:** The proposed SNAPPY codec avenue is rejected under this bounded experiment. Attempt 01 stopped before SparkContext/table/write on a sandbox loopback-bind refusal. Attempt 02 completed the 225M ZSTD write and semantic readbacks but its supervisor failed closed on a permission-denied process-group probe; the retained write observation is separately documented. Attempt 03 reused that baseline and stopped the single SNAPPY candidate at the polled output threshold. It produced no completed INSERT/footer and no valid timing comparison. See the [attempt-01 failure](codec-pair-attempt-01-sandbox-refusal-20261023.md), [attempt-02 ZSTD observation](codec-pair-attempt-02-zstd-baseline-20261023.md), and [attempt-03 SNAPPY failure](codec-pair-attempt-03-snappy-output-cap-20261023.md). Do not retry or broaden this codec matrix. The selected `.4-xxhash64` ZSTD realization is unchanged; the few-minute end-to-end target remains open.

## Question and decision threshold

The original discriminator asked whether SNAPPY could bring the 225M-edge INSERT below **22.72 seconds**, while preserving the full Delta carrier and protocol. Attempt 03 hit the output threshold before INSERT completion, so the question has no timing answer and this codec avenue is closed within the approved envelope. The 22.72-second value remains only the arithmetic discriminator derived from the earlier 29.30133-second ZSTD reference and the retained 45M-node sample; it is not a result or a new setting.

The latest completed ZSTD edge INSERT observation is 25.962255 seconds in attempt 02; its supervisor outcome remains rejected due to process-group uncertainty, so the write time is a bounded observation, not a qualified case result. The earlier 29.30133-second reference made the 22.72-second discriminator a 22.5% reduction; against attempt 02 it would be 12.5%. Neither percentage is a SNAPPY result. The latest ZSTD rate projects 16.50 minutes for 1B/5B writes at 80%, still above the 15-minute owner-review trigger before reads or graph export. The owner's stronger few-minute end-to-end target remains unresolved.

## Exact pair definition

| Item | Pinned baseline | Isolated candidate |
| --- | --- | --- |
| Realization identity | Selected `ashlar-local-current/.4-xxhash64` profile | New, unpromoted `ashlar-local-current/.4-xxhash64-snappy-candidate-20261023` profile, versioned separately for this experiment |
| Runtime | Local Spark 4.2.0, Delta 4.4.1, Python 3.12.14, JDK 21.0.13.0.2, `local[18]`, 18 partitions, 24g driver | Identical pinned runtime and settings |
| Source and write | Retained attempt-04 `insert.sql`, exact executed SQL SHA-256 `088d365593b311cd9c75f14b9129cf4beaab24657e126a0d538de3fa99f2ff5e`, `range(225000000)` and the same `.4` edge projection | Byte-identical SQL and data expressions; table target differs only as required to make the effective writer codec SNAPPY |
| Delta semantics | Full Delta table and transaction protocol; selected `.4` schema, constraints, identity/value fields, statistics, and writer behavior | Intended to match except codec; attempt 03 stopped before a Delta write commit, so full semantics were not verified |
| Effective codec | ZSTD, verified in every output Parquet footer in attempt 02 | SNAPPY table property read back in attempt 03; no completed INSERT/footer to verify output codec |

The intended comparison used one baseline and one candidate, with only the effective codec changed consistently at the table/writer boundary; do not leave a ZSTD table property overriding a SNAPPY session setting. Attempt 02 supplied the baseline, and the continuation executed only one candidate. No other Spark, Delta, SQL, layout, partition, statistics, file-size, schema, or resource setting changed by plan. The candidate did not finish, so the pair is invalid and no timing interpretation is allowed.

Attempt 04 supplied the retained SQL/source identity. Attempt 02 completed the ZSTD write in 25.962255 seconds and its receipt hashes are retained. The candidate-only continuation verified the baseline receipt and Delta-log hashes, retained those hashes separately, and removed only the exact owned baseline warehouse before running SNAPPY. Attempt 03 stopped during INSERT at the output threshold; it never reached write completion, protocol/footer readback, or persisted-row witnesses. Its root and partial files remain preserved. The selected realization's warehouse was never targeted. The supervisor enforced a 300-second hard wall limit (termination at 290 seconds), polled output/diagnostic thresholds, and 12 GB free-disk admission; the candidate stopped for output after about 90 seconds with a reaped child and empty process inventory.

## Per-case bounds and stop conditions

- One Spark process at `local[18]` with the pinned 24g driver heap; execute cases sequentially and do not run unrelated Spark work concurrently.
- Require at least 12 GB free on the dedicated temporary volume before a case. Attempt 02's complete output was 2.59 GB, including 2,569,031,961 active Parquet bytes. The candidate monitor used 4,000,000,000 bytes as a polled stop threshold; this is not a filesystem quota or guaranteed maximum. Attempt 03's post-stop tree measured 5,669,004,782 bytes, 1,669,004,782 bytes above threshold. Preserve this overshoot as evidence; do not describe 4 GB as a hard storage bound.
- The diagnostic threshold was 128,000,000 bytes; attempt 03 used 1,442,423 bytes. Apply the 300-second hard wall cap to a continuation, with termination initiated at 290 seconds. Output/diagnostic thresholds are checked on 0.5-second polls; termination and reaping may add output after the first over-threshold sample. The observed final size does not reveal how much came between polls versus shutdown grace.
- Stop on any cap, timeout, unowned output, unexpected resource consumer, extra job/retry, or semantic/runtime mismatch. Preserve failure evidence and clean up only each case's owned temporary data after receipts are secured.

## Pair validity and interpretation

The planned pair would have required runtime/artifact identities, exact SQL digest, selected Delta version/protocol, full table schema/configuration, row count, footer geometry/codec and deterministic witnesses to match apart from the codec. Attempt 03 did not reach those acceptance checks. Its pre-write SNAPPY property/schema readback is not evidence of completed Delta output semantics. Do not relax constraints, omit columns/statistics, change partitioning, replace Delta with Parquet, or handcraft log actions. Predictive optimization must remain enabled on any later managed target; this local attempt makes no managed optimization claim.

No SNAPPY INSERT duration is available: attempt 03 failed before `insert_completed`, so it cannot be compared with ZSTD and the 22.72-second discriminator was not evaluated. The candidate exceeded the polled output threshold, so this codec avenue is rejected under the experiment's resource bound. No retry, promotion, reproducibility claim, or 1B/5B run follows.

The 15-minute threshold applies only to owner review of the row-linear write estimate. Total acceptance remains open and must include startup, table DDL, node and edge writes, two exact point lookups, two persisted-table scans, GraphFrames mapping/action and graph export, validation, and cleanup. Full-carrier scans and canonical graph-action timings are currently unbudgeted, and a codec-only write gain cannot close them. The owner’s few-minute end-to-end goal remains the stronger unresolved requirement.

## Evidence basis and limits

The retained [attempt-04 receipt](../edge-write-attribution-20261023/attempt-04/experiment.json) used the exact SQL digest and reported 18 ZSTD Parquet files with 2,569,031,961 active bytes. Its INSERT interval was 35.569 seconds inside a 54.4-second JFR-instrumented run; it is diagnostic, not the unprofiled 29.30133-second write timing in the pinned [18-core calibration](../current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-18core-calibration-20261021.json). The later [attempt-02 observation](codec-pair-attempt-02-zstd-baseline-20261023.md) completed the same 225M ZSTD INSERT in 25.962255 seconds but retains a failed supervisor acceptance for process cleanup. The [attempt-03 record](codec-pair-attempt-03-snappy-output-cap-20261023.md) documents the SNAPPY output-threshold stop; it gives no comparative timing. The [R05 budget synthesis](README.md) uses the latest ZSTD observation for revised write-only arithmetic and retains the unbudgeted read, scan, graph action, and cleanup work. No managed Databricks behavior, UC policy/grant, source publication, production entropy, target-scale behavior, or end-to-end pass follows from these local observations.

## Inert runner and review command

The checked-in runner reuses attempt-04's fixed SQL/profile validation and event attribution, adds a codec argument whose default remains ZSTD, requires the selected Python/wheel/Delta JAR hashes, and checks the generated table property and Spark Parquet writer setting agree. Both supervisors pin the reviewed runner source SHA-256 `b132dcf5d4d80fdaec1c47cdaee9980b3ba4a1ee3291ecf95f8d73b201f74003` and expected executed INSERT SHA-256 `088d365593b311cd9c75f14b9129cf4beaab24657e126a0d538de3fa99f2ff5e`. The runner reads the canonical Spark table schema and actual Delta protocol/metadata actions, footer codecs and five deterministic persisted-row witnesses. The candidate-only supervisor verifies the immutable baseline receipts, process inventory, exact baseline table path, and Delta log hashes; it checks its deadline before launch and final acceptance and is gated by an explicit review flag. Run the offline-only configuration check with:

```sh
python3 docs/helix/04-build/evidence/r05-end-to-end-budget-design-20261023/codec_pair_static_check.py
```

The one authorized candidate-only run has completed and its output threshold was exceeded. Do not invoke the candidate supervisor again. The offline static check above remains available; this plan retains no further execution command.

The attempt-01 failure, attempt-02 ZSTD observation, and attempt-03 SNAPPY partial output are preserved in separate one-use roots and evidence notes. The current selected ZSTD write-only estimate is 16.50 minutes at 80% of the attempt-02 edge rate, above the 15-minute owner-review trigger. The review trigger is not release acceptance. No valid SNAPPY timing exists. Load, exact point lookups, scans, graph export/action, validation, cleanup, and the owner's few-minute end-to-end target remain open.
