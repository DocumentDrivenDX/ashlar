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

# One-pair SNAPPY edge-write discriminator (inert plan)

**Status:** Prepared for review; not run and not an authorization to run. This is one bounded, local codec experiment for the R05 write-time question. It does not alter the selected `.4-xxhash64` ZSTD realization or qualify the few-minute end-to-end target.

## Question and decision threshold

Does changing only the effective Parquet compression codec from ZSTD to SNAPPY reduce the retained 225M-edge INSERT below **22.72 seconds**, while preserving the complete Delta carrier and protocol semantics? This is a deliberately discriminating sample threshold: with the retained 45M-node write time unchanged at 9.67956 seconds, 80%-of-measured-rate arithmetic gives 268.877 seconds for 1B nodes. A 5B-edge write below the 15-minute write-review line then requires an edge sample below `(900 - 268.877) / ((5,000,000,000 / 225,000,000) / 0.8) = 22.72 seconds`.

The current ZSTD edge observation is 29.30133 seconds. The threshold therefore asks for at least a 22.5% reduction against that single reference. It is a review discriminator, not a stable performance claim, a hard runtime limit for the product, or evidence that a larger run should proceed. Falling below the 15-minute write-only review line still would not meet the owner's stronger “few minutes” end-to-end target.

## Exact pair definition

| Item | Pinned baseline | Isolated candidate |
| --- | --- | --- |
| Realization identity | Selected `ashlar-local-current/.4-xxhash64` profile | New, unpromoted `ashlar-local-current/.4-xxhash64-snappy-candidate-20261023` profile, versioned separately for this experiment |
| Runtime | Local Spark 4.2.0, Delta 4.4.1, Python 3.12.14, JDK 21.0.13.0.2, `local[18]`, 18 partitions, 24g driver | Identical pinned runtime and settings |
| Source and write | Retained attempt-04 `insert.sql`, exact executed SQL SHA-256 `088d365593b311cd9c75f14b9129cf4beaab24657e126a0d538de3fa99f2ff5e`, `range(225000000)` and the same `.4` edge projection | Byte-identical SQL and data expressions; table target differs only as required to make the effective writer codec SNAPPY |
| Delta semantics | Full Delta table and transaction protocol; selected `.4` schema, constraints, identity/value fields, statistics, and writer behavior | Same full Delta semantics, protocol, schema, constraints, statistics, and properties except the single codec setting |
| Effective codec | ZSTD, verified in every output Parquet footer | SNAPPY, verified in every output Parquet footer |

Run exactly one baseline and one candidate, sequentially, each in a new isolated output root. In the candidate, change the actual effective codec consistently at the table/writer boundary; do not leave a ZSTD table property that overrides a session SNAPPY setting. No other Spark, Delta, SQL, layout, partition, statistics, file-size, schema, or resource setting may change. If the runtime cannot establish that only the effective codec changed, or any non-codec profile value differs, stop and do not interpret the pair.

The existing attempt-04 record is the baseline reference, not a request to rewrite it. If a fresh pair is later authorized, retain the exact harness/source identity and create fresh baseline and candidate roots; do not point the candidate at or mutate the selected realization's warehouse. This plan contains no harness/code change and no launch command.

## Per-case bounds and stop conditions

- One Spark process at `local[18]` with the pinned 24g driver heap; execute cases sequentially and do not run unrelated Spark work concurrently.
- Require at least 12 GB free on the dedicated temporary volume before each case. The retained attempt-04 output tree was 2.60 GB, including 2.569 GB active Parquet; cap each case's output tree at 4,000,000,000 bytes. Do not retain both warehouses concurrently; preserve only the receipts and diagnostics needed to compare them.
- Cap diagnostics at 128,000,000 bytes per case. Apply the existing bounded supervisor policy: 300-second wall cap with termination initiated at 290 seconds. These are enforced by the supervisor only if it can actually terminate the process and account for owned output; otherwise refuse to start.
- Stop on any cap, timeout, unowned output, unexpected resource consumer, extra job/retry, or semantic/runtime mismatch. Preserve failure evidence and clean up only each case's owned temporary data after receipts are secured.

## Pair validity and interpretation

For both cases, retain the runtime and artifact identities, exact executed SQL digest, selected Delta version and protocol, full table schema and relevant table properties, Delta history, row count, footer row count/file count/active bytes/codec, and the bounded supervisor receipt. Require 225,000,000 rows, identical `.4` logical schema and values, independently checked row/endpoint witnesses, and identical Delta reader/writer protocol and feature set. Require all output files to report the intended codec (ZSTD baseline, SNAPPY candidate). Any mismatch invalidates the comparison. Do not relax constraints, omit columns/statistics, change partitioning, replace Delta with Parquet, or handcraft log actions. Predictive optimization must remain enabled on any later managed target; this local pair makes no managed optimization claim.

Record actual INSERT duration and output bytes for each case. The candidate crosses this discriminator only if it is below 22.72 seconds and passes all pair-validity checks. A single pair can justify only a follow-up review; it cannot promote the candidate, establish reproducibility, or authorize a 1B/5B run. A result at or above 22.72 seconds closes this proposed codec avenue without a broader codec matrix under this packet.

The 15-minute threshold applies only to owner review of the row-linear write estimate. Total acceptance remains open and must include startup, table DDL, node and edge writes, two exact point lookups, two persisted-table scans, GraphFrames mapping/action and graph export, validation, and cleanup. Full-carrier scans and canonical graph-action timings are currently unbudgeted, and a codec-only write gain cannot close them. The owner’s few-minute end-to-end goal remains the stronger unresolved requirement.

## Evidence basis and limits

The retained [attempt-04 receipt](../edge-write-attribution-20261023/attempt-04/experiment.json) reports one 225M-edge ZSTD INSERT, 29.30133 seconds, 18 Parquet files and 2,569,031,961 active bytes. Its 54.4-second overall run included JFR setup/inspection overhead; use the INSERT wall interval for this discriminator. The [R05 budget synthesis](README.md) records the 80%-rate estimate and missing end-to-end components. Existing instrumentation observed Parquet statistics/comparison and UTF8 conversion activity but did not establish whether compression or storage is causal. SNAPPY may reduce CPU time while increasing bytes and IO; this pair measures only the net result on this exact local sample. No managed Databricks behavior, UC policy/grant, source publication, production entropy, target-scale behavior, or end-to-end pass follows from the result.
