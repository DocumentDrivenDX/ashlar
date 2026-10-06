# Local and Databricks 24-million-carrier comparison

Owner authorization: large local test followed by Databricks (2026-10-06).
Governed by CONTRACT-003 and ADR-001. Unity Catalog Delta remains selected.

The native layout avoids the severe scattered-update rewrite seen in OSS Delta.
It does **not** meet the provisional singleton latency budgets on this compute.
A maintenance invocation made no physical change. Full graph publication,
consumer engines and billion-scale admission remain unqualified.

## Dataset and resources

Four million objects and twenty million independent edges use the complete 0.3
current-carrier column surface, including exact property/retained strings,
source cursor and delivery references. Eight property shapes include empty/null,
large integers, exact decimal/time text and variable SHA-derived strings.
Digest blocks repeat within strings and compress heavily. Stored current widths
are near 100 bytes, not a realistic-width or independent-random-byte assumption.
Native full-field bidirectional multiset checks pass across all 24M rows; all
20M typed source/target tuples close over the fixed 4M-object snapshot.

The workload has 400k distinct source nodes, target nodes and endpoint pairs.
Most vertices are isolated; endpoint pairs repeat, and object/edge ID ranges
overlap rather than using Truss shared allocation. This is a generic Ashlar
physical fixture, **not** legal native Truss unique-pair producer input, a real
consumer distribution or a real feed. Do not collapse its independent edge IDs.

Local: macOS, 128GiB RAM, 18 logical CPUs, approximately 3.3TiB free disk;
Spark 3.5.3 / Delta 3.2.1 / Java 17, local[8], 32GiB driver. Hash-range-sort
into 256 files per table with Parquet Zstd. OSS rejects the Databricks-only
file-target/compression properties. Current and retained temporary data use
8.1GiB by `du` after correction; data remains recoverable.

Native: existing `data-gateway`, 2X-Small, one cluster, no resize/new compute;
DBSQL 2026.38, history reports Runtime 19.8.x aarch64 Photon / Scala 2.13.
Hash liquid clustering, Zstd and declared 64MiB target. Actual initial files:
4 objects / 17 edges; bytes 378,561,211 / 2,024,478,320. Mean files exceed the
64MiB target. The target is not a hard maximum or an assumed observed mean.
Reader3/writer7 includes clustering, deletionVectors, rowTracking and v2Checkpoint.
External-reader compatibility still requires independent qualification.

These CTAS tables test the column surface and physical settings; they do not
prove production NOT NULL enforcement or instantiate all durable graph roles.
Exact billing dollars are unavailable. Reported bytes exclude Delta logs,
DV sidecars, source/history/projections and retained versions on Databricks.

## Write and update observations

| Measurement | Local | Databricks |
| --- | ---: | ---: |
| Initial 4M-object write, caller | 9.58 s | 23.85 s |
| Initial 20M-edge write, caller | 24.81 s | 55.51 s |
| First 200k scattered-edge MERGE, caller | 18.37 s | 12.22 s |
| First MERGE unchanged rows copied | 19,800,000 | 0 |
| First MERGE output rows | 20,000,000 | 200,000 |
| First MERGE bytes added | 2,074,615,472 | 20,197,944 |
| First MERGE original files affected | 256 removed | 17 deletion vectors |
| Corrected property-map repair, caller | 24.52 s | 5.45 s |

The first update fixture mistakenly wrapped old properties under a non-ID
`previous` key. Its timings qualify carrier/file behavior only. The generator
was corrected to add property 107 while retaining existing keys/values; 200k
corrected rows match exactly on both systems and all 20M maps pass numeric-ID-key
validation. Corrected state is Delta version 2. Initial generation is pinned by
commit f99fce6 and native SQL logs; the correction is separately recorded.

Initial native MERGE operation metrics report 10.5s execution, zero unchanged
copies and 17 DVs; SQL-history engine time is 11.44s. These measures have different
boundaries and should not be interchanged. No history/source/manifest work or
arrival schedule is included. Neither MERGE proves sustained 10k/s, 100k/s burst
publication or the 60s freshness target.

## Corrected native singleton reads

Persistent SQL connector 4.3.0 sessions disable result caching. Every one of the
following 100 query-history records is final, uncached and reads version 2 with
hash plus full source/relationship/id predicates and all carrier columns.
Nearest-rank p95; 50 samples per shape. Remote bytes are zero throughout.

| Corrected version-2 shape | Engine p95 | Caller p95 | Files/read | Read-byte p95 |
| --- | ---: | ---: | ---: | ---: |
| Serial | 218 ms | 525 ms | 2 | 170,652,229 |
| Four persistent clients | 247 ms | 570 ms | 2 | 170,652,229 |

Both provisional warm budgets (100ms engine / 250ms caller) fail. These are warm
file reads, not cold-data proof or concurrent-ingest admission. The REST control
hit result cache despite a separate SET statement; it is retained but excluded.
The valid persistent-session measurements replace it. Local first-version
read p95 was 304ms serial / 782ms concurrent, but predates the map correction and
is retained as physical comparison only.

## Maintenance result and design consequences

One `OPTIMIZE ... FULL` on edge version 1 took 4.07s caller and scheduled no data
rewrite: zero added/removed files, zero removed DVs, and version stayed 1.
Its recorded clustering coverage was about 99.0% and quality about 93.8%.
Repeating uncached reads returned 226ms engine / 530ms caller p95, with no
physical-change claim. This does not prove that FULL forces reclustering or
repairs singleton performance on this runtime/workload.

Retain the native identity-hash/full-key predicate and deletion-vector capability
as the physical candidate: the measured update avoids 100x output-row amplification.
Do not disable native features solely to accommodate an unqualified graph reader;
use the separately qualified immutable release design. Track DV growth,
unclustered files, actual files/bytes read and latency when selecting maintenance.
Do not prescribe full OPTIMIZE after each batch or extrapolate a no-op into a
maintenance-cost policy. No current measurement justifies changing UC architecture.

Next scale comparisons should raise independent payload entropy and metadata/file
count, then include canonical + raw source + property history + structural changes
and actual publication. The current write/current-state slice is now real scale
evidence, but cannot replace those requirements or 1B-node/5B-edge admission.

## Evidence and reproduction

- [Audited comparison](out/native/ashlar_scale_20261006_r85/comparison.json)
- [Local run](out/local-scale-20261006/summary.json)
- [Property-map correction](out/property-map-correction-20261006/summary.json)
- [Native statements and exact results](out/native/ashlar_scale_20261006_r85/statements.jsonl)
- [Final version-2 driver metrics](out/native/ashlar_scale_20261006_r85/uncached-v2-driver-0/query-history.json)

`scale_workload.py` declares IDs, counts, payloads and updates. Run
`scale_local.py` in the documented Spark environment, followed by
`scale_native.py` in the authenticated SDK environment. Native fresh-schema
checks deliberately refuse replay into the same experiment: choose a new isolated
run/schema for repetition; never blindly retry a submitted write. Prior versions
and statement handles remain the authoritative recovery evidence.

`scale_correct_properties.py --local-only` and `--native-only` record the
one-time correction using each environment. The native precondition requires
version 1. `scale_native_driver_reads.py --version 2` reproduces the corrected
read workload. `scale_analyze.py` audits saved final metrics offline;
`--refresh` reloads history for the same IDs. Run-specific paths and versions
are evidence bindings, not a production publisher. No cleanup/VACUUM was performed.
