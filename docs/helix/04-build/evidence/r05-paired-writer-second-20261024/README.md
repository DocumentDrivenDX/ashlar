# R05 bounded paired writer comparison — 2026-10-24

This receipt records one owner-directed local comparison of the opt-in
node/edge concurrent Delta writer against the sequential writer. The worker
used the same deterministic `.4-xxhash64` generated range projections and
fresh Delta table pairs. The mode order was sequential first, concurrent
second, in one Spark JVM; JIT/codegen or cache warming may therefore favor the
second mode. This is one discriminator, not a reverse-order comparison or a
full-scale throughput qualification.

## Admission and runtime

The owner authorization was `ashlar-r05-paired-writer-authorization/0.1`
(SHA-256 `13bdf8931edb15763bc46c618e943b6ea4f3f1ffda753bb53a9c1ce84955780a`).
It authorized one 45M-node/225M-edge local retry, after the first attempt's
Spark 4.2 job-group API failure; it retained the 15-minute review threshold and
explicitly held the 1B/5B run. Limits were 780 worker seconds, 900 total
seconds including cleanup, 16 GiB active Delta bytes, 32 GiB attempt-root
bytes, and 64 GiB minimum free space. Predictive optimization remained enabled.

The pinned packet SHA-256 was
`408ef7cf3ef4d0aebc7a3d47ef15d695525ab17f5735b76332c116c91ecf8821`;
source-pin SHA-256 was
`692fd31a299962e00aac3288f8bc3984d68a76bc038ab0c72185fc5ea1926364`;
runtime-pin SHA-256 was
`504ad0a1958e343ae0b4b5c7787b40f1e86063519b86006aa6435ec3b4117316`.
The source logical-plan hashes were `cf1f483685fb0fae9376044cee868ef7480962c15d9e82f99ad69bff9d9d3648`
(objects) and `666bfee1dcc4b48281a3350f9936e31b7fff90370f2e3469a95b8de8e2f6475b`
(edges), both pinned UMF `.4-xxhash64` range projections. Runtime was Spark
4.2.0, Delta 4.4.1, GraphFrames 0.12.3, Python 3.12.14, and JDK 21.0.13
(build 21.0.13.0.2), on `local[18]` with 128 GiB physical memory and a 24 GiB
JVM maximum heap. The selected Java binary SHA-256 was
`382e51393d712cf7e228a606142d3c079f5fe12e4b599c62fdd1470e40fe7916`; the
selected Python package-closure SHA-256 was
`78759f226b20645077cbe7c4dd1eb61fa27c5cf17e87dc7e9e76c0c01c897a1c`.
The runtime descriptor binds these JAR SHA-256 values:

- Delta Spark 4.4.1: `2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be`
- Delta storage 4.4.1: `f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f`
- GraphFrames Spark 4.2 / 0.12.3: `a78958f7ea3ac89ed1ea9dbbd1546f403d94151e969bb0de96e33697a5f49f50`
- GraphFrames GraphX Spark 4 / 0.12.3: `385ea9934bdeedff9cd4591df66ada45ce739148f15fe947a032886997626675`
- DataSketches Java 6.2.0: `1b55103e1f7564150a0867eca4ce3bca13cd5935a32c199a5e738f8c5c24901a`
- DataSketches Memory 3.0.2: `a3dbdec4de16bf2b0a4c9b1b253bd4064d587675fc76063f8972cdfa104c66cb`

## Result

| Mode | Write wall | Mode total (writes plus compact validation) | Persisted rows | `DESCRIBE DETAIL` table bytes |
| --- | ---: | ---: | ---: | ---: |
| Sequential | 35.227 s | 66.278 s | 45M objects + 225M edges | 3,041,334,560 B |
| Concurrent node/edge | 30.620 s | 60.924 s | 45M objects + 225M edges | 3,041,334,560 B |

The concurrent sample reduced write wall by 4.606 s (13.1%) and mode total by
5.354 s (8.1%) relative to the sequential sample. Both table pairs had the
same schemas and Delta protocol digest; object and edge row counts matched
45,000,000 and 225,000,000. The byte total is the sum of table `sizeInBytes`
(471,896,757 object + 2,569,437,803 edge). These are measured local table
sizes for this fixture, not a total storage requirement; logs, staging,
retained versions, temporary space and engine export are outside that figure.

Both modes passed the compact value witness: zero bad rows; object IDs ranged
0–44,999,999 with exact decimal sum 1,012,499,977,500,000; edge IDs ranged
0–224,999,999 with exact decimal sum 25,312,499,887,500,000. The 225M-edge
endpoint witness reported zero invalid source/target types, zero out-of-range
source/target IDs, and zero mismatches against the generator formulas. The
witness uses count, range and exact-decimal sum; it does not prove exact
multiset uniqueness because exact distinct counting was intentionally omitted.
No point lookups, full scans, GraphFrames export/action, publication, ACK,
Unity Catalog operation or source lifecycle was part of this comparison.

The supervisor reported child exit 0, child process-group quiescence before
cleanup, successful cleanup quiescence, and `bounded-paired-comparison-passed`
in 191.742 seconds total including cleanup (186.399 seconds before cleanup),
within the 900-second cap. It removed all four Delta tables and six
supervisor-owned directories after group quiescence, preserving ownership
receipts. Post-exit inspection found only the retained authorization,
admission, ownership, result, runtime-pin and diagnostic receipt files in the
attempt root; the four table directories and six owned scratch directories were
absent. The attempt root itself and receipts remain for audit.

## Scale implication

With identical row scaling from 45M/225M to 1B/5B (factor 22.2222), the
sequential write-only extrapolation is 13.047 minutes measured, or 16.309
minutes at 80% of this sample's throughput. The concurrent write-only
extrapolation is 11.341 minutes measured, or 14.176 minutes at 80%. The
concurrent value is only 49 seconds below the 15-minute threshold and may be
biased by its second position in the shared JVM. Neither extrapolation includes
full-size scale effects or the requested two point lookups, two scans and
GraphFrames export. Linear table-byte arithmetic gives 67,585,212,444 bytes
(about 67.6 decimal GB) for this fixture's 1B/5B target; it excludes the same
ancillary storage categories above and is not a capacity guarantee.

The concurrent paired sample makes a sub-15-minute **write-only** planning
case plausible, but does not establish a sub-15-minute end-to-end result. Keep
the full 1B/5B run held pending independent review of this discriminator and a
revised full-workload plan that includes reads, export, validation, cleanup,
storage headroom and a defensible end-to-end time bound. Do not substitute the
14.176-minute write-only projection for that whole-workload bound.

See the [R05 materialization physical limits](../r05-materialization-physical-limits-20261024.md),
[paired writer packet](../r05-full-delta4-scale-packet-20261023/README.md), and
[prior storage reassessment](../r05-storage-throughput-reassessment-20261023/README.md)
for design, earlier calibration and exclusions. This receipt does not change
the carrier contract, promote concurrent writing to default, or qualify
production capacity.

## Retained attempt receipts

Attempt root:
`/private/tmp/ashlar-r05-paired-writer-20261024-attempt-parent/ashlar-r05-paired-ec64cb81b11a625643ee1c90`

- Supervisor result SHA-256: `ddaa748dbe4efd17168a5431d87812b451dc399ccf1f7b7c5064b683f5e939df`
- Worker result SHA-256: `edecda003a356c00fea2db99819715da05258e958afa52df93f9b0a4fc87da44`
- Cleanup result SHA-256: `f9c9610d427964a9e5a2605fddad4937b4051e3e99f4c1ad58645eac9e6af4bf`
- Runtime descriptor SHA-256: `9af32638dbe5fcd67dbb2f3c3fc732e9e2e25319e5ba0904a3dff37885f51fb9`
