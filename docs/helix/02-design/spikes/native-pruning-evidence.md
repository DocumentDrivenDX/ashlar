# Multi-file native identity pruning

Executed 2026-10-05 in `client_dev.ashlar_pruning_20261005_c1`, existing shared
serverless Photon 2X-Small data-gateway warehouse on dbw-aidev-cus. Persistent
SDK client; SQL channel 2026.38. One million synthetic objects, source `pilot`,
type 1, preserved IDs, exact property and retained carriers. Of these, 600k have
roughly 2KB varied SHA2-hex payloads; the remainder retain repetitive payloads.
Full row count, identity uniqueness and carrier equality against the fixed
source version passed before read measurement. Twenty-five measurements per
layout in round-robin order, one excluded warmup, zero result-cache hits.

| Layout | Actual files / stored bytes | Median files read / pruned | Median bytes read | Engine p95 ms | Server total p95 ms | Caller p95 ms |
| --- | --- | --- | --- | --- | --- | --- |
| Liquid id, target 16MiB | 32 / 656,251,438 | 1 / 31 | 24,771,840 | 278 | 421 | 696 |
| Liquid id, target 128MiB | 4 / 658,522,344 | 1 / 3 | 204,234,846 | 276 | 495 | 844 |
| 64 routing buckets + ZORDER id | 64 / 656,829,477 | 1 / 63 | 10,520,506 | 303 | 445 | 710 |

Pruning now works: these are actual native file metrics, unlike the earlier
one-file fixtures. The 16MiB liquid variant reads about 8.2x fewer bytes than the
128MiB variant, but engine tail timing is effectively unchanged in this small
warm experiment. The bucket variant reads about 19.4x fewer bytes but has a
higher observed engine p95. I/O cache percentages are 100%, 100% and 99% at the
median, respectively. These observations do not establish cold-data performance
or a production layout winner. The 100ms engine and 250ms caller gates still fail.

`routing_bucket=pmod(id,64)` is an explicit diagnostic routing column, never a
replacement identity or graph endpoint. Queries still include source/type/id.
Every partition is only about 10MB here, so this over-partitioned diagnostic is
not a production recommendation. A production bucket design needs sizing,
negative-ID semantics, source/type distributions, migration/repartition and
write amplification evidence. Liquid clustering and partition/ZORDER remain
separate tables; they are not combined. File-size targets are best effort, as
the measured physical sizes demonstrate.

The result supports keeping id statistics/clustering in the candidate and
measuring cold I/O and maintenance cost before choosing smaller files. It does
not support adding a bucket column to CONTRACT-003 yet. One type and 32/64 files
do not prove cross-type/source skipping or billion-node metadata scalability.
At equal synthetic density, simply multiplying this fixture by 1000 would imply
thousands to tens of thousands of files; that is a planning estimate, not a
measured billion-node performance result. Five-billion-edge storage and adjacency
layout are absent from this experiment.

Raw statements, exact DDL/settings, full equality checks, OPTIMIZE responses,
DESCRIBE DETAIL results, query metrics and summary are retained under
`SPIKE-001-table-layout/out/native/ashlar_pruning_20261005_c1/`. All 95 statements
have execution metrics after refreshing asynchronous history, without rerunning
reads. No warehouse settings changed; three isolated diagnostic tables remain.
Stored active data totals roughly 1.97GB; obsolete versions and prior fixtures
are additional and have not been vacuumed. Cost attribution remains separate.

The interventions follow the documented separate layout mechanisms and file
size controls: [liquid clustering](https://learn.microsoft.com/en-us/azure/databricks/tables/clustering),
[file sizes](https://learn.microsoft.com/en-us/azure/databricks/tables/tune-file-size).

A bounded read of `system.billing.usage` for workspace 7405607548213398, warehouse 2439e1f2e37ac563 and UTC 2026-10-05 20:00–24:00 returned no aggregate rows at observation time. This is missing/possibly delayed usage evidence, not zero cost. The probe is retained under `out/native/cost-probe-20261005/`. Shared warehouse billing would still require attribution; no numerical dollar estimate or larger-scale cost approval is inferred. See [billing metadata reference](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing).
