# Actual commerce publication through OneLake and native Fabric GQL

The existing commerce publication's complete Delta projection was copied to
OneLake, loaded into a new Fabric Graph model, and queried through the native
beta GQL API. All seven complete result-bag comparisons passed:11 nodes with18
canonical STRING fields,10 edges with23 fields,10 endpoint triples,11 distinct-edge
two-hop paths,11 explicit WALK paths, and node/edge counts11/10. Original scalar
properties remain opaque exact JSON text; this does not qualify numeric promotion.
The seven queries took36.602seconds in total, one functional observation including
an initially cold request, not a latency benchmark.

The source projection is the actual held publication materialization in
[the preceding native proof](../commerce-fabric-delta-native-20261016/README.md),
release SHA256
878291819c574f7a7d32d09c92d4a4687b65d3ab8ebaeb639263cb58d31ad29a.
The uploader checked all four exact source files against the retained native
evidence, used conditional new paths, wrote Parquet before Delta commits, and
compared complete uploaded bytes by GET. Both table version-zero UUIDs are
preserved:24dab8a3-e377-4a3b-8187-0facdb608bcd for nodes and
c45941c5-0b10-4fc9-b032-fc7526c5d952 for edges. Upload completed in14.237seconds.
The GQL oracle decoded these same hash-checked Parquet files with DuckDB1.5.6;
the preceding Spark/Delta proof independently checked them against the held
publication. Exact expected rows and requests are retained.

The existing active FTL4 free trial in West US3 was used. No paid capacity,
Databricks compute, new grant, source mutation or ACK submission was involved.
The new graph is
[Ashlar_Commerce_Publication1_20261016a](https://app.fabric.microsoft.com/groups/0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793/graph/c974e2ff-8bdb-4653-a007-1d213882d5c5?experience=fabric-developer).
The model and two release-specific OneLake table paths remain for inspection.

The single creation POST returned202; its original operation
92929d60-d8a4-43e0-ae96-50856b12c428 subsequently succeeded. The creation-produced
Refresh job0d9b2c00-edec-44a4-ace1-2678295be7f6 was observed first as NotStarted
and later Completed, with no failure, at21:12:03.6306451UTC. No separate refresh
was submitted. The exact terminal observation preceded all GQL requests and was
independently verified. Native responses were retained before strict schema,
scalar, warning, pagination and full-bag checks. All responses were HTTP200 with
GQL status00000. The finite operator performs no automatic retry.

This closes actual copied-publication load/query correspondence, not live source
custody or refresh exclusion. The materializer's source/ACK hold ended before
upload. No loaded-generation owner or native snapshot token protects these GQL
requests; full opening/closing bag equality cannot establish continuous exclusion
or exclude an intervening refresh. The public held Fabric executor was therefore
not invoked with a substitute policy. Actual-publication R1/R2 evolution and an
interrupted refresh under genuine authority remain separate required work.

manifest.json pins44 compressed records and their uncompressed lengths/hashes:
source upload inventory/readback receipts, exact definition, creation/load
observations, query oracle/requests/responses/result and executed operators.
Tokens were acquired in memory through the existing Azure CLI session and are
not archived. Validator source copies pin the actual comparison logic from main
dc957eb0. Independent review covers file correspondence and native result bags.
