# Dedicated Databricks scale preparation

The owner authorized a maximum of **USD25** on 2026-10-15. Preparation is
committed in `29756a4f`; it is not a completed native benchmark.

The prepared initial workload has 100,000 nodes and 1,000,000 edges, followed
by an explicitly selected 1,000,000-node/10,000,000-edge stage only if measured
runtime and remaining budget permit. Data generation stays on Databricks.
The canonical UMF-generated managed Delta carriers and liquid clustering remain
unchanged. Predictive optimization is observed, never disabled. Singleton,
one-hop, two-hop edge-occurrence bags, distinct terminals and isolates have
independent bounded oracles. This synthetic bounded-degree topology does not
qualify hub/skew behavior, publication throughput or production authority.

An authorized dedicated single-node Personal Compute creation was attempted
using policy `001654FA4AA08D1A`, runtime `17.3.x-scala2.13`,
`Standard_D4ds_v5`, zero workers and explicit ten-minute auto-termination.
It terminated unsuccessfully with the native CLI error:

> Current organization 7405607548213398 does not have any associated worker environments

A subsequent `clusters list --profile aidev-cus --output json` succeeded and
returned an empty list. No cluster ID was issued. No ingest or query ran, and
no dedicated cluster remained in that inventory. The existing shared
`data-gateway` warehouse was not used, started or stopped. These observations
are not a billing receipt or a claim about unrelated workloads' charges.

SQL endpoint creation had previously been refused for this caller. A serverless
job is a distinct possible route, but no job has been submitted and its cost
bound has not been established. A timeout alone must not be represented as a
USD25 billing cap. Resume native measurement when a dedicated compute route
with an adequate spending bound is available; do not silently use shared compute.

Record startup, ingest, maintenance, complete execution/fetch and independent
validation separately, plus fully guarded publication-request latency when that
path is exercised. Missing native timing, scan/cache statistics and attributable
usage remain unknown. The earlier three-row local workflow's approximately
23-second whole-process duration includes startup, staging, authority checks,
two queries and shutdown; it does not measure three-row ingest throughput.
