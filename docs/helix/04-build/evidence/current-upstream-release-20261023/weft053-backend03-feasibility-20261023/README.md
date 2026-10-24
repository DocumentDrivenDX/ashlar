# Weft 0.4.53 Backend03 public-target feasibility

Status: **the historical crossed-target refusal is confirmed, and a separate target-matched public 0.4.53 check passes**. The first attempt changed only the Backend03 profile to the tagged Backend02 fixture's Spark 4.0.1/Delta 4.0.0 target and was correctly refused. That does not establish a gap for the Backend03 target declared by Weft source.

The target-matched R02 receipt replays the exact historical Backend03 request and exact Backend02 first-page/continuation requests from the parity artifact through the public 0.4.53 wheel. All three compile using their source-declared Spark 4.2.0/Delta 4.4.1 candidates. See the compact [target-matched receipt](../weft053-r02-spark42-public-target-20261024/README.md) for request/response hashes and exact target tuples. The Backend02 Spark 4.0.1 fixture remains unrelated and does not declare a Backend03 capability.

The compact [machine-readable receipt](backend03-feasibility.json) is retained as the historical crossed-target probe, including its expected refusal. The isolated runtime's wheel provenance is documented in the [0.4.53 adoption receipt](../weft053-ashlar-adoption-20261023/README.md). No SQL was rewritten; Spark, Delta, native, cloud, and publication operations were not run.
