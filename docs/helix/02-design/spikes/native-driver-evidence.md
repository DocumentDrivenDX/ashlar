# SQL-driver singleton evidence

Existing shared dbw-aidev-cus data-gateway serverless Photon 2X-Small warehouse,
SQL channel 2026.38. Official Python SQL connector 4.3.0, SDK 0.102.0, local macOS
Python 3.9 benchmark environment. A persistent SQL session explicitly sets
USE_CACHED_RESULT=false and reads back that setting. Query-history flags verify
zero result-cache hits. No warehouse configuration or source data changed.

## Initial driver controls

Twenty-five measured samples per shape, one excluded warmup, round-robin order.
Plain singleton queries return full stored carriers without a UUID expression.

| Shape | Engine median / p95 ms | Server total p95 ms | Caller median / p95 ms |
| --- | --- | --- | --- |
| SELECT 1 | 16 / 21 | 56 | 141 / 180 |
| One-row Delta table | 69 / 76 | 178 | 259 / 276 |
| 1M objects, 16MiB liquid-id files | 80 / 88 | 194 | 276 / 306 |
| 1M objects, 128MiB liquid-id files | 84 / 96 | 203 | 281 / 336 |

The 100ms engine target passes this bounded warm screening. The 250ms caller
target still fails. These observations do not admit production tail latency,
cold I/O, concurrent reads or billion-node support. The initial comparison also
changed query shape versus prior REST UUID tests, so transport was isolated next.

## Transport versus query shape

Twenty-five measured samples for each cell on the same 16MiB liquid table,
round-robin within each identity. All returned identity, property and retained
carriers match exactly. Each REST plain query uses a distinct identity; driver
sessions disable result caching. Native history confirms zero cache hits in
all four cells. At the median every query reads one file.

| Transport / shape | Engine median / p95 ms | Compile median / p95 ms | Server total p95 ms | Caller median / p95 ms |
| --- | --- | --- | --- | --- |
| REST plain | 194 / 378 | 136 / 199 | 612 | 639 / 889 |
| REST UUID | 195 / 262 | 95 / 106 | 397 | 593 / 772 |
| SQL driver plain | 79 / 86 | 89 / 97 | 190 | 282 / 303 |
| SQL driver UUID | 80 / 83 | 91 / 111 | 208 | 288 / 342 |

The driver advantage remains with either query shape; UUID alone does not explain
it. The earlier REST controls establish that path's observed overhead, not a
universal Databricks or Delta execution floor. Shared compute variation and fixed
within-round ordering remain limitations; these runs do not isolate every
underlying server scheduling or session difference.

## Native parameters

A further fifty measured samples per shape compare literal IDs with native
bound integer parameters in one cache-disabled driver session. Returned full
carriers match for every pair; zero result-cache hits and one read file at the
median.

| Shape | Engine median / p95 ms | Compile median / p95 ms | Server total p95 ms | Caller median / p95 ms |
| --- | --- | --- | --- | --- |
| Literal ID | 72 / 80 | 84 / 89 | 180 | 262 / 279 |
| Bound ID | 71 / 76 | 84 / 103 | 186 | 262 / 286 |

Binding is the preferred safe query surface, but it did not remove enough
compilation/caller time to pass 250ms in this environment. Do not infer an
in-region caller result from these local timings or silently relax the budget.
Cold tests must be repeated through the selected driver rather than reuse REST
cold latency as a driver claim. Engine admission also requires the planned
concurrency, payload/type distribution and scale tests.

Raw evidence under `SPIKE-001-table-layout/out/native/`:
`driver-20261005-f1`, `client-matrix-20261005-f2`, and
`parameter-driver-20261005-f3`. All 312 statement IDs have complete execution
metrics after history refresh, without repeating read statements. Harnesses:
`native_driver.py`, `native_client_matrix.py`, `native_parameters.py`;
regenerate summaries with `summarize_client_reads.py <run>`.

Session cache configuration and connector behavior follow official documentation:
[SQL connector](https://docs.databricks.com/aws/en/dev-tools/python-sql-connector),
[USE_CACHED_RESULT](https://docs.databricks.com/gcp/en/sql/language-manual/parameters/use_cached_result).
This is a spike runtime choice, not a selected Ashlar library language or an
external graph connector compatibility claim.
