# Remote-I/O-qualified singleton reads

Executed on running shared dbw-aidev-cus serverless Photon 2X-Small compute using
the persistent SDK client. Two new isolated liquid-id tables each contain the
same 1M-object synthetic seed, with 16MiB and 128MiB target files. Tables were
created and optimized, then queried before full data validation. Afterward, full
outer-join carrier equality passed and the identical IDs were read warm.
Native query-history metrics qualify remote I/O; a new table name alone does not.
All 100 reads bypass result caching with UUID and report zero result-cache hits.

| Layout / phase | Reads | Reads with remote bytes | Remote-byte sum | Caller p95, all reads ms | Caller p95, remote-I/O subset ms | Engine p95, all reads ms |
| --- | --- | --- | --- | --- | --- | --- |
| 16MiB first touch | 25 | 13 | 258,317,467 | 1257 | 1450 | 848 |
| 128MiB first touch | 25 | 6 | 643,058,402 | 1107 | 1329 | 763 |
| 16MiB warm repeat | 25 | 0 | 0 | 627 | — | 210 |
| 128MiB warm repeat | 25 | 0 | 0 | 649 | — | 227 |

First-touch requests revisit files, so not all are cold. A remote-I/O-qualified
request can also include cached bytes; this is not proof that every byte was
cold. The qualified subsets contain only 13 and 6 samples: their p95 is a
screening statistic near the maximum, not a stable population tail estimate.
Nevertheless, observed requests exceed the provisional <=1s cold caller target.
The warm repeats also fail the 100ms engine/250ms caller budgets. No layout
winner or gate admission follows from these samples.

Smaller files reduced remote bytes but did not yield a passing cold latency
result here. This test makes the cache condition observable without clearing
shared caches or restarting compute. It does not establish startup latency,
dedicated-compute performance, sustained concurrent cold reads or billion-node
metadata behavior. The source has one type and mixed payload entropy; graph
edges are absent.

Reproducible harness: `SPIKE-001-table-layout/native_cold_reads.py` and
`summarize_cold.py`. Raw statements, full-equality assertions, table detail,
110 query-history entries and summary are under
`SPIKE-001-table-layout/out/native/ashlar_cold_20261005_d1/`. SQL channel 2026.38;
SDK 0.102.0, requests 2.32.5, local macOS Python 3.9 benchmark environment.
No warehouse setting or shared cache changed; isolated synthetic tables remain.
Cost attribution and all production compatibility claims remain open.
