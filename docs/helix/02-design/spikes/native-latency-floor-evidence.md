# Persistent-client latency controls

Existing shared dbw-aidev-cus serverless Photon 2X-Small warehouse, SQL channel
2026.38. Official Databricks SDK 0.102.0 uses a reusable requests 2.32.5 transport
and the active aidev-cus profile; credentials are neither printed nor stored.
Twenty-five measured samples per shape, round-robin order, one excluded warmup,
UUID result-cache bypass. All 100 measurements have query-history metrics and
zero result-cache hits. Running warm compute, local macOS caller; no cold claim.

| Control / table | Execution min / median / p95 ms | Server total p95 ms | Caller median / p95 ms |
| --- | --- | --- | --- |
| SELECT 1, uuid(), no table | 151 / 168 / 311 | 393 | 508 / 826 |
| One-row Delta table | 165 / 186 / 453 | 587 | 606 / 874 |
| 1M typed repetitive objects | 171 / 194 / 322 | 500 | 640 / 786 |
| 1M typed varied objects, pinned version | 216 / 246 / 391 | 588 | 688 / 908 |

The 100ms engine and 250ms caller gates fail even for minimal controls. Layout
optimization can remove scan work; these observations do not support a claim
that file pruning alone will meet either latency gate on this warehouse. This is
an empirical limit of this test environment, not a universal Databricks lower
bound or a reason to silently change the requirements. Shared compute variation,
caller placement, transport and runtime configuration need controlled tests.
The SDK removes per-call CLI process creation but is still a REST Statement API
client, not evidence of JDBC/Thrift driver latency. Twenty-five samples support
screening, not stable tail SLO admission.

The larger varied table reads three files and prunes one at the median. This
motivates the separate multi-file pruning experiment. Raw statement responses,
timestamps, query metrics and regenerated summary are under
`SPIKE-001-table-layout/out/native/ashlar_floor_20261005_b1/`. Query metrics arrive
asynchronously; the completed statements were not rerun when metrics were first
incomplete. The same history IDs were refreshed and then summarized.

No compute settings changed. Synthetic one-row control remains in its isolated
schema. SDK installation is confined to `/tmp/ashlar-db-client`; no repository
runtime dependency or production library choice was made. Billion-node scale,
cold reads, continuous ingest, publication correctness and graph mappings remain
separate open gates.

SDK authentication uses the documented configuration-profile interface:
[official SDK authentication](https://databricks-sdk-py.readthedocs.io/en/stable/authentication.html).

Subsequent [SQL-driver evidence](native-driver-evidence.md) passes the 100ms engine screening target using a cache-disabled persistent session. Its transport/query-shape matrix shows that the earlier REST controls are a path-specific observation, not a universal Databricks execution floor. Caller, cold, concurrent and scale admission remain open.
