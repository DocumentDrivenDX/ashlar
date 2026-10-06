# Native incremental ingest screening

Schema: `client_dev.ashlar_ingest_20261005_a2`. Existing serverless Photon 2X-Small warehouse `data-gateway` on dbw-aidev-cus; no compute settings changed. Synthetic data only.

| Changed entities | Publication wall seconds | Effective entities/s | <=60s single batch |
| --- | --- | --- | --- |
| 10,000 | 20.274 | 493.2 | True |
| 100,000 | 25.605 | 3905.5 | True |
| 600,000 | 40.578 | 14786.5 | True |

Publication includes canonical MERGE, property journal append, serving MERGE, table-version discovery, full changed-set parity validation and manifest insert. Stage construction is excluded; CLI/auth/network overhead is included. Property string journal values are validated by decoding each old/new JSON token and comparing against the corresponding staged value. Three different-sized batches are not a p95 distribution. Effective batch rate is not sustained ingest capacity. No edges, deletion/replay handling, source feed adapter, concurrent readers, or failure recovery were exercised.

Each changed object carries roughly 2KB of deterministic SHA2 hex text. It is more varied than the original repeated-x fixture, but is not a measured production payload distribution. The starting serving table contains 1M objects; this does not prove 1B-node/5B-edge admission.

| Batch | Post-publication reads | Engine p95 ms | Server total p95 ms | CLI wall p95 ms | Result cache hits |
| --- | --- | --- | --- | --- | --- |
| 1 | 10 | 333 | 721 | 1150.7 | 0 |
| 2 | 10 | 316 | 551 | 1051.4 | 0 |
| 3 | 10 | 284 | 504 | 963.5 | 0 |

Read results carry uuid() to defeat result reuse; inspect raw history for cache and I/O evidence. Ten samples per batch are descriptive and insufficient for p99 or stable population p95. Version-pinned reads ran after publication; they do not establish old-publication availability during writes.

Raw reproducible evidence: `SPIKE-001-table-layout/out/native/ashlar_ingest_20261005_a2/`. Harness: `native_ingest.py`; regenerate this report with `summarize_ingest.py ashlar_ingest_20261005_a2`. Freshness, concurrency, cost attribution, graph scale and low-latency gates remain open.

Stage-3 preparation encountered a client timeout under eight-reader load; its successful server statement was recovered without resubmission. The 40.578-second batch-3 publication window starts after recovery, excluding staging and that interruption. It therefore cannot establish end-to-end <=60s freshness. See [concurrent reader evidence](native-concurrency-evidence.md). The first run (a1) has a known invalid journal string encoding and is excluded from correctness evidence.
