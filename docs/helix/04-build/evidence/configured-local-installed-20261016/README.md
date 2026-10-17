# Installed configured-source local Delta workflow

The documented `ashlar run-configured-local` command initialized a fresh six-table
installation, published all three original transactions in
`examples/end-to-end/configured-source.json`, and replayed the last transaction.
The exact installed wheel was built from `dfe97dbb0af402d26bb02ae09171686b40d52297`.
No checkout imports, cloud compute or protected producer ACK were used.

| Operation | Current objects | History | Tombstones | Local byte checkpoint | Whole command seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Initialize | 0 | 0 | 0 | — | 28.165 |
| Publish 1 | 2 | 2 | 0 | 815 | 38.876 |
| Publish 2 | 2 | 3 | 0 | 1331 | 40.953 |
| Publish 3 | 1 | 4 | 1 | 1828 | 31.053 |
| Replay 3 | 1 | 4 | 1 | 1828 | 22.545 |

All graph edge counts are zero in this STRING object fixture. The installed
runtime compares complete native carrier bags with the independent original-source
oracle, checks actual UUID/location/schema/protocol and finite retention under an
exclusive cooperative local lease, and closes those owners before success.
Replay preserved the complete manifest, all six native Delta heads, three local
progress rows, three effect plans and 26 journaled native operations. The source
contains four records, including one delete; the retained original UMF receipt
contains three valid, complete non-delete logical-value checks. Original schema
warnings and the explicit upgrade subset remain in that receipt.

The retained JSON reports are the actual public command results. `summary.json`
records their hashes, exact versions, timings and the outer read-only SQLite
harness correction. It was corrected before replay started; successful publications
were not rerun. Each command used a fresh private output and one local worker with
a 512 MiB driver. Durations include startup and shutdown and are not query latency
samples or ingest throughput benchmarks.

The private installation and original journal remain at
`/private/tmp/ashlar-configured-local-installed-20261016-fuqjbp40/native` for
recovery and further reads. Use the operator guide under `examples/configured-local/`
to select your own explicit paths; the receipt paths are historical selections.
This run does not qualify native interrupted-run recovery, Unity Catalog,
production remote fencing or a PostgreSQL/Truss protected ACK.
