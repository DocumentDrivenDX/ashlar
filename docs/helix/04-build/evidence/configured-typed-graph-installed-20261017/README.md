# Installed typed graph publication lifecycle

Six sequential installed CLI invocations passed on local Spark/Delta using the
original [typed graph source](../../../../../examples/end-to-end/typed-graph-source.jsonl)
and authored UMF core 0.8 model. The offline wheel was built from Ashlar
`a16d96dc192fdc6dc07fb72457e42ad7a0d47acd`; wheel SHA256 is
`badd0ced2947598d2732c184a675e350f9d6ed7daa4ff0d3b2bdbd72b6ad8875`.
The public UMF producer was pinned to
`07357eadbda9cf8299b9d72d004cbb77f8091aee`. Each invocation freshly admitted
original carried Records and complete supplied-dataset keys/relationships before
native work. The original Record context warnings remain preserved; complete
joint dataset checks supply the separately qualified relationship context.

| Operation | Seconds | Objects | Edges | History | Tombstones |
| --- | ---: | ---: | ---: | ---: | ---: |
| Initialize | 41.031 | — | — | — | — |
| Publish ordinal 1 | 49.096 | 4 | 4 | 8 | 0 |
| Publish ordinal 2 | 57.779 | 4 | 3 | 10 | 1 |
| Publish ordinal 3 | 66.283 | 3 | 1 | 13 | 4 |
| Replay ordinal 3 | 29.272 | 3 | 1 | 13 | 4 |
| Read Person 2 at ordinal 1 | 36.031 | — | — | — | — |

Publication checks compare complete native row bags with independently retained
source carriers, including history and tombstones. The fixture exercises distinct
parallel edges, a self-loop, an isolate, cross-type endpoints, endpoint replacement,
edge deletes and object deletion. The historical singleton returned Person 2's
original property and retained JSON bytes after its later deletion; the operator
compared these with the original source event. Selected ordinal 1 retained its
complete four-table vector, independently of the latest ordinal 3 vector.

Replay reported `replayed: true`. Separately archived before/after files for both
replay and historical read match exactly across installation registry SHA256,
all Delta JSON log hashes and complete SQLite operation/plan/progress row
fingerprints. The final local checkpoint was byte offset `5724`. All six CLI
stdout results matched their public report files and declared `protected_ack:
false`. Required closing checks completed; a final process inspection found no
remaining operator or Spark worker.

This is a 13-event functionality check, not ingest throughput or a latency
benchmark. Timings include fresh local process/Spark setup and verification.
Python 3.11.17, Spark 4.0.1 and Delta 4.0.0 ran sequentially with a 512 MiB driver.
It qualifies local managed-path Delta, immutable-file progress, typed graph source
admission and historical object reads. It supplies no protected source ACK,
Unity Catalog/cloud authority, production fencing or graph Weft query evidence.
No Databricks compute was used.

`manifest.json` records compressed and original hashes and byte lengths for 68
archived files, including original invocation/result files, nested diagnostics,
installation registry, runtime metadata and exact preparation/run scripts.
Gzip timestamps are zero. Parquet data and bulk native storage are not committed;
the local installation is retained for subsequent bounded graph query work.
