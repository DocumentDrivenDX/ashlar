# Installed configured-publication Weft queries

The exact `81db0daa44431835b42f5ccbb4f63c0f06bb3b4a` wheel passed two actual
queries through the indexed Weft 0.4.1 profile on Python 3.11.17, Spark 4.0.1
and Delta 4.0.0 on macOS 27.0.1 arm64.
The original UMF 0.7 configured model, revision 3, and ordinal-3 publication
remained unchanged. `SELECT COUNT(*) AS n FROM Item i` returned `1`;
`SELECT i.label FROM Item i` returned `updated`. Independent literal source-event
folds supplied both expected bags. Each request compiled twice with identical
response bytes, passed the installed public schemas and artifact admission, and
executed unchanged SQL and every emitted guard (two for count, one for projection).

Fresh external aliases preserved the complete original Delta UUID/version vector;
the manifest retained its original names. The actual configured runtime supplied
source admission, installation lease, native protocol/retention and progress/phase
checks. A disclosed singleton test hook composed the new SDK adapters inside that
owner, then called the original singleton. This proves the installed SDK path,
**not a query CLI**. Per-query evidence remained provisional until alias, read,
source and Spark closing completed. No protected producer ACK or remote fencing
is claimed. Delta JSON logs, registry bytes and complete journal-row digests were
identical before/after; ledger counts remained 26 operations, 3 plans and 3 local
progress entries. These checks do not inventory every Parquet byte.

Count and projection took 6.486s and 4.163s including compilation, guards and
custody checks. The whole runtime took 38.116s; supervised termination took
40.567s. These are functional observations, not query latency benchmarks. No
ingestion or Databricks compute was started. Two earlier attempts refused. The
first wrapper discarded worker diagnostics, so its exact cause is unretained.
The second retained a refusal before compilation because the reused runtime
lacked optional `weft-validation` dependencies. Installing the six already-cached,
pinned wheels offline resolved it. All attempts shut down Spark; no failed attempt
is counted as a pass.

`manifest.json` pins each exact archived file before and after deterministic gzip
compression. The final harness/launcher describe the successful attempt; they do
not reconstruct the unretained first worker traceback. `summary.json` retains
results and qualifications. Raw compiler protocols, guards, source witnesses,
closing receipts, dependency versions/hashes and the test hook are archived.
