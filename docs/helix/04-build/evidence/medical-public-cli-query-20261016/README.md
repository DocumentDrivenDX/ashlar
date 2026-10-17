# Actual installed public medical query CLI

The installed public invocation completed with exit zero in **287.060862584
seconds** using committed Ashlar `ef92d5e047959be342c5db23b2bc470dbec7d553`:

```text
python3 -I -B -m ashlar query-original-medical --configuration query-configuration.json
```

The exact absolute argv is retained in `outer-attempt.json`. Only the explicit
`filtered-limit` case ran. Its original SQL projects five encounter fields,
orders by resource key and applies `LIMIT 2`; the complete original encounter
population has **one row**. That complete row matches the original graph in all
five fields and identity. This proves this actual ORDER BY/LIMIT invocation;
it does not prove selective filtering, truncation of a larger population, the
full medical corpus, production security, or other query engines.

The actual public CLI owns the worker/native phase and fresh source/publication
custody. The outer harness merely observes bounded streams and selected native
file correspondence; its descendant sampling is a cleanup supplement, not a
global process-absence claim. The run uses the existing selected SDK environment
(Python 3.12.14, Spark 4.0.1, Delta 4.0.0, JDK 21.0.12), local[1]/512 MiB.
The reported duration includes full setup, custody and closing; **it is not query
latency**. This run does not repeat the six-case V SDK query proof.

The three exact request/response/evidence files retain unchanged recompilation
and five zero-violation native guard checks whose carriers exactly match the
original emitted obligations. Public source revalidation pins the original
model, graph and baseline. Canonical CLI stdout is byte-identical to the full
retained receipt; CLI stderr is empty. Complete original native before/after
inventories match, opening native observations match batch closing, and full
protected ACK request/manifest/receipt byte pins and position match closing.
No source, publication or ACK advancement was requested.

`manifest.json` pins archived and uncompressed bytes; gzip timestamps are zero.
`source-digests.json` pins the installed wheel, original inputs and exact streams.
The full receipt, actual configuration, installed-file inventory and frozen
preparation retain the original ownership and compiler selections. SDK payloads
and raw log streams are omitted. Archive verification used retained bytes only;
no source, compiler, native or cloud calls were rerun.
