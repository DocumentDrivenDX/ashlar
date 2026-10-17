# Installed typed Delta lifecycle

The exact `10ceb5692b418372585f219d8c5ae77ef4feb6b0` wheel ran seven successful public `python -I -B -m ashlar run-configured-local --configuration ...` commands without test hooks or checkout library imports. Python 3.11.17, Spark 4.0.1 and Delta 4.0.0 used one local worker and a 512 MiB driver. No cloud compute ran. Wheel SHA256: `1f954ece1ef3d98d6b22ace17a3f3d93451ee72a4fb15dc0ee8167df5277e830`.

| Command | Whole-command seconds |
| --- | ---: |
| initialize | 41.874 |
| publish-1 | 48.309 |
| publish-2 | 45.880 |
| publish-3 | 48.612 |
| replay-3 | 33.946 |
| read-original | 28.471 |
| read-deleted | 29.744 |

The original authored UMF core 0.8 model and four complete source events remain unchanged. Every invocation obtains fresh checks from the clean public UMF producer at `c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3`. All three publication prefixes passed the independent complete source/carrier-bag checks: current/history/tombstone counts were 2/2/0, 2/3/0 and 1/4/1; edge count stayed zero. This covers create, update and delete, original property text and unknown retained content, uint64 maximum, beyond-safe-integer values, decimal scale/signed zero, booleans, absent/null and Unicode. Delete checks validate complete carried Record values; core apply owns deletion semantics.

Exact replay reported `replayed: true`; the operator runner compared Delta JSON log and registry hashes plus complete journal-row digests immediately before and after and required equality. Historical singleton ordinal 1 returned object 2 with exact properties `{"21":9007199254740993,"22":-0.00,"23":false,"24":null}` after ordinal 3 deleted it. The ordinal 3 singleton returned null. The runner required the same before/after equality for both reads. Intermediate fingerprint objects were compared in memory; only the final fingerprint is archived, so these equality assertions are supported by the successful retained runner/results rather than separately archived before/after pairs. Final checkpoint is byte offset 2051 with three completed groups; batch ordinals are not byte cursors. Public stdout and closed report JSON agreed for every command. Process inspection after completion found no remaining Spark or lifecycle command process; the original installation remains retained for subsequent work.

This is a local cooperative private-file installation and immutable-file consumer progress proof, not managed Unity Catalog, remote writer fencing, protected producer ACK, dataset keys/relationships or typed Weft qualification. Original publication vectors are retained. Initialization, producer checks and Spark startup are included in timings; these are functional observations, not ingest or singleton latency benchmarks.

`manifest.json` inventories 73 exact gzip archives with original/compressed SHA256 and lengths, zero gzip timestamps. Configurations, public and runtime reports, bounded diagnostics, actual UMF receipts, registry, journal/log fingerprints and operator build/run scripts are retained. Operator scripts are execution receipts, not supported deployment APIs. Inputs remain in the committed example and its exact references. No original input, old STRING installation, predictive optimization or Databricks resource was changed.
