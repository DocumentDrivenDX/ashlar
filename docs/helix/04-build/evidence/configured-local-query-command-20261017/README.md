# Installed configured local query command

The exact `ca836c6494730497b3c18d4d3c0b07d5fd026b8f` wheel passed two normal
`python -I -B -m ashlar run-configured-local --configuration ...` invocations
with `operation: "query"`. No test hook or substituted runtime was used.
Qualification is the original UMF 0.7 configured STRING model, revision 3,
Python 3.11.17, Spark 4.0.1, Delta 4.0.0 and the indexed Weft 0.4.1 profile on
macOS 27.0.1 arm64. Exact optional validator versions and wheel digest are retained.

| Case | Selected ordinal | Native carrier rows | Guards | Whole command seconds |
| --- | ---: | --- | ---: | ---: |
| Current global count | 3 | `[["1"]]` | 2 | 37.826 |
| Historical repeated-field projection with distinct aliases | 1 | `[["first","first"],["second","second"]]` | 1 | 29.411 |

The historical query returned the original values despite subsequent update and
delete publications. Both reports retained the selected original manifest and
complete UUID/version vector, while latest completed progress remained ordinal 3,
checkpoint `1828`. Each query compiled twice with identical original response
bytes, passed public schema/artifact admission, executed unchanged SQL and all
zero-result guards, and matched the independent complete source-prefix bag.
Repeated fields retained their distinct output positions and multiplicity.

Public stdout, private `report.json` and `runtime-report.json` agreed as JSON.
The runtime report followed source, publication, temporary-alias, native and Spark
closing; the public report additionally followed worker supervision and parent
closing. Original compiler protocol text and exact decoded STRING/count carriers
are retained. Registry bytes, all six tables' Delta JSON logs and complete
journal-row digests were unchanged, with 26 operations, 3 plans and 3 local progress
entries. These facts do not inventory every Parquet byte or establish protected
producer ACK, remote fencing or Unity Catalog qualification.

The commands ran sequentially with a 180-second command bound and 240-second outer
capture bound. Durations include startup, compilation, validation and closing;
they are not native query latency measurements. No ingestion, Databricks compute
or other cloud resource was started. Spark cleanup was independently observed.
`manifest.json` pins exact archived bytes and deterministic gzip files, including
the actual runner and configurations. The retained runner's `3and` spelling emits
a harmless Python syntax warning; both commands and the runner terminated
successfully. The [operator guide](../../../../../examples/configured-local/README.md)
describes the supported command profile.
