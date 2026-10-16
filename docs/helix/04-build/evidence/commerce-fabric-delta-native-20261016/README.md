# Actual commerce publication to fresh Fabric-compatible Delta carriers

The installed public `materialize_commerce_fabric_delta` bridge completed against
the existing original commerce publication and its actual ordinary PostgreSQL
source/ACK owner. It created two new local Delta projections:11 nodes with18
STRING columns and10 independent edges with23 STRING columns. Both complete
version-zero row bags, exact ordered schemas, endpoint/identity carriers and
closing native UUIDs passed. Original scalar JSON text and nulls remain exact;
no nested scalar promotion or Fabric numeric semantics are claimed.

The supervised process exited0 after65.989seconds. The workflow including reader
and Spark closing took65.199seconds. This is one small functional observation,
not an ingest throughput or latency benchmark. Resources were local[1],512MiB
driver, Python3.12.14/SQLite3.53.1, Spark4.0.1/Delta4.0.0 and JDK21. The existing
one-CPU/512MiB PostgreSQL17.9 container was reused; no source fixture, batch,
publication, cloud resource, grant or ACK was created.

The actual publication manifest and complete four-role snapshot vector are in
report.json. The distinct new targets are nodes UUID
24dab8a3-e377-4a3b-8187-0facdb608bcd and edges UUID
c45941c5-0b10-4fc9-b032-fc7526c5d952, both version0. Their Delta logs and Parquet
files are retained. These observed projections confer no immutable loaded-graph
lease or continuing source authority.

Opening/closing protected ACK records were identical at position1. Their full
request and manifest bytes match the original held publication; the decoded
manifest equals the materialization report. All78 original native files had
identical opening/closing hashes. The operator used ordinary reader/ACK metadata
connections only, and exposed the successful report after both the reader and
Spark closed. The original publication/source setup is retained in
[the preceding integration](../publication-current-local-20261016/README.md).

The wheel built from main d4f09387 was installed into a fresh copied Python
virtual environment. installed.json records its SHA256 and all408 exact installed
Ashlar files. Existing selected SDK links were reused; runtime-selection.json
qualifies actual import/version and selected-file correspondence, not a fresh
whole-SDK or OS closure. Fixed Delta JAR hashes were checked before Spark opened.
The reviewed operator has a240second process-group deadline and fresh destinations.

manifest.json covers exact raw/compressed hashes for the operator, configuration,
installed-file inventory, full terminal output/status, report, ACK comparison and
native projections. Independent review verified the invocation and final evidence.
Logs preserve native warnings, including the benign /dev/null password-file warning.
This qualifies the explicit private loopback-trust commerce source and local
carrier materialization only. Fabric upload/load/GQL, R1/R2 refresh exclusion,
managed Unity Catalog and production authentication remain unqualified here.
