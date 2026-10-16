# Original-pack OneLake copy and Graph Model creation

Four small typed Delta tables were copied to fresh release-specific paths in the
existing trial Ashlar_Graph_Releases lakehouse. The one-shot operator completed in
27.406 seconds. Every selected Parquet file and version-zero Delta JSON commit
was read back byte-for-byte. Conditional creation refused overwrites; data files
preceded commits. Local Hadoop checksum files were excluded. No Fabric Spark,
Databricks endpoint, paid capacity, capacity configuration or grants were used.

The tables are the archaeology/ecology projections qualified by the preceding
[fresh local native run](../fabric-original17-typed-delta-20261016/README.md).
This copy verifies selected bytes; it is not an additional semantic Delta read.
The upload plan records all selected file hashes, sizes, paths and native UUIDs.
No bearer tokens are retained. The archived scripts are historical operator
receipts with temporary paths, not an installed general-purpose uploader.

Native creation exposed two generator compatibility defects: dataSources 1.1.0
requires referenceName and rejected the name-based mapping, while Graph Model
property type INT64 was invalid. The original attempts are terminal Failed with
ModelValidationError; they were not retried unchanged. The working existing R1
native definition uses dataSources 1.0.0 with name/type/properties. The generator
now emits that version and maps its internal INT64 projection to Graph Model INT.
Delta BIGINT and signed64 readback semantics remain unchanged. See the
[documented signed64 mapping](https://learn.microsoft.com/en-us/fabric/graph/limitations).

Both corrected native creation operations succeeded:

| Pack | Graph Model ID | Existing load job ID |
| --- | --- | --- |
| archaeology | 7c2c0f82-336d-495b-ac1b-98c41453c8f0 | b2aa2054-40eb-402d-9eb4-f8b85d2c9632 |
| ecology | 1f78d089-933b-4ad9-8058-f7d3ac6d3c5f | ddc0c812-0c1f-46ac-bc24-3498b6282f38 |

Workspace: 0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793. Lakehouse:
ee66f2e8-9072-4a48-8364-ecd2f2967c3e. Destinations, full mapping parts,
original-source selections and operation/result responses are archived verbatim
with zero-timestamp gzip and compressed/uncompressed hashes in manifest.json.

Manual refresh requests were accepted then terminal Deduped because other
loads already existed (native invokeType Manual). Native job lists identified those actual handles; the
latest exact-job observations report NotStarted, not completion or failure.
Continue observing these handles rather than resubmitting refresh. No original
GQL case was submitted against these unloaded models. The tables/models remain
for continuation; no cleanup or immutability guarantee is asserted.

Fifteen focused graph-definition, original-pack preparation, typed materializer
and GQL-plan tests pass; module boundaries pass. Independent review approved
nonoverwrite copy behavior and the wire compatibility changes. Native creation
success does not prove loaded graphs, original17 GQL results, immutable release
refresh or live publication/source-ACK authority. Those qualifications remain open.
