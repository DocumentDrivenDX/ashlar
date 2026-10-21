# Publish a configured JSONL source on local Delta

Install the toolkit wheel with its `local-delta` extra into Python 3.12. This
configured-local profile uses Spark 4.2.0, Delta 4.4.1 and JDK 21, with one
local worker and a 512 MiB driver. It does not select cloud compute. The two
selected JARs are pinned by SHA-256 in the invocation template; a directory may
also contain the upstream `inventory.json` beside those two JARs.
The profile pins `spark.sql.legacy.parameterSubstitution.constantsOnly=true`
before session creation and checks it throughout the held runtime. This Spark
4.2 compatibility setting preserves bound `:name` parameters across Delta
relations and parameterized publication statements.

Copy [the invocation template](configuration.template.json) to your own file.
Replace every example path and installation identity with explicit selections.
Select the prescribed Delta 4.4.1 JARs in a directory containing exactly those
two JARs, optionally with `inventory.json`; the toolkit verifies their bytes.
Select a JDK 21 home, the installed Python executable, and regular Bun/Git
executables. For the STRING and scalar typed sources, the UMF checkout must be
clean at revision `c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3`. The typed graph
source below selects its own explicit producer revision.

The current template selects the authored typed graph source and UMF producer
revision `2ede4c879bea5db13794a6fa30e67c821306c17f`. That checkout provides the
R01 cohort: core `0.10.0`, access `0.1.0.dev5` and codegen `0.1.0a2`. Preserve
the source's pinned model and its declared version. The prescribed JAR digests
are `2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be`
(`delta-spark_4.2_2.13-4.4.1.jar`) and
`f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f`
(`delta-storage-4.4.1.jar`).

Use the original configured-source file and its pinned inputs unchanged. The
committed template selects the original UMF 0.10 typed-graph model and its
explicit development graph bindings. Its three complete transactions cover
create, update and delete; the source's original byte-offset checkpoints remain
separate from batch ordinals. The historical STRING-source workflow is described
separately below and is not selected by this template.

Create an existing private parent directory owned by you, with no group or world
write permissions. `installation` and `output` must be separate fresh children;
keep both outside the producer checkout and original inputs. Set an explicit UTC
epoch-microsecond `materialization_clock_us`, and retain that same value for all
operations on this installation. It labels materialized rows; retention admission
uses actual native commit timestamps and the current clock.

For initialization set `operation` to `initialize`, `ordinal` to `0` and
`installation_sha256` to `null`, then run:

```sh
python -m ashlar run-configured-local --configuration /absolute/operator/initialize.json
```

Retain the original installation, its journal and the successful report. For
publication, create another invocation file with `operation: "publish"`, the same
source/installation/stream/clock selections, a fresh `output`, and the SHA256 of
original `installation/installation.json` in `installation_sha256`. Select
`ordinal: 1`, then run the same command with that invocation. Repeat with fresh
outputs and ordinals `2` and `3` to publish the example's remaining transactions.
Repeat the last completed ordinal with a fresh output to verify exact replay.
Never reset a journal or regenerate an installation to recover an interrupted run.

A successful invocation prints its final JSON report and writes the same JSON result
to private `report.json`, after worker cleanup and closing checks. It retains
bounded private diagnostics and original UMF admission receipts. Internal
`runtime-report.json` and partial files alone do not establish command success.
Publication reports include the exact manifest, local checkpoint, replay state
and native current/history/tombstone/edge counts. Local checkpoints are durable
consumer progress, **not protected source ACKs**.

This profile relies on an exclusive cooperating local installation lease and
owned local files. It does not qualify Unity Catalog, production remote writer
fencing, arbitrary typed models, or PostgreSQL/Truss ACK authority. Preserve the
original artifacts when an operation refuses; partial native work may require
recovery through its retained original operation handles.

## Use an authored typed source

Select [configured-typed-source.json](../end-to-end/configured-typed-source.json)
in `source_configuration` for a separate fresh installation. Keep its original
core 0.8 model, complete source events and explicit development bindings unchanged.
The same initialize/publish/replay/read commands apply; its admitted type ID is
`17`, with object IDs `1` and `2`. The closed `ashlar-jsonl-typed-source/0.1`
profile pins all three input files and the public UMF producer revision.

Fresh public Record validation checks each complete carried value, including
payloads on delete events, before planning or Spark setup. Canonical properties
retain integer and decimal lexical tokens, booleans and absent/null/value states.
Host references are limited to 256 KiB each, the model to 64 KiB and complete
source groups to four. Empty delete payloads, dataset keys and relationships need
separate admission. Development IDs establish no native catalog or protected
source authority. This typed profile supports publication and raw singleton
reads. For typed Weft queries, select the separately authored named revision 2
source described below; the original revision 1 model has no SQL names and remains
unchanged.

## Publish an authored typed graph source

Select [configured-typed-graph-source.json](../end-to-end/configured-typed-graph-source.json)
for a separate fresh installation, using a clean public UMF checkout at
`2ede4c879bea5db13794a6fa30e67c821306c17f`. Preserve the pinned model, complete
source events and explicit bindings unchanged. Follow the same installed CLI
initialization, publication of ordinals 1 through 3, exact replay and singleton
read instructions. Keep source, stream, clock and installation selections fixed
and use a fresh output for each invocation.

Object types are `17` (Person) and `18` (Place); object ID `1` may occur in both
types. Relationships are `31` (knows) and `32` (located), with independent edge
IDs. Singleton selectors admit object types only, not relationship IDs. The
13 events in three complete groups cover parallel edges, a self-loop, an isolate,
cross-type endpoints, endpoint replacement and deletion. See the
[typed graph example](../end-to-end/typed-graph-README.md) for the source contract
and read selections.

Fresh carried Record checks and joint compact-dataset key/relationship checks
precede native planning; original public receipts, context and warnings remain
retained. Only empty edge property objects are admitted. Host references are at
most 256 KiB each, model admission at most 64 KiB, at most four complete groups
and 1000 events, and each public validation request at most 1 MiB. The core source
configuration's 1 MiB source ceiling does not raise the host's 256 KiB reference
ceiling.

Development IDs and supplied-dataset closure establish no protected source ACK,
Unity Catalog or production writer authority. Query operation MUST refuse this
graph source before Spark acquisition until separate graph-query admission
exists; publication or singleton reads do not qualify graph Weft queries.

## Read a retained publication

Use a fresh invocation with `operation: "read"`, the pinned original installation,
a fresh `output`, and the exact completed `ordinal` to read. Add the required
selector, using admitted type IDs from the original configured bindings:

```json
"selector": { "type_id": 1017, "entity_id": 1 }
```

Run the same `ashlar run-configured-local --configuration ...` command. The
result contains the full native row or `row: null` for an absent object within
an admitted type. Unknown types refuse. The selected ordinal binds the complete
original publication vector; it does not follow current table heads. An older
publication can therefore return a value that was later updated or deleted,
while retention and current local admission still allow that snapshot.

Read mode opens existing local progress only and refuses missing proof tables.
It does not publish, replay or advance the checkpoint. Its report includes the
selected manifest and the unchanged latest local checkpoint, which may be later
than the selected publication's checkpoint. The local lease supplies cooperative
snapshot custody; it is not a PostgreSQL pin or protected producer ACK.

## Query a retained publication with Weft

Install both the `local-delta` and `weft-validation` extras. Select an existing
[indexed COUNT-star compiler installation](../../src/ashlar/README.md); this
command uses its indexed macOS 27.0.1 arm64 realization and explicit local Spark
0.4.1 profile.

Copy the read invocation, keep the original installation/source pins and selected
completed ordinal, choose a fresh output, remove `selector`, set
`operation: "query"`, and add:

```json
"query": {
  "profile": "ashlar-configured-local-weft/0.1",
  "sql": "SELECT i.label FROM Item i",
  "compiler": {
    "index": "/absolute/operator/weft/distributions/index.json",
    "installation": "/absolute/operator/weft/compiler"
  },
  "bounds": {
    "maximum_artifact_bytes": 1048576,
    "maximum_rows": 1000,
    "maximum_cell_bytes": 65536,
    "maximum_total_cell_bytes": 1048576
  }
}
```

Run the same `ashlar run-configured-local --configuration ...` command. For the
example, `SELECT COUNT(*) AS n FROM Item i` selects a global count instead.
For the original STRING source, required facetless STRING fields may repeat using
distinct output aliases. The configured query route requires one unfiltered
record scan. Joins, grouping, ordering, limits, arithmetic and relationships need
separate admission; typed fields use the explicit subset below.

The result contains positional native carrier rows, original column descriptors,
exact compiler protocol texts and bounded execution evidence. Both compilations
must agree; emitted guards and SQL run unchanged under the selected publication.
Independent original source-prefix rows supply the expected bag. Temporary
external aliases preserve the original table UUIDs and versions. Results become
available only after all source, publication, alias, native and supervisor closing
checks. Query mode does not publish or advance progress. Compiler/source failures
or a result exceeding the whole receipt bound refuse without truncating output.

### Query the named typed source

Use [configured-typed-named-source.json](../end-to-end/configured-typed-named-source.json)
for a **separate fresh installation**. This explicitly authored revision 2 adds
SQL names while pinning its own model, source events and development bindings.
Do not replace or relabel the original revision 1 files or reuse its installation.
Keep the same indexed compiler paths, extras and query bounds shown above.

After publishing ordinal 1, select `operation: "query"`, remove `selector`, choose
a fresh output and set:

```json
"ordinal": 1,
"query": {
  "profile": "ashlar-configured-local-weft/0.1",
  "sql": "SELECT i.quantity, i.amount, i.active FROM Item i",
  "compiler": {
    "index": "/absolute/operator/weft/distributions/index.json",
    "installation": "/absolute/operator/weft/compiler"
  },
  "bounds": {
    "maximum_artifact_bytes": 1048576,
    "maximum_rows": 1000,
    "maximum_cell_bytes": 65536,
    "maximum_total_cell_bytes": 1048576
  }
}
```

The typed descriptor subset preserves exact unsigned 64-bit integers,
`DECIMAL(18,2)` and BOOLEAN. Original lexical property text remains available;
query carriers and decoders must retain their exact descriptor meanings. This
does not admit arithmetic, joins, relationships or arbitrary typed models.

After publishing ordinal 2, use a fresh query invocation with `ordinal: 2` and
`SELECT i.quantity, i.amount, i.active, i.note FROM Item i`, retaining the same
compiler and bounds. At this publication both notes are present: one Unicode
value and one explicit null. The optional STRING home uses the explicit
`ashlar-weft-json-native-null/0.1-candidate` encoding. A missing property key MUST
refuse its emitted guard; absence is not a successful null result. In particular,
selecting `note` at ordinal 1 must refuse because the first object's key is
missing.
