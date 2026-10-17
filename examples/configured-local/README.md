# Publish a configured JSONL source on local Delta

Install the toolkit wheel with its `local-delta` extra into a Python 3.11
environment. This development profile uses Spark 4.0.1, Delta 4.0.0, one local
worker and a 512 MiB driver. It does not select cloud compute.

Copy [the invocation template](configuration.template.json) to your own file.
Replace every example path and installation identity with explicit selections.
Select the cached `delta-spark_2.13-4.0.0.jar` and `delta-storage-4.0.0.jar` in a
directory containing exactly those two files. The toolkit verifies their bytes.
Select a real JDK home, the installed Python executable, and regular Bun/Git
executables. The UMF checkout must be clean at revision
`c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3`.

Use the original configured-source file and its pinned inputs unchanged. The
committed example contains three complete transactions covering create, update
and delete; it uses explicit development STRING bindings. The source's original
byte-offset checkpoints remain separate from batch ordinals.

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
reads; the STRING-only Weft query profile below refuses typed sources.

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
Required, facetless STRING fields may repeat using distinct output aliases. This profile
requires one unfiltered record scan; joins, grouping, ordering, limits, optional
values and other scalar families need separate admission.

The result contains positional STRING carrier rows, original column descriptors,
exact compiler protocol texts and bounded execution evidence. Both compilations
must agree; emitted guards and SQL run unchanged under the selected publication.
Independent original source-prefix rows supply the expected bag. Temporary
external aliases preserve the original table UUIDs and versions. Results become
available only after all source, publication, alias, native and supervisor closing
checks. Query mode does not publish or advance progress. Compiler/source failures
or a result exceeding the whole receipt bound refuse without truncating output.
