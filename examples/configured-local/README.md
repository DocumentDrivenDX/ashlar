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
fencing, general typed records, or PostgreSQL/Truss ACK authority. Preserve the
original artifacts when an operation refuses; partial native work may require
recovery through its retained original operation handles.

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
