# Accept original schema through Truss

`accept-schema.py` is a concrete existing-installation example using the public
Python APIs at Truss commit `559f3cda1566685c106b6f1b8a16d157b617cb4b`.
It prepares original UMF bytes through Truss's bundled Bun producer, accepts them
in a caller-owned transaction, commits, then reads the actual report and complete
catalog in a new transaction. No schema conversion or fabricated IDs are used.

Prerequisites: installed Ashlar and that Truss Python package, Python 3.11+,
psycopg/psycopg-binary 3.3.6 with libpq 18.6, PostgreSQL 18.6, and the Bun version
selected by Truss's bundled producer. This version tuple follows Truss's public
`packages/python/README.md`. A
[small native run](../../docs/helix/04-build/evidence/truss-schema-acceptance-20261012/README.md)
accepted one original UMF 0.7.0 schema and read its real catalog successfully.

An administrator must already have installed the **compatible current Truss
catalog runtime**, committed it, and granted its registrar role to your ordinary
login. Select that existing database directly, with `SET ROLE` left `none`.
The example does not provision roles, create an installation, or migrate an older
Truss runtime. The selected documents must be supported by Truss's current catalog
profile. Preparation does not imply acceptance; unsupported content can refuse.

Create an operator-private JSON file (for example mode 0600):

```json
{
  "connection_uri": "postgresql://your-user:YOUR-PASSWORD@localhost:5432/your-database",
  "bun": "/absolute/path/to/bun",
  "documents": [
    {
      "path": "/absolute/path/to/original.umf.json",
      "document_id": "your-original-document-id",
      "document_revision": "1"
    }
  ],
  "request_key": "accept-your-schema-v1",
  "origin_path": "/absolute/path/to/original-origin.json"
}
```

IDs/revisions are your original document selections, not generated catalog IDs.
Origin is the exact caller-selected JSON bytes, separate from authenticated actor.
Use original documents without rewriting unknown extensions. This example bounds
configuration/origin to 64 KiB and each of 1–16 documents to 1 MiB; Truss applies
its own preparation and semantic limits.

```sh
ashlar accept-truss-schema \
  --configuration /absolute/path/to/private-config.json \
  --output /absolute/path/to/fresh-accepted-catalog.json
```

The fresh report is written only after transactions, assembly admission, host
attachment and connection cleanup. It contains the actual registration ID,
revision and replay flag, plus **original complete report and catalog JSON bytes
as Base64** (`report_original_base64`, `catalog_original_base64`), preserving
unknown fields without reconstruction. Catalog readback selects the accepted
historical revision, so a concurrent catalog advance cannot change the report.
Decode those fields to inspect accepted
native IDs and definitions. Credentials are never printed by this script.

Failure does not automatically retry or roll back a commit that already
succeeded. A network error during commit can leave the outcome unknown; inspect
Truss's durable registration/report using the same original request before
choosing a retry. A post-commit report/cleanup failure likewise does not undo the
schema acceptance. This example qualifies no mutation, import, feed, source ACK,
query compilation or cross-engine schema correspondence.

## Create objects and an independent relationship

`mutate-graph.py` uses the accepted, unchanged `cohort` document from the example
above: module `m`, records `A` / `B`, string fields `A-label`, `A-value`, `B-label`,
`B-value`, and directed relationship `A-B`. It resolves genuine current type and
relationship references through public Truss calls; no native IDs or profile pins
are fabricated. This is deliberately one supported schema, not a schema converter.

An administrator must configure `ModuleAccessGrant('cohort', 'm', reader_role,
writer_role)` through public `configure_module_access_in_transaction`, commit it,
and grant the selected ordinary actor both roles with `SET FALSE`. The roles must
already exist as safe distinct non-login roles. The script provisions nothing.

Use a private configuration file:

```json
{
  "connection_uri": "postgresql://your-user:YOUR-PASSWORD@localhost:5432/your-database",
  "bun": "/absolute/path/to/bun",
  "document_path": "/absolute/path/to/original-cohort.umf.json",
  "document_revision": "r1",
  "request_key": "create-cohort-pair-v1",
  "origin_path": "/absolute/path/to/string-origin.json",
  "a": {"label": "first", "value": "one"},
  "b": {"label": "second", "value": "two"}
}
```

Origin is an original JSON map with **string values**, for example
`{"x-action":"create-cohort-pair"}`. The script makes an explicit public
`MapValue` from that map; other value kinds refuse without conversion. Keep
original schema bytes, request key, origin and values unchanged for a retry.

```sh
ashlar mutate-truss-graph \
  --configuration /absolute/path/to/private-mutation.json \
  --output /absolute/path/to/fresh-mutation-readback.json
```

The atomic group creates rootless A/B objects and an independent A-B edge using
within-group aliases. After caller commit, it reads both objects and the edge by
actual returned identities and compares complete string values, versions and
endpoints. It then requests the original group again to obtain a genuine
**committed receipt replay**, not additional mutation, and checks semantic parity.
The fresh report includes complete public group/readback dataclass fields and
receipt evidence (its original byte carrier remains Base64), plus the returned
receipt token. It is written only after connection/host cleanup. No `str`/`repr`
fallback or automatic retry is used. If commit or later readback fails, inspect
current Truss outcome before retrying; already committed data is retained.

A [small native run](../../docs/helix/04-build/evidence/truss-graph-mutation-20261012/README.md)
passed create, exact identity readback and committed receipt replay in 2.6 seconds.
It does not qualify updates/deletes, evolution, imports,
feed/ACK, compiled queries, graph projections or broader schemas.

The installed `mutate-truss-graph` command and the compatible
`mutate-graph.py` script share the same public Truss implementation.

## Update and delete the created graph

`change-graph.py` selects the actual IDs from the prior mutation report, then
resolves current public type/relationship references and reads every object and
edge under current module authorization. A report supplies IDs, never permission.
It updates only the edge's retained `note` string and order key with an exact
expected version, commits, and checks the complete edge values/endpoints/version.
It then deletes the edge **before** both objects in one atomic group with current
expected versions, commits, and verifies all three identities are absent. It also
retrieves and preserves both genuine committed receipt replays. Primary key
properties are never modified.

The original installation, accepted cohort bytes and existing ordinary reader /
writer memberships from the preceding example must remain available. There is
no installation, acceptance or permission setup in this script. Select a fresh
private configuration:

```json
{
  "connection_uri": "postgresql://your-user:YOUR-PASSWORD@localhost:5432/your-database",
  "bun": "/absolute/path/to/bun",
  "document_path": "/absolute/path/to/original-cohort.umf.json",
  "document_revision": "r1",
  "origin_path": "/absolute/path/to/string-origin.json",
  "prior_report": "/absolute/path/to/mutation-readback.json",
  "update_request_key": "update-original-cohort-v1",
  "delete_request_key": "delete-original-cohort-v1",
  "note": "updated relationship",
  "order_key": "first"
}
```

```sh
ashlar change-truss-graph \
  --configuration /absolute/path/to/private-change.json \
  --output /absolute/path/to/fresh-change-readback.json
```

The installed `change-truss-graph` command and compatible `change-graph.py`
script share the same implementation.

The report retains complete supported public before/after values, semantic group
results, receipt replay evidence and absence outcomes. Receipt original byte
carriers remain Base64; there is no stringification fallback. It is written only
after caller connection/host cleanup. Two separate commits mean update may have
succeeded even if deletion or reporting fails. Inspect current durable outcomes
before an operator-chosen recovery; no automatic retry or reset occurs. This
source example passed a [small native run](../../docs/helix/04-build/evidence/truss-graph-change-20261012/README.md)
in 1.1 seconds. It does not broaden feed/query/import
support.

## Edit an object's non-key text

[note.umf.json](note.umf.json) is an explicitly authored original UMF 0.7 schema:
`ashlar-note` / module `notes` / record `Note`. Required string `note-id` is the
primary key; required string `note-text` is **not** a key. It uses the supported
public scalar-string catalog shape, not a conversion of another corpus.

First use `accept-schema.py` with this original document (`document_id`:
`ashlar-note`, revision `r1`) and a new registration request key. This is an
**additional document**, not evolution of an existing accepted definition. An
administrator must separately configure current module access for
`ModuleAccessGrant('ashlar-note', 'notes', reader_role, writer_role)` and give your
ordinary actor the real reader/writer memberships as described above.

Then select this private edit configuration:

```json
{
  "connection_uri": "postgresql://your-user:YOUR-PASSWORD@localhost:5432/your-database",
  "bun": "/absolute/path/to/bun",
  "document_path": "/absolute/path/to/note.umf.json",
  "document_revision": "r1",
  "origin_path": "/absolute/path/to/string-origin.json",
  "create_request_key": "create-note-v1",
  "update_request_key": "edit-note-v1",
  "key": "note-1",
  "text": "Original text",
  "updated_text": "Edited text"
}
```

```sh
ashlar mutate-truss-note \
  --configuration /absolute/path/to/private-edit.json \
  --output /absolute/path/to/fresh-edit-readback.json
```

The example resolves the current accepted type, creates one Note, commits and
reads its exact values/version. It changes only `note-text` using `UpdateObject`
and the actual expected version, commits and checks full values, unchanged key,
identity, retained fields and the incremented version. Both complete original
group results and committed receipt replay evidence are preserved, with byte
carriers remaining Base64. No IDs/profile pins are fabricated, no schema is
rewritten, and no stringification fallback is used. Commit uncertainty requires
operator inspection before any retry. A [small native run](../../docs/helix/04-build/evidence/truss-object-edit-20261012/README.md)
passed in 5.8 seconds, including document addition, non-key update and committed
receipt replays. It
claims no schema evolution, feed/import/query or ACK support.

## Bootstrap a fresh local Note database

`ashlar setup-truss-local` supplies the concrete local prerequisite for the Note workflow.
Use the same installed Truss/Python/native version tuple above, plus Truss's
supported local runtime dependency `pgserver-pg18==18.6.1`, installed in the same
Python environment used by `ashlar`. No server binaries are
built or downloaded by the script. Select a **fresh** absolute data directory
under an existing parent and a fresh report path; existing data/report paths
refuse without reset or takeover. It provisions only its own newly created local
cluster, never a remote database.

Example private configuration (replace all paths):

```json
{
  "data_directory": "/absolute/private/local-note-data",
  "bun": "/absolute/path/to/bun",
  "document_path": "/absolute/path/to/examples/truss-native/note.umf.json",
  "document_id": "ashlar-note",
  "document_revision": "r1",
  "origin_path": "/absolute/private/string-origin.json",
  "request_key": "bootstrap-note-v1"
}
```

```sh
ashlar setup-truss-local \
  --configuration /absolute/private/setup-note.json \
  --output /absolute/private/fresh-bootstrap-receipt.json
```

The repository's `setup-local.py` delegates to the same implementation. The
command prints the private receipt location after its owned server has stopped.

The script installs the catalog through its public installer, creates local
`ashlar_note_consumer` LOGIN and distinct `ashlar_note_reader` /
`ashlar_note_writer` NOLOGIN roles, grants the **actual returned** registrar role
with `SET FALSE`, accepts the original Note document, and configures module
access through public `ModuleAccessGrant`. It grants both existing module roles
to that ordinary consumer with `SET FALSE`. It closes all connections and stops
the server before writing the receipt. The receipt preserves actual installation,
catalog status, role labels and original report bytes, and confirms the caller
commits; the installer's original `pending_commit` field is retained as returned.
No connection URI or credential is output. Partial local data remains on failure;
do not reset it or retry blindly.

Keep the initialized server running in one terminal:

```sh
ashlar serve-truss-local --data-directory /absolute/private/local-note-data
```

The foreground command prints the public SDK's current connection URI after
startup. Use that URI for administrator operations and the same endpoint with
the `ashlar_note_consumer` user for ordinary operations in another terminal.
Ctrl-C or SIGTERM stops its owned server. An existing server marker is refused;
the command does not attach to another running server.

Socket locations can change between runs, so obtain the current URI each time.
Applications can instead own a public `LocalPostgres` context directly:

```python
import json
from pathlib import Path
import psycopg
from truss import LocalPostgres

edit_config = Path("/absolute/private/edit-note.json")  # config from edit section
with LocalPostgres("/absolute/private/local-note-data") as runtime:
    params = psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
    params["user"] = "ashlar_note_consumer"
    selected = json.loads(edit_config.read_bytes())
    selected["connection_uri"] = psycopg.conninfo.make_conninfo(**params)
    # Keep this private (0600); do not print its connection URI.
    edit_config.write_text(json.dumps(selected))
    edit_config.chmod(0o600)
    from ashlar_host.truss_note_mutation import run_mutate_truss_note_command
    run_mutate_truss_note_command(edit_config, "/absolute/private/fresh-edit-result.json")
# Every consumer connection closes before the runtime stops.
```

Use fresh business keys/request keys for a new Note, or inspect original durable
outcomes before deciding an intentional replay. Bootstrap is local development
only; it adds no remote provisioning, credentials, production grant, feed/ACK,
compiled-query or schema-evolution claim. A
[small native setup/restart run](../../docs/helix/04-build/evidence/truss-local-bootstrap-20261012/README.md)
passed in 20.4 seconds, including ordinary-account Note editing after restart.

## Evolve the same Note document

`ashlar evolve-truss-note` uses public Truss APIs at committed
`5366f7d64ae1c5657cb6ec261b956fe1acd1e234`, including current identities returned
by object reads. Install that compatible package/runtime on a fresh local setup;
the earlier example package does not establish upgrade compatibility.
[note-r2.umf.json](note-r2.umf.json) is an authored **UMF 0.7.0** revision of
`ashlar-note`, preserving its existing IDs/key/fields and adding optional string
`note-tag`. Create/edit one r1 Note using the preceding examples first. The
ordinary actor needs existing module read/write and registrar memberships.

Private configuration:

```json
{
  "connection_uri": "postgresql://your-user:YOUR-PASSWORD@localhost:5432/your-database",
  "bun": "/absolute/path/to/bun",
  "document_path": "/absolute/path/to/note-r2.umf.json",
  "document_revision": "r2",
  "origin_path": "/absolute/private/string-origin.json",
  "accept_request_key": "evolve-note-r2",
  "update_request_key": "tag-note-r2",
  "object_id": 1,
  "tag": "follow-up"
}
```

Select the actual object ID returned by your own earlier mutation, not the
illustrative `1`. Run while the selected local server is owned, using its current
connection URI:

```sh
ashlar evolve-truss-note \
  --configuration /absolute/private/evolve-note.json \
  --output /absolute/private/fresh-evolution-result.json
```

The script reads the complete original Note, accepts the revised original document
and commits. It checks the exact committed document/report, unchanged object/type
IDs, values and version, and the new current definition identity. The optional
field must remain absent. It then updates only `note-tag` using that returned
identity and expected version, commits, reads all values, and retrieves the
original committed update receipt. The report preserves public carriers and
original document/report bytes as Base64, and is written after connection/host
cleanup. Existing `pending_commit` carrier status remains unchanged; caller
commit confirmation is recorded separately. This is a one-time transition;
commit uncertainty or a partially completed run requires durable-outcome
inspection before any intentional recovery. It performs no automatic retry,
runtime migration, source feed/ACK or Delta publication.

## Deliver complete committed feed pages

`read-feed.py` uses the public committed feed at Truss
`5ca2ed96f44f82b167ba16cf84c442c739e022d4` (introduced by `ae273e92`).
Use a compatible fresh installation with the Note workflow above. It enrolls
an existing ordinary reader role through public administrator registration,
then reads as an existing ordinary login which already holds that role.
Registration grants feed access; it is deliberately separate from Note mutation
permissions. No roles or memberships are fabricated by this example.

Private configuration:

```json
{
  "administrator_uri": "postgresql://local-administrator@localhost/your-database",
  "consumer_uri": "postgresql://ashlar_note_consumer@localhost/your-database",
  "consumer_id": "ashlar-note-files-v1",
  "reader_role": "ashlar_note_reader",
  "maximum_pages": 16
}
```

Use current connection URIs from your owned local runtime. The administrator
connection must be the actual installation administrator; keep configuration
private. Run once with a fresh absolute spool directory:

```sh
python examples/truss-native/read-feed.py \
  --configuration /absolute/private/feed.json \
  --output /absolute/private/fresh-feed-spool
```

Each page is one complete committed transaction or a safe empty interval,
including original payload bytes, catalog prerequisites, revisions, source
records and reservations. All public byte fields use explicit Base64 carriers;
opaque prior/next checkpoints retain their installation/epoch/registration
binding. The example bounds each page to 4,096 facts / 8 MiB and the run to
1–16 pages, without truncating a transaction. Large transactions refuse.

It fsyncs the complete page and directory **before** source ACK, then commits
that ACK and checks its genuine `equal` replay in a new transaction. Original
pending durability flags remain intact beside caller-commit confirmation.
`result.json` appears only after connections/hosts close; its empty-interval flag
reports whether the finite run reached the observed safe frontier. A page limit
can leave more work pending. Partial files or unknown commits require inspection
of the source checkpoint and retained original pages before recovery; the script
never deletes them or retries automatically.

This intermediate example establishes local durable **file delivery**, not an
Ashlar Delta publication. Source ACK asserts only that delivery; it does not
verify another database's durability or authorize history retention release.
The complete feed-to-Delta workflow must publish corresponding original source
transactions durably before acknowledging them.

For application integration, `ashlar.truss_native_feed.native_feed_batch`
accepts a complete original page, its SHA-256, original prior/next cursor bytes
and an explicit feed name. The native batch can use `ashlar.staging.batch_row`
and `ashlar.source_checkpoint.truss_checkpoint` to retain transaction custody
in the publication path. It preserves Truss's opaque checkpoints rather than
converting them to outbox positions or JSONL offsets.

`scalar_object_changes` maps full rootless STRING object afterimages into
Ashlar's create/replace/delete changes, using the true entity version. Property
events require a corresponding full afterimage. Schema admission, durable Delta
effects and descriptor-bound source ACK remain responsibilities of the consumer;
these extraction and custody APIs alone do not perform that workflow.

`scalar_entity_changes` also extracts native edges with typed source/target
identities and null/text order keys. Native Delta planning materializes those
order keys and retains complete lifecycle event bytes. It still requires an
independent schema-admission policy; the Note consumer remains object-only.
`ApplyState` does not carry edge ordering. The native graph publisher checks
original before-image order keys against its previously published physical edge
rows before planning effects, including updates and deletion. Pure apply checks
alone do not establish that parity.

`ashlar_host.truss_graph_admission.TrussGraphAdmission(batch, catalog_views)`
admits rootless STRING records and empty-property directed independent edges.
Use `read_historical_catalog_views(connection_factory, batch)` from
`ashlar_host.truss_catalog_reader` to obtain those bytes through an ordinary
read-only Truss transaction. Supply original complete historical public
catalog-view bytes keyed by revision;
the accepted UMF/report bytes come from the original page. Admission checks actual
allocated IDs, definition pins, required fields and endpoint types. Scoped views
and unsupported semantics refuse admission. Pair this policy with the same actual
`TrussFeedSessions` owner for source reads and ACK; semantic admission alone does
not publish data or establish current source authority.

`TrussNoteAdmission(batch)` admits the Note's authored STRING fields against
original accepted UMF bytes and actual catalog IDs. Native graph SQL planning
retains lifecycle events in history and every property event in the complete
staged page. `TrussFeedSessions` supplies ordinary source reads and source ACK
to `NativeDriver`; use the same owner for both session ports. It resolves and
pins the whole destination publication before the public Truss ACK and caller
commit. Commit uncertainty uses a fresh read, without retrying the write.
The same owner closes a completed ACK against a fresh authenticated read of its
exact committed cursor. Replay from a fresh owner after ACK requires retained-
interval read support; an advanced source cursor currently refuses that replay.

Use `ashlar.truss_publication.publish_truss_transaction` with the configured
publication backend to run the durable coordinator with a native batch.
`ashlar_host.publication_planning.truss_request_for` constructs its matching
request for effect planning. Register a separate consumer for Delta delivery;
the file-spooling example's ACK asserts file delivery and consumes that interval.

## Publish Note pages into local Delta

`ashlar publish-truss-note` composes the actual public Truss feed with Ashlar's existing
native Delta effect journal, six tables, immutable publication manifest and
ordinary source ACK. Use the compatible Truss feed package described above and
installed Spark/Delta/JDK dependencies already selected for Ashlar. It starts
only a local Spark context (`local[1]`, 512 MiB); it installs nothing.

Create/edit/evolve your Note first. Select a **new distinct Delta consumer ID**;
do not reuse the file-delivery consumer from `read-feed.py`, which has already
acknowledged its pages. The installation administrator registers this consumer
against your existing ordinary reader role and commits. Your existing ordinary
consumer login must hold that role. The first source page must retain the actual
genesis checkpoint. Catalog-only initial pages are published before later entity
pages; none are skipped to start at the current Note state.

Private configuration (replace all selections):

```json
{
  "administrator_uri": "postgresql://local-administrator@localhost/your-database",
  "connection_uri": "postgresql://ashlar_note_consumer@localhost/your-database",
  "reader_role": "ashlar_note_reader",
  "consumer_id": "ashlar-note-delta-v1",
  "feed": "ashlar-note-native",
  "jars": ["/absolute/path/to/delta-spark.jar", "/absolute/path/to/delta-storage.jar"],
  "maximum_pages": 8
}
```

Use current URIs from your owned local Truss runtime. Select an existing pair of
compatible Spark/Delta JARs and your installed JDK through `JAVA_HOME`. Run with a
fresh canonical output directory:

```sh
ashlar publish-truss-note \
  --configuration /absolute/private/publish-note.json \
  --output /absolute/private/fresh-note-delta
```

The repository's `publish-note.py` calls the same command implementation. CLI
output contains the result location and publication count; complete manifests
remain in the private destination.

The example reads at most eight complete pages, admits Note schema semantics from
original accepted UMF 0.7 document/report bytes and actual catalog IDs, and derives
full expected destination rows from native afterimages. Entity versions are the
actual source versions. Local Delta effects, history and manifest are checked
before the public source ACK is submitted and caller-committed. Opaque cursor
bytes retain the original installation/epoch/registration correspondence.
`result.json` is written after native journal and lease cleanup; the wrapper closes
Spark before returning. A source or destination failure retains partial tables,
original attempts and durable checkpoints. Inspect those outcomes before recovery;
this fresh-run example never resets or automatically retries them.

The supported entity subset is rootless scalar-string Note create/update/delete,
including the additive optional tag revision. Separate property events remain in
the complete original staged page; lifecycle afterimage events form entity history.
The example does not claim Unity Catalog execution, arbitrary schemas/edges,
restart recovery or already-acknowledged source replay. Native execution evidence
is still required to qualify this newly composed workflow.

### Publish scalar records and edges

Use `ashlar publish-truss-graph --configuration /absolute/private/graph.json
--output /absolute/private/fresh-graph` with the same configuration shape as
`publish-truss-note`. Select a separate consumer at genesis and existing local
Spark/Delta dependencies. The command reads accepted historical catalogs through
the ordinary reader, admits original UMF 0.7.0 rootless STRING records and
empty-property directed independent edges, and publishes complete transactions
before their public Truss ACK. Object and edge updates/deletes retain lifecycle
history; edge order keys preserve null/text values.

The destination must be fresh. This command is a finite local workflow, limited
to eight pages; it does not provide restart recovery or Unity Catalog deployment.
Retain partial destinations on failure rather than resetting them or reusing an
advanced source consumer.

Local setup and publication check the installed public Truss
`PsycopgProfile` against the selected Python driver before starting resources or
registering a consumer. Use the driver version, implementation and libpq build
required by that installed SDK in the same Python environment. Truss verifies
the PostgreSQL server version on the actual connection. A mismatch is a setup
error; retain partial destinations and select a compatible environment before
retrying.
