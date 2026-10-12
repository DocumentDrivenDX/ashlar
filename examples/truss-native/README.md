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
python examples/truss-native/accept-schema.py \
  --configuration /absolute/path/to/private-config.json \
  --output /absolute/path/to/fresh-accepted-catalog.json
```

The fresh report is written only after transactions, assembly admission, host
attachment and connection cleanup. It contains the actual registration ID,
revision and replay flag, plus **original complete report and catalog JSON bytes
as Base64** (`report_original_base64`, `catalog_original_base64`), preserving
unknown fields without reconstruction. Decode those fields to inspect accepted
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
python examples/truss-native/mutate-graph.py \
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
python examples/truss-native/change-graph.py \
  --configuration /absolute/path/to/private-change.json \
  --output /absolute/path/to/fresh-change-readback.json
```

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
python examples/truss-native/edit-object.py \
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

`setup-local.py` supplies the concrete local prerequisite for the Note workflow.
Use the same installed Truss/Python/native version tuple above, plus Truss's
supported local runtime dependency `pgserver-pg18==18.6.1`. No server binaries are
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
python examples/truss-native/setup-local.py \
  --configuration /absolute/private/setup-note.json \
  --output /absolute/private/fresh-bootstrap-receipt.json
```

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

For later work, restart the retained directory with a **new public**
`LocalPostgres` context and obtain its current connection URI. Socket locations
can change: never cache the bootstrap run's URI. For example, run the Note edit
library example while the restarted server is owned:

```python
import json, runpy
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
    edit = runpy.run_path("examples/truss-native/edit-object.py")
    edit["run"](edit_config, "/absolute/private/fresh-edit-result.json")
# Every consumer connection closes before the runtime stops.
```

Use fresh business keys/request keys for a new Note, or inspect original durable
outcomes before deciding an intentional replay. Bootstrap is local development
only; it adds no remote provisioning, credentials, production grant, feed/ACK,
compiled-query or schema-evolution claim. A
[small native setup/restart run](../../docs/helix/04-build/evidence/truss-local-bootstrap-20261012/README.md)
passed in 20.4 seconds, including ordinary-account Note editing after restart.
