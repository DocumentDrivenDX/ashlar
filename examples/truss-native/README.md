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
