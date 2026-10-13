# Local Truss toolkit

## One typed-record command

With the compatible public Truss toolkit, Spark 4.0.1 / Delta 4.0.0, Bun and JDK
21 installed in your selected environment, supply these existing runtimes:

```json
{
  "profile": "ashlar-local-truss-record-example/0.1",
  "bun": "/absolute/path/to/bun",
  "java_home": "/absolute/path/to/jdk21",
  "jars": ["/absolute/path/delta-spark.jar", "/absolute/path/delta-storage.jar"]
}
```

```sh
ashlar run-truss-record-example --configuration /absolute/private/typed-example.json \
  --output /absolute/private/fresh-typed-example
```

The command creates a fresh local Truss installation, accepts the bundled
[typed-record model](typed-record.umf.json), creates one exact typed object,
publishes its catalog and source transaction to local Delta, then reads that
publication by the actual allocated identity. Integer `9007199254740993` and
decimal `1.2300` remain exact; optional fields remain absent. Weft is not required.
The private output retains the setup, source receipt, publication, reader
configuration and singleton result. The server stops before the final report is
written. Retain partial output on failure; never reuse it as a fresh destination.
The component create/publish/read paths have separate native receipts below;
the [one-command local check](../../docs/helix/04-build/evidence/truss-typed-record-example-native-20261013/result.json)
qualifies this composition with its recorded runtime versions. It uses local development authority and caller-asserted
source ACK, with no Unity Catalog or production authorization claim.

### Restart the typed example

Start the retained local installation in one terminal:

```sh
ashlar serve-truss-local --data-directory /absolute/private/fresh-typed-example/data \
  --connection-output /absolute/private/typed-live.json
```

In another terminal, select the exact saved reader configuration and prepare an
ordinary connection for that same installation:

```sh
ashlar configure-truss-example-reader \
  --example-directory /absolute/private/fresh-typed-example \
  --reader-configuration /absolute/private/fresh-typed-example/reader-configuration.json \
  --connection-configuration /absolute/private/typed-live.json \
  --output /absolute/private/typed-reader.json
ashlar read-truss-record --configuration /absolute/private/typed-reader.json \
  --type-id <type_id-from-result.json> --entity-id <entity_id-from-result.json> \
  --output /absolute/private/typed-record-read.json
```

After a continuation, select its saved reader configuration with the same option.
The helper retains the original publication intent and page selection, checks
that they belong to the example, and changes only the connection URI. It performs
no database calls and grants no authority. Omit `--reader-configuration` for the
original STRING demo's `query-configuration.json` default.

## One local end-to-end command

Install Ashlar and the compatible public Truss toolkit with its local PostgreSQL
extra in the selected Python environment, alongside Spark 4.0.1 / Delta 4.0.0. Supply an
existing Bun executable, JDK 21 directory, Delta Spark/storage JARs and installed
public Weft distribution. The workflow uses fresh
local data, a two-record/one-edge graph, two publications and one inverse query.

```json
{
  "profile": "ashlar-local-truss-example/0.1",
  "bun": "/absolute/path/to/bun",
  "java_home": "/absolute/path/to/jdk21",
  "jars": ["/absolute/path/delta-spark.jar", "/absolute/path/delta-storage.jar"],
  "weft_index": "/absolute/path/distributions/index.json",
  "weft_installation": "/absolute/path/weft-installation"
}
```

```sh
ashlar run-truss-example --configuration /absolute/private/example.json \
  --output /absolute/private/fresh-example
```

The [installed command receipt](../../docs/helix/04-build/evidence/truss-example-installed-20261013/result.json)
qualifies this small local workflow with the selected existing dependencies.

Set optional `"export_graph": true` in the configuration to retain the original
publisher's immutable release/custody pair during its live pinned interval.
The final report then includes exact `release` and `custody` path/SHA selectors:

```sh
ashlar configure-graph-export --export-report /absolute/private/fresh-example/result.json \
  --export-report-sha256 <exact-result-file-sha256> \
  --output /absolute/private/graph-selection.json
```

Read the selection locally:

```sh
ashlar query-duckdb-export --configuration /absolute/private/graph-selection.json \
  --output /absolute/private/duckdb-query.json --maximum-hops 2
```

The [small export receipt](../../docs/helix/04-build/evidence/truss-example-export-20261013/result.json)
records the real two-node, one-edge example and DuckDB read.

That pair can also configure the [GraphFrames export workflow](../../README.md)
with explicitly selected existing JARs/JDK. Capture does not qualify native
GraphFrames, PuppyGraph or Fabric execution; each engine retains its own admission.

The private directory retains original model, acceptance, mutation, publication
intent and query reports. `result.json` is written after owned PostgreSQL and
Spark resources close. Failed runs retain partial files; select a fresh directory
for a new example rather than overwriting them. Configuration files contain
local connection details and use mode 0600. Restarting the stopped server requires
its actual newly returned URI; saved runtime URIs are not restart credentials.


## Stream updates and deletions

Add `"exercise_mutations": true` to the example configuration to update the
created B record's label, then delete its incoming edge and the B record through
public Truss groups. Ashlar consumes the two new feed pages into the existing
Delta installation and queries the final publication. A remains present; the
default inverse query returns no B rows after deletion.

The fresh directory retains `lifecycle.json`,
`mutation-continuation.json` and `continued-query-configuration.json` for the
source changes and publication selection. `result.json` reports four completed
publications. An interrupted continuation retains its original recovery
selection; use the existing resume commands before continuing. The option can
be combined with `query_batch` to run the gallery on the final publication.
When also using `export_graph`, the immutable export retains the initial graph
before these mutations; `graph_export_publication_id` identifies that snapshot
separately from the final query publication.
This one-command composition has basic adapter coverage; it adds no native
qualification beyond the separately recorded component workflows.

## Run the gallery during setup

Add `"query_batch": "/absolute/path/cohort-queries.json"` to the example
configuration to run the [three-query gallery](cohort-queries.json) after its two
publications. `run-truss-example` uses the same batch command and saves
`query-batch.json` in the fresh output directory. Its final `result.json` includes
the selected publication, named query results and batch report path. The source
checkpoint must remain unchanged during the queries.

Use the accepted cohort's record names and catalog revision in custom galleries.
The batch accepts one to eight queries within its aggregate row and report bounds.
A query failure prevents the final example result. The default configuration
continues to run the single inverse query. The gallery option has basic adapter
test coverage; the existing native receipt qualifies the default workflow.

## Retain destination receipts with source ACK

Set optional `"destination_id": "your-ashlar-installation"` in the
`run-truss-example` configuration to use receipt ACKs for both publications and
preserve the setting in its saved query configuration.

For a fresh `publish-truss-graph` configuration, add an explicit
`"destination_id": "your-ashlar-installation"`. Select a public Truss SDK and
installation that provide `acknowledge_feed_with_receipt_in_transaction` and
`reconcile_feed_ack_in_transaction`; missing support refuses the option. Keep the
same destination identity in resume and continuation configurations. Omission
retains the existing plain delivery ACK mode. A plain ACK attempt cannot be
converted into receipt proof by adding the setting later.

Ashlar validates the durable publication before retaining its exact request,
manifest and version vector with the source ACK. Uncertain source commit uses
read-only exact receipt reconciliation, with no second ACK write. The receipt
proves source retention; Ashlar still verifies the destination publication.
Oversized complete receipts use a lossless compressed envelope within Truss’s
one-MiB limit. Small receipts keep their original byte encoding.
The [native resume receipt](../../docs/helix/04-build/evidence/truss-receipt-resume-20261013/result.json)
records a small source CLI resume, continuation, pinned inverse query and exact
read-only reconciliation of both receipt formats. It does not qualify a fresh
installed-wheel receipt workflow.

## Compile queries for an owned Truss publication

Use the actual `NativeDriver` from graph publication or recovery with the selected
installed Weft distribution. This API binds the original accepted UMF document to
Truss's allocated type and property IDs, and pins all four graph/history tables
while the public compiler runs:

```python
from pathlib import Path
from ashlar.weft_distribution import DistributionPaths
from ashlar_host.truss_weft_query import compile_truss_string_query

compiled = compile_truss_string_query(
    driver,
    DistributionPaths(Path('/absolute/weft/index.json'),
                      Path('/absolute/weft/installation')),
    'SELECT n."note-text" AS text FROM Note n',
    revision='1', document_id='your-original-document-id',
)
request_bytes = compiled['request_bytes']
response_bytes = compiled['response_bytes']
```

Select the actual record/field names and physical catalog revision from your
accepted schema. This path supports Truss's admitted UMF 0.7 STRING records and
returns the original compiler protocol bytes without executing them.
Weft resolves records by their authored `name`. Accept the named example schema
before publishing data for these queries; upgrading Ashlar does not alter an
older accepted schema that omitted names. Hyphenated field names need SQL quotes,
as shown above.

To execute a STRING projection on an existing publication, use the saved
configuration produced by `configure-truss-resume` and select the installed
Weft compiler explicitly:

```sh
ashlar query-truss-graph \
  --configuration /absolute/private/resume.json \
  --weft-index /absolute/weft/index.json \
  --weft-installation /absolute/weft/installation \
  --sql 'SELECT n."note-text" AS text FROM Note n' \
  --revision 1 --document-id your-original-document-id \
  --maximum-rows 1000 \
  --output /absolute/private/query-result.json
```

Keep the source server running with the same compatible Truss, Spark and Delta
dependencies used to publish. The command restores the original applied state,
runs the compiler's scalar checks and unchanged SQL under the complete publication
hold, then writes a fresh private result after cleanup. The report includes the
original compiler request and response. It does not publish or acknowledge data.
Exceeding the row bound refuses the complete answer rather than truncating it.

Use `--sql 'SELECT COUNT(*) AS n FROM Note n'` with the same command to count all
records of the selected type. The result is an exact nonnegative integer, admitted
from the compiler's aggregate IR rather than inferred from SQL text. This route
supports one unfiltered global count. To count records grouped by required STRING
fields, project each grouping field once and add one `COUNT(*)`:

```sh
--sql 'SELECT b."B-value" AS value, COUNT(*) AS n FROM B b GROUP BY b."B-value"'
```

Select `--document-id cohort` for this example. Grouped counts return an unordered
row bag checked against the selected publication's complete source oracle. Both
global and grouped counts are exact nonnegative integers. Filters, general joins, relationship counts and `COUNT(DISTINCT ...)` need
separate query support; the equality join-count profile below is admitted separately.

For the accepted `cohort.umf.json` graph, use the same command with
`--document-id cohort` and:

```sh
--sql 'SELECT a."A-label" AS label, RELATED_KEYS(a."A-B", 2) AS related FROM A a'
```

To select source records connected to a particular target key:

```sh
--sql "SELECT a.\"A-label\" AS label FROM A a WHERE HAS_RELATED(a.\"A-B\", KEY('Value-B', 'Beta'))"
```

The key uses the target's declared order: `B-value`, then `B-label`. Each matching
source record appears once, even when more than one edge matches. This profile
supports one forward or explicitly authored inverse `HAS_RELATED` predicate with literal required STRING key
components and required STRING source projections, without outer ordering.
Scalar and relationship guards run before the original compiled query, and its
complete row bag is checked against the selected publication's source oracle.
Inverse predicates swap endpoint keys while preserving canonical edge direction
and select each matching source once. For the fresh inverse cohort, use:

```sql
SELECT b."B-label" AS label FROM B b
WHERE HAS_RELATED(b."B-A", KEY('Value-A', 'Alpha'))
```

The [local inverse filter receipt](../../docs/helix/04-build/evidence/truss-inverse-filter-20261013/result.json)
returns Beta from the existing publication, without ingest or checkpoint advancement.
This profile does not provide an edge-occurrence join.

The related result contains `items` and `truncated`. Each edge contributes its
target's declared key tuple (`B-value`, then `B-label`); equal tuples remain
repeated. Tuples are ordered by their UTF-8 field values, then numeric edge ID,
and the bound selects a prefix. Both scalar and relationship integrity checks
run before the query. Use an authored forward name or explicitly declared inverse
name on an unfiltered, unjoined source without outer `ORDER BY`. Inverse reads
swap endpoint keys while preserving the canonical physical edge direction and
edge occurrences. The host verifies the complete result bag and truncation
against the original source oracle. The [small local inverse workflow](../../docs/helix/04-build/evidence/truss-inverse-query-20261013/result.json)
executes the original Weft query against a published two-record, one-edge graph.

For a fresh installation with an authored inverse, select
`examples/truss-native/cohort-inverse.umf.json` as setup `document_path`, with
`document_id: "cohort"` and your fresh revision/request key. It preserves the
cohort's fields and forward relationship and declares inverse name `B-A`:

```sql
SELECT RELATED_KEYS(b."B-A", 2) AS related FROM B b
```

This is a separate authored setup choice; it does not upgrade an existing catalog.
Its [local Truss acceptance receipt](../../docs/helix/04-build/evidence/truss-inverse-acceptance-20261013/result.json)
checks schema acceptance only; the separate inverse workflow above publishes and queries data.

A required STRING equality join can also project fields from both occurrences:

```sql
SELECT b."B-label" AS left_label, c."B-value" AS right_value
FROM B b JOIN B c ON b."B-value" = c."B-value"
```

This preserves the complete matching-pair bag, with up to 64 projected fields
and 10,000 matched pairs. It supports no filters, grouping, ordering or limits;
it does not traverse relationship edges. The
[local projection receipt](../../docs/helix/04-build/evidence/truss-join-projection-20261013/result.json)
returns `Beta` and `Value-B` from the existing publication without advancing the source.

Required STRING equality joins can produce a single global count:

```sql
SELECT COUNT(*) AS n FROM B b JOIN B c ON b."B-value" = c."B-value"
```

This profile counts matching record pairs, retaining duplicate-value multiplicity;
it does not follow relationship edges. It supports one INNER equality join,
without filters, grouping, ordering or limits. Original compiler SQL and scalar
integrity guards run under the same complete publication pins. The [local join-count receipt](../../docs/helix/04-build/evidence/truss-join-count-20261013/result.json)
returns `1` for the original single B record, without ingest or source advancement.

## Repeat bounded streaming batches

After a successful `continue-truss-graph` run, select its exact last completed
intent for the next batch without copying request digests or page paths:

```sh
ashlar configure-truss-continuation --configuration /absolute/private/reader.json \
  --continuation-result /absolute/private/continued.json \
  --output /absolute/private/next-reader.json
ashlar continue-truss-graph --configuration /absolute/private/next-reader.json \
  --output /absolute/private/next-continued.json --maximum-pages 8 --wait-seconds 60
```

An empty result preserves the original selection. Connection URI, selected JARs
and archived catalogs remain unchanged. Preparation grants no source authority;
the next run revalidates the actual committed head and current source checkpoint.
Truss ACKs remain caller-asserted downstream application. Interrupted runs use
their saved original resume intent instead of this completed-result command.

## Query an example after restart

Keep the existing example data and publication. In one terminal, start only its
owned server and capture its actual current connection details privately:

```sh
ashlar serve-truss-local --data-directory /absolute/private/example/data \
  --connection-output /absolute/private/live-connection.json
```

While that server remains running, prepare a fresh ordinary reader in another
terminal; the file must be outside the original example directory:

```sh
ashlar configure-truss-example-reader --example-directory /absolute/private/example \
  --connection-configuration /absolute/private/live-connection.json \
  --output /absolute/private/restarted-reader.json
ashlar query-truss-graph --configuration /absolute/private/restarted-reader.json \
  --output /absolute/private/restarted-query.json \
  --weft-index /absolute/path/distributions/index.json \
  --weft-installation /absolute/path/weft-installation \
  --revision 1 --document-id cohort --maximum-rows 2 \
  --sql 'SELECT b."B-label" AS label, RELATED_KEYS(b."B-A", 2) AS related FROM B b'
```

Preparation preserves the original publication/page selectors and grants no
source authority. The actual query rechecks the live source and complete original
publication pins. Stop the owned server with Ctrl-C. Connection files contain
private runtime details; use a fresh file on each restart. The
[local restart receipt](../../docs/helix/04-build/evidence/truss-example-restart-20261013/result.json)
reuses the original publication and leaves the source checkpoint unchanged.

## Accept into an existing installation

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

### Bootstrap a graph schema locally

`setup-truss-local` accepts the selected original document and grants its declared
modules to the returned local reader and writer roles. Select
`cohort.umf.json` from this directory with `document_id: "cohort"` to use the
record/edge mutation commands above, or keep `note.umf.json` for the Note workflow.
The cohort file derives from the UMF 0.7.0 acceptance example with authored record
names added for Weft queries; the original accepted evidence remains unchanged.
Configure the existing Bun executable, origin, fresh data
directory and report as in local setup; source preparation remains responsible
for whether the selected document is supported.

After setup, run `serve-truss-local` with that directory, use the returned ordinary
consumer and reader roles, then run `mutate-truss-graph` and
`publish-truss-graph` with a distinct fresh feed consumer. Schema selection alone
does not broaden the publisher's supported scalar record/edge semantics.

Set optional `wait_seconds` to an integer from 0 to 60 in publication
configuration to wait for additional committed transactions after genesis. The
command polls the ordinary public feed and publishes new arrivals using the
same destination and publication chain. This is one total wait budget, with at
most eight pages; the default zero keeps immediate finite execution. Empty
intervals are never published or acknowledged.

Catalog-only native transactions retain their complete source page and advance
publication progress even when they produce no graph mutations. Their empty
effect plan is journaled under the same writer policy and complete snapshot
checks as a nonempty plan. Recovery requires that original journal entry; it
does not insert a dummy Delta write or skip the transaction before source ACK.

Native history STRING values use the exact closed carrier
`{"kind":"string","text":"..."}` for declared and retained properties. The
adapter projects that text into canonical property JSON while retaining the
original tagged lifecycle bytes and complete source page. It refuses plain
untagged values, extra carrier members and unsupported value families.

For graph publication after schema evolution, retain the private outputs of
`accept-truss-schema` or `evolve-truss-note`. The evolution output captures both
original catalogs before and after acceptance. Select them in publication
configuration with `"archived_acceptance_files": [{"path": "/absolute/private/evolution.json", "sha256": "<SHA256 of the original file>"}]`.
The publisher matches each capture to the feed page's exact original accepted
report; it never interprets an older revision using the current catalog.
These files supply schema interpretation. Live Truss sessions still establish
source access and perform delivery ACKs.

Set `"export_graph": true` in `publish-truss-graph` configuration to capture the
final canonical graph release while the original source owner and complete
publication snapshots remain held. After successful cleanup, the command writes
`graph-release.json`, a separately bound `graph-custody.json`, and both digests
in `result.json`. The existing graph materialization workflow can select this
pair using its explicit release and custody digests. The release preserves typed
node/edge identities and original canonical properties. It carries the local
Truss publication's qualification; exporting it does not establish protected
outbox custody or qualify an external graph engine.

With a compatible Truss SDK exposing `reread_feed_in_transaction`, source
admission can reproduce an original retained delivery after its consumer has
advanced. Truss checks current permissions, the original registration and exact
page bytes; Ashlar refuses an unavailable or changed reproduction. ACK
reconciliation still reads the current consumer checkpoint and requires the
original ACK attempt. Retained-page admission alone does not authorize another
ACK or establish complete destination restart recovery.

The publisher saves each complete original feed page to a private
`source-page-N.json` before submitting that transaction's destination effects.
Successful result entries include its path and SHA256. Recovery tooling can
restore the exact public page carrier and request retained reproduction from
Truss. The archive supplies original input, not source permission or proof of
ACK; destination recovery still requires its original operation journal.

Hosts reopening a Truss graph publication use
`NativeDriver.from_existing_truss` to require the original journal, then call
`restore_applied_truss_publication(driver, request_digest)` inside the driver's
writer interval. The restoration uses the retained applied artifact and original
SQL plan, independently reconstructs expected rows from source history, and
checks live source access and native snapshots. It performs no SQL writes or
ACK. Missing applied artifacts remain a separate interruption-recovery path.

For an interruption before the applied artifact is retained, call
`recover_interrupted_truss_apply(driver, request_digest)` inside the reopened
writer interval. New Truss source plans retain the original publication intent
before effects begin. Recovery requires that intent and the original whole
operation plan, rebuilds expected rows from source changes, and dispatches the
existing effect-recovery path. It does not publish a manifest or ACK the source.
Older interrupted plans lacking that intent require separate handling; an
applied older artifact can still use the applied-restoration path above.

`resume_truss_publication(driver, request_digest)` completes an original durable
publication whose source checkpoint is still pending. It checks the existing
attempt phases, restores the original applied state or interrupted effects, and
uses the regular coordinator to publish and ACK. The coordinator owns its writer
interval, so call this function outside an already-held writer. An advanced
checkpoint requires the exact original durable ACK attempt and committed native
publication. The function verifies those records and reconciles the checkpoint
without submitting another ACK. Missing intent or a different checkpoint refuses
before recovery effects.

Truss ACK intent is retained in the original journal before the public ACK call,
binding the request, manifest, complete snapshot vector and source registration.
A restarted owner can observe the original next checkpoint only after validating
that intent and the committed publication. This is caller-asserted delivery
reconciliation; it does not turn Truss delivery into a protected outbox receipt.

## Continue and resume publications

### Export an existing publication for graph tools

Select a committed publication through its saved intent and resume configuration:

```sh
ashlar export-truss-graph \
  --configuration /absolute/private/resume.json \
  --output /absolute/private/export-result.json
```

The command writes `export-result.json-release.json` and
`export-result.json-custody.json`, then the result file with their paths, SHA256
digests, publication identity and node/edge counts. All three files must be fresh.
Capture checks the full original publication vector and complete source-derived
graph, then closes resources before writing private files. It consumes no new
feed page and performs no ACK or publication.

Use the reported paths and digests with `materialize-graph-export` to create
separate graph-tool tables, or `prepare-fabric-notebook` to prepare a local import
notebook. Those commands retain their own configuration and execution requirements.
The pair describes an immutable local capture; it grants no future source or
engine authority. A partial file-write failure leaves its original files for
inspection and withholds the final result report.

Run a small GraphFrames traversal directly over this pair with
`run-graphframes-export`. In a fresh Python environment containing Ashlar,
PySpark 3.5.3 and `graphframes-py` 0.12.3, generate the configuration from your
selected export report and existing runtime paths:

```sh
ashlar configure-graphframes-export \
  --export-report /absolute/private/export-result.json \
  --export-report-sha256 <SHA256-of-selected-export-result.json> \
  --jar /absolute/jars/io.graphframes_graphframes-spark3_2.12-0.12.3.jar \
  --jar /absolute/jars/io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar \
  --java-home /absolute/canonical/jdk-home \
  --output /absolute/private/graphframes.json
```

This checks the selected report and immutable export pair, then writes a fresh
private configuration. It starts no engine and installs no dependencies. The
report digest selects the exact report you intend to use. You can also author the
same configuration explicitly with the export report's exact paths and digests:

```json
{
  "release_path": "/absolute/private/export-result.json-release.json",
  "custody_path": "/absolute/private/export-result.json-custody.json",
  "release_sha256": "<release digest from export report>",
  "custody_sha256": "<custody digest from export report>",
  "jars": [
    "/absolute/jars/io.graphframes_graphframes-spark3_2.12-0.12.3.jar",
    "/absolute/jars/io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar"
  ],
  "java_home": "/absolute/canonical/jdk-home"
}
```

```sh
ashlar run-graphframes-export \
  --configuration /absolute/private/graphframes.json \
  --output /absolute/private/graphframes-result.json
```

This finite local profile owns `local[1]` with 512 MiB driver memory and permits
at most 10,000 nodes and edges each. It checks canonical carrier values, one-hop
edge occurrences and out-degrees against the original export, including zero
out-degrees. Properties remain exact JSON text; numeric property queries and the
full authored engine corpus require their separate adapters. Results appear only
after the Spark and file owners close. No canonical Delta tables or source
checkpoints are changed.

Add `--maximum-hops 2` to include directed two-hop paths. Each path retains its
first edge, middle vertex and second edge identities; repeated vertices, cycles
and repeated use of an edge are allowed. The result reports `path_count` and
`distinct_destinations` separately for every source vertex. A complete witness
above 10,000 paths refuses rather than returning a truncated answer. These
counts are checked against the original graph independently of native results.

### Compare immutable graph exports

Generate a reusable selection directly from each saved export result:

```sh
ashlar configure-graph-export \
  --export-report /absolute/path/r2-export-result.json \
  --export-report-sha256 REPLACE_WITH_RESULT_SHA256 \
  --output /absolute/path/r2-selection.json
```

The result digest identifies the exact report you selected. This command checks
its original release/custody pair and needs no Java, JARs, source connection or
engine. Reuse its selection for DuckDB queries and export comparisons.

Use `compare-graph-exports` to inspect changes between two saved exports without
starting Spark. Each configuration contains exactly `release_path`, `custody_path`,
`release_sha256` and `custody_sha256`, taken from its export result. Paths must be
absolute and digests must identify the original files.

```sh
ashlar compare-graph-exports \
  --first-configuration /absolute/path/r1-selection.json \
  --second-configuration /absolute/path/r2-selection.json \
  --output /absolute/path/comparison.json
```

The private report lists added, removed and changed nodes and edges by graph ID,
including before/after rows and changed columns. Property carriers are compared
as exact strings; this command does not interpret property values or establish
ancestry between the exports. It reads both complete exports before creating a
fresh report and leaves the source and its checkpoint untouched.

### Query a saved graph with DuckDB

Install the optional dependency using Python 3.10 or newer:

```sh
python3 -m pip install '.[duckdb]'
ashlar query-duckdb-export \
  --configuration /absolute/path/r2-selection.json \
  --maximum-hops 2 \
  --output /absolute/path/new-duckdb-result.json
```

Use the same exact four-field selection as `compare-graph-exports`. The command
runs in memory with one thread and a 64 MiB DuckDB memory limit. The private report
contains complete original node and edge carriers, directed one-hop edge results,
out-degrees including isolates, and optional two-hop paths with path and distinct
destination counts per node. Parallel edges remain distinct occurrences; property
carriers remain opaque strings. Each input and result bag is limited to 10,000
rows, including two-hop paths. Exceeding a bound refuses the complete result.

This is a relational topology query over one immutable saved release. It creates
no source transactions and does not establish Weft, native Delta, Fabric or
PuppyGraph query support. The command checks the complete carriers and query bags
before closing its owners and creating a fresh report.

### Publish a saved page before its first preparation

If continuation retained its source-page archive and publication intent but failed
before retaining any source plan or attempt phase, select that exact intent with
`publish-saved-truss-graph`. Supply the committed predecessor configuration and
the original intent path and digest from the interruption report:

```sh
ashlar publish-saved-truss-graph \
  --configuration /absolute/private/predecessor-resume.json \
  --publication-intent /absolute/publication/publication-intent-<digest>.json \
  --publication-intent-sha256 <original-intent-file-SHA256> \
  --output /absolute/private/saved-publication-result.json
```

The command checks the original committed predecessor, unchanged current graph
versions, current source cursor and exact archived page before retaining its first
plan. It preserves the saved page, request and publication identity. A private
planning companion retains the first timestamp for retry. Publication completes
before source acknowledgement. If plans were retained before any phase, retry uses their exact original bytes
and planning companion. Once an attempt phase exists, use `resume-truss-graph`.
Neither path replaces an existing plan. A missing installation journal, missing
planning companion for retained plans, or changed predecessor refuses.

### Consume new feed pages into existing tables

After a publication commits, use its saved intent and original publisher
configuration with `configure-truss-resume`. Then consume new transactions into
the same tables and journal:

```sh
ashlar continue-truss-graph \
  --configuration /absolute/private/resume.json \
  --maximum-pages 1 --wait-seconds 0 \
  --output /absolute/private/continued.json
```

The source checkpoint must equal the selected publication's original next
checkpoint. New pages must retain the same installation, epoch and registration,
and follow it contiguously. Each page gets a fresh saved intent before effects;
the normal publication coordinator acknowledges it only after durable publication.
Use the newest intent for the next continuation. A pending attempt requires
`resume-truss-graph` with that attempt's saved intent before continuing.
This command creates no replacement tables, journal or source registration.

If continuation fails after saving a new intent, inspect the requested output
file for the exact `publication_intent.path` and digest. Use that path with
`configure-truss-resume`, then resume the original attempt. The interruption
report does not confirm rollback or publication; recovery checks the retained
source page, journal and native outcome. The command still exits with an error.
If no report could be written, retain the destination and original saved files
for inspection rather than starting a replacement publication.

### Configure and resume an interrupted publication

The publisher writes `publication-intent-N.json` with the original request digest,
feed and source-page path/digest before entering the publication coordinator.
Select that saved file by its SHA256 with the original publication directory and
source login. The command reads its feed, request digest and original page
path/digest, so these fields need not be copied by hand:

To generate the configuration from your original publisher configuration:

```sh
ashlar configure-truss-resume \
  --configuration /absolute/private/publish.json \
  --publication-intent /absolute/private/publication/publication-intent-2.json \
  --output /absolute/private/resume.json
```

This local setup reads the selected files and writes a fresh private configuration.
It retains the source login, installed JAR paths and any archived catalog
selections, omitting administrator credentials. It does not start Spark or contact
Truss. Inspect the generated selections before running the resume command below.
Alternatively, author the same configuration explicitly:

```json
{
  "connection_uri": "postgresql://your-user:YOUR-PASSWORD@localhost:5432/your-database",
  "publication_directory": "/absolute/private/publication",
  "publication_intent": {
    "path": "/absolute/private/publication/publication-intent-2.json",
    "sha256": "<selected original intent file SHA256>"
  },
  "jars": ["/absolute/path/delta-spark.jar", "/absolute/path/delta-storage.jar"]
}
```

The existing explicit selection form also works. Use either form, without mixing
intent selection and copied fields:

```json
{
  "connection_uri": "postgresql://your-user:YOUR-PASSWORD@localhost:5432/your-database",
  "feed": "your-original-feed",
  "source_page_path": "/absolute/private/publication/source-page-2.json",
  "source_page_sha256": "<original page archive SHA256>",
  "publication_directory": "/absolute/private/publication",
  "request_digest": "<original request digest>",
  "jars": ["/absolute/path/delta-spark.jar", "/absolute/path/delta-storage.jar"]
}
```

```sh
ashlar resume-truss-graph \
  --configuration /absolute/private/resume.json \
  --output /absolute/private/resumed.json
```

Keep the source server running and select the same compatible Truss, Spark and
Delta dependencies as publication. Optional `archived_acceptance_files` uses the
same path/digest selections as graph publication. The command opens the existing
journal and tables, restores the original attempt, and completes publication or
ACK reconciliation. It writes the fresh report after resource cleanup. It creates
no replacement journal, consumer registration, schema or source dataset.

For a graph publication stopped at `prepared`, resume requires its original
source and effect plans, the unchanged four-table prior vector, and a source
checkpoint still at the original prior position. It restores the saved SQL,
timestamp and publication identity before the coordinator starts applying.
Changed tables or missing original plans require inspection; recovery does not
generate a replacement plan from the current source state.

### Read one published object or edge

Use the same explicit publication configuration as `query-truss-graph` or
`export-truss-graph`. Select native Truss IDs from the accepted catalog and
public source receipts, rather than logical labels. No Weft installation or
external graph engine is needed.

```sh
ashlar read-truss-singleton \
  --configuration /absolute/private/publication-selection.json \
  --kind object --type-id 2 --entity-id 4 \
  --output /absolute/private/object-read.json
```

For an edge, use `--kind edge` and its relationship type and edge IDs. The
command performs an exact-version Delta lookup by hash and complete typed
identity under the selected publication's full pin vector. The private JSON
report contains the complete row, or `null` for an absent identity, after source,
native and resource-closing checks. Integer fields and `published_at` retain
exact text carriers; `published_at` is epoch microseconds. An older publication
can return a subsequently deleted entity while a later publication returns
absence. The command uses the existing local Truss authority profile and does
not publish or acknowledge data. Local Spark startup is included in command
duration; this workflow makes no production latency claim.

### Add a field to an existing record

`evolve-truss-record` accepts an explicit compatible UMF revision through Truss,
then sets one previously absent scalar field on a selected existing object.
Legacy `field_value` strings retain STRING behavior. To set an integer, decimal
or boolean, use an exact tag such as `{"kind":"decimal","text":"1.2300"}` or
`{"kind":"boolean","value":true}`. The new declaration must match that family;
null tags and mismatched declarations refuse. Truss owns schema compatibility.
Truss owns compatibility and field validation. The command preserves original
values and native identity, reads back both commits, and replays the original
committed update receipt. The existing `evolve-truss-note` command remains
available for the Note example.

For the cohort, [cohort-r2.umf.json](cohort-r2.umf.json) adds optional `B-tag`.
Use a private configuration with exactly these fields:

```json
{
  "connection_uri": "postgresql://ordinary-consumer@127.0.0.1:REPLACE_PORT/truss",
  "bun": "/absolute/path/to/bun",
  "document_path": "/absolute/path/to/cohort-r2.umf.json",
  "document_revision": "r2",
  "origin_path": "/absolute/private/origin.json",
  "accept_request_key": "cohort-add-tag",
  "update_request_key": "beta-set-tag",
  "object_id": 2,
  "document_id": "cohort",
  "module_id": "m",
  "record_id": "B",
  "field_name": "B-tag",
  "field_value": "reviewed"
}
```

Select your actual object ID and authorized ordinary connection. Run
`ashlar evolve-truss-record --configuration /absolute/private/evolution.json
--output /absolute/private/evolution-report.json`. Schema acceptance and the
object update are separate source commits; retain original request keys and
inspect source state if a later step fails. The report retains exact previous
and revised catalogs and acceptance reports. Supply those captures through the
existing publication configuration's `archived_acceptance_files` and use
`continue-truss-graph --maximum-pages 2 --maximum-page-bytes 2097152` to publish
both contiguous feed pages. The byte option is explicit and bounded at 2 MiB;
the existing default remains 1 MiB. A complete transaction must also fit the
retained page archive's original carrier bounds before publication begins.
To accept the revision **and** publish both feed pages with one command:

```sh
ashlar evolve-truss-graph --configuration /absolute/private/evolution.json \
  --reader-configuration /absolute/private/current-reader.json \
  --output /absolute/private/fresh-evolution
```

Both configurations must select the same ordinary source connection. The command
retains the exact catalog archive automatically, consumes two pages into the
existing publication and writes `reader-configuration.json` selecting the final
publication. It retains `evolution.json` and `continuation.json` for inspection
and recovery. Source commits survive a subsequent publication failure; resume
the retained original attempt before continuing, rather than repeating acceptance
with new request keys. This command has basic composition coverage; it does not
qualify a new native run or evolved-schema Weft queries.

For the inverse schema accepted by `run-truss-example`, use
[cohort-inverse-r2.umf.json](cohort-inverse-r2.umf.json), which adds `B-tag` while
preserving the authored inverse relationship. Use the actual B identity from
that example and its restarted server's current connection URI. A B record
deleted by `exercise_mutations` cannot be selected for this update.

The source command itself does not publish or ACK. Use a singleton read of the
new publication to inspect the field; optional-field Weft decoding needs its
own supported query profile.

The additive [typed-record R2 model](typed-record-r2.umf.json) adds optional
`verified` without changing existing fields or keys. Select that model with
`document_revision: "r2"`, `field_name: "verified"` and
`field_value: {"kind":"boolean","value":true}` in the evolution configuration.
Use the actual source object ID from your creation result. `evolve-truss-graph`
composes schema acceptance, the new-field update and both feed publications,
retaining the previous catalog archive for subsequent reads. The scalar extension
has focused adapter coverage. The [one-record Boolean evolution check](../../docs/helix/04-build/evidence/truss-typed-record-evolution-delta-20261013/result.json)
qualifies optional Boolean addition, both local Delta publications and exact
readback with prior numeric tokens preserved. Integer/decimal additions, Unity
Catalog and mixed-revision Weft queries require separate qualification.

### Create a typed source record

Accept [typed-record.umf.json](typed-record.umf.json) with the local setup or
schema acceptance command, then use the actual ordinary source connection:

```json
{
  "connection_uri": "<ordinary authorized Truss connection>",
  "bun": "/absolute/path/to/bun",
  "document_path": "/absolute/path/to/typed-record.umf.json",
  "document_revision": "r1",
  "document_id": "typed-record",
  "module_id": "m",
  "record_id": "TypedRecord",
  "origin_path": "/absolute/private/origin.json",
  "request_key": "create-typed-record-1",
  "values": {
    "record-id": {"kind": "string", "text": "R1"},
    "quantity": {"kind": "integer", "text": "9007199254740993"},
    "amount": {"kind": "decimal", "text": "1.2300"},
    "active": {"kind": "boolean", "value": true}
  }
}
```

```sh
ashlar create-truss-record --configuration /absolute/private/create-record.json \
  --output /absolute/private/created-record.json
```

Truss validates the selected fields against its accepted schema. Omitted `note`
and `tag` remain absent. The report retains the
allocated object identity, committed readback and receipt replay. The command
creates source data only. Use the graph publisher or continuation to ingest its
complete feed page into Ashlar, then `read-truss-record` with the actual type and
object IDs. If a commit outcome is unknown, retain the original request key and
inspect source state before another write. This command has focused adapter
coverage. The [one-record source check](../../docs/helix/04-build/evidence/truss-typed-record-source-20261013/result.json)
qualifies local public schema acceptance and exact typed creation/readback/replay
with the recorded SDK. Native feed, Delta and query execution remain separate.

### Update a typed source record

Use the same connection, document selection, Bun and origin settings as creation.
Replace `values` with `set` and `unset`, and supply the actual `object_id` and
`expected_version` from the committed source object:

```json
{
  "object_id": 1,
  "expected_version": 1,
  "request_key": "update-typed-record-1",
  "set": {
    "quantity": {"kind": "integer", "text": "9007199254740994"},
    "amount": {"kind": "decimal", "text": "2.3400"},
    "active": {"kind": "boolean", "value": false},
    "note": {"kind": "string", "text": "updated"}
  },
  "unset": []
}
```

This fragment supplements the common settings; the complete configuration must
include all of them. Set and unset fields must be disjoint. Truss validates
availability and scalar meanings. Unsetting an optional field makes it absent.

```sh
ashlar update-truss-record --configuration /absolute/private/update-record.json \
  --output /absolute/private/updated-record.json
```

The command checks the selected object version, submits one original public
update group, and reads back its complete committed values and receipt replay.
A stale expected version refuses; the command never refreshes it automatically.
An exact no-op may retain the same version, as defined by Truss.
Use graph continuation to publish the resulting complete feed transaction, then
read the new publication with `read-truss-record`. The source update itself does
not publish or ACK. Retain the original request key and inspect source state if
commit outcome is uncertain. This adapter has basic unit coverage; native
update/publication qualification is separate. The [one-record local check](../../docs/helix/04-build/evidence/truss-typed-record-update-delta-20261013/result.json)
qualifies the recorded source update, committed replay, Delta continuation and
exact singleton read with the selected versions. It does not qualify Unity
Catalog or numeric Weft queries.

### Delete a typed source record

Use the common connection, document selection, Bun and origin settings from
creation, with `object_id`, `expected_version` and a fresh `request_key`. Omit
`values`, `set` and `unset`. The source ID and version come from the committed
source readback; they are not logical key values.

```sh
ashlar delete-truss-record --configuration /absolute/private/delete-record.json \
  --output /absolute/private/deleted-record.json
```

The command submits one public Truss deletion with the explicit expected version,
checks that the source object is absent after commit, and replays the original
committed receipt. Truss owns relationship and lifecycle constraints; a referenced
object may refuse deletion. Ashlar does not remove related objects automatically.
Use graph continuation to publish the deletion. The source command itself does
not ingest or ACK. Retain the original request key and inspect source state if
commit outcome is uncertain. Basic adapter coverage does not imply native
publication qualification. The [one-record local check](../../docs/helix/04-build/evidence/truss-typed-record-delete-delta-20261013/result.json)
qualifies the recorded source deletion, committed replay, Delta continuation and
pinned singleton absence. Unity Catalog and external graph engines require
separate qualification.

### Ingest exact scalar fields

The graph publisher and continuation support accepted UMF 0.7 single-valued
STRING, integer, decimal and boolean fields with `required` or `absent-allowed`
availability. Unknown labels such as UMF 0.7 `nullable` refuse admission; no native
nullability interpretation is inferred.
Integer and decimal source tokens stay exact in `props_json`; Ashlar does not
convert them through floating point. Required STRING keys remain the admitted
key profile. The source's actual field IDs and definition pins determine each
value's interpretation; mismatched families and unsupported nested carriers refuse.

Use `read-truss-record` for declared names and presence states. Its numeric values
use explicit `integer_token` or `decimal_token` objects to retain the original
lexical token. The full physical row remains in the report. Existing STRING Weft
bindings remain separately scoped; typed ingestion does not imply numeric query
support in that compiler profile. This extension has focused adapter coverage,
with a [one-record native Delta continuation/read](../../docs/helix/04-build/evidence/truss-typed-record-delta-20261013/result.json)
qualifying the recorded local Spark/Delta versions. Unity Catalog and typed Weft
query execution remain separate.

### Prepare a typed Weft binding

`ashlar_host.truss_weft_query.truss_scalar_compile_request` binds the original
accepted model and property allocations to a complete publication vector. It
preserves declared facets; the STRING-only binding API remains separate.
The [compiler probe](../../docs/helix/04-build/evidence/truss-typed-weft-binding-20261013/result.json)
records refusals for the facetless typed example, including COUNT. No query
execution is qualified for this typed compiler path.

[typed-query-record.umf.json](typed-query-record.umf.json) is a separate model
with signed 64-bit integer and decimal precision 18 / scale 4 declarations.
Public UMF preparation validates it, but the tested Truss catalog profile
refuses its acceptance with `unsupported_profile`. Upstream facet support
is required; this model is not a runnable source example.

### Read an evolved record by field name

Use the reader configuration saved by `evolve-truss-graph` with the selected
record's actual native type and object IDs:

```sh
ashlar read-truss-record --configuration /absolute/private/fresh-evolution/reader-configuration.json \
  --type-id <actual-type-id> --entity-id <actual-object-id> \
  --output /absolute/private/record.json
```

The report retains the full original row and adds ordered field observations
from that row's admitted catalog revision: declared module/field names, allocated
property ID, scalar family, nullability and `absent`, `null` or `value` state.
A missing record remains explicit absence. Unknown executable properties refuse;
retained content stays in the unchanged raw carrier. The selected Truss scalar admission determines the permitted families and
nullability; the reader does not broaden that admission.
The named-field adapter has basic coverage and makes no new native qualification.

### Stream a selected target update and deletion

Use `update-delete-truss-target` when the source A must remain connected to other
B targets. The cohort profile updates the selected B's `B-label`, retaining its
native identity and other values, then deletes its selected A-B edge before
that B object in one complete group. `B-label` is part of the logical key;
Truss validates the new key and uniqueness under current permissions.

Select existing identities from public source reads in a private configuration:

```json
{
  "connection_uri": "<ordinary authorized Truss connection>",
  "bun": "/absolute/bun",
  "document_path": "/absolute/cohort.umf.json",
  "document_revision": "r1",
  "origin_path": "/absolute/private/origin.json",
  "object_id": 4,
  "edge_id": 5,
  "replacement_label": "Gamma-edited",
  "update_request_key": "<fresh selected update request key>",
  "delete_request_key": "<fresh selected deletion request key>"
}
```

Replace the IDs with your selected current B and incoming A-B edge; they must
match. The origin file is a map of string keys to string values. Run:

```sh
ashlar update-delete-truss-target \
  --configuration /absolute/private/target-lifecycle.json \
  --output /absolute/private/target-lifecycle-result.json
```

The command uses actual current definition pins and expected versions, performs
source readback and original committed receipt replay, and preserves source A.
Its two source commits are separate; a later failure does not undo an earlier
commit. Retain request keys and inspect source state before retrying. This source
operator does not consume feed pages or acknowledge them. Use
`continue-truss-graph --maximum-pages 2` with the existing publication selection
to publish the complete update and deletion pages in order. Retained older
publications keep their original object/edge values and native version vector.

## Run the query gallery in one session

Use [cohort-queries.json](cohort-queries.json) with the accepted inverse cohort
and its saved reader configuration. It selects an inverse relationship query,
a STRING equality join and a record count. Each keeps the original compiler
checks and publication pins.

```sh
ashlar query-truss-graph-batch --configuration /absolute/private/reader.json \
  --batch /absolute/path/cohort-queries.json \
  --weft-index /absolute/weft/index.json \
  --weft-installation /absolute/weft/installation \
  --output /absolute/private/fresh-query-batch.json
```

All named results use the same immutable publication in one owned session.
The combined report is written after closure; any failed query withholds it.
This batch interface has basic adapter coverage; its native workflow has not
yet been qualified. The individual gallery query shapes retain their existing
scoped native evidence.

### Prepare compatible physical-revision bindings

`ashlar_host.truss_compatible_binding.truss_compatible_compile_request` targets
Weft's `0.1.1-compatible-revisions-candidate` source registration. Supply the
actual admitted source, resolved manifest, native UUIDs, authored SQL and an
explicit `physical_revisions` tuple including the primary revision. Original
accepted models and allocations remain in the request. The builder checks
historical declarations, field pins, scalar domains and keys before adding the
compiler's exact revision-set binding.

`admit_truss_compatible_revisions` checks the emitted
`ashlar.compatibleSchemaRevisions` obligation against those accepted revisions
for an unjoined source scan. It is supplementary schema correspondence proof:
the caller must use the original built request binding and independently enforce
native publication admission, every integrity guard and the complete result bag.
The helper grants no source or result authority. This foundation has basic adapter
coverage and an [actual public CLI compilation check](../../docs/helix/04-build/evidence/compatible-revision-compiler-20261013/result.json)
on a separately authored named inert fixture. Transport and the two-record mixed R1/R2 local Delta query have
[actual native evidence](../../docs/helix/04-build/evidence/compatible-revision-native-query-20261013/result.json)
for the pinned source candidate and direct STRING subset. Existing query commands retain their qualified profiles.

### Query compatible record revisions

Use the local candidate compiler explicitly to project required or optional STRING
fields across admitted additive catalog revisions:

```sh
ashlar query-truss-compatible --configuration /absolute/private/reader.json \
  --output /absolute/private/compatible-query.json \
  --compiler /absolute/private/weft-cli \
  --sql 'SELECT b."B-label" AS label,b."B-tag" AS tag FROM B b' \
  --revision 2 --document-id cohort \
  --physical-revision 1 --physical-revision 2 --maximum-rows 100
```

The compiler executable must match the pinned local candidate identity. The query
holds the complete publication, validates historical field/key correspondence,
executes every original integrity guard, and compares the full decoded result bag
with admitted source data. This command supports direct unfiltered, unordered
single-record STRING projections; other query shapes require separate admission.
Its output retains the original request and response strings. Qualification is
scoped to the local candidate, with native execution evidence recorded separately.

### Query required scalar fields

Use `query-truss-scalars` with the same explicit reader, compiler and physical
revision selections to project required STRING, Boolean, signed64 integer and
decimal(18,4) fields. The numeric facets must be present in the original accepted
model. For `typed-query-record.umf.json`, select:

```sql
SELECT r."record-id" AS id, r.quantity AS quantity,
       r.amount AS amount, r.active AS active FROM TypedRecord r
```

Numeric results retain `{ "integer_token": "..." }` or
`{ "decimal_token": "..." }` carriers. Complete-bag comparison uses exact
mathematical values, preserving original source and compiler tokens. The command
requires direct unfiltered, unordered required-field projections and runs all
original publication and integrity checks. Optional scalar outputs, arithmetic
and aggregates need their own admission. Truss numeric source support requires
the main-source iteration after `v0.0.1`; that published release does not include it.

The [actual one-record native query](../../docs/helix/04-build/evidence/truss-faceted-scalar-native-query-20261013/result.json)
preserves `9007199254740993`, decimal `1.2300`, Boolean and STRING results with
an unchanged source checkpoint on local Spark/Delta.

### Join required numeric fields

`query-truss-scalar-join` takes the same explicit reader, compiler and revision
options as `query-truss-scalars`. It admits one equality INNER JOIN between
required signed64 fields or between required decimal(18,4) fields, with required
scalar projections. For example:

```sql
SELECT a."record-id" AS left_id, b."record-id" AS right_id,
       a.quantity AS quantity, b.amount AS amount
FROM TypedRecord a JOIN TypedRecord b ON a.quantity=b.quantity
```

Both scan occurrences retain their accepted catalog mappings and the complete
publication. Every duplicate match and self-join pair participates in the
independent source comparison. Numeric results retain exact carrier tokens.
Filters, outer joins, arithmetic and aggregates require their own admission.

The [actual two-record native joins](../../docs/helix/04-build/evidence/truss-scalar-join-native-20261013/result.json)
return four integer equality pairs and two decimal equality pairs from one
publication, with exact values and an unchanged source checkpoint.

### Project exact numeric arithmetic

Select the installed public 0.4.2 arithmetic distribution explicitly:

```sh
ashlar query-truss-arithmetic --configuration /absolute/private/reader.json \
  --output /absolute/private/arithmetic-query.json \
  --weft-index /absolute/weft/distributions/index.json \
  --weft-installation /absolute/private/arithmetic-installation \
  --sql 'SELECT r.quantity+1 AS next_quantity,r.amount*2 AS double_amount FROM TypedRecord r' \
  --revision 1 --document-id typed-query-record --maximum-rows 100
```

This command admits direct, unfiltered arithmetic projections using required
signed64 or decimal(18,4) fields, numeric literals, and `+`, `-`, `*`. Results retain
exact numeric carrier tokens. Source integrity checks precede every original
arithmetic capacity guard; the complete result bag must match exact source-derived
values. The publication holds one selected physical revision. Filters, joins,
aggregates, optional arithmetic and historical revision unions need separate
admission. Original request, response and recompilation strings are retained in
the result; native qualification is scoped by its execution evidence.

The [actual two-record arithmetic query](../../docs/helix/04-build/evidence/truss-arithmetic-native-20261013/result.json)
returns exact integer addition and decimal multiplication/subtraction results
from one publication, with an unchanged source checkpoint.

### Run the arithmetic toolkit end to end

`ashlar run-truss-arithmetic-example` creates a fresh local Truss installation,
accepts the bundled original faceted `typed-query-record.umf.json`, creates one
record, publishes its catalog and object pages to Delta, reads the named singleton,
and queries exact integer addition and decimal multiplication/subtraction on that
same publication. Use a fresh private output directory:

```json
{
  "profile": "ashlar-local-truss-arithmetic-example/0.1",
  "bun": "/absolute/path/to/bun",
  "java_home": "/absolute/path/to/jdk21",
  "jars": ["/absolute/path/to/delta-spark.jar", "/absolute/path/to/delta-storage.jar"],
  "weft_index": "/absolute/path/to/distributions/index.json",
  "weft_installation": "/absolute/private/arithmetic-installation"
}
```

```sh
ashlar run-truss-arithmetic-example --configuration /absolute/private/example.json \
  --output /absolute/private/fresh-arithmetic-example
```

Install the public 0.4.2 arithmetic distribution first. Truss must include the
signed64/decimal(18,4) source implementation on main after `v0.0.1`; that release
does not contain it. Use Spark 4.0.1, Delta 4.0.0, the compatible PostgreSQL runtime,
Bun and JDK21. The final bounded private `result.json` is released after source and
query runtimes close, input renewal, and environment restoration. It includes the
singleton fields, original arithmetic protocol strings, exact results and unchanged
source checkpoint. Local caller-asserted ACK and execution evidence define its
qualification.

The [installed-wheel workflow](../../docs/helix/04-build/evidence/truss-arithmetic-example-installed-20261013/result.json)
executes the documented command from fresh setup through the exact singleton
and arithmetic results on one publication.
