# End-to-end example — under construction

Run the local schema-to-source-to-state workflow first:

```sh
python3 tools/run_local_example.py
```

No Docker, cloud account or installed dependencies are needed. The command reads
original v3 UMF intake and interpretation, uses explicit fixture type/property
mappings and validates three committed JSONL transactions under the selected
string-record policy. It creates two objects, updates one and deletes the other,
then replays all transactions against the complete retained state. Expected output:
one live object at version2, four history entries, one tombstone, replay unchanged.
Unicode caption and the original large numeric token in retained_json survive.
UMF completeInterpretation remains false. Fixture mappings are development inputs,
not native Truss accepted IDs; this command issues no publication or source ACK.
The input is local-string-source.jsonl; graph-source.jsonl is a different fixture
and intentionally does not satisfy this selected UMF string-record policy.

To run actual upstream logical Record checks as part of the same workflow, use
Bun and the pushed UMF checker branch (currently unmerged). Create a clean pinned
checkout without changing your existing UMF branch:

```sh
git -C ../umf fetch origin codex/core-record-value-check
git -C ../umf worktree add --detach /tmp/ashlar-umf-record-check c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3
(cd /tmp/ashlar-umf-record-check && bun install --frozen-lockfile)
python3 tools/run_local_example.py --umf-source /tmp/ashlar-umf-record-check --umf-check-output out/local-example/umf-record-check.json
```

The runner verifies the clean producer revision, explicitly upgrades the original
0.7 schema to 0.8 through UMF, verifies its receipt, and checks all three original
create/replace records through validateCoreRecordValues before application. It
retains complete original results when --umf-check-output is supplied. Delete is
a source operation. The result adds upstream_record_checks with three complete
logical checks; original_document_complete and complete_interpretation stay false.
Exact original record/schema/request hashes tie the development result to its
inputs. Fixture IDs remain development mappings; these checks establish no native
Truss acceptance, qualified validator isolation, key/relationship dataset proof,
publication or source ACK. Original schema/intake/interpretation files remain intact.

Repeat the small actual-producer integration check with:

```sh
python3 tools/check_local_umf_example.py /tmp/ashlar-umf-record-check
```

The same pinned producer also checks the schema-evolution workflow:

```sh
python3 tools/run_schema_evolution.py --umf-source /tmp/ashlar-umf-record-check --umf-check-output-dir out/local-evolution/umf
python3 tools/check_local_umf_evolution.py /tmp/ashlar-umf-record-check
```

Each original event revision selects its own definition: two v1 creates and one
v3 replacement receive actual UMF logical checks. Before the explicitly admitted
v1→v3 fixture transition, both existing records are checked against the new v3
logical definition, tied to their original retained-history delivery IDs/hashes.
This does not rewrite their old revisions or declare automatic schema compatibility.
The workflow keeps the existing exact transition guard, retains source/check/upgrade
receipts when requested, and verifies independent retained evolution replay. History
includes revisions1/3, the surviving object uses3, and one old-revision object is
deleted. Unknown meaning never falls back to a newer definition. Native migration,
Truss acceptance, publication and source ACK remain unproved.

The isolated PostgreSQL substrate is also runnable:

```sh
python3 tools/start_truss_sandbox.py
docker exec ashlar-e2e-truss-pg17 pg_isready -U postgres -d truss_e2e
```

Requires Docker and the PostgreSQL17.9 image; the script creates a labelled
container bounded to512MiB/1CPU and binds only127.0.0.1:15432. Re-running it reuses
that container. It neither installs Truss nor advertises a working data feed.
The generated database password stays in the private container configuration;
it is not printed or committed. Keep the sandbox private.

The [implementation plan](../../docs/helix/04-build/end-to-end-plan.md) tracks the
remaining UMF/schema, Truss runtime/feed, Ashlar publication and additional-source
work. This example will gain actual user-facing commands as those slices run.
Do not use the candidate packages as a production deployment.

## Shared UMF intake

Using a clean source checkout with its dependencies installed:

```sh
bun tools/inspect_umf.ts /path/to/umf EXACT_GIT_REVISION examples/end-to-end/schema-v1.umf.json
python3 tools/check_schema_intake.py /path/to/umf EXACT_GIT_REVISION
```

The checked artifacts use UMF source16c35e8d943769ccfa7bb57d16785aa7159abe65
and Bun1.4.2. This matches the Truss source handoff's UMF baseline; a separate
newer worktree was inspected but missing dependencies, and was not used for the
successful run. schema-v1 is the exact original Truss0.11 source-review model;
schema-v2 asserts an `optional` string field (later actual semantic inspection
finds that token unknown), and schema-unknown retains an unknown
root assertion. The `.intake.json` files preserve original source bytes/digests
and UMF diagnostics. All examples are structurally valid, but UMF's experimental
warnings keep completeInterpretation false. Do not equate that flag with target
catalog acceptance or silently remove warnings to make it true.

[CONTRACT-005](../../docs/helix/02-design/contracts/CONTRACT-005-schema-intake.md)
owns this shared intake boundary. Native raw intake custody now runs on both stores; semantic binding, stable
catalog allocation and accepted schema evolution are next;
this inspection component does not yet claim to feed a running Truss engine.

## Native Truss storage checkpoint

The isolated sandbox now contains the current source-review 0.11 declarations:
46 tables, 442 columns and two catalog functions; catalog head remains revision 0.
Install into a fresh sandbox with:

```sh
python3 tools/install_truss_layout.py /path/to/truss/docs/helix/02-design/models/truss-layout-weft-review-0.11.proposal.sql 1ac7cc82405ff581072d45ad586f54d8c48343eaecc7c011195535ed359e37f9 /tmp/truss-install-receipt.json
```

The installer refuses an existing Truss namespace. It verifies the exact source
hash and runs one transaction. PostgreSQL rejects the source's generated UTF-8
conversion expression as non-immutable; this development profile substitutes
one explicitly recorded immutable UTF-8 helper after checking database encoding.
The original source remains unchanged and the derived execution has a separate
hash. The exporter also omits its final statement terminator; the installer adds
it. Both failed attempts and the successful native inventory are retained in
the spike's `out/truss-*-20261008.json` receipts. Five small UTF-8 boundary cases
match native byte conversion.

This is storage installation evidence, not full catalog semantics or a working
producer/feed. Application privileges, atomic schema acceptance and runtime
operations are still required before the end-to-end workflow can run.

## Native schema custody checkpoint

`src/ashlar/schema.py` reads the shared artifact under an explicit revision and
trusted validator source pin. It verifies source bytes/digest/identity and retains
the complete original intake artifact. The small native runner is
`tools/check_native_schema_registry.py`; it requires the Databricks SDK, profile
`aidev-cus`, Docker and the isolated sandbox. It intentionally creates fixed
private development tables once and refuses ordinary reruns; it is evidence
tooling, not the finished user deployment command. Its passed receipt is
`docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/schema_registry_20261008/summary.json`.

Three documents (v1, additive v2 and unknown-assertion variant) are retained on
PostgreSQL and UC Delta. Replay preserves exact original source/diagnostics; a
different valid document under the same revision refuses on both. The Delta
profile is one serialized development writer. No accepted schema, stable catalog
IDs, application authority or data stream is inferred from this custody check.

## Authored identity inventory

```sh
bun tools/project_umf_identities.ts /path/to/umf EXACT_GIT_REVISION examples/end-to-end/schema-v2.umf.json
```

The checked `schema-v2.identities.json` records original document/module/element
identities and source positions using the pinned UMF reader. It is separate from
a target binding: display names and document revisions cannot replace lineage.
`src/ashlar/catalog.py` provides the pure stable-ID planner for the forthcoming
complete admitted binding inventory and locked native acceptance. The planner
retains retirement history and same-identity IDs, but does not persist or issue
a catalog revision.

## Semantic inspection and binding plans

```sh
bun tools/inspect_schema_semantics.ts /path/to/umf EXACT_GIT_REVISION examples/end-to-end/schema-v3.umf.json
```

The original revision-2 caption uses optional, which the pinned UMF API interprets
as unknown; its checked binding plan blocks. The new revision-3 fixture explicitly
uses absent-allowed and has a candidate string-record binding plan. Both full
interpretation receipts are retained. Revision 2's source and native intake stay
unchanged. Schema-property inspection requires UMF core 0.8.0; its original
refusal under 0.7.0 remains in the report.

`plan_string_record_binding` in `src/ashlar/binding.py` verifies complete receipt
correspondence and prepares qualified type/property identities. It blocks unbound
assertions and reports engine enforcement as unimplemented. This is the planning
component before native acceptance, not a working mutation or streaming command.

## Stream another source through stdin

```sh
python3 tools/read_jsonl_source.py synthetic-jsonl epoch-1 < examples/end-to-end/source.jsonl
```

This runnable reader emits a complete transaction custody batch: feed/epoch,
original batch identity, byte-offset cursor strings, exact begin/event/commit
bytes in base64, ordered record digest and exact delivery identities. It verifies
explicit commit count/digest before emitting. The default limits are 1000 events
and 1MiB for the entire transaction, including control lines; stdin reads are
bounded before parsing. Truncated transactions, duplicate delivery IDs, malformed
JSON and unknown control envelopes refuse. Event payload content remains opaque.
The synthetic example includes original unknown origin content and a large
numeric token; no conversion to host float or narrowed SQL integer occurs.

This reader supplies the adapter batch boundary; it has not applied the create/
update payloads or produced an Ashlar publication. It does not acknowledge source
progress. Host file/feed/epoch custody, authorized semantic/schema admission,
durable stage replay and completed publication/checkpoint are the next pieces.
The same boundary will receive the Truss native feed and PostgreSQL outbox under
their own qualified profiles. JSONL does not claim Truss-native semantics.

## Durable native stage checkpoint

`src/ashlar/staging.py` validates complete exact source batches and persists them
through authenticated SQL plus an explicitly injected exclusive-writer policy.
Same batch replay preserves originals; changed bytes or metadata under the same
batch key refuse. A returned stage receipt grants no publication or source
acknowledgement. The development runner `tools/check_native_source_stage.py`
creates a private table once and intentionally refuses ordinary repeat setup.
It requires the existing Databricks SDK/profile/warehouse and uses local process
locking under administrative development authority. Native/remote fencing is
still required for deployment.

The actual small native receipt is
`docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/source_stage_20261008/summary.json`.
One two-event transaction stored/replayed correctly; a changed valid transaction
with the same ID refused and original full bytes were recovered independently
from Delta. Per-delivery apply/version conflicts, graph effects, immutable
publication and durable checkpoints are still next.

## Stream-to-native graph example checkpoint

The separate `graph-source.jsonl` is an explicit versioned whole-entity source
fixture; it contains two complete transactions and nine events. It does not
claim Truss-native property feed or accepted UMF schema status. The reusable
`changes_from_batch` adapter in `src/ashlar/whole_entity.py` requires its exact
profile and authored identity/version text and blocks unbound executable fields.

`tools/check_native_graph_apply.py` exercised that source on fresh private UC
current/tombstone tables. Native evidence at
`docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/graph_apply_20261008/summary.json`
verifies creates, parallel edges, an isolated node, replacement, deletes and
original exact history bytes. The fixed development setup is one-shot and
refuses ordinary rerun; its recovery option applies only to a verified empty
installation after the recorded initial DELETE failure.

These table heads remain unpublished. The experiment writes tables separately
and supplies no safe production replay/recovery, schema authority or source
acknowledgement. The next integration must make these effects durable/replayable
and publish one validated immutable version vector before users read or advance
source checkpoints.

## Durable publication attempt custody

`src/ashlar/attempt_store.py` adds durable exact-byte phase custody for the
publication coordinator. The native receipt is
`docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/attempt_store_20261008/summary.json`.
Five small UC rows replay unchanged, refuse a changed result and reload from a
fresh store/session. The fixed one-shot native checker is
`tools/check_native_attempt_store.py`; it uses administrative development
authority with local process locking, not qualified native/remote fencing.

Its result/descriptor artifacts are explicitly synthetic byte carriers. They
prove storage behavior and grant no graph publication or source acknowledgement.
Connecting real native effect/recovery and immutable manifest/checkpoint
producers remains the next integration step.

## Additional PostgreSQL transactional outbox

Fresh private source DDL is `sql/ashlar-outbox/01-postgresql.sql`; the small native
checker is `tools/check_native_outbox.py`. It installs only a separate namespace
in the isolated sandbox and uses ordinary NOLOGIN writer/reader roles to verify
append-only admitted access. The setup intentionally refuses namespace reuse.
Its native receipt is
`docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/outbox_20261008/summary.json`.

Two committed groups/seven events round-trip exactly; rollback leaves pending
position/payload invisible, replay preserves original position and byte-different
reuse refuses. `PostgresOutbox` in `src/ashlar/outbox.py` supplies bounded committed
pages with native sequence positions. Those are separate from the contained
JSONL byte offsets. The reader supplies no source acknowledgement.

This is an additional outbox source, not Truss-native capture or automatic
observation of arbitrary SQL. Producers must append in their actual application
write transaction and retain original commit recovery. Registered source identity,
retention and outer-cursor publication/checkpoint integration are still required.
The development roles have no login; application credentials/service authority
are not provisioned by this example.

The recoverable native graph fixture is implemented in
`tools/check_native_recoverable_graph.py`; it uses nine events and a private
fixed development schema on the existing warehouse. Completed native evidence
is under `out/native/recoverable_graph_20261008` in the layout spike. The retained
host SQLite journal is `/private/tmp/ashlar-recoverable-graph-20261008.sqlite`;
retain it for original-handle recovery rather than deleting it as temporary
telemetry. The completed fixture refuses another setup/parity run. It verifies
materialization and original-plan replay, not a Truss feed or read publication.

## Isolated Unity Catalog setup

The fresh `ashlar_e2e_private_20261008.runtime_clean_user` namespace contains eight
empty candidate graph/source/history/publication tables. Setup requires the
Databricks SDK (tested0.102.0), a configured authenticated CLI/SDK profile and an
existing catalog owned by that identity without unexpected inherited writers.
From the Ashlar repository, the demonstrated command is:

```sh
python3 tools/setup_native.py \
  --catalog ashlar_e2e_private_20261008 --schema runtime_clean_user \
  --profile aidev-cus --warehouse 2439e1f2e37ac563 \
  --journal /private/tmp/ashlar-clean-user-setup-20261008.sqlite \
  --output /private/tmp/ashlar-clean-user-setup-20261008
```

Run it with the Python environment containing the SDK. The demonstrated journal
is retained on this development machine; it is recovery state, not replaceable
telemetry. For a new deployment, choose a persistent private journal path and a
fresh schema, retaining the same arguments/path through retries. Setup has no
overwrite, cleanup or blind resubmission. A pending returned statement resumes
its original handle; a submission with no retained handle requires reconciliation.
It checks current actor, complete paginated effective permissions and native
managed-table type, compares exact columns with the UMF-generated model, then
renews all eight UUIDs and owner lanes before reporting readiness.
Raw permission pages are retained in effective-grants.jsonl. The fresh setup
passed nine original CREATE statements, 24 warehouse reads and 53 permission
pages; all tables reported zero files.
This command deploys empty carriers; actual UMF schema acceptance, Truss feed,
streaming publication and user reads are still required by the end-to-end goal.

The older `runtime` and CSV installations remain separate retained fixtures.
Do not reuse their journals for this schema or rewrite their historical receipts.
Pass this new installation's summary.json to register_umf.py with the pinned
UMF source and its own persistent registry journal. That new registration and
publication/query sequence remain to be demonstrated on runtime_clean_user;
the earlier native CSV sequence belongs to its original installation.

## Stream complete source batches into raw custody

After native setup, this command streams the supplied nine-event JSONL fixture:

```sh
python3 tools/stage_source.py \
  --installation docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/private_setup_20261008/summary.json \
  --input examples/end-to-end/graph-source.jsonl \
  --feed whole-entity-fixture --epoch epoch-1 \
  --journal /private/tmp/ashlar-private-stage-20261008.sqlite \
  --output docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/private_stage_20261008
```

Use the SDK-enabled Python environment and retain the same journal across
restarts. Native staging and fresh-process replay have passed. Complete original
batches (including unknown payloads) are stored before interpretation; no graph
publication or source acknowledgement follows from a staged receipt. The
optional --cursor-before describes the original offset of the input fragment,
not an instruction to seek within the file. Feed/epoch/input/cursor provenance
are trusted development-owner configuration, not automatic native source
registration. Other adapters must preserve their outer cursor independently.

## Register a UMF document in raw schema intake

The command runs the actual pinned UMF reader/validator from a clean checkout:

```sh
python3 tools/register_umf.py \
  --installation /private/tmp/ashlar-csv-stream-setup-20261008/summary.json \
  --umf-source /path/to/clean/pinned/umf \
  --validator-revision 16c35e8d943769ccfa7bb57d16785aa7159abe65 \
  --document examples/end-to-end/schema-v3.umf.json --revision 3 \
  --journal /private/tmp/ashlar-csv-stream-schema-20261008.sqlite \
  --output out/native-umf-registration
```

Requires the SDK-enabled Python environment, Bun on PATH (or --bun), and installed
UMF checkout dependencies. The clean checkout must match the supplied exact Git
revision; the owner's changing checkout is not an implicit source pin.
Use your original `setup_native.py` installation summary and an authorized
existing `--profile`/`--warehouse`. The command derives the registry namespace
from the complete UMF-generated installation receipt. It retains raw paginated
effective permissions in `effective-grants.jsonl` and checks fresh actor/owners,
a managed registry, exact UUID/columns and unchanged inspected source bytes.
The revised command has native one-row registration and fresh-process replay
evidence on this current CSV installation. Retain the journal. Historical v1 and v3 receipts remain separate evidence. The revised command
retains v3/revision3 in the CSV installation and replays without new native
mutations. Change --document, --revision and --output for another source revision;
keep that installation’s registry journal. Unknown/partial
interpretation is retained truthfully. This registers raw schema custody, not an
accepted executable schema or stable native catalog IDs; graph ingestion must
still await selected semantic admission.

The original two mutation receipts (CREATE and MERGE) remain unchanged across a
fresh-process replay. Original registration issued six warehouse reads; replay
issued five, plus retained effective-permission pages. The older runtime setup
receipt predates current generated-model provenance and is refused by the revised
command. Do not manufacture an updated receipt for that installation.
This registry's proof belongs to its own installation/workload. Keep the existing
CSV publication journal bound to its original intake proof; swapping the registry
UUID changes the workload and must refuse. A fresh complete carrier setup and
actual Truss acceptance/feed remain required for a clean end-to-end workflow.

## Apply the selected local fixture to the private Delta sandbox

The existing development installation can run this small example:

```sh
python3 tools/run_native_example.py --journal /tmp/ashlar-local-native.sqlite --output /tmp/ashlar-local-native-receipts
```

Requires the Databricks SDK, profile aidev-cus and the recorded existing private
runtime installation. This command accepts only the fixed four-event fixture and
private namespace. A fresh journal requires all four graph tables empty; rerun
using the same original journal after an interrupted or completed attempt. Keep
that journal permanently: a new journal cannot safely resume populated tables.
Do not copy the example command's new journal path over an existing populated run.
The actual completed run uses /private/tmp/ashlar-local-native-20261008.sqlite.

Actual create/update/delete and fresh-process replay passed on the existing
warehouse. Nine original mutation submissions and three plans were retained;
replay reused them. Selected final object payload/version, edge/tombstone/history
counts and renewed owner/grant/UUID observations passed. This is development
owner custody with fixture IDs; full-column effects, remote fencing, source ACK,
retained pins, immutable publication and resolver-backed read remain unfinished.


For read-only complete effect verification of the completed native fixture:

```sh
python3 tools/check_native_local_example.py
```

It checks four final table snapshots and the initial object snapshot against
independently derived complete original-event expectations. The retained evidence
includes the observed version vector; do not treat it as an active retention pin
or published manifest. The command reads existing data and does not change it.


## Evolve the selected local schema

```sh
python3 tools/run_schema_evolution.py
```

The command uses original v1 and v3 UMF intake/interpretation receipts and stable
fixture type/label IDs. It explicitly admits the additive replacement from v1 to
v3, adds caption and retains both revisions in history. Deleting the other object
uses its original v1 schema. Replay preserves complete state. Unknown revisions
and transitions without an explicit admission refuse; no latest-schema fallback
exists. Both original experimental interpretation flags remain false. This runs
locally and does not mutate the existing native example or establish Truss catalog
acceptance/publication. schema-evolution-source.jsonl is a separate source fixture.


## Run the live PostgreSQL additional-source example

```sh
python3 tools/run_outbox_example.py
# Read already-installed exact fixture groups with actual UMF logical checks:
python3 tools/run_outbox_example.py --read-only --umf-source /tmp/ashlar-umf-record-check --output-dir out/native-outbox-umf
```

The optional UMF path checks the original native-read create/replace records
against each original revision before apply. --read-only skips every append;
it requires the demonstrated groups already present at native positions 3–5.
Retained original transactions and per-revision producer results are joined by
exact payload hashes and outer PostgreSQL checkpoints. Native positions remain
separate from the contained JSONL byte cursors. These are logical checks under
explicit fixture mappings, with no Truss acceptance, publication or ACK authority.


Requires Docker, psycopg3.2.13 and the existing isolated sandbox/outbox containing
its initial two source groups. This fixed development runner appends three exact
schema-evolution transactions idempotently and requires their original native
positions3–5. It refuses a different source layout/position. Re-running preserves
positions and original bytes. The ordinary reader reconstructs the evolved graph
and proves a page resume has equal state. Complete native positions and contained
source artifacts are retained together in out/native/outbox_evolution_20261008.
No Delta write, publication or source acknowledgement is issued. The source is
an explicit PostgreSQL whole-entity outbox, not the unfinished Truss property feed.


## Stream the live outbox into the private Delta example

The completed run uses the separate runtime_outbox installation:

```sh
python3 tools/run_native_example.py --source outbox --installation docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/outbox_setup_20261008/summary.json --journal /private/tmp/ashlar-outbox-graph-20261008.sqlite --output /tmp/ashlar-outbox-delta-receipts
```

Requires the live outbox example, Databricks SDK/profile aidev-cus, psycopg and
Docker. Keep and reuse the original graph journal: a new journal refuses populated
tables and cannot substitute for original outcome custody. The separate installation
was created with setup_native.py in the existing private catalog; its original
setup journal is /private/tmp/ashlar-outbox-setup-20261008.sqlite.

Native source positions3–5, complete original transaction bytes and selected
schema evolution are retained before ordered Delta effects. Actual application
and fresh-process replay passed with nine unchanged native mutation submissions.
This writes the small graph and raw event history; it does not publish, pin,
acknowledge the source or establish accepted Truss authority. Full-column evolved
snapshot validation and publication/read composition are still unfinished.


Verify every stored column of the evolved outbox stream at exact Delta versions:

```sh
python3 tools/check_native_local_example.py --source outbox
```

Actual checks pass for the four final carriers and initial object snapshot,
including v1/v3 revision custody and independent inner cursors. The captured
version vector is observed effect evidence, not an active retention pin or
published descriptor. This is a read-only command on the existing warehouse.

### Read the streamed development object

After the outbox native example, use the host environment with Databricks SDK
and profile `aidev-cus`:

```sh
python tools/lookup_native_example.py --version 5 --id 1 --output /tmp/ashlar-point-read-new
```

The output directory must be new. The command checks the recorded development
table UUID before/after a parameterized exact-version singleton lookup and saves
original native responses. Object 1 currently carries version 2, schema revision
3, the updated label and Unicode caption. This is an unpublished diagnostic
under current Unity Catalog permissions. It does not establish manifest admission,
active publication pins, future retained-file availability or source acknowledgement.

Publication hosts can use `ashlar.manifest.manifest_pin_vector(original_row,
trusted_table_uuids, authority=...)` after independent manifest/identity admission,
and `bind_manifest_pins(descriptor, held_vector, trusted_table_uuids, authority=...)`
in their mandatory singleton policy. Supply every published table, not only the
one queried. The digest preserves original manifest JSON strings; rewriting equal
JSON does not reuse existing pins. These helpers do not authorize registration or
reads and do not qualify a retained-file or cleanup policy. NativeBackend returns
manifest timestamps as exact microsecond text for this correspondence.

### Additional source: single-line CSV

```sh
python tools/run_csv_example.py
```

This runnable local example uses `string-source.csv`, the actual v3 UMF intake
and selected string policy. Four singleton transactions create two objects,
replace one and delete the other; replay through retained staging leaves the
complete graph unchanged. Fixture IDs remain development mappings, not accepted
Truss IDs. This example does not write Delta or publish/acknowledge a source.

To include actual upstream UMF Record checks, use the clean pinned UMF checkout
and Bun setup above:

```sh
python3 tools/run_csv_example.py --umf-source /tmp/ashlar-umf-record-check --umf-check-output out/local-csv/umf-record-check.json
python3 tools/check_local_umf_csv.py /tmp/ashlar-umf-record-check
```

The runner first verifies original CSV correspondence, then checks all three
create/replace records through UMF's explicit 0.7-to-0.8 upgrade and Record
checker before applying any batch. The retained result carries the original
schema, upgrade receipt, adapted-record digest and delivery identity containing
original CSV header/row bytes. Deletes remain source operations. Empty mapped
CSV cells are present empty strings; they are not inferred absent or null.
These logical checks do not admit native catalog IDs, prove source epoch custody,
check dataset keys/relationships, publish Delta or authorize source ACK. The
original document's incomplete validation remains visible.

For another CSV producer, call `ashlar.csv_source.csv_batches(binary_lines,
feed=..., epoch=..., source_system=..., schema_revision=..., type_id=...,
properties={column_name: property_id_text})`. Supply independently admitted source
custody and mappings. Required columns are `id`, `entity_version`, `operation`;
operations are create/replace/delete of complete objects. Property values remain
strings, including empty strings; the downstream schema policy decides admission.
Each complete physical line is one explicit transaction, with an ordinal batch
ID. Its delivery ID contains the original header/row as base64 plus row ordinal,
so exact provenance survives source staging and history. Unmapped columns remain
strings in `retained_json`. Delete props and retained columns must match the
previous object carrier as required by the existing apply contract.

The selected CSV profile is UTF-8, comma-separated, single-line records with
Python's strict CSV parser, LF or CRLF, unique nonempty headers, at most 1,000
records and 65,536 bytes per input line. Embedded newlines, patches, edges, inferred
versions, numeric coercion and implicit null interpretation are unsupported.
Source epochs identify immutable ordered files; changed input must not reuse the
epoch. Each emitted transaction's inner JSONL offset starts at zero: it is not a
CSV resume watermark. Retained row ordinal plus independently verified original
file custody are required for resume. Do not acknowledge CSV progress until an
admitted immutable publication binds that outer custody; the adapter issues no ACK.

The shared adapter interface is a stream of `SourceBatch` values with original
begin/event/commit bytes, explicit feed/epoch, complete ordered digests and text
cursors. Use `batch_row`/`batch_from_row` for custody and `changes_from_batch` then
`plan_apply(..., schema_policy=...)` for this whole-entity subset. Native source
checkpoints remain separate: PostgreSQL outbox positions and CSV ordinals must
never be substituted for inner JSONL offsets. Additional schema meanings require
a qualified policy rather than a permissive fallback.

For publication intent, `ashlar.source_checkpoint.csv_checkpoint(batch)` retains
the outer CSV ordinal and ordered original header/row digest. Pass its returned
text as `source_checkpoint_json` to `publish_batch`; retained attempt reads
revalidate it against the full original transaction. Use `bind_source_descriptor`
for descriptor correspondence. The older `bind_outbox_descriptor` remains
outbox-only. This correspondence does not prove original immutable file custody,
source permission or the semantic column mapping; the host must independently
admit these before source ACK, alongside manifest/effect/pin/retention admission.

The CSV example also runs against the isolated managed Delta development
namespace. With the existing host SDK environment/profile and installation:

```sh
python tools/run_native_example.py --source csv \
  --installation docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/csv_setup_20261008/summary.json \
  --journal /private/tmp/ashlar-csv-apply-20261008.sqlite \
  --output /tmp/ashlar-csv-native-observation
```

Keep the original journal permanently; a new journal refuses a nonempty graph.
The runner retains original CSV checkpoints before effects, uses exact original
SQL submission handles, verifies current owner/grants/UUIDs and checks the final
selected object plus edge/tombstone/history counts. Four transactions and twelve
mutations were applied; fresh-process replay preserved all original receipts and
source intents. This is a candidate development apply workflow. Full-column
parity, remote fencing, native retention/publication and source ACK remain
unqualified; no manifest is produced by this command.

The installed toolkit also accepts your CSV on stdin:

```sh
ashlar inspect-csv --feed csv-example --epoch immutable-example-1 \
  --source-system local-example --schema-revision 3 --type-id 17 \
  --properties-json '{"label":"23","caption":"24"}' < string-source.csv
```

Each output line carries `batch_row` (recover with `batch_from_row`) and exact
`source_checkpoint_json` (pass to the publication coordinator after independent
admission). This command performs custody adaptation only. Source identity, epoch,
IDs and column mapping are explicit caller inputs, not inferred catalog acceptance;
it does not apply a schema policy, publish or ACK. Invalid mappings and malformed
rows refuse; a later failure leaves earlier complete emitted batches intact and
unacknowledged. The fresh isolated wheel installation passed outside the checkout.

Before trusting adapted CSV semantics, call `validate_csv_batch(batch, feed=...,
epoch=..., source_system=..., schema_revision=..., type_id=..., properties=...)`
with independently admitted original configuration. It reconstructs the exact
singleton transaction from retained header/row bytes and ordinal, and compares
all batch/event custody. Altered mapped values with a recomputed commit digest
refuse. Mapping order is part of the original producer codec; preserve it. This
check does not admit IDs, schema meaning, file authority or read/write permission.
The CLI and both example runners now perform it before downstream use.

### Truss complete-feed adapter component

`ashlar.truss_feed.assemble_feed(original_manifest_artifact, original_fragment_bytes,
policy=..., context=...)` implements bounded proposed complete-feed/0.1 custody
assembly. It accepts the original complete canonical manifest archive, verifies
its domain-framed semantic digest and each exact payload artifact, retains all
original fragment bytes and returns payload bytes in original manifest order.
Missing/foreign/conflicting members refuse; exact fragment replay is idempotent
for the assembled payloads. Bounds are 1,000 manifest members, 128 fragments,
one MiB per fragment and 16 MiB total custody. No ACK occurs.

The host policy must independently implement `admit_manifest(original_bytes,
context)` and `admit_complete(original_bytes, ordered_payload_bytes, context)`,
returning None or refusing. Admission must prove current source authorization,
registered original profile/member ordering, full committed native membership,
configuration/revision prerequisites, safe clock and retention/worker custody,
and payload interpretation. Hashes alone prove none of these. Supply original
manifest archive hash and framed manifest hash as distinct identities.

This component has local wire/refusal evidence only; Truss still has no installed
public runtime here. The 0.2 transition-capable manifest is unsupported and
refuses; do not rewrite it as 0.1 or treat journal-only `(xid, seq)` progress as a
complete transaction. It does not reconstruct property changes into object state.

Predictive optimization stays enabled. The selected publication policy is finite
readability based on verified effective data/log retention. Use
`publication_retention_report(snapshots, configurations, margin_us=...)` to retain
original snapshot commit times and immutable expiry ceilings in the validation
report's `retention` member. Configurations contain explicit `data_retention` and
`log_retention` interval strings for every table. Host admission calls
`validate_publication_retention(descriptor, fresh_configurations, now_us=...,
table_uuids=...)` before and after native consumption. Its returned text is the
effective earliest deadline; policy methods must still return None after all
admission checks. Unsupported/unknown settings and expired snapshots refuse.
Longer settings cannot renew original expiry. Shorter settings tighten it. Real
file/log availability, authenticated configuration/clock/UUID observations, schema,
protocol and permissions remain mandatory. No retention setting or pin lifecycle
is changed by these functions. The earlier disable requirement is superseded.

## Original materialization clock for native runs

New `run_native_example.py` runs retain a Databricks server clock through their
original SQLite submission journal before applying the fixture. Replay uses
that same clock. Keep the journal: a lost submission handle requires explicit
reconciliation. Existing fixture journals preserve their historical fixed
timestamp and original plans. This clock is row metadata, not a publication or
retention anchor.

For a new run, copy `materialization_clock.materialized_at` from its summary
into the independent parity check and select a fresh output directory:

```sh
python3 tools/check_native_local_example.py --source csv \
  --materialized-at '<exact original UTC value from the run summary>' \
  --output /private/tmp/ashlar-new-csv-parity
```

Omitting the clock uses the historical fixture timestamp. Existing evidence
directories are refused rather than overwritten. The checker still requires
the recorded private installation and independently derives all expected rows
from original source events; no Truss acceptance or publication is inferred.

## Publish and resolve the existing native CSV fixture

The private four-event CSV snapshot has a demonstrated immutable manifest and
native publication-resolver singleton. Use the SDK-enabled Python environment,
existing warehouse/profile and private PostgreSQL container:

```sh
/private/tmp/ashlar-db-client/bin/python -B tools/publish_native_csv_example.py \
  --journal /private/tmp/ashlar-csv-publication-20261008.sqlite \
  --output /private/tmp/ashlar-csv-publication-new-observations
```

For this existing publication, **retain and reuse that original journal**. Every
run needs a fresh output directory. The tool preserves the original proposal,
manifest clock, complete snapshot vector and native statement handle; it never
advances a source ACK or changes predictive optimization, grants or settings. A
lost handle or missing original journal requires reconciliation. Do not replace
the journal or choose a fresh clock for the same publication ID.

The successful retained evidence is
`docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/csv_publication_20261008_recovery/`.
It proves one original manifest submission, four active pin guards and an actual
resolver result for object 1 / entity version 2 / schema revision 3. All complete
row inventories, raw intake, configured retention and current permissions are
checked. Repeated checks make this a slow development example; it is not the
production latency path. Original receipts include the pin-role refusal and
pending-handle recovery, rather than hiding those attempts.

The fixture IDs are explicit development mappings, not accepted Truss IDs. This
publishes a previously applied CSV snapshot; streaming through the complete
native publisher phase path and real Truss runtime remains unfinished. The
private pin role adapter relies on the existing trusted administrative login
while executing functions/inventory under ordinary roles. It changes no grants
and must not be used as production authentication.


## Inspect retained effect recovery without cloud calls

For the demonstrated original CSV journal, this command works from a read-only
snapshot and checks exact plan/response recovery without native transport:

```sh
python3 tools/check_retained_effect_recovery.py --journal /private/tmp/ashlar-csv-apply-20261008.sqlite --output out/offline-effect-recovery.json
```

The output path must be fresh. Original native journals must be retained; do not
replace them to restart a pending operation. Native replay uses an explicit
recovery entry point for already-retained plans and refuses lost plan custody.
This offline check does not authenticate current authority, admit a source,
execute native effects, publish a descriptor or acknowledge progress.


## Connect native effects to the stored publisher

The host composition now provides `tools.journaled_publisher_driver.JournaledPublisherDriver`
for `StoredPublisherBackend`. Supply the existing `DurableEffects` instance,
a writer policy holding the complete source/target admission interval, an
original-request planner, artifact `capture` and `recover` producers, complete
native validator and descriptor-bound source acknowledger, and a unique admitted
operation namespace. All services are required; none defaults to acceptance.

The driver retains exact original request/steps before effects and the original
applied artifact before advancing the publisher. Recovery refuses missing plan
custody. Unresolved original artifact observations must be reconciled by the
recovery producer. A known proposed descriptor is never replaced. Artifact
validation checks exact original response/descriptor correspondence before the
injected native validator; source ACK still requires the stored publisher's
committed descriptor and the supplied current source policy.

This host composition has focused test-transport evidence. The development
CSV snapshot's earlier native publication does not establish that this complete
stored-publisher path has run natively. Concrete native capture, validation and
source policies remain required; this is not a ready production CLI.


`tools.journaled_snapshot_artifacts.JournaledSnapshotArtifacts` implements the
host driver's artifact port. Supply the journal, native query executor, current
admission policy, independent expected snapshot inventory producer, and admitted
manifest producer. Each target supplies exact UUID, version, complete columns
and complete expected rows. The component checks native full-row parity and
requires the manifest's entire version vector to match those targets.

Capture records original inputs before native observation and retains one exact
proposal before returning. Recovery reads the original proposal; an interrupted
capture without retained proposal bytes requires reconciliation and refuses a
new observation. This component is wired into the focused host-pipeline tests.
A native installation must still supply source/writer custody, independent
expected-state derivation, protocol/retention/pin policies and ACK semantics.


## Validate the existing native publication without writing

With the documented SDK/psycopg environment, existing local PostgreSQL sandbox,
original CSV publication journal and authorized Databricks profile:

```sh
python3 tools/check_native_csv_artifact.py --journal /private/tmp/ashlar-csv-publication-20261008.sqlite --output out/native-artifact-check
```

The output directory must be fresh. This reads the original journal and immutable
native manifest, holds all original PostgreSQL pin guards, and invokes
`NativeArtifactValidator` against the four original versioned tables. Current
private owner/grants, raw UMF intake, complete rows, protocol, finite retention
and descriptor correspondence must all pass. It changes no data, manifest,
permissions or maintenance settings and issues no source ACK. It requires the
existing demonstrated private fixture; it does not initialize Truss or validate
a substitute deployment. Original statement receipts and their summary are kept
in the output directory.

The same validator is callable from `JournaledPublisherDriver` for new
request-bound artifacts. The earlier CSV descriptor has no publisher request
digest and is checked through the retained-descriptor surface, without relabeling
it as a native stored-publisher execution. A separate one-row stored-publisher
run now demonstrates native phases and local progress; see the workflow below.


## Bind local CSV consumer progress to publication

`tools.journaled_csv_progress.JournaledCsvProgress` is the host driver's concrete
local CSV checkpoint component. Supply the original journal/file/SHA-256, exact
stream/feed/epoch and CSV/schema mapping, held source/writer admission policy,
and a native resolver context that yields the exact committed descriptor while
holding the complete pin/read interval. Its `acknowledge` method can be supplied
to the publisher driver; `position()` observes retained local progress only.

The file must be bounded, owned by the local user, non-symlink and not writable
by group/other. The source authority still comes from the admitted immutable
epoch and held writer policy. Reusing that epoch with different full file bytes
or mapping refuses. Positions advance contiguously, bind exact original rows and
native descriptors, and preserve predecessor custody. Earlier exact repeats are
idempotent. This is local consumer progress, not a remote source or Truss ACK.

A source admission failure before COMMIT rolls back the new checkpoint. A lost
COMMIT receipt or native resolver closure refusal after COMMIT raises
`LocalProgressOutcomeUnknown`; retain and reconcile the original checkpoint.
Callbacks must not close the component's local database transaction. No new
publication or replacement source epoch is licensed by uncertainty. This
component has connected test-transport evidence and a demonstrated one-row native
stored-publisher run. Multi-row continuation/recovery remains to be demonstrated.

## Continue the fresh native installation

The current fresh installation is `ashlar_e2e_private_20261008.runtime_clean_user`.
Its original carrier receipt is `/private/tmp/ashlar-clean-user-setup-20261008/summary.json`.
Revision 3 is now retained in that namespace's managed raw registry, with exact
original source/artifact bytes and incomplete interpretation recorded. Its proof
is `/private/tmp/ashlar-clean-user-schema-20261008/summary.json`, and its registry
journal is `/private/tmp/ashlar-clean-user-schema-20261008.sqlite`.
These are separate original receipts from the older four-row fixture.

The one-row stored publication uses this installation and registry together:

```sh
python3 tools/run_native_csv_stream.py \
  --installation /private/tmp/ashlar-clean-user-setup-20261008/summary.json \
  --intake-proof /private/tmp/ashlar-clean-user-schema-20261008/summary.json \
  --journal /private/tmp/ashlar-clean-user-stream-20261008.sqlite \
  --output out/clean-user-publication \
  --umf-source /path/to/clean/umf-record-check \
  --profile aidev-cus --warehouse 2439e1f2e37ac563 --limit 1
```

The record-checker checkout must be clean at
`c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3` with dependencies installed.
Use the existing SDK/psycopg environment and local PostgreSQL pin service.
Retain the stream journal; use a fresh output directory for each invocation.
After successful terminal publication and checkpoint 1, the same command with
`--query-only --entity-id 1` and another fresh output directory reads the object
through the native singleton resolver. Do not substitute a new journal or old
registry UUID, clear tables or infer success from pending effects. Native
one-row publication has completed at checkpoint 1, with independent complete
current/history/tombstone/edge parity at the stored vector. The native singleton
query completed against the same stored publication without new Delta mutations.
All non-timestamp fields match the independent source oracle. The original raw
TIMESTAMP projection truncated published_at microseconds to milliseconds. The
corrected projection returns this field as canonical signed epoch-microsecond
text (or null). Corrected native verification matches every source-oracle field,
including exact 1791486158556147 microseconds; original publication, checkpoint
and Delta mutation receipts remain unchanged. The source and IDs remain synthetic fixture
inputs; actual Truss acceptance/feed is still required for the full toolkit.

## Stored native CSV publication and singleton query

`tools/run_native_csv_stream.py` is a bounded development workflow for the
supplied four-row CSV and its fixed mapping. It derives the catalog/schema from
the original eight-carrier installation summary. It requires an actual v3
schema-intake proof, an authorized Databricks profile and existing warehouse,
a local PostgreSQL pin service,
and clean pinned UMF checker checkout described above. It uses fixture catalog
IDs and a same-host cooperating writer lane. A clean Truss deployment and remote
producer ACK/fencing remain separate integration work.

The original four-row run has completed, resuming from the initial checkpoint 1
through the remaining create, update and delete. Keep its tables and original
journal; never reinstall, clear phases or replace the journal to retry an
uncertain outcome. A fresh output directory retains each new observation:

```sh
python3 tools/run_native_csv_stream.py \
  --installation /private/tmp/ashlar-csv-stream-setup-20261008/summary.json \
  --intake-proof docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/private_schema_v3_20261008/summary.json \
  --journal /private/tmp/ashlar-csv-stream-20261008.sqlite \
  --output out/native-csv-query \
  --profile aidev-cus --warehouse 2439e1f2e37ac563 \
  --umf-source /path/to/pinned/umf-record-check \
  --limit 4 --query-only --entity-id 1
```

Use `--profile` and `--warehouse` to select existing authorized compute; the
example defaults remain the previously demonstrated development settings.
The installation receipt must match the current UMF-generated model fingerprints,
generator provenance, all eight carrier names, ordered column types and distinct
canonical native UUIDs. Fresh native actor/owner/grant, managed-table, UUID and
protocol checks remain mandatory; the receipt does not grant authority.
The demonstrated namespace remains `ashlar_e2e_private_20261008.runtime_csv_stream`.
Caller-provided namespaces pass focused local configuration checks; a fresh
namespace and its native schema-registry setup have not yet been demonstrated.
Keep each installation's original journals and use a new output directory.

Use the SDK/psycopg-enabled Python environment documented for the native checks.
`--query-only` requires an existing original journal and the exact last retained
ordinal. It reads the original committed descriptor through `read_singleton`,
holds all four PostgreSQL pin guards, renews native authority/protocol/retention
and full snapshot parity, and binds the lookup to the descriptor's Delta version.
The lookup uses bound source/type/object identity and its lookup hash. A missing
object returns `singleton: null`; duplicate identity or failed admission returns
no result. The published_at output is exact signed epoch-microsecond text (or null),
not ISO datetime text. Retained unknown source content survives.
The mode creates no graph effects, manifest, attempt phase or consumer progress.

Omit `--query-only` for the original bounded publisher workflow. Its default
`--limit 1` processes through ordinal 1; already checkpointed rows are skipped.
The demonstrated native continuation reached ordinal 4 with one surviving object
at entity version 2, four original history records and one tombstone. Use the last
retained ordinal (currently 4) with query-only; ordinal 1 is a historical
publication and cannot stand in for the last checkpoint. The final deleted-object
point lookup and exact-repeat native run remain untested. Output includes the original publication ID, singleton and local
consumer position. Repeated metadata checks still dominate runtime; this is
integration evidence rather than a latency benchmark. Predictive optimization
and grants remain unchanged.

Within one complete native pin hold, the first validation checks all fixture
rows. Later admission in that same hold reuses the immutable rows and renews
source/schema/permissions, UUID/protocol, descriptor/pin binding and finite
retention. Closing clears reuse; a fresh query scans again. Any refusal prevents
reuse even if caught. The qualified cooperating maintenance profile still applies,
and predictive optimization remains bounded by the finite retention window.
The demonstrated query issued 112 warehouse reads instead of the earlier 128;
this reduces duplicate fixture scans but is not a latency or scale claim.


## Additional JSONL source through the stored publisher

`tools/run_native_source_stream.py` now has `--source csv|jsonl` (CSV default).
The original CSV command remains a compatibility entrypoint with the same
original journal/request semantics. JSONL selects the supplied
`local-string-source.jsonl` and revision-3 fixture mapping. Its three complete
transactions preserve exact begin/event/commit bytes, required/optional strings,
unknown retained content, updates and deletes. This is a bounded development
source; it does not register real Truss IDs or supply a real Truss feed.

Use an independent admitted empty eight-carrier installation, actual same-target
UMF intake proof and its own retained stream journal:

```sh
python3 tools/run_native_source_stream.py \
  --source jsonl --limit 1 \
  --installation /private/tmp/ashlar-jsonl-setup-20261008/summary.json \
  --intake-proof /private/tmp/ashlar-jsonl-schema-20261008/summary.json \
  --journal /private/tmp/ashlar-jsonl-stream-20261008.sqlite \
  --output out/native-jsonl-first \
  --umf-source /path/to/clean/umf-record-check \
  --profile aidev-cus --warehouse 2439e1f2e37ac563
```

`--limit` counts complete source groups. JSONL's local_consumer_position and
publication progress are actual byte offsets; local_completed_batches is the
separate resume count. Do not replace byte offsets with group ordinals. Continue
through groups 2 or 3 using the original journal and fresh output directories.
After a completed publication, `--query-only --entity-id 1` uses the same original
proof/journal and exact last group limit; published_at is microsecond text.
The local checkpoint only advances after resolver admission inside the complete
pin/retention interval, with post-COMMIT uncertainty explicitly retained.

Three JSONL and five CSV local handoff tests pass. Copies of original native CSV
journals still recover their retained positions. Native setup and exact raw UMF intake passed in
`ashlar_e2e_private_20261008.runtime_jsonl_stream`. Its first complete group
published two objects and two original history rows, with no edges or tombstones,
and advanced the actual byte checkpoint to 797 (one completed group). All four
exact-version inventories match independent original-source reconstruction.
Query-only object 1 matches all 17 fields, including exact epoch microseconds,
through the stored publication at object-table version 2. Original nine mutation
receipts and checkpoint remain unchanged. Publication used 359 warehouse reads
and 154 permission pages; query used 112 reads and 66 pages. These are integration
checks, not singleton latency measurements. Native later-group replay/update/delete
for JSONL remains unverified. This runner cannot combine sources into an existing installation;
source switching must not clear existing tables/phases or manufacture new epochs.
Broader source/schema support and actual Truss remain required for the full goal.


## Stored schema-evolution source (native verification pending)

The source runner now accepts `--source evolution --limit 3` for
`schema-evolution-source.jsonl`: revision-1 creates, an explicitly admitted
revision-3 replacement adding optional caption, then a revision-1 delete.
It requires an independent empty admitted installation and both actual raw
UMF intake receipts in that installation's same registry UUID. Register v1 and
v3 sequentially through `register_umf.py`, retaining the **same registry journal**
so its original CREATE handle is recovered rather than replaced. Keep separate
output directories and original schema revision arguments.

Pass the revision-3 summary as `--intake-proof` and revision-1 summary as
`--additional-intake-proof`; retain one new original stream journal. All other
setup, profile/warehouse, actual UMF checker and query arguments follow the
JSONL command above. Use `--limit 1` to publish only the first group or `--limit 3`
to include the complete four-event transition/deletion sequence. Query-only
requires the exact last retained limit. Original byte progress, immutable history
and both schema proofs remain bound throughout; source switching is forbidden.

Fifteen focused local checks pass, including the explicit transition, original
bytes, independent expected state, missing/foreign proof refusals and unchanged
CSV/JSONL checkpoint behavior. Actual UMF checks cover both original schema
revisions and existing values under v3. Native setup, both original raw UMF intakes and the complete four-event
publication now pass in `ashlar_e2e_private_20261008.runtime_schema_evolution`.
Byte checkpoints are 805, 1318 and 1814. Final publication retains one v3 object,
all four original history records across both revisions, and one v1 delete
in the tombstone carrier. All twelve exact-version inventories match independent
source reconstruction. A survivor query-only check is running; its success and
native exact-repeat remain **unverified**. This remains the fixed string-Record example with development
IDs; broader schemas and actual Truss still require implementation.


## Logical checks with your explicit string-Record bindings

The reusable policy now exposes `record_value_request(change)`, translating
admitted type/property IDs to UMF Record and Field references from the retained
binding plan. It preserves absent optional properties and opaque content. Host
helper `check_bound_umf_records.check_bound_records(umf_source, intake, policy,
batches, schema_path=..., output_dir=...)` invokes the actual upstream checker
once per qualified Record and retains each original receipt in a fresh directory.
Supply your original intake, matching interpretation and independently admitted
MappingEntry inventory to StringRecordPolicy.from_intake; IDs are never inferred.
The selected profile remains required/absent-allowed singleton strings.

A scoped actual check passes three non-delete records with explicit development
type ID 1017 and property IDs 1023/1024. Each source transaction was re-encoded
and its manifest recomputed; no original receipt was relabeled. This demonstrates
that the helper no longer assumes the example IDs. It does not qualify arbitrary
schemas or accepted Truss IDs, and the native runner still selects its documented
fixed sources. The helper does not write graph data or acknowledge a source.
