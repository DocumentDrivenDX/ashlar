# End-to-end example — under construction

The first runnable component is the isolated PostgreSQL substrate:

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
