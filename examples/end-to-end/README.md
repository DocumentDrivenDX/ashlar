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
