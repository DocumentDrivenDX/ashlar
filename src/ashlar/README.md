# Ashlar toolkit components

Python3.9+ standard-library core implementing the proposed
[CONTRACT-004](../../docs/helix/02-design/contracts/CONTRACT-004-publication-resolver.md).
The portable core uses no credentials or cloud SDK. Host deployment and transport
tooling are separate, with explicit authenticated profiles and retained journals.

resolve_publication requires an explicit trusted table/UUID inventory, supported
profiles/revisions and a backend. It refuses missing/ambiguous descriptors,
duplicate JSON members, malformed pins, unsupported revisions and native identity
mismatches, and returns defensive immutable metadata. Unknown original JSON text,
large integers and exact decimals are retained. Snapshot versions never follow
latest physical heads implicitly.

NativeBackend builds read-only parameterized descriptor SQL and pinned metadata
probes through an injected authenticated Executor. The Policy must independently
validate descriptor authority, effective caller policy and retained data availability.
There is no permissive default policy. This is the first runtime slice, not a
complete native transport, publisher, authorization service or support claim.
The existing SQL package supplies table definitions and singleton templates.

Run the small local suite from repository root:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The suite covers refusal order, exact metadata, immutable pins, native UUID/version
mismatches and pinned schema checks. No live warehouse or Spark is used. Next wire
an authenticated transport and qualified policy/custody provider into these ports;
keep their integration claims separate from these simulated checks.

SchemaIntake.read adds exact-byte shared UMF custody under CONTRACT-005. Supply
an explicit document revision and trusted validator source pin. It retains the
original source and diagnostic artifact, checks digests/identity/validation flag
consistency and refuses malformed input. It does not authenticate the producer
or perform target catalog acceptance; trust and admission are separate.

plan_catalog_ids in catalog.py plans exact document/owner-qualified IDs from a
complete admitted target inventory and trusted prior state. It preserves retired
reservations and same-identity reactivation; allocation stays above supplied
highwaters. Native acceptance must serialize/revalidate/persist the plan with
its full semantic and report effects. Pure planning does not provide acceptance,
authorization, field binding or a native allocator.

plan_string_record_binding in binding.py consumes independently trusted original
UMF interpretation custody for core0.7 singleton string Records, retaining the
full original report. It emits candidate identities or source-qualified blocked
assertions; engine enforcement remains unimplemented. The host interpretation
CLI calls actual pinned UMF APIs rather than defining private UMF meanings.

jsonl_batches in source.py emits immutable transaction-complete raw custody from
a bounded positioned binary stream under an explicit feed/epoch. The stdin CLI
is tools/read_jsonl_source.py. It preserves original bytes and verifies commit
count/digest; it neither interprets operations nor acknowledges progress. Durable
stage/replay, admitted schemas and complete publication remain required.

DeltaBatchStage in staging.py persists complete exact source custody through
parameterized Delta SQL and a required exclusive-writer policy. It checks native
UUID before/after, exact full readback and identical replay/conflict semantics.
StagedBatch is raw custody only; it authorizes no source acknowledgement. The
local development policy in the native checker does not qualify remote/native
fencing, application grants, graph apply or publication.

plan_apply in apply.py plans one complete explicitly-versioned whole-entity
transaction against trusted prior state and mandatory schema admission policy.
It retains history/tombstones/delivery claims, refuses conflicts/stale changes
and validates final typed endpoints while preserving parallel/isolated graph
identities. Returned state is immutable and prior state remains unchanged on
refusal. Native persistence/publication and Truss property-feed reconstruction
are separate unfinished integration steps.

changes_from_batch in whole_entity.py verifies original complete batch custody
and consumes an explicit signed64 whole-entity event profile. It never guesses
versions from property positions or aliases another source profile. Unknown
executable fields block normalized admission. Use its Change values only with
the mandatory target schema/constraint policy in plan_apply. The native nine-
event example is unpublished materialization evidence, not durable production
apply/recovery or a Truss property-feed adapter.

publish_batch in publisher.py coordinates a mandatory durable backend through
prepared/applying/applied/committing/committed phases. Uncertain submissions use
original apply/commit recovery ports; committed replay only acknowledges the
original descriptor. Native attempt custody, source fencing, complete validation
and immutable manifest/checkpoint producers are unfinished mandatory integration
work. Local mock tests establish orchestration order, not native durability.

DeltaAttemptStore in attempt_store.py retains exact original request/result/
descriptor artifact bytes as a contiguous immutable phase prefix, using bound
Delta MERGE/readback and mandatory exclusive-writer policy. Sessions verify UUID
and invalidate access after release. Native five-row custody/reload is verified;
real effect/commit proof, source fencing/grants, manifest and acknowledgement
producers remain required. A stored phase label is not publication authority.

PostgresOutbox in outbox.py reads bounded committed native source groups and
verifies exact original bytes/membership. OutboxTransaction preserves the outer
PostgreSQL previous/position separately from its contained JSONL batch offsets.
Protected development producer DDL and native role/rollback/replay evidence are
available. Registered source authority/retention, application effect recovery
and outer-cursor publication/acknowledgement integration remain mandatory.

DeltaManifestStore in manifest.py provides bound immutable manifest append and
exact original readback under mandatory writer and independent publication
admission policies. A validation flag is insufficient: the policy must prove
original effects, source progress, schemas, retained version pins and authority.
recorded_at is a canonical UTC microsecond decimal string at this API boundary;
the native carrier is TIMESTAMP. Fifty-nine focused local checks pass, including
replay/conflict and pre-effect denial. Native execution and producer/resolver
wiring remain unfinished; this component creates no default permissive policy.

DurablePublisher in durable_publisher.py connects publish_batch to the Delta
attempt store. Fresh instances reload original request/result/descriptor bytes
and resume uncertain phases through mandatory effect recovery. The effects
provider owns native operations, complete pin/admission proof and source
acknowledgement; it must return exact JSON strings for result/descriptor custody.
There is no default effect provider. Sixty-two local checks pass; native composed
execution remains unfinished.

Host transport tools/durable_sql.py journals each original SQL request before
submission, then retains its native handle and terminal response in synchronous
SQLite storage. Reopening the same operation replays original terminal custody
or polls its original handle; an uncertain submission without a handle refuses
reposting and requires independent native reconciliation. Operation identity
binds exact SQL/parameters, warehouse and explicit authenticated authority.
Retain the private journal; it is recovery state, not expendable telemetry.
This host module is outside the portable core and does not establish source
fencing, effect parity or publication authority. One native SELECT/reload passes.

Host tools/durable_effects.py retains an immutable bounded ordered SQL effect
plan bound to publication request digest, authenticated authority and warehouse
before executing any steps. Mandatory whole-plan writer/admission policies must
prove source/schema/target UUID/prior-state correspondence. Recovery uses each
step’s retained original SQL handle/terminal response. Successful statements do
not prove graph parity, retained pins or publication. Graph SQL-plan generation
and full native effect-provider wiring remain unfinished.

Host whole_graph_sql.graph_sql_plan consumes the original verified whole-entity
batch plus a trusted complete prior state and mandatory schema policy, producing
ordered canonical current/history/tombstone SQL. It binds source identity in
deletes, preserves exact property/retained carriers, uses explicit signed64 IDs
and versions, and emits no effects for already retained original deliveries.
The clock is explicit materialization metadata; it does not declare publication.
Persist plans with DurableEffects before writes. The nine-event native fixture
now passes fresh-journal replay with zero new effect statements, independent
selected-field inventories, all source event bytes and tombstone checks.
Complete all-column parity, native interruption recovery and publication/pin
authority remain unfinished.

validate_effect_snapshot in effect_validation.py verifies a bounded complete
STRING/BIGINT/TIMESTAMP inventory at a trusted exact native UUID/version. It
checks exact schema, every carrier column, SQL NULL versus text, duplicate
multiplicity and original JSON text, with UUID checks around the read. Expected
rows must come from an independent admitted source/prior-state oracle. The
1000-row limit is this fixture validation profile, not graph architecture or
scale acceptance. Native final four-table parity and initial node/edge carrier
parity pass. This proves observed version readability, not future retention,
protocol support, active pin custody or publication authority.

Protected development active-pin DDL is in sql/ashlar-pins/01-postgresql.sql.
It preserves original authority/scope/table UUID/version/custody and explicit
release history, without automatic expiry. Native ordinary-role replay/conflict/
rollback/retention-refusal checks pass. This registry is not a resolver policy:
trusted registration needs independent target availability proof, and every
Delta retention operator must hold its guard transaction over the complete
affected pin/file union. No external Delta retention guard is wired yet.

PostgresPins in pins.py registers complete bounded PinVector scopes and holds
all original active pins through a transactional read context. UUID/version,
authority/scope and custody digest must match every expected row; missing,
released, extra or altered pins refuse the vector. The host transaction adapter
must retain one authenticated PostgreSQL transaction throughout yield. Policy
authentication/registration admission is mandatory. Native four-pin registration
and competing-release lock refusal pass; snapshot/retention policy integration
is still required before using this as read publication authority.

read_singleton in singleton.py holds the complete pin vector across resolution
and native lookup. It binds the original descriptor via a mandatory policy,
requires exact vector versions, uses UTF-8 canonical lookup hash plus complete
typed predicates and checks UUID before/after execution. BIGINT identity/version
and edge endpoints return as strings; original JSON text remains untouched.
Duplicates refuse; absence still requires current row policy. Result escapes
only after final pin custody and authorization checks. Seventy-nine local checks
pass; real manifest/snapshot policy and native composed execution are required.

validate_writer_inventory in authority.py refuses unexpected native owners,
MODIFY/ALL PRIVILEGES/MANAGE/ownership holders, incomplete grant inventories and
unqualified privilege types. Supply fresh complete inherited grants under the
real writer lane; saved inventory is not a fence. Read-only audit found the
client_dev fixtures inherit shared db-aidev-users MODIFY. Their process-local
locks never establish ordinary remote writer exclusion. Dedicated development
catalog ashlar_e2e_private_20261008 is now created with the authenticated owner
and no explicit catalog grants; native table setup/policy wiring is next.

Host tools/setup_native.py now installs the six baseline Delta carriers plus
durable attempt and whole-source-history tables in an admitted existing catalog.
Explicit catalog/schema/journal/output flags avoid global defaults. Authenticated
owner and inherited grant checks run before setup, per table and at completion;
all declared columns/types and retained native UUIDs are verified. Original DDL
handles remain in the host journal. Native isolated eight-table setup passes;
the tables remain empty candidates awaiting schema/feed/publication integration.

Host DatabricksTransport in tools/databricks_transport.py connects authenticated
SDK responses to core SQLResult without coercing native string carriers. It
requires the same authenticated SDK instance/warehouse for fresh reads and
journaled mutations. Mutations require explicit original operation IDs; normal
query routing cannot create replacement writes. Failed/truncated/chunked results,
missing column metadata, duplicate columns, row-shape/count mismatches refuse.
Trusted generated SQL only: prefix routing is not an arbitrary-SQL sandbox.
Native wide-text and17-column metadata adaptation pass; schema/source producer
commands are the next integration step.

Host stage_source.py now streams complete JSONL batches into an additional
source_batch_stage carrier in the admitted runtime. It renews authenticated owner
and inherited grants, retains its original table UUID, validates the exact nine
columns and binds stable original mutation IDs per table/feed/epoch/batch. Fresh
reads validate complete original readback around UUID checks. Explicit operation
wrapping keeps reads fresh while recovering original mutation handles. Native
two-batch/nine-event staging and fresh-process replay pass with no new mutation
submissions, and full original source bytes reconstruct exactly. This is raw
intake; schema/graph admission, remote source fencing and publication/ACK remain
required. The input/cursor/feed/epoch registration is trusted owner configuration.

DeltaSchemaRegistry in schema_registry.py retains complete SchemaIntake source
and diagnostic artifact bytes by immutable document/revision identity under a
mandatory writer policy, bound MERGE and exact UUID/readback checks. Host
register_umf.py invokes the actual clean pinned UMF reader/validator before raw
registration; caller-authored validation flags are not substituted. It verifies
the installed owner/grants, registry UUID/schema and original mutation custody.
Native v1 and additive v3 exact bytes/digests and incomplete interpretation flags
pass independent readback. This is raw intake, not native catalog acceptance,
semantic binding/IDs or a schema barrier for graph data.

StringRecordPolicy in semantic_policy.py rebuilds the selected binding from
retained actual UMF interpretation and enforces required/absent-allowed singleton
string properties by explicit trusted catalog IDs. It refuses unsupported UMF
assertions, unknown properties, null/array/type coercion, source/revision drift
and incomplete/ambiguous ID bindings. Retained JSON stays opaque exact custody.
This is executable host constraint policy for the selected profile, not native
catalog acceptance or proof that supplied bindings came from an accepted head.
Relationships and broader value/key/facet bindings remain required.

StringRecordPolicy.from_intake binds enforcement to the complete retained
SchemaIntake artifact, exact original source SHA256, pinned validator and owner
revision. It reparses the original intake and refuses mismatched interpretation
source or altered custody before returning a policy. Native ID/revision acceptance
is still a separate requirement; a local fixture MappingEntry is not Truss
acceptance authority. The installed private Truss head remains revision0.

lineage.py produces the exact bounded candidate Truss type and tagged relationship
lineage carriers. Twelve independently authored Truss vectors verify exact bytes,
lengths and routing digests, including derived/authored separation, Unicode spelling
and component boundaries. Controls use the required long lowercase escapes. Closed
shapes reject unknown members/profiles, NUL and unpaired surrogates. The encoder
establishes byte correspondence only: original source/ownership admission, native
allocation/report/head authority and protected acceptance remain outstanding.

`tools/probe_truss_catalog.py` executes an initial-catalog rollback-only probe
in the labeled isolated PostgreSQL container. It invokes the actual pinned UMF
reader/interpretation APIs, verifies source correspondence and selected constraints,
holds the catalog head lock, captures native allocation high waters, inserts
candidate source/type/property rows, compares every stored definition/provenance
column and exact source bytes, then rolls back and checks the empty catalog.
The source-completeness constraint rejected the initial missing-provenance attempt;
its original SQL and error remain retained. The corrected native allocation run
passes. This is a producer-development probe with admin custody, not a protected
public acceptance producer or accepted catalog; candidate IDs have no authority.

truss_input.py verifies complete candidate acceptance-input custody against
original profile archives, UMF intake receipts and an explicitly supplied dependency
order. It retains original transport and full domain-framed canonical bytes,
verifies closed shapes/canonical base64/digests and refuses duplicate documents,
unknown profiles/members, source/revision drift and host numeric transform values.
Exact-repeat comparison uses complete preimages; outer JSON formatting is immaterial,
while policy, binding and ordered transform changes remain distinct. Four original
Truss complete-wire vectors independently verify tokens and domain-framed hashes.
This is custody verification, not profile registration, current authority, target
semantic admission, native report production or committed replay.

The catalog probe now retains the complete request and unregistered profile
archives, round-trips its original transport/canonical preimage with native
candidate effects, and still rolls back. Its actual complete request passes
Truss's original input schema using `tools/check_truss_input_schema.ts`; that
shape check is independent of the Python custody checks.

assertions.py and `tools/inspect_umf_assertions.py` retain the selected explicit
Record/Field assertion inventory from actual pinned UMF inspection. Each assertion
resolves its original document/qualified owner/source pointer; complete original
intake/interpretation bytes and their partial interpretation flag are retained.
All entries are none/unqualified. Unsupported closure, wrong source, ambiguity or
resource overflow refuses instead of returning a truncated complete inventory.
The explicit profile and byte/count bounds are unregistered candidates; this
inventory cannot stand in for an admitted complete Truss enforcement report.
`tools/check_truss_assertion_schema.ts` independently checks entries against the
original Truss schema without promoting shapes into completeness/authority.

report_parts.py assembles initial-candidate documents, original diagnostic custody,
interpretation coverage and effect counts only after exact native source/request/
definition/lineage/provenance correspondence. It checks explicit complete additional
key/relationship/endpoint/schema-change/journal/object/edge counts; unknown effects
or columns, missing definitions and boolean/numeric aliases refuse. Original
UMF warnings are retained as one complete original producer artifact under an
explicit candidate diagnostic profile; no diagnostic is extracted/reserialized or
reclassified as source invalidity. Partial interpretation stays partial.
These parts lack execution/origin/report/lifecycle registration and protected
persistence, so they deliberately have no accepted-report interface/revision.
`tools/check_truss_report_parts_schema.ts` verifies four original fragment schemas
without treating fragments as a complete accepted report.

origin.py maps the bounded canonical-tree asserted origin to an ExactValue
representation without treating tag-looking objects or numeric text as values
of another family. It preserves null, booleans, strings, arrays, objects and
empty-container distinctions; duplicate members, host numbers, NUL, surrogates,
unknown mapped variants and resource overflow refuse. Restoring the mapped
representation reproduces the canonical asserted tree. The profile is explicitly
unregistered, and mapping authenticates no database role.

The initial catalog probe now requires `--origin`, captures actual current_user,
session_user and exact xid8 text on the held original transaction, and separately
round-trips asserted/mapped origin with the candidate effects. The development
profile is direct PostgreSQL admin custody only. Caller `db_role` text remains
asserted metadata while the captured database role remains postgres. Report
parts version0.2 checks this correspondence and the original report schemas'
three origin fragments; it still lacks registered installation/epoch/capture
and complete protected report/head persistence. Older candidate parts are not
relabelled as the new version.

The isolated profile-custody SQL store now retains immutable original definition
and selected source-bundle bytes under identity/version. A protected owner function
compares both complete originals and returns their original native context on exact
repeat; changed bytes refuse. Writer/reader NOLOGIN roles have no direct mutation
privileges. Native binary/conflict/direct-write controls, fresh-process replay,
independent reader equality and actual owner/function/grant observations pass.
profile_custody.py verifies complete native receipts and exact xid8 text without
inferring active registration. `tools/retain_profile_custody.py` has bounded original
files and complete submission limits. Its bundle is explicitly selected-source
custody, not full transitive implementation recognition or accepted Truss support.
Six candidate artifacts are durably retained; the accepted Truss head stays zero.

StringRecordPolicy.from_truss_catalog takes an explicit positive canonical native
catalog revision separately from the original owner's document revision. Truss
feed rev maps to Ashlar schema_revision; owner-issued documentRevision must not
be substituted for it. The policy keeps both domains and enforces the event's
catalog revision. from_intake retains the raw document-source default and accepts
an explicit source-schema revision for other admitted adapters. Neither path
establishes acceptance authority: original report/IDs/source-revision admission
remains the caller's independent obligation. The installed Truss head is still zero.


## Install and import

From the repository, `python3 -m pip install .` installs the candidate
ashlar-graph-toolkit distribution and ashlar console command. Core runtime has no
third-party dependencies. A clean Python3.9.6 environment installed the original
wheel offline and verified all packaged Python modules against source, isolated
public imports and installed CLI byte roundtrip. This qualifies local packaging,
not native provider deployment or a production support profile.

The ashlar import exports source batches/decoding, exact UMF intake, selected
string-record and multi-revision policies, immutable graph apply types/planning,
publication resolution and resolver-backed singleton orchestration. Submodules
retain their explicit provider interfaces. Runtime policy and schema-transition
admission are required; installing the library grants no schema/native authority.

```sh
ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
python3 -m ashlar inspect-source --feed example --epoch one < examples/end-to-end/local-string-source.jsonl
```

The bounded command emits complete committed transactions with original bytes,
exact digest and text cursors. A truncated first transaction emits no batch and
fails. Each completed transaction is independent; a later stream failure does not
undo earlier emitted custody. Output never authorizes source acknowledgement.


recover_whole_entity_state supplies bounded whole-entity reconstruction from
complete retained stage rows. Pass an admitted source/epoch interval, exact start
and terminal cursor, trusted prior ApplyState and schema/transition policies. It
checks contiguous original transaction custody, preserves complete retained
history/tombstones/deliveries and returns an ordered original-artifact digest.
Native table/snapshot/order/interval authority is external; the result is neither
source ACK nor publication and must not replace original-handle effect recovery.
The schema-evolution example exercises complete reconstruction and checks equality
against the directly applied source stream.


## Additional source: PostgreSQL committed outbox

PostgresOutbox.read returns complete immutable OutboxTransaction groups under
ashlar-postgresql-outbox/0.1. The supplied authenticated executor and source scope
must bind the protected native table and original epoch. Use the returned previous/
position strings to page; do not use batch.cursor_after as the PostgreSQL checkpoint.
Each contained transaction has its own independent zero-based JSONL byte cursor.

apply_outbox_transactions composes a page into a trusted prior graph state under
explicit schema and transition policies. Supply the admitted feed/epoch, previous
native after position and exact expected_position for that page. It verifies
original payload digest, complete inner custody and contiguous native positions,
then returns immutable graph state and terminal native position. The page/result
never grants source ACK. Advance a durable source checkpoint only after the
publisher independently binds the original group interval to a committed descriptor.
The event profile must be ashlar-whole-entity/0.1; opaque events or Truss property
journals require their own semantic adapter and cannot be relabelled.


protocol.ReaderProtocolProfile supplies explicit conservative SQL-reader protocol
recognition. validate_protocol_detail checks the exact original table UUID, delta
format, canonical reader/writer versions and complete advertised feature inventory.
inspect_protocol performs a fresh authenticated native observation. Unknown
features or versions refuse; the library supplies no permissive profile. Qualify
the host SQL engine independently for the chosen feature set. This table-level
observation neither proves pinned files remain available nor replaces schema,
active pin, descriptor and authorization checks in NativeBackend.Policy.


## Configured finite readability in publisher and reader policies

`ashlar.retention_policy` provides `RetentionGate`, `RetentionNativePolicy` and
`RetentionManifestPolicy`. Wrap qualified existing policies; do not replace their
source/custody/file/protocol/permission checks with these adapters. The same gate
checks the original immutable snapshot ceiling against fresh complete-vector
configuration, physical UUID and server-clock observations, including the closing
singleton and manifest checks. No wrapper disables predictive optimization.

For bounded development SQL observations:

```python
from ashlar.retention_policy import (SQLRetentionProvider, RetentionGate,
    RetentionNativePolicy, RetentionManifestPolicy)

provider = SQLRetentionProvider(authenticated_executor, admitted_table_uuids,
    defaults=qualified_native_defaults, default_profile=qualified_default_profile,
    max_observation_span_us=5_000_000)
gate = RetentionGate(provider, minimum_margin_us=60_000_000)
reader_policy = RetentionNativePolicy(qualified_reader_policy, gate)
manifest_policy = RetentionManifestPolicy(qualified_manifest_policy, gate)
# Inject reader_policy into NativeBackend and manifest_policy into DeltaManifestStore.
```

These inputs deliberately have no defaults for credentials, authority or native
qualification. The illustrative 5-second observation/60-second margin choices
are development assumptions; original reports must reserve the host budget.
The serial SQL provider adds 13 queries per four-table check and is not a fast
singleton production path. Qualified metadata providers may batch observations
within an explicitly admitted age/configuration-change budget. Native availability
and permissions remain required; original snapshot timestamps need independent
publication custody. The closing manifest check can refuse after a native commit;
that preserves the committed row and returns no success, rather than rolling it
back or authorizing a blind retry. Keep the original operation/handle journal.

All 180 small tests pass. Native publication with these composed policies and
real Truss integration remain to be demonstrated.

## Compose durable publication stores

`StoredPublisherBackend` supplies the coordinator implementation over the phase
store. The host must provide a qualified effect/source driver and original
manifest factory:

```python
from ashlar import StoredPublisherBackend
from ashlar.attempt_store import DeltaAttemptStore
from journaled_attempts import JournaledAttemptExecutor
from journaled_manifest import JournaledManifestStore

phase_executor = JournaledAttemptExecutor(transport, namespace=admitted_namespace)
attempts = DeltaAttemptStore(phase_executor, attempt_writer_policy,
    attempt_table, attempt_uuid)

def manifests(request, context):
    return JournaledManifestStore(transport, qualified_manifest_policy,
        manifest_table, manifest_uuid,
        operation='manifest:' + request['request_digest'])

backend = StoredPublisherBackend(attempts, qualified_effect_source_driver, manifests)
# Pass backend to publish_batch with original predecessor, revisions and checkpoint.
```

The two `journaled_*` modules are host tools, imported with `tools` on the host's
Python path. They use the supplied original `DatabricksTransport`/`DurableSQL`;
no credentials or authority are inferred. The namespace and permanent journal
must be independently bound to the original installation and caller.

The driver supplies `writer`, `apply`, `recover_apply`, `validate` and
`acknowledge`; see the class docstring for signatures. Apply/recovery must return
the exact original JSON artifact with `effects` and `manifest` fields, retained
from actual native effects and manifest planning. Driver validation/ACK still
require current full source/schema/pin/file/protocol/retention/authorization
admission. No default driver is supplied. Commit recovery requires original
submission custody; missing submission/handle refuses for reconciliation.

All 192 tests pass, including local composed-store recovery. Actual native
publication/resolver success and runnable Truss integration remain unfinished.
