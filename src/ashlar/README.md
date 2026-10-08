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
