# End-to-end toolkit implementation plan

Owner goal2026-10-08: stand up Truss, consume UMF in Truss and Ashlar, add/evolve
Ashlar schemas, stream Truss and additional sources into UC managed Delta, and
provide a runnable setup/publish/query workflow. This supersedes the earlier UMF
deferral for this integration work. Existing meaning, identity, history and
publication requirements remain binding. Use small examples, not scale benchmarks.

## Deliverable and acceptance

A clean user can follow one documented workflow to start the selected Truss
runtime, submit a retained UMF document, write graph data, run the feed consumer,
resolve a completed Ashlar publication and perform a native singleton lookup.
A second schema revision and a second source can be applied without silently
renumbering identities, dropping unknown content or bypassing revision barriers.

Acceptance requires actual end-to-end execution, not a running PostgreSQL server,
source-only DDL, local simulator or successful resolver metadata probe. Retain
independent expected outputs and target/profile versions. Replay must preserve
current/history/tombstones, same-identity/version conflicts must refuse, an
interrupted publication must retain the previous descriptor, and source progress
must advance only after the required complete publication/durable handoff.
Demonstrate a create, property update, parallel edge, isolated node and delete.

## Ordered slices

1. **Local Truss deployment substrate and source selection.** Start isolated
   PostgreSQL17.9 with1CPU/512MiB. Pin the Truss/UMF source revisions and reconcile
   the current0.11 storage/installation inventory with accepted Truss decisions.
   Install the selected runtime through its public host interfaces; do not
   concatenate draft SQL fragments or substitute legacy0.2 silently. Native
   roles/producers/feed registration and complete transaction boundaries belong
   to the selected Truss profile. Server readiness alone is only infrastructure.
2. **Shared UMF schema intake.** Use UMF's existing TypeScript APIs at a pinned
   source/package/envelope version. Retain exact original documents and hashes;
   report known supported constructs, unknown retained assertions and enforcement
   classes. Compose stable catalog identity mappings for both consumers, keeping
   Truss catalog IDs authoritative where imported. Add the Ashlar schema-registry
   contract and revision compatibility/refusal rules before its implementation.
3. **Small Truss producer/feed.** Implement the declared mutation and complete
   feed boundary under Truss contracts. Register the consumer, reconstruct full
   current carriers from the native property/lifecycle/revision records, and
   preserve opaque source cursors/transaction boundaries. Do not replace the real
   Truss source with invented whole-entity journal rows and call it equivalent.
4. **Ashlar ingestion/publication.** Add a source-adapter port, staging/raw custody,
   idempotent history/current/delete apply and serialized immutable publication.
   Use small private UC tables on existing authorized compute. Wire the Python
   resolver to authenticated native transport, effective policy and retained pin
   custody. Preserve original source/revision/unknown bytes and source progress.
5. **Other source adapters.** Add JSONL/stdin and a PostgreSQL transactional outbox
   source profile, with declared key/version/replay/transaction semantics and
   durable checkpoints. Both use the same ingestion boundary; neither claims
   Truss-native semantics. Schema additions and an additive revision must run.
6. **Runnable example and user packaging.** Supply setup commands, a shared UMF
   fixture, producer changes, bounded consumer execution, native query and replay/
   interrupted-publication examples. Record live evidence separately from local
   tests and expose incomplete/unsupported outcomes. Keep secrets out of Git.

## Current authoritative starting points

Ashlar has candidate0.3 DDL and Python resolver/native-verifier ports under
CONTRACT-001–004. Truss's current documentation records a0.11/46-table source
packet but unstarted runtime and unfinished native installation/adoption. Its
accepted TypeScript/Bun portable-core and PostgreSQL generic-catalog directions
apply. UMF's checked-out source has implemented0.7 relationship APIs and earlier
field/key/facet capabilities; exact package/API selection must be pinned when
composing schema intake. Branches may evolve independently: retain per-step
source hashes and do not mix incompatible profiles.

## First execution

The isolated container ashlar-e2e-truss-pg17 is healthy on PostgreSQL17.9,
localhost15432, capped at512MiB/1CPU. tools/start_truss_sandbox.py creates/reuses
only its labelled container and generates a private database password without
printing it. The current Truss 0.11 declarations are now installed in this sandbox; no runtime
or feed has yet been implemented.
Other running containers are untouched. Use docker exec for local administrative
setup; design a least-privilege application role before the runtime integration.
No production data, broad database grants, retention cleanup, warehouse resize
or scale benchmark is part of this setup.

## Shared intake implementation checkpoint

The pinned UMF16c35e8d reader/validator now produces exact-byte shared artifacts
for the original Truss source-review model, an additive optional-string revision
and an unknown-assertion variant. Six bounded integration checks pass against
real APIs. UMF experimental warnings remain visible; no complete-interpretation
or target-enforcement claim is manufactured. CONTRACT-005 defines the intake
boundary. Next persist the same artifact in both native registries and implement
the selected binding/catalog acceptance; no full Truss runtime/feed is delivered.

## Native storage installation checkpoint

The exact 0.11 declaration source SHA256
1ac7cc82405ff581072d45ad586f54d8c48343eaecc7c011195535ed359e37f9
now installs transactionally on PostgreSQL17.9 using an explicit isolated UTF8
compatibility helper for one generated expression and a final statement
terminator. Native membership matches all 46 source tables and 442 columns;
two catalog functions are present and schema head remains 0. Five small helper
byte-conversion cases pass. Preserve original-source and derived-execution
hashes, failed receipts and the full native inventory. This does not establish
constraint/privilege equivalence or complete runtime adoption.

Next implement the selected atomic schema acceptance and stable binding, then
mutation/feed operations; storing a raw UMF document alone must not advance the
accepted catalog head. Application roles need their explicit privilege profile
before user-facing runtime access.

## Native shared schema custody checkpoint

The raw shared artifact now persists in both the isolated PostgreSQL intake
store and a private UC Delta intake table. Three synthetic documents retain
exact source/diagnostic bytes; identical replay preserves originals and a
same-key different valid document refuses without changing either store.
The separate raw registry does not mutate accepted Truss head (still 0).
Twelve focused local tests pass, including custody corruption/duplicate/unknown
refusals. Native receipts preserve the initial rejected Delta inline-CHECK DDL
and its terminal-state recovery through separate ALTER constraints.

The next runtime boundary remains complete target binding and atomic accepted
catalog persistence, including stable identity mapping and native report/effect
accounting. Truss HEAD 3f578b2 was consulted for this iteration; its source
contracts still mark native producers/guards/complete runtime unfinished. Do
not replace these with a schema_doc insert or advance a data publication from
raw registry custody. Additional-source adapters and publication wiring remain
part of the same active end-to-end goal.

## Stable identity planner checkpoint

The pure ID planner now retains authoritative supplied IDs across unchanged
identity, retirement and same-identity reactivation; distinct identity allocates
above the family highwater. It refuses duplicate state, missing active owners
and exhaustion, and preserves retired reservations. Eighteen focused local
tests pass. A separate host projection uses the pinned real UMF reader to
record all three original authored elements in the additive fixture with source
positions/digest and original diagnostics. Neither synthetic test IDs nor that
identity inventory are installed catalog acceptance.

Next compose the complete target semantic inventory with these identities and
locked native state, then persist it together with immutable acceptance report
and complete original effects. No native catalog head is advanced by this
planner. Source/feed and publication integration remain required.

## Actual semantic inspection and binding checkpoint

The pinned real UMF interpretation APIs now produce full source-qualified
receipts. This revealed revision 2 caption nullability optional is unknown under
the selected API, although structural intake is valid. Preserve its original
bytes/native intake row; its target binding is blocked. Separately authored
revision 3 uses absent-allowed and produces a candidate string-record binding.
Schema-property inspection requires 0.8.0 and its actual 0.7 refusal is retained.
No envelope rewrite or private interpretation fills that version gap.

The initial selected string-record target planner maps complete explicit members
to document/owner-qualified identities for the stable ID planner, preserving
source diagnostics and refusing unbound assertions, foreign/duplicate/missing
receipts and mismatched source custody. Twenty-two focused local tests pass.
Engine enforcement, broader value/key/relationship bindings and complete native
acceptance remain required; candidate plans cannot advance any accepted head.
No Databricks workload occurred in this iteration.

## Streaming source adapter checkpoint

The binary JSONL/stdin adapter now produces transaction-complete custody batches
with exact byte offsets, original control/event bytes and independently checked
ordered count/digest. One small synthetic create/update transaction runs through
the CLI; its output reconstructs the complete source byte-for-byte. Twenty-eight
local tests pass, including truncation, duplicate delivery, malformed control,
wide cursor and bounded transaction refusals. This supplies the common batch
boundary without claiming event interpretation or Truss-native semantics.

Next connect durable raw staging and replay/conflict custody to serialized apply/
publish, then add PostgreSQL outbox and the complete qualified Truss feed. The
reader exposes no acknowledgement and no checkpoint advance; publication remains
required before source progress. Native schema acceptance and the remaining
bindings remain active work. No Databricks workload ran in this iteration.

## Durable Delta raw staging checkpoint

The SourceBatch now reaches a real private UC Delta stage through a reusable
parameterized backend and mandatory exclusive-writer policy port. One synthetic
two-event transaction passes native original-byte custody, identical replay and
changed-valid-source conflict refusal with preserved original row/UUID. A
separate native readback independently reconstructs the full source bytes.
Thirty-four focused local tests pass, including admission/order/refusal cases.
The development process lock is only local cooperating-writer exclusion, not
qualified native/remote fencing. No graph publication or checkpoint advanced.

Next implement admitted per-delivery/version replay and graph current/history/
tombstone apply, followed by serialized immutable publication and checkpoint
admission. Raw batch stage custody is not semantic support or a source progress
receipt. Native Truss acceptance/feed, broader schema binding and the outbox
source remain required for the full end-to-end goal.

## Graph apply planning checkpoint

The pure explicitly-versioned whole-entity apply boundary now handles create,
replace, delete, exact delivery replay, immutable original history and tombstones.
It refuses changed delivery/version custody, stale changes, overwrite/resurrection
and incomplete typed endpoint state, retaining unchanged prior state on failure.
Independent cases cover parallel edges, an isolated node, endpoints created later
in the same transaction and node/incident-edge deletion ordering. Thirty-nine
focused local checks pass. No native mutation or source progress occurred.

Next normalize complete admitted source records into this boundary and persist
current/history/tombstone/delivery effects on Delta under serialized authority,
then publish immutable validated pins. The Truss per-property adapter must
reconstruct native complete state and journal independently; this whole-entity
profile does not infer its version semantics or close the Truss runtime goal.

## Stream-to-native graph materialization checkpoint

The explicit whole-entity adapter now consumes actual complete JSONL source
batches without inferring IDs/versions or executable meaning. A two-transaction,
nine-event fixture ran on fresh private UC canonical0.3 current/tombstone tables
and a full original-event history carrier. Independent inventories verify
initial parallel edges/isolated node, replacement preserving null/wide retained
text, final deletes, three tombstones and all nine original source byte records.
Native final object lookup hashes match host encoding. Forty-two local checks
pass. Original unsupported DELETE and verified-empty recovery are preserved.

This native experiment is unpublished and process-serialized; its separate
table writes do not establish atomic read publication, native source fencing
or durable replay/recovery. Reusable version/delivery admission and native
recovery must precede immutable manifest/checkpoint production. Truss native
schema/runtime/feed, broader UMF bindings and PostgreSQL outbox remain required.
No scale workload or warehouse resize occurred.

## Publication orchestration checkpoint

The reusable coordinator now retains a stable complete request identity through
prepared/applying/applied/committing/committed phases. It requires persisted
submission phases before graph or descriptor effects and separate original-handle
recovery callbacks on interruption. Only the retained committed descriptor can
reach idempotent source acknowledgement. Forty-eight local tests verify order,
changed intent/refusal and uncertainty recovery without repeating mutations or
commits; incomplete validation keeps prior publication/progress. No Databricks
workload or actual graph publication occurred this iteration.

Next implement native durable attempt/result/descriptor stores and recovery
producer bodies under qualified source/writer authority, connect complete effect
validation to actual pins and wire the resolver/acknowledgement transport. The
coordinator is not itself native durable state or proof of authority. The private
graph fixture stays unpublished. Truss schema/runtime/feed and broader schema/
outbox support remain required parts of the same goal.

## Native durable attempt custody checkpoint

The reusable Delta attempt store now retains original request and contiguous
phase artifact bytes under injected writer authority, native UUID checks and
parameterized append/readback. Five tiny native rows pass original-byte replay,
changed-result refusal and fresh object/session reload from durable UC state.
The result/descriptor are explicitly synthetic storage artifacts; neither is a
real graph effect report or publication. Fifty-one local checks pass. No graph
heads or source checkpoints changed during this storage check.

Next bind real native effect receipts/handles and complete effect/pin validation
to the coordinator/store, then implement immutable descriptor and checkpoint
producers. Native source fencing/privileges and Truss acceptance/runtime/feed
remain unqualified; this store does not replace them. The existing graph fixture
still has no completed publication. No scale test or warehouse resize occurred.

## PostgreSQL outbox source checkpoint

The additional outbox source now has a real protected PostgreSQL append function
and committed-group reader. Two native committed groups/seven events preserve
original bytes; a rolled-back pending group is absent with unchanged head.
Exact replay retains original position, changed valid source bytes refuse,
writer direct DML and reader append are denied under ordinary role tests.
The consumer pages exact native positions separately from contained JSONL byte
offsets and refuses missing/original-custody failures. Fifty-four local tests
pass. This used the existing bounded local container, with no Databricks run.

Next bind its trusted connection/schema/feed/epoch and outer native cursor to
publication/checkpoint custody, qualify application write transaction/lost-commit
correspondence and keep required source retention. No automatic arbitrary-SQL
capture or Truss-native feed is inferred. Native publication producer/recovery
and Truss schema/runtime/feed remain active work; the private graph still has
no completed publication.

## Immutable manifest append checkpoint

The native SQL manifest append component now accepts exact seven-field carrier
rows, checks the trusted table UUID around mutation/readback, binds all values,
and refuses changed original publication bytes. Writer serialization and
independent complete publication admission are mandatory injected policies;
complete validation flags alone cannot authorize publication. Its clock boundary
is canonical UTC microseconds, converted to/from native TIMESTAMP. Fifty-nine
focused local checks pass. SQL execution, real admission proof and resolver
wiring are still required; the existing graph remains unpublished.

Next connect the durable coordinator to real effect recovery and retained pin
proof, then exercise one genuine publication and singleton lookup. Truss native
acceptance/runtime/feed and broader UMF bindings remain required for the full
end-to-end goal. This iteration ran locally and incurred no Databricks workload.

## Durable coordinator integration checkpoint

DurablePublisher now bridges the reusable coordinator to DeltaAttemptStore,
retaining exact JSON effect and descriptor artifacts without reserialization.
Its writer scope nests explicit source/effect authority and attempt-store
authority. Fresh coordinator/store instances resume applying/committing phases
through original recovery ports, committed replay only acknowledges the original
descriptor, and failed validation retains applied custody without committing.
Sixty-two local tests pass. These integration checks use an in-memory SQL
transport, not native effect or publication proof. No Databricks run occurred.

Next implement the qualified native effect provider and retained-pin proof,
then exercise the composed publication/resolver/singleton workflow. Truss still
has design/reference artifacts and no runtime source at inspected commit
29bde23; actual Truss acceptance/mutation/feed remains required by the goal.

## Original native SQL submission custody checkpoint

The host transport now retains per-operation request intent before POST and
original native handle/terminal response afterward in synchronous SQLite.
Fresh connections replay terminal custody or GET the retained handle; a lost
POST response with no handle refuses another POST. Exact changed SQL, parameter,
warehouse or authority conflicts refuse. Terminal failures remain original
failures. The journal is mandatory retained recovery state, with host filesystem
access and lifetime controlled by deployment; it is not remote source fencing.

Sixty-six focused local checks pass. One bound read-only SELECT on the existing
warehouse and fresh journal connection replay pass with retained native receipt.
No graph data or manifests changed. Run tools/check_native_durable_sql.py with
explicit --journal and --output paths; pending outcomes resume the same journal
and operation. An unknown no-handle submission requires native reconciliation.

Next use these original handles in the real effect provider, retain admitted
plans before graph writes, and qualify complete effect/schema/source/pin parity
before manifest publication. Truss acceptance/runtime/feed remains unfinished.

## Immutable ordered effect plan checkpoint

The host effect runner now retains the complete ordered SQL/parameter plan
before native statements, binding the original publication request digest,
authenticated authority and warehouse. Each step has a stable digest/ordinal
operation identity in the original-handle journal. Resuming completed steps
replays their original terminal receipts; a pending step GETs its original
handle. Changed plans cannot replace the retained original, and an unadmitted
plan writes neither intent nor effects. Whole-plan writer/admission is required.

Sixty-eight local checks pass, including a two-step interruption followed by
fresh journal/runner recovery with two POSTs total and one original-handle GET.
No native workload occurred this iteration. Successful SQL remains effect
evidence, not complete graph/source/schema parity or retained-pin proof.

Next replace the one-shot graph fixture’s direct writes with generated admitted
plans and independent native current/history/tombstone validation; connect the
result to DurablePublisher before producing a genuine manifest. Truss native
acceptance/runtime/feed and broader UMF schema bindings remain required.

## Recoverable native graph materialization checkpoint

The whole-entity planner now generates ordered parameterized current deletion/
insertion, source-history and tombstone steps from verified original batches,
trusted complete prior state and mandatory schema admission. Deletes bind the
complete source/typed identity; exact properties/unknown retained text survive.
Replayed already retained deliveries produce no new SQL plan steps. Metadata
clock is supplied explicitly and is not a completed publication declaration.

The fresh private UC schema client_dev.ashlar_recoverable_graph_20261008 ran
two source batches/nine events through immutable admitted plans and original
statement handles. Fifteen setup/effect statements retain terminal responses.
Reopening the SQLite transport and replaying each plan emits zero new effect
statements. Independent inventories verify create/update/delete, parallel edges
and isolated nodes, exact original source event bytes and three tombstones.
Seventy local checks pass; native receipts and exported original journal custody
are retained. This is private admin/local cooperating-process serialization;
remote/native fencing, ordinary-role immutable grants and injected interrupted
native mutation are unqualified. This is still unpublished materialization.

Next extend effect validation to complete all-column correspondence and retained
version/pin custody, connect results to DurablePublisher and exercise a genuine
manifest/resolver/singleton path. Actual Truss schema acceptance/runtime/feed
and broader UMF bindings remain required. No benchmark or resize occurred.

## Complete native carrier parity checkpoint

The reusable exact-version verifier now checks the full declared schema and
every carrier column, preserving original JSON text, NULL distinction and
duplicate multiplicity under native UUID checks. The native source oracle
reconstructs expected carriers directly from the original events, not generated
SQL rows. Seventy-three local checks pass. Read-only native verification passes
for the final object_current4, edge_current3, tombstone1 and whole_source_history2
version vector, plus initial object/edge version2 inventories. All17 object,
20 edge, 10 tombstone and6 history columns are covered, including nonempty
initial parallel edges. Retained unknown wide numeric tokens remain exact text.

The bounded1000-row validator is a tiny-fixture profile, not a changed graph
scale requirement or production full-graph collect plan. Successful reads prove
current data-file readability for these versions, not future retained-data
availability, protocol policy or active pin custody. No manifest/checkpoint or
data changed. Next qualify retained pin authority and connect the real effect
result to manifest/resolver/singleton. Actual Truss native acceptance/runtime/
feed and broader UMF bindings remain required by the same active goal.

## Protected active-pin registry checkpoint

Installed the private PostgreSQL ashlar_pins substrate with immutable original
pin identity and source/custody scope, explicit retained release and no expiry.
Ordinary roles separate registration/release, read and maintenance guard; direct
table mutations are unavailable. Native replay preserves one original active
version; changed-version reuse refuses, writer DELETE and reader release are
denied, active-pin retention admission refuses, and rolled-back release leaves
the pin active. The first recovery-fixture object version4 is registered.

This is registry/guard evidence, not full retained-pin authority. Every admitted
Delta retention operator must hold the guard transaction and reconcile all
affected files/versions and pin scopes; registration must independently verify
retained availability before commit. Those integrations and privileges remain
unqualified. No automatic TTL, purge, VACUUM, retention setting or Databricks
workload occurred. Next connect full publication-vector registration/read custody
and the authenticated snapshot policy, while preserving the external retention
operator obligation. Actual Truss acceptance/runtime/feed remains unfinished.

## Full-vector read pin custody checkpoint

PinVector defensively retains the exact native UUID/version vector and original
authority/scope/custody digest. PostgresPins registers every entry in one host
transaction, validates the complete retained scope inventory and refuses
partial/released/extra/changed entries. Stable pin IDs bind scope/table, so a
changed version cannot silently allocate another original pin. Registration
admission and current read authorization are mandatory policies.

A protected assert_active function holds a SHARE lock throughout reader custody.
Native checks register all four final graph recovery pins, hold their guards
under the ordinary reader role and observe a competing ordinary writer release
fail with native lock timeout. Committing the reader leaves all originals active.
Seventy-six local checks pass. This qualifies read/release exclusion and recovery
vector storage, not future Delta retention, protocol or publication authority.
No Delta workload or cleanup occurred. Next connect authenticated snapshot policy
and native resolver execution, retaining the separate requirement that every
admitted Delta retention operator honor the complete affected pin/file union.
Truss native acceptance/runtime/feed remains required by the active goal.

## Resolver-bound singleton execution checkpoint

The portable singleton adapter now holds all original pins through resolution,
requires the complete publication vector to match held UUID/versions, and binds
original descriptor custody via a mandatory policy. The point read uses explicit
VERSION AS OF, bound source/type/id/hash and full typed predicates, with LIMIT2
to detect duplicate identities. UUID is checked around execution; current caller
and row policy are revalidated even on absence. The result cannot escape before
final pin context checks. Identifiers/version/typed endpoints cross SQL as exact
strings and JSON carrier text stays original.

Seventy-nine local checks pass, including UTF-8 Unicode hashing, signed64 max,
wide retained text, absence/duplicate controls, custody/row refusal, vector drift,
namespace replacement and final pin-authority loss. Corrected the SQL planner’s
identity hash encoder to ensure_ascii=False, matching the established native UTF-8
hash profile; the ASCII native fixture is unchanged. These are local composition
checks, not a native completed publication or live policy claim. No Databricks
workload occurred. Next connect actual immutable manifest/admission and protocol/
retention snapshot policy to this path and demonstrate native execution. Truss
native acceptance/runtime/feed and broader UMF bindings remain required.

## Native permission inventory and isolated catalog checkpoint

Read-only inventory found all four recoverable fixture tables inherit shared
db-aidev-users MODIFY from client_dev, alongside other ALL PRIVILEGES/MANAGE
grants. They cannot establish ordinary remote writer exclusion. Protocol is
reader3/writer7 with deletionVectors/rowTracking/v2Checkpoint and other recorded
features; native successful reads do not promote external-engine compatibility.
The SDK catalog creation attempt refused a missing metastore storage root.
Azure’s documented Default Storage SQL path on serverless compute succeeded:
ashlar_e2e_private_20261008 is owned by the authenticated principal and currently
has no explicit catalog grants. No existing shared grants were changed.
See https://learn.microsoft.com/en-us/azure/databricks/storage/default-storage .

The writer inventory checker now refuses unexpected owners/writers/delegators,
incomplete metadata and unknown privilege types. Eighty-two local checks pass;
saved native inventory admits the dedicated catalog and refuses all four shared
fixtures. This is metadata admission, not a current writer fence or retained pin
proof. Console automation was explicitly refused, so the browser path was
stopped and the supported SQL API was used. No data/retention changes or
benchmark occurred. Next stand up the graph/manifest/attempt carriers in the
dedicated catalog, renew table/ancestor grants and complete native snapshot/
publication policy there. Truss acceptance/runtime/feed remains required.

## Isolated native carrier deployment checkpoint

The reusable setup_native.py command now installs all six proposed baseline
carriers plus publication_attempt_phase and whole_source_history in an admitted
existing catalog. Catalog/schema, journal, evidence output and host authentication
are explicit; warehouse/profile retain the selected development defaults. Native
DDL requests persist before submission and original handles/terminal responses
remain available for recovery. Installed native UUIDs are retained and checked
against later observations, rather than silently adopting recreated targets.

Eight empty tables are installed in ashlar_e2e_private_20261008.runtime. Complete
declared column/type comparisons, authenticated owners and fresh inherited
permission inventories pass per table and for both ancestors. SDK0.102.0 was
used; the shared SQL evidence transport now honors an explicit selected profile.
Eighty-two existing local checks still pass. No graph data/manifest/checkpoint,
retention property, cleanup or shared grant changed. These are ready development
carriers, not accepted UMF/Truss schemas or a published graph. Next connect schema
intake and admitted recoverable streaming/publication bodies to this namespace,
then exercise the native resolver/singleton path. Truss acceptance/runtime/feed
remains unfinished and required by the same goal.

## Reusable authenticated host-to-core transport checkpoint

DatabricksTransport now adapts complete native SDK responses to portable SQLResult
with exact STRING carriers and column/type metadata. Reads use fresh observations;
mutations require an explicit original operation ID and the retained-handle
journal. Read/write clients must share the authenticated SDK object and warehouse.
Failed/truncated/chunked, malformed schema/rows and incomplete row-count responses
refuse rather than manufacturing success. SQL is trusted generated implementation
content; prefix checks route operations and do not authorize arbitrary caller SQL.

Eighty-four local checks pass. Two native read-only checks preserve a wide numeric
token as original STRING and all17 object_current columns in the isolated runtime.
No source/schema/graph/manifest writes occurred. Next connect actual UMF raw intake
and complete-batch stream staging to the admitted namespace through explicit
original mutation operations, then join graph publication/resolver execution.
Truss acceptance/runtime/feed remains unfinished and required by the active goal.

## Isolated runtime streaming intake checkpoint

The runnable stage_source.py command now reads one complete bounded transaction
at a time from a binary JSONL input and retains original complete source custody
in ashlar_e2e_private_20261008.runtime.source_batch_stage. It uses installed
namespace/owner configuration, fresh owner/inherited permission and UUID/schema
checks, same-host serialization and stable original operation IDs with retained
native handles. It does not infer schema meaning or acknowledge source progress.
Input feed/epoch/cursor provenance remains trusted explicit owner configuration;
production source registration/remote fencing is still required.

Two native batches/nine events pass original readback, digest checks and exact
byte reconstruction including begin/commit markers and line endings. Fresh
process replay completes with the same three terminal mutation submissions
(one carrier DDL/two batches), without replacement writes. Eighty-five local
checks pass. Raw staging preserves unsupported payloads but cannot advertise
normalized interpretation. No manifest/checkpoint or retention/grant changes
occurred. Next connect UMF schema intake/admission and graph effect/publication
consumption to this staged source; Truss acceptance/runtime/feed remains required.

## Runnable pinned UMF registry intake checkpoint

The portable immutable raw registry now validates original SchemaIntake receipts
and retains source/diagnostic bytes by document/revision under mandatory writer
policy, bound native MERGE and complete original UUID/readback checks. The host
register_umf.py command invokes actual clean pinned UMF code first, then renews
installed owner/inherited permission admission and preserves original operation
handles. It does not allocate schema/catalog IDs or advance Truss acceptance.

Native schema_intake in the isolated runtime retains independently validated
v1/revision1 and explicitly corrected additive v3/revision3. One independent
query recovers both complete original source and actual diagnostic artifacts,
matching their SHA256 values. UMF complete interpretation remains false for both;
no flag was promoted. The v2 unknown optional token is not reinterpreted or
overwritten. Eighty-seven local checks pass. No graph publication/checkpoint or
retention/grant changes occurred. Next implement actual selected target semantic
admission/catalog identity custody and connect admitted schema barriers to
staged source consumption. Truss native acceptance/runtime/feed remains required.

## Executable selected semantic constraint checkpoint

StringRecordPolicy now enforces the actual interpreted singleton string Record
closure using complete explicit trusted type/property bindings. It rebuilds the
binding plan from retained UMF interpretation, refuses unresolved assertions and
invalid/ambiguous reserved IDs, and checks source and schema revision at ingestion.
Required properties must be present; absent-allowed permits absence without
coercing JSON null. Nulls, arrays, nonstrings and unknown executable property IDs
refuse. Original retained extension text is not silently interpreted or narrowed.

Ninety-one local checks pass against actual v3 interpretation, including preserved
wide unknown tokens and v2 unknown availability refusal. Test IDs17/23/24 are
explicit fixture bindings, not accepted native IDs. This implements host-side
constraints, not native schema/catalog acceptance or Truss engine enforcement.
Next persist and admit actual catalog identity/revision custody, extend selected
relationship/value bindings and connect the policy to schema-barriered source
consumption. The existing nine-event graph fixture has independent synthetic
schema/IDs and is not retroactively admitted by this string-only model. Truss
acceptance/runtime/feed and full native publication remain required. No Databricks
workload occurred this iteration.

## Original intake-to-meaning barrier checkpoint

The executable selected policy can now bind directly to a complete original
SchemaIntake: it reparses and compares the original artifact, requires identical
interpretation source SHA256 and uses its pinned validator/revision. It refuses
same-named but different source interpretation and tampered custody. Ninety-two
local checks pass. Fixture IDs remain explicitly unaccepted native bindings.

Truss inspection at958f0ee confirms source layout0.11 remains the original
1ac7cc82405ff581072d45ad586f54d8c48343eaecc7c011195535ed359e37f9
declaration, while its current implementation handoff requires protected complete
acceptance reports and installer readiness. It has no runtime source; private
native schema_head is still0. Do not invent an independent accepted Ashlar
catalog that conflicts with its authoritative IDs/revision boundary. Next
implement the protected Truss acceptance producer/report path and then consume
its admitted identity custody at Ashlar’s schema barrier; broader bindings and
mutation/feed/publication remain required. No Databricks workload this iteration.

## Native catalog candidate probe — 2026-10-08

The rollback-only probe now consumes actual pinned UMF v3 source and interpretation,
holds the installed native head at revision zero, captures all three global ID
high waters and applies the selected one-Record/two-string-property candidate.
Independent expected values cover exact original source/validation, lineage bytes
and generated digest, all definition columns and definition-source provenance.
Initial omitted provenance correctly fails native `type_def_definition_source_complete`;
corrected effects/readback pass and rollback restores zero definitions/reports and
one genesis revision. Original failed and successful observations are retained
under SPIKE-001-table-layout/out/native/truss_catalog_candidate_20261008*.

This closes a small native candidate-storage observation, not acceptance, revision
publication, protected-role closure, Truss runtime or the end-to-end acceptance.
Complete report construction/admission and protected persistence/ordinary-role
inventory remain the next producer work. Do not use the probe's tentative IDs
for Ashlar ingestion. All 97 local checks pass; no Databricks workload was run.

## Complete acceptance input custody — 2026-10-08

The native catalog candidate now retains the complete acceptance-input request:
exact document bytes/revision, original layout/acceptance/validator/support/UMF
and policy descriptor artifacts, absent binding and ordered transform inventory.
Profiles are explicitly **unregistered candidates**. Original raw transport and
full `truss-canonical/0.1.0` / `truss-acceptance-input/0.1.0` preimage round-trip
with candidate catalog effects. Original-schema shape validation passes; four
independent complete-wire vectors and drift/refusal controls verify the custody
implementation. All 102 local checks pass; one small isolated native rollback
probe passed and no Databricks workload ran.

This supplies report construction's full input custody. It does not close original
profile registration/recognition, authority, complete assertion inventory/actual
enforcement evidence, native report encoder/persistence or accepted head publication.
The head remains zero. Required next work remains the protected producer and its
full report/effect correspondence; no ingestion may treat these candidate IDs as
accepted. Receipts live in SPIKE-001-table-layout/out/native/truss_catalog_candidate_20261008_input_custody.

## Selected assertion inventory — 2026-10-08

Report-producer development now has eleven independently expected v3 source
assertions: Record kind/two members, plus both Fields' kind/scalarType/nullability/
cardinality. Exact original document bytes, qualified source pointers, profile
artifacts and original UMF intake/interpretation receipts remain retained. The
source's partial interpretation is preserved. All entries are **none/unqualified**;
no database/engine evidence is inferred from candidate storage or test success.
Original Truss entry shapes pass, unsupported/wrong-source/resource cases refuse,
and all106 local checks pass. No database/cloud workload ran in this iteration.

This is the explicitly selected unregistered assertion-source profile, not the
accepted catalog's complete enforcement report. Complete report assembly still
needs admitted profile/execution/origin context, full effect/count/diagnostic
correspondence and protected report/head persistence. Candidate inventories and
IDs still cannot authorize ingestion. Evidence: SPIKE-001-table-layout/out/truss-assertion-inventory-20261008.

## Initial report source/effect parts — 2026-10-08

Report production now composes actual original documents, complete native UMF
validation/warning custody, partial interpretation coverage, selected source
assertions and verified candidate type/property counts. The probe explicitly
observes complete additional key/relationship/endpoint/schema-change/journal/
object/edge counts rather than supplying unchecked zeros. Native original request,
source/validation, complete definition columns/lineage/provenance and semantic
fields must match before any parts are emitted. Unknown/missing effects, changed
original requests and numeric/boolean aliases refuse. Four original Truss report
fragment schemas pass; all109 local checks pass. One small isolated rollback probe
ran; no Databricks workload or committed catalog change occurred.

These are unregistered initial-candidate parts, deliberately lacking an accepted
report interface/revision. Full original execution/authority/origin/report/lifecycle
registration and complete protected persistence are still required before a
positive accepted head or ingestion authority. The goal's schema evolution, real
Truss mutation/feed and composed Ashlar publication/read remain unfinished.
Evidence: SPIKE-001-table-layout/out/native/truss_catalog_candidate_20261008_report_parts.

## Origin mapping and native capture — 2026-10-08

The candidate mapper losslessly preserves the acceptance canonical-tree origin
in the journal ExactValue carrier, including nested empty collections, literal
tag-looking objects and exact numeric text. Finite bytes/nodes/depth and malformed
value/refusal cases are enforced. It grants no authenticated role or registration.
The native rollback probe separately captures current_user/session_user and exact
xid8 text on the original held transaction; asserted `db_role=asserted-admin`
remains metadata and captured databaseRole is postgres. Both original and mapped
origin representations round-trip and match the original Truss origin fragments.
All113 local checks pass; one small PostgreSQL rollback probe passed and no cloud
workload ran.

Report-parts wire0.2 adds the checked origin fragments. The development capture
is direct isolated admin custody, not qualified ordinary-role/driver/authorization
or original installation/epoch registration. Complete report/context registration
and protected persistence remain required; the head stays zero and candidate IDs
cannot authorize ingestion. Evidence: SPIKE-001-table-layout/out/native/truss_catalog_candidate_20261008_origin_capture.

## Durable original profile custody — 2026-10-08

Six original candidate definitions and their selected original archive/producer
source bundle are now durably retained in isolated PostgreSQL. Identity/version
originals are immutable to the ordinary writer/reader roles; the private owner
function compares full definition and bundle bytes, refuses changed originals and
returns original native role/xid/timestamp on repeat. Actual binary/refusal/direct
DML/read-role schedules, a fresh-process repeat, independent reader equality and
native owner/function/privilege observations pass. All115 local checks pass; no
cloud workload ran. Owner/superuser and unqualified global/proxy paths remain
outside this narrowly observed ordinary-role scope.

This closes original byte custody, not executable profile registration, complete
implementation/dependency recognition, current acceptance authority or Truss ready
installation. No activation/admission flag exists; the head remains zero. Complete
report/context registration and protected persistence are still required before
accepted IDs can authorize ingestion. Evidence: SPIKE-001-table-layout/out/native/profile_custody_20261008
and profile_custody_replay_20261008.

## Source revision domain correction — 2026-10-08

The existing feed mapping assigns native Truss rev to Ashlar schema_revision.
Owner-issued UMF document revisions and accepted catalog revisions are independent
(e.g. owner revision3 accepted at catalog revision1). The selected policy now has
an explicit Truss catalog source path that keeps both domains and checks events
against the independently admitted positive native catalog revision. Genesis,
noncanonical/out-of-range revisions and owner-version substitution refuse. Raw
document sources retain their original default; other adapters may supply an
explicit admitted source revision. All116 local checks pass; no native/cloud run.

This prevents a future integration barrier mismatch, without manufacturing a
positive accepted head or ID authority. Complete report/context registration and
protected native acceptance/persistence remain unfinished. Existing immutable
profile custody is unchanged; changed implementations require explicit new
profile/version recognition rather than overwriting archived originals.

## Runnable local composition — 2026-10-08

`python3 tools/run_local_example.py` now composes original v3 UMF intake and
interpretation, selected string-record policy, bounded committed JSONL decoding,
whole-entity translation and complete graph apply/replay. Three transactions/four
events leave one updated Unicode-bearing object, one tombstone and four original
history entries; exact replay preserves complete state and retained numeric text.
The actual command passed with Python standard library only. No cloud/native
workload ran. Explicit fixture mappings are not accepted Truss authority; no
publication or ACK is issued. This provides a runnable local entry point while
protected Truss acceptance/mutations and composed Delta publication/read remain
required by the full goal. Truss HEAD f78785b still contains no runtime package.

## Retained source recovery reader — 2026-10-08

batch_from_row reconstructs a complete staged transaction from retained original
begin/event/commit bytes, reparses count/digest/cursor custody and compares every
field against the canonical original stage row. Closed shapes, finite artifact/
original byte/record limits, canonical base64 and exact metadata correspondence
refuse rehashed drift. The caller still owns native UUID/snapshot/authorization
and source ordering. No recovered batch authorizes publication or ACK.

All117 local checks pass. The local workflow now replays through serialized
retained rows rather than cached source objects. Three complete original
Databricks readbacks from source_stage_20261008 also reconstruct successfully;
partial projections are not complete custody. This reused archived native evidence,
not a fresh cloud run or restart/retention qualification. Native composition and
protected Truss acceptance/mutations remain required.

## Native local-fixture composition — 2026-10-08

run_native_example.py now connects the actual retained v3 UMF/interpretation and
selected host constraints to bounded original transaction recovery, whole-entity
SQL planning and durable original native effect submissions. The existing private
runtime received three transactions/four events. Final selected object version/
payload/retained bytes and 0edges/1tombstone/4history counts passed, with renewed
owner/inherited grant/target UUID observations. Fresh-process replay passed using
the same nine original mutation submissions/three plans; no replacement writes.
All118 local checks pass, including the user-facing local CLI. No scale/new compute.

Evidence: SPIKE-001-table-layout/out/native/local_example_20261008 and
local_example_replay_20261008. Journal exports preserve original effects/handles;
exports were taken after replay. This remains an explicit fixture-ID development
owner lane, not accepted Truss authority or remote/source fencing. Full-column
parity, active retention/pins, immutable publication, native resolver read and
Truss protected acceptance/mutations/feed remain required by the full goal.

## Complete native local-fixture effects — 2026-10-08

The independent source-to-carrier checker now verifies five exact-version native
inventories: final object_current (1row/17columns), initial object_current
(2rows/17columns), final edge_current (0rows/20columns), tombstone (1row/10columns)
and whole_source_history (4rows/6columns). It derives expectations directly from
original events rather than SQL planner rows. Exact schemas, SQL NULL versus text,
lookup identities, timestamp microseconds, source cursors/deliveries, raw event
bytes and complete multiset multiplicity pass. The immutable observed version
vector and source/checker/summary correspondence receipt are retained for subsequent
publication work. This was 24 read-only SQL statements on existing compute; no
writes, scale tests, retention changes or publication/ACK occurred.

Evidence: SPIKE-001-table-layout/out/native/local_example_full_parity_20261008.
The result proves complete observed fixture effects at those snapshots, not future
file availability, active pins, protocol/read policy or complete Truss authority.
Native publication still requires those independent obligations and original
source checkpoint binding. All118 local checks continue to pass.

## Native transaction adapter for read pins — 2026-10-08

The host psycopg transaction adapter now supplies the real PostgresPins interface:
one fresh authenticated non-autocommit connection throughout held custody,
bound named string parameters, bounded native row inventories, explicit commit/
rollback/close and rejection of nested/stale sessions. Factory and authorization/
retention policy remain explicit required ports; no permissive connection exists.

Actual PostgresPins.hold on the existing four recovery pins passed. A competing
ordinary writer release hit native lock_timeout while every read guard was held;
no pin was released. Actual ordinary reader role/PostgreSQL17.9/xid and complete
four-pin UUID/version/digest/active inventory are retained. Ended session refusal
and injected rollback/advisory-lock release pass with psycopg3.2.13. All118 local
checks remain green. No cloud query, new pin, cleanup or retention change occurred.
Evidence: SPIKE-001-table-layout/out/native/postgres_transactions_20261008.

This closes transaction execution wiring, not Delta file availability, maintenance
operator enforcement, registration admission or publication/read composition.
The existing recovery scope is used only to test the adapter; it is not the new
local-example publication scope and must not authorize that example's snapshots.

## Explicit multi-revision schema evolution — 2026-10-08

SchemaPolicies dispatches an explicitly supplied bounded source/revision inventory
without latest/default fallback. Complete apply and native SQL planning now refuse
replacement/deletion across schema revisions without explicit transition admission
against the prior entity and new change. Revision strings remain opaque; no numeric
ordering or compatibility is inferred. Exact delivery replay still invokes current
schema admission but never reexecutes a previously retained transition.

run_schema_evolution.py demonstrates v1-created objects and the explicitly admitted
v1-to-v3 fixture replacement adding caption, followed by deletion of the other
object under its original v1 schema. It retains both original schema revisions in
history and preserves completeInterpretation=false. The v1 interpretation was
obtained from the actual clean pinned UMF source/API, not synthesized. All122 local
checks pass, including unknown/unsupported revision, v1 new-field refusal, missing/
noncompleting transition, prior-state preservation and native-planner propagation.
No native/cloud workload ran this iteration. This is selected local fixture
schema-evolution wiring, not accepted native Truss IDs/catalog evolution or a
published evolved Delta stream; those remain required by the full goal.

## Installable candidate toolkit — 2026-10-08

pyproject.toml now builds ashlar-graph-toolkit0.1.0.dev0 with Python3.9+ declaration,
no runtime dependencies and the ashlar console command. Public imports expose
source/schema/apply/resolver/singleton components; native cloud/PG tooling remains
host-owned. The bounded inspect-source CLI retains original byte/cursor/digest
custody and fails incomplete transactions without ACK/publication.

An actual wheel built and installed offline in a fresh Python3.9.6 environment
outside the repository. Isolated public import, installed command original-byte
roundtrip (3transactions/4events), no runtime dependencies and all packaged Python
modules equal source passed. All124 local checks pass, including module CLI
roundtrip and truncated-first-transaction refusal. Evidence and original wheel SHA:
SPIKE-001-table-layout/out/local/toolkit_install_20261008. The wheel remains a
local development artifact, not a published release; this evidence covers that
original wheel's code and metadata before the subsequent documentation update.
No native/cloud workload ran. Truss acceptance/feed, qualified native providers,
retention and immutable publication/read remain required for the full goal.

## Ordered interval graph recovery — 2026-10-08

recover_whole_entity_state now rebuilds exact graph/current/history/tombstone/
delivery state from complete verified staged transaction rows under explicitly
supplied source scope, trusted prior state, both admitted checkpoint boundaries,
schema and transition policies. It refuses gaps, order/overlap/source mismatch,
duplicate batch identities, incomplete terminal progress and finite batch/event/
artifact budget overflow. It never sorts, guesses missing state, acknowledges a
source or repairs unresolved native effects. The ordered artifact digest frames
full original retained batch bytes with exact lengths.

The schema-evolution example now reconstructs from retained serialized source
rows starting at the empty initial state and matches its independently applied
state. A partial restart from an exact trusted prior/cursor also matches full
recovery. All128 local checks pass. No native/cloud workload ran. Native stage
UUID/snapshot/full-scope ordering and checkpoint authority are still caller
admission obligations, and original-handle native effect recovery stays separate.
This is necessary restart wiring, not a completed live publisher/Truss feed.

## PostgreSQL source-to-graph bridge — 2026-10-08

apply_outbox_transactions composes exact original committed-group custody and
whole-entity graph apply under explicit source/prior/checkpoint/schema/transition
admission. Native signed64 group positions drive page continuity and terminal
checkpoint, while each contained JSONL transaction retains its own zero-based
byte cursor. Scope/position/order/payload/duplicate identity and finite page/byte
budgets refuse; no sorting, position inference, publication or source ACK occurs.

All130 local checks pass. The v1-to-v3 fixture composes across independent
contained transactions and a partial page resume produces identical complete
state. Gaps/overlaps/scope drift/original-payload drift and JSONL byte offsets used
as native checkpoints refuse. No native/cloud workload ran. The prior existing
native outbox installation/reader evidence remains separately scoped; actual
protected source append through this bridge and Delta publisher/checkpoint
composition remain required. This is a PostgreSQL whole-entity outbox adapter,
not an alias for the unfinished Truss property-feed reconstruction.

## Live PostgreSQL schema-evolution source — 2026-10-08

run_outbox_example.py now appends the three original v1-to-v3 fixture transactions
through the existing protected ordinary outbox writer, reads exact committed
groups through PostgresOutbox on the real host transaction adapter/ordinary reader,
and composes them through explicit schema/transition policies. Native positions
3–5 and complete original payloads match the fixture. Independent contained byte
cursors reset to0; native page resume from3 produces the same complete graph
(1live object/4history/1tombstone), and reading after5 observes the source head.
Fresh-process exact repeat preserves all original positions/head/payloads.

All130 local checks pass. Evidence: SPIKE-001-table-layout/out/native/outbox_evolution_20261008.
Original outer positions/digests and complete inner stage rows are retained together.
Only three groups/four events were added to the isolated PG source; no cloud,
retention or source ACK occurred. This is the actual additional whole-entity source
boundary, not Truss property reconstruction or completed native Delta publication.
The factory authenticates the trusted isolated admin session into ordinary roles;
ordinary-role scope does not establish separate production identity/role fencing.

## Durable publication intent binds native source progress — 2026-10-08

publish_outbox_transaction now carries exact original native checkpoint text into
the publisher's immutable request/digest and all durable phases. It binds the
original source profile/feed/epoch/previous-position/position/payload bytes/batch
identity separately from the contained JSONL byte cursor. Phase readers support
this explicitly closed added carrier and revalidate the complete original inner
source artifact. Legacy JSONL-only request bytes/digests remain unchanged.

All133 local checks pass: exact committed replay avoids apply, same inner bytes
at a changed native group position conflict, malformed bindings refuse before
writer/effects, and rehashed phase requests cannot hide mismatched native source
identity/digest/profile. No native/cloud workload ran. CONTRACT-001 records the
handoff: backend descriptor progress/ACK admission must bind this original native
checkpoint; the new carrier grants no authority by itself. Qualified native
publisher/retention/read and Truss producer remain required for the full goal.

## Explicit SQL reader protocol boundary — 2026-10-08

ReaderProtocolProfile now requires explicit recognized reader/writer versions and
advertised table features. validate_protocol_detail refuses changed UUID/format,
unknown/noncanonical/numeric-alias versions, missing/malformed/duplicate feature
inventories and every unrecognized advertised feature. inspect_protocol provides
a fresh authenticated DESCRIBE DETAIL observation through the required executor;
no default compatible profile is inferred. The conservative table-level boundary
recognizes writer features too, avoiding a guess that unknown features are harmless.

Four original native observations from the full local-fixture parity receipts
match the explicitly selected reader3/writer7/seven-feature candidate SQL-reader
profile. Those same original receipts include successful exact-version full reads.
All135 local checks pass. Evidence: SPIKE-001-table-layout/out/local/reader_protocol_20261008.
No fresh native/cloud query ran; this evidence is archived correspondence only,
not a custom Delta file-reader claim, retained availability/pins or complete
publication admission. Snapshot/file/schema/authority checks remain independent
mandatory native policy obligations; the full goal remains unfinished.

## Whole-table pin exclusion for cleanup integration — 2026-10-08

The isolated native registry now provides assert_table_unpinned and the portable
PostgresTableGuard transaction/authorization port. Every active scope/authority/
version of the exact table UUID refuses whole-table cleanup; held SHARE custody
excludes competing registration/release. It submits no native cleanup and supplies
no permissive authority policy.

Actual ordinary maintenance-role checks passed: the older exact-version guard
allows an unrelated version, while the new whole-table guard refuses the same
pinned table; a held guard for the unregistered private example table blocks a
competing ordinary registration, whose control transaction rolls back. Native
source/body/security-setting correspondence is retained. All135 local checks pass.
Evidence: SPIKE-001-table-layout/out/native/table_guard_20261008. No pins added or
released, Delta query/cleanup, TTL or retention setting changed.

Current physical-target/operator closure, remote lost-lock/outcome quarantine and
complete file availability remain unfinished. The new guard is an integration
boundary, not evidence that every Delta maintenance path already participates;
published/readable pin admission still requires those independent obligations.

## Live PostgreSQL-to-Delta schema evolution — 2026-10-08

The native example now selects the actual committed PostgreSQL groups3–5,
verifies every original transaction against the fixed UMF evolution source and
runs explicit multi-revision/transition admission before planning Delta writes.
A separate runtime_outbox namespace in the existing private catalog contains the
unchanged eight-carrier candidate baseline. No existing example tables changed.

Three groups/four events applied: selected final object payload/version, empty
edge inventory, one tombstone and four history rows passed. Complete source rows
and native checkpoint text are retained in immutable original source_intent rows
before native effects. Fresh-process replay reuses the same nine mutation
submissions/three effect plans/three original source intents; complete original
journals and selected summaries remain equal. All135 local checks pass. Existing
warehouse only; no scale, new compute, grants or retention changes.

Evidence: SPIKE-001-table-layout/out/native/outbox_setup_20261008,
outbox_delta_20261008 and outbox_delta_replay_20261008. Keep original journals
/private/tmp/ashlar-outbox-setup-20261008.sqlite and
/private/tmp/ashlar-outbox-graph-20261008.sqlite for recovery. This is a live
additional-source-to-Delta development stream with explicit fixture IDs, not
accepted Truss catalog/feed authority. Full-column evolved-table parity, qualified
remote fencing/retention, durable publication and native resolver/read/source ACK
remain required. No publication or acknowledgement was issued.

## Complete evolved outbox Delta snapshot validation — 2026-10-08

The independent native checker now selects the live outbox fixture and its
separate runtime_outbox installation. It reconstructs expected carriers directly
from original source events with independently zero-based contained transaction
cursors, rather than SQL planner output or globally concatenated offsets. Five
exact snapshots pass: final object (1row/17columns), initial v1 objects
(2rows/17columns), empty edge (0rows/20columns), tombstone (1row/10columns) and
complete original history (4rows/6columns). Original owner schema revisions1/3,
source feed/epoch, exact JSON text, SQL NULL, native numeric text, lookup identities,
materialization microseconds and event/delivery custody all agree.

The complete version vector/source SHA and 24 original read-only SQL receipts are
retained at SPIKE-001-table-layout/out/native/outbox_delta_full_parity_20261008.
All135 local checks pass. No data write, acknowledgement, publication or retention
change occurred. This completes observed fixture effect parity, not future file
availability, active publication pins, full maintenance/operator containment or
native Truss authority. Those remain predecessors of live publication/read.

## Durable cleanup quarantine survives host connection close — 2026-10-08

The isolated pin registry now persists original cleanup intent before remote work
can begin and refuses new ordinary pins for every alias/version/scope of a pending
UUID. Marker creation serializes with pin registration/release and refuses active
UUID pins. Exact pending repeat preserves originals; closed operations cannot
reopen. Only a distinct recovery role may retain immutable terminal bytes and
close custody. CleanupQuarantine requires explicit scope/authority and independent
actual native outcome verification; no default terminal verifier exists.

Six actual native metadata-only controls pass, including committed marker custody
across closed connections, alias registration refusal, maintenance-role close
denial and exact original/terminal readback. The post-close registration control
rolls back, so no pin was added/released. All137 local checks pass, including
unverified terminal refusal before SQL and binary intent custody. Evidence:
SPIKE-001-table-layout/out/native/cleanup_quarantine_20261008. No Delta query,
cleanup, TTL or retention setting change occurred.

This closes persistent uncertainty custody, not actual remote cleanup termination,
all-operator/physical-target closure or retained snapshot availability. The one
closed native operation is explicitly metadata-only with no remote action started;
it must not qualify a real Delta cleanup receipt. Live publisher/read admission
and protected Truss acceptance/feed remain unfinished.

## Descriptor-to-source checkpoint binding — 2026-10-08

bind_outbox_descriptor now revalidates full original request/source custody and
checks independently admitted publication identity, exact schema inventory,
validation request digest and the complete native checkpoint in descriptor feed
progress. Other original feed progress is preserved. JSON numeric positions,
changed epoch/payload/batch/schema/ID/intent refuse. All139 local checks pass; no
native/cloud workload ran. This provides the required correspondence handoff for
native ACK, not manifest/effect/pin/source authorization itself.

Truss HEAD21a04f3 was rechecked: still no src/packages/package runtime. Its current
CH-02/03 body/bootstrap dependencies and CH-05 package work remain open. That
absence is not a completed Truss integration or a goal-completion basis. Live
Ashlar native publication/read admission and protected Truss acceptance/feed
remain required by the full objective.

## Pin admission checks durable cleanup uncertainty — 2026-10-08

PostgresPins now checks every distinct UUID for pending cleanup under its native
transaction during registration and before/after held reads. Registration checks
follow the existing exclusive registration lock, avoiding a SHARE-to-exclusive
upgrade race between competing registrars. The UUID-wide guard retains SHARE
custody through reads and rejects unresolved cleanup across aliases.

All140 local checks pass. Actual PostgresPins transaction controls continue to
pass; a separate native metadata-only pending/closed control refuses/admitted
availability as expected. Evidence: SPIKE-001-table-layout/out/native/pin_availability_20261008.
The earlier transaction receipt was restored byte-for-byte after saving the new
observation separately; existing native evidence is preserved. No cloud query,
remote cleanup, retained pin changes, TTL or retention setting changes occurred.
UUID availability here means no pending cleanup, not Delta file availability.
Native publication/read and protected Truss acceptance/feed remain unfinished.

## Runnable native singleton diagnostic — 2026-10-08

The outbox-streamed development object was read directly at Delta version 5
through a new host command, with parameterized signed64 identity, exact lookup
hash, LIMIT 2 ambiguity refusal, full native response parsing and before/after UUID
checks. Three read-only statements on the existing warehouse returned object 1,
entity version 2, schema revision 3, updated label, Unicode caption and unchanged
opaque extension integer. Original receipts: out/native/singleton_diagnostic_20261008.
No write, pin, publication or source ACK occurred. The command is documented in
examples/end-to-end/README.md. Protected Truss acceptance/feed and composed
manifest/resolver/pin admission remain required for the full end-to-end goal.

## Exact manifest-to-pin resolver binding — 2026-10-08

manifest_pin_vector binds every original manifest field (including unchanged JSON
strings and clock) to the complete independently admitted table UUID/version
inventory and manifest scope. bind_manifest_pins verifies both parsed resolver
fields and the complete held vector against that original custody. These functions
provide correspondence for mandatory admission policies, not authorization or
retained-file proof. NativeBackend now projects recorded_at through unix_micros
to canonical text, matching immutable writer/readback custody without a host Date
conversion. Five additional focused checks cover full UUID inventory, changed
original bytes, scope/version substitution, parsed descriptor substitution, nested
progress and composition with singleton reads. Altered custody refuses before
object SQL. All145 local checks pass; no cloud workload occurred.

Truss HEAD4a8d8d6 was inspected: still no src/packages runtime paths. Its concurrent
spec work is preserved. Actual protected acceptance/feed and native publication
admission/retention/read composition remain open under the full goal.

## Native manifest reader transport verified — 2026-10-08

check_native_manifest_read.py now exercises the actual NativeBackend against the
private runtime_outbox manifest UUID. Its absent-publication control refuses
through resolve_publication before descriptor/snapshot admission. A separate
parameterized SQL expression round-trips 1791450000123456 microseconds to exact
STRING, verifying subsecond custody through the real SQLResult transport. Four
read-only statements ran on the existing warehouse. Original receipts are retained
at out/native/manifest_read_20261008. No row was inserted, pin registered, source
acknowledged or retention setting changed. This verifies transport and refusal,
not a positive publication or retained-file admission. The full objective remains
open at native publication/retention and protected Truss acceptance/feed.

## Runnable additional CSV source — 2026-10-08

csv_batches adds an incrementally consumed bounded UTF-8 single-line CSV source
profile. Explicit source/epoch/schema/type/property mappings feed the existing
whole-entity transaction adapter. Original header and row bytes are retained in
the delivery identity; unmapped column strings remain in the entity carrier.
The runnable four-row example uses actual UMF v3 intake and selected constraints,
creates/replaces/deletes, preserves unknown content, and replays through original
staging custody without changing graph state. The first generated batch consumes
only header plus first row. Documentation specifies immutable epoch custody,
separate outer ordinal versus inner offset, bounds, unsupported semantics and no
ACK/native publication. All149 local checks pass. No cloud workload occurred.

This completes a runnable local additional-source path, not its native published
composition. Protected Truss acceptance/feed, native publication admission/retention
and resolver-backed publication singleton use remain required by the full goal.

## CSV outer checkpoint publication-intent binding — 2026-10-08

Publisher checkpoint admission now dispatches only explicit outbox and single-line
CSV profiles. csv_checkpoint binds ordinal previous/position, feed/epoch/batch
and the ordered original header/row digest from retained delivery custody; inner
JSONL offsets remain distinct. Retained phase request validation rechecks this
correspondence, and bind_source_descriptor provides descriptor/source matching.
The existing bind_outbox_descriptor rejects CSV to preserve its qualified scope.
All152 local checks pass, including stable original attempt replay, changed outer
positions/epoch/digest/profile refusal before writer and rehashed request refusal.
No cloud workload or source ACK occurred. File identity/semantic mapping authority
and positive native publication/retention remain external mandatory admission,
not claims established by digest correspondence. The full goal remains active.

## CSV source applied to native managed Delta — 2026-10-08

run_native_example --source csv now connects the bounded additional-source
adapter to the actual native graph SQL/effect journal. A separate runtime_csv
namespace was installed in the existing private catalog with eight candidate
carriers; no compute/grant/retention setting changed. Four original checkpoint
intents were retained before twelve graph mutations. The selected final object
has version 2, quoted label/Unicode caption and the large unknown value retained
as its original CSV string. Edge count 0, tombstone count 1 and history count 4
were verified. Fresh-process replay preserved complete original mutation/effect/
source journals and final summary exactly. Evidence: out/native/csv_setup_20261008,
csv_apply_20261008 and csv_replay_20261008. All152 local checks pass.

This is real additional-source managed Delta ingestion/replay, not full-column
parity, remote fencing, native publication/pin/retention admission or source ACK.
The full goal still requires protected Truss acceptance/feed and composed native
publication/resolver singleton use.

## Automatic native maintenance discovered — 2026-10-08

Fresh UC metadata API observations show effective predictive optimization ENABLE
inherited from metastore_centralus for the private catalog, runtime_csv schema and
all four graph/history carriers. Table retention properties are absent from those
API records; absence is not a disabled setting. Evidence and an unexecuted scoped
policy-change proposal are retained in out/native/csv_retention_metadata_20261008.
No SQL workload or setting change occurred. This is a specific automatic operator
outside the PostgreSQL guards, so current pin presence cannot admit protected
native publication retention.

The reviewable proposal disables predictive optimization only on runtime_csv.
Executing it requires maintenance-policy authorization, then verification of fresh
effective flags and safe terminal custody for already submitted operations. It
does not alone prove log/data availability, all-operator closure or authorize
publication. Other schema/canonical Truss work can continue independently while
this policy decision remains pending. The full goal is not complete.

## Full native CSV effect parity — 2026-10-08

The original CSV now supplies an independent full-column oracle for the managed
Delta fixture. No CSV adapter, apply planner or generated SQL rows are used to
derive expected events/effects. Five exact snapshots pass schema/value/multiplicity
comparison: final object, initial object, empty edges, tombstone and all four
history rows. Original native SQL responses also pass the shared complete-result
parser locally. Evidence: out/native/csv_delta_full_parity_20261008. The checker
now uses that complete parser for subsequent observations. All152 local checks
pass. Only bounded read-only fixture scans occurred; maintenance settings remain
unchanged pending the earlier explicit policy authorization.

This qualifies observed ingestion effects and exact-version readability, not
future retention, active publication pins, native fencing or protected Truss IDs.
The full goal remains open at Truss acceptance/feed and native publication/read
admission; no source ACK was issued.

## Installed CSV source workflow — 2026-10-08

ashlar inspect-csv now accepts arbitrary bounded stdin with explicit source/epoch/
schema/type/property mapping and emits recoverable original batch rows plus outer
checkpoint text. Duplicate/non-string mappings refuse before output. All154 local
checks pass. A fresh wheel was built and installed without runtime dependencies
into a new isolated environment; its actual ashlar entrypoint ran outside the
checkout. All packaged source bytes match current source. Four emitted custody
rows/checkpoints recovered and applied under the actual selected UMF policy,
leaving one object, one tombstone and four history records. Receipt: out/csv-cli-installation.json.
Truss HEADadd84ce remains specification/source review without src/packages runtime.
No maintenance setting, cloud workload or publication/ACK changed. The earlier
maintenance-policy authorization remains pending; native publication and protected
Truss integration remain required by the full goal.

## CSV original semantic correspondence — 2026-10-08

validate_csv_batch now reconstructs complete producer bytes from retained original
CSV header/row/ordinal under explicit independently admitted feed/epoch/source/
schema/type and ordered property mapping. It compares the full SourceBatch, not
only source digests. The CLI, local and native example runners invoke it before
use. Three new checks cover every ordinal, fully rehashed property substitution
and source/schema/type/mapping substitution. All157 local checks pass; the local
UMF-backed four-event replay remains unchanged. No cloud workload, policy setting
or source ACK occurred. This supplies semantic correspondence to mandatory source
admission; it does not prove file epoch authority or catalog/retention admission.
Native publication and protected Truss integration remain required by the goal.

## Automatic maintenance history inspected — 2026-10-08

The real system.storage.predictive_optimization_operations_history schema is
readable. A second bound query scoped only to ashlar_e2e_private_20261008.runtime_csv
returned zero recorded operations, with a 128-row refusal bound and complete
original response verification. Evidence: out/native/automatic_maintenance_history_20261008.
The native column comments describe SUCCESSFUL/FAILED statuses; this history
observation does not establish complete queued/running work custody. Do not use
zero rows as safe-terminal or no-maintenance proof. Two small read-only SQL
observations ran; no settings, source ACK, pins or publication changed.

The earlier schema-only predictive-optimization disable proposal remains pending
authorization. Even after that change, a trusted scheduling/termination observation
or other qualified containment is required for previously submitted operations;
this completed-operation table alone cannot provide it. Protected Truss runtime
and native publication/read admission remain required by the full goal.

## Truss complete-feed fragment assembly — 2026-10-08

The new truss_feed component implements bounded original custody assembly against
Truss's reviewed complete-feed/0.1 and ExactArtifact proposals at HEADadd84ce.
Canonical archive spelling and domain-framed manifest digest are distinct from
artifact byte hash; complete membership payload hashes and original manifest order
are preserved across fragments. Missing/foreign/tampered members and unadmitted
source context refuse. Exact fragment duplicates preserve ordered payloads. Both
original manifest and all original fragment bytes are returned defensively as
immutable bytes/tuples. All160 local checks pass; no native workload occurred.
Source paths/hashes: out/truss-feed-assembly-source-review.json.

Mandatory host admission must still independently prove native complete committed
membership, original registered definitions/order, prerequisites, safe watermark,
retention/worker custody, authorization and payload interpretation. The component
does not issue ACK, reconstruct property state, install Truss or support 0.2
transition manifests. Full native Truss integration and publication/read admission
remain open. Maintenance settings remain unchanged pending prior authorization.
