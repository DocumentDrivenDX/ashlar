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

## Original Truss feed vector correspondence — 2026-10-08

The exact unmodified upstream feed-manifest canonical vector file is now archived
as a test fixture. All four original canonical trees/framed preimages/digests
match; the original complete 0.1 archive reaches mandatory native admission
unchanged, whose test policy deliberately refuses unadopted profiles. Three
original 0.2 wires refuse before policy. Digest-excluded preimages and unframed
byte hashes cannot substitute for complete wire archives. The assembler now also
verifies closed context/profile pins and exact revision/configuration prerequisite
artifact bytes before policy; a recomputed manifest hash cannot hide corrupted
prerequisite custody. All165 local checks pass; no native workload ran. Receipt:
out/truss-feed-original-vector-review.json. Upstream vectors remain unadopted and
synthetic; this is byte correspondence, not full source semantics or native runtime
qualification. The full goal and pending maintenance-policy decision remain open.

## Native automatic-maintenance exclusion check — 2026-10-08

validate_predictive_optimization_disabled makes the discovered deployment gap an
explicit reusable refusal check. Explicit DISABLE or INHERIT with effective
DISABLE and original inheritance source can pass this narrow scheduling check;
enabled/missing/unknown/contradictory observations refuse. The native metadata
inspection tool applies it to fresh observations. Local application against all
six original private-deployment records refused enabled automatic maintenance;
receipt: out/native/csv_retention_metadata_20261008/admission-refusal.json.
All167 local checks pass. No new cloud workload or setting change occurred.

The check does not establish full retention admission, current state from archived
records, outstanding operation termination, all-operator fencing, native file/log
availability or active publication pins. The pending maintenance-policy proposal
remains unexecuted. Native Truss integration and positive publication/resolver
composition remain required by the active full goal.

## Owner-selected finite publication readability — 2026-10-08

The owner chose to retain predictive optimization and make readability depend on
its effective retention configuration. This supersedes the blanket DISABLE gate
and its pending change request; no maintenance-policy alteration is needed for
that gate. The exclusion helper was replaced with bounded fixed-interval parsing,
immutable per-snapshot expiry reporting and current-configuration validation.
Original expiry uses native snapshot commit time, shorter data/log duration and
an explicit margin; fresh shorter settings tighten it, longer settings cannot
renew it. Complete UUID/version/configuration correspondence and trusted clocks
are required. Singleton admission now repeats after execution before returning.
All171 local checks pass, including exact deadline, tightened config, no original
extension, wrong UUID/version, unknown/future configuration and mid-query expiry
refusal. No cloud workload or setting changes occurred. Governing CONTRACT-004
and the user-facing workflow now describe this finite profile. Existing historical
receipts remain unchanged. Native configuration/default/availability admission and
Truss runtime integration are still required for the full end-to-end demonstration.


## First Truss runtime package — 2026-10-08

The isolated Truss candidate `codex/ashlar-runtime` is committed and pushed at
`7a77233`, based on the owner-reviewed source packet `64d8469`. Its private
experimental ESM package implements the governed inert reference assembly.
Strict TypeScript 7.0.2 build and three Bun tests (18 assertions) pass. A clean
packed consumer compiles and executes; the public declaration closure contains
37 files and one canonical transaction brand, and an independently counterfeit
brand is rejected. Construction, selection, observation and disposal make zero
native calls. Configured readiness remains unverified; native capabilities are
unavailable. Evidence is retained in the Truss candidate under
`docs/helix/04-build/evidence/inert-assembly/` and package `dist/`.

This starts the package implementation without changing the Truss owner's
planning checkout. It does not install the selected 0.12 source-review layout,
interpret UMF, mutate data, deliver a feed or qualify Node/browser/native driver
support. Required native bodies, security dependencies and the complete bootstrap
qualification remain unfinished. The full Ashlar end-to-end goal remains active.

UMF owns the reusable Delta DDL generator at `fac1497a`, pushed on
`codex/delta-ddl-generator` and not yet merged into UMF's default branch. Ashlar's
eight runtime physical models already generate their checked installer input
through that version. No further database workload or settings change occurred
in this package iteration.


## Truss native vector decoder prerequisite — 2026-10-08

Truss candidate `codex/ashlar-runtime` is pushed at `889bfd0`. It implements
CONTRACT-008's bounded canonical OID/int2 native-output vector grammar, preserving
original text/order and using exact integer domain checks. All eight original
independent vectors and additional malformed/resource controls pass: five total
package tests, 45 assertions. Strict TypeScript build and clean packed consumer
checks pass. The retained native-vector receipt records exact original SQL/stdout
and decoder correspondence for three constant observations on existing local
PostgreSQL 17.9, including empty OID bounds `[0:-1]`. No data mutations, cloud
workload or settings changes occurred.

This is a concrete bootstrap decoder dependency, not complete native inventory
or installed Truss. Field-specific index/signature semantics, complete native
array/JSON/dimension correspondence, required routine bodies/security and
bootstrap/feed qualification remain unfinished. The end-to-end goal stays active.


## Truss native array decoder prerequisite — 2026-10-08

Truss candidate `codex/ashlar-runtime` is pushed at `1b64e6a`. Its ordinary
text-array decoder preserves original text, native NULL/empty/null-element
distinctions, quoted/escaped text, ordered rectangular nested shape and exact
signed native bounds. It checks supplied dimensions and byte/node/depth limits.
All eight original independent array vectors and additional controls pass; the
combined package suite has seven tests and 88 assertions. Strict build and clean
packed consumer checks pass. One small read-only local PostgreSQL 17.9 query
retains seven original text/dimensions/JSON observations, all matching decoded
elements exactly. Native evidence is in the candidate's inert-assembly directory.

No cloud workload, mutations or setting changes occurred. Type/field-specific
admission, ACL authority, aggregate native collector custody, complete bootstrap
bodies/security and actual installed Truss/feed remain unfinished. This completes
a decoder dependency and leaves the full end-to-end goal active.


## Truss trigger argument decoder prerequisite — 2026-10-08

Truss candidate `codex/ashlar-runtime` is pushed at `f667ee2`. CONTRACT-008's
UTF8 trigger argument decoder preserves original lowercase hex/count/byte length
and ordered strings, including empty strings and BOM data. Exact native-int2
count, allocation bounds, NUL framing and strict UTF8 are checked. All twelve
original independent vectors plus additional controls pass; the package suite
has nine tests and 116 assertions. Strict build and clean packed consumer pass.
One temporary local PostgreSQL 17.9 UTF8 trigger supplied actual pg_trigger
bytes/count; decoded arguments match the original supplied arguments. SQL/stdout
are retained in the candidate's native-trigger receipt. The entire transaction
was rolled back, leaving no installed probe or Truss trigger.

No cloud workload or configuration changes occurred. Complete native trigger
definition/WHEN/dependency admission, full bootstrap bodies/security, installed
Truss and streaming remain unfinished. The end-to-end goal remains active.


## Actual Truss routine carrier composition — 2026-10-08

The Truss runtime candidate now composes its native decoders against the original
CONTRACT-008 routine observation query. One temporary PostgreSQL 17.9 routine
supplies actual catalog carriers: input count/zero-based vector bounds and
text-array names/modes/settings agree with original same-row JSON projections.
The complete original catalog row remains opaque text, preserving unknown
content and avoiding numeric reinterpretation. The exact original query hash,
executed SQL/stdout and decoded values are retained in `native-routine.json`
under the candidate's inert-assembly evidence. The entire native probe rolled
back. Eleven package tests, 124 assertions, strict build and packed consumer pass.

This composes carrier prerequisites rather than installing Truss. Full routine
field/type/ACL/dependency/security admission and aggregate collector resource/cut
custody remain required. No cloud workload, persistent probe or settings changes
occurred. Native bootstrap and actual Truss streaming are still unfinished; the
full end-to-end goal remains active.


## Shared Truss catalog decode budget — 2026-10-08

The runtime candidate now accepts one caller-owned aggregate input-byte/node
ledger across vector/array codecs and routine original-row/JSON projections.
Reservations remain consumed; exhaustion is sticky and later fields refuse.
Tests show individually admissible fields exceeding their combined allowance
and shared-node refusal. Fourteen tests, 134 assertions, strict build and clean
packed consumer pass. No native/cloud workload or configuration change occurred.

This closes one accounting gap without claiming complete collector resource
control: total heap, dimensions/hex/transport buffers, work/deadline/cancellation
and native containment remain unfinished, as do full bootstrap/security and
actual Truss streaming. Callers must supply the shared ledger to use aggregate
accounting. The full end-to-end goal remains active.


## Native Bun execution foundation — 2026-10-08

The Truss runtime candidate now has actual Bun 1.4.2/PostgreSQL 17.9 direct
unprepared execution evidence. The small local probe verifies explicit exact
text transport for large integer/decimal/timestamp values, unchanged JSON text
parameters and SQL NULL; native SERIALIZABLE/read-only settings and connection
affinity; callback rollback independently observed through absence of its
temporary table; and savepoint rollback preserving earlier work. Evidence and
reproduction are under `inert-assembly/bun-execution.json` and
`scripts/check-bun-execution.ts`. No persistent table, cloud workload or setting
change remains. Credentials stay memory-only.

This resolves the immediate direct-driver viability question for the execution
adapter. It does not establish Executor conformance, caller adoption, cancellation
cleanup, unknown COMMIT/quarantine/recovery, prepared/pooler/Node support or native
Truss bootstrap. Implement the qualified execution boundary next; actual Truss
streaming and the full end-to-end goal remain unfinished.


## Truss engine-owned executor composition — 2026-10-08

The runtime candidate implements the canonical engine-owned executor over
mandatory native connection ports: affine/lifetime-checked handles, serialized
statements, original-entry savepoints, text/null result copying and explicit
commit/rollback settlement. Unknown COMMIT quarantines original custody without
release or guessed rollback. Four controlled-port tests supplement the package
suite: eighteen tests, 146 assertions; strict build and packed consumer pass.
No native/cloud workload or settings change occurred this iteration.

Concrete Bun port integration, caller adoption/cancellation, parameter domains,
SQLSTATE mapping, full native result/resource admission and shared public-operation
arbitration remain unfinished. Controlled port promises are not native confirmation;
no complete Executor conformance or installed Truss is claimed. Native bootstrap
and streaming remain open under the unchanged full end-to-end goal.


## Actual native PostgreSQL executor bridge — 2026-10-08

The Truss runtime candidate now connects its engine-owned executor to pinned
pg 8.16.3 in a separate host package. Native text-format parsers preserve stored
integer/decimal/temporal/JSON values as text; ordered native descriptions preserve
duplicate names and empty-result columns. The actual read-only PostgreSQL 17.9
probe through the executor verifies exact large numeric values, columns, savepoint
operations and confirmed commit. This replaces inferring column metadata from
Bun ordered results, which the native inspection found insufficient.

Host strict typechecking, eighteen package tests/146 assertions, portable build
and packed consumer pass. Compiler path resolution was corrected after a local
build-path failure. Native evidence is `inert-assembly/pg-executor.json`. No
persistent tables, cloud workload or settings changes occurred. The host package
is source-only; caller adoption/cancellation, native uncertainty settlement, full
parameter/resource/error/current-operation admission and complete Truss bootstrap
remain unfinished. Actual Truss streaming and the full goal remain open.


## Native executor writes and bootstrap DDL — 2026-10-08

The actual pg executor now supports known rowless DDL command tags with zero
affected data rows; unknown absent counts remain refused. This fixes temporary
CREATE/DROP execution needed for later bootstrap. Native checks verify exact
parameterized bigint/JSON writes, uniqueness-error containment through the
original savepoint preserving earlier work, and full callback rollback independently
observed in a following transaction. All native probe tables are temporary and
removed or rolled back. Carrier domains validate before native dispatch while
original JSON text remains unchanged. Nineteen tests/154 assertions, strict host
typecheck, portable build and packed consumer pass; final native probe also passes.

No cloud workload or settings change occurred. Complete error/resource admission,
caller adoption/cancellation and uncertain native settlement remain unfinished,
along with full Truss bootstrap and actual feed integration. Goal remains active.


## Native Truss deadlock retry settlement — 2026-10-08

The executor preserves statement SQLSTATE 40001/40P01 as whole-transaction retry.
Savepoint rollback cannot clear that state; outer confirmed rollback/release
returns the original retry failure without automatic replay or commit. Twenty
tests/164 assertions, strict host typecheck, portable build and packed consumer
pass. The actual PostgreSQL probe produces one 40P01 victim with confirmed outer
rollback and one committed peer using opposing transaction-scoped advisory locks.
Initial runs hit 55P03 under the one-second lock timeout; both rolled back. The
successful probe uses a three-second transaction-local lock limit and retains the
five-second statement limit. No deployment setting, table, persistent lock or
cloud workload remains. Original final outcomes are retained in pg-executor.json.

COMMIT-phase error classification, cancellation/adoption and complete native
recovery/resource/bootstrap admission remain unfinished. Actual Truss streaming
and the unchanged full end-to-end goal remain open.


## Confirmed Truss native COMMIT rejection — 2026-10-08

The native commit port distinguishes rejected from uncertain only after actual
pg server DatabaseError classification and confirmed original-connection rollback.
The executor withholds callback data, releases confirmed rejected resources and
returns selected whole-transaction retry or no-retry constraint failure. Unknown
transport/error/rollback still quarantines original custody without guessed replay.
Twenty-one tests/168 assertions, strict host typecheck, build and packed consumer
pass. An actual temporary deferred-FK transaction rejects COMMIT with 23503,
confirms rollback, and independently observes both tables absent afterward.
No persistent tables or cloud/deployment settings changes occur. Evidence is
retained in pg-executor.json.

Native commit-time serialization, lost-transport containment/recovery, caller
adoption/cancellation, complete resources and full bootstrap remain unfinished.
Actual Truss feed integration and the full end-to-end goal remain open.


## Built Truss PostgreSQL runtime package — 2026-10-08

The separate host package now builds public ESM/declaration exports against
canonical named Truss types. Dependencies/workspace links are locked; private
source-relative declaration paths refuse. After correcting an initial missing
root workspace link, the native executor probe passes through both built public
package imports, including exact cells/columns, writes, rollback, deadlock and
deferred-COMMIT settlement. Build evidence hashes are retained in the host dist.

This supersedes source-only delivery status but does not establish published
release, clean packed-host consumption, Node/pooler or complete native bootstrap.
Required routine bodies still need protected original-operation producer/security
composition; observation SQL cannot substitute. Existing full bootstrap/feed
readiness remains unavailable. No persistent tables, cloud workload or deployment
settings change occurred; the full end-to-end goal remains active.


## Clean packed Truss runtime consumer — 2026-10-08

Both built Truss runtime artifacts now typecheck and execute from a fresh external
consumer with no repository source links. Actual archives carry public ESM/types;
the host's staged workspace dependency becomes the exact candidate version and
an explicit consumer override supplies the unreleased portable archive. An initial
no-override attempt returned npm 404 for that unpublished version; this does not
claim released-package support. Canonical transaction type composition passes
without brand repair and retains one declaration in the full portable closure.
Construction/shutdown acquire no connection. Exact consumer/archive/manifest/lock
provenance is retained in packed-host-check.json. The portable checker’s compiler
path is also corrected and passes with its default invocation.

No native/cloud workload or deployment change occurred. This closes the clean
packed-consumer gap; publication, full native bootstrap, protected routine bodies,
adoption/cancellation/recovery and actual Truss streaming remain unfinished. The
unchanged full end-to-end goal remains active.


## Original Truss operation-registry decoder — 2026-10-08

The runtime now validates the original sixteen-column registry observation before
using it: exact descriptor/count/xid correspondence, unique identities, native
integer bounds, phase/generation/null rules and immutable original hex fields.
Twenty-three tests/181 assertions, package builds and packed consumers pass.
A temporary copy of the original table with actual native xid and opaque fixture
bytes passes the original query/decoder through the built executor. Original
DDL/query hashes and results are retained in pg-executor.json; the fixture is
dropped before commit. A source-comment semicolon extraction mistake initially
refused and rolled back; exact known SELECT locators corrected the probe.

This is a required observer-body dependency, not original protected admission:
fixture bytes do not qualify context/effect/group meaning or roles. Full native
producer/security/body composition, bootstrap and streaming remain unfinished.
No partial selected layout, persistent tables, cloud workload or deployment
settings change occurs. The full end-to-end goal remains active.


## Truss coordination and savepoint lifetime correction — 2026-10-08

The authorized Truss Impl chat is reviewing the candidate against OC01–OC07/E06
and the protected producer/security chain; its planning checkout remains unchanged.
The review is live, not completed. Initial findings identify missing shared
operation arbitration and the selected predecode/native-protocol capture path.
Existing public-driver observations do not qualify those stronger claims.

A concrete executor lifetime bug is fixed: released/foreign savepoint rollback
or release now returns invalid_transaction before native calls and does not
poison otherwise valid outer work. A focused test verifies both refusals issue
no SQL and a later valid read/commit succeeds. Twenty-four tests/188 assertions,
both builds and the portable packed consumer pass. No new native/cloud workload
or settings change occurred. Full protected producers, bootstrap and actual
Truss streaming remain unfinished under the active full goal.


## Original Truss protocol-frame prerequisite — 2026-10-08

Truss Impl completed candidate design review at 9665a4d on its separate branch,
identifying missing E06 and predecode producer integration and the full protected
chain. The runtime now adds a bounded T/D/C/Z frame decoder preserving original
hex, ordered/duplicate column descriptions, raw value/NULL bytes, exact command
count text and transaction status. Counts beyond host precision are retained as
text. Twenty-six tests/197 assertions, host build and packed consumer pass.
A single actual PostgreSQL 17.9 SELECT through pg 8.16.3 supplies original frames
matching independently expected descriptors/data/command/status; evidence is
retained in native-wire.json. No mutations/cloud/deployment changes occurred.

The probe captures after the existing driver parser, so predecode ingress/heap/
transport enforcement and original issuer/epoch/cut authority remain unfinished.
The frozen reviewed pg 8.23.0/pg-protocol 1.16.0 profile is not adopted by this
component. Full arbitration/protected native producers, bootstrap and Truss
streaming remain open under the unchanged active end-to-end goal.


## Incremental Truss pre-parser component — 2026-10-08

The host runtime now admits fragmented original response frames before parser
forwarding, with bounded advertised frame size, total delivered bytes/frame count
and DataRow descriptor/strict UTF8 correspondence. Oversized header and invalid
text controls prevent forwarding, and refusal is sticky. Twenty-nine tests/208
assertions, host build and packed consumer pass. The one-SELECT native probe now
replaces exactly one private pg listener after startup and invokes its original
parser only with admitted complete frames; the receipt records beforeParser=true
and accounting. The original listener is restored afterward.

This supersedes the prior post-parser capture only for that successful component
probe. Production bridge adoption, other/error/notice/binary message coverage,
transport allocation limits, original issuer/epoch and refusal settlement remain
unqualified. Full arbitration/protected producers, bootstrap and Truss streaming
remain unfinished. No mutations/cloud/deployment changes occur; goal stays active.


## Original Truss response completion — 2026-10-08

The incremental component now requires ordered description/rows/command/ready
state, checks original SELECT count against all observed rows, and refuses missing
readiness, duplicate/out-of-order completion or post-end reuse. finish now proves
this single response ended rather than merely accepting complete frame bytes.
Thirty tests/212 assertions, strict host build, clean packed consumer and the
small original native one-SELECT pre-parser probe pass. No mutations/cloud or
deployment changes occur.

Error/notice/extended-query/binary/multiple-result coverage, original native
producer/issuer/resource/settlement admission and bridge adoption remain open.
ReadyForQuery does not independently authorize quarantined-resource release.
Protected producers, bootstrap and actual Truss streaming remain unfinished;
the unchanged full end-to-end goal remains active.


## Original Truss failed-response completion — 2026-10-08

The pre-parser component now retains ordered ErrorResponse/NoticeResponse fields
with exact SQLSTATE, unique tags and terminal framing. Error completion requires
original ReadyForQuery; notices cannot replace a command. Thirty-two tests/218
assertions, host build and packed consumer pass. Actual SELECT 1/0 inside BEGIN
produces original 22012/E response frames before parser forwarding; the probe
waits for ReadyForQuery after rejection and confirms following ROLLBACK. Original
bytes/status and scope are retained in native-wire-error.json. Notice evidence
is synthetic only. No table/cloud/deployment changes occur.

Production integration, remaining protocol kinds, original issuer/resource/
refusal-settlement authority, protected producers, bootstrap and actual Truss
streaming remain unfinished under the unchanged active full goal.


## Original-response Truss adapter integration — 2026-10-08

Every experimental native control/statement now uses bounded frame admission
before pg parser forwarding and original ReadyForQuery/error correspondence.
Public columns/raw cells/exact command count text derive from original frames,
replacing converted Result metadata. Unnamed parse/bind/no-data responses have
order/body validation. Thirty-three tests/221 assertions, strict host build and
packed consumer pass. Existing small actual native executor checks also pass
through this integrated path, including writes, rollback, deadlock and deferred
COMMIT rejection. No persistent tables/cloud/deployment changes occur.

Per-query frame/delivered-byte/field/count/deadline limits are implemented; socket
allocation, total heap/shared operation accounts, durable original transcripts,
issuer/epoch/cut and unknown/refusal recovery remain unfinished. This supersedes
probe-only integration without claiming the complete selected native producer,
E06/protected bodies, bootstrap or actual Truss streaming. Goal stays active.


## Original Truss control/transaction correspondence — 2026-10-08

Native controls now require their exact original command plus one ReadyForQuery
with expected state: BEGIN/savepoints stay T, COMMIT/whole rollback end I. Execute
requires actual T rather than a local started flag. Mismatches quarantine original
custody before guessed cleanup/release. Thirty-four tests/225 assertions, host
build, packed consumer and existing small actual native executor checks pass.
A synthetic COMMIT tag with active state cannot confirm durability. No persistent
tables/cloud/deployment settings change occurs.

Full issuer/epoch, durable transcript/recovery, shared resources, caller adoption/
cancellation and protected producers/bootstrap/streaming remain unfinished. The
unchanged full end-to-end goal remains active.

### Explicit original-query journal candidate (2026-10-08)

Truss candidate `01f7b70` adds optional host-local file retention: original SQL
and ordered text/null parameters are fsynced before submission; original validated
frames are fsynced before driver parser forwarding. Exclusive private files and
an existing private host-owned directory are required. Response-complete,
server-error and uncertain outcomes are observations, not commit/replay authority.
Files contain sensitive data and require a trusted directory without concurrent
writers. No automatic replay or deletion is provided. Missing outcome remains
uncertain. Journal failure destroys transport. No host file I/O occurs without
explicit opt-in.

Thirty-five local tests/230 assertions and the host package build pass. The small
native executor rerun with journaling was dispatched but remains pending; no
native journal correspondence is claimed by this iteration. Durable records are
an experimental recovery input, not native issuer/epoch custody, protected
producer admission, full shared resource accounting or a completed Truss stream.
The full end-to-end objective remains active.

### Native retained-original correspondence (2026-10-08)

The previously pending original-query journal probe completed successfully on
existing local PostgreSQL 17.9. Truss candidate `bafe00e` retains the repeatable
native probe, independent offline verifier and hashed receipt. A separate process
read all 50 private original request files and validated their complete response
frames without database submission: 47 response-complete observations and three
original server errors (23505, 40P01, 23503). Sensitive SQL/parameters/results remain
in the private host directory; committed evidence contains file hashes/inventory
and classifications. This supersedes the pending native status above.

The journal unit check and a fresh packed host/portable consumer typecheck/runtime
also pass. This proves process-independent retained-original correspondence for
the exercised subset, not power-loss durability, uncertain-commit reconciliation,
native issuer/epoch, safe source ACK or full protected Truss bootstrap/streaming.
No cloud run or persistent native table change was needed. Full toolkit completion
remains unproved; retained originals must next bind to connection/operation custody
and explicit uncertainty handling before serving as recovery inputs.

### Local original-query custody binding (2026-10-08)

Truss candidate `2dd59e9` binds each retained request to the adapter's original
pool checkout through a random local lease locator and a canonical decimal query
ordinal from a bigint counter. Ordinary statements and controls share the same
counter; checkout reuse generates a new locator. The journal refuses malformed
custody before file creation/admission. Exact large ordinals survive persistence.

The existing small native PostgreSQL probe passes. Independent offline reads
validate 50 requests across nine checkout groups, each with consecutive ordinals
starting at zero and no duplicates. Forty-seven response-complete observations
and three original errors (23505/40P01/23503) remain preserved. Thirty-five local
tests/232 assertions, host build and clean packed consumer checks pass. Older
original files remain untouched; records lacking custody cannot satisfy this
new grouping check.

Local lease/ordinal provenance is not native xid, backend identity, issuer epoch
or protected operation authority. Full uncertainty reconciliation and native
protected producer/bootstrap/schema/feed paths remain incomplete. The full
end-to-end goal stays active; no new cloud resources or scale runs were used.

### Actual committed effect with lost driver completion (2026-10-08)

Truss candidate `dd588bf` adds a repeatable isolated native uncertainty probe on
existing local PostgreSQL 17.9. The explicit journal retains the server's actual
COMMIT CommandComplete before injected forwarding failure destroys the transport.
The executor reports `commit_unknown` with retryScope none, withholds callback
result, retains one quarantined checkout and refuses ordinary close. A separate
native connection independently observes exactly one committed fixture row.
Original private records prove a single local checkout with ordinals zero/two
inclusive: BEGIN, INSERT and one COMMIT; no guessed ROLLBACK or repeated write.
The retained COMMIT response is exactly the original CommandComplete frame; the
journal outcome remains uncertain rather than granting durability from that frame.

The uniquely named small fixture table is removed in finally. The isolated child
then exits with unresolved host bookkeeping, not via a library recovery/settlement
API. This is deliberate completion-loss containment evidence, not natural packet
loss, crash recovery, native protected operation identity/issuer/epoch, full Truss
bootstrap/feed or source ACK authority. It closes a native evidence gap previously
covered only by controlled ports. The complete end-to-end goal remains active.

### Bounded explicit local transport teardown (2026-10-08)

Truss candidate `ad741c9` adds `shutdownQuarantinedTransports()` to the host source.
It refuses healthy active checkouts before changing admission. Explicit shutdown
then closes admission, destroys only original quarantined local transports and
awaits actual local close with a five-second bound. Dead pool checkouts are removed
while original quarantine/outcome custody remains retained. Repeated successful
shutdown is inert; missing transport/timeout keeps uncertainty and closed admission.
Ordinary close still refuses unresolved quarantine and now refuses live checkouts.

The actual native commit-loss probe passes without forced process exit, replacing
that earlier harness workaround. It independently observes the committed row,
checks healthy-checkout protection, retained quarantine after local teardown,
closed future acquisition and repeated shutdown, then removes its fixture table.
Thirty-five local tests/232 assertions, host build and clean packed consumer checks
pass. Local socket closure is not native backend termination, transaction settlement,
source fencing or replay authority; full original recovery/protected native
producer/installation/schema/feed work remains incomplete. The full goal stays active.

### Public bounded original-journal inspection (2026-10-08)

Truss candidate `2669fd4` exposes `inspectOriginalQueryFile(path, {maxBytes})`
through the built host package. Explicit file bounds, private ownership, no symlink,
exact observed size and strict UTF-8/wire admission protect the offline reader.
Exact original bytes remain available as hex even when torn or unknown content
cannot be interpreted. Immutable supported request/custody/frame snapshots separate
complete response observation, uncertain, incomplete and invalid states. No state
provides native outcome, nonexecution proof or permission to retry. Unknown fields
refuse interpretation rather than silently disappearing. Old records lacking new
custody are still preserved as originals; no migration manufactures that custody.

Thirty-six local tests/241 assertions pass; final focused journal rerun also passes.
Host build, fresh packed consumer and actual native lost-COMMIT probe pass. The
latter reads its actual retained original files through this public API, comparing
exact bytes, while independently observing committed effect and preserving unknown
classification. No recovery SQL or cloud call is added. Full issuer/epoch, protected
producer/installation, UMF acceptance and Truss-to-Delta feed remain incomplete.
The full end-to-end objective stays active.

### Real UMF example bytes through Truss integrity ingress (2026-10-08)

Truss candidate `a4561f6` adds the first CONTRACT-003 original-artifact ingress
primitive, `verifyExactArtifacts`. Complete input-count/single-byte/shared-byte
bounds, canonical base64, scalar identity and SHA-256 correspondence precede
successful immutable carrier return. Own data snapshots are captured before
asynchronous hashing; caller mutation cannot replace checked originals. Unknown
carrier fields/accessors refuse. Raw artifact bytes are not JSON-parsed or converted,
so unknown UMF extensions and original numeric spelling remain intact.

The built public Truss package processes Ashlar's actual v1/v2/v3/unknown UMF
example files unchanged, comparing Web Crypto SHA-256 against Bun CryptoHasher.
The exact source hashes/byte counts are retained in the candidate evidence receipt.
Thirty-eight local tests/252 assertions, portable/host builds and clean packed
portable consumer checks pass. The browser-target build passes; actual browser
execution of this new primitive remains unverified.

This establishes bounded byte integrity, not UMF semantic validation/completeness,
version/support compatibility, dependency ordering, authority, native acceptance,
accepted catalog IDs or schema evolution effects. Those remain required next
compositions before the full Truss-to-Ashlar workflow can be claimed. No database
submission/cloud run occurred; the full objective remains active.

### Actual UMF semantic producer exposes required-check compatibility gap (2026-10-08)

Truss candidate `a87c6d3` builds the actual readDocument/validateDocument producer
from clean UMF commit fac1497a and runs all four original Ashlar example schemas
after the public byte-integrity ingress. Original validation results and every
diagnostic are committed with source/bundle/input hashes. All four are valid=true,
complete=false. Experimental core nullability/cardinality/facets/keys/relationships
warnings remain; v2 additionally reports UNKNOWN_NULLABILITY and the unknown
example UNKNOWN_CORE_FIELD. Its assertion is independently checked as preserved.

CONTRACT-003's required-check route demands valid and complete, so these actual
inputs cannot presently qualify that writable acceptance route. Shape validity,
byte integrity or removal of warnings cannot fill this gap. A browser-target
producer bundle was executed in Bun; real-browser execution and qualified bounded
validator isolation/authority remain separate. No native catalog IDs or acceptance
are manufactured, and no database/cloud call was needed.

The owner-authorized Truss Impl chat has been sent the exact results and requested
supported profile/acceptance composition within its existing planning scope. This
new evidence changes the next action: resolve original semantic completeness and
support correspondence before implementing writable acceptance for these models.
Native protected installation/feed also remain incomplete. The unchanged full goal
is active; this is its first newly verified required-check compatibility finding.

### Real browser ingress and selected-check qualification correction (2026-10-08)

Truss candidate `1b473e2` records actual Chromium 153.0.8010.12 execution of the
built portable Truss artifact ingress and identical pinned UMF producer. All four
original Ashlar inputs remain exact; original validity/completeness and every
diagnostic match Bun. Unknown assertion preservation, corrupt digest/shared byte
overflow/noncanonical base64 refusal and absence of Node globals are verified.
The repeatable browser probe retains exact Truss/UMF bundle hashes and diagnostics.

Correction to the preceding compatibility conclusion: CONTRACT-003 requires valid,
complete original results for separately selected semantic checks. Aggregate
validateDocument.complete=false alone is not the writable-readiness rule. Truss
Impl's active review clarified this distinction; the probe now labels its result
aggregateValidationCondition and separately records writable acceptance unavailable
because no qualified semantic-check composition has been supplied. It does not
convert aggregate warnings into either accepted/native authority or an absolute
ban on qualifying a separately selected supported subset. Original diagnostics
remain unchanged; none is suppressed to manufacture completeness.

Actual browser execution is now proved for this narrow ingress/producer subset.
Qualified semantic-check composition, native acceptance/installation, original
producer authority and streaming still remain required. The owner-authorized
Truss review is active; full end-to-end goal remains active. No cloud/scale run or
native installation occurs in this iteration.

### Source-qualified UMF interpretation/check inventory (2026-10-08)

Truss planning review 93633f2 is completed and pushed. It confirms that original
document diagnostics, complete selected required checks and actual native acceptance
are separate conclusions. It requests a source-qualified check inventory rather
than a Truss semantic validator or warning suppression.

Truss runtime candidate `c615503` now emits this prerequisite inventory using the
actual pinned UMF nullability/cardinality/facet/key/relationship inspection producers.
Twenty-six full original operation results are tied to original example artifact
bytes/hash and declaration pointers (5/8/8/5 across v1/v2/v3/unknown). Raw inputs,
full envelope diagnostics and unknown-scope diagnostics stay preserved. Required
semantic-check producer, native binding and support-profile slots are explicitly
unresolved. No inspection's unverified provenance is promoted into author/native
custody, complete value-check evidence or accepted IDs.

The repeatable semantic probe and real Chromium probe pass; every actual original
inspection result agrees across Bun and Chromium 153.0.8010.12. Updated producer
bundle hashes anchor both receipts. This advances the required profile composition
without claiming writable acceptance, actual native installation or a Truss source
stream. Complete selected-check producers and original native bindings remain the
next implementation dependencies. Full goal remains active; no cloud/scale run.

### Existing complete UMF field-value producer with explicit upgrade (2026-10-08)

Current-state inspection of the newer sibling UMF checkout found an existing
producer: validateCoreFieldValue at source 16c35e8d, for experimental core 0.8.
Truss candidate `28bc152` uses actual upstream explicit 0.7→0.8 upgrade/verification/
rollback operations against all original Ashlar examples, retaining original bytes
and complete receipts. Direct legacy 0.7 value checking refuses; no silent version
relabeling replaces an original schema. Unknown document content stays preserved.

Four actual present string values selected from local-string-source.jsonl through
explicit fixture property mappings receive original valid=true/complete=true checks.
Required nulls and wrong scalar-family probes refuse; v3's explicitly absent-allowed
caption accepts null. All source/value/upgrade/rollback results agree between Bun
and actual Chromium 153.0.8010.12. Original source hashes and exact producer bundle
hash are recorded. Existing fixture schema/mappings are not rewritten or turned
into native accepted IDs. Truss Impl was sent the actual producer evidence for
incorporation into its owner-authorized compatibility/profile planning review.

This closes one previously unidentified producer slot for selected present values,
not the entire required-check composition. V2's unknown availability and the unknown
document assertion remain unresolved even when a present string check passes;
whole-record/key/relationship constraints, native bindings/security, complete report/
head, protected installation and feed are still required. No qualified writable
profile or native acceptance is claimed. Full goal remains active; no cloud run.

### Reusable UMF logical Record checker and actual source integration (2026-10-08)

UMF now owns validateCoreRecordValues on pushed branch codex/core-record-value-check,
commit `c45c72a2`. CONTRACT-049 defines explicit core0.8 qualified membership and
absent/present typed values, composed with the original Field checker. Original
incomplete envelope diagnostics remain separate. Required absence, invalid values,
duplicate/unregistered entries fail; unknown source/availability, missing member
inventory and dataset key/relationship context remain incomplete. Defaults are
never inserted and old versions require explicit upgrade. The new managed UMF
worktree isolates this code from the existing DDL/owner checkouts; the branch is
not merged. Four new tests plus existing schema-property regression pass (33 tests,
97 assertions), as do library typecheck and real Chromium public-API checks.

Truss candidate `8af84f7` consumes that exact clean UMF source with verified explicit
upgrade/rollback receipts. Three actual create/replace records from the original
local source receive complete logical Record results, agreeing across Bun and real
Chromium. Unknown v2 availability and the unknown assertion remain incomplete; all
original schema/producer/value results remain retained. Explicit fixture property
mappings remain development IDs. Delete is a separate source operation, not a
Record-value input. Truss Impl was sent this new original producer evidence for
its owner-authorized planning/profile integration.

This adds a real reusable upstream checker and closes the membership/required-value
producer gap for the exercised known logical subset. It is not dataset uniqueness,
relationship/native enforcement, qualified validator isolation, protected installation,
accepted catalog IDs, source ACK or a complete stream. Native bindings/security,
complete report/head and protected Truss-to-Delta workflow remain unfinished.
The unchanged full objective stays active; no new cloud resources/scale runs.

### Runnable Ashlar example consumes actual UMF Record checks (2026-10-08)

The local user workflow now accepts --umf-source for the clean pinned UMF Record
checker c45c72a2 and optional --umf-check-output for complete original producer
results/upgrade receipts. A host runner invokes the actual upstream reader, explicit
upgrade/verifier and Record checker, with clean source observed before/after.
It refuses incomplete/invalid logical results before local source application.
Complete original schema/request/source-record custody is compared by the caller;
original intakes/interpretations and fixture mappings remain unchanged. The source
request reader uses UMF's duplicate/Unicode/number-aware JSON reader rather than
silently flattening duplicate members. No defaults are inserted or native IDs minted.

The actual user command passes on the existing three create/replace records. Apply,
delete/history and retained replay still yield one object, one tombstone, four history
entries, unchanged replay, original Unicode and the exact opaque large numeric token.
The small real-producer integration check verifies retained original bytes/upgrade
and complete result inventory, and refuses missing required values/duplicate request
members without successful output. The default dependency-free command also passes
its focused unittest. The README documents a reproducible pinned checkout and both
commands. Original document completeness and native acceptance remain false.

This moves the reusable checker from a Truss integration probe into a runnable
Ashlar consumer workflow. It remains a local development fixture path, not protected
native Truss acceptance/streaming, dataset key/relationship proof, qualified validator
isolation, publication or source ACK. Full goal remains active; no cloud/scale run.

### Actual UMF checks in the schema-evolution workflow (2026-10-08)

The user-facing evolution command now optionally consumes the same actual pinned
UMF Record checker, with complete per-revision/prestate results retained through
--umf-check-output-dir. Original event revision must match its selected definition;
the fixed fixture's complete batches are not reassembled or dispatched through a
newer-schema fallback. Two v1 creates and one v3 replacement receive actual checks.
Before the existing explicitly admitted v1→v3 transition, both existing records are
checked against the new logical v3 definition using their original matching retained
history/delivery/hash custody. Their original versions/revisions are not rewritten
by this check; no automatic compatibility or native migration is introduced.

The actual producer integration passes: original schema bytes, explicit upgrade
receipts, 2/1/2 complete source/new/existing check inventory, original prestate
locators and absent optional caption are verified. Independent evolution replay
retains both historical revisions, four events, one tombstone and live revision3.
Four evolution unittests and the default local-example check pass; the previous
actual UMF example integration also passes after shared-runner factoring. README
commands reproduce the checks and retain complete receipts. No defaults, native IDs,
publication or source ACK is manufactured. Full native Truss installation/acceptance,
protected feed and native end-to-end workflow remain unfinished; goal stays active.


### Actual UMF checks in the CSV source workflow — 2026-10-08

The runnable additional-source path now optionally invokes the same actual pinned
UMF c45c72a2 Record checker as the JSONL and evolution workflows, before local
apply. Original CSV correspondence is verified first. Full original producer
results and explicit upgrade receipts can be retained; three create/replace
records bind to original adapted-record hashes and exact header/row custody.
Deletes remain source operations. Empty cells remain present strings.

Seven focused CSV tests and the real-producer integration check pass. The latter
independently compares schema bytes, delivery custody, field values, history,
delete and replay results, including quoted labels, Unicode and the large opaque
future-column token. No cloud jobs or new native mutation occurred. Fixture IDs,
original incomplete schema validation, native acceptance, publication and ACK
qualifications remain unchanged. Native Truss producer/feed and full toolkit
acceptance remain required. The Truss owner planning chat incorporated the shared
Record producer at 221cb72; this is design progress, not native installation.


### Actual UMF checks after native outbox read — 2026-10-08

`run_outbox_example.py` now supports optional actual UMF Record checks and an
explicit read-only mode that performs no append calls. Existing committed native
groups 3–5 were read under ashlar_outbox_reader, independently compared with the
original fixture payloads and checked under their original v1/v3 definitions
before apply. Two v1 create checks and one v3 replacement check are complete;
the original envelope validation remains incomplete. Outer native checkpoints
are retained separately from inner source bytes/offsets, including the delete
group that has no Record-value check. Page resume reaches the same graph.

Five focused outbox tests and the actual PG17.9 read-only/UMF integration pass.
Original native groups and UMF receipts are retained under
[evidence/native-outbox-umf-20261008](evidence/native-outbox-umf-20261008/summary.json).
This iteration issues no source append, cloud mutation, publication or ACK.
Fixture/native source custody is not Truss catalog acceptance or a real Truss
feed. Full Truss installation/acceptance/mutation/feed and publication remain
required; the authorized owner coordination request is pending its tool result.


### Explicit retained-effect recovery — 2026-10-08

The native effect runner now distinguishes first application from recovery.
`DurableEffects.recover` requires the exact retained operation/intent/plan digest
and refuses missing original custody rather than inserting a replacement plan.
The native example uses this entry point for originally retained operations.
Original unsubmitted ordinals may still be submitted after renewed admission;
previously submitted ordinals retain the original-handle/no-blind-retry rules.
This is a needed effect port for the unfinished native stored-publisher driver,
not evidence that that full driver now runs.

Thirteen focused effect, SQL custody and stored-publisher tests pass, including
original-handle resume and missing/changed/corrupt/deleted-plan refusal before
transport. An offline check opened the actual original CSV apply journal read-only,
backed it up privately, recovered all four original plans from retained responses,
and proved missing-plan refusal on the copy with zero native transport calls.
[evidence/csv-effect-recovery-20261008.json](evidence/csv-effect-recovery-20261008.json)
qualifies this as offline receipt recovery, not current native authority/admission.
No cloud workload, original journal rewrite, publication or source ACK occurred.
The Truss coordination tool operation is still live without a delivery result;
it has not been retried or interpreted as accepted owner work.


### Journaled effect-to-publisher host driver — 2026-10-08

`tools/journaled_publisher_driver.py` connects actual DurableEffects execution and
recovery to StoredPublisherBackend's driver interface. Original publication
request/ordered steps are retained before effects, and one exact effect-bearing
artifact is retained before returning applied. Restart recovery requires original
plan custody; known artifact bytes/digest cannot change. When original artifact
observations remain unresolved after effects, the supplied recovery producer
must reconcile them; the driver does not generate a replacement proposal.

Writer/source admission, complete native parity/retention/pin validation, artifact
capture/recovery and descriptor-bound ACK remain explicit mandatory injected
services. The driver supplies neither permissive implementations nor SQL/schema
generation. Original effects are bound to the actual publication request digest
(including its original native source checkpoint). Existing legacy effect intents
are not relabeled as new publication requests.

Three new focused composition checks and nine existing effect/stored-publisher
checks pass. They exercise reopen/replay with one original manifest submission,
original-handle effect resume, missing artifact reconciliation, missing plan
refusal, native validation denial before manifest/ACK, context mismatch and
corrupt artifact refusal. These use a test transport; full native phase execution
is still unproved. No cloud job or source ACK was performed. Concrete native
artifact/validation/source policies and the actual Truss producer remain next.


### Full-row native snapshot artifact component — 2026-10-08

JournaledSnapshotArtifacts now supplies the journaled publisher driver's capture
and recovery port. Caller-provided expected inventories are copied, bounded,
explicitly admitted and retained before native observation. Every target gets
full-column/multiset parity at its exact UUID/version through
validate_effect_snapshot. The proposed manifest must name precisely the captured
version vector and bind the original publication request/source checkpoint.
Original request/effect/target inventory and applied artifact have digest custody.
Callback effect inputs are copied so a producer cannot rewrite the driver's
original response evidence while constructing its result.

Recovery returns only retained original proposal bytes after renewed explicit
admission. Missing or interrupted capture remains unresolved and refuses; it
never samples current tables to manufacture a replacement proposal. Native
retention, protocol, pin and source checks remain mandatory supplied policy and
validator obligations; parity alone does not establish them or source ACK.

Nine focused artifact, connected effect-to-stored-publisher and complete-row
parity tests pass with a test transport. The connected test now uses the real
artifact component, not a replacement artifact stub. Evidence covers reopen
without new snapshot queries/submissions, initial capture failure with later
replacement refusal, exact vector binding, changed target/artifact custody and
current admission failure. Full native phase execution and real Truss still
remain required. No cloud job, schema mutation or source ACK occurred.


### Concrete native artifact validator — 2026-10-08

NativeArtifactValidator now connects full effect parity, target/manifest UUID and
reader-protocol recognition, renewed finite-retention checks, mandatory complete
pin custody and independent source/schema/writer admission to the journaled
publisher driver. Its callable surface verifies the original request-bound
artifact; a separate retained-descriptor method never adds a request digest to
an older publication. Parsed descriptor fields must equal the original raw row.
Expected targets are copied at construction and source policies receive copies.

Twelve focused validator, connected publisher, artifact and effect-parity checks
pass. A real read-only run on the existing immutable CSV publication also passes:
all four native snapshot inventories, raw UMF intake, current owner/effective
grants, original vector, recognized protocol and finite retention were checked
under complete ordinary PG pin guards and the same-host cooperating lane.
The successful run issued 68 read statements on existing compute. Earlier
one-read attempts exposed timestamp-projection and catalog/schema quoting bugs
in the new check tool; both are corrected and original evidence remains retained.
No data/manifest write, source ACK, grant/settings change or new compute occurred.
Predictive optimization stays unchanged.

[evidence/native-artifact-validation-20261008.json](evidence/native-artifact-validation-20261008.json)
retains source hashes, original statement handles and response hashes; original
read receipts remain in the named private output directory. This verifies the
new native descriptor-checking service, not full native stored-publisher phases
or actual Truss acceptance/mutation/feed. Those remain required.


### Descriptor-bound local CSV consumer progress — 2026-10-08

JournaledCsvProgress supplies a concrete local consumer checkpoint service for
the forthcoming native CSV publication runner. Its scope retains immutable full
file SHA-256 and exact CSV/source/schema mapping for the original stream/feed/
epoch; reuse with different file contents or mapping refuses. Original rows,
requests/checkpoints and complete descriptor bytes are retained, with contiguous
positions and publication predecessor checks. Exact repeat cannot regress progress.

Native committed descriptor resolution under full pin/read admission and held
source/writer authority is mandatory via injected services; no default supplies
authority. The native resolver interval encloses the local durable transaction.
Closing source refusal before COMMIT rolls back the new row. A lost local COMMIT
receipt or resolver closure failure after COMMIT is explicitly outcome-unknown,
not rollback; the original durable checkpoint must be reconciled. This records
local immutable-file consumer progress only, never remote producer/Truss ACK.

Five focused progress tests and three connected host-pipeline tests pass. The
connected pipeline now uses actual SQLite CSV progress through reopening, with
the original committed test-transport manifest required before advancement.
Scope change, gaps, wrong predecessor, missing native resolution, precommit
refusal and both postcommit uncertainty modes are exercised. No new cloud
workload or native ACK was issued. Full native phase wiring and actual Truss
installation/acceptance/mutation/feed remain required.

### Native effective-permission observations — 2026-10-08

The native CSV runner can obtain complete effective privileges through the Unity
Catalog API without warehouse grant statements. tools/effective_grants.py retains
each original raw page before interpretation, explicitly requests pagination,
continues through empty pages until the continuation is absent, and preserves
inherited privilege origins. Unknown content/actions, partial origins, origins
outside the target ancestry, repeated principals/tokens and observation-budget
overflow refuse admission. Current authenticated actor, native owner and held
writer/source admission remain independent requirements.

Three focused tests pass, including complete pagination, inherited writer
inventory, empty-page continuation and retained unknown-content refusal. An
earlier native catalog observation retained one empty privilege page and issued
zero warehouse statements; its private receipt is
/private/tmp/ashlar-csv-stream-effective-permissions-20261008.json (SHA-256
efc447dc0355e4eaf43e00c72b302a8ba42da50ee72f361bc5f4c2a70f484b33).
That observation preceded the explicit pagination parameter; multi-page native
evidence remains open. The API contract was checked against the
[Databricks effective-permissions reference](https://docs.databricks.com/api/uc-grants/v1/get-effective-permissions).
No permissions or predictive optimization settings are changed by this helper.

### One-row native stored CSV publication — 2026-10-08

tools/run_native_csv_stream.py now connects the original CSV adapter, actual UMF
Record checks, raw native schema intake, durable immutable Delta attempt phases,
original effect plans and snapshot artifacts, immutable manifest commit, native
publication resolver, ordinary PostgreSQL pin guards and descriptor-bound local
CSV progress. A real run processed only ordinal 1 in the fresh private
ashlar_e2e_private_20261008.runtime_csv_stream namespace. It exited successfully
with local consumer position 1 and the retained four-table vector: object_current
2, edge_current 0, tombstone 0, whole_source_history 1. Nine mutation statements
have successful original receipts, including the manifest and attempt phases.

The run issued 523 warehouse reads and retained 154 effective-permission pages;
repeated complete validation dominates this one-row workflow. Reduce redundant
checks under demonstrated continuous writer/pin custody before treating this as
an efficient user command. No additional rows or scale workload were run. The
helper's later explicit-pagination/ancestry hardening has local test evidence;
this native process imported the earlier helper version.

[evidence/native-csv-stream-20261008.json](evidence/native-csv-stream-20261008.json)
binds the descriptor, original mutation handles/request/response hashes, runner
hash, private original receipt hashes and qualifications. The existing native
installation summary, UMF v3 registry proof, pinned UMF checker checkout and
private PG service are prerequisites. Reuse the original journal and a fresh
output directory with --limit 1; never reset original tables, phases or progress.
Later ordinal progression remains untested. This run uses explicit fixture IDs
and same-host cooperating writer/source custody. Actual Truss acceptance/feed,
remote producer ACK/fencing, a clean setup workflow and a singleton point query
through this runner remain open. Predictive optimization was unchanged.

### Singleton from the stored CSV publication — 2026-10-08

The native runner's --query-only mode now resolves the last retained CSV
publication through the existing read_singleton API. It holds the complete
four-table PG pin vector, renews authority/raw schema/protocol/finite retention
and complete fixture inventory checks, and executes the bound lookup at the
descriptor's exact Delta version. Original source timestamps and unknown bytes
remain in the returned carrier. Missing/duplicate identity and failed current
admission follow the existing resolver boundary. The exact retained ordinal and
original journal are required; this mode advances no source progress.

An actual --limit 1 --query-only --entity-id 1 run exited successfully with
object 1 at entity version 1 from object_current VERSION AS OF 2. All 128 new
warehouse statements were reads; the original nine native mutation receipts
(handles, requests and responses) were unchanged, and local consumer position
remained 1. Four focused singleton tests pass, including absence/duplicates,
wrong UUID/version, final custody refusal and expiry during execution. Native
absence/deletion and later CSV ordinals remain untested. Metadata redundancy
still requires improvement; this is not a latency benchmark.

[evidence/native-csv-stream-singleton-20261008.json](evidence/native-csv-stream-singleton-20261008.json)
retains the original query handle, exact-version SQL, response/private receipt
hashes and runner hash. [The example guide](../../../examples/end-to-end/README.md#stored-native-csv-publication-and-singleton-query)
documents the command and existing private prerequisites. This native run also
exercised the permission helper's explicit paginated requests; multi-page native
privilege evidence remains open. No grant or predictive optimization setting was
changed. Actual Truss acceptance/feed, remote source ACK/fencing and clean setup
remain required for the overall goal.

### Continuously pinned parity reuse — 2026-10-08

PinnedArtifactValidation now owns a real complete pin hold before reusing any
parity result. Its first descriptor check performs the existing full native
validation. Renewals in that same hold retain current source/schema/writer,
owner/effective grants, UUID/protocol, descriptor/vector/pin binding and finite
retention checks, while reusing the already proven immutable snapshot rows.
Descriptor/expectation change or any refusal poisons reuse until the interval
closes. Reopening forces full parity. No persisted flag admits a new interval.
The native CSV read path uses this service; unpinned publication validation
continues to scan full inventories. CONTRACT-004 records the qualified boundary.

Three new interval tests, three native-validator regression tests and four
singleton tests pass. The new interval checks exercise expiry, unknown protocol,
source refusal, changed expectations, rejected native pin admission, nonreentrant
holds, closing custody failure and full parity on reopening.

A single native query of the existing stored publication passed with the exact
same singleton and checkpoint position 1. It issued 112 read statements versus
the prior 128, with one full inventory scan per each of four snapshots. Original
nine mutation receipts were unchanged. This is scoped integration evidence;
metadata work remains substantial and does not establish latency targets or
production-scale admission. No new ingest, publication, source ACK, grant,
predictive optimization setting or compute resource was changed.

[evidence/native-csv-pinned-validation-20261008.json](evidence/native-csv-pinned-validation-20261008.json)
retains source/private receipt hashes, original outcome comparison and scan
counts. The finite retention window continues to bound autonomous maintenance;
PostgreSQL pins supply no indefinite promise. Manual destructive/configuration
changes require the cooperating profile. Native corruption or uncoordinated
administration is outside this qualified private fixture result. Actual Truss,
later source ordinals, schema evolution through this runner, clean setup and
remote source/fencing semantics remain open for the end-to-end goal.

### Publication-to-checkpoint pin interval — 2026-10-08

The native CSV runner now opens one complete read-pin interval immediately after
the initial full artifact validation and native pin registration, retaining it
through manifest commit, immutable committed phase, resolver admission and local
checkpoint handoff. The first validation inside that interval still performs
full parity; subsequent calls renew the existing qualified pinned admission.
Original request/artifact bytes and independently retained target expectations
must match. Graph effects precede this interval; consumed snapshots are unchanged
inside it. No missing proposal or original native handle is replaced.

Each batch owns its own ExitStack; closing releases the actual native pin guards
before the next batch starts. A closure failure after local progress is observed
raises LocalProgressOutcomeUnknown and requires original-receipt reconciliation.
It must not be reported as checkpoint rollback or license replacement effects.
Eleven focused pin-interval, connected publisher and checkpoint checks pass.

The original four-row CSV continuation was started from retained position 1 with
--limit 4, using the original journal and existing private tables/compute. Its
native run is still in progress; create/update/delete progression remains
unproved until original terminal receipts, descriptor chain, full current/history/
tombstone parity and closing custody are inspected. Original receipts remain in
/private/tmp/ashlar-csv-stream-complete-20261008. This extends the small runnable
fixture and does not supply actual Truss producer/catalog IDs or remote ACK.

### Native CSV resume, update and delete completed — 2026-10-08

The original process exited successfully at local consumer position 4. It resumed
from position 1 without reapplying that row and published new ordinals 2, 3 and 4:
second object creation, object 1 replacement and object 2 deletion. The final
vector is object_current 7, edge_current 0, tombstone 1, whole_source_history 4.
One object survives at entity version 2, with the quoted label and Unicode
caption; four exact original history records and one tombstone remain.

Offline inspection independently reconstructed each new prefix from original
CSV bytes and compared every retained native full-inventory response at its exact
version: 12 table/publication combinations passed. The original checkpoint chain
is contiguous 1–4 and preserves each predecessor/request digest. All 36 mutation
receipts are terminal SUCCEEDED; the original nine receipts from ordinal 1 are
unchanged. The run issued 1,069 warehouse reads and 418 effective-permission pages.
These counts expose remaining metadata overhead, not a scale/latency benchmark.

[evidence/native-csv-stream-complete-20261008.json](evidence/native-csv-stream-complete-20261008.json)
retains the full new descriptors, checkpoint chain, inventory counts, original
mutation handles/request/response hashes and private receipt/source fingerprints.
No extra warehouse statements were needed for the independent receipt checks.
The example's query command now selects retained ordinal 4. Native exact-repeat
and deleted-object point lookup through that final descriptor remain untested;
full final inventory does establish absence of object 2. Earlier publication
descriptors are retained and source ACK remains local consumer progress only.
Actual Truss acceptance/feed, native schema evolution through this runner, clean
setup and remote source/fencing semantics remain open. Predictive optimization,
grants and compute settings were unchanged.


### Native CSV installation configuration — 2026-10-08

The bounded CSV runner now derives catalog/schema and stream identity from the
original installation receipt instead of a hardcoded namespace, and accepts
explicit existing Databricks profile/warehouse arguments. The source remains the
four-row synthetic CSV with fixture type/property IDs; this is not a generic
source mapper or real Truss acceptance. The existing namespace retains exactly
the same stream identity and journal workload scope.

Configuration admission requires current UMF model fingerprints and generator
provenance, a complete same-namespace eight-carrier inventory, exact ordered
columns and distinct canonical native UUIDs. Receipt fields are not mutated.
Fresh native authority observations also require every carrier to be a current
Unity Catalog MANAGED table; original UUID/protocol, permissions, schema,
retention and source checks remain independently mandatory before effects.

Two focused local tests pass, including stale model, altered generator, unsafe
namespace, missing/foreign carrier, columns, duplicate UUID and owner refusals.
The original actual installation receipt passes the same configuration helper,
and CLI help exposes both new compute arguments. No SQL or cloud mutation was
run in this iteration. Fresh caller namespace execution and reusable native
schema-registry setup remain unverified; no clean end-to-end installation claim
is made. This closes one configuration gap while actual Truss remains required.


### Reuse and renew native UMF registration admission — 2026-10-08

Inspection found that register_umf.py already installs a same-namespace raw
registry and retains caller-selected documents/revisions with durable original
SQL handles. No second schema-registry installer was added. The remaining
configuration/authority gap is now addressed in that command: complete current
installation custody, fresh actor and owners, retained paginated effective
permissions, managed-registry admission and original inspected source bytes
are required before/after registration and before success. Exact registry UUID,
columns and immutable revision conflict/readback rules remain mandatory.

Two new authority checks, three existing effective-permission checks and two
existing registry checks pass (seven unique tests). CLI help loads through the
SDK environment. No warehouse SQL or cloud mutations were run. Native revised
registration and a fresh setup/publish/query remain unverified. Historical v1/v3
receipts retain their original scope. Actual Truss implementation/feed and remote
schema/source acceptance remain necessary to complete the goal.


### Native revised registry registration and replay — 2026-10-08

The revised command passed actual native registration of the single retained v3
UMF source in the existing current generated CSV installation. It created the
managed registry ashlar_e2e_private_20261008.runtime_csv_stream.schema_intake
with native UUID 729976af-7831-403a-9847-c78e03a29847, then retained revision 3.
Actual clean UMF source remains pinned at 16c35e8d943769ccfa7bb57d16785aa7159abe65.
The source/artifact SHA256 match the original v3 intake, including explicit
incomplete interpretation. This is raw custody, not accepted executable schema.

A fresh-process same-journal replay exited successfully. Independent offline
inspection compared both full native selected rows against original retained
source/artifact bytes, complete fields and boolean interpretation. The two
original terminal SUCCEEDED mutation handles, request digests and full response
bytes are identical across both journal exports. Native initial registration
used six warehouse reads and 14 effective-permission pages; replay used five
reads and 11 pages with zero new mutations. No graph effects, publication, ACK,
grants, predictive optimization or compute changes occurred.

[evidence/native-umf-registry-admission-20261008.json](evidence/native-umf-registry-admission-20261008.json)
retains the original handles, response/source fingerprints and separate private
receipt directories. The older runtime installation receipt correctly failed
current generated-model admission before effects; it was not rewritten or
silently upgraded. Existing CSV publication workload remains bound to its older
shared registry UUID/proof; this new registry is not swapped into that journal.
A fresh complete carrier installation, broader schemas and actual Truss runtime/
acceptance/feed remain open. Native concurrent uniqueness/remote fencing and
hash CHECK constraints on this newly created registry are not claimed.


### Fresh generated carrier setup — 2026-10-08

The Truss delivery request remains live (tool cell 1306), while its owner chat
remains idle at 221cb72 and the runtime candidate remains inert at 8af84f7.
No duplicate request or fabricated feed/catalog admission was issued. Independent
progress closes the carrier installation gap: setup_native.py now shares the
NativeOwnerLane used by raw registry admission, with fresh actor/native owners,
managed-table checks and complete retained effective-permission API pages.
Ancestors are renewed before every native CREATE; all eight UUIDs are reobserved
before readiness. Original generated model custody is checked at completion.
Read and mutation transports use the same actual SDK client. Five focused shared
authority/permission tests pass; no new Truss capability is claimed.

The revised installer completed in new existing-catalog schema
ashlar_e2e_private_20261008.runtime_clean_user on existing compute. All nine
original native CREATE statements are terminal SUCCEEDED. Offline inspection
matches the complete native statement set against UMF-generated intent, every
ordered column/type inventory and eight final UUID observations against original
journal identities. All initial DESCRIBE DETAIL records report zero files.
The resulting receipt passes the stream/registry configuration helper unchanged.
Native observations comprise 24 warehouse reads and 53 permission API pages.

[evidence/native-clean-carrier-setup-20261008.json](evidence/native-clean-carrier-setup-20261008.json)
retains tested source fingerprints, original statement handles/hashes, all native
UUIDs, generated model provenance and private receipt/journal paths. Tested source
is the documented edited snapshot on d2ee6da; no later commit is backdated as
execution evidence. The user setup command now names this current installation.
No graph writes, cleanup, grants, compute or predictive optimization changes
occurred. Actual UMF registration, publication/query and schema evolution on
this fresh installation remain unexecuted. Actual Truss remains required for the
full goal; historical native CSV proofs cannot stand in for this new workload.


### Fresh-installation raw UMF intake and one-row continuation — 2026-10-08

Actual pinned UMF revision 16c35e8d retained schema-v3 in the new
runtime_clean_user managed registry, UUID 4d925bcc-b4fd-4c9b-9e97-07d77417789f.
The complete original source/artifact row readback matches byte-for-byte and
preserves incomplete interpretation. Both native mutation receipts are terminal
SUCCEEDED. Five warehouse reads and 11 retained effective-permission pages were
used on existing compute; no additional setup or graph writes occurred during
registration. Exact UUID and receipt/source fingerprints are retained in
[evidence/native-clean-schema-intake-20261008.json](evidence/native-clean-schema-intake-20261008.json).

The one-row native stored-publisher process was started using exactly this new
installation, registry proof and independent original stream journal. Its active
process handle is 80087; retained receipts are
/private/tmp/ashlar-clean-user-stream-20261008 and journal is
/private/tmp/ashlar-clean-user-stream-20261008.sqlite. Publication, local
checkpoint and singleton success remain unproved until that original process is
terminal and complete receipt/oracle/pin-closure checks pass. No replacement
attempt or source epoch is licensed by pending observations. This is a small
fresh setup workflow, not a repeated four-row or scale benchmark. The guide
provides the same-installation continuation and required pinned checker/service
inputs. Actual Truss, schema evolution and supported wider source paths remain
open under the original goal.


### Fresh-installation one-row native publication completed — 2026-10-08

Original process 80087 exited successfully at local consumer checkpoint 1.
The fresh workflow now runs generated carrier setup, actual same-namespace UMF
intake and one-row CSV stored publication. Its committed vector is object_current
2, edge_current 0, tombstone 0 and whole_source_history 1. One complete object
and one exact original history record are retained; no edge/delete was sent.
Independent offline source reconstruction compares all 16 retained full-inventory
observations (four per target) against original source bytes and materialization
clock. All complete fields, SQL nulls and duplicate multiplicity match.

The original checkpoint JSON SHA256, request digest and exact descriptor match.
All nine original Delta mutation handles are terminal SUCCEEDED. Publication
used 359 warehouse reads and 154 effective-permission pages; substantial metadata
work remains, and this is correctness/setup evidence rather than latency proof.
[evidence/native-clean-csv-publication-20261008.json](evidence/native-clean-csv-publication-20261008.json)
retains original descriptors, checkpoint, handles/hashes and source/native receipt
fingerprints. Execution started from clean Ashlar 0331374; later evidence-only
commit ea3d95a does not change the executed tool source. Query-only process 54800
was then started using that same journal, installation and intake proof, with
entity ID 1 and no new ingestion. Its terminal singleton and closing custody
remain unverified until the original query completes. Actual Truss, wider schema/
source paths and remote admission remain open; no grants/compute/maintenance
changes occurred.


### Fresh-installation singleton completed with timestamp gap — 2026-10-08

Original query process 54800 exited successfully. The fresh setup -> actual raw
UMF registration -> one-row stored publication -> resolver-bound object lookup
now executes on the new installation. It returned object 1 at entity version 1,
with all non-timestamp fields independently matching source reconstruction.
Exactly one parameterized native point query used object_current VERSION AS OF 2.
The complete nine original Delta mutation request/response/handle records and
original checkpoint remained unchanged. Query used 112 warehouse reads and 66
retained effective-permission pages, with no new Delta mutation submissions.

[evidence/native-clean-singleton-20261008.json](evidence/native-clean-singleton-20261008.json)
records a material fidelity counterexample: raw TIMESTAMP JSON renders
published_at as 2026-10-08T19:02:38.556Z, while the retained native effect/oracle
value is exactly 1791486158556147 microseconds. Full pinned inventory checks use
unix_micros and preserve that precision, but the singleton raw projection does
not. Therefore complete singleton timestamp preservation is unproved; next fix
its explicit output carrier and validate exact source precision rather than
silently accepting millisecond truncation. This evidence is scoped as
passed-with-timestamp-fidelity-gap, not complete support. Actual Truss and broader
schema/source workflows remain open under the full goal.


### Singleton microsecond fidelity corrected — 2026-10-08

read_singleton now excludes raw published_at from the wildcard and explicitly
projects cast(unix_micros(published_at) AS STRING) under the original field name.
Its output carrier is documented as canonical signed epoch-microsecond text or
SQL NULL; missing, malformed, noncanonical, narrowed and out-of-range values
refuse. Schema/storage remain TIMESTAMP. This is an explicit experimental API
output interpretation change from earlier ISO datetime text; original precision
loss evidence remains retained rather than rewritten.

Six focused singleton checks pass, covering microsecond text, null, negative
values, both signed64 bounds and original custody/retention refusal controls.
A corrected query-only native process exited successfully on the same original
publication and object_current version 2. Independent offline reconstruction
matches every returned field, including published_at 1791486158556147 exactly.
All nine original Delta mutation handles/request/response records and local
checkpoint are unchanged. The run used 112 warehouse reads and 66 effective
permission pages with one parameterized point SELECT and no new Delta mutations.

[evidence/native-clean-singleton-micros-20261008.json](evidence/native-clean-singleton-micros-20261008.json)
retains corrected original query/response fingerprints, tested singleton source
hash and source/base-revision qualification. Native null/negative/boundary/edge
cases are not claimed; those are local tests. This closes the observed timestamp
fidelity gap for the fresh one-row workflow. Actual Truss runtime/catalog/feed,
wider schema/source integration, remote fencing and ACK remain open. No compute,
grants, predictive optimization or native source/table data changes occurred.


### Additional immutable JSONL source publication wiring — 2026-10-08

The stored native runner now exposes explicit CSV/JSONL selection rather than
embedding a CSV-only consumer handoff. Generic runner is
run_native_source_stream.py; the original CSV entrypoint delegates with the same
CSV default, request IDs/checkpoints and journal scopes. Shared
JournaledFileProgress retains private immutable file custody, mandatory source/
resolver policies, exact publication correspondence, contiguous original-prefix
progress and post-COMMIT uncertainty semantics. Separate adapters retain CSV row
ordinals and JSONL complete-transaction byte offsets without converting one to
the other. JSONL publication requests bind a new explicit checkpoint profile;
existing outbox/CSV profiles remain separately selected and tested.

Three new JSONL tests and five existing CSV handoff tests pass, alongside three
source-checkpoint, three CSV-checkpoint and three journaled-publisher-driver
checks (17 unique tests). JSONL checks cover all three original fixture groups,
reopen/exact old replay, gap/source changes, resolver refusal/closure uncertainty
and cursor/ordinal substitution. Initial new-test assumptions incorrectly treated
the existing fixture as two groups and expected a later checkpoint exception
where original staging refused first; assertions were corrected to the actual
three-group source and preserved earlier refusal. No admission was relaxed.
Actual pinned UMF Record checks also pass for the JSONL source records.

Offline read-only backup copies of both completed native CSV journals recover
positions 1 and 4 unchanged through the extracted component; no original journal
or cloud data was modified by that check. Both CLI entrypoints load. No native
JSONL run or new native mutation occurred in this iteration. Its next integration
check must use its own empty installation/retained UMF proof and small first
complete group, then independently verify publication, byte checkpoint and
singleton. This is an additional supported wiring slice, not completion of
actual Truss, arbitrary schemas, combined multi-feed state or remote fencing/ACK.


### Native JSONL installation/intake completed; original publication live — 2026-10-08

The independent runtime_jsonl_stream setup completed on existing private
catalog/compute. All nine original CREATE statements are terminal SUCCEEDED;
eight managed carriers match exact generated columns, native journal UUIDs and
final identity reobservations, with initial zero-file details. Current generated
installation receipt passes the runner admission unchanged. Setup used 24
warehouse reads and 53 retained effective-permission pages.

Actual pinned UMF inspection/registration retained source revision 3 in this
same namespace's managed schema_intake. Both native mutations are terminal
SUCCEEDED; the complete raw source/artifact row independently matches retained
original bytes and incomplete interpretation. Registration used six warehouse
reads and 14 effective-permission pages.
[evidence/native-jsonl-setup-intake-20261008.json](evidence/native-jsonl-setup-intake-20261008.json)
retains all original handles, request/response hashes, source/model/generator
and native UUID fingerprints. These checks do not admit actual Truss catalog IDs.

The actual new source publisher was then started with --source jsonl --limit 1,
using this installation, same-namespace registry proof and original journal
/private/tmp/ashlar-jsonl-stream-20261008.sqlite. First complete group has two
object records and actual checkpoint byte position 797; it is not ordinal 1.
Original process 53121 remains live; receipts are
/private/tmp/ashlar-jsonl-stream-first-20261008. Native publication, checkpoint,
full independent inventories and singleton remain unproved until terminal
receipts/closure checks pass. Do not restart or replace this journal/epoch based
on an observation timeout. No source graph rows were written during setup/intake;
any later effects belong to that still-live publication. Actual Truss, wider
schemas and combined multi-source/remote fencing semantics remain open.


### Native JSONL first publication and singleton completed — 2026-10-08

Original publisher process 53121 and query process 4638 both terminated with exit
0. First complete JSONL group published two current objects and two complete
original history records; edge and tombstone inventories remain empty. Four
full observations of each exact-version inventory agree with the independent
original-source oracle. Nine original Delta mutations are terminal SUCCEEDED.
Actual byte checkpoint 797 covers the original complete transaction bytes;
completed-group count 1 is separate. Publication used 359 warehouse reads and
154 retained permission pages.

Query-only object 1 reads the original publication at object-table version 2.
All 17 fields equal the independent original-source reconstruction, including
exact signed epoch microseconds and opaque retained content. The nine original
mutation handles, request/response hashes and complete checkpoint request/
descriptor/digest remain unchanged. Query used 112 warehouse reads and 66
permission pages; it is integration evidence, not latency evidence.

Evidence: [first publication](evidence/native-jsonl-publication-first-20261008.json)
and [singleton](evidence/native-jsonl-singleton-first-20261008.json).
No new compute, grants or predictive optimization changes occurred. Native later
JSONL groups/replay/deletes, actual Truss engine/catalog/feed/ACK, wider schema
admission/evolution and combined multi-source state remain open. Full end-to-end
goal remains active; synthetic source IDs are not accepted Truss catalog IDs.


### Stored native schema-evolution wiring — 2026-10-08

The runner now connects the existing explicitly admitted v1-to-v3 schema policy
and original evolution JSONL bytes to durable native publication. It requires
two exact original intake proofs in the same installed registry UUID, renews
both raw rows at admission and includes their complete intake custody in its
original workload. Manifest inventory retains both revision aliases; replay
cannot substitute revision 3 for revision 1. Prior values receive actual UMF
v3 Record checks before transition planning. Existing single-revision workload
and request encodings remain unchanged, including the historical CSV proof.

Fifteen focused tests pass: three new multi-intake/source-oracle tests, four
schema-evolution tests and eight existing JSONL/CSV handoff tests. Actual pinned
UMF source and prior-value evolution checks pass. Native CLI help loads with the
existing SDK environment. Initial test import used the wrong PublicationError
module and was corrected; direct py_compile attempted a protected system cache,
so CLI loading provided the syntax/import check instead. No admission relaxed.
Native evolution setup/intake/publication/query remains pending; local tests do
not establish cloud migration or Truss acceptance. Full goal remains active.


### Native evolution setup/intakes passed; publisher live — 2026-10-08

The independent runtime_schema_evolution installation completed, original process
30527 exit 0. All nine original CREATE mutations are terminal SUCCEEDED; eight
exact column/UUID inventories, initial zero-file details and closing native UUIDs
match generated inputs. Setup used 24 warehouse reads and 53 permission pages.
Both actual pinned UMF revisions 1 and 3 are retained in the same managed registry
UUID through /private/tmp/ashlar-evolution-schema-20261008.sqlite. Original CREATE
was recovered; only two independent MERGE mutations were added. Both complete raw
rows equal original intake bytes. Each registration used five warehouse reads
and eleven permission pages. Interpretation completeness remains false.

Evidence: [setup](evidence/native-evolution-setup-20261008.json),
[intakes](evidence/native-evolution-intakes-20261008.json), and
[logical preflight](evidence/native-evolution-preflight-20261008.json).
Actual UMF checker c45c72a2 verifies two original v1 records, one original v3 record,
and two prior source values under v3 with optional caption absent.

Publisher original process 59082 is live, --source evolution --limit 3, using
/private/tmp/ashlar-evolution-stream-20261008.sqlite and receipts
/private/tmp/ashlar-evolution-stream-20261008. The complete sequence has four
events in three groups, actual source byte positions 805, 1318 and 1814.
Native final publication, inventories, checkpoint and singleton remain unproved;
observe the original handle rather than restarting on timeout. Existing private
warehouse/catalog only; no compute/grants/predictive optimization changes.

The updated authorized Truss capability request was delivered successfully after
older requests returned terminal tool timeouts. Truss Impl confirms microsite
work is not a runtime prerequisite, but still reports owner decision/authorization
dependencies. No real Truss engine or accepted catalog IDs are inferred.


### Native evolution first group published; remaining groups live — 2026-10-08

Original publisher 59082 remains live. Its first complete revision-1 group has
committed byte checkpoint 805, covering exactly the original begin/two-event/
commit bytes. First nine chronological native mutations are terminal SUCCEEDED.
Complete exact-version observations for all four targets independently match
original-source reconstruction: object_current version 2 has two rows,
whole_source_history version 1 has two exact original records, and edge_current/
tombstone version 0 are empty. The stored artifact manifest equals the committed
checkpoint descriptor and explicitly retains both admitted revision aliases.

[First-group evidence](evidence/native-evolution-publication-first-20261008.json)
retains original mutation handles/request/response hashes, full inventory handles
and complete checkpoint custody. The offline check initially selected next-group
attempts through their predecessor digest; it was corrected to the first nine
chronological original mutations. No admission or native effects changed.
The revision-3 replacement, revision-1 delete, final byte checkpoint 1814 and
singleton remain unverified while the original publisher continues. No extra
warehouse queries were submitted for this independent receipt comparison.

Truss Impl's returned clarification at 0ad0b4c separates the microsite from the
runtime path. Protected orchestration, atomic acceptance and feed fencing have
design direction; exact runtime profiles and native engine implementation remain
unfinished. This is a dependency clarification, not evidence of a running Truss.


### Binding-derived UMF Record checks — 2026-10-08

StringRecordPolicy now retains immutable qualified Record/Field bindings and
exposes an admitted value-request translation without fixed fixture IDs. A host
helper groups original records by qualified Record identity, invokes actual
pinned UMF and retains independent complete receipts. It verifies original
intake/binding source correspondence, checks delete carriers without treating
them as new value observations, and never supplies native acceptance or ACK.

Eleven focused semantic-policy/evolution tests pass. Actual UMF checker c45c72a2
passes three non-delete source records with development type 1017 and property
1023/1024, including absence and Unicode, from three newly encoded complete
transactions with recomputed manifests. Original schema is unchanged; arbitrary
schema/native qualification remains open. Evidence:
[bound Record checks](evidence/umf-bound-records-20261008.json).

Original native evolution publisher 59082 is still live and now has committed
checkpoints 805 and 1318. The second checkpoint follows the explicit revision-3
replacement. Final delete/checkpoint 1814 and singleton remain pending; full
independent final receipt comparisons will follow terminal completion.


### Complete native schema-evolution publication verified — 2026-10-08

Original publisher 59082 terminated exit 0 at byte checkpoint 1814 after all
three groups/four original events. Complete local positions are 805, 1318 and
1814, each independently matched to the original complete transaction bytes and
request/descriptor source bindings. All 27 original native mutations succeeded;
the first nine handles and complete request/response hashes remain unchanged.

All twelve ordinal/table exact-version full inventories independently equal
source-to-row reconstruction. Final object_current version 5 contains only
object 1 at entity version 2/schema revision 3 with updated label and Unicode
caption. Object 2 is absent, tombstone version 1 contains its revision-1 delete,
and whole_source_history version 3 preserves all four original records across
both schema revisions. edge_current remains version 0/empty. Publication used
1145 warehouse reads and 418 effective-permission pages; this is small correctness
integration evidence, not performance or scale evidence.

[Complete publication evidence](evidence/native-evolution-publication-complete-20261008.json)
retains all original checkpoint requests/descriptors, mutation custody and full
inventory read handles. Independent comparison submitted no additional warehouse
queries. A final query-only survivor read was started as original process 50926,
receipts /private/tmp/ashlar-evolution-query-20261008, using the same original
stream journal and both intake proofs. Singleton success remains unproved until
that original process and complete oracle/unchanged-receipt checks finish.
Native exact-repeat remains untested. No real Truss catalog/producer, arbitrary
schemas, combined-source state or remote ACK/fencing is established. Existing
warehouse/catalog only, no predictive optimization/grant changes; full goal active.


### Native evolved singleton completed; Weft integration requested — 2026-10-08

Original query-only process 50926 terminated exit 0. Surviving object 1 reads
through the original final publication at object_current version 5. All 17 fields
match independent original-source reconstruction, including revision 3, entity
version 2, Unicode optional caption and exact epoch microseconds. All 27 original
mutation handles/request/response hashes and complete byte checkpoints
805/1318/1814 remain unchanged. Query used 124 warehouse reads and 66 permission
pages. [Singleton evidence](evidence/native-evolution-singleton-20261008.json)
retains complete parity, original SQL and fingerprints. Native exact-repeat and
deleted-entity point lookup remain separately untested; full inventories already
prove the original delete and tombstone. No new compute/grants/PO changes.

A delegated explicit human request now authorizes integrating Weft as the query
engine. Preserve this completed publication workflow. Use merged Weft commit
2744531735c2a771fbe7ed24a7f67e3afc851b25, qualified ashlar-databricks feature,
backend 0.1.0-qualified / dbsql2026.39-qualified, and actual model/storage bindings.
Owner checkout contains unrelated uncommitted B-008 security work; do not alter
or build from it. Read contracts, exact profiles and support inventory from the
pinned committed source. Native engine/settings/layout, integrity, authorization,
publication/schema context and exact decoding must be admitted and rechecked
before releasing buffered results. Unfinished security work is not included.
The requested native Weft integration is not yet implemented or qualified.

### Pinned Weft build and publication-bound compilation — 2026-10-08

Clean separate source checkout /private/tmp/ashlar-weft-2744531735c2 is pinned to
merged 2744531735c2a771fbe7ed24a7f67e3afc851b25. Existing cached Rust 1.90.0 and
Maturin 1.9.6 built the ABI3 Python wheel offline/locked with two local jobs and
only ashlar-databricks-qualified. No owner dirty checkout or global packages were
changed. Wheel RECORD member hashes and actually loaded native-extension hash
are retained in [compiler evidence](evidence/weft-pinned-compiler-20261008.json).

New owner binding generator composes exact original evolved UMF source, admitted
string Record/Field IDs, schema revision and all native publication versions/UUIDs.
The actual built compiler accepts label/caption selection at final object version
5 with candidate opt-in false. Four real compiler controls refuse stale model pin,
binding pin, unknown target profile and unavailable candidate registration without
SQL. Nine focused binding/semantic tests pass. The full owner layout reference
hash exactly matches Weft's pinned copy; the packaged baseline has a separate
subset hash. Only current props homes are mapped; no typed projection invented.

Initial cargo lookup targeted a nonexistent user rustup home; locating the already
installed private toolchain resolved it without installation. The locked build
emits existing vendored ahash/dead-code warnings but succeeds. Actual compiler
work is local only; no new warehouse statements or writes were issued.
Next implement host obligations, exact decoding, buffered context recheck and
native tests against these actual publications, then broader admitted homes,
joins/aggregates/keys/relationships. No native Weft execution or full goal completion
is claimed by this build/compile checkpoint.

### Buffered Weft execution boundary — 2026-10-08

New read_weft executes integrity checks and compiler SQL inside one complete
resolver/pin interval, requires mandatory compiler-custody/profile/authorization/
decoding/release policies and returns only after closing checks succeed. It
strictly recognizes the initial three original obligation families, ordered exact
parameter slots, complete STRING carriers and one-zero guard outcomes. Unknown
families are not ignored. CompiledWeftTransport admits exact compiler WITH SQL and
checks without changing ordinary native read routing. Explicit string/presence
decoding retains absence versus value and refuses native null, duplicate/unknown
JSON content, invalid Unicode and coercion.

Eleven focused execution/decoder/binding/singleton tests pass, including guard
ordering, exact parameter reuse, late authority/pin failure, type mismatch and
profile/unknown-obligation refusal before user SQL. Test policies and backend are
explicit synthetic boundary fixtures; no native authorization or execution claim.
This iteration submits no warehouse statements or graph writes. Next compose the
actual owner policy and profile observations with these boundaries, recompile to
verify original artifact custody, then execute queries against the completed
native evolution publication. Numeric/recursive/typed-home/join/aggregate/key/
relationship support and broader source/Truss paths remain unfinished. Full goal
is active; a callback boundary alone does not satisfy native Weft integration.

### Actual native Weft string/presence query — 2026-10-08

The source runner now exposes query-only `--weft-sql` with explicit clean pinned
Weft source and verified wheel paths. Native host admission checks the actually
loaded extension, original entrypoint, qualified layout, regenerated owner request
and full recompiled artifact. Existing retained publication schema/source/full
inventory/grants/UUID/protocol/finite-retention/PG pin checks remain active.
Native release/build and ANSI mode are observed before execution and release,
without setting or resetting configuration. Separate observations do not establish
same-statement settings evidence; broader qualification remains open.

The actual final evolution publication passes `SELECT i.label, i.caption FROM
Item i`. Both original emitted scalar integrity queries return exact STRING zero;
the buffered result is `updated` and the explicit caption value `雪`, matching
independent original-source reconstruction. Closing admission/pin checks pass.
All 27 original mutation handles/request/response hashes and all three checkpoint
originals/digests remain unchanged. The small read-only run has 140 warehouse
statements and 99 permission pages, with no graph writes or source advancement.
[Native evidence](evidence/native-evolution-weft-query-20261008.json) retains the
actual compiler artifact, parameters, SQL responses, profile observations and
receipt hashes. Twelve focused local checks pass, including engine/build/ANSI
profile drift and duplicate identity refusal. The only SET statement reads
ANSI_MODE; it contains no assignment. The documented read-only syntax is
[Databricks SET](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-conf-mgmt-set),
inspected 2026-10-08. Predictive optimization/grants/compute settings unchanged.

This is a private development owner fixture string/presence query, not complete
native Weft language conformance or production authorization. Exact per-execution
settings proof, numeric/recursive/typed homes, joins/aggregates/keys/relationships
and the actual Truss catalog/producer/feed remain unfinished. Goal stays active.

### Binding-derived native preflight wiring — 2026-10-08

The native source runner now uses the reusable admitted-binding UMF checker for
original source records and selected target-schema prestate checks. Qualified
Record/Field references replace its old example-ID switches. Original prestate
IDs are decoded under their original schema binding, so changed target storage
IDs cannot reinterpret history. Missing original bindings/history or unmapped
Fields refuse; exact target selection does not imply automatic compatibility.
Per-Record receipts are retained in fresh directories. Original publication
request encoding and checker revision remain unchanged.

Twenty-five focused local checks pass, including changed target IDs and missing
prestate custody. The actual pinned UMF checker passes all three source values
and two retained v1 values against v3; its complete results exactly equal the
previous native run's logical receipts. Evidence:
[bound preflight](evidence/bound-native-preflight-20261008.json). No warehouse
statements, graph writes or source ACK were issued. An initial test invocation
named a nonexistent test module; the corrected focused set passes. Native runner
reexecution and broader source configuration remain separate qualification.

The current Truss owner checkout at b8ab902 provides Weft query components but
still no public protected schema acceptance/producer/feed entrypoint. An updated
explicitly authorized capability request was successfully delivered to Truss Impl
in this iteration. Actual Truss runtime remains required; that gap does not
license synthetic acceptance/ACK or completion of the end-to-end goal.

### Caller-configured immutable JSONL workflow — 2026-10-08

The native source runner now accepts configured-jsonl with an exact pinned
source/model/intake/interpretation and explicit qualified ID binding configuration.
Configuration and all referenced bytes are rechecked during custody; configuration
SHA256 joins the original journal scope. Logical checks, publication and Weft
binding use the selected model; singleton uses the selected source/type. Built-in
CSV/JSONL/evolution paths remain available. The configured branch does not load
fixture_inputs to construct its model or IDs.

The runnable configured example has four events with development type1017 and
properties1023/1024. Actual pinned UMF validation checks three nondelete values;
local staging/apply/delete/history and exact replay leave one current entity,
four history records and one tombstone at byte1828. Fifteen focused source/binding/
evolution checks pass. Initial CLI help used system Python without the SDK;
verified SDK Python runs it correctly. Evidence:
[configured local workflow](evidence/configured-source-local-20261008.json).
No cloud statements were issued. Native configured installation/intake/publication/
query remains the next integration check. No Truss acceptance, arbitrary semantic
support, remote fencing/ACK or complete goal achievement is claimed.

### Native configured source setup and intake — 2026-10-08

Fresh namespace ashlar_e2e_private_20261008.runtime_configured_jsonl now contains
the eight current UMF-generated managed Delta carriers. All nine original CREATE
handles are terminal SUCCEEDED, with exact schema/UUIDs, zero files and closing
UUID checks. Setup performs 24 metadata reads and retains inherited permission
pages. Evidence: [configured setup](evidence/native-configured-setup-20261008.json).

Raw UMF registration also completes in that namespace using the original clean
16c35e8d943769ccfa7bb57d16785aa7159abe65 validator. Original configured schema and
intake bytes/digests match complete native registry row readback. Registry CREATE
and MERGE are terminal SUCCEEDED; five reads and retained permission pages prove
the scoped registration. completeInterpretation:false remains explicit. Evidence:
[configured intake](evidence/native-configured-intake-20261008.json).
The first offline verifier looked for BOOLEAN columns in the registry's complete
STRING projection; correcting that expected observation verified the original
false token without issuing more warehouse statements.

The three-group/four-event configured publication is now running from its
original /private/tmp/ashlar-configured-stream-20261008.sqlite journal, output
/private/tmp/ashlar-configured-stream-20261008, native process handle5165. Its
results are pending; no published/query/ACK claim follows from setup/intake.
Continue observing that original handle/journal; do not restart on an observation
timeout. Existing warehouse/catalog only, no predictive-optimization, grant or
compute configuration change. Actual Truss catalog/producer/feed remains required.

### Configured source first native publication verified — 2026-10-08

The original configured-source native publisher process5165 remains active.
Its first complete transaction is durably published at byte815. All four original
exact-version native inventories equal independent reconstruction from the original
configured source: two objects using source configured-example/type1017 and
property1023/1024, two whole-source history records, empty edges/tombstones.
Nine original mutations are terminal SUCCEEDED. Evidence:
[first configured publication](evidence/native-configured-publication-first-20261008.json).
This offline receipt comparison issues no additional warehouse statements.

The update and delete groups remain running/unverified. Observe the original
handle and /private/tmp/ashlar-configured-stream-20261008.sqlite journal; do not
restart or replace originals. A candidate reusable completed-run offline verifier
is retained at /private/tmp/ashlar-verify-configured-publication-pending-20261008.py;
its full-run execution check awaits the original summary before committing it as
a supported command. An initial column comparison used native types instead of
the recorded complete STRING projections; corrected before first-prefix parity.
Native query/replay/complete configured stream and real Truss remain unproved.

### Installed configured-source inspection — 2026-10-08

Configured loader/local inspection now live in the dependency-free Python library;
the installed ashlar inspect-configured-source command uses those components.
Host imports remain compatible, and actual UMF preflight reuses the same local
inspection rather than maintaining a second staging/apply/replay flow. Root README
now reflects scoped native publication/singleton/Weft evidence and directs users
to the current workflow instead of the obsolete executor-wiring next action.

Thirty-two focused local checks pass. A real fresh Python3.9.6 wheel installation
runs the actual entrypoint outside the checkout, with all44 packaged source files
byte-matching current code. Four configured events leave one current entity,
four history rows and one tombstone after unchanged replay; unknown configuration
refuses without summary output. Actual upstream three-record results remain
unchanged after the portable move. Evidence:
[installed configured CLI](evidence/configured-cli-installed-20261008.json).
The SDK environment lacked wheel/current setuptools; an offline uv build also
confirmed missing cache. A small isolated uv build resolved those build-only
dependencies and succeeded. No global Python dependency or native/cloud setting
changed. Actual UMF logical-value validation remains a separate host preflight.

Original native configured publisher process5165 remains active; its update
checkpoint1331 is present alongside815. Final delete/publication/query and complete
receipt parity remain pending. The portable-code move does not restart or alter
that process's retained configuration/model/source bytes or original journal.

### Complete configured native publication and singleton — 2026-10-08

Original publisher process5165 is terminal exit0 and has consumed all1828 source
bytes through three original checkpoints815/1331/1828. All twelve ordinal/table
exact-version complete inventories equal independent original-source reconstruction.
All27 native mutation requests/handles/responses are terminal SUCCEEDED; the first
nine exactly match the prior committed first-publication evidence. Final state is
one configured-example/type1017 object at entity version2, one delete/tombstone
for entity2, four original whole-source history records and empty edges. Original
configuration/model/installation journal scope is checked. The complete offline
verifier now runs as tools/verify_configured_publication.py, issuing no new SQL.
[Complete configured publication](evidence/native-configured-publication-complete-20261008.json)
retains original proofs and inventory handles. Publication issued1069 read statements
and retained permission observations; these are custody overhead for four events,
not a scale/performance benchmark or architecture gate.

The configured singleton process49952 is also terminal exit0. It reads the final
original publication using configured source and the sole explicit bound type1017.
All17 fields equal the independent original-source oracle, including exact epoch
microseconds, Unicode props, opaque retained large-integer JSON and byte cursor.
All27 original mutation handles/hashes and three checkpoint originals/digests remain
unchanged. It uses112 read statements and66 permission pages; no effects/progress
are advanced. [Configured singleton](evidence/native-configured-singleton-20261008.json)
retains original query/parameters, response and hashes. Existing compute, grants
and predictive optimization unchanged.

Configured immutable JSONL setup/intake/publication/update/delete/history/singleton
now has scoped actual execution evidence in a fresh private namespace. Local exact
replay is verified; native exact-repeat and configured Weft queries remain separate
unexecuted checks. Actual Truss acceptance/catalog/producer/feed, broader semantic
models, combined source orchestration and production remote fencing/ACK remain
unfinished. Full goal remains active.


## Expanded UMF generator compatibility — 2026-10-08

The actual clean UMF revision `385c8cf119a3c2e40ba59cc5f15e24edab52f3b2`
regenerates all eight current carrier SQL statements, ordered columns and layouts
identically. `tools/check_delta_generator_upgrade.ts` reproduces this comparison
against the exact original model and generated artifact hashes. The retained
[evidence](evidence/umf-delta-generator-upgrade-20261008.json) records both revisions.
The current default remains the original `fac1497a` generator: native installation
proofs bind that exact artifact digest and provenance, and must not be rewritten
when a newer generator becomes available. An upgrade is therefore a separately
qualified installation/provenance transition, not an automatic historical repin.
No SQL, settings or cloud workload was executed. The four generated-carrier and
native configuration unit checks pass; compatibility generation covers the current
eight carriers, not native acceptance of the new nested/comment/bundle features.
Actual accepted Truss schema/mutation/feed/ACK remains absent from the owner's
current implementation; the end-to-end goal remains active.


## Weft same-execution profile guard — 2026-10-08

The native compiled transport now retains the original artifact SQL allowlist
and exact named STRING parameters while enclosing each integrity/user statement
in a trusted read-only compound statement. Before the original SQL, a nested EXIT
handler observes the ANSI malformed-cast error; an exact JSON engine/build guard
then rejects drift. A missing, duplicate or changed native profile refuses before
submission. Separate profile observations, artifact admission, authority, retention
and closing buffered-result checks remain mandatory. No warehouse setting changes.

Three native probes on the existing warehouse passed: bound Unicode text, empty
result with its exact STRING descriptor, and deliberate engine mismatch refused
before an empty SELECT. Original statement handles/receipts are retained privately;
[evidence](evidence/native-weft-execution-guard-20261008.json) records their hashes
and scope. Eight focused local tests pass. The complete compiled publication
workflow with this new wrapper remains unexecuted; wider Weft families and actual
Truss runtime remain incomplete. This closes the individual-execution guard gap
for the tested script mechanism, not the end-to-end goal.

Syntax sources inspected: [compound statement](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/control-flow/compound-stmt),
[SQL scripting](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-scripting)
and [SIGNAL](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/control-flow/signal-stmt).
