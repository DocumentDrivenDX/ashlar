---
ddx:
  id: TD-005
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: CONTRACT-005
    kind: informed_by
  - id: CONTRACT-006
    kind: informed_by
  - id: ADR-001
    kind: informed_by
  - id: TD-002
    kind: informed_by
  - id: TP-001
    kind: informed_by
---

# Original historical medical local operator

## Scope and technical approach

Provide an installed, explicitly configured small local workflow for the original
historical medical pack: fresh native initialization, protected PostgreSQL outbox
publication, settled exact replay, and read-only reopen. This implements the
bounded medical representative in P08 and runnable installed workflow in P09.
The separately scoped singleton below adds one medical Weft read; it establishes
no broader medical query or graph-engine support.
Canonical production storage remains managed Unity Catalog Delta under ADR-001;
the selected private local profile does not establish production authority,
real Truss acceptance, patient-data access or clinical suitability.

Preserve the original declared UMF0.8 model, historical pack version and graph,
notices, all unknown content, absence of an authored model revision and original
lexical tokens. The source profile is `ashlar-medical-development-bindings/0.2`.
Its explicit injective development identities are not accepted Truss IDs. The
complete original corpus is 51 objects, 62 independent edges, 113 source/history
records and zero tombstones. Three Boolean lexical witnesses retain their exact
original token spelling and separately admitted Boolean values. No version
relabeling, floating-point numerical substitution or inferred missing value is
permitted.

## Governing surfaces and component ownership

CONTRACT-001 owns writer exclusion, source fencing, durable publication, replay
and protected ACK ordering. CONTRACT-003 owns physical schemas and UMF-generated
carriers. CONTRACT-004 owns retained publication reads. CONTRACT-005 owns original
UMF intake; CONTRACT-006 owns bounded diagnostics. TD-002 supplies the fresh-only
local root/lease/generated-schema composition pattern, not commerce logical
admission for another pack.

CONTRACT-001's installed historical-medical subsection owns the installed commands,
closed profiles and exact entry/configuration types. Initialization, publication,
settled replay and read-only reopen have distinct authority and mutation surfaces;
no initializer is available to publication, replay or read operations. The consumer
conformance plan owns their installed positive and refusal corpus.

| Proposed owner | Responsibility and allowed dependencies |
| --- | --- |
| `medical_producer.py` | Packaged public UMF compact-dataset and Boolean lexical calls through explicit producer/runtime configuration; exact original receipt comparison and complete original warnings/provenance retention |
| `original_medical_publication.py` | Exact medical admission, independent raw-model/graph full-row oracle, typed original source ports and existing-installation publication/replay/reopen composition |
| `original_medical_local_configuration.py` | Closed immutable selections, bounded input custody, source registration, runtime, six targets, capacity and secret-free handoff/report encoding |
| `original_medical_local_setup.py` | Fresh root/installation lease, generated-schema native creation, observed UUID/version-zero emptiness and public journal initialization |
| `original_medical_local_cli.py` | Installed lazy dispatch, finite separate-process ownership, exclusive evidence release and primary-safe cleanup |
| `medical_query_configuration.py` | Closed read/compiler/case wrapper and exact indexed compiler entry/final selection renewal; no execution authority |
| `medical_process_supervision.py` | Typed worker native phase pipe ownership under CONTRACT-001, without importing command composition or granting data authority |
| `medical_worker_capture.py` | Parent enforcement of bounded whole/native deadlines, independent capped diagnostic streams, and owned process-group cleanup |
| `medical_failure_location.py` | Passive bounded failure locations through the already acquired private output; preserves the primary failure and all closing, excludes exception payloads under CONTRACT-001 |
| `medical_commit_inspection_configuration.py` | Closed inspection wrapper and exact retained configuration/executable file pins; depends on medical configuration and file custody, not native execution |
| `medical_process_inspection.py` | Bounded Darwin process inventory and physical executable observation; depends on the inspection selection and lifecycle owners, never publisher or ACK mutation ports |
| `medical_mapped_vnodes.py` and packaged `resources/native/medical_mapped_vnodes.c` | Explicit optional Darwin kernel mapping observation through a pinned operator-built helper; bounded process capture and JSON validation, no compiler discovery or data mutation |
| `medical_commit_inspection.py` | Read-only original phase/artifact/native-manifest observation and exact proposed-manifest correspondence through existing driver/source owners |

Reuse `ashlar.medical_source.build_transaction`, public `ProducerConfig` and
invocation custody patterns, `OutboxSourceRegistration`,
`OutboxAuthorityRegistration`, `RegisteredOutboxSources`, `RegisteredOutboxAcks`,
`ExistingOutboxAuthority`, `NativeDriver`, `StoredPublisher`, `DeltaAttemptStore`
and `LocalDeltaTransport.initialize` where their exact interfaces apply. A
commerce-named admission is not a medical grant. Any necessary driver composition
extension must select an exact separately owned medical admission type, preserve
existing commerce/evolution behavior and be reconciled in CONTRACT-001 first.
No tools imports, private cross-module accesses, copied publisher, supplied
no-op policy, fake current context or caller-controlled validation bypass.
Run `python3 tools/check_module_boundaries.py` and affected strict tests.

## Intake, setup and source authority

Select all paths, digests, package/runtime versions, producer limits and operation
capacity explicitly. Recompute the original public compact finite-dataset receipt
and all three Boolean lexical receipts before native acquisition. Preserve exact
model/graph/receipt/source/binding bytes and all individual Record warnings,
supplied-dataset closure, incomplete or unverified provenance as emitted. A
receipt checksum proves correspondence, not source or writer authority. Retained
original model/graph and receipt equality must renew before effects and at closing.

The producer may retain a constructor-owned immutable witness after complete
semantic verification succeeds. Every renewal must independently prove exact
nominal types, selected owner/configuration/descriptors, original input/script
bytes and complete result bytes against that witness, including closing checks.
Changing both current bytes and mutable tracked values must not establish a new
witness. Only the repeated pure semantic comparison may be omitted while these
conditions hold; metadata decoding, plan derivation and all current native,
lease, source and ACK checks remain required. The witness grants no authority.

Fresh setup consumes the packaged checked UMF physical models/generated schemas
for the four materialization and two coordination roles. Reuse an existing local
physical variant only after complete ordered type/nullability/metadata/layout/
property correspondence is proved. No handwritten field lists or type interpreter
becomes schema authority. Observe all six actual UUIDs, version-zero empty bags
and schemas before public journal initialization; afterwards observe exactly the
public registration stage, properties, metadata, reservations and journal.
Native registration may create its defined next version. Handoff retains observed
stage/version correspondence but grants no authority. Existing roots or incomplete,
missing, replaced or foreign journals must refuse, never initialize or repair.

Protected ACK requires a real dedicated PostgreSQL outbox fixture and ordinary
writer/reader/ACK role sessions. A local JSONL source or file checkpoint cannot
replace it. Provisioning remains a separately authorized explicit operator step;
publication creates no roles, grants, schema or source registration. Bind the
medical feed/epoch, consumer, installation, scope, service/source schemas,
signatures and roles exactly. Initialization installation identity and protected
ACK scope installation must agree. One exact medical source and protected ACK scope suffice. Do not reuse another
run's batches or authority; provisioning-tool fixture conventions add no medical
acceptance requirement.

Use explicit isolated-loopback-trust development authentication with closed
libpq selections, disabled ambient PG configuration discovery, explicit host and
hostaddr, empty password, `/dev/null` passfile, disabled SSL/GSS and finite
connection/statement/lock limits. Actual current role, narrow append/read/ACK
privileges, schema/signature and source lease are independently renewed through
public owners. Loopback selection and a package pin are not remote authentication.

## Publication, replay and read-only reopen

The actual shared installation lock, native writer reservation, original registry
and source authority remain held through publication and independent closing.
Append only at head zero, then independently read back the exact complete original
transaction. Exact head-one readback may recover an append reply loss before
native publication; different bytes, epoch or further progress refuse. Native
publication follows the existing request/attempt/handle protocol, full independent
four-role oracle and durable descriptor validation before protected ACK.

`medical_publication_plan.py` owns `MedicalPublicationPlan` under CONTRACT-001.
It depends on the public medical admission/oracle and graph/effect planners,
but not on the native driver or command composition. `publication_planning.py`
owns the shared pure request/effect helpers in CONTRACT-001; the driver re-exports
them for existing callers. Move their exact behavior without changing checkpoint,
digest or elision rules, and prove existing commerce/evolution correspondence
before using them in the medical plan. Original transaction and
request correspondence are renewed before activation; generated schemas come
from `local_physical_carriers.py`, never from observed native columns.
Pure plan computation validates complete public medical admission at opening
and independently at closing, derives exact original changes through public
`changes_from_batch`, and applies only a closed nominal membership check during
graph planning. This avoids repeated producer calls for every record while
retaining complete original batch correspondence and refusal of closing drift.
It grants no effect authority or exemption from current native/source/ACK gates.
`original_medical_local_publication.py` composes the owning configured entrypoints
`publish_original_medical_local` and `replay_original_medical_local`. Publication
uses public `publish_outbox_transaction`, the shared attempt store and stored
publisher backend; this module cannot implement a second phase or ACK protocol.

The command composition binds its exact plan and capture limits once through
`bind_original_medical_publication`, after existing registered driver construction
and before acquiring the writer. Binding retains the original complete active
request/effect/oracle view; it cannot activate a replacement or confer authority.
The driver owns `observe_original_medical_publication` as specified by
CONTRACT-001, using the existing bounded transport capture internally. A thin
medical adapter delegates shared publisher ports and requires that full schema
and bag observation before ACK. Committed replay also requires the guard when
ACK is already present. The driver may import the lower plan owner, never the
command composition. Focused controls must prove schema metadata/nullability
drift refuses before ACK, original transaction/request/descriptor drift refuses,
all four role closings run after failure, and exact primary cancellation survives
cleanup. Native execution remains separately qualified.

Settled replay observes the same original request, descriptor, source progress and
ACK; full native histories and bags remain unchanged. Replay may reconcile a
missing required ACK only through the governing publisher recovery protocol, and
must not be described as read-only verification. `medical_publication_sources.py`
owns the narrow `OriginalMedicalSourceOwner.read_original` port in CONTRACT-001.
Replay selects this ordinary reader port; it cannot reach the append path or
writer connection. Controls prove head-one full-byte readback and zero writer
acquisitions, including on missing source, drift and cancellation.
Read-only reopen uses existing
registry/reservations and an independently selected immutable publication, proves
current source/ACK authority with existing-record SELECT readback, holds actual
pins, and compares complete four-role bags. It creates no native tables, request,
publication, source progress, ACK advancement or journal repair. Current schema,
UUID/location, retained versions and complete native custody are mandatory for
old reads too; no mutable-latest fallback.

`medical_read_sources.py` owns the separately typed `MedicalReadSourceFactory`
specified by CONTRACT-001. The driver applies the same complete read reservation
and ordinary source/ACK gates to that type while requiring exact `MedicalAdmission`.
The owning medical reopen composition renews all acquired native and ordinary
intervals independently at opening, use and closing, compares the independent
four-role medical oracle, and retains the original request/manifest and versions.
It imports public source registration, admission, reader transport and driver
surfaces; it cannot use a commerce admission, publisher or writer to establish
read-only authority. Controls cover nominal type/profile mismatches, full target
and vector correspondence, drift at use/closing, and refusal of every mutation
surface. Reuse of the shared gate must preserve existing commerce read controls.

`original_medical_local_read.py` owns configured reopen composition.
`medical_read_observation.py` owns the separate bounded medical read binding and
complete native observation. The driver exposes
`bind_original_medical_read(plan, bounds)` and
`observe_original_medical_read(descriptor, context=...)` under CONTRACT-001.
Read-side manifest validation, snapshot inspection and closing use this binding
without activating publisher policy or falling back to unbounded captures.
The local source factory constructs only ordinary reader and existing ACK-record
SELECT ports; it cannot acquire a source writer. Prove complete six-role schema
and history correspondence and cumulative four-bag limits, including closing
drift and capacity refusal, before emitting the read receipt.
Medical PostgreSQL identity gates require the actual complete read binding and
same acquired manifest vector, gate, registry, source and closing owners; they
perform no full Spark snapshot loop. Full native qualification remains in the
hold's independent opening/closing validation and separate all-path medical
closing. A source-only graph identity port cannot grant this medical read binding
or release results. The generic commerce source snapshot path is unchanged.

## Bounds, lifecycle and diagnostics

`OriginalMedicalRecoveryConfig` selects a pinned request and the existing
installation under `ashlar-original-medical-local-recovery/0.1`.
`recover_original_medical_local` in `original_medical_local_publication.py`
composes ordinary head-one original readback and the existing shared publisher
recovery owners. It performs no source append or initialization. Require complete
existing journal/plan/ordinal custody and exact retained request correspondence
before effects; inspect committed originals and reconcile uncertain handles
through their owning ports before proceeding. A completed recovery must preserve
source head/batch identity, validate all four bags and six schemas, durably publish
and ACK, and close independently. Test missing/conflicting custody, interruption,
cancellation and recovery reentry as well as actual partial-journal continuation.

The preflight-only host entry and installed command in CONTRACT-001 reuse this
same recovery composition and supervisor with a native ceiling of 60 seconds.
Branch after complete preflight and before publisher construction. Preserve
ordinary original readback, writer exclusion, complete prior/current captures,
committed ordinal inspection and every independent closing. The stage timing
owner receives only closed stage/role/ordinal selections and an exclusive bounded
output sink; it obtains no publication or ACK port. Persist begin records before
checked actions so a killed worker retains a useful incomplete prefix. End records
measure monotonic duration without exposing exception text. Timing failure cannot
skip cleanup or replace an existing primary failure.

Focused controls must prove the explicit branch cannot reach publisher or ACK,
reject excess deadlines, retain safe bounded prefixes on cancellation and drift,
and preserve all source/native/runtime closings. This changes observation and
composition, not publication state transitions: MED-F2–F4 and MED-L1 remain the
safety/liveness obligations. No new state-space exploration is required for
unchanged publication transitions; actual native recovery still requires separate
evidence. Disabling this additive command leaves retained source and Delta state
intact.

The commit inspector in CONTRACT-001 branches after the same complete recovery
preflight and before any publisher construction. Its separate configuration owner
retains the whole nested recovery selection plus original execution and process
observer pins. The installed parent and worker renew that wrapper independently;
forwarding only its recovery fields cannot omit inspection custody. The native
observer reuses the stored publisher's pure applied-artifact validation and actual
attempt/native/source read owners rather than reconstructing a descriptor grant.

Process observation runs under the original installation lease before Spark
acquisition and again at closing. Physical OS image observation distinguishes the
selected JVM from unrelated applications; only the inspector's owned process
lineage may be excluded. Raw process arguments are not diagnostic output. The
report carries the bounded development writer/process-visibility assumptions and
cannot authorize a prospective mutation. Cancellation, candidate identity races,
changed original pins and incomplete native/source closing withhold the report.

This read-only slice preserves MED-F2–F4 and MED-L1 without adding publication or
ACK transitions. Scoped assurance consists of semantic ownership review and
focused controls proving no publisher/effect/ACK reachability, exact proposal
correspondence, process uncertainty refusal and all-path closing. Native inspection
qualifies only its actual observed outcome; it does not complete publication or
reconcile missing intent. Disabling the command leaves original custody intact.

Failure diagnostics must expose a bounded, closed classification of the owning
failure and observed worker status/native phase, without exception text, stack,
SQL, source values, paths or credentials on public streams. Distinguish native
and whole-worker deadlines at the actual supervising guard. Missing evidence
remains unknown. Preserve the original exception and cancellation identity even
if diagnostic retention or cleanup fails; diagnostics never authorize success.
The existing bounded private `medical_capture` is the source of observed facts,
not a basis for reconstructing a capture discarded by an earlier invocation.

Select an explicitly compatible qualified Python/Spark/Delta/JDK/JAR version and
origin profile, local[1], 512MiB and an explicit native child limit within the
900-second ceiling. Historical
runtime qualification and any newly selected profile require their own evidence;
a newer qualified Spark profile is not excluded. Portable Ashlar imports retain
their Python support independently of selected native SDK origins. Configurations own finite
input, producer, row/cell/total native capture, stream, receipt and whole-invocation
ceilings with no ambient defaults. Enforce bounds before reading, buffering or
serialization; an oversized SQLite journal is not an operation-intent capacity
violation. The explicit 8MiB operation-intent profile does not enlarge unrelated
source, capture, journal or core effect limits.
`journal_bytes` selects the canonical SQLite main-file budget, at most the
carrier's existing 128MiB ceiling. Enforce the selected value before opening and
on every lease renewal; lower selections remain lower. This is separate from WAL
and total disk footprint. Keep the source corpus, native cell/total limits and
selected native deadline fixed when selecting a sufficient journal budget.

### Original medical effect proof

Use the CONTRACT-001 medical effects profile to reference the complete original
source-plan row instead of duplicating its generated SQL and prior anchors in
each phase payload. Keep full source/effect plans and native receipts in the
unchanged five-table journal. The driver resolves and compares the exact full
original plan, observes committed ordinal custody and invokes the public current
native receipt inspector before accepting the reference. Recheck at closing;
missing custody cannot authorize reconstruction. Preserve compact ordinal
receipts, elisions and the full manifest in the artifact, and preserve every
legacy effects profile and the shared publisher's phase protocol.
Measure the complete encoded artifact, committed phase and native cell sizes
against the selected bounds before phase submission. Modeled sizes alone do not
qualify native publication or replay.

### Runtime selection and custody

`OriginalMedicalRuntime.qualification` selects the closed JSON correspondence
record `ashlar-original-medical-runtime-selection/0.1`. Its exact members are
`profile`, `runtime`, `imports`, `selected_files`, `namespaces`, `evidence` and
`scope`; `scope` is `selected-runtime-correspondence-only`. Reject duplicate or
unknown members, nonfinite numbers, Boolean/integer aliases and ambient selectors.
The record must correspond to every explicitly selected runtime version, origin,
executable, JAR and input configuration. Its bytes are limited to 1MiB. Separate
referenced descriptors may use up to 8MiB each; their selection must be bounded
before reading. These limits do not enlarge native capture or invocation limits.

Reuse qualified runtime provenance through selected metadata/import-origin/cache
file pins and complete namespace descriptors. Preserve the descriptor owner's
actual semantics: complete dictionaries for filesystem entries and startup files,
ordered link records, and ordered JAR/import selections. Check exact symlink
lexical and resolved origin correspondence where the selected profile requires
it. Do not replace full namespace correspondence with a partial inventory,
blanket cache exclusion, reset, or a fresh whole-SDK/JDK payload hash. Every
referenced evidence record retains its original bytes, digest and limited scope.
A selected record's claimed success does not establish qualification; Spark-only
guard evidence cannot qualify Delta publication or source/ACK authority.

`OriginalMedicalRuntimeCustody.open(runtime, inputs, bounds, producer)` requires
the exact configuration owners and admits the closed record before native SDK
imports. It exposes unchanged model/graph/baseline bytes, `jar_paths` and the
explicit native environment. `renew()` independently checks all acquired input,
executable, resource, namespace and evidence ownership. `verify_imports()` checks
the actual selected Python process and loaded SDK origins/versions without
starting Spark or a JVM. The initializer separately verifies the fresh session's
actual version, effective local master, memory and JAR configuration.

Select Python's Spark authentication socket timeout as 15 seconds and transfer
buffer as 65,536 bytes in both the launch environment and effective Spark
configuration. On Darwin, explicitly select the UID-derived CoreFoundation
startup encoding value without reading ambient preferences. Unexpected native
environment entries or changed selected values must still refuse execution.

Failed opening, import, body or closing must attempt independent renewal and
cleanup for every acquired resource while preserving the original primary or
cancellation. No usable owner or successful receipt escapes failed construction
or closing. Closing repeats import checks only after that interval was acquired;
it does not run a producer, native operation or connection.

Installed native commands own isolated process-group supervision and descendant
reaping, not just a Python deadline around JVM work. Current sessions cannot be
silently reused. Renew actual source/native/lease/runtime/config custody on every
acquired interval, including opening failure, body failure, timeout and cancellation.
The parent measures the configured whole-invocation deadline from worker launch
and the separately selected native deadline from the worker's entry into native
session acquisition. The native interval includes session opening, work and
cleanup; it ends only after successful session/context cleanup. A bounded private
control channel identifies that interval independently of stdout/stderr. Missing,
duplicate, out-of-order or malformed control messages refuse; a successful child
exit cannot conceal an unfinished native interval. Timeout or cancellation kills
and reaps the owned process group, including descendants that outlive the worker.
These limits belong to this medical profile and do not alter another workflow's
deadline or reuse a current Spark session.
The CLI injects the CONTRACT-001 `OriginalMedicalNativePhase` through each host
entry to the native session boundary. Lower entries retain their externally
supervised usage; only the installed parent owns the two process deadlines.
Attempt independent closing checks while their leases/pins remain live; release
resources afterwards through primary-preserving cleanup. Retain incomplete owned
state on uncertainty; evidence failure does not imply rollback. Release bounded
final receipt only after required closing and cleanup. CONTRACT-006 diagnostics
contain safe phase/category/cleanup references, no DSNs, credential values or
connection locals. Complete raw evidence remains private and bounded.

## Properties and acceptance

| Property | Required witness |
| --- | --- |
| MED-F1 Original meaning | Exact original UMF/model/graph/113 event bytes, all 48 qualified properties, Boolean token/value pairs, unknowns and warnings survive admission and native readback |
| MED-F2 Current authority | Real ordinary source roles/signatures/scope, held native writer/installation lock, six observed UUIDs and current retention/pins admitted at entry, use and closing |
| MED-F3 Durable handoff | Complete independent 51/62/0/113 materialization bags and original descriptor precede protected ACK; uncertain outcomes retain original request/handles |
| MED-F4 Exact replay and reads | Settled replay changes no native history/bag/progress; fresh-process old-publication readback returns the same complete bags or explicitly refuses lost custody |
| MED-F5 Semantic witness correspondence | Complete constructor verification plus unchanged exact typed input/result/script bytes and selected owners/descriptors preserve the same interpretation; simultaneous tracked-value/file mutation refuses, and unrelated witness drift cannot suppress original input/result/producer closing attempts |
| MED-L1 Bounded completion | Finite supervised effects, all-path independent closing and cleanup precede receipt; original primary/cancellation identity survives secondary failures |

Tests exercise actual owners with small inert seams: exact public producer calls,
original byte/token drift, malformed/unsupported receipts, missing/null/value,
identity/endpoints/conflicts, typed scope/role/signature mismatch, ambient PG
refusal, shared-lock conflict, generated schema drift, fresh v0-to-registration
stages, missing journal, source head-one exact reuse, unknown commit, ACK ordering,
replay immutability, opening/body/closing cancellation and output no-clobber.
Independent expectations read original model/graph lexical values directly, not
transaction/compiler output. Formal properties map to owning guards and tests;
no abstract model substitutes for native acceptance.

Small installed acceptance uses a fresh offline toolkit installation outside the
checkout with no tools or hidden temporary provider imports: explicit fixture,
fresh six-table initializer, public publisher, settled replay, stopped/reopened
read-only process, complete original four bags and protected ACK byte/scope proof.
Retain original requests/receipts/native observations, opening/closing custody,
runtime versions and elapsed time as functional evidence, not a benchmark.
Only the separately scoped singleton can establish medical Weft execution. No
engine capability is implied. General interrupted eight-run
recovery, the shared P07 engine corpus, P09 managed replay/query and P10 actual
Truss acceptance/feed remain separate requirements.

## Implementation sequence and compatibility

Reconcile the owning contract/test boundary, then package public producer and
independent oracle; review generated physical correspondence; implement closed
config/setup/operator/CLI with focused controls; independently review source and
installed recipe before bounded native acceptance. Existing source-only commands,
commerce/evolution profiles and all prior evidence remain unchanged. Disabling
additive commands affects future setup only; it must not delete retained source
progress or native state. Unsupported public capability or ambiguous custody
refuses explicitly and retains originals for investigation.

The optional mapped-vnode visibility profile follows CONTRACT-001. Its native
helper uses the selected SDK's `proc_regionwithpathinfo` and `proc_bsdinfo`
structures, rather than guessing ctypes layouts. The Python owner bounds both
streams and owns termination/reaping. The strict primary-image profile retains
its behavior. The mapped profile's explicit cooperative assumption excludes
self-unmapping or modified interpreters; anonymous mappings stay unknown.
Kernel observation follows Apple's public
[region-path implementation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/proc_info.c)
and [mapped-vnode comparison](https://github.com/apple-oss-distributions/xnu/blob/main/libsyscall/wrappers/libproc/proc_listpidspath.c).
The high-level path-list wrapper's ignored per-process errors cannot establish
absence. Unit controls cover matching executables, partial/error observations,
process reuse, bounds and closing drift; an actual same-user probe plus matching
Java positive control qualifies only the selected development observer.

Prospective manifest reconciliation follows CONTRACT-001's separate command and
authorization selection. `medical_prospective_manifest_configuration.py` owns the
closed wrapper and exact pinned authorization, original inspection and proposal.
`medical_prospective_manifest.py` owns its immutable authorization sidecar and
new manifest operation namespace, delegating SQL, intent preparation, native
mutation and uncertain-response receipt recovery to the existing Delta owners.
The composition retains the real lease, source, process observer and full
original medical validation. `StoredPublisherBackend.reconcile_commit` owns the
same existing committing-to-committed transition through an explicit prospective
store port; ordinary commit recovery keeps its historical-intent requirement.
The medical composition then uses the existing guarded original ACK and full
publication observation. A committed or acknowledged continuation validates the
same new receipt before existing replay/ACK handling, without repeating fresh
absence conditions or creating another intent.

MED-F2 and MED-F3 apply to this added authorization guard: exact original effects
and proposal, durable new intent before SQL, same operation on reply loss, and
protected ACK only after native receipt plus committed attempt. MED-F4 and MED-L1
continue to require exact replay and all-path cleanup. Scoped assurance uses
semantic review of these guards and small execution controls for reachable
success, lost response, wrong authorization, native/source/process drift and
cancellation. Phase states and the original ACK protocol remain unchanged;
this slice adds a separately admitted committing-to-committed owner, with actual
installed native acceptance required to qualify reconciliation. No abstract
model or read-only inspection substitutes for that acceptance.

The journal inspection alternative follows CONTRACT-001's independently selected
version-specific write-ahead premise. `medical_journal_inspection.py` owns exact
lineage selection, read-only snapshot-prefix comparison and current file/lease
binding; configuration parsing has its own owner. A closed dispatcher admits
only process or journal inspection configurations and nominal owners. Composition
keeps every existing native preflight, manifest observation, source and ACK guard.
Journal results use their own evidence envelope and never imply process absence.
The historical exclusive-storage/no-rollback premise remains an explicit private
development assumption. Snapshot endpoints prove preserved recorded rows, not
historical physical-file continuity. Unknown code, incomplete known executions,
missing baseline rows and unexplained new operations refuse before new intent.
MED-F2/F3 and the existing committing-to-committed/ACK ordering remain unchanged;
small tests cover exact prefix admission, lost/changed rows and unsupported code,
with native installed execution required for a publication/ACK support claim.

## Publication-pinned original singleton

`query_original_medical_singleton(read_configuration, compiler_paths)` composes
one fixed original required-STRING singleton with the existing read-only operator.
Its nominal selection is consumed only inside the actual medical source interval
and complete publication hold. It uses indexed f36/0.4.2 original compile and
recompile admission, complete original obligations, generated native schema
correspondence and the original graph oracle. Fresh session-local external Delta
aliases preserve compiler SQL while retaining the actual installation UUIDs and
locations. The buffered query evidence joins the existing receipt only after all
source, native, reader and Spark closings. Missing canonical publication authority
refuses; inspection proposals do not substitute for publication receipts.

## Finite full-row singleton and complete two-hop batch

`query_original_medical_cases(read_configuration, compiler_paths, cases=...)`
selects `singleton`, `two-hop`, explicit `filtered-limit` and `all-objects`; the default remains
singleton and two-hop. It preserves the existing required-key-only
singleton entrypoint. One source/native reader and Spark session own all selected
statements and final closing. The full-row case projects the five original
encounters Fields and reconstructs its original typed key and values. The path
case composes all five compatible original relationship pairs through unchanged
RELATED_PATHS statements, retains actual edge identities and compares the complete
20-tuple original occurrence bag. True truncation or a partial pair inventory
refuses.

The batch reuses public indexed f36/0.4.2 admission, executor and path decoder.
Full native/source/ACK observations bracket the batch; each statement still
executes every original guard and checks schemas/results under fresh same-vector
original graph-read renewal. Immutable buffers and exclusive private protocol/
evidence files keep the complete outer receipt within its unchanged 1MiB bound;
each retained evidence file is separately bounded by that receipt limit and the
whole batch by 4MiB. Files are retained and renewed only after all owner closings.
The order/limit case selects the original encounters type, projects its complete
row, orders by the original STRING key and applies unchanged `LIMIT 2`. Its
independent population witness is one row; it does not qualify arbitrary predicate
filtering, binding-limit behavior or truncation.
The all-object case includes all eight record projections and reconstructs the
complete 51-object bag, including isolates, original keys, full values and eight
present-null states. Its medical source owner binds the actual admission, public
dataset and held vector, validating the complete original mathematical-integer
source population before representability or user SQL. Original Boolean receipts
and explicit present-key null encoding preserve their distinct source meanings.
All-edges, one-hop and grouped-count remain outside
this finite query profile; no production, Truss or clinical authority is implied.

### Installed batch query composition

The public `query-original-medical --configuration PATH` command selects a closed
`OriginalMedicalQueryConfig` through `medical_query_configuration`. The wrapper
contains an existing `read` configuration, explicit compiler `index` and
`installation` paths and explicit finite `cases`; it delegates read runtime,
inputs and bounds without adding ambient defaults. `MedicalQueryBatch` owns case
validation and the public `query_original_medical_cases` SDK owns execution.

`original_medical_local_cli` owns the shared fixed `query` worker dispatch, parent
runtime custody, native phase/capture, descriptor-one/Python stdout isolation and
strict result framing. Both parent and worker compare the complete selection and
verified compiler ready bytes at entry/final closing. The compiler is verified
afresh for each compilation as well. Compiler paths cannot authorize installation
or another trust profile. This composition must preserve one full-vector read
interval and all batch/outer closing before protocol evidence and receipt release.
