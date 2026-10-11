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
It establishes neither new medical Weft queries nor new graph-engine support.
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

CONTRACT-001's installed historical-medical subsection owns the four commands,
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
`original_medical_local_publication.py` composes the owning configured entrypoints
`publish_original_medical_local` and `replay_original_medical_local`. Publication
uses public `publish_outbox_transaction`, the shared attempt store and stored
publisher backend; this module cannot implement a second phase or ACK protocol.

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

## Bounds, lifecycle and diagnostics

Select an explicitly compatible qualified Python/Spark/Delta/JDK/JAR version and
origin profile, local[1], 512MiB and a 300-second native child limit. Historical
runtime qualification and any newly selected profile require their own evidence;
a newer qualified Spark profile is not excluded. Portable Ashlar imports retain
their Python support independently of selected native SDK origins. Configurations own finite
input, producer, row/cell/total native capture, stream, receipt and whole-invocation
ceilings with no ambient defaults. Enforce bounds before reading, buffering or
serialization; an oversized SQLite journal is not an operation-intent capacity
violation. The explicit 8MiB operation-intent profile does not enlarge unrelated
source, capture, journal or core effect limits.

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

Failed opening, import, body or closing must attempt independent renewal and
cleanup for every acquired resource while preserving the original primary or
cancellation. No usable owner or successful receipt escapes failed construction
or closing. Closing repeats import checks only after that interval was acquired;
it does not run a producer, native operation or connection.

Installed native commands own isolated process-group supervision and descendant
reaping, not just a Python deadline around JVM work. Current sessions cannot be
silently reused. Renew actual source/native/lease/runtime/config custody on every
acquired interval, including opening failure, body failure, timeout and cancellation.
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
No medical Weft or engine capability is implied. General interrupted eight-run
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
