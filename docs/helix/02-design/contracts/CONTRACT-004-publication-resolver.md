---
ddx:
  id: CONTRACT-004
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-002
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: FEAT-003
    kind: informed_by
---

# Contract: publication resolver

Proposed ashlar-resolver/0.1; Python core candidate. This applies the existing
publication/read boundaries; it does not add a production writer protocol.

## Interface

`resolve_publication(backend, publication_id, required_tables, context=...,
supported_profiles=..., supported_revisions=...)` returns an immutable
ResolvedPublication or raises. required_tables maps fully qualified table names
to trusted native UUIDs; it includes all hydration/structural dependencies.
Supported profiles/revisions and UUIDs are trusted configuration, never supplied
by the untrusted descriptor. Versions are exact nonnegative signed-int64 integers;
bools, floats, strings, unsafe names and malformed unconsumed vector entries
are refused. Optional unconsumed tables need not be physically available.

The backend methods are mandatory: authorize, descriptors, validate_descriptor
and inspect_snapshot. authorize authenticates context and applies effective
caller policy before descriptor access; a caller principal string is not proof.
descriptors reads authoritative rows for one bound publication ID. Exactly one
row must match that ID and an explicitly supported profile. All four descriptor
JSON columns are decoded with duplicate-member and non-finite token refusal.
Original row text is retained, unknown metadata preserved, integers exact and
non-integer numbers parsed as decimal values without binary float narrowing.

validate_descriptor verifies descriptor custody, complete source/projection
validation, progress and effective policy according to the selected profile.
Stored validation flags alone are not trusted evidence. inspect_snapshot returns
Snapshot(table, uuid, version) only after verifying native identity, pinned schema,
protocol suitability and retained data availability. Missing/expired snapshots
or unavailable evidence refuse the whole plan; no latest-version fallback occurs.

## Native backend

NativeBackend accepts an authenticated Executor, trusted Policy, allowlisted
manifest table/UUID and per-consumed-table schema column/type sequences. Executor
query returns SQLResult(rows, columns), with complete untruncated rows and native
schema metadata. The integration must normalize column/type metadata to the
configured profile and propagate failures; it must not report success after a
query failed or timed out. No retry of writes exists because this component
submits only DESCRIBE DETAIL and SELECT statements.

The backend checks manifest UUID before/after bound descriptor lookup, probes
consumed VERSION AS OF metadata and compares exact column/type sequences, then
checks table UUID again. Policy.validate_snapshot must independently validate
protocol, retained data-file availability and active pin custody: LIMIT0 proves
only pinned metadata, not readable retained files. There is no default trusting
implementation of this policy. It must refuse unavailable evidence.

## Failures and limits

ResolutionError denotes invalid, ambiguous or unsupported resolution. Trusted
adapter policy/transport exceptions propagate; the read executor maps them to
CONTRACT-002 outcomes without leaking protected existence. No partial result or
partially resolved plan is returned. Published progress remains opaque exact
profile data; it is not flattened into a global scalar or fabricated freshness.

ResolvedPublication is an immutable plan input, not a durable lease or final
read response. Before consuming it, the execution adapter must preserve/recheck
pin custody and effective authorization; namespace replacement or policy change
between resolution and execution must refuse or require renewed resolution.
Simulated backend checks cannot qualify live authentication, retention, data-file
custody, native execution or concurrency. UMF schema meaning requires its pinned
shared producer and independent target admission.

## Arras authored-read integration boundary

The first Arras bridge may call `read_singleton` only for an explicit original
publication ID and complete `(source, kind, type ID, entity ID)` identity. The
host supplies the current person's authenticated executor, backend, policy and
full pin vector. That operation returns a row or authorized absence; it does
not compile Weft, issue a source position or compare a Truss receipt.

The existing bounded local authored-Key page path uses an original configured
UMF 0.10 graph publication, full vector and local Spark aliases:
`configured_typed_graph_sdk_request` → Weft
`compile_engine_page_json` → `EngineRead.from_original` →
`AshlarTarget.page`. Its Backend02 `weft-sql/0.2.0` request binds required,
facetless STRING keys/properties and STRING parameters. It is a local
`ashlar-delta/0.3` candidate, not a person-scoped managed `.4` execution
receipt. The original compiler artifact, result model, current policy and
complete held publication vector must agree; cursor and rows share that
vector. `AshlarTarget.read/page` require original compiler catalog entries
and do not accept a raw Arras `Statement` as a generic query.

`AshlarTarget` recognizes the selected
`ashlar-managed-current-core/0.2-xxhash64` page target for
`ashlar-delta/0.4-xxhash64`, and `managed_current_sdk_pages_request` is a
separate candidate builder. Arras's bounded Backend02 mapping can supply its
accepted source bundle and exact `weftBindingInputs` to that builder, compile
the original result with `compile_engine_page_json`, make
`EngineRead.from_original` and call `AshlarTarget.page`. Ashlar still lacks a
native-qualified production current-person managed session/policy owner and
execution proof for that composition.
`ashlar_host.managed_current_person_read.ManagedCurrentPersonPageOwner` is a
separate local candidate opener for this selected `.4` Backend02 page subset;
its `backend02_target(compiler, entry)` constructs the held `AshlarTarget`:
the host MUST supply a Spark session authenticated as the current person, an
exact observed runtime/namespace, the accepted original source bundle and
retained managed publication receipt, and a mandatory policy that rechecks
effective UC row/column rights at opening, request, result and closing. The
opener MUST independently reopen the exact manifest, full four-role vector,
six-role native inventory, accepted source progress and selected physical
profile; only compiler SQL with exact typed parameter carriers may execute,
under a finite deadline and row cap. The policy revision is part of page
continuation context and a change MUST refuse the read. This candidate does
not create compute or claim production source authority. The campaign
`ManagedCurrentReadOwner` requires an oracle and expected rows and MUST NOT be
used as an ordinary runtime opener. A local alias or `.3` receipt MUST NOT be
relabeled as managed `.4` support.

No public owning mapping turns every Arras `ModuleInfo` and raw `Statement`
into a generic query; the bounded Backend02 composition requires original
source provenance and explicit selected-profile evidence. Backend03 grouped
queries use a different
`weft-sql/0.4.1` compiler/request boundary and require separate mapping and
execution qualification. Until those bindings exist, Arras must advertise
only its independently proven pinned singleton bridge and refuse broader
query profiles. Published `source_progress_json` and `recorded_at` may supply
snapshot-bound freshness facts only after the same held policy/vector checks;
they cannot substitute for receipt inclusion proof.

## Active-pin registry substrate

The private PostgreSQL ashlar_pins registry retains immutable pin identity,
authority, manifest/cursor/recovery/release scope, native table name/UUID/version
and original custody digest. Explicit release retains the original row; exact
registration replay preserves it and released identities cannot reactivate.
Pin rows have no automatic release; publication readability follows the finite
configuration-based window below. Ordinary writer/reader/maintenance roles
have scoped function/read permissions, with no direct table mutation.

Registration and release share a native table lock. assert_unpinned holds a
conflicting SHARE lock until its transaction ends and refuses an active exact
version. Registration must independently prove the target/source/custody and
retained data availability before committing. A maintenance operator must hold
the guard transaction throughout retention and check every affected version/file
in the complete union of all pin scopes. One checked version cannot authorize
removing files required by another version. Every external Delta maintenance path and privilege must be bound to the guard. Registry presence alone is not
proof of future retention or valid resolver policy; the existing mandatory
validate_snapshot contract remains unchanged. Require native role, replay/conflict, rollback and retention-refusal controls.

PostgresPins/PinVector provide complete scoped vector registration and held
read custody. assert_active validates every original field and holds native
SHARE exclusion through resolution/execution; registration/release cannot
commit against it. The injected host transaction adapter must preserve the
transaction through context yield and roll back exceptions. Current read
authorization is checked before and after consumption, while pin inventory is
revalidated before release. Competing release MUST be tested against the held complete native vector. This supplies one custody component of validate_snapshot; protocol,
retained availability and effective external retention policy remain mandatory
independent checks. It does not turn the recovery fixture into a publication.

## Singleton execution composition

read_singleton holds a complete PinVector read context around resolve_publication
and exact-version native point execution. The descriptor-to-pin custody policy
is mandatory and independent; every held version must match the resolved vector.
A bound lookup_hash improves pruning but complete source/typed identity predicates
remain authoritative. Duplicate native identities refuse. UUID checks and current
authorization run before returning; absence also undergoes row policy. The final
read result is released only after the pin context’s closing checks. Resolution
and execution failures return no partial singleton response. Native composed publication and policy support require cited execution evidence.

For the proposed `ashlar-delta/0.4-xxhash64` table profile in
[TD-007](../technical-designs/TD-007-native-xxhash64-lookup-layout.md), the
reader computes the bound pruning value with native SQL `xxhash64` over
explicit STRING/BIGINT/BIGINT casts and compares a signed BIGINT. It still
filters and checks the complete source/type/ID tuple. The legacy SHA-256 hex
reader remains selected only for its original table profile. A descriptor or
pin vector crossing the two layouts refuses before returning a row or absence.

## Singleton timestamp output carrier

For the selected ashlar-delta/0.3 object/edge layout, read_singleton MUST project
published_at with cast(unix_micros(published_at) AS STRING) and exclude the raw
TIMESTAMP field from the wildcard. The returned published_at is either SQL NULL
or canonical signed-int64 decimal text of microseconds since the Unix epoch.
It is not ISO datetime text, milliseconds, a float or a JavaScript/Python date.
Positive zero is "0"; negative zero, leading zeros, decimals, missing fields,
non-string values and out-of-range integers MUST refuse. At most 20 characters
are interpreted. This carrier is independent of session timezone and preserves
all six fractional digits of the native TIMESTAMP instant. The input/native
schema remains TIMESTAMP; generation/storage are not changed. Other fields and
opaque source JSON remain original retained text.

Consumer timestamp interpretation MUST use epoch-microsecond text consistently;
historical millisecond responses cannot establish precision. Qualify null,
negative, signed64-boundary and edge timestamp carriers independently.

## Native permission observation boundary

Current native observations MUST verify owner and complete effective permissions
for every target. validate_writer_inventory must refuse unexpected owners,
writers/delegators, unknown privileges and incomplete grants. Saved grant pages
are evidence, not a lock. Local cooperating-process locks and PostgreSQL pin
rows do not fence arbitrary remote Delta writers. Production publication claims
require an independently admitted authority lane; administrative trust, retention
operator enforcement and active-pin custody remain separate obligations.

### Conservative whole-table cleanup guard

assert_table_unpinned checks every active pin authority/scope/version for an exact
native table name/UUID and holds the registry SHARE lock through the transaction.
It refuses whole-table cleanup when any such pin is active. This conservative
boundary avoids substituting one caller-selected unpinned version for the complete
affected pin/file union. PostgresTableGuard supplies the required authenticated
transaction and explicit guard authorization port; it submits no Delta cleanup.

A host must independently bind actual physical cleanup targets/UUIDs and every
operator to the guard, retain locks throughout confirmed terminal work and keep
original remote outcome/containment custody. A lost transaction or uncertain
remote cleanup requires quarantine/recovery; releasing the SQL guard does not
prove remote termination. This new guard does not close those obligations or
establish retained file availability. No TTL, pin release, VACUUM or retention
policy change is performed by the implementation or native fixture checks.


### Durable cleanup uncertainty quarantine

The private registry additionally retains an immutable original cleanup operation,
table/UUID and exact intent bytes before remote work may start. A pending marker
blocks ordinary pin registration across every name/version/scope of the UUID,
including renamed aliases, even after the registering host connection has closed.
Marker creation refuses every existing active pin of that UUID and uses the same
native pin-table lock as registration/release. No timeout or automatic expiry
clears uncertainty.

Only the separately granted recovery role may retain original terminal custody
and close pending metadata; the maintenance role cannot close it. The host
CleanupQuarantine port requires explicit current scope/authority admission and
independent original native terminal/outcome verification before that close call.
PostgreSQL preserves bytes and role separation; it cannot itself prove a remote
Delta job terminated. Closed original operations cannot reopen; different original
or terminal bytes conflict. Every cleanup operator and physical target must still
participate, and actual remote verifier/containment qualification remains required.
Metadata-only controls run no cleanup and are not evidence of Delta termination.

## Configuration-based publication readability

Predictive optimization remains enabled. DISABLE is not a publication
prerequisite; readability follows the observed configuration and finite window.

Proposed ashlar-retention-window/0.1 stores complete per-table UUID/version, original
native snapshot committed_at microseconds and readable_until microseconds in
validation_report.retention, together with the original explicit safety margin.
The ceiling for each table is snapshot commit time plus the shorter verified
data-file/log retention duration minus that margin. Publication time cannot renew
an old snapshot. The complete vector's earliest ceiling bounds readability.

Admission uses fresh authenticated effective configurations for every published
table, verified native UUIDs, original snapshot commit timestamps and a trusted
current clock. It caps each original ceiling by the current shorter configuration.
Longer current settings cannot extend the immutable original ceiling. At or beyond
the effective deadline the publication refuses; no latest fallback occurs. Unknown
settings, unsupported intervals, mismatched UUID/version, future snapshot times
and insufficient margin/window refuse. Native defaults require an explicitly
qualified platform profile; absent properties alone do not prove settings.

The selected SQL timestamp observer admits only fixed zero-offset UTC/Etc/UTC
spellings; unsupported or ambiguous timezone interpretations MUST refuse. Retain
original observations and request/response custody. Offline replay of original
observations MUST NOT be represented as fresh native evidence.

Fixed integral seconds/minutes/hours/days/weeks are the selected parser subset.
Calendar/decimal/negative intervals are unsupported. Host policy must independently
verify native data/log availability, schema/protocol and current permissions;
configured retention does not prove that manual cleanup or an earlier shorter
configuration has not already removed files. Recheck descriptor admission after
query execution under held read custody; crossing expiry returns no response.
The host must reserve a margin appropriate to clock error, observation age and
read duration and refuse when that budget cannot be met.

Expiry changes readability only: manifests/source history remain immutable, no
source checkpoint is advanced, and no pin is automatically released. Explicit
pin retirement/cleanup accounting is separate. Autonomous predictive maintenance
need not consult Ashlar's PostgreSQL pins for this finite profile; pins do not
supply an indefinite retention promise. Manual destructive/configuration changes
remain governed by the effective policy and availability checks.

### Parity reuse inside one read interval

A host MAY reuse successful full-row parity for the same immutable Delta
UUID/version vector only within one continuously held, complete native pin
interval. The original raw descriptor, independent target expectations, current
source/writer authority and supported native snapshot semantics MUST remain
bound to that interval. The first validation MUST perform full availability and
parity checks. Reopening an interval MUST repeat those checks; a retained flag,
previous process receipt or prior query result MUST NOT establish a new interval.

Every renewal MUST still validate original descriptor/vector binding, current
source/schema/owner/effective permissions, manifest and target UUID/protocol,
actual complete pin custody and finite retention before and after admission.
Any refusal or changed descriptor/expectations MUST invalidate reuse for the
rest of that interval, even when a caller catches the exception. Closing pin
refusal MUST return no query result. The host MUST close reuse before releasing
the native pin guards. Historical schema/data immutability and exclusion of
manual destructive/configuration changes are qualified profile requirements.

This permission does not extend a retention deadline or require predictive
optimization to consult PostgreSQL pins. Autonomous maintenance remains bounded
by the freshly checked finite Delta window; manual maintenance follows the
admitted cooperating authority. Unpinned publication validation retains its full
checks. Native corruption, uncoordinated destructive administration and wider
read-policy semantics require independent evidence and MUST NOT be inferred
from fixture parity reuse.


## Finite-retention policy composition

`RetentionGate(provider, minimum_margin_us=...)` MUST be constructed with an
independently qualified fresh observation provider and an explicit host margin
budget. `provider.observe(descriptor, context)` MUST return exactly
`configurations`, `table_uuids` and canonical native `now_us`, covering the complete
original descriptor vector. It MUST independently admit configuration defaults,
physical identities, clock authority and observation age; it MUST NOT treat
manifest-supplied UUIDs or a saved metadata report as fresh native observations.
The gate MUST refuse an original margin smaller than the current host budget,
unknown/incomplete observations, tightened expiry or expired original ceilings.
It MUST NOT renew the original snapshot window.

`RetentionNativePolicy(policy, gate)` composes with `NativeBackend`. It MUST
preserve independent authorization and snapshot admission and require completed
original descriptor custody admission before observing retention. Resolver and
singleton closing checks use the same gate and MUST take new observations on
each invocation. Native schema/protocol/data-file checks remain the underlying
policy's responsibility; the wrapper supplies no permissive admission.

`RetentionManifestPolicy(policy, gate)` composes with `DeltaManifestStore`. It
MUST preserve the independent writer lane and full source/schema/effect/pin
admission. It converts exact retained manifest strings to the same immutable
descriptor representation used by reads; admission MUST NOT mutate those strings.
Manifest admission MUST repeat after exact native readback and closing UUID
inspection, while still under the writer lane. A closing refusal returns no
success and MUST NOT be interpreted as rollback: the native manifest may already
be committed. Preserve original submission/handle custody; no automatic ACK, pin
release, cleanup or resubmission with a new identity follows from that refusal.

`SQLRetentionProvider` is a bounded development provider. Its independently
admitted UUID allowlist/default profile and positive maximum observation span
are explicit inputs. It observes UUID/properties with existing bookends and
queries canonical server-clock microseconds; an exceeded monotonic observation
span MUST refuse. The gate's host margin MUST reserve at least that span budget.
This provider does not inspect original commit history or supply original
timestamp custody, authorization, file/protocol evidence or a writer fence.
For four tables it adds 13 serial SQL observations per gate check; this is not a
qualified low-latency production provider. Production hosts MAY inject a
separately qualified metadata batching/cache provider whose observation age and
configuration-change policy fit the reserved margin. Predictive optimization
remains enabled; no setting mutation or performance acceptance claim is implied.

## Weft query integration boundary

Owner-authored `weft_binding.string_compile_request` binds retained UMF 0.7
source bytes and explicit string Record IDs to the original publication and
complete native table UUID/version inventory. Selected schema alias MUST match
the original document revision. Only actual props_json homes are emitted here;
physical object IDs MUST NOT become logical keys or invented typed columns.
The qualified layout reference is the retained full delta-layout-v03.sql hash
ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e, which exactly
matches Weft's pinned owner-source copy. The packaged baseline is a subset with
its own different byte hash; this does not qualify missing typed projections.
Host admission MUST independently compare every consumed native schema to the
selected qualified owner realization before issuing integrity/query SQL.

Pinned Weft source is 2744531735c2a771fbe7ed24a7f67e3afc851b25; build only explicit
ashlar-databricks-qualified feature, backend 0.1.0-qualified and
 dbsql2026.39-qualified. Candidate opt-in is false. Unknown or stale model/binding/
profile must block without SQL. Package/build verification and compiler success
MUST NOT imply host execution or native profile admission.

The host query API MUST verify exact Databricks engine/build/settings, actual
caller authorization and original model/storage correspondence. Hold the complete
resolver publication/pin/retention context for every emitted integrity check and
user query. Buffer complete exact STRING results, decode all declared result
representations without numeric coercion, and renew context/policy before release.
Refuse unknown obligations, failed checks, partial transport, profile drift or
unsupported meanings. The nativeProfile obligation requires engine 2026.39 and
both exact build hashes from pinned Weft; no newer-engine fallback is allowed.
Compiler support MUST NOT be inferred from uncommitted security changes or
compiler success alone; host/native execution requires separate evidence.

`weft_query.read_weft` supplies the buffered execution boundary. It MUST hold
the complete pin vector across resolver admission, every emitted scalar-integrity
check, exact ordered-parameter execution, decoding and final authorization/context
checks. The pin context MUST close successfully before rows return. The selected
implementation handles publication, scalarIntegrity and nativeProfile obligations;
unknown/duplicate/additional families refuse pending explicit handlers. Mandatory
policy callbacks independently verify original compiler/model/binding custody,
native profile, caller authority, exact decoding and final row release. Library
callback presence is not evidence that a deployed policy establishes these facts.

The decoded return MUST be a completed data snapshot, including every nested
carrier. The current structural gate supports exact list/tuple/string-key dict,
None/bool/int/float/str/bytes, Decimal/date/timedelta, datetime/time with no zone
or the native UTC singleton, and detached bytearray. It refuses opaque objects,
custom subclasses/temporal metadata, UUID objects, cycles and deferred carriers;
this structural inventory does not admit new scalar domains under the selected
codec. The initial qualified string/envelope meanings remain unchanged.

Snapshot processing shares100,000 visited nodes and8MiB copied bytearray payload
across every decoded cell and the final post-authorization snapshot; depth is
bounded to64. The byte charge precedes allocation. These are component copy/work
limits, not total driver/native/process heap or transport guarantees. The final
snapshot detaches mutable aliases retained by decoder and authorizer callbacks.
Recognized generators/coroutines found within the bounded supported traversal
are closed while held, with every recognized close attempted and the original
refusal preserving the first cleanup failure as its cause. Unsupported opaque
objects, async carriers, values beyond the walk budget and incomplete cleanup
remain the qualified decoder/host's resource obligations; no permission or
complete deployed mediation follows from this structural gate.

The exact compiled-statement transport may execute WITH statements only from its
trusted original artifact allowlist. Ordinary native read routing is unchanged.
All guard results MUST be one exact STRING zero. Result names/order/native STRING
types and complete row inventory MUST match compiler descriptors. Unexpected null
carriers refuse. No numeric parameter or result conversion through floats occurs.
The initial decoder admits qualified string scalars and explicit absent/value
string envelopes bound to original Field identities; native null, duplicate JSON
members, unpaired Unicode, NUL, extra states and numeric substitution refuse.
Numeric/recursive/entity/relationship result support requires explicit admitted
handlers and separate evidence; unsupported families MUST refuse.

### Native Weft host integration

The native immutable-source runner may compile query-only Weft requests from the
original retained model/publication using the pinned qualified wheel. Host
admission MUST verify the clean source revision, loaded extension digest/path,
original entrypoint, layout digest, regenerated owner request and complete
recompiled artifact. Existing native inventory/authority/schema/retention/pin
checks remain mandatory. Exact warehouse release/build and ANSI setting MUST be
observed before execution and buffered release; observation mismatch refuses
without settings changes or profile fallback. The executing compiled statement MUST also enforce the admitted engine/build
and ANSI profile before its actual integrity/user SQL. Separate earlier metadata
observations cannot establish same-statement settings. String/presence and proved
COUNT decoding require their selected original descriptors and model/source
correspondence; unsupported families refuse.
This private development host does not establish production authorization or
complete Weft conformance. Query-only MUST NOT advance graph/source progress.

### Separately selected indexed Paths host

The Paths host is an explicit additional profile. Its command is
`query-commerce-paths`; `query-commerce` retains its selected compiler profile.
An unavailable Paths installation MUST refuse without choosing another compiler.
The application fixes the trusted index and realization and independently
observes the host platform; operator file locations MUST NOT replace that trust
selection.

Weft owns the paired `weft-compile/0.4.0`, `weft-ir/0.4.0` and
`weft-application-result/0.4.0` surfaces and `weft-backend/0.3.0` manifest.
Its authored-path contract and exact installed JSON schemas govern syntax,
logical descriptors, occurrence semantics and host obligations. Ashlar MUST
validate against those retained schemas offline, reject unknown obligations and
preserve original request, response, SQL, ordered parameter and result bytes.
An injected callback without actual schema validation does not satisfy admission.

The host MUST derive the complete request from the admitted original model,
source and selected publication vector. Preserve the declared UMF version,
including original 0.8 documents, all consumed Record/Field/relationship identities,
ordered typed keys and independent physical edge identities. Model revision,
binding digest, table UUID/version and source correspondence MUST agree.
A physical object ID MUST NOT substitute for a logical key.

The host MUST compile and independently recompile the same original request
through the retained indexed installation and compare complete artifacts before
native execution. Both compiles establish compiler custody only. Every integrity
check and user statement MUST execute inside the same original authorized
publication hold, under the exact selected native profile and source policy.
Missing, expired or drifted context MUST refuse without rows or source progress.

Capture complete bounded native STRING rows and original schema metadata before
decoding. Exact integer/decimal arithmetic MUST preserve mathematical values;
float conversion, repaired SQL/results and inferred relationships are forbidden.
Preserve absent/null/value states, parallel path occurrences and opaque identities.
Expanded path counts and distinct terminal counts MUST follow their different
original descriptors. A positive nonempty grouped answer is required evidence;
an empty grouping alone cannot qualify grouped counts.

Release a report only after final publication/policy/ACK/source checks, reader
and client closure, and Spark shutdown succeed. Query-only work MUST NOT advance
source ACK or publication progress. Cleanup MUST preserve an existing primary
failure; incomplete or uncertain closure MUST withhold success. The private local
commerce profile establishes no production Truss authority, end-user delegation,
Unity Catalog retention or cloud execution claim.

### Separately selected indexed count-star profile

An explicit `count-star` profile MUST select the independently indexed
`ashlar.databricks.paths-keys` realization
`0.4.1-count-star-having-candidate`, paired with `weft-compile/0.4.1` and
`weft-ir/0.4.1`. The application-result surface remains
`weft-application-result/0.4.0`; the backend interface remains
`weft-backend/0.3.0` and target remains
`spark4-delta4-paths-keys-candidate`. Existing profile defaults and refusals MUST
remain unchanged. A compiled older-profile response MUST refuse; malformed
preselection requests MAY retain their original blocked response envelope.

Trusted immutable index selection precedes package inspection. Regular Git
checkout source files MAY retain normal writable checkout modes; all admitted
content bytes and exact closure MUST match before and after inspection. The
installer MUST create its own readonly executable, schemas, provenance and ready
marker. Source file modes MUST NOT substitute for content custody or force an
undocumented source chmod step. Failed installation remains unavailable without
fallback. Cancellation retains its identity; a closing cancellation outranks an
ordinary prior error, while an existing body cancellation retains precedence.

All original source, policy, publication, retention, integrity and bounded
result-release obligations remain mandatory. Compiler package admission and
installed compilation do not establish native query execution. Grouped projected
COUNT(*) MUST retain complete occurrence-bag multiplicity and original ordered
parameters; HAVING MUST NOT hide a failed complete-bag integrity or capacity
check. Weft owns the exact language and row-count obligation contracts.

`install-weft-count-star` and `compile-weft-count-star` MUST expose the selected
indexed package through the public installation and original-byte transport ports.
`query-commerce-count-star` MUST use a separately typed bounded host configuration
and the pinned offline 0.4.1 request, response and IR schemas. The required original
ten commerce intents remain in the query gallery; plain row count, grouped row
count, grouped COUNT(*) HAVING and expanded occurrence COUNT(*) HAVING are added.
The five required supply-chain intents remain separate required coverage.

### Separately selected implicit arithmetic labels

An explicit `implicit-arithmetic` profile MUST select
`0.4.2-implicit-arithmetic-labels-candidate` with the exact paired
`weft-compile/0.4.2`, `weft-sql/0.4.2` and `weft-ir/0.4.2` interfaces.
Its immutable owned profile descriptor MUST bind the backend, target, public
compiler entrypoint, complete capability set and three offline schema names and
digests together. Arbitrary caller-supplied versions and implicit upgrades from
`count-star` MUST refuse. Installation requires independently admitted indexed
0.4.2 products; existing 0.4.1 package evidence MUST NOT qualify them.

Original commerce SQL, module bytes and binding bytes MUST remain unchanged.
Fresh public recompilation and exact artifact correspondence precede admission.
Weft owns expression interpretation and implicit labels. The host MAY compare
an admitted UTF8 source span with its exact original substring; it MUST NOT parse
or repair SQL to manufacture a label. Complete ordered physical carriers MUST
preserve logical names, output ordinals and exact values, including repeated
field labels. Unknown capabilities, malformed spans, name collisions and mixed
version pairs MUST refuse before execution.

The separately typed host configuration MUST retain all existing source,
publication, retention, integrity, capacity and closing obligations. Query-only
execution MUST NOT advance source progress. All four original commerce scenarios,
including the unnamed partial-return arithmetic projection, require actual
publication-bound execution and independent complete-result comparison. Compiler
acceptance alone does not satisfy this requirement.

The installed commerce 0.4.2 query profile MUST require an explicit exact ordinary
source registration alongside its ordinary protected ACK session factory. Bind
that registration to the original admitted reader, source schema/signature,
roles, checkpoint and complete native UUID/version vector before opening source
sessions. Current source admission MUST renew the original read-only journal
registry and reservation under the actual exclusive quiescent reader lock and
complete publication pins. This interval grants no native mutation, publication
or ACK submission permission. Missing or changed registration MUST refuse;
container credential discovery MUST NOT be a fallback. Existing 0.4.1 entry
points retain their selected behavior.

The installed supply-chain request builder MUST preserve all five original SQL
statements, complete UMF 0.8 module bytes, development carrier identities and
present-null property encoding. Its selected `supply_chain_count_star_request`
port derives the request from the exact original pack/model/graph and supplied
publication metadata. `compile_supply_chain_cases` MUST prepare the complete
corpus before compiler effects, retain original request/response bytes before
parsing, freshly recompile each request and apply the pinned offline 0.4.1
schemas and closed host admission. A 0.4.0 refusal cannot satisfy required
0.4.1 coverage. The independent original graph oracle MUST retain complete
result bags, including both lineage rows and independent replay events before
HAVING; it MUST NOT consume compiler SQL or native results as expected values.
These compilation ports grant no publication/source/ACK authority. Native
execution MUST separately hold the complete original source and publication
through every guard, result comparison and closing check under the lifecycle
requirements above; installed library code MUST NOT import checkout tools.

Tagged optional scalars with `nativeNull: true` MUST require explicit admission
of `value.nativeNull` and the original scalar descriptor with `absent-allowed`
availability. This selected representation preserves explicit present-null and
exact values without widening the ideal scalar type. Native SQL NULL refuses;
an absent state requires the separately admitted outer-join representation.
Expected result text MUST be derived independently from original source semantics
and the selected carrier profile; captured cells MUST NOT be repaired to match.

The finite original supply-chain source profile MUST renew the exact original
model/graph file identity, original emitted public dataset receipt and complete
independent projection through publication and query. Its native owner MUST hold
an actual exclusive cooperating installation lease and current original target,
writer and retention admission. Generic durable attempt and manifest ports govern
publication and exact replay; the finite replay receipt MUST NOT acquire outbox or
protected ACK semantics. A reader MUST reopen the original complete committed
attempts, exact descriptor and pinned native snapshots without applying effects,
replacing plans or treating nonempty tables as a fresh bootstrap. All five original
SQL statements execute unchanged under the selected 0.4.1 source and complete-bag
guards. A final successful result MUST wait for native transport, source, lease and
owned runtime cleanup under the lifecycle contract above. This private local
profile does not grant Unity Catalog or remote-source admission.

Reusable finite-pack admission MUST select an immutable, closed definition of
the original pack, model and graph digests, declared UMF version, source namespace,
complete scenario inventory and independent source/result oracles. It MUST retain
lexical integers and decimals, absent versus present-null values, qualified Field
identities and every relationship occurrence and endpoint. The selected public
UMF producer MUST validate the original supplied dataset; a definition, callback,
receipt digest or development carrier binding MUST NOT grant source or native
authority. Current source admission and original file/producer custody MUST be
renewed independently. Unknown definitions and executable equality on caller
carriers MUST refuse before effects. Shared installed owners MUST support the
original supply-chain, archaeology and ecology profiles without importing checkout
tools or silently upgrading their UMF declarations. Each profile requires its
entire original scenario inventory and complete independent bags; another pack's
successful execution cannot establish its support.

A separately versioned finite-query invocation MAY declare an explicit
`maximum_public_source_bytes` budget, bounded to 16 MiB, for the complete source
request and verification receipt. This budget MUST remain independent of the
compiler-artifact byte budget; neither may enlarge the other implicitly. Original
source bytes and complete verification content MUST remain intact. Existing
invocation profiles retain their selected bounds and unknown-field refusals.
Changing this host receipt budget MUST NOT change UMF producer limits, native
capture or execution limits, compiler admission, or source authorization.

The shared finite-pack producer MAY select UMF's experimental compact dataset
operation `validate-core-dataset-values-compact` version `1.0.0`, with
`scope: supplied-dataset-only` and `provenance: unverified`. It MUST call both
public compact validation and complete verification against independently retained
original source and input, retain the whole emitted receipt and unknown content,
and preserve the original declared core `0.8.0` semantics. Record results, Key
results and relationship target-Key results MUST each contain the sole reference
`sourceRef: "#/source"` in place of their `source` member, referring only to that
receipt's complete top-level source. Compact fragments MUST NOT be accepted as
standalone receipts. The selected compact profile MUST preserve upstream depth,
value, byte and semantic-work limits and explicit host capture bounds; projection,
truncation or a blanket limit increase MUST NOT turn a refusal into admission.
The outer producer profile MUST distinguish compact receipts from the original
full operation. Compact verification establishes supplied-dataset correspondence;
independent current source, native writer, publication and ACK authority remain
mandatory and separate. Existing full-operation profiles retain their original
API, source pin and meaning.

The selected host MUST refuse unknown capabilities or owning obligations and
preserve the original SQL, full module documents, binding text and ordered
parameter values. Its independent oracle MUST count original source occurrence
identities, including parallel edge pairs, before HAVING selection. Every full
candidate bag MUST pass original integrity and capacity checks before user SQL;
an empty result or a HAVING threshold that excludes all groups cannot discharge
those checks. The host MUST close the held publication reader, stop its owned
Spark session, and recheck source, JAR, installation and publication custody
before publishing any complete report. Cleanup failure withholds the report;
cancellation follows the lifecycle precedence above.

An optional native-result observation port MUST receive a bounded independent
copy of the captured schema and rows while source and publication custody are
held, before decoding or comparison with the original oracle. Mutating that
copy MUST NOT change the query result. Observation persistence failure or
cancellation MUST withhold the complete report and preserve lifecycle failure
precedence. Retained diagnostics do not establish a passing query or grant
publication authority.

### Explicit Paths profile with one-hop keys

`query-commerce-paths --profile paths-keys` MUST select the independently indexed
`ashlar.databricks.paths-keys` backend, version `0.4.0-paths-keys-candidate`,
interface `weft-backend/0.3.0`, target `spark4-delta4-paths-keys-candidate`.
Omitting `--profile`, or selecting `paths`, retains the original Paths profile.
Unknown profiles refuse before compiler or engine construction. The immutable
typed host configuration owns this selection; neither SQL nor operator file
locations can override trusted realization/index/platform pins. All explicit
artifact, row, cell and total-capture bounds remain mandatory. Installation or
qualification failure MUST NOT cause profile fallback or metadata relabeling.

The new profile admits required-root, nongrouped, nonaggregate `RELATED_KEYS`
over one monomorphic authored relationship with complete required String keys,
including composite keys and authored inverse traversal. Potentially unmatched
LEFT roots and mixing with path expansion refuse. The selected original logical
expression and its `relatedKeys` result representation MUST correspond exactly
in relationship, starting scan, terminal Record, ordered key Fields/types and
bound. Weft CONTRACT-005 owns the exact capability and closed obligation payloads;
no new language/IR/compile wire version is implied.

Ashlar MUST independently admit both `ashlar.relatedKeys.collectionIntegrity`
and `ashlar.relatedKeys.ordinalCapacity`, including their complete ordered output
inventories and the original pinned consumed-edge schemas. Native BIGINT identity
schema must be observed under the same publication hold even for an empty edge
table or a table absent from `binding.records`. Complete-source non-null and
unique edge IDs, endpoint/key/source correspondence, full-bag encoding and exact
ordinal capacity must pass before the user result is released. A bound or
bound-plus-one prefix never proves capacity of the complete per-owner bag.
Authored degree counts distinct neighboring Records; collection occurrences
retain independent parallel edges even when projected key tuples are equal.

`decode_related_keys(representation, raw, *, model_pins, config)` is a pure
portable API. `RelatedKeysDecodeConfig.maximum_cell_bytes` is an explicit positive
integer at most 16 MiB, excluding Boolean values. `DecodedRelatedKeys` retains
`original:bytes`, `items:tuple` of original ordered String tuples, and
`truncated:bool`. Pins and descriptors require prior host admission. The closed
carrier is exactly `{items:[StringTuple,...],truncated:boolean}`, selected from
Weft's inherited `weft-application-result/0.2.0` `relatedKeys` definition through
the original result descriptor. The 0.4 path-only result schema cannot attest this
carrier. Duplicate JSON members, native numeric atoms, invalid Unicode, NUL key
atoms, unknown members, wrong tuple arity, descending String key order and
over-bound items refuse. A true marker requires a full bound of items. Equal
tuples and every original byte remain intact; the pure decoder cannot establish
full-bag completeness, edge tie-breaking or truncation truth. Those require
the owning held checks, reviewed lowering and independent native oracle.

The installed commerce workflow MUST run the same ten source-derived query cases
with this profile, including the one-hop collection, two-hop collection/counts,
positive grouping, original property join and exact arithmetic. Success needs
fresh compiler, package, schema and native evidence for the selected profile;
existing profile receipts cannot qualify it. Query-only progress and outer
cleanup/release rules above apply unchanged.

## Same-original current-value Databricks execution

Current-value execution MUST consume the live native Weft
`CurrentQueryPreparation` and preserve its exact original
`ApplicationLineagePreparation` parent throughout the held interval. Imported
designation, plan or result JSON MUST NOT reconstruct either object. Select the
Databricks contributors or explicit original-SUM consumer directly; PostgreSQL
SQL, ordinary recompilation and implicit profile migration MUST NOT substitute.
Compiler `authority:false`, `securityReady:false` and
`nativeAdmission:required` are conditional component declarations, never grants.

An explicitly supplied trusted backend owns `open_current_query(child, context)`.
Its held port supplies the exact binding JSON, admits its correspondence to the
original child and authenticated context, captures only the exact compiler
artifact, verifies captured metadata, complete contributor bags and public-cell
decoding, and renews authority before and after execution. Opening/closing checks
MUST preserve the original binding bytes and parent identity. No result escapes
until all closing checks and context cleanup succeed; refusal, cancellation,
timeout, partial capture or closing drift MUST release no public prefix.

The backend MUST independently establish the complete original publication
vector, UC relation UUID/version/schema correspondence, native caller and current
policy, original UMF source/rule/route/action decisions, complete eligible scan
populations and exact per-use domains, presence and scopes. Required unique
opaque row keys retain a private mapping to original qualified resources. Stage
declarations, digests, callbacks and successful compiler execution establish none
of these facts by themselves. Native metadata and result budgets apply before
capture; private contributor carriers MUST NOT appear in public output.

Managed policy-preserving time travel requires an admitted compatible compute,
table and policy configuration, including any required preview. Unsupported
table-level filters/masks or missing native retention MUST refuse. Policy
exemptions, caller simulations, unprotected staging and local Spark evidence MUST
NOT substitute for managed UC enforcement. Exact integer accumulation MUST
produce an exact value or explicit capability refusal, never wrapped, rounded or
overflow-null output. Backend registration requires genuine native evidence;
an injected conditional test port cannot qualify managed authority.

A read-only UC metadata inspector MAY obtain the selected SDK workspace host,
the current native principal ID and the named table's current UC identity and
ordered column metadata. It MUST explicitly distinguish `uc_table_id` from the
publication's Delta table UUID. Equality, inequality or an old receipt MUST NOT
substitute for a fresh independent native Delta identity/version check. Native
metadata errors, browse-only responses, unexpected relation identities and
ambiguous or incomplete column metadata MUST refuse the observation.

Current metadata visibility establishes neither SELECT permission nor effective
ABAC, per-use UMF authority, historical schema correspondence, snapshot
readability or retention. No inferred policy absence, statistics-derived Delta
version, caller email or username may supply those facts. Native metadata facts
remain observations scoped to the selected workspace and principal, never a
provider registration or a result-release grant.

The portable current-query contributor decoder MUST retain the complete original
UTF-8 carrier and decode only the closed
`weft.stage-contributor-bag/0.1-candidate` shape. Each tuple MUST contain one
required opaque String key for each original scan, in the declared scan order;
keys MUST be nonempty and at most 256 UTF-8 bytes. Duplicate JSON members,
unknown members/versions, null or numeric keys, wrong tuple arity/order and
capacity overflow MUST refuse. Repeated contributor tuples MUST remain repeated.
Empty global contributor bags are valid carrier data, not proof of empty source.

Comparison with independently established expected contributors MUST compare
complete multisets, ignoring only outer bag order. Deletion, duplication or a
scan/key substitution MUST refuse even when public scalar results match. Carrier
decoding and multiset equality MUST NOT establish expected-source completeness,
per-use action authority, aggregate arithmetic or public-cell correspondence;
those remain mandatory held-backend checks. Expected contributors derived only
from the returned carrier cannot serve as an independent oracle.

The host MAY explicitly select current-query result refinement. Omission MUST
retain the existing execution path; missing checker support MUST refuse without
falling back to unrefined execution. The selected native Weft checker MUST run on
the same live CQP with the exact original binding and complete frame supplied by
the held provider. The frame MUST retain independently captured eligible scan
populations/counts, exact per-use cells and the actual captured native results
from that same held invocation and cut. Returned contributors cannot supply the
expected population.

The adapter MUST verify the closed conditional report, including its original
source/designation/binding/artifact anchors and digest of the complete original
frame bytes, before invoking the owner's remaining verification and public
decoder. Reports and frame/capture correspondence remain private host evidence;
they MUST NOT be reused across invocations or treated as native receipts. A
successful match cannot replace native source/action authority, metadata/value
correspondence, complete capture or closing validation. Any checker failure,
anchor mismatch, cancellation or closing failure MUST release no public result.

## Local current-query development composition

An explicitly selected local development profile MAY compose the live CQP,
held Spark provider and complete source capture over authored fixture tables.
It MUST bind the original child and parent, exact request/designation/binding,
actual local process identity and Spark session, and every selected Delta table's
UUID, location, pinned version and complete native schema. Its schema digest
convention MUST be named locally; an opaque compiler schema digest MUST NOT imply
an independently standardized native serialization.

The Spark compiler target MUST be selected explicitly with its own binding and
artifact versions; it MUST NOT replace a failed Databricks target implicitly.
Native function references MUST come from that compiler target. Local admission
MUST observe the complete bounded native user-function inventory and refuse
functions shadowing the selected compiler's unqualified `count`, `raise_error`,
`named_struct`, `array`, `collect_list` or `to_json` in its current resolution
namespace. Harmless registered functions MAY remain. The exact observed inventory
and actual current catalog/database MUST remain unchanged through closing;
capacity overflow or ambiguous native function names MUST refuse, never admit a
prefix. No function registration, SQL rewriting or namespace repair may supply
this correspondence.

Opening, renewal and closing MUST refuse changed native identity, schema, version,
original mapping or context. The unchanged compiler SQL, complete eligible source
capture and native refinement checker MUST run within that same local interval;
private contributors MUST remain private and successful output MUST follow closing.
The profile MUST identify its authored fixture and local-only scope. Local process
identity and table custody MUST NOT stand in for UMF Subject/action/disclosure
admission, protected source/ACK custody, an immutable Ashlar publication, or managed
UC enforcement. Those require their own admitted authority integration.

## Publication-backed local current queries

A local current-query owner MAY derive a bounded physical stage from an actual
admitted Ashlar publication. Stage creation, native query execution, complete
source capture and refinement MUST share the original reader's active publication
interval. Opening, renewal and closing MUST retain the complete native vector,
original model/graph/mapping/request, ordinary source registration and current
protected ACK custody. A staged bag or historical report cannot replace these
checks. Queries MUST NOT advance source ACKs.

Within this original local immutable-publication profile, repeated renewals MAY
use the existing original graph-read renewal port after full snapshot admission
at opening. They MUST bind the same complete publication pin vector, corroborate
the original native-byte inventory, all six installation target identities,
registry/reservation and ordinary source/signature/role/head plus fresh protected
ACK. Full snapshot admission MUST run independently at interval closing, including
error paths. This permission does not cover a changed source, publication,
projection or installation and MUST NOT become cached authorization. Complete
staged-population and native staging checks remain required at each query boundary.

The stage MUST preserve the complete selected source bag, original logical Field
identities and exact values. Its fresh namespace, native table UUID/version/schema,
original live CQP, binding and Spark session MUST remain bound through closing.
Final workflow output MUST follow both query and outer publication closing.
Explicitly authored local query-policy documents MUST identify their conditional
scope; accepted source mappings and ordinary PostgreSQL privileges MUST NOT imply
UMF Subject/action/disclosure permission or managed Unity Catalog enforcement.

### Current-core configured local runtime

The separately selected `ashlar-configured-local-runtime/0.2` profile MAY run
current UMF core `0.10.0` publications on local Python 3.12, Spark 4.2.0,
Delta 4.4.1 and JDK 21. Its operator-supplied native JAR inventory MUST name
and hash exactly the selected Spark 4.2/Scala 2.13 Delta JARs. Configuration
MUST freeze that inventory before effects, and opening and closing MUST attest
the same complete files, bytes, package versions, JDK identity and active Spark
session/JAR selection. A mutable caller dictionary, mere file names, a cached
attestation or a changed inventory MUST NOT qualify the interval.

Before `getOrCreate`, this profile MUST set
`spark.sql.legacy.parameterSubstitution.constantsOnly=true`. Opening and closing
MUST attest that exact active-session value, and the setting MUST be included in
the persisted runtime identity. A missing, changed or unattested value MUST
refuse the interval; it MUST NOT be silently repaired on a foreign session.

For this profile, Backend03 grouped graph and Backend02 SDK page/continuation
MUST each retain their original Weft preparation, exact target binding and
compiler artifact through the complete publication-backed read interval. A
component preparation or an SDK page assembled without native execution does not
qualify the path. A compiler target that does not declare the selected native
runtime MUST refuse until a compatible public target is evidenced; Ashlar MUST
NOT rewrite compiler SQL, infer compatibility from an older runtime, or silently
fall back to another target. Both paths MUST verify exact values, grouping or
page boundaries and continuation against independent source-derived oracles,
renew the complete publication vector and native runtime identity at closing,
and release no partial result after a closing failure.

### Proposed managed current-core read authority

The separate proposed `ashlar-managed-current-core/0.1` resolver MUST admit an
actual selected Unity Catalog MANAGED installation, authenticated ordinary
principal, native Delta UUID/protocol/schema/retention, original source and
protected ACK state, and the complete publication vector in one held interval.
Opening, each native singleton or Weft query/page, and closing MUST recheck the
same selected runtime/build, catalog/schema/table identities and principal.
Existing bounded SQL transport, pin and metadata mechanics MAY be reused only
after this current-core authority is established. The local JDK/JAR hashes and
Spark parameter-substitution setting above, and historical dedicated job
layout, MUST NOT be inferred as managed runtime facts. Unknown runtime/build,
principal, UC protocol or source authority MUST refuse before public output.

## Excluded administrators and isolated test scope

An excluded administrator MUST remain distinct from admitted ordinary data
writers and query callers. Its classification acknowledges an existing privileged
capability outside the ordinary-protection claim; it MUST NOT grant privileges,
exempt a principal from native row/column policy, or promote administrator
membership into ordinary source/action authority. Schema isolation MUST NOT claim
to remove the parent catalog owner's ability to administer child privileges.

A separately admitted operator policy MUST bind any human administrator
classification to the original proposal, selected workspace/metastore/catalog,
exact owner/group identity and isolated schema. A proposal, digest or JSON
approval flag MUST NOT issue runtime authority. Missing classification or native
identity, incomplete grant/membership closure, unknown privilege/delegation
origin, original source/binding drift and opening/closing inventory drift MUST
refuse. Account and workspace SCIM identities MUST retain their actual realm;
unknown or denied membership observations MUST NOT imply nonmembership.

The original privileged negative control requires actual native administrator
bypass and independent ordinary-caller inability to reach that authority.
Classification alone cannot satisfy it. A caller within the excluded
administrator closure cannot serve as its ordinary baseline. The
[isolated test proposal](../../../../examples/security/README.md) defines a
reviewable scope without activating this classification or changing the existing
writer policy.

## Execution evidence

Current results and remaining work belong in the [execution plan](../../04-build/end-to-end-plan.md).
The [original contract text](../../04-build/evidence/contract-history-20261009/CONTRACT-004-publication-resolver.original.txt)
preserves prior execution diaries verbatim; those results do not amend this contract.

The explicit original-commerce publication source profile MUST retain its
original journal path, installation identity and ordinary reader/ACK roles.
The reader MUST use these exact typed selected values and independently admit
the current original registry, reservations, source registration and complete
publication vector; report fields are correspondence, never authority. It MUST
refuse missing, aliased or changed original journal custody. Historical reports
without this explicit profile retain their existing layout and role selection.

### Original medical read source identity

Within the exact original medical read binding, ordinary PostgreSQL reader and
ACK-record role admission MUST verify the current acquired gate, complete original
manifest/pin vector, source/factory/admission owners, journal identity and registry,
and the nominal native observer and independent closing owner. These source-role
identity gates MUST NOT repeat full Spark snapshot captures or grant native result
qualification. Missing complete medical binding or a foreign held vector MUST
refuse. Generic commerce/source-session snapshot admission remains unchanged.

The medical hold MUST independently validate the complete six-role native schemas
and original bags before yielding to any read, and again at closing. Its separate
medical closing MUST still attempt the four pinned snapshots, all six target
identities, native-byte custody, ordinary source and ACK-role scope checks on
every exit. A successful read additionally requires the full exact protected ACK
opening/closing observations owned by `MedicalReadObservation.ack`. No source-only
observation, cached admission or report may replace these native opening/closing
guards or release a buffered result after a closing failure.

### Proposed Spark receipt read interval amendment (TD-009)

Draft proposal only: `ashlar-managed-current-spark-receipt-read/0.1` MUST own a
fresh read-only Spark session in a subsequent verifier task on the selected
dedicated Jobs cluster. It MUST independently observe authenticated principal,
namespace, job/run/task and cluster identity, runtime/Spark/Delta build and complete
six-role inventory at opening and closing, and resolve the separate complete
four-role immutable vector under current source, authorization and retention
custody. Publisher session state and saved reports MUST NOT establish admission.

Every acquired interval MUST attempt substantive independent closing after body
failure, deadline or cancellation, preserving the primary failure. Changed runtime,
principal, table identity/schema/protocol/head, unavailable retained files or lost
authority MUST release no successful observation. Read-only statement allowlisting,
finite cumulative deadlines and complete row/cell/byte bounds MUST reject writes,
unbounded or truncated captures. This offline owner MUST NOT provision compute,
mutate data/source cursors, ACK or confer a remote pin lease. A separately reviewed
live external-holder read/pin renewal boundary remains mandatory through protected
ACK; no task output alone qualifies that boundary or production authority.

As a draft addition to this proposal, the verifier observation MUST remain a
receipt carrier only: it MUST NOT expose ACK submission, a source lease, or a
renewable pin handle. The external source holder composes the exact receipt with
the source-owned `ManagedPublicationReadPort`, `NativePinPort`, and
`TrussSourceCustodyPort` defined in the proposed CONTRACT-001 amendment. Each
port MUST be freshly opened from independently retained original custody, bound
to the same request and complete publication vector, and renewed immediately
before the protected ACK transaction. The source holder alone commits and
reconciles the exact receipt ACK while those ports remain live, then attempts
each independent closing check. A renewal, reconciliation, or closing failure
releases no slot success or next-slot authorization; after a possibly committed
ACK it preserves the original submission and requires reconciliation rather
than retry. The Spark reader's offline `close()` is not a substitute for these
source-holder-owned closing checks.
