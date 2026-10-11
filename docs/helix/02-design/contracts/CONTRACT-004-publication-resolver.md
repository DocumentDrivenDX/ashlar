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

## Execution evidence

Current results and remaining work belong in the [execution plan](../../04-build/end-to-end-plan.md).
The [original contract text](../../04-build/evidence/contract-history-20261009/CONTRACT-004-publication-resolver.original.txt)
preserves prior execution diaries verbatim; those results do not amend this contract.
