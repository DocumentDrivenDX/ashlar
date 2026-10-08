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
The local suite uses simulated backends. It does not qualify live authentication,
retention, data-file custody, native execution or concurrency. No Spark/Databricks
benchmark is needed for this implementation iteration. UMF remains deferred.

## Active-pin registry substrate

The private PostgreSQL ashlar_pins registry retains immutable pin identity,
authority, manifest/cursor/recovery/release scope, native table name/UUID/version
and original custody digest. Explicit release retains the original row; exact
registration replay preserves it and released identities cannot reactivate.
Pin rows have no automatic release; publication readability now follows the finite
configuration-based window below. Ordinary writer/reader/maintenance roles
have scoped function/read permissions, with no direct table mutation.

Registration and release share a native table lock. assert_unpinned holds a
conflicting SHARE lock until its transaction ends and refuses an active exact
version. Registration must independently prove the target/source/custody and
retained data availability before committing. A maintenance operator must hold
the guard transaction throughout retention and check every affected version/file
in the complete union of all pin scopes. One checked version cannot authorize
removing files required by another version. External Delta maintenance and
privileges are not yet wired through this guard. Registry presence alone is not
proof of future retention or valid resolver policy; the existing mandatory
validate_snapshot contract remains unchanged. Native private role/replay/conflict/
rollback/retention-refusal checks pass; no purge or Delta retention change ran.

PostgresPins/PinVector provide complete scoped vector registration and held
read custody. assert_active validates every original field and holds native
SHARE exclusion through resolution/execution; registration/release cannot
commit against it. The injected host transaction adapter must preserve the
transaction through context yield and roll back exceptions. Current read
authorization is checked before and after consumption, while pin inventory is
revalidated before release. Native four-pin guard versus competing release
passes. This supplies one custody component of validate_snapshot; protocol,
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
and execution failures return no partial singleton response. Native composed
publication/policy evidence remains unqualified; local refusal controls pass.

## Native permission observation boundary

The current client_dev graph fixtures inherit MODIFY for a shared ordinary user
group. Local cooperating-process locks and PostgreSQL pin records do not fence
those remote writers. Do not claim full native publication authority from these
fixtures. The dedicated ashlar_e2e_private_20261008 catalog has an authenticated
owner and observed empty explicit grants; table setup and renewed inherited
permission checks are still required. validate_writer_inventory refuses
unexpected owners/writers/delegators, unknown privileges and incomplete grants.
Inputs must be complete fresh native observations under the real authority lane;
saved grants are evidence, not a lock. Platform administrative trust, retention
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

Owner-selected on 2026-10-08: predictive optimization remains enabled. Its
DISABLE state is not a publication prerequisite. The previous blanket exclusion
proposal and pending authorization request are superseded.

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


## Native finite-window observation (non-normative)

[Original read-only receipts](../spikes/SPIKE-001-table-layout/out/native/csv_retention_window_20261008/statements.jsonl)
cover UUIDs, explicit/absent retention properties and exact selected snapshot
commit timestamps for the four previously verified CSV tables. The initial host
check refused the `Etc/UTC` spelling after all 17 SQL statements succeeded; the
[refusal disposition](../spikes/SPIKE-001-table-layout/out/native/csv_retention_window_20261008/initial-refusal.json)
and [local recovery](../spikes/SPIKE-001-table-layout/out/native/csv_retention_window_20261008/recovery-summary.json)
retain that distinction. Recovery binds exact original SQL/parameters and
complete responses; it issues no new native requests and does not make old
observations fresh. The tool accepts only the fixed zero-offset `UTC`/`Etc/UTC`
spellings for its selected SQL timestamp interpretation.

As of the retained clock **2026-10-08 12:52:54.711244 UTC**, the configured finite
ceiling is **2026-10-15 10:03:15 UTC** for the earliest selected snapshot, using
explicitly qualified documented defaults (7-day data/30-day logs) and a 60-second
development margin. This is a configuration-based ceiling, not a guarantee of
file availability. Current configuration, file/log availability, protocol, UUID
and permissions remain required before and after a real read. No publication,
ACK, pin retirement, predictive optimization or retention-setting mutation ran.

`python3 tools/check_native_retention_window.py --replay-original` recovers the
original receipts offline. Fresh observations require a new `--output` directory.
The native observer distinguishes explicit properties from qualified defaults
and refuses identity changes, ambiguous properties or unsupported intervals.
All 175 small local tests pass; no scale or renewed cloud test was run.


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

Non-normative implementation evidence: all 180 small local tests pass, including
successful manifest replay with fresh opening/closing admission, expired closing
admission retaining the already-written manifest, independent source-admission
refusal before effects, actual retention-gate expiry after a singleton fetch,
and bounded SQL observation/margin refusal. These are component checks, not
native manifest publication or end-to-end Truss evidence. No cloud run or renewed
benchmark was performed for this iteration.
