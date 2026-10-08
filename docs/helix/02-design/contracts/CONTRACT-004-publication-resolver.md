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
No TTL or automatic expiry is selected. Ordinary writer/reader/maintenance roles
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
