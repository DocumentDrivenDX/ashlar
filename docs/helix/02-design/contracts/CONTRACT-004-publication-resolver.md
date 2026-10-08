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
