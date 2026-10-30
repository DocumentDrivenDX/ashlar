---
ddx:
  id: ashlar.evidence.g03-delta4-coordinator-boundary-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.release-closure-20261029
      kind: informed_by
    - id: ashlar.evidence.g03-weft056-delta4-consumer-candidate-20261029
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-009
      kind: governed_by
---

# Gate 3 transaction-1 coordinator boundary

Status: **native `prepared`/`applying` metadata lane and graph coordinator fake-tested; native `.4` execution remains unrun**. CONTRACT-001 phase ordering is represented by two separately journaled metadata operations before graph SQL. Fake transport verifies the v1→v2→v3 attempt-table progression, exact readback, submitted quarantine/recovery, and graph-prefix execution through the coordinator. No Spark/native `.4` effect, publication, consumer query, or acknowledgement occurred.

## Evidence inspected

The `.4` candidate packet freezes source transaction `typed-graph-1`, its
source-derived expected bags, the six-role installation profile, and three
parameterized insert intents. The current code has a journal-pinned fresh-empty
admission, a source/runtime/writer-lock owner, and a post-effect validator for
complete four-role row bags and native `xxhash64`.

`LocalDeltaEffects` retains an exact whole ordered plan and ordinal operation
custody, supports original recovery, and returns native receipts. The `.4`
coordinator now calls those APIs under a separate temporary policy scope that
admits only the exact whole plan and its three original operation IDs. The
`.3` `PrivatePolicy.active` lifecycle remains unchanged. Each admission checks
the held source/runtime/lock and exact next native predecessor; the in-process
post-commit callback verifies the named commit and complete source-derived
snapshot before the transport persists its receipt. Restarted submitted rows
remain quarantined; recovery uses the original effect recovery port for a
verified committed prefix or exact prepared tail.

The coordinator advances expected closing heads only after all three exact
operations, receipts and source-derived snapshots reconcile. This was verified
with fake transport. CONTRACT-003 still requires native partial role commits
to remain unpublished and retain the prior descriptor, and its recovery table
requires inspection of actual commits and retained input before repair. TD-009
requires exact original operation/effect receipts and complete native vectors
before a publication can be accepted.

## Implemented coordinator boundary

The separately versioned `.4` coordinator durably binds the exact
`source.spec(1)` request and source custody to the
effect-plan digest, installation-profile digest, ordered SQL steps, and the
original six-role fresh heads. Retention is idempotent for byte-identical
intent and refuses any changed or incomplete prior record. The whole effect plan
is retained through `LocalDeltaEffects.prepare`; an absent or changed
original plan cannot be reconstructed from native rows.

The coordinator owns the existing local writer lock and current
`ConfiguredLocalAdmission` through opening, dispatch, reconciliation, and
closing. It grants only the three frozen transaction-1 insert statements.
Before each step it binds the target table/path/UUID and compares current
native heads with the retained original state plus the exact committed prefix.
It uses the original `LocalDeltaEffects` operation IDs and its recovery
port. A timeout or exception does not permit a replacement submission or
continue from observed rows alone.

Before graph operations, the held owner reserves both native
`publication_attempt_phase` rows, `prepared` and `applying`, in a distinct
bounded metadata lane. Each is a separate exact SQL operation with a stable ID
and a predecessor at the then-current native version (v1 and v2 respectively).
The lane journals `submitted` before SQL and records `committed` only after the
named commit, version and complete phase-row inventory validate. A submitted
operation without its named commit stays quarantined and is never retried; a
later exact named commit can be read back and its original receipt completed.
Opening, renewal and close accept attempt-table v3 only when both original
reservations, receipts and complete native readback revalidate. The manifest
role and all unrelated role heads remain pinned. The graph observer continues
to enforce its exact three-operation inventory and compares prefixes against
this validated baseline. Fake tests cover v3 reopen and refusal of unrelated
metadata commits.

Recovery reopens the exact retained source binding and whole plan, inspects all
three ordinal states, and reconciles each named native commit, operation
receipt, version, and snapshot against its original intent. A submitted or
otherwise ambiguous outcome remains quarantined until the original operation
can be resolved. A committed prefix must match the source-derived complete bag
for each committed role; uncommitted roles must still match their retained
predecessor. Missing source custody, lost journal rows, duplicate/ambiguous
named commits, changed identity, or any state outside those exact alternatives
must refuse without retry or cleanup.

Only after all three original operations are committed and their native
receipts, role versions, four complete data-role bags, the exact prepared and
applying attempt rows at v3, and the unchanged manifest head agree may the coordinator issue a non-publication
completion witness. That witness must advance the held owner’s expected closing
heads to the observed six-role vector. It does not mint a manifest, publication
descriptor, query vector, or source acknowledgement. Failure or uncertain
closure withholds the witness and preserves all original custody for
reconciliation.

## Implementation and verification status

The coordinator code contains durable source-plan custody, prepared whole-plan
retention, per-operation execution admission, committed-prefix recovery, full
source-derived bag and native receipt checks, and owner closing-head advancement.
The public orchestration now passes both native metadata phases before graph
dispatch. The focused fake suite verifies:

- exact source/request/profile/plan bytes retained once; changed replay refuses;
- no SQL dispatch before source and whole-plan custody are committed;
- only the three exact role statements and original operation IDs are admitted;
- source/plan-bound prepared and applying phases are retained with exact named
  commits/readbacks, advancing the attempt head v1→v2→v3;
- an applying submission with no named commit remains quarantined, and a later
  exact named commit recovers without retry;
- graph SQL dispatch occurs only after native applying readback;
- each possible committed prefix recovers from the same plan and matches the
  corresponding source-derived role state;
- prepared, submitted, missing, duplicate, conflicting, and lost-journal cases
  never create replacement intent or silently retry an uncertain operation;
- changed source, profile, UUID, schema, protocol, history, or lock fails
  opening, renewal, or closing and releases no completion witness;
- existing `.3` policy admission behavior remains unchanged.

These tests exercise only fake transport and stubbed journal/native results.
They do not prove Spark SQL, Delta commit behavior, native snapshots, manifest
commit/readback, Weft execution, a realized publication vector, or consumer
parity. Native `.4` effects remain pending an exact later execution packet and
independent review; this finding grants no native permit.

The read-only prefix observer checks the four contiguous states
against retained plan/operation rows, named native commits, exact receipts and
complete six-role bags. It also checks the catalog table location against the
original path. Recovery-only rehydration now reconstructs the original
fresh-head token from exact retained source scope and historical empty version-1
snapshots, including original CREATE/registration history, allowing a new local
owner to observe a committed prefix. The prepared tail is checked against exact
intent bytes, digest, baseline version, state and null receipt before the
observer accepts it. These are inert tests only.
The whole three-step effect plan is retained through the original
`LocalDeltaEffects.prepare` API, then run or recovered under the coordinator's
temporary exact per-operation grant. The coordinator verifies complete role
bags and receipts before advancing the six-role closing vector. These paths
have fake-transport tests only; no native effect has run. Native `applied`,
manifest, query-vector, consumer and source-acknowledgement phases remain open.
