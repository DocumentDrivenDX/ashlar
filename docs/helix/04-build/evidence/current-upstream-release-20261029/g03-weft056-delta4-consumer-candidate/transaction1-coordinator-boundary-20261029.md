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

Status: **design finding only**. No `.4` effect, native execution, publication,
query, or acknowledgement occurred. This note records the smallest safe
coordinator boundary found while preparing the next Gate 3 implementation
slice. It does not approve a native run or change a contract.

## Evidence inspected

The `.4` candidate packet freezes source transaction `typed-graph-1`, its
source-derived expected bags, the six-role installation profile, and three
parameterized insert intents. The current code has a journal-pinned fresh-empty
admission, a source/runtime/writer-lock owner, and a post-effect validator for
complete four-role row bags and native `xxhash64`. The owner intentionally
admits no effect intent.

`LocalDeltaEffects` already retains an exact whole ordered plan and ordinal
operation custody, supports observation and original recovery, and returns
native receipts. Its existing execution path is guarded by the `.3`
`PrivatePolicy.active` publisher lifecycle. Directly enabling that path inside
the `.4` owner would allow effects before a coordinator has made the durable
source binding and recovery state authoritative.

The current fresh token proves only six empty role heads. The owner’s closing
check compares against those same heads, so a successful `.4` insert would
currently be reported as head drift. The existing `.4` validator checks only
the complete post-effect state; it cannot admit or reconcile a partially
committed prefix. CONTRACT-003 requires partial role commits to remain
unpublished and retain the prior descriptor, and its recovery table requires
inspection of actual commits and retained input before repair. TD-009 requires
exact original operation/effect receipts and complete native vectors before a
publication can be accepted.

## Smallest coherent coordinator boundary

Before any first native dispatch, one separately versioned `.4` coordinator
must durably bind the exact `source.spec(1)` request and source custody to the
effect-plan digest, installation-profile digest, ordered SQL steps, and the
original six-role fresh heads. Retention must be idempotent for byte-identical
intent and refuse any changed or incomplete prior record. The whole effect plan
must then be retained through `LocalDeltaEffects.prepare`; an absent or changed
original plan cannot be reconstructed from native rows.

The coordinator must own the existing local writer lock and current
`ConfiguredLocalAdmission` through opening, dispatch, reconciliation, and
closing. It may grant only the three frozen transaction-1 insert statements.
Before each step it must bind the target table/path/UUID and compare current
native heads with the retained original state plus the exact committed prefix.
It must use the original `LocalDeltaEffects` operation IDs and its recovery
port. A timeout or exception is not permission to submit a replacement or
continue from observed rows alone.

Recovery must reopen the exact retained source binding and whole plan, inspect
all three ordinal states, and reconcile each named native commit, operation
receipt, version, and snapshot against its original intent. A submitted or
otherwise ambiguous outcome remains quarantined until the original operation
can be resolved. A committed prefix must match the source-derived complete bag
for each committed role; uncommitted roles must still match their retained
predecessor. Missing source custody, lost journal rows, duplicate/ambiguous
named commits, changed identity, or any state outside those exact alternatives
must refuse without retry or cleanup.

Only after all three original operations are committed and their native
receipts, role versions, four complete data-role bags, and two unchanged
metadata-role heads agree may the coordinator issue a non-publication
completion witness. That witness must advance the held owner’s expected closing
heads to the observed six-role vector. It does not mint a manifest, publication
descriptor, query vector, or source acknowledgement. Failure or uncertain
closure withholds the witness and preserves all original custody for
reconciliation.

## Implementation and verification gate

This coordinator is too cross-cutting for the current authority-only slice:
it must combine durable source-plan custody, journaled effect execution,
partial-prefix recovery, full source-oracle comparison, and owner head
advancement in one versioned lifecycle. The following focused inert tests are
the minimum before the coordinator grants any effect intent:

- exact source/request/profile/plan bytes retained once; changed replay refuses;
- no SQL dispatch before source and whole-plan custody are committed;
- only the three exact role statements and original operation IDs are admitted;
- clean completion requires all three exact receipts, full four-role bags,
  fresh metadata roles, and a six-role advanced closing vector;
- each possible committed prefix recovers from the same plan and matches the
  corresponding source-derived role state;
- prepared, submitted, missing, duplicate, conflicting, and lost-journal cases
  never create replacement intent or silently retry an uncertain operation;
- changed source, profile, UUID, schema, protocol, history, or lock fails
  opening, renewal, or closing and releases no completion witness;
- existing `.3` publication, recovery, and replay behavior remains unchanged.

Until those guards and recovery tests exist, the reviewed authority must remain
read-only with respect to policy admission. The one-shot native permit, if any,
must bind the exact later coordinator build and effect packet after independent
review. No native execution is authorized by this finding.

The subsequent read-only prefix observer now checks the four contiguous states
against retained plan/operation rows, named native commits, exact receipts and
complete six-role bags. It also checks the catalog table location against the
original path. Recovery-only rehydration now reconstructs the original
fresh-head token from exact retained source scope and historical empty version-1
snapshots, including original CREATE/registration history, allowing a new local
owner to observe a committed prefix. These are inert tests only. Native effect
dispatch, ambiguous-operation recovery and completion witness remain open.
The whole three-step effect plan can now be retained through the original
`LocalDeltaEffects.prepare` API under a temporary prepare-only policy grant;
every ordinal operation intent remains denied. No native effect has run.
