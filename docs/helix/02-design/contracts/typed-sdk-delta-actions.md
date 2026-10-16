---
ddx:
  id: ashlar.typed-sdk-delta-actions
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.delta-atomic-primitive
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
---

# Local native Typed SDK Delta actions and bulk

**Version:** `umf.actions.native-delta/0.1` and
`umf.actions.native-delta-bulk/0.1`. **Status:** local implementation candidate.

## Scope

The original Weft program owns SQL, complete model/binding/action/security
sources, input slots, Key checks, preconditions, candidate mutations,
postconditions and bulk candidate verification. Ashlar executes that program
inside one original native Delta transaction. Qualification is restricted to
Spark 4.0.1, the pinned Delta 4.0.0 JVM bytes, PostgreSQL 18.6 admission, one
bounded complete table, scalar string/boolean/signed integer carriers and the
compiler's explicit closed subset. Temporal, managed Delta, Truss protected
authority and unsupported selections remain held or refused.

## Authority and native lifetime

Privileged enrollment independently recompiles the original program and binds
its complete bytes to an existing native table UUID, epoch, local path, native
Spark executor and original source role mappings. An ordinary caller supplies
the program digest for routing and its complete original bytes. The issuer
requires exact byte equality with the privately enrolled program on admission
and final recheck; a matching digest alone grants nothing. The caller cannot
enroll programs or read their private store.
The issuer associates an eligible native login's actual PostgreSQL role OID with
an explicit original principal identity and human/service Subject kind. Native
action role membership does not turn a service Subject into a human.

The SECURITY DEFINER issuer observes the actual session actor, enrolled Subject,
program and native role closure. SHARE locks on original program/Subject and
native PostgreSQL authority catalogs last through Delta commit and final checks.
The host owns a dedicated Spark lease, finite deadline and cancellation group;
the adapter restores job properties and closes the owned PostgreSQL connection.
This local administrative installation does not claim protection against its
cluster administrator or a distributed PostgreSQL write transaction.

## Action and bulk behavior

Generated action invocation checks original input, complete Keys, preimages,
first-false preconditions, candidate effects, postimages and postconditions.
Validation collects evaluable preconditions without mutation. Missing/error
frames retain errors rather than becoming false. Native changes, exact changed
and created nominal Keys, verification markers and SetTransaction share one
actual Delta commit and its returned UUID/epoch/version receipt. Semantic no-op
retains an original marker commit. No invocation retries automatically.

Bulk preview is advisory and commits nothing. Execute reselects its own native
preimage, refuses a complete maxItems-plus-sentinel overflow and checks the
complete candidate, unchanged Key census, selected assignments, unchanged
columns and unselected rows before committing exact counts/Keys with data.

## Failure and custody

A known native optimistic conflict returns Conflict without retry or quarantine.
Failure before submission is distinct from uncertainty after submission.
Response loss after an actual commit returns Indeterminate and quarantines the
original executor/admission; original custody is retained for explicit recovery.
No empty-result, replacement request, fabricated receipt or automatic resubmit
resolves that uncertainty. Explicit retained native custody can inspect the
original bounded commit history and recover its exact receipt/markers after
response loss, including after a later native commit. It grants no new caller
authority and does not clear quarantine or fabricate a typed action replay.

## Evidence and limits

Real local E2Es exercise generated validation/actions/bulk, original commit JSON,
human/service/native-role denial, first-false/evaluation failure, postcondition
discard, complete bulk bounds/reselection, actual concurrent native writes,
native role revocation blocked through commit and response loss after an actual
commit. Compiler-only, callback-only and local native evidence do not qualify
Unity Catalog, Lakebase or Truss protected execution. All temporal qualification
remains HELD.
