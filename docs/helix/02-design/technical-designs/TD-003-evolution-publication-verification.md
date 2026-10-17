---
ddx:
  id: TD-003
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ADR-001
    kind: informed_by
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: CONTRACT-006
    kind: informed_by
  - id: TD-001
    kind: informed_by
  - id: US-004
    kind: informed_by
---

# Installed evolution publication verification

## Scope and technical approach

Provide an additive installed verification-only invocation selecting one completed
absolute ordinal, zero through seven, of the exact retained eight-publication
commerce evolution run. A selected older publication remains admissible only
under current source, protected ACK, installation and retention authority. The
selection changes the read target, not the original schedule or completed prefix.
The result proves four complete native bags for that descriptor and their
correspondence to the original source-prefix union. It does not publish a missing
ordinal or establish completion of the entire run.

Apply CONTRACT-001 original attempt/run custody and CONTRACT-004 held-vector
reads, with TD-001's PUB-F1–F4/L1 assurance model. Add the verification invocation
and result boundary to CONTRACT-001 before implementation; continuation's
effectful interface does not implicitly authorize verification. No new producer
semantics, schema layout, source transaction or permissive policy is introduced.

The verifier has no preparation, submission, phase transition, effect, commit,
manifest publication, ledger update or source-progress port. Transient exclusive
leases, native pins, local database read bookkeeping and bounded evidence output
are permitted. These are distinct from persisted business mutations. The native
installation and original run ledger are existing-only; verification never
initializes, migrates, repairs or reconstructs either.

## Installed invocation and owning components

The proposed `VerifyEvolutionInvocation` is a distinct exact typed carrier,
separate from fresh, resume and continuation. It binds canonical absolute original
ledger and receipt paths, expected original run SHA256, exact producer admission,
and one integer ordinal. Booleans, floats, unknown members, duplicate JSON
members, path aliases and ambient environment selections refuse. The
receipt is distinct from every input and original installation file. Configuration
and provider must agree on ordinal, run digest and installation before effects.
Configuration/provider files retain the existing bounded custody and trusted-code
qualification; a provider digest proves correspondence, not source authority.

The independent `native_schema_profile` selection is a closed developer-owned
comparison profile, separate from `VerificationBounds`. Omission preserves the
legacy SQL-default-nullable profile and its result shape. Canonical managed mode
requires exact `load_managed_carriers()` ordered columns and fields, including
declared nullability and empty field metadata. Apply one profile to all four
immutable bags at opening and closing; never auto-detect or accept a per-role
choice between schemas. Canonical receipts identify the selected profile and
packaged generated-carrier digest. Provider agreement is checked before work;
the selection neither repairs storage nor grants authority.

`evolution_verification.py` owns verification composition and provisional typed
results; `evolution_verification_cli.py` owns bounded parsing, trusted provider
loading, installed dispatch and final receipt release. `evolution_operator.py`
owns an additive exact verification binding for `ExistingEvolutionOperator`,
sharing its actual `ExistingEvolutionLease`, native session, original six-target
registry, two `OutboxAuthorityRegistration` values and selected operation capacity.
It must not route through an effectful fresh/resume/continue configuration.
`src/ashlar/cli.py` adds a separately named installed command after the normative
invocation surface is reconciled. These names describe intended ownership, not
an already implemented interface.

`evolution_run.py` remains the sole original ledger owner. Use
`EvolutionRunLedger.inspect` with its existing `mode=ro` connection and a private
lease-bound custody adapter; do not call `open`, mark a slot or create a run.
`NativeEvolutionRunPolicy` in `evolution_composition.py` composes actual lease
admission with native observations. `OriginalRunCorrespondence` supplies original
requests, predecessor ancestry, schema state and independent expected bags.
`NativeDriver` owns native lineage admission and a narrow public selected-completed
descriptor observation, internally reusing its existing ledger validator. The
composition must not import `_validate_evolution_run_ledger` across modules or
invoke `observe_completed_evolution_attempt`, which enters publisher recovery.

The existing public `NativeDriver.inspect_snapshot` corroborates readability and
returns a `Snapshot` identity, not rows. Therefore the driver also owns a new
bounded public selected-publication bag verification port. It receives the exact
admitted original run/ordinal/descriptor and real context, requires active writer
and pins, validates the four exact role/version bindings, and reads complete rows
inside the driver owner. The coordinator never accesses `transport._snapshot`.
The port independently compares all four bags to the original semantic oracle
and returns bounded typed evidence; it retains current source/ACK/native gates
and closing correspondence. Its row schema, row count, cell size and total
encoded-byte capacity checks precede reading or buffering the complete rows;
overflow refuses without truncation or digest-only promotion. The existing
`LocalDeltaTransport._snapshot` row-limited `collect` is not sufficient byte
admission, and the new port must not merely assert byte size after that collection.
These are four materialization bags, not the four
Weft SQL query cases used by the separate original-commerce query workflow.

ADR-001's portable/host separation remains: portable original semantic owners
have no SDK, filesystem or operator imports; installed composition imports public
host ports only. No tools/example module imports, arbitrary validation callbacks,
caller-supplied proof booleans, policy substitution or source-session replacement
are permitted. Declare the new owners and public edges in the existing module
map and check them with `python3 tools/check_module_boundaries.py`.

## Opening, selected read and closing

1. Capture bounded invocation/provider bytes and original run reservation under
   current canonical file custody. Acquire the actual installation exclusion
   lease. Before constructing `NativeDriver`, independently inspect the existing
   native journal schema and canonical registry/reservations, including original
   operation, plan, publication-artifact and source-plan tables. Missing or changed
   schema refuses: the current driver constructor can create absent artifact or
   source-plan tables, so constructor behavior cannot be used as a preflight.
   Never initialize a missing journal or infer absence from missing custody.
2. Compose current ordinary source and ACK owners with the actual held lease,
   `ExistingOutboxAuthority`, native transport and real driver context. Acquire
   the driver writer reservation for exclusion and current authority; this grants
   no permission to invoke writes. Renew the complete installation identity,
   target UUID/location, reservation and producer/source correspondence.
3. Inspect all eight original slots and admit the complete retained run through
   `NativeEvolutionRunPolicy.admit_run/admit_ledger` and
   `driver.admit_evolution_run_ledger`. Retain exact original bytes and digest,
   contiguous completed prefix, any retained/started slot and unprepared suffix.
   Require the selected slot to be completed. Inspect every completed slot's
   original request, applied artifact, phase prefix, original operation handles,
   source plan, native descriptor/vector and protected ACK. A later uncertain
   slot remains uncertain and cannot be reconciled by this invocation; ambiguous
   lineage or failed current admission withholds success.
4. Observe the selected original descriptor through the driver-owned public read
   seam. Hold its exact declared data vector under the real pin owner; retain the
   independent complete six-target installation/coordination identity. Descriptor
   data-vector membership and installation registry membership are separate exact
   sets, never an arbitrary subset or synthetic six-table manifest. Read all
   `object_current`, `edge_current`, `tombstone` and `whole_source_history` bags at
   the selected descriptor versions. Preserve complete columns, presence/null,
   lexical/numeric carriers, tombstones, history and multiplicity. Compare to
   `OriginalRunCorrespondence.at(ordinal)`'s complete original prefix-union oracle,
   not a receipt flag, row count, current-head snapshot or filtered projection.
5. Attempt independent closing native/vector/raw-bag, full ledger/original-run,
   ordinary source/ACK, producer/runtime and configuration custody checks while
   their requisite writer, lease and pins remain active. On opening/body failure
   or cancellation, attempt every acquired closing obligation independently,
   then release pins, database sessions, driver/transport, native runtime and
   lease. Preserve the original `BaseException` using `finish`; a failing first
   closing callback must not suppress the remaining checks. Repeat exact opening
   correspondence where required; do not promote a changed baseline.
6. Release a bounded result only after successful closing and resource cleanup.
   The receipt identifies original run digest, selected ordinal/publication,
   exact descriptor/vector, four bag evidence digests and completed-prefix count
   out of eight. It distinguishes selected verification from full-run completion.
   Receipt acquisition/write/fsync/close failures preserve primary error priority;
   missing evidence never authorizes replacement of an original publication.

`RegisteredOutboxAcks.reconcile` uses the ordinary protected ACK session to SELECT
the retained request, manifest, receipt and progress, with current scope and
publication admission before and after. Its read transaction is committed and
closed, but it does not call the service ACK routine or submit source progress.
The verifier requires a nonmissing exact original receipt; `None` is refusal,
not permission to acknowledge. No `acknowledge`, stored-publisher replay or
publication recovery path is reachable. Readback may observe current progress
beyond an older selected checkpoint only when exact retained original receipt
correspondence and existing protected policy admit it.

## Bounds, observability and qualification

Use explicit selected installed runtime and ordinary factories, without SDK or
credential discovery, environment defaults or implicit provisioning. Existing
native capacity, complete input/output bounds, protocol and current authority
guards remain mandatory. The distinct closed typed configuration owns explicitly
supplied input, row, cell, total encoded-byte, receipt and capture limits, constrained
by declared ceilings and the selected current native profile. It has no ambient
defaults. Apply each bound before the corresponding read or buffering operation,
including complete row capture; valid row count alone cannot admit unbounded cells.

The installed invocation requires operator-selected finite process and stream
supervision, with an explicit deadline and an owning supervisor responsible for
cancellation, termination, reaping and bounded output retention. The invocation
and provider composition must bind that supervision selection before native
effects. This design does not claim a built-in timeout in the current installed
CLI. A selected local verification profile may retain a separate 300-second native
limit, without increasing existing limits. Separate invocations avoid coupling
evidence reads to a publication deadline; this is not a promise that every
selected read completes within that budget. Timeout withholds the receipt; it
does not authorize a deadline increase or a weakened validation boolean.

Use CONTRACT-006 bounded safe lifecycle phases and closed outcome categories.
No DSNs, credential values, exception messages/locals, SQL, source rows or journal
payloads enter diagnostics. Exact rows belong only in explicitly selected bounded
evidence artifacts. Diagnostics neither grant authority nor suppress cancellation.
Timeout, malformed capture, worker leak, unavailable snapshot, current source
revocation, cleanup failure or evidence overflow withholds a positive receipt.

Local development evidence qualifies only its selected producer, ordinary role,
Spark/Delta, filesystem and private installation profile. It establishes no Truss
acceptance, production authentication, managed UC/cloud authority or scale claim.
The verification surface does not replace the whole P04 requirement: eight publications and 32
complete bags, retained older reads, settled exact replay, durable independent
feed ACK/restart, missing-journal refusal and uncertain-operation behavior remain
required in their owning publication/recovery lanes.

## Property and test correspondence

PUB-F1 maps to exact selected immutable vector, full four-bag equality, current
six-target identity and independent opening/closing under pins. Test completed
ordinal zero and a later completed ordinal, then verify an older ordinal after a
newer publication. Refuse version/UUID/location substitution, partial bags,
duplicate/missing rows, changed lexical/presence carriers, expired retention and
mixed vectors. Exercise the actual public driver/ledger composition, not copied
validation logic.

PUB-F2 maps to exact protected ACK readback and absence of progress mutation.
Spy actual registered ACK composition: require SELECT observation and controlled
read commit, forbid the ACK routine and source append. Test missing receipt,
changed request/manifest/receipt bytes, feed/epoch/signature/role/privilege drift,
progress conflict and revocation during closing. Actual policies remain required
even when original metadata is unchanged.

PUB-F3 maps to immutable original eight-slot custody and existing-only schema
preflight. Refuse missing journal/table/reservation, altered original run digest,
slot gaps, bool ordinals, forged completed flags, missing phase/effect handles,
changed artifact/source-plan bytes, unfinished selection and foreign installation.
Assert no driver construction on incomplete schema and no constructor repair,
ledger transitions, native submit/commit, prepare or replacement invocation on
any verification path. A started slot cannot become completed through verification.

PUB-F4 maps to full original two-source schedule, independent checkpoint union
and predecessor ancestry at every admission. Test overlapping source-local IDs,
alternating A→B→A lineage, changed source order, wrong predecessor and changed
schema/producer revisions. Selecting one ordinal never truncates ledger admission.

PUB-L1 is not discharged by this read-only surface: it cannot advance a committed
publication's missing ACK. Verification may establish current readable/ACKed
custody after the owning recovery succeeds; its refusal leaves recovery state
unchanged. Preserve separate end-to-end liveness evidence obligations.

Lifecycle controls must exercise actual context methods: opening admission
failure, body cancellation plus multiple closing failures, pin/lease release,
native stop and database close failure, timeout/reap, bounded capture overflow
and receipt refusal. Record closing attempts under active ownership and exact
primary cancellation identity. Assert business-state byte/value equality before
and after success and failure, separately classifying permissible read-side
bookkeeping; never claim every filesystem byte is immutable from a read-only API.
TP-001/US-004 retain broader publication acceptance ownership; these focused
controls establish this additive boundary, not a new formal proof or completed
native qualification.
