---
ddx:
  id: ashlar.evidence.g03-delta4-output-completion-receipt-boundary-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.release-closure-20261029
      kind: informed_by
    - id: ashlar.prd
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-001
      kind: governed_by
    - id: TD-009
      kind: governed_by
---

# Gate 3 `.4` output-completion receipt boundary

Status: **native `prepared`/`applying` metadata lane and graph coordinator fake-tested; native `.4` execution remains unrun**. The two phases are separately reserved and journaled before graph SQL, with exact v1→v2→v3 commit/readback and submitted recovery checks. No Spark/native effects ran. No publication descriptor, realized vector, consumer result, source acknowledgement, or native `applied` phase is claimed.

## Implemented safe boundary

The output-receipt helper is implemented for a fully reconciled fake observation. `retain_delta4_output_completion_receipt` performs a
fresh complete effect-prefix observation. That observation verifies the
original source scope, whole effect plan, three operation journal rows and
receipts, named native commits, all four source-derived data-role bags, native
Spark `xxhash64`, the verified prepared/applying rows at attempt-table v3, the
unchanged manifest head, and the six-role
identity/schema/protocol inventory. It then retains one bounded canonical
receipt in a separately named local SQLite journal table. Identical replay is
byte-exact; changed or corrupted receipt bytes refuse.

The receipt binds the exact request JSON and digest, source-scope digest,
publication ID, installation/profile identities, effect-plan bytes and digest,
three exact original operation receipts, four data-role versions, six-role
inventory and the complete coordinator observation. The implementation does
not add rows to `local_plan` or `local_operation`; the existing observer's
exact-three-operation invariant remains unchanged. Focused fake tests check
receipt replay, tamper refusal, refusal of a restarted committed prefix without
the current hold's completion witness, and that the attempt-table v3 head is
accepted only with both exact phase receipts while the manifest head stays fresh.
A subsequent hold on the same owner also
resets the prior witness and requires exact recovery before receipt replay.

## Required next boundary

The new local journal adds source/request/checkpoint/installation/journal/graph
plan-bound `prepared` and `applying` rows in its own versioned SQLite table.
`prepare_and_execute_effect_plan()` retains both rows before dispatch, and
`execute_effect_plan()` refuses without its ephemeral held-owner admission. The
fake suite covers missing direct admission, byte-exact replay and tampered
custody refusal before dispatch. These rows establish local ordering only; they
are not native `publication_attempt_phase` rows and do not establish native
Spark/Delta effects or an `applied` phase.

The current `.4` prefix observer still requires exactly one graph plan and at
most three graph operations. Reusing `LocalDeltaEffects` for native attempt or
manifest SQL in the same journal would break that invariant. Extending the
observer to ignore extra records would be unsafe.

The separate local metadata-lane reservation records the exact proposed
`prepared` and `applying` phase MERGEs, common original phase payload/digest,
JSONL request/checkpoint, source-scope digest, graph-plan digest, installed
attempt-table UUID/path and successive predecessor versions. It uses its own
versioned reservation table and a distinct
`ashlar-local-delta-metadata-operation/0.2` intent profile.

An isolated `LocalDeltaTransport` metadata executor now has its own bounded
SQLite operation table, so graph `local_plan`/`local_operation` inventories
remain exactly scoped to graph effects. It binds the operation to the retained
reservation, lane, installation, source request, target UUID/path, predecessor,
exact SQL/parameters and prepared payload. It records `submitted` before
dispatch, never retries an absent/ambiguous named commit, and records a local
receipt only after the exact named Delta commit, next version, complete native
phase-row readback, snapshot and closing identity checks agree. Reopen/recovery
uses the same bytes and validates an existing commit. The metadata table is
allowed by journal preflight only as a known `.4` extension; arbitrary extra
tables remain rejected. `DeltaAttemptStore` now accepts the configured
`ashlar-immutable-jsonl/0.1` profile, while preserving its exact original
request/checkpoint validation. Its readback session uses the same private
writer context and independently rechecks native table identity; data queries
remain read-only and use the transport query port.

The coordinator opens this lane through exact held-owner `PrivatePolicy`
admissions. Before each dispatch, it checks the original request/source/plan,
reservation bytes, writer lock, exact predecessor and graph-operation
inventory. After dispatch, it admits only the exact named commit with the full
phase row read back. Prepared commits at v2; applying commits at v3. Opening,
renewal and close accept the attempt table at v3 only when both metadata
operations have exact committed receipts whose named commits, phase-row
snapshot, UUID/path, schema and protocol revalidate. The graph observer retains
its exact-three graph-operation inventory and uses the validated v3 baseline
while reconciling graph prefixes. Fake transport under the real `PrivatePolicy`
covers v3 reopen, graph-prefix execution, and an applying submission that stays
quarantined until its original named commit arrives; recovery does not retry
the SQL. These remain inert checks; no native phase or graph effect has run.

The next implementation must add native `applied` and explicit inventory proof that distinguishes graph, attempt and manifest operations. It must bind the complete validated
output receipt as the original `applied` result; then retain `committing`
before the immutable manifest mutation. The contract's recovery
alternatives remain: partial graph effects without a complete receipt stay
unpublished and are inspected without retry; a complete receipt without a
manifest may publish once after exact authority/predecessor/version revalidation;
a manifest without ACK reopens the same descriptor and does not replay graph
effects. Production release remains blocked on native `applied` and the remaining manifest and consumer custody transitions.

The existing manifest store and resolver primitives can supply their shared
row validation, immutable merge, and readback semantics only after a `.4`
policy owner admits one exact original manifest operation and independently
revalidates the durable applied receipt. The current configured-local runtime
rejects `.4` publication and consumer routes. Therefore this slice adds no
manifest writer, no manifest readback route, no realized vector, and no ACK.
The `.4` physical profile remains a design proposal under CONTRACT-003, not a
qualified release claim.
