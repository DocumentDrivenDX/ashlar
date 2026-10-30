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

Status: **local `prepared/applying` attempt journal, metadata reservation and
supplemental output receipt implemented and fake-tested; native phase insertion/
recovery, `applied` and manifest transitions remain unimplemented**. No Spark
process or native effect ran. These local records are not a publication
descriptor, realized vector, consumer result, source acknowledgement, or native
Delta attempt phase.

## Implemented safe boundary

After the held `.4` coordinator has executed and reconciled its exact three
original graph operations, `retain_delta4_output_completion_receipt` performs a
fresh complete effect-prefix observation. That observation verifies the
original source scope, whole effect plan, three operation journal rows and
receipts, named native commits, all four source-derived data-role bags, native
Spark `xxhash64`, the two unchanged metadata-role heads, and the six-role
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
the current hold's completion witness, and that the two native metadata-role
heads stay at their fresh versions. A subsequent hold on the same owner also
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

A separate local metadata-lane reservation now records the exact proposed
`prepared` phase MERGE, original phase payload/digest, JSONL request/checkpoint,
source-scope digest, graph-plan digest, installed attempt-table UUID/path and
fresh predecessor version. It uses its own versioned reservation table and a
distinct `ashlar-local-delta-metadata-operation/0.1` intent profile. Fake tests
check exact replay/refusal and verify that the graph operation table and native
attempt-table history remain unchanged. This is reservation/admission evidence
only; it does not insert the native phase row.

The held-owner gate now prevents bypass through the public graph executor
method: only the orchestration entrypoint can supply its current in-memory
admission, after re-reading the exact durable `applying` row. Native metadata
execution still needs a separate LocalDeltaTransport operation table and
admission/recovery path. The current `.4` observer must continue to reject
unexpected graph-journal rows, so metadata operations cannot be silently
ignored or mixed into `local_plan`/`local_operation`.

The existing `DeltaAttemptStore` is not a drop-in phase owner for this source:
its source-qualified scope currently admits PostgreSQL outbox and Truss-native
feed profiles, while this candidate is bound to the configured immutable JSONL
checkpoint. Reusing it without a source-profile extension and an exact `.4`
native operation authority would misstate source custody. No phase-row write is
therefore implemented in this slice.

The next implementation must introduce native `prepared/applying/applied`
attempt operations and explicit inventory proof that distinguishes graph
operations from attempt and manifest operations. It must bind the complete
validated output receipt as the original `applied` result; then retain
`committing` before the immutable manifest mutation. The contract's recovery
alternatives remain: partial graph effects without a complete receipt stay
unpublished and are inspected without retry; a complete receipt without a
manifest may publish once after exact authority/predecessor/version revalidation;
a manifest without ACK reopens the same descriptor and does not replay graph
effects. The native phase owner must strengthen the current local-only gate so
`execute_effect_plan()` is reachable only after its exact native `applying`
receipt, preserving the same ordering against direct graph-coordinator calls.

The existing manifest store and resolver primitives can supply their shared
row validation, immutable merge, and readback semantics only after a `.4`
policy owner admits one exact original manifest operation and independently
revalidates the durable applied receipt. The current configured-local runtime
rejects `.4` publication and consumer routes. Therefore this slice adds no
manifest writer, no manifest readback route, no realized vector, and no ACK.
The `.4` physical profile remains a design proposal under CONTRACT-003, not a
qualified release claim.
