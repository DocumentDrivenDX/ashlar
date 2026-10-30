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

Status: **supplemental output receipt implemented and fake-tested; native phase
and manifest transitions remain unimplemented**. No Spark process or native
effect ran. This receipt is not a publication descriptor, realized vector,
consumer result, source acknowledgement, or native `applied` attempt phase.

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

This supplemental journal receipt is not the native `publication_attempt_phase`
store required by the contracts. The present `.4` coordinator persists the
whole graph plan and three graph operation receipts, but it does not persist
native `prepared` and `applying` attempt phases before graph dispatch or a native
`applied` phase after validation. The current `.4` prefix observer also requires
exactly one graph plan and at most the three graph operations. Reusing
`LocalDeltaEffects` for phase or manifest SQL in the same journal would break
that invariant. Extending the observer to ignore extra records would be unsafe.

The next implementation must introduce a separately versioned phase coordinator
and explicit inventory proof that distinguishes graph operations from attempt
and manifest operations. It must retain exact source-qualified phase payloads,
including the configured JSONL checkpoint, before any graph effect; bind the
complete validated output receipt as the original `applied` result; then retain
`committing` before the immutable manifest mutation. The contract's recovery
alternatives remain: partial graph effects without a complete receipt stay
unpublished and are inspected without retry; a complete receipt without a
manifest may publish once after exact authority/predecessor/version revalidation;
a manifest without ACK reopens the same descriptor and does not replay graph
effects.

The existing manifest store and resolver primitives can supply their shared
row validation, immutable merge, and readback semantics only after a `.4`
policy owner admits one exact original manifest operation and independently
revalidates the durable applied receipt. The current configured-local runtime
rejects `.4` publication and consumer routes. Therefore this slice adds no
manifest writer, no manifest readback route, no realized vector, and no ACK.
The `.4` physical profile remains a design proposal under CONTRACT-003, not a
qualified release claim.
