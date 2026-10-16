---
ddx:
  id: ashlar.delta-atomic-primitive
  type: contract
  activity: design
  status: draft
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
  authoring:
    home: repo
---

# Local Delta atomic commit prerequisite

**Type:** engine integration. **Version:** `umf.actions.native-delta-atomic/0.1`.
**Status:** local implementation candidate, not SDK action qualification.

## Purpose and scope

UMF CONTRACT-073 requires action verification markers and data effects in one
native atomic unit. Independent SELECT/UPDATE/SELECT calls cannot establish
that property. `ashlar_host.delta_atomic.DeltaAtomic` supplies the native unit
for trusted engine implementers on Spark4.0.1 and the exact Delta4.0.0 jar bytes.
It does not compile actions, authorize a caller, prove supplied marker semantics,
or turn arbitrary DataFrames into a governed action. Those remain separate
engine/Weft/SDK gates. Temporal qualification remains HELD.

## Normative surface

Construction requires the host's existing dedicated Spark session, an original
local Delta table path/UUID, a previously installed native `umf.actions.epoch`
property, an original complete request SHA-256 and a whole-table bound1..10000.
The exact loaded DeltaLog class jar is checked. Protocol reader1/writer2 and
native `delta.isolationLevel=Serializable` are mandatory. No session, table,
epoch or replacement identity is created by this primitive.

One original OptimisticTransaction reads the native request's SetTransaction
identity, registers the complete table read predicate/files, and exposes a
version-pinned preimage. Trusted engine code must execute the original Weft
program, source/authority checks, complete Key checks and pre/postconditions
against this preimage and its complete candidate. It must retain its admission
and native deadline context through commit. Missing permission refuses before
constructing or committing a transaction.

`commit(candidate, markers_json=...)` is single-use. Candidate physical schema
and complete row bound must match. Marker JSON is bounded, object-shaped,
duplicate-free and finite; the primitive preserves it but does not interpret
its policy meaning. One native commit contains removed/added data files, the
original marker metadata and the original request's SetTransaction marker.
The receipt is the actual table UUID, native epoch and returned commit version,
verified against that exact original commit JSON, never latest history. An
identical original request encounters its durable native transaction marker
and requires original-commit reconciliation rather than another mutation.

## Failure and recovery

Concurrent changes must conflict with the registered whole-table read under
Serializable isolation. No automatic retry is permitted. A failed native commit
leaves unreferenced staged files; this primitive performs no retention or file
cleanup. Transport/readback failure after submission is uncertain and must be
quarantined/reconciled by the owning engine. Missing retained commit JSON never
means that the action did not commit. Epoch/UUID/protocol drift refuses.

## Validation

Owned native tests demonstrate same-commit data/marker/SetTransaction custody,
new-transaction duplicate refusal, an actual independent Delta UPDATE causing
native conflict, unchanged committed rows/version after conflict, and duplicate
marker refusal before write. These tests qualify this local native prerequisite
only. Full action compilation, first-false/validate semantics, native admission,
rollback verification, ACK-loss reconciliation and SDK typed outcome tests are
required before advertising Delta action support. Managed Delta/Unity Catalog
and temporal semantics are not qualified by local evidence.
