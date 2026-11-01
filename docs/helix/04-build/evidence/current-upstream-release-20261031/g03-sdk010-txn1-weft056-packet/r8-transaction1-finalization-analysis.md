---
ddx:
  id: ashlar.evidence.g03-txn1-finalization-analysis-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-txn1-weft056-packet-20261031
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# R8 transaction-one finalization boundary analysis

Date: 2026-11-01  
Base: Ashlar `afeb78c41679454730a3bf0129db6f438191953a`  
Scope: source and evidence inspection only; no Spark, Delta, Weft, cloud, or ACK action.

## Finding

No publication finalization or consumer route is enabled by this candidate. The
SDK0101 transaction-one runtime validates the frozen effect plan and four data
role snapshots, then retains an `ashlar-configured-local-delta4-output-receipt/0.1`
in the local SQLite journal. It leaves `publication_manifest` at its fresh
version and returns `publication: false`. The ordinary `_run` path explicitly
rejects `.4` non-initialize operations.

The retained completion receipt is not sufficient authority to append a
manifest. Its documented contract expressly excludes native manifest creation,
realized-vector construction, and source ACK. A valid manifest requires an exact
four-role table-version vector, original source progress, schema revisions,
complete validation report, and canonical record clock. CONTRACT-001 further
requires durable preparation of the exact mutation, current source/native
admission and recovery of the same intent. Adding a row based only on the
transaction-one effect receipt would skip those gates.

The existing `configured_local_read` owner cannot serve as the consumer route:
it resolves the `.3` descriptor profile and `.3` supported revision, and expects
the original configured JSONL progress and attempt journal. It cannot safely
reinterpret transaction-one's `.4` receipt as that descriptor. A direct Weft
0.4.56 route would also lack authoritative manifest resolution, a held complete
four-role vector, and the exact `.4` compiler request/response artifacts. The
current Weft candidate packet itself lists those artifacts as missing.

## Safe boundary and remaining work

Keep the `.4` non-init rejection and transaction-one result's `publication: false`
until a separate finalizer owns: (1) byte-preserving source-progress custody for
the admitted request; (2) exact immutable manifest intent prepared under the
existing local journal and writer authority; (3) independent full-vector,
schema, protocol, retained-snapshot and validation admission; (4) same-intent
native commit/readback and interruption recovery; and (5) resolver admission
bound to trusted table UUIDs and the complete vector. Such a finalizer may report
local publication with `protected_ack: false`; it must not invent or imply a
Truss ACK.

Only after that boundary exists should a bounded consumer owner resolve the
original `.4` publication and hold the complete vector while executing one
original Weft 0.4.56 request against independently retained expected rows.
The request, compiler artifact, result mapping, policy, and closing custody must
all name the same publication and carrier profile. This analysis does not
qualify a local `.4` publication, singleton, Weft query, source acceptance/feed,
protected ACK, managed runtime, or production authority.

## Inspected evidence

- `src/ashlar_host/configured_local_runtime.py`: `_run` allows only `.3` for
  non-initialize paths; `run_configured_local_delta4_transaction1` validates
  effects and returns `publication: false`.
- `src/ashlar_host/configured_local_delta4_publication.py`:
  `retain_delta4_output_completion_receipt` retains local effect custody only;
  `build_delta4_realized_vector` requires an independently obtained original
  manifest row and explicitly does not mint one.
- `src/ashlar/manifest.py`: exact manifest row validation and native mutation
  require full vector, progress, revisions, complete report and recorded clock;
  `DeltaManifestStore` also requires mandatory independent policy admission and
  an executor preparation port.
- `src/ashlar_host/configured_local_read.py`: existing read owner binds the
  `.3` manifest and original progress path.
- `r4-transaction1-gate/inputs/query-scope.json`: Weft 0.4.56 `.4` query
  request/response evidence remains incomplete.

