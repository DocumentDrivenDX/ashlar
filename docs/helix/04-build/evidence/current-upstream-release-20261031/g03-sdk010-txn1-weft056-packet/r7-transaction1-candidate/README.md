---
ddx:
  id: ashlar.evidence.g03-sdk010-r7-transaction1-candidate-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-txn1-weft056-packet-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-init-r7-sparse-source-20261101
      kind: informed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-009
      kind: governed_by
---

# R7 SDK010 transaction-one packet

Status: **effect-free transaction-one candidate; dispatch is disabled pending an external Sol review receipt.**
This packet is based on Ashlar `2a23b431` and the unchanged `src` tree admitted
by R7. It binds the frozen G03 source, model, binding, source oracle, SDK 0.10.0,
the exact sparse UMF producer checkout, the retained R7 installation, and the
read-only R7 reconciliation receipt. It does not create a synthetic
`init-result.json` and does not treat the original supervisor's
`uncertain-after-dispatch` status as completed.

The reconciliation records that the original SDK010 initialization completed
and created six `.4-xxhash64` Delta tables through version 1, then its worker
failed while checking its post-initialization receipt. It preserves the
original uncertain status and missing worker result. This packet consumes the
separate read-only reconciliation receipt at
`../r7-init-reconciliation/r7-init-reconciliation.json`, digest
`aa35757da908b74698ff81ea6791b0a4d797bf334f2a620fa5f26ebf66b53034`. It
checks registry, carrier, report and invocation digests against that receipt;
checks the runtime SDK010 source-policy revision separately from the carrier
adapter revision; and binds six unique role UUIDs, each at its reconciled v1
head with both Delta log hashes retained. Each inspection reopens all twelve
logs and refuses if any of the six current table heads has advanced beyond v1;
the runtime repeats its fresh-head admission before writing the transaction.

The packet contains a native Delta transaction worker. Before enabling dispatch,
an external Sol review receipt must bind the packet tree digest, source commit
and tree, R7 reconciliation digest, exact limits, and its narrow authorization
for native transaction-one effects. That review scope excludes publication,
manifest, protected ACK and cloud effects. The worker opens the exact R7 SDK010
source admission again before Spark, then calls Ashlar's existing
`run_configured_local_delta4_transaction1` boundary. It does not modify Weft
SQL or generate publication state. The runtime's source-derived plan and
snapshot admission compare the complete object, edge, tombstone and history
bags, including exact values and multiplicity; the worker additionally checks
the returned full six-role terminal vector against the reconciled role UUIDs
and versions plus the transaction's fixed effect counts. Its result schema
requires `typed-graph-1`, the completion receipt, and explicit false values for
publication, manifest and protected ACK. No consumer read, publication vector,
ACK or cloud action is in scope.

The packet limits are one 300-second child, 2 GiB aggregate process-group RSS,
512 MiB attempt storage, 1 MiB combined logs and receipts, local Spark
`local[1]`, a 512 MiB driver and zero automatic retries. Before dispatch the
outcome is `refused-before-dispatch`; a timeout or failure after dispatch is
`uncertain-after-dispatch` and closes that attempt. The manifest has
`dispatchEnabled: false` and has no Sol review receipt path or digest. An
effect-free inspection reports dispatch authorization as false. The attempt
parent `/private/tmp/ashlar-g03-sdk010-delta4-txn1-r7-attempts-20261101` is
absent; this work has not prepared an attempt, created a dispatch record, or
launched the native worker. Dispatch still requires a fresh current-head check,
one-shot permit, process inventory, and the listed resource bounds.

Effect-free inspection:

```sh
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python3.12 \
  docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r7-transaction1-candidate/r7_txn1_gate.py
```

Focused inert checks:

```sh
cd docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r7-transaction1-candidate
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python3.12 -B -m unittest -v test_r7_txn1_candidate
```

The read-only R7 reconciliation is an admission prerequisite, not a normal
initializer receipt. The original R7 attempt remains `uncertain-after-dispatch`;
its native initialization completion is established only by the separate
read-only reconciliation. This packet authorizes one future bounded local
transaction-one attempt. It has not been prepared or executed. Dispatch
requires an independent Sol receipt bound to the exact final packet, followed
by the one-shot permit and fresh process inventory.
