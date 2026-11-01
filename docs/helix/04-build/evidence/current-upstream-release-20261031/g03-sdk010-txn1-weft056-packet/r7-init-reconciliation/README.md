---
ddx:
  id: ashlar.evidence.g03-sdk010-r7-init-readonly-reconciliation-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-init-r7-sparse-source-20261101
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-txn1-weft056-packet-20261031
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-009
      kind: governed_by
---

# R7 read-only initialization reconciliation

The retained R7 attempt completed runtime initialization and created all six SDK010 `.4` Delta tables. The worker then failed in its post-initialize receipt predicate because it compared `carrier.source_policy_revision` (`ashlar-local-current-delta4-sdk010-source-policy/0.1`) to the distinct UMF source policy revision (`umf-sdk-0.10.0/...`). The runtime report and registry show the UMF revision in `installation.scope.typed_graph_policy_revision`; the carrier field names Ashlar's adapter policy. Both values are correct under their separate contracts.

The original supervisor `execution-status.json` remains `uncertain-after-dispatch` with `effectsPossible: true`; it is not edited or upgraded. The worker emitted no `init-result.json`. Its complete stderr SHA-256 is `a32ec32119457c236406d3e4824c7174107221298327a03743e2c181837fa4ed`, stdout is empty (`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`), and the supervisor cleanup receipt proves an empty process group (`795505c4b6bfa7efc12a226f246da684ed8bc40d39494e114aa02e353fee4e67`). The worker's runtime report binds the installation registry and carrier profile hashes. Every role has exactly Delta log versions 0 and 1: a `CREATE TABLE` commit followed by the Ashlar installation metadata commit. Each head's schema, protocol, table UUID/path, carrier proposal dispositions, installation ID, registry digest, and journal-path digest match the exact registry/carrier reservation.

The verifier is deliberately read-only with respect to the retained attempt. Its default operation prints a result; `--receipt` writes only to a caller-selected fresh output path. It checks the prepared receipt, exact R7 packet/manifest binding, process slots, one-shot permit and dispatch linkage, source preflight, original uncertain status, cleanup closure, runtime report, registry/carrier contracts, reservation snapshots, pinned Spark/Delta runtime, and all six v1 Delta heads. It keeps the two policy revisions distinct and refuses any mismatching value.

The resulting [machine-readable reconciliation receipt](r7-init-reconciliation.json) (SHA-256 `aa35757da908b74698ff81ea6791b0a4d797bf334f2a620fa5f26ebf66b53034`) classifies this as `runtime-initialization-completed; worker-receipt-verification-failed-after-effects` and sets `installationUsableForTransaction1: true` based on the independent checks. This is an installation prerequisite receipt only. The previous transaction-one gate still requires a supervisor-closed worker result and needs a separately reviewed input binding to consume this reconciliation; no transaction-one effect has run or been authorized here. No retry, Spark, or Truss action was performed.

Attempt: `/private/tmp/ashlar-g03-sdk010-delta4-init-r7-attempts-20261101/ashlar-g03-sdk010-init-4a86d7d16b364bb29bc0436ad61523b0`.

Run from the repository root:

```sh
python3 docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r7-init-reconciliation/r7_reconcile_readonly.py \
  /private/tmp/ashlar-g03-sdk010-delta4-init-r7-attempts-20261101/ashlar-g03-sdk010-init-4a86d7d16b364bb29bc0436ad61523b0
python3 -m unittest discover -s docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r7-init-reconciliation -p 'test_*.py'
```
