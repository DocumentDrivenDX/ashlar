---
ddx:
  id: ashlar.evidence.g03-sdk010-init-r5-import-fix-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-txn1-weft056-packet-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-txn1-effect-free-gate-r4-20261031
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

# R5 SDK010 fresh-install candidate

Status: **effect-free candidate; no attempt prepared and no native action
authorized**. R5 uses a distinct packet profile and fresh attempt parent
`/private/tmp/ashlar-g03-sdk010-delta4-init-r5-attempts-20261101`. The parent
does not exist. This candidate has not called `prepare`, `slot`, or `execute`.
R4 is consumed and retained unchanged; its uncertain status is not reclassified.

R4 reconciliation evidence is recorded in
`r4-attempt-reconciliation.md`. The worker's explicit `-I` child did not add
its own packet directory to `sys.path`, so line 17 failed importing the sibling
`r4_packet.py`. R5 explicitly adds the worker packet directory alongside the
shared source and timing roots. A focused test invokes the R5 worker under the
same isolated `-I -B` child mode with `--help`; module imports must succeed
before argparse exits, and no runtime or Spark action is reached.

R5 inherits the frozen SDK010 inputs and bounded one-shot initializer, with a
new manifest/profile, installation identity prefix, process-slot/permit
profiles, and a distinct attempt root. The source tree and SDK010 admission
remain pinned to the exact admitted `src` tree and source policy. A successful
init receipt still requires supervisor-confirmed process-group closure. The
transaction-one gate remains dispatch-disabled until a later review binds the
actual successful R5 receipts. This draft grants no execution permit.

Focused import preflight:

```sh
cd docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r5-sdk010-initialize-only
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -B -m unittest -v test_r5_init_packet.InitPacketTests.test_worker_imports_packet_modules_in_isolated_child
```
