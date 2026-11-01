---
ddx:
  id: ashlar.evidence.g03-sdk010-init-r6-invocation-parse-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-txn1-weft056-packet-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-init-r5-import-fix-20261101
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

# R6 SDK010 fresh-install candidate

Status: **effect-free candidate; R6 attempt has not been prepared and no native action is authorized**. R5 was consumed and failed before its initializer function. Its retained receipts and conservative classification are reconciled in `r5-attempt-reconciliation.md`; no R5 receipt is changed or reused.

The exact R5 cause is producer-key schema mismatch. The packet manifest records camelCase `timeoutSeconds`, `maximumOutputBytes`, and `maximumReceiptBytes`, but `parse_configured_local` requires the exact snake_case keys `timeout_seconds`, `maximum_output_bytes`, and `maximum_receipt_bytes`. R6 maps those manifest values explicitly into the installed invocation schema. It does not modify `src` or the installed parser.

The worker's `--preflight-invocation` mode calls the same `parse_invocation` function as its initializer path and exits before source admission or runtime initialization. The focused test launches the worker as an isolated Python `-I -B` child, generates the actual canonical R6 invocation from the frozen manifest, writes it to a temporary file, and parses it through the installed `ashlar_host.configured_local_cli.parse_configured_local`. It asserts the parsed operation, carrier profile, and producer timeout. No Spark or Delta code is called.

R6 uses a new profile and the absent attempt parent `/private/tmp/ashlar-g03-sdk010-delta4-init-r6-attempts-20261101`. This packet has not run `prepare`, `slot`, or `execute`; it has no permit. Any later initialization requires separate review, a fresh process inventory, and a new one-shot permit. Transaction 1 remains blocked on a successful supervisor-closed R6 installation result; the existing transaction gate still points to consumed R4 and requires reviewed rebinding after R6 completion.

Focused isolated-child test:

```sh
cd docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r6-sdk010-initialize-only
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -B -m unittest -v test_r6_init_packet.InitPacketTests.test_real_generated_invocation_parses_in_isolated_worker_child
```
