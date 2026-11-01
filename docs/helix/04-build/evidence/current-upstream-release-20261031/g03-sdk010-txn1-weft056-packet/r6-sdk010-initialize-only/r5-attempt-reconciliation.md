# R5 one-shot failure evidence

Original attempt:
`/private/tmp/ashlar-g03-sdk010-delta4-init-r5-attempts-20261101/ashlar-g03-sdk010-init-2f44fbff52e74bd9a12ff1895847cf03`.
R5's one-shot dispatch is consumed. The original attempt and receipts remain
unchanged and are not R6 inputs.

The worker traceback reaches `r5_init_worker.py:41` and fails while parsing
`initialize-invocation.json`. The exact validation failure is in
`src/ashlar_host/configured_local_cli.py:63`: `_closed` requires the producer
object's exact key set to be `source`, `bun`, `git`, `timeout_seconds`,
`maximum_output_bytes`, and `maximum_receipt_bytes`. R5 generated camelCase
`timeoutSeconds`, `maximumOutputBytes`, and `maximumReceiptBytes` from its
manifest. The mismatch is therefore in the candidate's invocation mapping; the
installed parser contract and `src` are unchanged.

The supervisor's `execution-status.json` retains
`classification: uncertain-after-dispatch` and `effectsPossible: true`; its
SHA-256 is
`37168dccbc09f825a9b6c2f162e0ceb7deaa949bceddf2c91a5121641ebde05b`. The
separate `process-group-cleanup.json` reports `inventoryVerified: true`,
`empty: true`, and no initial, remaining, or terminated members; its SHA-256 is
`795505c4b6bfa7efc12a226f246da684ed8bc40d39494e114aa02e353fee4e67`.
`initialize.stderr.log` SHA-256 is
`1381ef6c3313e81ebf5fa287ead1cc9e578a2e35aba5772d66a6277ea14f5e39`;
`initialize.stdout.log` is empty (SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).

Custody hashes: `dispatch.json`
`28aec2c8bf4e47aa5b6ac4fc974d891480ea14d3e16c1230b6dcab17790ccb46`;
`prepared.json`
`a7fe5797ba08419b66a5eb91058142df5497018007414811a42dc8bc623aa54d`;
`permit.json`
`3b68f4e3370aefc6940d4427b2e59c1e8883e7750e616b528c817736e0540e1b`;
`process-slot.json`
`4d4396a5e6e87795b9bb64cb30c31feb2d9aa6c0e0073c4ed069b7e392e19de2`;
`dispatch-process-slot.json`
`f349c30b7afd3ea44d27689de8bdbb59397a8b3e5ed3e11d0daee222254264f4`;
`initialize-invocation.json`
`1cca3113b95da34d5ff8d4f5db0774bf5cfdbe975e11e9c224329a6a53ae530e`.

Read-only inspection found no `installation/`, `initialize-output/`, or
`init-result.json`. The traceback fails at the invocation parser before the
worker calls `initialize_configured_local`; stdout is empty and supervisor
cleanup independently verified an empty process group. These facts support a
narrow pre-native-effect conclusion for the observed R5 failure, while R5's
supervisor classification remains uncertain-after-dispatch. This evidence does
not authorize retry or reuse. R6 has a separate, currently absent attempt
parent and requires a new review and permit.
