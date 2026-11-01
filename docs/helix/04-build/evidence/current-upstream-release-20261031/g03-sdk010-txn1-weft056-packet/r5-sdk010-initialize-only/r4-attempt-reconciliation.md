# R4 one-shot failure evidence

Original attempt:
`/private/tmp/ashlar-g03-sdk010-delta4-init-r4-attempts-20261031/ashlar-g03-sdk010-init-5b8b61df4b044481afb9f6fc157720a2`.
R4's one-shot dispatch is consumed. Original receipts are not copied, rewritten,
or used as R5 inputs; the hashes below bind the retained files in place.

The child traceback identifies
`r4-sdk010-initialize-only/r4_init_worker.py:17`, where importing sibling
`r4_packet` failed under the supervisor's explicit Python `-I` mode. The
retained `execution-status.json` remains
`uncertain-after-dispatch` / `effectsPossible: true`; this packet preserves
that conservative classification. Its SHA-256 is
`37168dccbc09f825a9b6c2f162e0ceb7deaa949bceddf2c91a5121641ebde05b`.

The supervisor's separate `process-group-cleanup.json` reports
`inventoryVerified: true`, `empty: true`, no initial members, no remaining
members, and no terminated survivors. Its SHA-256 is
`795505c4b6bfa7efc12a226f246da684ed8bc40d39494e114aa02e353fee4e67`.
The stderr traceback file SHA-256 is
`f6618b217fcb026d490f7721ef00740b8ddb7bcb64c6a5437831cdad8a6c30e8`; stdout
is empty (SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`). The
dispatch receipt SHA-256 is
`24daa69ad62e27d7ac318804dfa9b523177f76f82d25a0823507d6105403e2dd`; the
prepared receipt is
`a9b2481b6a8d7ec73fe3a9875bc429345d1eb6c7c3dfffd7b10b8207bceb7303`; the
consumed permit is
`1ad4e4c25a281e5a002a15962884c1cb3430cfc00c5869967c30461f22c166b9`; the
immediate dispatch process-slot receipt is
`6a4dc1cb675ca4e9c09626f55bc05c3e414c9851d8d32e137f989e37186368de`.

Read-only inspection found no `installation/`, `initialize-output/`, or
`init-result.json` at the attempt root. The traceback occurs before importing
the R4 supervisor module and before the worker calls the configured-local
initializer; stdout is empty and verified process-group cleanup found no
members. This supports the narrow independent conclusion that the observed
failure occurred before a native Spark/Delta effect. It does not change R4's
supervisor classification, which remains uncertain-after-dispatch, and does
not authorize retry or reuse. R5 has a distinct, currently uncreated attempt
parent and requires its own review and permit.
