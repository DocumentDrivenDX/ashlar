---
ddx:
  id: ashlar.evidence.g03-sdk058-local-delta4-txn1-packet-20261030
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk058-local-cutover-candidate-20261030
      kind: informed_by
    - id: ashlar.evidence.g03-sdk058-source-correspondence-20261030
      kind: informed_by
    - id: ashlar.evidence.g03-delta4-transaction1-zstd-preflight-failure-20261030
      kind: informed_by
    - id: ashlar.evidence.g03-delta4-transaction1-role-alias-preflight-failure-20261030
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# G03 SDK 0.5.8 local Delta 4 packet candidate

This is an additive, effect-free packet for one local six-role `.4-xxhash64`
initialization followed by transaction `typed-graph-1`. It binds the current
Ashlar source commit and `src/ashlar` plus `src/ashlar_host` tree digests, the
retained Spark 4.2.0 / Delta 4.4.1 / Java 21 runtime, the SDK 0.5.8 source
admission inputs and the versioned source-backed `/0.2` bridge. The preflight
checks exact SDK source correspondence, all six frozen proposal objects and
runtime bytes without opening Spark. The worker adds the same explicit
`sdk058_admission` configuration after parsing the invocation for both
initialization and transaction 1, and records the `/0.2` carrier profile and
policy identity before reporting initialization success.

The packet's one-shot r3 dispatch consumed its execution permit and reached the
450-second supervisor deadline after initialization and source admission, before
the first metadata operation. No transaction completed. The exact durable
boundary and current diagnosis are in the [r3 failure evidence](../../g03-sdk058-txn1-timeout-r3-20261030/README.md).
No retry is authorized by the spent permit. Its `local[1]` Spark setting,
512 MiB retained attempt limit, 2 GiB process-tree RSS cap, no-retry rule and
output bounds are inherited into its versioned manifest. The scoped transaction
would end after the original graph INSERT effects with the expected six-role
head vector; it does not publish a manifest, perform protected ACK, run
consumer queries, or claim cloud or production authority.

R1 initialized six roles and stopped at the separate-session ZSTD writer-setting
admission before graph effects; r2 initialized six roles and stopped at the
local alias preflight before graph or metadata operations; r3 reached the
supervisor deadline after reservation and before its first operation. All three
dispatches are consumed and their retained roots must not be reused. The packet manifest records
their exact roots and fixes a distinct fresh parent at
`/private/tmp/ashlar-g03-sdk058-delta4-txn1-attempts-20261030`; the parent was
observed absent during packet preparation. The r3 dispatch later created its
consumed attempt root under that parent; see the linked failure receipt.

Run only these effect-free checks when reviewing the packet, using its pinned
Python interpreter:

```sh
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python \
  -m unittest discover -s docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-local-cutover-candidate-20261030/one-shot-sdk058-native-attempt \
  -p 'test_packet.py'
```

The test suite performs local Node source-checker and MiniRacer bridge
preflights, verifies the retained runtime, and exercises invocation construction
and supervisor failure handling. It does not call `prepare` or `execute`, create
an attempt root, start Spark, create Delta tables, or dispatch native SQL. A
future execution would require a fresh review-specific permit bound to the
resulting packet digest, current source commit, prepared attempt and no-retry
dispatch; this candidate includes no permit.
