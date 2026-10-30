---
ddx:
  id: ashlar.evidence.g03-sdk058-delta4-bridge-runtime-20261030
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk058-delta4-realization-candidate-20261030
      kind: informed_by
    - id: ashlar.evidence.g03-sdk058-source-correspondence-20261030
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# SDK 0.5.8 Delta 4 bridge runtime check — 2026-10-30

The versioned source-backed bridge passed its focused Python tests in an
isolated environment with `mini-racer==0.14.1`: two tests passed in 1.249s,
including actual MiniRacer execution and exact proposal comparison for all six
`.4-xxhash64` roles against the frozen SDK 0.5.8 outputs. The check also verifies
bridge/resource custody and the retained G03 qualification baselines.

The 521,802-byte IIFE is pinned in the versioned
[`realization.json`](../../../../../../src/ashlar_host/resources/local-current-delta4-sdk058/realization.json)
manifest (SHA-256 `8ca2c7ec908c80b78d4bac66144511330f56ad722b9149175c6d5fb0495bcb93`).
The adapter remains a candidate: existing production call sites still use the
historical 0.5.6 bridge. No Spark or cloud runtime, CREATE execution, table,
catalog, publication, source holder or ACK activity is qualified here.
