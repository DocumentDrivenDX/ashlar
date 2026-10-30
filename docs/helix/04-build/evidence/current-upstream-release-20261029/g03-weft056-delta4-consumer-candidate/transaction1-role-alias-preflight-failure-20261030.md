---
ddx:
  id: ashlar.evidence.g03-delta4-transaction1-role-alias-preflight-failure-20261030
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-weft056-delta4-consumer-candidate-20261029
      kind: informed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# Gate 3 transaction-1 local-role alias preflight refusal

The separately frozen r2 one-shot packet reached six-role configured-local
initialization, then refused in `_require_delta4_transport` before any graph or
metadata operation. The installation profile and targets use the established
local table aliases `attempts` and `manifest`; the G03 coordinator was looking
up the carrier roles `publication_attempt_phase` and `publication_manifest`
as physical target names. The retained attempt is
`/private/tmp/ashlar-g03-delta4-txn1-attempts-r2/ashlar-g03-txn1-b7ad716baf2943f1baab0caeddf5ee17`.

The retained inspection found six initial tables at v0/v1, no SQLite operation
rows, and no graph or metadata effects. This attempt is failure evidence and
must not be retried.

The local configured API intentionally retains `attempts`/`manifest`, which
existing Truss and Weft local paths address directly. G03 now derives the
inverse role map from `LOCAL_CURRENT_ROLE_TO_UMF`, checks it is a closed
six-role bijection, and resolves coordinator carrier roles only through the
journal-pinned profile. Each alias must declare the matching `carrier_role`,
the expected `local.configured.<alias>` table, and unique table/path/UUID
identity; target resolution then checks the retained UUID and path. Canonical
carrier-role keys remain the G03 inventory and effect-plan interface. The
realized-vector binder also resolves inventory names through this same map;
its four data-role manifest keys remain unchanged because those local names
already match their carrier roles. This does not rename `.3` tables or change
Truss-facing local aliases.

Focused inert verification passed 60 `.4` publication, snapshot, authority,
and runtime tests. No Spark session or native SQL was run for this correction.
The historical producer/bridge is not evidence of parity with the selected
UMF SDK 0.5.8 / Core 0.10.3 cohort; its latest-upstream qualification remains
FAIL_OR_UNKNOWN. Do not prepare another native packet until the exact frozen
G03 source and six-role realization are effect-free bound to that selected
cohort.
