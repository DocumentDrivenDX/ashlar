---
ddx:
  id: ashlar.evidence.source-bound-pin-policy-owner-input-gap-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: ashlar.evidence.source-holder-a1-live-port-gap-20261024
      kind: informed_by
    - id: ashlar.evidence.managed-a1-operational-retention-profile-proposal-20261031
      kind: informed_by
---

# Source-bound pin policy owner-input gap — 2026-11-01

## Disposition

No concrete source-bound `PostgresPins` policy or `NativePinPort` adapter is
implemented in this candidate. The publication owner selected the bounded
`ashlar-managed-operational-retention/0.1` claim, including its exclusions, but
that decision does not identify or authorize a live Truss source-custody owner.
The required owner evidence is absent, so wiring `authorize_source_bound_read`
to a permissive callback or treating the opaque `source_selection` argument as
authority would fabricate the missing boundary.

The base revision already contains `PostgresPins.open_live()` and
`_LivePinHandle`. They keep one authenticated executor transaction open and
recheck ordinary read authorization, the source-bound policy callback, UUID
availability, every active pin and the exact inventory on open, renew and
close. This proves transaction-scoped pin-row identity/custody when its
injected owners admit it. It does not prove source custody, a retention
admission, exact-version data readability, or an ACK interval. The method is
not wired to a `NativePinPort`, campaign holder, or selected Truss owner.

## Exact owner input required

Before implementing a real source-bound policy/adapter, the source owner must
identify and expose one selected, installed source API and its authentication
boundary that can independently retain and reread all of the following for
the same source slot:

| Required source-owned fact | Required binding/evidence |
| --- | --- |
| Original operation | Exact original request bytes and accepted source-prefix bytes/digest, selected by the source owner rather than a verifier report or caller-supplied digest |
| Publication custody | Exact original manifest and destination receipt bytes, with the complete four-role `(name, UUID, version)` vector and source-derived expected-bag custody |
| Registered source scope | Feed registration, epoch, consumer, prior checkpoint/cursor, retained current page/prefix, destination registration and ACK scope, independently reread from source-owned records |
| Authorization context | Authenticated owner principal and selected source/consumer/destination identities, bound to the exact `source_selection`, request bytes, vector and phase |
| Renewable lifecycle | A source-owned `open`/`renew`/`close` (or equivalent) that detects drift, has explicit deadlines, and retains recovery custody if renewal, reconciliation or close is uncertain |
| ACK settlement | An existing protected ACK owner that accepts only the original request/manifest/receipt bytes once, reconciles from a fresh source transaction, and retains exact outcome evidence without blind retry |

For `NativePinPort`, a separate native pin owner must also bind the exact full
vector and authenticated read context to `PostgresPins`' transaction-scoped
handle. Under the selected operational profile, this owner establishes exact
pin identity and custody only. It must not claim that pin rows reserve files.
The managed read observer must independently provide the exact-version,
complete-row availability checks, and any stronger retention-backed policy
requires its separately admitted provider. The current
`authorize_source_bound_read(vector, source_selection, context)` hook does not
define or supply those authorities.

## Evidence reviewed

- CONTRACT-001's proposed source-holder ports require independently retained
  source registration, cursor and ACK-scope evidence, exact original bytes,
  complete vector binding, and renewal/close checks.
- CONTRACT-004 states that resolver metadata or a verifier report cannot
  substitute for the external holder's live read/pin/source renewal boundary.
- The 2026-10-24 live-port gap records that the selected Truss API does not yet
  have an installed owner that retains and renews exact feed registration,
  consumer, epoch, checkpoint, destination registration and ACK scope across
  the interval. Its local candidate is disabled/unexported and is not source
  authority.
- The 2026-10-31 operational-profile decision accepts bounded observation
  checks and residual administrator/storage risk, while expressly leaving the
  native and source-custody owners as prerequisites.
- `tests/test_pins.py` exercises `open_live()` against an in-memory transaction
  executor and fake policies. The fake source selection is `object()` and the
  successful fake policy returns `None`; those tests cover callback ordering,
  pin-vector rechecks, cleanup and refusal choreography only. They do not prove
  that a selected Truss owner can make the admission.

## Test and action manifest

No code or tests were changed, and no test was run for this documentation-only
candidate. Relevant existing test manifest: `tests/test_pins.py`, especially
`test_live_source_bound_handle_rechecks_complete_vector_in_same_transaction`,
`test_live_handle_refuses_missing_source_authority_and_vector_drift`,
`test_live_handle_final_failure_preserves_body_primary`, and
`test_caught_renewal_failure_poison_closes_live_interval`. No cloud, database,
native Truss, source mutation, compute, ACK, or scale action was performed.

## Next admitted implementation step

Once the source owner supplies the selected API, principal, exact retained
source/receipt anchors, renewal semantics, and protected ACK reconciliation
authority above, implement the smallest adapter against those owners and add
focused tests for exact binding, drift refusal, expiry, failure-preserving
independent closes, and uncertain ACK custody. Until then, `open_live()` remains
an injectable native-pin seam; campaign ACK and production source authority
remain unavailable.
