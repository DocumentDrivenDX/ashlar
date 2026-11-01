---
ddx:
  id: ashlar.evidence.native-pin-port-owner-api-gap-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: ashlar.evidence.source-holder-a1-live-port-gap-20261024
      kind: informed_by
    - id: ashlar.evidence.managed-a1-operational-interval-candidate-20261031
      kind: informed_by
---

# Native pin port owner API gap — 2026-11-01

## Finding

No production `NativePinPort` was added. The existing PostgreSQL owner supports
`PostgresPins.hold(vector, context=...)`: it authorizes the vector, enters one
transaction, checks UUID availability, asserts every exact registered pin,
checks the complete inventory, yields the vector, then repeats authorization,
availability and inventory checks before transaction close. Its transaction
adapter keeps locks for the context lifetime.

That surface does not implement the proposed renewable handle. During the
yield, callers cannot request an explicit renewal against the original vector;
the final check occurs only while exiting the context. `PinPolicy.authorize_read`
also accepts only `(vector, context)`, so the `source_selection` required by
`NativePinPort.open(vector, source_selection, context)` is not checked by the
pin authority or bound to the held transaction. A wrapper around `hold()` would
therefore report `renew()` success without rechecking and would silently ignore
one of the port's admitted inputs.

## Exact missing owner capability

Before Ashlar can expose this port, the PostgreSQL pin owner needs one
source-bound, transaction-scoped live-handle API that:

1. admits the complete `PinVector`, exact source selection and caller context
   together before opening the transaction;
2. retains the existing authenticated transaction and row-lock lifetime for
   every exact role UUID/version pair;
3. exposes a renewal operation on that same live transaction that reasserts
   UUID availability, each exact active pin, complete inventory equality and
   the owner admission for the same source selection/context; and
4. performs an independent final renewal before transaction close, refusing
   duplicate close or any changed vector, source selection, context, pin or
   failed transaction.

The source-selection admission must be supplied by the accountable owner and
must bind to the original publication/source custody, not merely compare an
opaque caller value to itself. This capability would establish exact pin
identity/custody only. It would not establish Delta-file readability or
retention duration; those remain separately admitted and observed under the
contract profile. No pin registration, release, database access, source action
or ACK was performed for this finding.

## Disposition

The existing transaction proves a held lock and entry/exit checks, but cannot
truthfully be adapted to the requested renewable, source-bound port without
the owner API above. Native-pin production integration remains open. No code or
tests were added because an Ashlar-only adapter would invent the missing owner
authority.
