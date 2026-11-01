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

The original `PostgresPins.hold(vector, context=...)` remains unchanged. It
authorizes the vector, enters one transaction, checks UUID availability,
asserts every exact registered pin, checks the complete inventory, yields the
vector, then repeats authorization, availability and inventory checks before
transaction close. Its transaction adapter keeps locks for the context
lifetime.

The candidate adds `PostgresPins.open_live(vector, source_selection,
context=...)` and a transaction-scoped handle. Its `renew()` and final `close()`
recheck read authorization, every exact active pin, UUID availability and
complete inventory on the same held session. It requires a separate
`authorize_source_bound_read(vector, source_selection, context)` policy method;
missing policy authority refuses before transaction acquisition. The handle
claims pin identity/custody only, not file readability or retention duration.

No production policy currently implements the source-bound method, so this is
an owner-side API seam, not a connected production `NativePinPort`. Until an
accountable policy binds the exact source selection to original source and
publication custody, the proposed port must refuse. The API cannot assert the
validity of a source-selection object merely because it retains the same Python
object reference.

## Exact missing owner capability

Before Ashlar can expose this port to a source holder, a concrete policy owner
must implement source-bound admission for the live-handle API that:

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

Focused fake-owner tests verify the source-policy gate, full-vector renewal,
drift refusal and final close checks. They do not qualify PostgreSQL locks or a
production source policy. Native-pin production integration remains open.
No database, cloud, source or ACK action was performed.
