---
ddx:
  id: ashlar.evidence.source-holder-a1-live-port-gap-20261024
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
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.source-holder-interval-proposal-20261023
      kind: informed_by
---

# A1 live read and pin port gap

Source review on 2026-10-24 found no safe production adapter for the managed
publication-read or native-pin ports. This is a design proposal and gap record;
it changes no contract approval, campaign path, ACK authority, or runtime claim.
The existing helper in
[`_source_holder_interval_proposal.py`](../../../../src/ashlar_host/_source_holder_interval_proposal.py)
tests only fake-port choreography. Its successful calls cannot establish held
native custody.

## Required port semantics and authority

| Port | Required open, renew, and close behavior | Trustworthy authority needed |
| --- | --- | --- |
| `ManagedPublicationReadPort` | `open(receipt_bytes, vector, context)` must bind the exact retained destination-receipt bytes to the complete four-role `PinVector` and selected runtime identity. `renew()` and `close()` must independently reopen the original publication and six-role installation, compare UUIDs, versions, schemas, protocols and runtime/principal identity, and prove every pinned version is still readable. A schema-only `LIMIT 0` inspection is insufficient. | The selected managed runtime's authenticated UC reader, exact receipt and manifest custody, and an independent source-derived oracle or equivalent complete versioned data check. Current retention configuration may show a time window; it cannot alone prove retained files are readable. A successful prior reopen or verifier report is not renewal authority. |
| `NativePinPort` | `open(vector, source_selection, context)` must acquire one live handle for all four exact role UUID/version pairs. `renew()` must assert the original complete pin is still active and the independently admitted retention window still covers the source and publication. `close()` must repeat both checks before releasing the handle. A vector value or registration row alone is not a hold. | The native pin service under an authenticated read-only principal, a transaction/lock that protects the original pin for the interval, and a separately admitted retention provider bound to original publication/source custody. The provider must reobserve the full target UUID/version vector, current retention facts and usable deadline; availability still needs an actual versioned-read witness. |

CONTRACT-001 defines these requirements in its proposed source-holder custody
ports. TD-009 says the path remains unavailable for campaign ACK until a reviewed
renewal handoff exists. CONTRACT-003 preserves the complete publication vector
and source/ACK custody; CONTRACT-004 requires native snapshot and retained-file
availability admission.

## Existing owner limits

[`managed_current_spark_receipt_read.py`](../../../../src/ashlar_host/managed_current_spark_receipt_read.py)
reopens a supplied request, manifest and expected receipt. Its `_renew()` checks
Spark runtime identity and the six-role inventory. Resolver inspection checks
schema at pinned versions with `LIMIT 0`; `_read_and_compare_source_bags()` gives
a substantive data witness only when an independent source oracle is supplied.
`close()` repeats runtime and inventory checks, not the complete read witness or
retention admission. The reader accepts an injected Spark session and does not
own a session factory or retained-file authority.

[`ashlar.pins.PostgresPins.hold()`](../../../../src/ashlar/pins.py) checks the
full registered vector and yields it inside one transaction. The transaction
can hold native locks, but the public context manager has no renewable live
handle. Its injected `authorize_read` policy is rechecked at entry and exit; it
does not specify a source-bound retention authority. The concrete
[`ManagedPinReader`](../../../../src/ashlar_host/managed_pin_reader.py)
authenticates a bounded PostgreSQL transaction and caller privileges; it does
not provide an external retention policy or prove Delta files remain readable.
`RetentionGate` and `validate_publication_retention()` check admitted retention
metadata and deadline, not data-file availability. These owners are useful
building blocks, but do not yet compose to the proposed port contract.

The remaining source-side prerequisite is separate: Truss 0.0.49 exposes
operation-level reread, ACK and reconciliation calls, not an owner that retains
and renews the exact feed registration, consumer, epoch, checkpoint, destination
and ACK scope across this interval. See the source API boundary recorded in the
[interval proposal](source-holder-interval-proposal-20261023.md).

## Minimal implementation sequence

After Truss exposes and independently evidences that source-owned renewal API:

1. Freeze exact argument and byte semantics for the three ports and ACK owner in
   the governing contracts, including the selected runtime, retention source,
   renewal cadence/deadline, and close-time observations.
2. Implement the Truss custody handle first, then native-pin and managed-read
   handles against their existing owners. Each must refuse missing authority,
   vector drift, runtime drift, retention shortfall, unreadable versions, expiry,
   duplicate close, or failed independent close; no handle may mint authority
   from a report or Boolean.
3. Add focused fake-owner drift/refusal tests, then bounded tests against the
   selected native owners with no source mutation or ACK. Verify that all three
   renewals occur before commit and every acquired handle is closed after every
   exit while the primary error is preserved.
4. Obtain independent review of the port implementations and close evidence.
   Only then design a separate holder integration; campaign wiring and ACK remain
   gated on full live-interval qualification.

No source edits, native connections, cloud actions, ACKs, or tests were performed
for this note. The A1 source-holder production-target qualification remains open.
