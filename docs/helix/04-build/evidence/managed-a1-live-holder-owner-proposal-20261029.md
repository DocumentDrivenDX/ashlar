---
ddx:
  id: ashlar.evidence.managed-a1-live-holder-owner-proposal-20261029
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
    - id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.source-holder-truss051-capability-20261029
      kind: informed_by
    - id: ashlar.evidence.source-holder-a1-live-port-gap-20261024
      kind: informed_by
---

# Managed A1 live holder owner proposal — 2026-10-29

**Disposition: no production port can be implemented from currently admitted
owners.** This read-only review identifies the smallest owner/API work needed to
reopen the A1 source-holder gate. It does not authorize cloud execution, source
or ACK calls, pin registration, retention changes, or a production claim.

## Findings from installed owners

The selected Truss 0.0.51 public `NativeFeedCapability` has current page read,
retained page reread, exact-receipt ACK and fresh reconciliation methods. It has
no named lease API. Its SQL does not lock the consumer during polling or
retained-page reread. For the post-verifier interval, the existing API can still
support active checks: each renewal must read the current page and reread the
original retained page, compare every original registration/cursor/content
field, and stop on any drift. ACK's `FOR UPDATE NOWAIT` checkpoint compare and
set protects the final transition. This remains a design inference from the
installed public API and SQL, not a qualified source-owner implementation or
native run. See the selected `.51` capability receipt.

Those methods prove observations about a page and its feed consumer; they do
not admit or renew the separately retained destination registration and exact
ACK scope required by `CONTRACT-001`'s `TrussSourceCustodyPort`. The source
holder must independently retain the original destination registration,
consumer/feed registration, cursor bytes, destination ID and ACK-scope binding
with their authoritative source custody. Re-reading a current or retained page
cannot manufacture or extend that authorization.

`ManagedCurrentSparkReceiptReader.reopen()` already reopens the exact manifest,
six-role installation, four-role vector and pinned resolver snapshots. It can
compare complete data bags when given the frozen source-prefix oracle. It is
not a live holder: the session is injected, its selected Truss source version
ends at 0.0.50, and the observation is explicitly offline. `close()` repeats
runtime and inventory checks, not the complete source-bag comparison or an
independent retention admission.

`PostgresPins.hold()` verifies the full registered vector while a database
transaction holds native registry locks and rechecks read authorization at
exit. It exposes no renewable handle or interval-level renewal operation.
`RetentionGate` reobserves table UUIDs and retention settings and checks a
finite deadline; its own contract says these observations do not prove that
files remain readable or fence maintenance. Neither a pin row nor a long-lived
PostgreSQL transaction supplies that missing external authority.

The current Delta access path is Spark SQL `VERSION AS OF` for exact literal
versions, with `DESCRIBE DETAIL`/`DESCRIBE HISTORY` and table-property reads for
identity, snapshot and retention metadata. The project pins `delta-spark==4.4.1`
for local development, while the A1 managed Spark/Delta build remains
unobserved. These read APIs do not reserve historical files against maintenance;
the reviewed Ashlar Delta surface exposes no renewable native retention lease.

## Exact missing external capability

Ashlar needs an independently admitted managed-retention provider for the
complete original four-role `(table name, UUID, version)` vector. The provider
must bind to the original receipt/manifest custody and return a bounded usable
deadline plus current native version/file-availability evidence. It must also
establish that all maintenance actors capable of removing those files are
covered by an enforceable retention hold for the interval, or refuse admission.
Current retention properties, successful earlier reads, and a list of files
observed once are not a renewable hold. No installed provider with this
authority was found in the Ashlar or selected Truss owners reviewed here.

This is the external blocker. Ashlar also needs a real host-owned live adapter
to invoke a managed Spark session and the external provider on open, renew and
close. That adapter can reuse existing reader and pin primitives after the
provider and host authority are admitted; adding protocol fakes or another
offline observation type would not close the gap.

## Minimal integration path

1. Admit and document a retention provider outside the verifier task. Its
   `admit(receipt_bytes, vector, source_custody, context)` must authenticate its
   independent authority, validate original custody and the exact full vector,
   and return an immutable deadline/hold identity. `renew(handle)` must
   independently reobserve all UUID/version targets, active maintenance hold,
   and usable deadline. `close(handle)` must repeat those checks before
   releasing the hold. Partial vectors, changed identities, unavailable
   evidence, expiry or loss of the hold refuse the interval.
2. Add a managed-read owner that accepts original request/manifest/receipt
   bytes, the exact four-role vector, frozen accepted source-prefix bytes and
   their digest, and a host-owned Spark-session factory. `open()` creates a
   fresh admitted session and runs the existing source-prefix rebuild plus
   complete pinned bag comparison. `renew()` repeats that work in a fresh
   session and requires exact equality of receipt, manifest, six-role
   inventory, runtime/principal identity, full vector and source comparison
   digest. `close()` performs one final renewal and closes the session even
   after failure, preserving the primary error. A missing source oracle or
   bag-comparison digest cannot authorize ACK.
3. Add a renewable native-pin handle around the authenticated PostgreSQL
   transaction. `open()` verifies the complete original registered vector and
   yields only after retention admission. `renew()` rechecks all active pin
   rows and authorization in the held transaction and calls the independent
   provider's renewal. `close()` repeats both while the transaction is still
   held, then closes it. A yielded `PinVector` without a live handle is not
   authority.
4. Implement the `.51` source interval owner using existing public methods,
   behind an independently admitted source-custody owner. On open, bind the
   actual feed/consumer registration and cursor bytes to the original retained
   destination registration, destination ID and exact ACK-scope evidence. On
   each renewal, re-admit that retained destination registration and ACK scope
   from their authoritative source store, then call current-page read and
   exact retained-page reread in a read-only transaction and compare all
   original page, registration and checkpoint bytes. Immediately before ACK,
   repeat both the external destination-registration/ACK-scope admission and
   the Truss reads; submit the exact original destination receipt once, commit
   once, reconcile from a fresh transaction, then close with a current
   checkpoint, retained-page and destination-scope check. Truss `.51` rereads
   alone cannot establish destination registration or ACK-scope authority.
   Unknown commit outcomes reconcile; they never retry ACK. No Truss lease API
   is required unless a controlled native test shows these checks cannot meet
   the bounded interval contract.
5. Only after these owners exist should the source-holder orchestrator acquire
   all three handles, renew them immediately before ACK, commit and reconcile,
   and attempt independent closing for every acquired handle after success,
   refusal, deadline or cancellation. Release no ACK success on a failed
   renewal or close.

## Qualification required before any A1 use

Focused fake-port tests may check ordering and refusal behavior, but they do not
qualify the authority. Required evidence includes: an independently admitted
retention-provider identity and maintenance coverage; a native exact-version
read witness against the accepted source prefix; renewal after source-page,
runtime, table UUID, version, permission, retention and deadline drift; a
competing Truss ACK between renewals followed by refusal or exact CAS behavior;
lost ACK-commit response reconciled without retry; and closing after body,
renewal, cancellation and timeout failures. Preserve original bytes and full
vector, and prove no ACK credential reaches either Jobs task. A controlled
native source test and independent owner review are required; no such test was
run for this proposal.

The source holder remains unavailable until the external retention capability
and actual managed-session owner are admitted and this integration is reviewed.
The Jobs admission packet remains effect-free, and no launch follows from this
proposal.
