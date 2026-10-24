---
ddx:
  id: ashlar.evidence.source-holder-interval-proposal-20261023
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: TD-009
      kind: informed_by
---

# Source-holder interval proposal: offline choreography

Implemented the private proposal-only helper in
`src/ashlar_host/_source_holder_interval_proposal.py`, following the draft
source-holder port sequence in CONTRACT-001 and the live-interval section of
TD-009. It opens managed-publication read, native pin and Truss source-custody
ports in order, renews all three before yielding to a caller, and closes acquired
handles in reverse order on every exit. It has no ACK port, is not exported or
wired into campaign or Truss entrypoints, and creates no native connections.

The nine fake-port tests passed with
`PYTHONPATH=src python3 -B tests/test_source_holder_interval_proposal.py`.
They cover open and renewal ordering, cleanup after partial open, primary-error
preservation, all-close attempts, close-failure reporting, distinct handle
identity, and refusal to renew a retained interval after it closes. Port return
values are ignored because the draft does not specify them; refusal is signaled
by an exception. This checks only the local orchestration semantics for injected
fakes.

No concrete managed-read, retained-file pin or accepted Truss custody adapter
exists in this slice. A successful renewal observation does not establish
continuous lease coverage. Source registration admission, effective principal
and writer exclusion, live renewal through commit and reconciliation, protected
ACK, and contract approval remain open. This helper returns no ACK-ready value
and supports no release or campaign qualification claim.

## Current Truss 0.0.49 renewal boundary

The read-only public API observation confirms that Truss 0.0.49 exposes
`read_feed_in_transaction`, `reread_feed_in_transaction`,
`acknowledge_feed_with_receipt_in_transaction`, and
`reconcile_feed_ack_in_transaction`. These are operation-level APIs, not a
source-owned lease or interval handle. `reread_feed_in_transaction` can recover
the exact retained page under current source authority, while receipt ACK writes
the source receipt and reconciliation performs a fresh read-only comparison.
Neither API keeps source custody continuously admitted between a managed
publication read and the later ACK, nor renews it through the reconciliation and
close sequence. See the [public API observation](current-upstream-release-20261023/truss049-public-receipt-api-observation-20261023.md)
and Ashlar's existing [feed-session implementation](../../../../src/ashlar_host/truss_feed_sessions.py).

The R7 local campaign worker does not bridge that gap. Its publication and
managed verifier complete before `TrussFeedSessions` enters
`_perform_receipt`; that method holds `pins.hold(...)` only for its own ACK or
reconcile call. Its source transaction, destination resolver and pin checks are
valuable point-in-time admissions, but they do not constitute one owner that
keeps all three authorities live across publication, terminal verifier readback,
ACK commit, fresh reconciliation, and independent close. The worker's
`run_genesis_publication` and `continue_actual_seed_page` therefore cannot be
treated as release-qualified ACK orchestration merely because they call this
receipt path. See the [R7 readiness packet](current-upstream-release-20261023/truss049-r7-readiness-20261023/README.md)
and its [local campaign worker](current-upstream-release-20261023/truss049-r7-readiness-20261023/campaign_worker.py).

No implementation is wired from the proposal helper to a public Truss
renewal-owner capability because the inspected release exposes none. A
production slice must wait for an independently evidenced source-owned owner
that can retain and renew the exact registration, consumer, epoch, checkpoint,
destination ACK scope, and original receipt bytes; its custody must span the
publication/verifier completion boundary and remain live through commit,
reconciliation, and independent close. The managed-publication and native-pin
adapters also need substantive reopening/retention checks, not successful fake
port calls or a `PinVector` value. Until those capabilities and their authority
are selected and admitted, no campaign path may expose or call ACK through this
helper. Existing operation-level ACK/reconcile APIs remain available to their
current callers but do not close this interval requirement. No source, managed
publication, ACK, or native effect was performed for this finding.
