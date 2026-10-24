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
