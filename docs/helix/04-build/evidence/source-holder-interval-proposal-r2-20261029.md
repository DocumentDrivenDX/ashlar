---
ddx:
  id: ashlar.evidence.source-holder-interval-proposal-r2-20261029
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
    - id: ashlar.evidence.source-holder-interval-proposal-20261023
      kind: informed_by
---

# Source-holder interval proposal R2

The private, unwired `_source_holder_interval_proposal` helper now composes an
injected protected-ACK port with its three draft live handles. It binds original
request and receipt bytes and the opening context, requires finite remaining
coverage for the declared ACK window at opening and immediately before commit,
renews all three handles, attempts the ACK once, requires exact-byte reconciliation,
and attempts every independent close. An uncertain commit or readback cannot be
retried in the same interval. A reconciled ACK with a failed close remains an
incomplete slot. Primary failures retain close-failure details.

Sol reviewed two fail-open edges in the first R2 candidate: nonfinite duration
values could bypass the coverage comparison, and a second call could resubmit an
uncertain ACK. Luna repaired both and added exact opening-byte/context binding.
`PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p
test_source_holder_interval_proposal.py -q` passed 18 focused fake-port cases;
`git diff --check` passed. No native adapter, source-owned lease, managed pin,
Truss registration renewal, production principal policy, or campaign wiring is
supplied by this helper. Its injected readback tuple is a proposal-level port
contract, not proof that a real Truss ACK settled. No source, database, cluster,
publication, or ACK effect was performed.
