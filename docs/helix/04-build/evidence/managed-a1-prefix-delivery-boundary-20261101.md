---
ddx:
  id: ashlar.evidence.managed-a1-prefix-delivery-boundary-20261101
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
    - id: ashlar.evidence.a1-managed-source-prefix-owner-gap-20261101
      kind: informed_by
---

# Managed A1 prefix delivery boundary

The pure `managed_current_prefix_delivery` boundary now accepts a closed
`ashlar-managed-current-source-prefix/0.2` package, separately selected external
anchors, exact canonical R04 schedule bytes, and exact Truss page/prior-cursor/
next-cursor bytes in global slot order. It returns the unchanged original prefix
bytes and digest with its correspondence summary, so a caller can pass that exact
carrier to the selected `.4` resolver. It verifies that each page reconstructs
the exact source batch, source checkpoint, and request retained in the package.
The existing `/0.2` validator checks the complete A/B genesis progress, selected
identities, R04 assignments, and predecessor/manifest chain before the delivery
summary is returned.

The helper establishes correspondence only. Separately selected anchors and
page observations still require their source owners' authority and freshness
checks. This code does not establish Truss acceptance, source or publication
authority, managed Unity Catalog execution, native effects, retention, or ACK.
The owner gap in `a1-managed-source-prefix-owner-gap-20261101.md` remains open
until those independently admitted inputs are available.

Focused verification on Python 3.11: all 16 tests in
`test_managed_current_prefix_custody_v2.py` and all 10 tests in
`test_managed_current_genesis.py` passed. The prefix tests include valid ordered-
page delivery and refusal when page bytes or global slot assignment differ. The
fixtures demonstrate correspondence behavior only; they are not source or
managed-runtime evidence.
