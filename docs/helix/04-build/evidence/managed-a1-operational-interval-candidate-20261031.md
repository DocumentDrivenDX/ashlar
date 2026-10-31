---
ddx:
  id: ashlar.evidence.managed-a1-operational-interval-candidate-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.managed-a1-readonly-refresh-20261031
      kind: informed_by
    - id: ashlar.evidence.managed-a1-operational-retention-profile-proposal-20261031
      kind: informed_by
    - id: ashlar.evidence.source-holder-a1-live-port-gap-20261024
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# A1 managed operational interval candidate — 2026-10-31

## Candidate boundary

[`src/ashlar_host/managed_operational_retention_interval.py`](../../../../src/ashlar_host/managed_operational_retention_interval.py)
implements the selected 15-minute operational observation profile as a
read-only interval owner. For one frozen `.4-xxhash64` publication selection,
it obtains fresh sessions and performs substantive four-role reads at `open`,
`pre_ack`, and `close`. Each observation checks exact source-custody digests,
runtime identity, the complete role vector, retention settings, predictive
optimization `ENABLE`, and role comparison results. The interval expires at
900 seconds, exposes no ACK operation, and does not publish, register a pin, or
mutate a source cursor.

This is pre-existing Ashlar-side **offline choreography** for the owner-selected
three-observation profile: one bounded owner orders the reads, policy
observations and source-custody observations and refuses drift. This candidate
records that boundary; it did not add or qualify a production adapter. It does
not close the production adapter gap in the [A1 live port review](source-holder-a1-live-port-gap-20261024.md).
The injected Spark session factory, source custody observer and policy observer
are authority boundaries. Passing protocol fakes or replaying report values is
not managed qualification.

## Adapter seam review

The three source-holder ports remain `Protocol` declarations in
[`_source_holder_interval_proposal.py`](../../../../src/ashlar_host/_source_holder_interval_proposal.py);
that module provides fake-port choreography only. No safe production adapter
can be assembled from current owners:

- `ManagedPinReader.transaction` plus `PostgresPins.hold` can hold and recheck
  the exact native pin vector inside one transaction. They expose no renewable
  handle or bounded remaining-lifetime proof. A wrapper claiming either would
  overstate the owner semantics.
- The operational interval performs substantive versioned UC reads, but closes
  each fresh session after its observation. It does not retain a read lease or
  prove continuous availability between observations.
- No selected Truss owner retains and renews exact source, destination and ACK
  custody across this interval. The source-holder gap review records that
  prerequisite as open.

Implementing these authority-bearing ports needs new owner capabilities and
native owner evidence. Fake-backed tests exercise refusal and choreography only;
they cannot close this gap. No adapter was added in this candidate.

## Focused evidence

The focused tests are in
[`tests/test_managed_current_spark_receipt_read.py`](../../../../tests/test_managed_current_spark_receipt_read.py),
class `ManagedOperationalRetentionIntervalTests`. They exercise three fresh
sessions, four substantive role reads per observation, seven-day data and log
retention, predictive optimization enabled, close on failure, and refusal when
the maintenance inventory changes before ACK. They are fake-owner tests and
establish choreography only. No cloud connection, schema, compute, ACK, source
mutation, or scale run was performed for this candidate.

## Live admission inputs still required

1. A caller-owned fresh-session factory bound to a dedicated Jobs cluster, with
   authenticated principal, workspace, job/run/task/cluster identity, DBR,
   Spark, Delta and Unity Catalog mode observed natively at every stage. The
   current workspace refresh exposes only a Personal Compute policy; it does
   not admit this cluster or task attachment.
2. A source observer that independently reopens the original Truss registration,
   consumer, exact cursor/checkpoint, destination registration and ACK scope,
   current/retained page, and original receipt custody at all three stages.
   These observations must be owned by the source and prove complete exact
   scope; saved digests alone are not authority.
3. A policy observer that reads the selected four managed tables' current
   retention values, predictive optimization state, and maintenance inventory
   natively. It must establish the agreed seven-day retention minimum and keep
   `ENABLE`; it cannot infer these from the earlier workspace refresh.
4. Separate source-owned renewable `ManagedPublicationReadPort`, `NativePinPort`,
   and `TrussSourceCustodyPort` handles, bound to the same original request and
   full publication vector, kept live through protected ACK and independently
   closed. The operational interval's `close()` is an observation receipt, not
   a replacement for those holder ports.
5. An admitted runtime/access mode, task and total-trial cost bound below $100
   (aim below $10), plus prior spend. No paid compute may start until those
   inputs and the owner’s cleanup plan are admitted. Cleanup remains limited to
   the explicitly owned schema.

## Disposition

The existing effect-free choreography has focused test coverage, but the
managed A1 gate remains held. The 2026-10-31 read-only refresh
established catalog grants and effective predictive optimization `ENABLE`,
while exposing only Personal Compute. No dedicated compute creation/task
authority, runtime/access-mode admission, live source/ACK custody, native pin
renewal, or whole-trial cost bound is established here. This candidate makes no
managed publication or production support claim.
