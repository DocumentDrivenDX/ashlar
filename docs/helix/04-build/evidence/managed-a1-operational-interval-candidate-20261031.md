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

This is Ashlar-side **offline choreography** for the owner-selected
three-observation profile. The source-facing adapter in
[`managed_operational_read_port.py`](../../../../src/ashlar_host/managed_operational_read_port.py)
binds one exact receipt, full vector and caller context, then maps `open`, one
`renew` and `close` to those three observations. It is a sampled operational
read adapter: its remaining lifetime is only time to the 900-second profile
deadline. It is not a file-retention lease, a continuous availability proof, a
pin handle, or source/ACK authority.

The injected Spark session factory, source custody observer and policy observer
remain authority boundaries. The adapter is not connected to a selected native
owner or campaign/ACK orchestration; protocol fakes and saved report values do
not qualify managed execution. It therefore narrows the Ashlar read-port code
gap but does not close the broader gap in the [A1 live port review](source-holder-a1-live-port-gap-20261024.md).

## Adapter seam review

The proposal module's three source-holder ports remain disconnected from
campaign code. Ashlar now has a bounded read-observation adapter, but no current
owner supplies all the native authority needed to use it as the full CONTRACT-001
source-holder handle:

- `ManagedPinReader.transaction` plus `PostgresPins.hold` can hold and recheck
  the exact native pin vector inside one transaction. They expose no renewable
  handle or bounded remaining-lifetime proof. A wrapper claiming either would
  overstate the owner semantics.
- The operational read adapter performs the selected exact-version observations
  and closes each fresh session after that observation. It does not retain a
  read lease or prove continuous availability between observations.
- No selected Truss owner retains and renews exact source, destination and ACK
  custody across this interval. The source-holder gap review records that
  prerequisite as open.

Using the adapter for a managed trial still needs admitted native session,
source, and policy owners. Implementing `NativePinPort`, `TrussSourceCustodyPort`,
and protected ACK needs additional owner capabilities and native owner evidence.
The adapter tests exercise binding, refusal, ordering and closing choreography
only; they cannot close these gates.

## Focused evidence

The focused tests are in
[`tests/test_managed_current_spark_receipt_read.py`](../../../../tests/test_managed_current_spark_receipt_read.py),
class `ManagedOperationalRetentionIntervalTests`. They exercise three fresh
sessions, four substantive role reads per observation, seven-day data and log
retention, predictive optimization enabled, close on failure, and refusal when
the maintenance inventory changes before ACK. Adapter tests also exercise exact
receipt/vector/context binding, one pre-ACK renewal, and an independent close
attempt after refusal or deadline. These are fake-owner tests and establish
choreography only. No cloud connection, schema, compute, ACK, source mutation,
or scale run was performed for this candidate.

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
4. Source-owned ACK and custody authority. The Ashlar read adapter can supply
   the sampled open/pre-ACK/close observations after its native observers are
   admitted; it does not supply a continuous read lease. `NativePinPort`,
   `TrussSourceCustodyPort` and protected ACK still require independent owners
   bound to the same request and full publication vector, held through
   reconciliation and independently closed.
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
