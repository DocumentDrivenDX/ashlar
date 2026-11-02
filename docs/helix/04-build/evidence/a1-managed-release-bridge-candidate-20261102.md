---
ddx:
  id: ashlar.evidence.a1-managed-release-bridge-candidate-20261102
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: ashlar.evidence.a1-managed-native-adapter-prerequisite-gap-20261101
      kind: follows
    - id: ashlar.evidence.managed-genesis-owner-local-20261101
      kind: informed_by
    - id: ashlar.evidence.managed-a1-prefix-delivery-boundary-20261101
      kind: informed_by
---

# A1 managed release bridge candidate — 2026-11-02

## Implemented scope

`src/ashlar_host/managed_a1_release_bridge.py` adds a post-publication owner
composition boundary for one prepared `.52` A1 slot. It requires the exact
`ManagedCurrentA1TrialPlan`, a complete four-role vector, original request,
manifest and destination-receipt bytes, an independent verifier-completion
owner, two renewable owners and a separate protected-ACK owner:

- the existing managed operational read plus native-pin port;
- a live Truss source-custody owner bound to the selected A/B output; and
- a Truss protected receipt-ACK owner with fresh exact reconciliation; and
- a separate read-only verifier-completion observer.

Before opening a read interval, the bridge asks the verifier-completion owner
to independently check terminal durable success for the exact request, manifest,
receipt and vector. A nonterminal, failed or mismatched observation refuses
before managed reads or source custody open. The completion record is
correspondence evidence; its authority comes from the supplied owner's live
observation, not from a caller-supplied result object.

The managed input must be the exact existing `ManagedOperationalReadPinPort`
and its exact `ManagedOperationalReadPort`, with original request, manifest,
receipt and vector bytes matching that port's frozen selection byte for byte.
The bridge does not parse and reserialize the request to establish this bind.
The read interval, Truss source holder and native pin transaction are separate
lifetime authorities. The managed composite's existing
`remaining_lifetime_seconds()` reports only the read interval. The bridge also
requires a distinct native-pin lifetime observation, and checks it alongside
the managed read and source-holder lifetime at open and after pre-ACK renewal.
Each must cover the explicit 30-second ACK/reconciliation window. The ACK owner
receives that same monotonic deadline on both commit and reconciliation; an
overrun returns incomplete even when exact readback succeeded.

The current `PostgresPins.open_live()` `_LivePinHandle` exposes no bounded
transaction-lifetime admission named
`native_pin_transaction_remaining_lifetime_seconds()`. Consequently, the production
`ManagedOperationalReadPinHandle` raises when asked for native-pin lifetime,
and this bridge refuses before ACK. That is an intentional fail-closed state;
the managed read deadline does not prove pin transaction lifetime. A live ACK
remains unqualified until the native pin owner supplies independent bounded
lifetime or renewal coverage for the same held transaction.

The existing managed read/pin composite opens both of its underlying handles;
the bridge opens it and the separate source-custody handle, renews both
immediately before ACK, calls the protected ACK once, and reconciles the exact
request, manifest and receipt bytes. A lost commit response does not trigger a
retry. It closes the managed read/pin composite first and source custody last,
so source custody remains current through the composite's final read and pin
checks. A close failure after matched reconciliation returns an incomplete
result. The interval limit is at most 900 seconds and describes the selected
operational observation interval, not a Delta file hold.

The publisher/verifier task remains separate from this bridge and receives no
ACK port. The bridge creates no compute, UC schema/table, source registration,
pin, publication or credential. It does not include a production
verifier-completion observer, Truss source port, UC read/pin observer, or
protected ACK adapter. The current ports are requirements for live owners, not
evidence that those owners exist.

## Focused checks

`PYTHONPATH=src:tests python3.12 -m unittest
tests.test_managed_a1_release_bridge -v` passed 16 focused Python 3.12 tests.
They cover renew-before-ACK ordering, one-shot behavior after commit response
loss, exact reconciliation, refusal before ACK on renewal failure, interval
expiry, refusal of nonterminal verifier status before any read interval,
byte-exact managed-selection binding, separate native-pin lifetime refusal at
open and after renewal, insufficient managed/source lifetime at open/renewal,
ACK/reconciliation deadline overrun, close failure after reconciliation, and revalidation of the A1 plan,
complete progress union, manifest version vector and canonical destination
receipt. These use protocol doubles and
qualify only orchestration behavior. They establish no Truss, Databricks,
Unity Catalog, retention, PostgreSQL pin, managed runtime, or ACK authority.
`git diff --check` passed. No live source, UC, compute, ACK or schema operation
was performed, and no spend was incurred.

Related pure source-boundary coverage also passed: `PYTHONPATH=src:tests
python3.12 -m unittest test_managed_current_prefix_custody_v2
test_managed_current_a1_trial_plan test_truss052_source_custody -v` (29 tests).

## Remaining live inputs

The source port needs an independently authorized read-only Truss `.52` A/B
invocation that returns the exact accepted report/view/document bytes,
registrations, both cursors and original A page, then rereads current and
retained page plus registration/ACK-scope custody. The existing genesis source
owner can gather the original A/B inputs, but it closes its read sessions and
does not supply this renewable holder or protected ACK port.

The verifier-completion port needs read-only access to independently query the
original serverless run/task terminal status and durable verifier result,
binding the returned observation to exact request, manifest, receipt and
vector bytes. The managed side still needs an admitted serverless task/runtime and
`client_dev` principal; the selected six-role managed target owner and exact
four-role vector; a native full-row observer with independent source oracle;
and a real native pin owner. The read-only workspace/Jobs inventory, task
cost-meter, and total-spend admission must establish the selected task's exact
runtime/access mode and stay under the existing `$100` total cap (aim below
`$10`) before any paid execution. No such admission is made by this change.

The next useful read-only source input is the selected live Truss `.52` A/B
owner's exact `admit_a1()` output followed by a same-identity reread of both
registrations/cursors and the retained A page, before any page mutation or
ACK. In Databricks, the next useful read-only input is a Jobs/serverless
inventory for the approved workspace showing the exact serverless environment
version, task/run-as principal, UC access mode and grants, effective
`client_dev` schema/table ownership, and an authoritative per-run cost bound
including cleanup. Until those inputs exist and Sol independently reviews this
candidate, managed A1 remains held.
