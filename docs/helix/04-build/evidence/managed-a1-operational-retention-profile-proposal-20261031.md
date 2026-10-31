---
ddx:
  id: ashlar.evidence.managed-a1-operational-retention-profile-proposal-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: informed_by
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-retention-provider-assessment-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-readonly-refresh-20261031
      kind: informed_by
---

# Managed A1 operational retention profile proposal — 2026-10-31

**Disposition: owner selected the bounded operational profile; implementation remains blocked on native host and source-custody ports.** A small finite A1 run can use the original Unity Catalog (UC) managed Delta tables while predictive optimization (PO) remains enabled, provided the contract describes a time-bounded operational availability procedure and does not claim a retention lease. The profile cannot guarantee availability against a workspace/metastore administrator, Databricks service action outside the observed policy, storage administrator, or out-of-band file deletion. If the owner requires protection from those actors, this route does not satisfy the requirement and protected ACK stays unavailable.

No cloud mutation, compute start, schema creation, source operation, or ACK is authorized by this proposal. It defines the change to review before any implementation or launch.

## Proposed profile and contract delta

Add a separately versioned boundary profile, `ashlar-managed-operational-retention/0.1`, referenced by the exact verifier receipt and source-holder ACK evidence. It qualifies one complete four-role original publication vector for one source slot and one ACK interval. It is not a table-layout version, a resolver-selected read policy, a native provider lease, or a guarantee of file retention. Keep the existing `ashlar-delta/0.4-xxhash64` physical schema and all four `(three-part name, native UUID, literal version)` identities unchanged.

The profile binds these immutable inputs: original request bytes and digest; accepted source-prefix bytes and separately selected digest; original manifest bytes and digest; complete four-role vector; source-derived expected-bag digest and bounds; verifier run/task/runtime/principal identity; and exact UTC opening time. Digests establish correspondence to selected bytes, not their authority. The source owner selects and retains those bytes independently of the verifier.

Amend CONTRACT-001 with a profile-specific custody sequence and outcome. The original external source holder retains the request, accepted source prefix, original destination receipt, manifest and full vector before publication. It receives the verifier's bounded observation only after the verifier task closes. The holder independently checks the observation's receipt/vector/runtime anchors against its retained originals, then opens the read interval. The holder performs native `VERSION AS OF` reads for all four exact pins, compares complete bounded row bags to its independently rebuilt source-derived oracle, and records the resulting read-witness digest and exact vector. Immediately before protected ACK, it repeats the four exact-version reads and source-custody rereads; it submits the original exact destination receipt once, reconciles from a fresh source transaction, and closes every acquired handle. The holder stores the exact ACK request/response or unknown-outcome evidence with original recovery custody. Publisher and verifier tasks receive no ACK credentials or holder handle.

A holder may report `acknowledged` only after the exact ACK is reconciled and closing checks pass. Any failed/missing read, vector drift, changed source checkpoint/page, expired 15-minute interval, unknown ACK outcome, or close failure returns a non-success state and retains recovery custody; never retry an uncertain ACK. A successful read witness says the versions were readable at the observation times. It does not say files were reserved or will remain readable between observations. A disappearance after the final read and before/during ACK therefore remains an explicit residual failure mode; in that case the source owner must reconcile the ACK outcome before any retry or later slot.

Amend CONTRACT-003 to define the profile's read-witness payload and limits, without changing physical table DDL: exact four-role vector; native UUID/version before and after each role read; complete ordered carrier fields; `VERSION AS OF` literal reads; exact row-bag comparison; original runtime/time-zone identity; open, pre-ACK and close timestamps; selected retention settings; PO effective setting; principal and maintenance-identity inventory; result digest; and any refusal reason. Require `delta.deletedFileRetentionDuration >= 7 days` and `delta.logRetentionDuration >= 7 days` on each role, plus fresh exact-version reads at open, pre-ACK and close. Record settings and reads as observations; neither is a file hold. Keep PO effectively `ENABLE` at all three checks. No config/property write may attempt to disable PO or claim the seven-day setting prevents privileged override.

Amend CONTRACT-004 so a profile-aware verifier can authorize only the exact four-role vector and selected profile. Resolver metadata inspection alone is insufficient: profile success requires complete native row reads at the pinned versions and exact comparison. Do not make ordinary consumer reads depend on this pilot profile, and do not treat the verifier receipt as a reusable lease or resolver authorization token.

Amend TD-009 with an `OperationalRetentionInterval` host owner wrapping the existing bounded native reader and external source-holder port. `open()` validates frozen inputs and performs the first four-role compare; `renew()` repeats full compare and source custody checks using a fresh session; `close()` performs the final checks, closes the session, and preserves the primary failure. For this 15-minute profile, check at opening, immediately before ACK, and at close; reject a wall-clock interval above 15 minutes. The source holder owns its source pin/registration evidence and exact ACK bytes. This host handle is process-local evidence custody with active checks, not a filesystem lock or service lease.

## Threat model and honest claim

The profile covers accidental expiry, stale publication vectors, visible table replacement/UUID changes, unavailable historical versions at each check, changes to selected retention settings, visible maintenance privilege/identity changes, source checkpoint/page drift, verifier/runtime substitution, and interval timeout. It relies on the operator and admitted application principals not intentionally performing destructive maintenance during the 15-minute window; it keeps ordinary platform PO active and treats it as an observed maintenance actor.

It explicitly excludes a malicious or mistaken metastore/workspace administrator, cloud/storage administrator, undocumented service-side deletion, and out-of-band file removal between checks. Enumerating grants and checking `VACUUM` privileges is useful operational evidence but cannot establish absence of every destructive actor or fence future grants. The receipt must use `observed_available_at_checks` or equivalent language. It must not use `retained`, `held`, `protected`, `guaranteed`, `lease`, or imply that PO's normal seven-day threshold is an immutable promise. The source owner accepts the remaining interval risk; all stronger producer/consumer contracts remain unchanged unless separately versioned.

## Smallest practical A1 run

Use the owner-designated disposable schema under `client_dev`, exactly four tiny managed Delta roles and one source slot. Preserve PO `ENABLE`. Use the current all-purpose candidate from TD-009: one separately owned D4ds_v5 driver with zero workers in single-node mode under the observed Personal Compute policy, with publisher and verifier submitted as sequential Jobs tasks attached to the dedicated cluster. Use distinct processes and Spark sessions, concurrency one and zero retries. The policy fixes `num_workers=0`, but the API create response and effective cluster configuration must still establish single-node Spark execution, task attachment, runtime and UC mode eligibility; this is a candidate route only. Limit the measured holder interval to 15 minutes and use four-role reads only at opening, pre-ACK renewal and close. Keep the previously recorded total ceiling below $100 and aim below $10, counting prior attributable spend, startup/idle, Jobs DBU/VM, storage, PO activity, ACK wait, termination and cleanup. Before start, read back exact runtime/access mode, UC authority, Jobs entitlement and rate/cost envelope; stop if any bound is missing. The prior read-only `client_dev` observation establishes PO `ENABLE` and schema authority only; it does not establish Jobs execution or total cost.

Do not run retention-setting writes as part of the pilot unless the owner separately selects them and the exact prior/new values and rollback are recorded. The admission check can refuse if existing properties are below the profile minimum. No source mutation or ACK occurs until the owner adopts this profile, real source/ACK custody is independently available, and the dedicated Jobs route and total cost are admitted.

## Owner decision recorded

The publication owner selected **Accept the bounded operational claim** for the managed `client_dev` trial. This accepts a 15-minute maximum with exact reads and source checks at open, pre-ACK and close, including the stated administrator/storage exclusions. The decision applies only to `ashlar-managed-operational-retention/0.1`; it does not weaken any stronger profile. CONTRACT-001/003/004 and TD-009 now carry the proposed versioned amendments.

The selection resolves the semantic question, but does not itself supply live authority. Implementation and launch still require the bounded native read/policy observer and independently authenticated source-custody/ACK owner described in TD-009, along with exact Jobs/runtime/access and total-cost admission. Focused fake-port tests can prove ordering and refusal only; they cannot qualify excluded threat actors or convert observations into a guarantee.

## Governing evidence

The provider assessment found no admitted native retention provider and explains why UC retention properties, PO, grants and successful reads do not establish an interval hold. The Jobs admission packet and 2026-10-31 read-only refresh show a plausible dedicated Jobs route and `client_dev` with PO `ENABLE`, but no Jobs execution, exact runtime/access mode, source holder, or total spend admission. This proposal resolves the retention question by making the owner-selected bounded operational claim explicit; the other gates remain independently required.
