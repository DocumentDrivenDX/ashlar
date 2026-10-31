---
ddx:
  id: ashlar.evidence.g03-sdk010-source-admission-gap-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk058-txn1-timeout-r3-20261030
      kind: informed_by
    - id: ashlar.evidence.sdk010-delta4-runtime-integration-20261031
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# G03 SDK 0.10 source-admission gap — 2026-10-31

The consumed r3 attempt reached its 450-second deadline after six-role
initialization, source admission, and durable local `prepared`/`applying`
reservations, but before any metadata operation or graph SQL. Its journal has
no `local_operation` row, and every Delta role remains at initialization v1
with metadata-only commits. Spark stderr records startup and completed stages
0 and 2, with no application traceback. The SDK checker created its temporary
`model.json` and `request.json` at 12:48:26 local time, about ten seconds before
the deadline. That locates an in-flight checker invocation near the end; the
receipt has no per-step timers or counters, so it cannot attribute the prior
~440 seconds to that invocation or to another admission step.

The effect-free source-admission receipt measures three full SDK 0.5.8 checker
runs at 2.474–2.871 seconds each, exact SDK file custody at 0.017–0.022 seconds,
and pure Python policy/receipt reconstruction at 0.0021–0.0025 seconds. Those
measurements do not reproduce r3, but they provide no evidence that custody
byte checks explain the timeout. The already-reviewed renewal correction
keeps the full checker at source open and final close; inner renewals preserve
the retained bytes/identities and reconstruct the request-bound policy without
rerunning Node/Core correspondence. Do not weaken custody or opening/closing
admission on the basis of r3's incomplete timeline.

The selected Gate 1 SDK 0.10.0 route is not wired into configured-local source
admission. `ConfiguredLocalRuntimeConfig` accepts only optional
`sdk058_admission`; that route pins SDK 0.5.8 (semantic Core 0.10.0) and its
source producer remains historical revision `2ede4c...`. The separately
versioned `local_current_delta4_umf_sdk010.py` admits the SDK 0.10.0 six-role
DDL proposal bridge only; it does not provide source admission. The `sdk010`
profile branch in the runtime creator recognizes an installation-profile
shape, not an SDK 0.10.0 source owner. Consequently the r3 timeout is historical
SDK 0.5.8 evidence and cannot qualify the current SDK 0.10.0 source/native
consumer path.

The smallest current-cohort implementation slice is a separately versioned,
opt-in SDK 0.10.0 typed-graph source-admission owner, with exact selected SDK
source/archive/package pins and request/source custody. It should retain the
full public correspondence result at opening, use only deterministic retained
receipt/source/request checks during renewals, and rerun full correspondence
plus exact custody comparison at close. Keep `sdk058_admission` behavior and
historical receipts unchanged. Its focused effect-free gate should cover the
selected public API/source pins, exact source and request receipt, open–renew–
close call counts, changed-source/custody refusal, close failure withholding,
and `ConfiguredLocalRuntimeConfig` opt-in selection with no Spark construction.
Only after that gate passes should a separately reviewed native packet bind
the current source owner, publication and Weft 0.4.56 consumer.

This note records a wiring gap and a bounded candidate boundary. It makes no
SDK 0.10.0 source-validation claim, does not explain r3's timeout, and authorizes
no native run, retry, source ACK or publication.
