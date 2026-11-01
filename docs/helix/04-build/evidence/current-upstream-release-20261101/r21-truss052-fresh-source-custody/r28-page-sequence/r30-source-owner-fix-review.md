---
ddx:
  id: ashlar.evidence.r30-truss052-source-owner-fix-review-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: informed_by
    - id: TD-004
      kind: informed_by
    - id: ashlar.evidence.r29-truss052-native-attempt-reconciliation-20261101
      kind: informed_by
    - id: ashlar.evidence.r21-truss052-fresh-source-custody-20261101
      kind: informed_by
---

# R30 fresh-root Truss source-owner correction candidate

The R29 failure was a deterministic Ashlar profile-gate omission, not a Truss
source or ACK refusal. Commit `31370a041190eb0aa607d7731f5669ad77733973` adds the
published Truss 0.0.52 local-candidate profile to `NativeDriver`'s Truss owner,
typed-admission, publication-intent, prepare, and observation profile sets. It
also describes the .52 wheel as published while continuing to qualify native
feed, publication, ACK, and production authority separately. Focused tests cover
required original `TrussFeedSessions`/ACK ownership, refusal of a fake typed
admission, and the prepared apply/observation path.

The candidate keeps the R24 accepted Truss source as its immutable source input
and creates no source mutation before each public preflight. It does not replay
acceptance, consumer enrollment, or the already committed seven-result genesis.
R29's selected public page is a zero-change prerequisite page with digest
`942a325702683382c99bd92c69a1af6478cf04879fee0f9d8db4cd4e80f8a9a0` and prior
cursor `9afcb17ae6f64d065c0bc11f25c2f111c43b95ade0bd20a354a568001a91ad4a`;
R29 receipts confirm no ACK attempt. The R30 scope requires a fresh output root
and Delta destination. R29's six-role Delta root is retained only as failed
attempt evidence and is explicitly marked non-reusable.

`r30-host-scope.json` binds the new attempt identifier, exact R24 source tree,
published Truss 0.0.52 wheel/runtime, loopback Spark candidate, source and model
identities, and exact implementation file digests. The same reviewed R28 worker
and supervisor are hash-bound; their profile denotes the worker/supervisor
contract revision, while the scope names the new R30 attempt. The scope remains
`standing_local_native_authorization_pending_sol_review`. This packet has not
been dispatched.

The actual supervisor `--preflight-only` invocation passed its structural
worker admission in 1.9 seconds. It verified the source/model/seed/continuation
inputs, imports, role names, runtime and artifact hashes, Spark/JAR setup, and
fresh paths without opening PostgreSQL, starting Spark, writing Delta, attempting
source ACK, or contacting cloud services. The captured result is
[`r30-effectfree-preflight-receipt.json`](r30-effectfree-preflight-receipt.json);
its scope SHA-256 is `f46a22ee4127e36ee150758314b0a0a6f415d03489341e5b872893fdd7558321`.
The R29 forensic record remains under
[`r29-attempt-evidence/`](r29-attempt-evidence/).
