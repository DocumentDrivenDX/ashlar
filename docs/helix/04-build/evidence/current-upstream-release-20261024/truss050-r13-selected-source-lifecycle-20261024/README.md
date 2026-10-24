---
ddx:
  id: ashlar.evidence.truss050-r13-selected-source-lifecycle-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss050-package-cohort-qualification-20261024
      kind: informed_by
---

# Truss 0.0.50 selected-source R13 lifecycle packet

Status: **blocked before lifecycle packet construction; focused effect-free blocker check passed; no lifecycle worker/supervisor, execution approval, or native action.** This record binds current Ashlar commit `21d4941c84c2a2c3528eb1b01dc34078f71b41b2` (tree `f9e5c1bbc4383db5b62b2bcf48840c8ce989b11b`) and the exact tracked `src/`, `sql/`, and `pyproject.toml` objects in [`source-inputs-manifest.json`](source-inputs-manifest.json), SHA-256 `5a98b08a72600549985b5253836357861b6d28ba78850066cc3757d357d8b3e2`. The manifest covers 588 files and records each Git blob, byte count, and content SHA-256.

The official Truss 0.0.50 wheel is available at the exact path used by the prior package qualification and has SHA-256 `7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00`. The immutable [package cohort receipt](../truss050-package-cohort-qualification-20261024/package-cohort-receipt.json) records wheel/RECORD and dependency resolution plus read-only wheel-path imports. That check used the frozen R12 runtime, whose installed distribution remains Truss 0.0.49; its .50 imports came directly from the wheel path. It did not create an installed .50 lifecycle runtime receipt. Reusing the .49 receipt for .50 would claim an unobserved runtime.

R12 cannot be rebound by changing identifiers. Its worker and supervisor encode Truss 0.0.49 version gates and lifecycle profiles, selected source commit `e1001bd9e95c0fa555536b2c94c892b2b5b94cdb`, source archive and bridge hashes, runtime receipt, R11 preflight, approval payload, and single-use attempt paths. Its static checker explicitly enforces those values. R12's effect-free result is historical evidence and does not establish R13 gates. A fresh .50 worker/controller, current-source bridge, runtime receipt, source archive, approval proposal and preflight need independent bindings and review; no replacements are asserted here.

The requested lifecycle sequence remains out of scope for this preparation: preserve stopped .48 PGDATA, independently re-read accepted source and consumer state, perform only the two-edge latest-only .48-to-current migration, then genesis verify/ACK followed by the approved seed continuation/ACK. This packet authorizes none of those actions. R13 limits, if later separately prepared, remain one attempt, no retries, 600-second child deadline, 1-GiB cap, local sequential execution, and no PostgreSQL/Spark/native/source effects during preflight.

A further governing-data blocker remains in the current design evidence: the real original A/B registrations, exact genesis cursor bytes, complete initial Ashlar progress, and their independently selected identities/digests are not established by the retained digest-only .48 feed registration receipt. A cursor cannot be converted into `truss_checkpoint`. The current .50 package receipt also establishes API presence only, not method behavior. These source-owner and runtime admissions must be supplied as evidence before a selected-source lifecycle can be safely prepared.

[`check_r13_blockers.py`](check_r13_blockers.py) performs one effect-free focused check: it rehashes the current source inventory against Git, verifies the official wheel digest, and confirms that R12 worker/supervisor and installed runtime remain .49-bound. It does not import Truss, start services, access source databases, or claim readiness. Result: [`blocker-check-result.json`](blocker-check-result.json).
