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

Status: **effect-free preparation and preflight completed; lifecycle blocked before native execution by the selected Ashlar source's missing .50 admission profile. No execution approval or native/source action was created.** The selected source snapshot was the exact Ashlar commit `9c74527875535417676213f0e31a12605da8fc43` (tree `03c4aa8dfb8cd69d9b1a6e241d579af1ba899ff2`), the latest main commit at packet freeze. R13 is an evidence-only commit based on that snapshot. `source-inputs-manifest.json` hashes the 588 selected `src/`, `sql/`, and `pyproject.toml` files; the immutable archive is bound by SHA-256 `fee624831fc09c49e509577101d2be183313778224a2bc008d510a2112b8b052`.

The official Truss 0.0.50 wheel SHA-256 is `7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00`. A fresh APFS copy-on-write runtime was made from the immutable R12 runtime, then only Truss was force-reinstalled from this local wheel with `--no-index --no-deps`. [`runtime-receipt.json`](runtime-receipt.json) records the installed .50 distribution, all 27 wheel origins and hashes, the 28-distribution cohort, 4,445 RECORD payload checks, passing `pip check`, and measured retained-root sizes within the 1-GiB cap. The original .49 runtime and historical R12 packet were left unchanged.

The R13 supervisor exposes only `--preflight`; the worker has no execution mode. The one focused preflight verified exact source object/archive bindings, the isolated .50 cohort, the retained .48 helper's import-only compatibility gate, and pure UMF proposal generation for all six current managed Delta roles. It started neither PostgreSQL nor Spark and performed no DDL, migration, source read, publication, or ACK. [`r13-preflight-result.json`](r13-preflight-result.json) records the exact outcome. The old .48 helper's actual A custody and pending-page reobservation remains deferred to its existing read-only gate immediately before migration in any later separately approved attempt.

Preflight is blocked because the selected source's `src/ashlar_host/truss_current_catalog.py` pins `.49` as the newest admitted profile and its `installed_current_truss_profile()` raises `ValueError: Supported current Truss runtime profile required` when the isolated `.50` package is installed. The preflight records the installed version and manifest hash, source literal, and exception. A live A source/checkpoint re-read cannot repair this package admission allowlist. Do not change source code or bypass this guard within R13; Ashlar's .50 compatibility must first be implemented and separately qualified against the source contract.

The proposed later lifecycle remains single-source A only: preserve stopped .48 PGDATA; run the existing .48 read-only A custody and pending-checkpoint gate; if it passes, perform exactly the latest-only two-edge .48-to-.50 migration; verify genesis publication and ACK; only after that ACK and a separate explicit approval, continue the seven-operation seed and verify/ACK its actual continuation. One attempt, no retries, 600-second child deadline, 1-GiB retained/attempt cap, 16-MiB log cap, and sequential local native execution remain the intended limits. This packet did not authorize or create a start marker for that work. Feed/consumer B is outside the requested A lifecycle.

Run [`check_r13_packet.py`](check_r13_packet.py) for the focused static packet-integrity check. The historical `.49` R12 packet remains intact and is not rewritten or treated as .50 evidence.
