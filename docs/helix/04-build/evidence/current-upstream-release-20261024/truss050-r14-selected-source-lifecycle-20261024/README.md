---
ddx:
  id: ashlar.evidence.truss050-r14-selected-source-lifecycle-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss050-r13-selected-source-lifecycle-20261024
      kind: informed_by
    - id: ashlar.evidence.truss050-package-cohort-qualification-20261024
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.50 selected-source R14 lifecycle packet

Status: **worker preflight passed; supervisor preflight remains pending execution admission after the sandbox refused cleanup of its temporary SysV probe. No execution approval, start marker, PostgreSQL or Spark start, migration, source/feed mutation, publication, ACK, or seed submission was created.** The bounded worker and supervisor are review candidates, not authorization to execute.

The selected source-bearing snapshot is Ashlar commit `c33bef013d737b8a56f43faafe5f5c1966065d1f`, tree `8bfa46c16683bebff79124f1a35d668ab7a3c18a`, with exact Git objects `src=7c6b4f14f4d55e539371aa9e61fa5c4752bc2c93`, `sql=3cfe250fadd42ef4b25597f0eec7d10a426ce8ff`, and `pyproject.toml=7af01eeb58eaf0dbd7ae7d0246618e97a96ca724`. Main was observed at `b926ae84dc67c1835ab91a8b043b1606d55fc6fb` during R14 preparation. The intervening main changes are documentation/evidence only; a Git diff confirmed no changes to `src/`, `sql/`, or `pyproject.toml`. R14 binds the source-bearing c33 snapshot and does not label it as the latest main HEAD.

The exact source archive is `cd411af8de8d47bd8ae3c347244a6df2c60e69233722963e3cdc916aa0d47c2c`, and its 589-file manifest is `0f3c5a2ebe4e9a6262c53d7a5c1f160f56067c1f6243d0af27b31a81ddbc466f`. The installed Truss 0.0.50 wheel is pinned by `7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00`. The reused immutable APFS runtime receipt is `fe48796cead974bc06821bd296428f47b389e1016a7b43ff186ec15cb03f72e5`: 27 wheels, 28 distributions, 4,445 verified RECORD payloads, zero RECORD errors, passing `pip check`, and 753,451,008 allocated bytes against the 1-GiB cap. R14 does not modify the R12 runtime or historical R13 packet.

The worker preflight verified the installed `.5` admission profile against the exact selected source, the six-role UMF proposal set, the retained `.48` import-only helper, and the declared latest-only route `.48 manifest c433 → intermediate 35fd → .50 native manifest 2888`. It reported `ready_pending_hash_bound_operator_approval`; no PostgreSQL, Spark, migration, source, publication, ACK, or seed action occurred. The route is metadata-only evidence and does not claim a native migration test.

The supervisor’s read-only process inventory passed using macOS libproc. Its following temporary SysV shared-memory cleanup gate received `EPERM` from `shmctl(IPC_RMID)`, so supervisor preflight reports `pending_execution_admission` and does not establish approval-payload parity. Sol later performed an escalated cleanup-only inventory: it found the two one-byte segments created during those probes (IDs `1638476` and `1704014`, both with zero attachments) and removed those exact IDs; escalated inventory confirmed both absent. This cleanup does not retroactively change the captured supervisor result. No additional sandbox probe was run. See [`r14-preflight-result.json`](r14-preflight-result.json) for exact worker/supervisor result hashes and cleanup evidence.

Sol review identified and corrected a duplicate call to the admission gate in the supervisor's execute branch. The current candidate now reuses the single gate observation and digest already bound by approval parity, avoiding a second SysV probe before the start marker. Its SHA-256 is `179820043ef6224827cb16a2c59b7b835d8f9c8a6445ff9b3f1dc7c1bcaba336`. The captured supervisor preflight and its recorded supervisor SHA are from the prior candidate revision; this one-probe correction was syntax and packet checked but its supervisor preflight was not rerun. An independently reviewed fresh supervisor preflight must pass before any execution approval can be considered.

If separately reviewed and authorized later, the candidate lifecycle remains one attempt with no retries, a 600-second child deadline, 1-GiB combined retained/attempt cap, 16-MiB log cap, and sequential local execution. Before migration the worker reobserves accepted A source custody and the pending consumer checkpoint through the existing `.48` read-only gate. It then applies the exact two-edge latest-only migration, verifies the source and full view under `.50`, publishes and independently verifies/ACKs the genesis page, and only after that ACK submits the already approved seven-operation seed once and verifies/ACKs its exact actual continuation. Consumer B is outside this single-source packet. These remain proposed execution steps; this packet does not create owner authorization.

The preserved R13 packet remains the historical effect-free blocker record. R14 does not reinterpret R13 as `.50` lifecycle qualification. The local `.4-xxhash64` carrier uses its pinned physical `/0.3` binding; `/0.4` is the legacy carrier path, as stated in the checked bridge receipt.

Run [`check_r14_packet.py`](check_r14_packet.py) for the focused static integrity check. No broad test suite, PostgreSQL, Spark, migration, native realization, or source effect is part of this packet.
