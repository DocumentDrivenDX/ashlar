---
ddx:
  id: ashlar.evidence.truss050-r15-selected-source-lifecycle-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss050-r14-selected-source-lifecycle-20261024
      kind: informed_by
    - id: ashlar.evidence.truss050-r13-selected-source-lifecycle-20261024
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.50 selected-source R15 lifecycle packet

R15 refreshes the packet against current Ashlar main and the fixes made after R14's archived source snapshot. Its worker and final supervisor preflights passed, yielding `ready_pending_hash_bound_operator_approval`. One exact hash-bound, one-shot local execution was later approved and started. It stopped at the initial `.48` read-only custody gate when the helper received an empty JSON view; no migration, publication, ACK, seed, or continuation stage ran. The approval is consumed, so it authorizes no retry or replacement attempt. The [execution result](execution-result.json) records the attempt, retained evidence, stopped PostgreSQL state, preserved cold backup, and exact stage boundary.

The selected source is Ashlar commit `2eeecf409033c6c7c92ded72ec7e43c569dffaf9`, also observed at `origin/main` when prepared; tree `a97e9e1e275294e840690bbe792e795afbeffff7`. The exact runtime inputs are Git objects `src=02e10442a973712e49a357a6c2847236c9f4792b`, `sql=3cfe250fadd42ef4b25597f0eec7d10a426ce8ff`, and `pyproject.toml=7af01eeb58eaf0dbd7ae7d0246618e97a96ca724`. The 590-file source manifest SHA-256 is `eb1ae0da17735561b51f77c393e78d1cf60910cf7124c9bc791a111d67b99376`; archive SHA-256 is `f60d7c8388e4095bcfe8995dd9ea9578d6655a290578d3a4ea48d0835c512aa7`.

The effect-free worker checked the exact selected source, current `.4-xxhash64` bridge inputs, six UMF role proposals, `.48` read-only helper, `.48 → .50` declared migration route, and pinned runtime. The runtime receipt binds Truss 0.0.50, UMF core 0.10.1/access dev7, Weft 0.4.53, PySpark 4.2.0 and Delta 4.4.1. The bridge and package receipts are evidence of local preflight only; they do not qualify native migration or publication.

The first supervisor capture under the sandbox is preserved as `r15-sandbox-supervisor-preflight.json`; the platform refused the supervisor's temporary SysV `IPC_RMID` probe cleanup. Its exact one-byte segment was confirmed unattached and created by that probe, then removed by ID. `r15-sysv-cleanup-receipt.json` records the observed segment and cleanup. A single subsequent allowed-context supervisor preflight passed process inventory, SysV create/attach/detach/removal, and exact worker/supervisor approval-payload parity. The final capture SHA-256 is `85c47811b28dd38f8e9f3ce45bac4cc38c1fcab46fb354c86510357b536e61a5`; approval payload SHA-256 is `239bc4e2a500aeb5804ba071da0daf3cada9b84a1a5a1151e7cb6684f730304a`. The earlier sandbox failure remains part of the evidence and was not overwritten.

The consumed attempt used the R14 bounded lifecycle limits: one attempt, zero automatic retries, 600-second child deadline, 1-GiB combined retained/attempt cap, 16-MiB logs, and sequential local execution. It stopped before migration apply because the `.48` custody helper attempted to parse an empty JSON view. The exact cold backup was verified before PostgreSQL start and remains preserved. PostgreSQL is stopped; eight PostgreSQL-written files differ from the backup, as recorded in the result. Do not restore or restart this retained state under the spent approval. No `.50` migration, source/feed publication, ACK, seed submission, continuation, or release qualification resulted.

Run `python3 check_r15_packet.py` for the focused static integrity check. No broad suite or native service was used for this packet.
