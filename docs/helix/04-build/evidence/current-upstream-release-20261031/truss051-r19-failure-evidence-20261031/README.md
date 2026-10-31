---
ddx:
  id: ashlar.evidence.truss051-r19-pre-effect-failure-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r19-runtime-cohort-20261031
      kind: informed_by
    - id: ashlar.evidence.umf010-upstream-adoption-20261031
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.51 R19 pre-effect failure evidence

R19 exited during child import admission after 3.151 seconds. The supervisor
receipt reports `ModuleNotFoundError: No module named
'ashlar_host.local_current_delta4_umf_sdk010'`, return code 2, `native_effects:
none`, `effects_may_remain: false`, zero final attempt-root bytes, and no
persisted failure receipt. The supervisor's temporary attempt root was removed
after the run; the original supervisor receipt and child logs are retained here
byte-for-byte.

The failed scope SHA-256 is
`bfe4f9623efc3abe130b05f9f6188e95b681b3d446c44ce418d4591898c59c62`; its pinned
migration worker SHA-256 is
`385a761893f533c5efb952b05393edd2431d4063c235e7b0ce4f2d47c910a4ac`. The
failure occurred before PostgreSQL or Delta effects. The exact one-run R19
approval was consumed; its retained artifact has SHA-256
`84ace8e2cb2601a34a06c78cafc7e30c3fbf5ec6420a83b1d4969d1e69eb48f6`.
The retained supervisor
receipt SHA-256 is
`2a9167e83b1e461eb00250a05d910687cb2aa4beb386c35405b6e4da1012c9f8`;
child stdout SHA-256 is the empty-file digest
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`, and child
stderr SHA-256 is
`88087d1f47d6659831270d18a46cbd975347ac053c4b84945423e9d610789c74`.

The immediate cause was the isolated child using `python -I -B`, which ignores
`PYTHONPATH`. The frozen R16 source verifier also inserted the older Ashlar
source tree at the front of `sys.path`. R20 addresses both conditions with a
hash-checked clean source snapshot, an isolated preflight from an unrelated
working directory, and exact restoration of the prior import path after the
R16 source check. See the fresh [R20 candidate](../truss051-r20-runtime-cohort/README.md).

The retained raw files are `supervisor-receipt.json`, `child.stdout.log`, and
`child.stderr.log`.
