---
ddx:
  id: ashlar.evidence.truss051-r18-native-candidate-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r17-native-candidate-20261029
      kind: informed_by
    - id: ashlar.evidence.truss051-r17-pre-effect-failure-20261031
      kind: informed_by
    - id: ashlar.evidence.truss051-r16-native-candidate-20261029
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.51 R18 full-lifecycle candidate

**Status: proposed, not approved; no native action.** R18 is a fresh full-
lifecycle candidate based on the reviewed R17 workers. It changes the selected
and cold-backup input paths to the separately retained R16 copies that still
match their frozen inventories and preservation receipts. It does not reuse
R17's consumed approval, attempt root, destination ID, port, or output paths.

The [serialized R18 scope](full-lifecycle-scope-proposal-r18.json) is pinned by
SHA-256 `22b414c1793ece5e7f50277d7c0242396608e7b07274244c5f554b75c1713ed0`.
It binds prefix worker
`c826059ea2f089f9383c121aa4b73c6828a25bf3a440236b32b9f5500657be85`, migration
worker `2e90c433fb9bef53e3230a50cfbddb72cc8b497d9c29a1b3c07f2b74c593bdb7`, and
supervisor `b54e382aea78758de6b510d85eb678e93572a05167c43aee60c939f5fb34eb3d`.
The recovery-source block binds the R16 full-scope hash, R16 supervisor failure
receipt, and both retained-copy receipt files. The full-lifecycle validator and
prefix worker independently require those same pins, verify the receipt bytes
and receipt contents, and require the selected/cold input paths and tree hashes
to match. The prefix worker still inventories both complete trees against those
hashes immediately before it creates its output root.

The candidate keeps one local run, a 900-second total wall limit, a 2 GiB
process-group RSS cap, 2 GiB retained-plus-attempt cap, 512 MiB attempt-root
cap, one-second sampled supervision, zero retries, and no cloud resources.
Only a new independently reviewed approval bound to this exact scope and fresh
inputs could authorize execution. `approval_artifact_created` is false; no
approval file exists. All new attempt and supervisor output paths were absent
when this proposal was checked.

Focused effect-free checks pass: 34 R18 unit tests cover scope and worker hashes,
exact retained-R16 receipt custody, refusal on a changed receipt digest, the
900-second/2 GiB limits, mocked lifecycle behavior, and a full call to the real
scope validator with an inert, correctly shaped temporary approval. This catches
the undefined recovery-custody module reference before any native entrypoint.
Command:

```sh
PYTHONPATH=. /private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -m unittest -v test_r18_supervisor test_r18_pre_migration_custody test_r18_post_migration_admission
```

These checks did not invoke PostgreSQL, migration, Spark, Delta writes,
publication, ACK, seed, or continuation. R18 does not qualify the lifecycle,
selected Truss source authority, or production support. A future approval/run
must recheck the recovered input trees, all runtime/source pins, and immediate
pre-start PostgreSQL ownership inventory.
