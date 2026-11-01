---
ddx:
  id: ashlar.evidence.r41-truss052-inplace-resume-20261101
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
    - id: CONTRACT-003
      kind: informed_by
    - id: TD-004
      kind: informed_by
---

# R41 in-place Truss 0.0.52 resume preflight

R41 corrects two demonstrated R40 blockers: Truss's accepted registration report/0.6 has no `properties` array, so the existing request matcher now receives a closed projection derived from the selected complete catalog view/0.2.0 and checks each accepted property identity and pin; and an exact prepublication page/intent pair may be reused only in the bounded local `.4` deferred-ACK continuation path. Default continuation behavior still refuses pre-existing carriers.

Read-only reconciliation of the stopped R36 source retained the matched genesis ACK and unchanged six-role Delta journal. `operations.sqlite` passes immutable `PRAGMA integrity_check`; counts remain 6 committed operations and one each of plan, source plan, and publication artifact. The owner-private WAL is empty and the owner-private SHM is 32 KiB. Neither sidecar was removed. The logical Delta digest excludes only these two SQLite coordination files and is separately recorded with the sidecar receipt.

A bounded public Truss `.52` read-only probe confirmed the retained seven-change page SHA `0248fd1607b971f267df71b1262668930a96925a767855dd3400fb0ecd1a6456`, report/0.6 shape, and complete view/0.2.0 (11 definitions, including five property references). Transactions were rolled back. No Spark, Delta write, ACK, or cloud operation occurred.

The actual R41 worker effect-free preflight and the R37 supervisor `--preflight-only` both passed. They bind the stopped R36 PGDATA tree, existing Delta journal/targets, exact R40 page and intent, matched genesis ACK, current source-code hashes, and private SQLite sidecars. Both fresh R41 output and supervisor roots remain absent. Preflight did not open PostgreSQL, start Spark, write Delta, attempt ACK, or access cloud. Scope remains pending independent review; no native dispatch was performed.

The machine-readable receipts are [R40 read-only reconciliation](r40-read-only-reconciliation.json), [R41 public-view probe](r41-public-view-probe.json), [R41 SQLite sidecar reconciliation](r41-sqlite-sidecar-reconciliation.json), and [R41 effect-free preflight](r41-effect-free-preflight-receipt.json). The scope, worker, and supervisor hashes are bound in the R41 scope and preflight receipt. This is local-development evidence only; R34/R35/R36 recovery artifacts are preserved and no production source authority is claimed.
