---
ddx:
  id: ashlar.evidence.r42-inplace-resume-preflight-20261101
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

# R42 stopped-source preflight

R41 stopped before its custody callback, page-two Delta write, or ACK. A fresh read-only Truss `.52` page observation matched the retained canonical transaction archive in page digest, consumer, registration, writer XID, prior/next cursors, outcome, and profile. Only the snapshot safe frontier advanced, from `806` to `818`. The exact retained page and original intent remain authoritative; reuse is allowed only for this canonical transaction identity with a monotone safe frontier. Focused tests cover that path, regressions, changed registration, and a tampered intent. The default carrier path remains fail-closed.

The stopped R36 source baseline had no SQLite WAL or SHM sidecars. During the first R42 effect-free journal admission, SQLite `mode=ro` created a zero-byte WAL and a private 32 KiB SHM file. This did not change the main journal (`9a4f24e4…a476030`), its integrity/counts, or the logical Delta tree (`771b3431…21fc08`). Neither coordination file was removed. The revised R42 scope records both the original absent baseline and current exact sidecar state and digests; the worker rejects any nonempty WAL, foreign/nonprivate sidecar, or digest drift.

The actual R42 worker and supervisor effect-free preflights passed. The source checkpoint remains the matched genesis ACK (`8e370e14…9841f9f`), the retained page is still the seven-change entity page `0248fd16…a6456`, and the existing six-role journal remains at six committed operations, one plan, one source plan, and one publication artifact. No output or supervisor root was created; PostgreSQL was not opened, Spark was not started, and no Delta write or ACK was attempted.

Machine-readable evidence: [safe-frontier comparison](r41-prepublication-carrier-reconciliation.json), [SQLite sidecar reconciliation](r42-sqlite-sidecar-reconciliation.json), and [effect-free worker/supervisor preflight](r42-effect-free-preflight.json). The R42 scope remains pending review; no native dispatch was performed.
