---
ddx:
  id: ashlar.evidence.r40-inplace-resume-preflight-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# R40 in-place resume preflight

R40 prepares an in-place continuation from R36’s matched genesis ACK. It reuses
the existing stopped PG18 source and six-role Delta journal; it does not replay
genesis, re-enroll the consumer, or replay the seed group. R39’s failure and
public page receipts remain intact.

The worker binds the post-R39 stopped PGDATA tree at 1,261 files / 65,208,181
bytes, SHA-256 `06165099b313b5470120140f9e8efbe6a670a7a9c3045ec57016148badb550ce`.
The four graph-role heads remain at genesis vector v1, matching the R36 native
readback with empty bags. Manifest v2 and the journal still contain only the
single genesis publication. The exact next page is bound to R39’s retained
public page SHA-256 `0248fd1607b971f267df71b1262668930a96925a767855dd3400fb0ecd1a6456`,
its genesis-ACK prior cursor, next cursor, and explicit `quantity` null proof.
The current `truss_graph_admission.py` source hash is in the worker’s closed
source-binding map at SHA-256 `406434aec69f0283c56d5f70a589803e8c23ae24a2618517e4cb7026255937ad`.

The native-entry sentinel reached `LocalPostgres.__enter__` and stopped before
startup; PostgreSQL, Spark, Delta and ACK effects were absent, and its test roots
were removed. The actual R37 supervisor then ran the R40 worker’s effect-free
preflight successfully. It verified retained R36 receipts, the exact current
PGDATA/Delta/journal baselines, the R39 page/model evidence, the public resume
configuration parser, source bindings and runtime imports. The R40 attempt and
supervisor roots remained absent. Scope status is `pending_sol_review`; there
is no dispatch receipt and no native launch.

- Scope SHA-256: `651fb7787e2d66d178e21ae92f7aaf1ab5560dcac5bc26df12c97d3bee2d7d40`.
- Worker SHA-256: `a4b27c5271ad6bb0cab3b9b8378c68fa63b944e4d7b4f81ab70fda1b8bbc3df3`.
- Supervisor SHA-256: `dbd2ac1563885eead43d6241c2d82013372ca83927e670705372bdcac8b2fece`.
- Effect-free supervisor receipt SHA-256: `450a52f9c5e0a46449d0a15ff5f403b6dda715d22396878c39e30fe80ac1f450`.
