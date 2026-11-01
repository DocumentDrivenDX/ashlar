---
ddx:
  id: ashlar.evidence.r41-carrier-reconciliation-20261101
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

# R41 prepublication carrier reconciliation

R41 reached the retained seven-change public page and stopped before the native custody callback or Delta publication. The failure was an exact-byte comparison against the existing R40 page archive. A bounded read-only public Truss `.52` reread confirmed the page SHA and every archive field except `safe_watermark_xid`: the R40 archive records `806`, while the current observation records `818`. Writer XID remains `768`; content, prior/next cursor, feed, consumer, registration, outcome, and page SHA all match. Truss `read_feed` sets its safe watermark to the current snapshot frontier, so that field can advance while the immutable transaction page remains the same.

The retained R40 page archive is `fe63c1affe12bf60a15fdeb505aee09b925df74d0c40646aa4b9835970cf6`; its existing intent `0ac8a41387eba0275695aff0e52628ff525be5a7e0a3befd023c8bdfb5aec34a` binds that exact archive. Re-encoding the fresh observation yields `ca80c742e78d4fbf76b14dfd66b07b3a76c912f11273efa4239a5a3f78d24ab3` and would require a different intent hash. The correction therefore validates the existing canonical archive against the current transaction identity, permits only a canonical monotone safe-frontier advance for transaction pages, validates that the existing intent binds the retained bytes, and returns those retained bytes to downstream result and recovery receipts. The normal path remains exact and fail-closed.

After R41 stopped, an immutable journal check still reports integrity `ok`, six operations and one each of plan, source plan, and publication artifact; no page-two Delta publication or ACK exists. PostgreSQL is stopped. The R41 attempt did start Spark, but did not write Delta or attempt ACK. The separate comparison used only a public read-only Truss page read and started no Spark. See the machine-readable [carrier comparison](r41-prepublication-carrier-reconciliation.json) and [post-attempt state](r41-postattempt-state.json). The correction is covered by focused unit tests; native dispatch remains out of scope for this evidence.
