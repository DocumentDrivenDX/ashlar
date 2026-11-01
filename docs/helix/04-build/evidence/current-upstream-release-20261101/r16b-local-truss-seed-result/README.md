---
ddx:
  id: ashlar.evidence.r16b-local-truss-seed-20261101
  type: evidence
  activity: build
  status: recorded
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
---

# Truss 0.0.51 local source and seed result

The R16b continuation used a stopped clone of the fresh, publicly accepted
Truss source. The original R15 source was preserved. The bounded local run
completed in 9.718 seconds with a maximum observed process-group RSS of
166,494,208 bytes. Its reviewed scope, worker and supervisor SHA-256 digests
were `29f837589d68016097a6b2f37fc06d52a72ef4dc8fbcd692bcfec341351cdf49`,
`c0202b30377d011e1597526fb6ef28acc557f43077290aefe8d890ba05effb0f`,
and `f12e40e9f4fcf270af0e0d93c566af2a27cf75dd5b4527e4777771c6e326919b`.
The worker source is retained in isolated candidate commit
`b441605d74639aa394727b20feb51a925ad69cdc` pending integration.

The public group API returned `GroupSuccess(AppliedPending)` for all seven
operations: four objects and three independent edges. The transaction then
committed. A public feed read returned one complete genesis transaction page,
32,337 bytes with Truss SHA-256
`de5c6d50f03082e5650126db86b4b50871a70f3a74f183a06a1f645a31e51760`.
These [original run receipts](.) retain the readback, typed group result,
post-commit marker, page metadata and supervisor outcome.

No source ACK, Delta publication, managed Unity Catalog run, production source
authority or scale claim follows from this result. The page bytes were not
archived by R16b; the next publisher must reread the original unacknowledged
page and verify its digest, registration and cursor against these receipts.
