---
ddx:
  id: ashlar.release-closure-20261101
  type: implementation-plan
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: informed_by
    - id: ashlar.toolkit-delivery-plan
      kind: refines
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: ashlar.evidence.g03-weft-native-consumer-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-production-hardware-envelope-20261101
      kind: informed_by
---

# Ashlar release closure — 2026-11-01

The release target is a tagged, published GitHub artifact from reviewed
`origin/main`, with a production-target trial and claims limited to its exact
runtime, model, source and consumer subset. Local verification should answer
specific implementation questions; repeat neither CI matrices nor network/file
throughput demonstrations. The local GitHub publisher is prepared because the
Actions quota is exhausted. No stable tag or release exists yet.

| Gate | Current evidence | Required closure |
| --- | --- | --- |
| Upstream cohort | Selected UMF SDK 0.10.1, Python Core 0.10.5 and Weft 0.4.56 are pinned. Truss 0.0.52 is published; Ashlar's `truss-local` extra pins the public wheel by exact URL and SHA-256 `00d8b11abc83d9c84fa131ae6f3eef23ecad9d321d4b04a13641ed31c77e9747`. The SDK 0.10.1 `.4-xxhash64` six-role proposal matches the versioned bridge. | Recheck selected production runtime assets before the stable Ashlar build. |
| Local publication and native reads | SDK 0.10.1 transaction one committed three graph operations and two metadata operations; a manifest-only continuation completed with no replay. One Spark 4.2.0 / Delta 4.4.1 local read matched the singleton, two Weft grouped queries, page/lookahead and continuation against the frozen source oracle. All six table heads matched at open and close; cleanup succeeded ([manifest completion](evidence/current-upstream-release-20261101/g03-sdk0101-r3-manifest-continuation/r5-completion.md), [Weft result](evidence/current-upstream-release-20261101/g03-weft-native-consumer/README.md)). | Preserve these narrow local results. Do not rerun them unless code or binding changes. Linux/managed execution and Truss source/ACK are separate gates. |
| Truss source lifecycle | A fresh local source was accepted through public Truss APIs and migrated to 0.0.51. R16b committed seven seed operations and read genesis without ACK. R20 then committed that exact schema-only page to four local Delta v1 tables; independent reconciliation confirmed it remains unacknowledged. Its supervisor exited nonzero after detecting and terminating one lingering process. R20 did not preselect source-holder registration or ACK-scope custody, so its committed page cannot be retroactively admitted for protected ACK ([reconciliation](evidence/current-upstream-release-20261101/r20-truss051-registrar-catalog/r20-publication-outcome-reconciliation.md)). | Preserve R20 as unacknowledged local evidence. Create a fresh disposable Truss source with 0.0.52 source/destination/ACK selection before publication, verify and ACK its genesis, then publish its gated continuation without replay. |
| Production source and pin custody | Truss 0.0.52's native source fence and `.74→.75` migration passed focused local checks on the retained source clone. Ashlar's managed publication-read/native-pin adapter and source-owned selection builder are on main; the latter requires public Truss runtime status and exact source registration/scope evidence. The combined source/destination/ACK flow has not run. | Compose the 0.0.52 source fence with independently authorized destination/ACK, managed-read and pin handles. Prove renew/close and unknown-commit behavior in the narrow native continuation before production ACK. |
| Managed `client_dev` trial | Empty owned-schema create/read/delete authority was proved, then the schema was removed. The owner adopted a 15-minute operational-retention profile and a total spend ceiling of $100, aiming below $10. Personal Compute is permitted only if the contract is met; no paid compute has started ([admission packet](evidence/managed-a1-jobs-admission-packet-20261029.md)). | Admit one dedicated runtime, exact UC mode, predictive optimization enabled, cost meter and live source/ACK/pin custody. Run one small native correctness trial, inspect all required original Delta versions before ACK and at close, retain receipts, and delete the owned schema. |
| Performance and graph export | The one local 72-partition 45M/225M candidate projects 22.437 minutes for setup, writes and one combined witness at 80% of measured throughput, before separate scans, point reads, graph action, startup and cleanup. No further performance run has occurred. The full 1B/5B path remains held; this local result does not qualify target-scale performance ([R05 target napkin](evidence/r05-few-minute-target-napkin-20261101.md)). | Follow the napkin's conditional full-path discriminator only after compute, runtime, access and spend admission. Review a complete projection over 15 minutes with the owner. Preserve full Delta semantics and the two required independent scans; do not infer scale qualification from the local sample. |
| Release artifact | Focused development wheel/sdist inspection and the local GitHub publisher are ready. Draft 0.1.0 release notes and stable package metadata are prepared for a focused build check; this does not create a tag or authorize an Ashlar release. CI's heavy qualification workflows are already manual and PR checks are path-scoped with cancellation. | After the remaining functional gates, rebuild and install the exact wheel/sdist from reviewed clean main, verify selected resources/licenses, create the immutable tag and publish locally to GitHub. Report exact supported subset and evidence; keep unsupported paths out of the release claim. |

The remaining gates are functional and authority boundaries, not a request for
another broad verification matrix. A local success does not turn a draft
contract, unverified ACK scope or held production interval into a release pass.
