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
| Truss source lifecycle | Published Truss 0.0.52 accepted a fresh local-development source and consumer; R24 committed seven typed graph operations. R27's read-only diagnosis found the first feed page is a zero-entity catalog/revision transaction, with the seed on the next page. R27 stopped before Spark, Delta, or ACK ([packet](evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/README.md)). | Publish and verify each actual prerequisite page, ACK only under admitted custody, then publish the seven-change seed and two-change continuation in feed order. Never equate group results with the first feed page. |
| Production source and pin custody | Truss 0.0.52's native source fence and `.74→.75` migration passed focused local checks. The successful acceptance/feed observations still have local-development authority only. The managed data-prefix owner, independently authorized destination/ACK scope, and live managed pin composition remain unqualified. The [A1 owner gap](evidence/a1-managed-source-prefix-owner-gap-20261101.md) names the original A/B registrations, cursors, two-feed progress and initial manifest that no current owner supplies. | Compose an independently selected managed graph-data prefix, native Core observation, source reread and retained Delta pin under real source authority; prove renew/close and unknown-commit behavior before production ACK. |
| Managed `client_dev` trial | Empty owned-schema create/read/delete authority was proved, then the schema was removed. The owner adopted a 15-minute operational-retention profile and a total spend ceiling of $100, aiming below $10. A bounded serverless Jobs task helper and Weft 0.4.56 `.4.1` compiler route are reviewed on main; focused compilation/admission checks pass. No paid compute has started ([admission packet](evidence/managed-a1-jobs-admission-packet-20261029.md)). | Admit one dedicated runtime, exact UC mode, predictive optimization enabled, cost meter and live source/ACK/pin custody. Run one small native correctness trial, inspect all required original Delta versions before ACK and at close, retain receipts, and delete the owned schema. |
| Performance and graph export | The local 45M/225M samples project 22.437 minutes for the 72-partition variant and 28.206 minutes for the 18-core range packet at 80% of observed throughput, each before the separate scans, point reads, graph action, startup and cleanup. The [parallel-range review](evidence/r05-parallel-range-layout-design-20261101.md) identifies an 18-task cap in the current full-size packet but does not establish local or production speedup. The full 1B/5B path remains held ([R05 target napkin](evidence/r05-few-minute-target-napkin-20261101.md)). | Admit a complete target-specific projection under the existing owner request and resource/cost limits. Review any estimate over 15 minutes before a run; preserve full Delta semantics and both independent scans. |
| Release artifact | Focused development wheel/sdist inspection and the local GitHub publisher are ready. Draft 0.1.0 release notes and stable package metadata are prepared for a focused build check; this does not create a tag or authorize an Ashlar release. CI's heavy qualification workflows are already manual and PR checks are path-scoped with cancellation. | After the remaining functional gates, rebuild and install the exact wheel/sdist from reviewed clean main, verify selected resources/licenses, create the immutable tag and publish locally to GitHub. Report exact supported subset and evidence; keep unsupported paths out of the release claim. |

The remaining gates are functional and authority boundaries, not a request for
another broad verification matrix. A local success does not turn a draft
contract, unverified ACK scope or held production interval into a release pass.
