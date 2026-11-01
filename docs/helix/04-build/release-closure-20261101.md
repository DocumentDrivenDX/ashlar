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
| Upstream cohort | Selected UMF SDK 0.10.1, Python Core 0.10.5, Weft 0.4.56 and Truss 0.0.51 are pinned. The SDK 0.10.1 `.4-xxhash64` six-role proposal matches the versioned bridge. | Recheck immutable published assets and exact package digests immediately before the stable build; qualify only selected production runtime assets. |
| Local publication and native reads | SDK 0.10.1 transaction one committed three graph operations and two metadata operations; a manifest-only continuation completed with no replay. One Spark 4.2.0 / Delta 4.4.1 local read matched the singleton, two Weft grouped queries, page/lookahead and continuation against the frozen source oracle. All six table heads matched at open and close; cleanup succeeded ([manifest completion](evidence/current-upstream-release-20261101/g03-sdk0101-r3-manifest-continuation/r5-completion.md), [Weft result](evidence/current-upstream-release-20261101/g03-weft-native-consumer/README.md)). | Preserve these narrow local results. Do not rerun them unless code or binding changes. Linux/managed execution and Truss source/ACK are separate gates. |
| Truss source lifecycle | A fresh local source was accepted through public Truss APIs and migrated to 0.0.51. On a stopped clone, R16b committed all seven seed operations and read the complete genesis feed page through public APIs without ACK; the original source stayed untouched ([result](evidence/current-upstream-release-20261101/r16b-local-truss-seed-result/README.md)). | Reread and archive the exact unacknowledged page, publish it to local Delta with a complete manifest/vector, then verify and ACK under admitted source/destination custody. Continue the gated feed sequence without replaying effects. |
| Production source and pin custody | Truss 0.0.51 supplies operation-scoped feed read/reread and receipt ACK/reconciliation, but no renewable, fenced source interval. A proposed Truss 0.0.52 fence migration rolled back on an installed `public` privilege drift; the original source was untouched. Ashlar's managed publication-read/native-pin and independently authorized destination-registration/ACK-scope owners remain incomplete ([A1 seam review](evidence/source-holder-a1-live-port-gap-20261024.md)). | Resolve the exact native ACL drift, qualify the versioned Truss fence across ACK, re-registration and migration, then compose it with Ashlar-owned destination/ACK, managed-read and pin handles. Prove renew/close and unknown-commit behavior natively before production ACK. |
| Managed `client_dev` trial | Empty owned-schema create/read/delete authority was proved, then the schema was removed. The owner adopted a 15-minute operational-retention profile and a total spend ceiling of $100, aiming below $10. Personal Compute is permitted only if the contract is met; no paid compute has started ([admission packet](evidence/managed-a1-jobs-admission-packet-20261029.md)). | Admit one dedicated runtime, exact UC mode, predictive optimization enabled, cost meter and live source/ACK/pin custody. Run one small native correctness trial, inspect all required original Delta versions before ACK and at close, retain receipts, and delete the owned schema. |
| Performance and graph export | The measured 45M/225M sample projects 28.2 minutes for setup, writes and one combined scan at 80% of observed throughput, before other requested operations. Ideal scale-out makes a 192–256-vCPU production candidate plausible but unqualified; five billed minutes of compute projects about $2–3 from the retained rate reference, excluding startup and other charges ([napkin math](evidence/r05-production-hardware-envelope-20261101.md)). The owner held the full 1B/5B trial pending redesign. | Admit one actual production runtime, worker shape and cost envelope, then use the small correctness trial for startup and native observations. Review any complete projection over 15 minutes with the owner. Run at most one separately admitted full load with two point lookups, two scans and one graph-engine export; preserve full Delta semantics. |
| Release artifact | Focused development wheel/sdist inspection and the local GitHub publisher are ready. CI's heavy qualification workflows are already manual and PR checks are path-scoped with cancellation. | After the above gates, set the stable version, build and install the exact wheel/sdist from reviewed clean main, verify selected resources/licenses, create the immutable tag and publish locally to GitHub. Report exact supported subset and evidence; keep unsupported paths out of the release claim. |

The remaining gates are functional and authority boundaries, not a request for
another broad verification matrix. A local success does not turn a draft
contract, unverified ACK scope or held production interval into a release pass.
