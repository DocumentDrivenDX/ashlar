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
| Truss source lifecycle | A fresh local source was accepted through public Truss APIs and migrated to 0.0.51. R12 stopped before consumer/seed on a missing accepted-document grant. R13 configured narrow roles/module access, then stopped on the new reader's public status permission. Those role effects remain in its stopped clone; no consumer, seed, feed read, ACK or publication occurred. | Reconcile R13 read-only, audit the entire exact public API privilege chain, then complete one local seed, publication and protected ACK lifecycle with original source/consumer/checkpoint custody. Do not reuse an uncertain clone or infer source authority from a descriptor. |
| Production source and pin custody | Truss 0.0.51 supplies operation-scoped feed read/reread and receipt ACK/reconciliation, but no renewable, fenced source interval. Ashlar's managed publication-read/native-pin and independently authorized destination-registration/ACK-scope owners are also incomplete ([A1 seam review](evidence/source-holder-a1-live-port-gap-20261024.md)). | Implement and independently qualify the versioned Truss fence across ACK, re-registration and migration, then compose it with Ashlar-owned destination/ACK, managed-read and pin handles. Prove renew/close and unknown-commit behavior natively before production ACK. |
| Managed `client_dev` trial | Empty owned-schema create/read/delete authority was proved, then the schema was removed. The owner adopted a 15-minute operational-retention profile and a total spend ceiling of $100, aiming below $10. Personal Compute is permitted only if the contract is met; no paid compute has started ([admission packet](evidence/managed-a1-jobs-admission-packet-20261029.md)). | Admit one dedicated runtime, exact UC mode, predictive optimization enabled, cost meter and live source/ACK/pin custody. Run one small native correctness trial, inspect all required original Delta versions before ACK and at close, retain receipts, and delete the owned schema. |
| Performance and graph export | The measured 45M/225M sample projects 28.2 minutes for setup, writes and one combined scan at 80% of observed throughput, before other requested operations ([napkin math](evidence/r05-redesign-napkin-math-20261029.md)). The owner held the full 1B/5B trial pending redesign. | Make a plausible five-minute design target from measured bytes, write rate, CPU and storage bandwidth. Review any predicted run over 15 minutes with the owner. Then run at most one appropriately budgeted full load with two point lookups, two scans and one graph-engine export; preserve full Delta semantics. |
| Release artifact | Focused development wheel/sdist inspection and the local GitHub publisher are ready. CI's heavy qualification workflows are already manual and PR checks are path-scoped with cancellation. | After the above gates, set the stable version, build and install the exact wheel/sdist from reviewed clean main, verify selected resources/licenses, create the immutable tag and publish locally to GitHub. Report exact supported subset and evidence; keep unsupported paths out of the release claim. |

The remaining gates are functional and authority boundaries, not a request for
another broad verification matrix. A local success does not turn a draft
contract, unverified ACK scope or held production interval into a release pass.
