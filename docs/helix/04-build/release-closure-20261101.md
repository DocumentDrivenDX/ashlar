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

Release publication is held. The local Truss continuation and protected ACK are
complete; finish the focused query and export checks against that publication.
Then qualify the actual managed
`client_dev` runtime and source/UC custody; local fixtures cannot establish that
target. Review the final candidate CI paths, build and install the exact
clean-main artifact once, and only then tag and publish. Do not turn the held
1B/5B scale projection into a performance claim without an approved target
and run.

| Gate | Current evidence | Required closure |
| --- | --- | --- |
| Upstream cohort | Selected UMF SDK 0.10.1, Python Core 0.10.5 and Weft 0.4.56 are pinned. Truss 0.0.52 is published; Ashlar's `truss-local` extra pins the public wheel by exact URL and SHA-256 `00d8b11abc83d9c84fa131ae6f3eef23ecad9d321d4b04a13641ed31c77e9747`. The SDK 0.10.1 `.4-xxhash64` six-role proposal matches the versioned bridge. | Recheck selected production runtime assets before the stable Ashlar build. |
| Local publication and native reads | SDK 0.10.1 transaction one committed three graph operations and two metadata operations; a manifest-only continuation completed with no replay. One Spark 4.2.0 / Delta 4.4.1 local read matched the singleton, two Weft grouped queries, page/lookahead and continuation against the frozen source oracle. All six table heads matched at open and close; cleanup succeeded ([manifest completion](evidence/current-upstream-release-20261101/g03-sdk0101-r3-manifest-continuation/r5-completion.md), [Weft result](evidence/current-upstream-release-20261101/g03-weft-native-consumer/README.md)). | Preserve these narrow local results. Do not rerun them unless code or binding changes. Linux/managed execution and Truss source/ACK are separate gates. |
| Truss source lifecycle | Published Truss 0.0.52 accepted a fresh local-development source and consumer. R36 published six-role Delta genesis, independently read back its four data roles, and matched a protected ACK ([R36 custody](evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/r28-page-sequence/r36-ack-admission-reconciliation.md)). R42 committed the seven-change seed: four objects, three edges, seven history rows, no tombstone. R49 restored that original applied artifact without replay, independently read back the exact native Delta row bags and four-role publication vector, durably recorded completion, then matched the protected Truss ACK to the archived page's next checkpoint ([R49 reconciliation](evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/r28-page-sequence/r37-inplace-resume/r49-read-only-reconciliation.json)). The R55 group committed two more objects. R58 recovered the pending page and independently verified 6 objects, 3 edges, 9 history rows and the four-role vector without group replay or ACK. The reviewed R59 ACK-only path matched original source receipt and consumer SQL before a fresh native Delta readback, then matched one protected ACK, post-ACK checkpoint and public empty interval ([R59 completion](evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/r28-page-sequence/r37-inplace-resume/r59-native-ack-completion.json)). The supervisor completed in 17.987 seconds with clean PostgreSQL/process shutdown. This is local-development authority only. | Qualify exact source, destination, query and export subset on the completed continuation. Production source authority remains a separate gate. |
| Production source and pin custody | Truss 0.0.52's native source fence and `.74→.75` migration passed focused local checks. Reviewed, focused-tested [genesis primitives](evidence/managed-genesis-owner-local-20261101.md) record typed A/B pre-page positions and an empty six-role manifest; the driver accepts that predecessor only with explicit A/B anchors and a caller-admitted first A batch. A read-only [Truss .52 source owner](evidence/managed-genesis-owner-local-20261101.md) selects original A/B report, catalog, accepted-document, cursor and page evidence, and a [prefix delivery check](evidence/managed-a1-prefix-delivery-boundary-20261101.md) binds separately observed pages to each retained `/0.2` request. These pure boundaries have no live A/B execution or source freshness authority. Managed native pin renewal, graph-data delivery composition and live ACK holding remain unwired. Acceptance/feed observations still have local-development authority only. The [A1 adapter gap](evidence/a1-managed-native-adapter-prerequisite-gap-20261101.md) records the exact missing runtime and owner inputs. | Compose independently selected A/B registrations, exact genesis and graph-data prefix with the managed driver; verify native Core observation, source reread and retained Delta pin under real source authority, then prove renew/close and unknown-commit behavior before production ACK. |
| Managed `client_dev` trial | Empty owned-schema create/read/delete authority was proved, then the schema was removed. The owner adopted a 15-minute operational-retention profile and a total spend ceiling of $100, aiming below $10. Read-only SSO inventory confirms `client_dev` exists in the approved `dbw-aidev-cus` workspace, with effective predictive optimization `ENABLE`; the usual dev workspace has no `client_dev` catalog. There are no running clusters in `dbw-aidev-cus`. Its only exposed compute policy is single-node Personal Compute with `cluster_type=all-purpose`, zero workers and a default 72-hour auto-termination; the current publication task requires `databricks-serverless`, so that policy does not satisfy its runtime contract. Read-only Jobs inventory shows existing serverless Python tasks in the workspace, but no Ashlar run-as, Spark/UC mode, grants, immutable dependencies or total cost meter is admitted. No live managed source-holder ACK or four-role native pin owner is wired. No paid compute has started ([admission packet](evidence/managed-a1-jobs-admission-packet-20261029.md)). | Admit one dedicated serverless runtime, exact UC mode, enforced short lifetime, total cost bound and live source/ACK/pin custody. Run one small native correctness trial, inspect all required original Delta versions before ACK and at close, retain receipts, and delete the owned schema. |
| Performance and graph export | The [full-path 80%-rate projection](evidence/r05-full-path-80-rate-design-proposal-20261101.md) uses the latest 45M/225M local sample to estimate at least 35.923 minutes for 1B/5B setup, one write, two scans and one graph input read, before point reads, startup, final checks or cleanup. The projected active Delta output is 69.25 GB and minimum four-pass data movement is 277.01 GB. Ideal 256-vCPU arithmetic gives 2.526 minutes only with unmeasured scale-out and about 2.28 GB/s admitted I/O at 80% utilization; it is a sensitivity, not acceptance evidence. The 72-partition sample improved the measured interval 1.37× but remains held by its supervisor. The full run remains held under the owner's 15-minute review threshold. | Select one defensible target-specific design and admitted hardware/storage envelope; estimate the complete path before a full run, preserving full Delta semantics, both scans, two point lookups and the graph action. Review any estimate over 15 minutes with the owner. |
| Release artifact | Focused development wheel/sdist inspection and the local GitHub publisher are ready. The package remains `0.1.0.dev1`; no standalone 0.1.0 release-notes file or stable Ashlar tag exists. The local publisher currently uses generated GitHub notes. A focused build and install check remains. Heavy qualification workflows are manual. The Truss-preparation and domain-pack suites now run on matching PRs or manual dispatch rather than duplicate post-merge main pushes; direct main pushes require focused local checks while Actions quota is exhausted. | After the remaining functional gates, rebuild and install the exact wheel/sdist from reviewed clean main, verify selected resources/licenses, create the immutable tag and publish locally to GitHub under the owner's release authorization. Report exact supported subset and evidence; keep unsupported paths out of the release claim. |

The remaining gates are functional and authority boundaries, not a request for
another broad verification matrix. The matched local ACK does not turn a draft
contract or held production interval into a release pass.
