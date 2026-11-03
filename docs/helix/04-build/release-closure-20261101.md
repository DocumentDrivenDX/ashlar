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

# Ashlar release closure — 2026-11-03

The `0.1.0rc1` provider-cache candidate is built and staged. No tag or release
has been made. The local GitHub publishing path is prepared, but publication
remains held while functional qualification is incomplete. Claims below are
limited to the exact local runtimes, source authority and evidence recorded.

| Gate | Evidence and status | Remaining closure |
| --- | --- | --- |
| Upstream cohort | Truss 0.0.55 wheel SHA-256 `156df20641d8c4fee6d836c6b29266a0a7eb3b1299c407e430ce0d2c07a79805`, runtime manifest `5ecb599f65d64e6656bf6c83c30cea6e2aefd488cb3fe821b749458d5e9b3522`; UMF SDK 0.10.2 / Core 0.10.6, Weft 0.4.56, Access 0.1.0.dev8 and model 0.10.0. Truss 0.0.55 wheel, sdist, HF archive and isolated-runtime constraints are locally staged and hash-verified, not published ([bundle manifest](evidence/current-upstream-release-20261102/truss-0.0.55-local-release-bundle.json)). | The Ashlar `0.1.0rc1` provider-cache candidate is built and staged. Refresh its final pins after remaining gates. Keep Truss .55 unpublished until its coordinated release decision. |
| Fresh local R61 graph path | A fresh Truss .55 local-development source published a metadata genesis, seven-operation seed and two-record continuation to Spark 4.2 / Delta 4.4.1 with `.4-xxhash64`. Protected ACKs matched. The original owner export/custody bytes contain six nodes and three edges; a separate retained-byte GraphFrames action passed. The full worker's initial GraphFrames adapter attempt failed after source/Delta closure, then the reviewed GraphFrames-only action passed. Native Weft singleton/grouped values were observed, but the full worker did not persist a durable Weft result hash. See [R61 composed closeout](evidence/current-upstream-release-20261102/r61-fresh-truss055-attempt13-continuation/qualification-closeout.json). | Keep the result limited to the recorded local-development authority and persisted receipts. The C17 classic Spark Jobs attempt was refused before cluster creation; see the classic Spark row. |
| Managed-current local A1 recovery | Recovery05 reopened the exact attempt14 pending A1 source page, independently compared all four literal Delta-version role bags with the source-derived oracle, and reconciled one protected ACK as matched. No source group or Delta write/publication was replayed. The work followed a recorded source-hold gap. Readback was path-based and made no Unity Catalog or Databricks claim. See [attempt14 recovery closeout](evidence/current-upstream-release-20261102/managed-current-attempt14-a1-recovery/closeout.json). | This local recovery does not close the managed target gate or establish an uninterrupted source hold. |
| Managed target | The original `aidev-cus` / `client_dev` serverless target is admitted for one 1680-second campaign, with each publication holder interval still limited to 900 seconds. The observed Premium workspace tier supports the documented approximate 120 DBU/hour default; the modeled campaign total including prior trials and reserve is $96.14 ([cost basis](evidence/current-upstream-release-20261102/managed-serverless-premium-basis-20261103.json)). | Complete the bounded managed trial with exact source, native role-vector and source/ACK custody proof. |
| Classic Spark publication | The C17 trial was refused before compute creation: the first command hit a local CLI argument-parser error; the corrected retry reached the workspace backend, which reported no associated worker environments. The post-terminal cluster inventory was empty; no task, Spark, Delta, source or ACK action ran. See the [C17 refusal evidence](evidence/current-upstream-release-20261102/managed-classic-c17-create-reconciliation-20261103/README.md). | A classic run remains unqualified; do not claim the classic Jobs route passed. |
| Fabric and scale | Fabric integration is unfinished. The 1B-node / 5B-edge trial remains held after the measured sample exceeded the owner's 15-minute review threshold. | Keep both claims out of this candidate until separately qualified. |
| Release artifact | Ashlar package metadata is `0.1.0rc1`; its provider-cache candidate is built and staged. The Truss .55 local asset bundle is not a published release. No Ashlar tag or GitHub release exists. | Close managed and classic gates, freeze exact source/artifacts, then publish through the prepared local GitHub path. |

Do not treat the composed R61 result or attempt14 recovery as a managed-target
pass. Prior failed attempts remain available as historical evidence; the
closeouts identify the one successful retained-byte GraphFrames action and the
one matched recovery ACK without hiding preceding failures.
