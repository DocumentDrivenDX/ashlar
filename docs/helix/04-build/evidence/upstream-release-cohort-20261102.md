---
ddx:
  id: ashlar.evidence.upstream-release-cohort-20261102
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
    - id: CONTRACT-004
      kind: governed_by
    - id: ashlar.evidence.a1-managed-release-bridge-candidate-20261102
      kind: informed_by
---

# Upstream release cohort — 2026-11-02

## Selected artifacts

The fresh-install candidate pins Truss 0.0.53, UMF Rust SDK release
`umf-rust-v0.1.0` (SDK 0.10.2 and Python Core 0.10.6), and preserves Weft
0.4.56 and `umf_access==0.1.0.dev8`. Truss 0.0.53 advertises UMF model
0.10.0; that remains the document model version. The `.53` profile is
`ashlar-truss-current-catalog-evidence/0.8-fresh-baseline`, separate from the
historical `.52` candidate. No `.52` database or state is eligible for reuse.

Pinned Truss wheel SHA-256:
`a6c8568d676cd69675dc27d92fdfed2b85b69089ff0007d2b5bd2f5f48be6d89`.
Its runtime-manifest SHA-256 is
`5ecb599f65d64e6656bf6c83c30cea6e2aefd488cb3fe821b749458d5e9b3522`.
UMF Rust SDK archive SHA-256 values are `785de2c4db18b030c9462c7f55f52d012db50a4d4b3c608a4dc3aa6f5fa0d39b`
(macOS arm64) and `0e210969dc6d585d73b0e70d35660421b40c141746389118f7181c54bfcff943`
(Linux x86_64). Python Core wheel SHA-256 values are
`e6219539dabcdfe9657bc09485ad991b3380ae306c658d0a51a35caac0196e36`
(macOS arm64) and `9a4fc6e739fb9c0c80e22916490484fb41b6f00985afc53fb81dcca2d72029b9`
(Linux x86_64). The selected release tag commit is
`e35cd92b45e16816be2bdbf28e83ef9c53ac99a2`.

## Focused compatibility evidence

The exact `.4-xxhash64` six-role source documents were passed to the UMF Core
0.10.6 Rust-backed `delta-ddl` adapter using operation
`realizeSparkDelta4`. All six schema hashes match the previously admitted
SDK 0.10.1 output: object_current `ae42872387493d3dd1d6393950ef177fbb513418b5935c1eba1f348f4bef25dc`,
edge_current `eb17bbf49525de56c8faac2f9005541f711119949db30441ccb087c715eede00`,
tombstone `2d7f473cfc1a389d3fd1d72d0f2fad4cf3a7d95efe4942888a5aa1f3d4fe46d3`,
whole_source_history `c28182433e92d4017f10b51b312de9bf1e67aa1aa684183cc4e2d96a094f4f12`,
publication_attempt_phase `25182ffd06f9e748919d7a4bd6576efa97d8e1f809fb35003a0c317cc91fff20`,
and publication_manifest `aecced8d524add1cc4b7324358bce16325b53d00fb41c35b51f18dc951f57ee8`.
The current Rust-backed bridge is versioned separately and binds proposals to
all six original carrier roles. This is proposal compatibility evidence only;
no Spark or Delta DDL was executed, and no source/ACK authority is established.

The follow-up native initializer is now selectable from the Truss graph
publisher with `umf_sdk_version="0.10.2"`. It writes a distinct
`ashlar-local-current-installation-profile/0.5`, records the selected SDK/Core
artifact and semantic-resource digests without describing them as an IIFE
bridge, and binds the resulting six target aliases through journal retention
and restore validation. Existing `/0.4` verification remains unchanged. The
initializer uses the same six Core proposal outputs before issuing Spark DDL;
the focused initializer test uses a recording Spark double, so live native
table creation and restore remain unqualified.

Graph export custody now has a separate `.53` source-admission profile that
retains the exact published Truss wheel and runtime-manifest digests alongside
the report/view/source revision digests and declared model versions. Its pure
round-trip regression builds, loads and admits a `.53` export receipt from the
retained graph manifest and rejects a substituted wheel digest. The older `.52`
source validator and evidence remain unchanged.

Focused Python 3.12 checks passed: `test_truss_current_catalog` (14 tests),
`test_truss_catalog_reader`, `test_truss_source_custody_selection`, and
`test_truss_graph_admission` (16 tests total). `git diff --check` passed.
These pure checks do not establish a live Truss `.53` feed, source-custody
session, Spark installation, Databricks runtime, publication or ACK. R61 fresh
local qualification remains a separate pending activity. Existing `.52`
evidence is retained without upgrade claims.
