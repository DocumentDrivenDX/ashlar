---
ddx:
  id: ashlar.evidence.sdk010-delta4-runtime-integration-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.umf010-upstream-adoption-20261031
      kind: informed_by
    - id: ashlar.evidence.sdk010-delta4-bridge-candidate-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-sdk058-delta4-realization-candidate-20261030
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# SDK 0.10.0 Delta 4 runtime integration — 2026-10-31

This evidence adds a versioned, source-backed SDK 0.10.0 Delta 4 bridge beside
the preserved SDK 0.5.8 route. The implementation lives in
`src/ashlar_host/local_current_delta4_umf_sdk010.py`; its packaged resources
include the pinned source-policy manifest, complete build input inventory,
bridge entry and IIFE, role semantics, and third-party notices. Runtime custody
checks require exact SHA-256 pins for those artifacts and exact byte hashes for
the six Ashlar `.4-xxhash64` source roles.

The selected SDK release is `umf-v0.10.0` at
`96230e0eef44b255d33575ba496402555b76800e`. The official SDK archive SHA-256
is `2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c`; the
public package entry SHA-256 is
`b32bce2fdcf928bc5b9fd8376b4c7cab9beb1b63d90cb1d20b9ad0ec94595951`. The
source-backed bridge is built from the two release source modules recorded in
the pinned inventory. Its pure proposal outputs matched all six existing
frozen release proposal objects in the earlier Node VM qualification.

The focused Python custody and six-role MiniRacer parity tests both pass in the
existing Python 3.12 Truss 0.0.51 local runtime cohort. The test compares each
new bridge proposal to the frozen SDK 0.5.8 release proposal at the same exact
target path. The SDK 0.10.0 resources are included in Ashlar's package-data
selection. An offline local wheel and sdist build confirmed all seven SDK 0.10
resource files in both artifacts match their source SHA-256 values exactly.
This profile is callable by Ashlar code but is not admitted by
existing R18 receipts, which retain the
SDK 0.5.8 profile and identity. No Spark, Delta, Databricks, or cloud execution
was performed. The SDK archive and source custody are release-pinned; Bun's
installed dependency bytes were inventoried and lock-matched, not independently
reconstructed from dependency tarballs.
