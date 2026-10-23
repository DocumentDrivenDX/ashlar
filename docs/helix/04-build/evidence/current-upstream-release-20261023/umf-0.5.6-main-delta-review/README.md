# UMF 0.5.6 public-main delta release-risk review (2026-10-23)

This receipt reviews the proposed UMF 0.5.6 candidate at
`9a11ae5c04c23f0cc0ae1d61a27c4d7a64819b08` for Sol's release review. It is a
source and evidence review only; no upstream files were changed and no upstream
builds, tests, or native execution were run.

## Lineage and change scope

Sol's fresh GitHub branch read on 2026-10-23 reports public UMF `main` exactly
at `009c042545a17b49ea857eff22d0fcf3fd71ef44`. The candidate descends directly
from that point by five commits, changing seven files with 343 insertions and
nine deletions. The checked candidate checkout independently shows the same
seven-file diff. The current proposal is therefore a fast-forward of the
reported public-main head.

The current push adds the Spark Delta 4 DDL realization contract and direct
Delta-path handling in `src/adapters/delta/ddl.ts`, with focused target tests.
The other changes are the SDK version bump, a narrow Rust conformance harness
lookup adjustment, and generated manifest/action producer digest refreshes.
The two Rust-spike merges are already ancestors of public main: PR #43
(`f323a2dec`, R2 core/metamodel) and PR #45 (`8b06efc2d`, additive R3
compiler/access-generation surface). The embedder completion merge PR #44
(`009c04254`) is also already public main.

The larger historical comparison from old base `5ba77cff91a121a44c82f6bce32a78d8842161b5`
to the candidate contains 135 commits and 1,192 files, with 3,130,861
insertions and 1,612 deletions. Most of that apparent size is retained
historical HELIX evidence under `docs/`, including large frozen predecessor and
review JSON. It is accumulated history, not the current proposed push. That
history includes the Rust R2/R3 work and the embedder work; the two Rust merges
did not arrive in the five-commit delta against current public main.

## Evidence already available

Ashlar has a clean SDK package-manager install receipt for the exact candidate
commit and tree. It records the candidate SDK tarball hash, successful Bun
1.4.3 install, 1,751 installed core distribution files, and passing unified
generator, 139 conformance cases, bundled actions, and loader entrypoint checks
([package-install receipt](../umf-0.5.6-sdk-package-install/README.md)). This
is candidate-head local package/install evidence; it does not establish a
public package release or native Spark behavior.

The Ashlar bridge was rebuilt from the exact candidate sources and its six
source roles passed focused offline checks. The retained browser probe covers
the candidate exports, YAML round-trip, and direct-path/named-relation
proposals. Those checks use inert Spark and do not establish actual Delta
table creation or writer behavior
([bridge receipt](../umf-0.5.6-local-delta4-bridge/README.md)).

## Release gates and limits

- Treat `9a11ae5c…` as an unreleased candidate until its upstream change is
  reviewed and merged or an explicit commit-based adoption is recorded. The
  SDK install receipt does not mean 0.5.6 has been published.
- Before claiming the new Delta 4 behavior works in a runtime, run the
  existing small Ashlar native acceptance for the selected Spark 4.2.0,
  Delta 4.4.1, JDK 21 profile. Check the emitted table DDL and observed table
  properties there. The local bridge evidence is preparation only; no current
  native result is claimed.
- Keep candidate support statements scoped to the recorded host and package
  evidence. No broad platform reruns are requested by this delta review; broader
  support claims still require their own platform evidence.

No credential or compiled native-binary issue was identified in the read-only
candidate scan. The historical range contains a same-size changed generated
gzip compiler payload; it is a generated asset, not a checked-in compiled
native library. Mutable major-version CI action references exist in the
historical Rust workflow additions, but those workflows are outside the
five-commit current push. This review does not certify workflow execution or
release artifact publication.
