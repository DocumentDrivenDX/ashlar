# UMF 0.5.6 / SDK 0.10.1 public-state update (2026-10-24)

This update supersedes the pre-merge observation below. GitHub reports [PR #47](https://github.com/DocumentDrivenDX/umf/pull/47) closed and merged as `259bf1ed91466ff7e9782d7517c14f2112282026`. The merge tree is `204cfba81eadbe9a6a0458bc9a7de2dc1e50e58b`; the public `umf-v0.5.6` release was published 2026-10-24 and its tag resolves to that commit.

The public release assets are independently byte-verified: macOS 11+ ARM64 wheel `49459bb2dbc37ac9b62b269e9664bac1483439c7d4c79f0b07855e9c8546c823` (3,250,983 bytes), Linux x86_64 wheel `3512e56730d1dab59d803b20301aea3ac8f6896b0dab452f767f4c3575b9ff4c` (3,527,255 bytes), and source archive digest `f0bc596a38de03d861212dd30d9d1d2844af215c4572dea7d7186e1f8cda0bb5` as release metadata. Ashlar installed the macOS wheel in a fresh CPython 3.12.14 environment and passed focused core compatibility checks. Linux bytes were verified but not installed or executed. See the [asset adoption receipt](../umf-0.10.1-ashlar-adoption-20261024/README.md). Databricks compatibility and native Spark/Delta behavior remain unqualified.

The merged SDK distribution and UMF core package are 0.10.1; UMF document/core semantics remain 0.10.0. Ashlar's source-model contracts continue to name their semantic model version independently. The platform asset verification and macOS-only isolated adoption are recorded above. None of this establishes Spark/Delta execution, Truss lifecycle, Unity Catalog, or publication/ACK qualification.

## Historical pre-merge observation (2026-10-23)

This later read-only GitHub observation supersedes the PR-base and generated
digest sequencing concern recorded in the earlier [source review](../umf-0.5.6-main-delta-review/README.md).
It does not change that review's historical timing or disposition.

Public UMF `main` is `9a11ae5c04c23f0cc0ae1d61a27c4d7a64819b08`. [PR #47](https://github.com/DocumentDrivenDX/umf/pull/47)
is still open and draft, with base at that exact `main` SHA and head
`b530ea2937b290c53892be318408f8be10ba0ef2`; the head includes integration
commit `fc1f8c969436523c54442802841aaa9a166dd9d6` and compares 13 commits ahead,
0 behind. The generated manifest/action producer digest refresh is already in
the PR head, so the earlier apparent base/digest conflict is resolved in the
currently observed refs. The [PR check run](https://github.com/DocumentDrivenDX/umf/actions/runs/40035286205)
passed for Linux x86_64 / Ubuntu 24.04 / CPython 3.11.17 and macOS ARM64 /
macOS 15 / CPython 3.11.9. At the observation, three production-conformance
checks remained in progress and independent final review and owner release
disposition remained open.

The latest non-draft public release observed is [`umf-v0.5.5`](https://github.com/DocumentDrivenDX/umf/releases/tag/umf-v0.5.5),
published 2026-10-23; no public `umf-v0.5.6` release/tag was found. PR #47's
candidate distribution version is 0.10.1 while the semantic UMF core/model
version remains 0.10.0. This observation does not claim package publication or
Ashlar adoption of the candidate.

The PR head also has a low-severity documentation follow-up: [`python/README.md`](https://github.com/DocumentDrivenDX/umf/blob/b530ea2937b290c53892be318408f8be10ba0ef2/python/README.md)
says PyPI publication is not configured, while the repository has a [manual
PyPI publishing workflow](https://github.com/DocumentDrivenDX/umf/blob/b530ea2937b290c53892be318408f8be10ba0ef2/.github/workflows/pypi.yml).
The README's installation example points to the existing `umf-v0.5.1`
`0.10.0` artifact rather than latest published [`umf-v0.5.5`](https://github.com/DocumentDrivenDX/umf/releases/tag/umf-v0.5.5).
This is an upstream documentation correction to consider; it is not a blocker
to publishing the GitHub release and was not changed here.

Remaining release sequence: finish the three pending checks and independent
review, record owner disposition, merge the PR, publish the qualified assets,
and verify their public hashes. Ashlar must then update its package pin only
against the verified published artifact and rerun focused adoption checks.
