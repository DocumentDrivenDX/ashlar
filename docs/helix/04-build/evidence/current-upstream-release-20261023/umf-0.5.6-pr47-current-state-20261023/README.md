# UMF 0.5.6 / SDK 0.10.1 public-state follow-up (2026-10-23)

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
