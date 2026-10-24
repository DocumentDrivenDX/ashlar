# UMF 0.5.6 public-main delta release-risk review (2026-10-23)

This receipt reviews the proposed UMF 0.5.6 candidate at
`9a11ae5c04c23f0cc0ae1d61a27c4d7a64819b08` for Sol's release review. It is a
source and evidence review only; no source code was edited as part of this
review, and no upstream builds, tests, or native execution were run here.

## Sol review disposition and upstream state

No blocking defect was found in the five-commit forward change from the public
0.5.5 base. The new API first validates the complete managed document, then
projects only the selected local target; it keeps the authored definition and
managed DDL path intact, rejects unknown target fields and unqualified codecs,
and reports the file-size hint as unrealized. The changed Rust harness line is
only a TypeScript non-null assertion after an `index > 0` guard. The remaining
changes update package and generated digest metadata. The focused target tests
cover the source-preservation, disposition, target-binding, and refusal cases.
This is a review of source behavior and existing evidence, not a native Delta
pass or certification of every historical Rust code path.

The historical `5ba77cff…` to candidate range was also inventoried as requested:
135 commits, nine merges and 1,192 changed files. The R2/R3 Rust and embedder
merges are already in the public `009c04254…` base, rather than being newly
introduced by this push. That history is dominated by retained HELIX evidence;
the production Rust migration remains an additive, separately qualified scope
under UMF's [migration plan](https://github.com/DocumentDrivenDX/umf/blob/009c042545a17b49ea857eff22d0fcf3fd71ef44/docs/helix/04-build/rust-migration-plan.md).
I found no new Rust default-engine switch or compiled native binary in the
five-commit forward diff. Historical CI additions and the wider native binding
surface were not rerun for this Ashlar release decision.

The reviewed commit was fast-forward pushed to `DocumentDrivenDX/umf` `main`
from exactly `009c042545a17b49ea857eff22d0fcf3fd71ef44` on 2026-10-23.
It is still source, not a published UMF 0.5.6 package or the requested 0.10.1
SDK release. At the time of this review, draft upstream PR #47 appeared based
on the former main and to overlap this push in the generated metamodel
manifest. A later public-state check found the PR base had advanced to the
reviewed main and its head already contained the generated digest refresh; see
the [current-state receipt](../umf-0.5.6-pr47-current-state-20261023/README.md).
That earlier apparent conflict is no longer an open sequencing item.

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
