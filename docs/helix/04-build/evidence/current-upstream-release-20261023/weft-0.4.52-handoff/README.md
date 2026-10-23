# Weft 0.4.52 Ashlar release handoff

**Observed:** 2026-10-23. This is an Ashlar-side proposal from the public
Weft candidate metadata; it is not release authorization.

## Proposed release identity

- Distribution/version: `weft-sql 0.4.52`, Git tag `v0.4.52`.
- Compiler source commit: `32ec02f0c3fd24a97b7726500dc5992c6475908d`.
- Proposed tag target/release-metadata commit:
  `b59938f4671e1aa9bd99941f233a8d83e1b576d7` (public Weft `main` at
  observation). The candidate compares as two commits ahead of compiler source;
  the delta adds selected-release evidence and publisher/release tooling, with
  no compiler, Cargo, spec, vendor, or contract input changes.
- Candidate pins file:
  [Weft release-pins-0.4.52.json](https://github.com/DocumentDrivenDX/weft/blob/b59938f4671e1aa9bd99941f233a8d83e1b576d7/docs/helix/04-build/releases/release-pins-0.4.52.json).
  It records status `candidate`, and the source and wheel hashes below.
- Public checks at observation: [main](https://github.com/DocumentDrivenDX/weft/commit/b59938f4671e1aa9bd99941f233a8d83e1b576d7)
  resolves to the proposed metadata commit. Both the
  [`v0.4.52` Git ref](https://api.github.com/repos/DocumentDrivenDX/weft/git/ref/tags/v0.4.52)
  and [release](https://api.github.com/repos/DocumentDrivenDX/weft/releases/tags/v0.4.52)
  return 404. No tag/release publication is evidenced.

| Platform | Selected wheel | Wheel SHA-256 | Verified scope |
| --- | --- | --- | --- |
| macOS ARM64 | `weft_sql-0.4.52-cp39-abi3-macosx_11_0_arm64.whl` | `598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46` | Local build/packaging; four page compiler cases, two current-read compiler controls, two selected Source-F controls |
| Linux x86_64 manylinux2014 | `weft_sql-0.4.52-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl` | `9145b7f683dc84325cfe3ab7c4b885685db05be814a4d554a4b739ae43d5cbdc` | Run 39981671006 packaging/installability; Ashlar separately has two offline compiler parity/refusal cases |

The macOS selected-assets receipt pins native module
`f4be63caaacedeaa3d8af1b1f0587c561a2ff1ae0a279f4f8f7dcf836f6bb24f`;
the Linux receipt pins
`a326a607898e4cbcf9f4801785059d096e8bdb0557f46818951c70db0d7a7956`.
Ashlar receipts are retained in this directory tree:
[macOS compiler-only receipt](../truss049-selected32ec-preflight/weft-functional-receipt.json),
[Linux package evidence](../weft052-selected-linux/README.md), and
[Linux functional cases](../weft052-selected-linux-functional/README.md).

## Remaining handoff gates

1. Review the candidate metadata at `b59938f` and approve its proposed tag
   target. The upstream selected-assets publisher requires a draft `v0.4.52`
   release at that exact metadata commit, source ancestry from `32ec02f`, and
   the selected Linux run ID `39981671006`. It validates staged macOS assets
   and exact bytes while the release remains a draft; a separate promotion is
   required. Neither workflow has been run for publication.
2. The Weft candidate's release pins use UMF core `0.10.0` and access
   `0.1.0.dev6`. The broader selected macOS compatibility receipt uses dev6;
   Ashlar separately passed two focused managed-page/compiler and typed-SDK
   controls with access `0.1.0.dev7` ([receipt](../weft052-umf-dev7-compat-20261023.json)).
   That offline result does not qualify native runtime behavior, which remains
   open alongside public release publication.
3. Ashlar `pyproject.toml` still pins `weft-sql==0.4.48`. After the public
   assets exist, review the exact 0.4.52 distribution source/install path and
   update the Ashlar pin only with an approved focused compatibility receipt.
4. No native SQL backend, PostgreSQL, Databricks, Unity Catalog, or production
   execution is qualified by these artifacts. The candidate release-pins labels
   Linux compiler behavior `not-claimed`; Ashlar's two selected Linux
   compiler-only cases are separate evidence and do not alter that upstream
   release claim.

The proposal is suitable for review as a release handoff; it does not authorize
tagging, upload, promotion, or an Ashlar dependency change.
