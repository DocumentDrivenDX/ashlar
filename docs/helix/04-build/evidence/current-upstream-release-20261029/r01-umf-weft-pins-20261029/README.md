---
ddx:
  id: ashlar.evidence.r01-upstream-pin-refresh-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.toolkit-delivery-plan
      kind: informed_by
---

# R01 upstream pin refresh — 2026-10-29

Status: **public package pins refreshed; focused Weft compiler and installed
UMF/Access page-target checks passed**. Ashlar now selects UMF
Python Core 0.10.3, UMF Access 0.1.0.dev8 and Weft 0.4.56 in the `typed-sdk`
extra. The separately reviewed `truss-local` candidate selects Truss 0.0.51.
This evidence does not establish native
publication, Databricks, Truss lifecycle, or production authority.

Read-only GitHub `releases/latest` checks on 2026-10-30 returned the selected
[`UMF SDK 0.5.8`](https://github.com/DocumentDrivenDX/umf/releases/tag/umf-v0.5.8)
(target `efb4042719866888ad80c01cb2da694972eb2738`),
[`Weft 0.4.56`](https://github.com/DocumentDrivenDX/weft/releases/tag/v0.4.56)
(target `2f62e1148d1b2030558550e4f6f2ffe518bac62d`), and
[`Truss 0.0.51`](https://github.com/DocumentDrivenDX/truss/releases/tag/v0.0.51).
This verifies the selected release coordinates at that check time; it adds no
runtime qualification.

## Exact public release assets

GitHub release API metadata identifies the immutable public tags and asset
digests below. Each locally cached asset listed here has the same byte count and
SHA-256 as the public release metadata.

| Component | Public tag / source target | Selected platform asset | Bytes | SHA-256 |
| --- | --- | --- | ---: | --- |
| UMF Core 0.10.3 | [`python-v0.10.3`](https://github.com/DocumentDrivenDX/umf/releases/tag/python-v0.10.3), `7d334dec45948953718bfacd3ed6dd199cadba79` | macOS arm64, `umf_core-0.10.3-cp311-abi3-macosx_11_0_arm64.whl` | 3,331,547 | `dc2586793e1492d1f825bd9e12f27d5db9f9330ae2343d9e7773c7052f1f6ea6` |
| UMF Core 0.10.3 | same | Linux x86_64, `umf_core-0.10.3-cp311-abi3-manylinux_2_34_x86_64.whl` | 3,612,043 | `cd35f255900ec122e5e2d47d1fbac658cc127d3f16ba82e643e199b7d8cebd75` |
| UMF Access 0.1.0.dev8 | [`umf-v0.5.8`](https://github.com/DocumentDrivenDX/umf/releases/tag/umf-v0.5.8), `efb4042719866888ad80c01cb2da694972eb2738` | Python wheel, `umf_access-0.1.0.dev8-py3-none-any.whl` | 580,062 | `1a21c1ddd59f6b23e9f0b33228280564af0e96bb4d13699b89616d5143e4fd23` |
| Weft 0.4.56 (`umf-weft`) | [`v0.4.56`](https://github.com/DocumentDrivenDX/weft/releases/tag/v0.4.56), `2f62e1148d1b2030558550e4f6f2ffe518bac62d` | macOS arm64, `umf_weft-0.4.56-cp39-abi3-macosx_11_0_arm64.whl` | 11,190,603 | `68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d` |
| Weft 0.4.56 (`umf-weft`) | same | Linux x86_64, `umf_weft-0.4.56-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl` | 14,358,901 | `32f4c38d9a614dd4c3a367f8b24d5cd4ba41ec80c2a4a2e3423e2082a294dbdd` |

The access wheel summary says `Target qualification pending`; the package is
selected as a published prerelease, not represented here as a fully qualified
runtime. Core's Linux wheel requires the `manylinux_2_34` platform baseline.
Only macOS arm64 and Linux x86_64 are selected for the native Core and Weft
distributions. Windows and Intel macOS are not selected.

## Current SDK release metadata — 2026-10-30

GitHub's immutable [`UMF SDK 0.5.8` release](https://github.com/DocumentDrivenDX/umf/releases/tag/umf-v0.5.8)
was published at `2026-10-30T00:15:07Z` from target
`efb4042719866888ad80c01cb2da694972eb2738`. Its release body identifies the
SDK dependency tuple as Access 0.1.0.dev8, Codegen 0.1.0a5, Core distribution
0.10.3 with semantic Core 0.10.0, and Weft 0.4.56. The Access wheel asset is
580,062 bytes with SHA-256
`1a21c1ddd59f6b23e9f0b33228280564af0e96bb4d13699b89616d5143e4fd23`, matching
the `umf-v0.5.7` asset exactly. Accordingly, `typed-sdk` now resolves Access
through the current SDK release URL while retaining the already checked bytes.
This updates the release coordinate; the prior installed Access checks remain
scoped to those identical bytes and were not repeated from the new URL. Ashlar
does not select the Codegen distribution, and SDK 0.5.8 is not an Access or Core
package version. The Core, Access and Weft package versions and the recorded
focused-check scope are unchanged; none of this adds native or managed-runtime
qualification.

## Focused check

The exact public Weft macOS arm64 release asset was installed into an isolated
Python 3.12.15 environment from the locally cached wheel after its SHA-256 and
size matched the GitHub asset record. The installed distribution is named
`umf-weft`; the Python import remains `weft`. Its `compile_paths_keys_json`,
`compile_engine_page_json`, `check_engine_page_json` and `check_engine_read_json`
APIs are present.

The original R02 Backend03 grouped request and Backend02 page/continuation
requests were replayed through the matching 0.4.56 compiler APIs. All three
responses were `compiled` and byte-equivalent as JSON values to the previously
retained 0.4.53 responses: Backend03 SHA-256
`be2e32908d9175c5206aa9a4a9c5383b8aa6eaee282b295bd9d9e890c5d4e7c9`, Backend02
page SHA-256 `879af9e863ef97016106a33ec95bb80a35095340cb1aa0cb2bfb5e68c2e3cdcb`,
and continuation SHA-256
`d96f63e9786f4b8d4e495708f9799edd8b9c7a66efbcbc88e0fee5eab89e98c4`. This
qualifies only compiler response compatibility for those three original
requests. Emitted SQL was not executed.

The Access dev8 and Core 0.10.3 macOS and Linux assets were hash/size checked
against public release metadata. A fresh isolated macOS ARM64 Python 3.12.15
environment installed the exact verified public Core 0.10.3, Access dev8 and
Weft 0.4.56 wheels with resolved Pydantic 2.14.2. Installed imports reported
those three selected versions, and the four focused
`test_managed_current_page_target` cases and twelve
`test_managed_current_core` cases passed. They exercised actual
Core/Access `EngineRead` admission of the compiler's managed `.4-xxhash64`
page target and the legacy target, plus wrong-target and carrier/hash refusals.
This qualifies that narrow installed Mac page-target path; it is not a complete
UMF typed-graph, native Delta, Linux, or managed runtime check. The earlier
offline package-index probe failed at DNS resolution; the later isolated
install resolved its Python dependencies successfully.

## Scope and follow-up

The dependency coordinates in `pyproject.toml` now use the exact public release
URLs and hashes. The managed page-target test resolves the renamed `umf-weft`
distribution. A separately versioned 0.4.56 runtime index and integrity/API
adapter now accepts only the verified macOS arm64 and Linux x86_64 release
wheel digests; the configured typed-page path accepts this adapter while the
historical 0.4.53 packet path remains unchanged. The exact 0.4.56 schema bytes
match the 0.4.53 schemas, and selected API/schema/index checks passed. This
remains effect-free package/API compatibility evidence, not Spark or native
execution qualification. The adapter refuses any preexisting `.pyc`/`.pyo`
or `__pycache__` under the indexed distribution before importing Weft, and
suppresses bytecode writes during that import; the index builder follows the
same cache-free import rule. A cache-free 13-file index/API/schema smoke passed;
a forged unindexed `.pyc` fixture was refused before `weft` entered
`sys.modules`. `configured_weft_query` still requires 0.4.53 for its retained
R02 precompiled gate, and `managed_current_campaign` still verifies its
separately versioned 0.4.50 Linux wheel/RECORD receipt; neither path was
silently rebound by this pin refresh. Truss 0.0.51 is not selected here; its
migration, runtime, and source/ACK gates remain open.
