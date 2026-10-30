---
ddx:
  id: ashlar.evidence.current-main-development-artifacts-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.release-closure-20261029
      kind: informed_by
    - id: ashlar.toolkit-delivery-plan
      kind: informed_by
---

# Current-main development artifact check — 2026-10-29

## Result

The committed current-main package inputs built into a wheel and source
distribution for `umf-ashlar` 0.1.0.dev1. Both artifacts report the expected
name, version and `Requires-Python: >=3.9`; they contain all three declared
license files and all eight resources in the
`managed-current-core-010-xxhash64` profile. The selected UMF Core 0.10.3,
UMF Access 0.1.0.dev8, Weft 0.4.56 and Truss 0.0.51 URLs and hashes appear in
wheel metadata and the source distribution metadata.

This is an effect-free development-artifact check. It does not qualify a stable
release, optional-extra installation, native execution, or publication.

## Source and artifacts

- Source commit: `f038143957d68cb0dfb1f66f13b74e17410231d4`
  (`Prepare guarded R16 pre-migration custody prefix`), equal to `origin/main`
  at check time. The worktree had unrelated documentation/evidence changes;
  only committed `pyproject.toml`, `README.md`, license files and `src/` were
  exported for this build.
- SHA-256 of the Git archive of those exact package inputs:
  `7b53e8a1fe29279c1f1056627d9149881e3aefe392e3acc0d56ee9dd593cd802`.
- `pyproject.toml` SHA-256:
  `a7c3dee5e9382fc27a9ea31c8b0ee0d82263622dfb1f0110be93cea9debe2848`.
- Build tool: uv 0.13.3, CPython 3.13.16 on macOS arm64. The initial isolated
  build requirement lookup could not reach PyPI because DNS/network access was
  unavailable; the subsequent offline build succeeded from the local cache.
  Exact isolated setuptools and wheel versions were not retained.
- Wheel: `umf_ashlar-0.1.0.dev1-py3-none-any.whl`, 2,391,964 bytes,
  SHA-256 `97f13722bebc106f343df4ee3141d3879ac6ada472b96dcd8cf4f7b6046200a0`.
- Source distribution: `umf_ashlar-0.1.0.dev1.tar.gz`, 2,007,897 bytes,
  SHA-256 `23708cd77a840c2c15a830812068cad9305d68308ee55b2bf339566ca76c27f4`.
- Both artifacts are retained in
  `/private/tmp/ashlar-artifact-check-20261029/dist/`.

## Package inspection

The wheel and sdist metadata agree on `Name: umf-ashlar`,
`Version: 0.1.0.dev1`, and `Requires-Python: >=3.9`. Each contains `LICENSE.md`,
`LICENSE-MIT`, and `LICENSE-APACHE`, byte-identical to the committed inputs.
All eight files in `managed-current-core-010-xxhash64/` are present in both
artifacts and byte-identical to committed source.

Both artifact metadata records preserve these optional dependency selections:

| Extra | Selected coordinates |
|---|---|
| `typed-sdk` | UMF Core 0.10.3, UMF Access 0.1.0.dev8, Weft 0.4.56 |
| `truss-local` | Truss 0.0.51 with `[native]` |

The package-level `>=3.9` declaration describes the base distribution. The
selected `typed-sdk` wheels narrow its metadata-eligible runtime subset to
CPython 3.11 or later on macOS arm64 (macOS 11+) and Linux x86_64 (glibc
2.34+), because the UMF Core assets carry `cp311-abi3` and
`manylinux_2_34` tags. Weft's assets carry `cp39-abi3` and a lower Linux glibc
baseline, so Core sets the effective Python/platform floor for this extra.
These tags do not qualify execution on those runtimes. Windows, Intel macOS,
and other Linux architectures have no selected Core/Weft asset in this
candidate. `local-delta` still declares Spark 4.2.0 and Delta Spark 4.4.1;
this artifact check did not install or validate that extra. Truss's wheel is
`py3-none-any`, but the `[native]` extra and external PostgreSQL 18.6 tool were
not installed or exercised here.

No optional or native/cloud dependencies were installed, and no tests, Spark,
PostgreSQL, Databricks, publication, tag or upload were run. Exact optional
runtime and engine qualification remains subject to the release gates in the
[release-closure sequence](../release-closure-20261029.md).
