---
ddx:
  id: ashlar.evidence.umf010-upstream-adoption-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.r01-upstream-pin-refresh-20261029
      kind: informed_by
    - id: ashlar.evidence.g03-sdk058-delta4-realization-candidate-20261030
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# UMF release adoption review — 2026-10-31

Status: **Core pin updated; focused local checks passed; native runtime remains unqualified.** The
`typed-sdk` extra now selects Python Core distribution 0.10.4 for macOS arm64
and Linux x86_64. The exact macOS wheel was downloaded and its SHA-256 matched
the release digest. It installed in a disposable Python 3.14.8 environment;
its METADATA reports version 0.10.4, `Requires-Python: >=3.11,<3.15`, and
Pydantic `>=2.11,<3`; its RECORD contains 42 rows. `umf.__version__` and installed
distribution metadata both reported 0.10.4. Selected Ashlar imports passed for
`ashlar.access`, `ashlar.async_access`, `ashlar.schema`, `ashlar.binding`, and
`ashlar.schema_registry` with the selected UMF Access dev8 and Weft 0.4.56
wheels; `pip check` reported no broken requirements. The Access dev8 and Weft
0.4.56 wheel digests also matched their existing pins. This is one macOS arm64
environment only ([runtime receipt](outputs/core-import-check.json),
[wheel metadata and digests](outputs/wheel-metadata-and-compatibility.json)).

The reviewed UMF source commit is
[`96230e0eef44b255d33575ba496402555b76800e`](https://github.com/DocumentDrivenDX/umf/commit/96230e0eef44b255d33575ba496402555b76800e).
Its `spec/release.json` declares SDK version 0.10.0, semantic Core version
0.10.0, and Python distribution version 0.10.4. These version numbers describe
separate surfaces. Ashlar's source documents and semantic version pins remain
Core 0.10.0.

The Python 0.10.4 source adds the `SagaRuntime` facade and saga package resource
to the shared code-generation runtime. The inspected Ashlar typed-access paths
do not call those APIs. Core 0.10.4 declares Python `>=3.11,<3.15`; its
`cp311-abi3` wheel tags match the existing supported Python 3.11+ selection.
UMF Access dev8 has no Core package dependency in its release `pyproject.toml`,
so the pin change does not conflict with its declared dependencies. Weft
0.4.56 and Truss 0.0.51 selections are unchanged.

The exact SDK 0.10.0 tarball was downloaded and its SHA-256 matched the release
digest `2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c`.
Its public package entry exports `readDocument` and `realizeSparkDelta4DDL`, and
both functions ran against the six frozen Delta 4 carrier inputs. All six
proposals exactly matched the retained SDK 0.5.8 proposal objects. The tarball
does not include `src/model/document.ts` or `src/adapters/delta/ddl.ts`; Bun also
refuses to package its public ESM entry as a browser IIFE because that entry
contains top-level `await init_src()`.

A new candidate bridge was built from the SDK 0.10.0 tag source at commit
`96230e0eef44b255d33575ba496402555b76800e`, using those two source modules and
Bun 1.4.3. Its browser IIFE output matched the same six frozen proposal objects
exactly. The proposed identity is
`ashlar-local-current-delta4-umf-source-sdk010/0.1`; the bundle and qualification
receipt are preserved in the [versioned bridge candidate](sdk010-delta4-bridge-candidate/README.md).
This candidate does not change or replace the existing SDK 0.5.8 runtime
resource or its historical receipts. It has not been installed in MiniRacer,
integrated into Ashlar's runtime, or used for Spark, Delta, publication, or
managed execution.

The macOS Core release wheel was downloaded and verified. The Linux asset digest
below is the supplied release digest; that asset was not downloaded in this
focused check.

| Platform | Asset | SHA-256 |
|---|---|---|
| macOS arm64 | `umf_core-0.10.4-cp311-abi3-macosx_11_0_arm64.whl` | `a3a4a68fcc4dfb637d7e9229ac7f6cbf029e464442304e6e49f8fc0c2467845e` |
| Linux x86_64 | `umf_core-0.10.4-cp311-abi3-manylinux_2_34_x86_64.whl` | `5f73e20aa2f5d5002bf33706eda7fba4dd37789c55ca912c7de0c1557b40209c` |

Core 0.10.4 passed selected import checks with Access dev8 and Weft 0.4.56 in
the single local environment above. This does not qualify Linux execution,
typed-graph behavior, native publication, or managed execution. The prior
[0.10.3 adoption evidence](../current-upstream-release-20261029/r01-umf-weft-pins-20261029/README.md)
remains the result record for that earlier cohort.

Exact Truss 0.0.51 wheel METADATA requires Python `>=3.11`, jsonschema 4.26.0,
referencing 0.37.0, and mini-racer 0.14.1; its `[native]` extra adds psycopg
3.3.6. It declares no Core dependency. The separate `[typed-sdk]` extra pins
Access dev5, but Ashlar selects Truss `[native]` and Access dev8 independently.
Weft 0.4.56 requires Python `>=3.9` and declares no runtime dependencies. These
published constraints permit a focused Core 0.10.4 local install with the
selected Access and Weft distributions. The R18 packet still binds its prior
Delta 4 realization profile and bridge/resource hashes, so this dependency
compatibility does not update or admit the R18 packet; it remains unchanged.
