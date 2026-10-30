---
ddx:
  id: ashlar.evidence.current-source-development-artifacts-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.release-closure-20261029
      kind: informed_by
    - id: ashlar.evidence.current-main-development-artifacts-20261029
      kind: supersedes
---

# Current-source development artifact build — 2026-10-29

## Result

One wheel and one source distribution were built from the committed package
inputs at the source commit below. Both report `umf-ashlar` 0.1.0.dev1 and
`Requires-Python: >=3.9`; both preserve the selected dependency URLs and
digests, all three declared license files, and all eight files in
`managed-current-core-010-xxhash64/`. The license and resource bytes match
the exported source inputs exactly.

This is an effect-free development-artifact check. It does not qualify a
stable release, optional-extra installation, native execution, or publication.
The earlier receipt remains historical evidence for its earlier package
inputs: [ashlar-current-main-development-artifacts-20261029.md](ashlar-current-main-development-artifacts-20261029.md).

## Source and attempted build

- Source commit exported: `94b42573e1bd07b8dceda5583b58d857cafb6bcd`.
  This is the requested package-input commit. A later docs-only successor,
  `8ca65c9a`, does not change those package inputs.
- Exported inputs: committed `pyproject.toml`, `README.md`, the three license
  files, and `src/`; no dirty worktree files were included.
- SHA-256 of the Git archive of those inputs:
  `b44b18c1bd99802fdbc2844757c1259ba9c9a6fab248ecd59e6e076f4aeadaaa`.
- `pyproject.toml` SHA-256:
  `c76093df1afc68ea0d8a2a2c5de511b45ef7dccd250fd7412d3c5ed698707224`.
- Build tool: uv `0.13.3`; the successful command was
  `uv build --offline --out-dir /private/tmp/ashlar-current-artifact-20261029/dist`
  from the exported source directory. It used the preexisting local uv cache;
  no dependency installation or network access occurred. The isolated Python
  build runtime and exact setuptools/wheel backend versions were not captured.
- Earlier sandboxed attempts failed before artifact creation: uv could not
  create a lock file under `/Users/erik/.cache/uv`, and a task-local empty
  cache lacked `setuptools>=77`. Access to the preexisting cache then allowed
  the offline build to complete.
- The source export is retained under
  `/private/tmp/ashlar-current-artifact-20261029/src/`.
- Wheel: `umf_ashlar-0.1.0.dev1-py3-none-any.whl`, 2,391,963 bytes,
  SHA-256 `44b0b888f1ac4915b3648f92aa1e96014196ddad5ad42b1cb9450e9d556890e0`.
- Source distribution: `umf_ashlar-0.1.0.dev1.tar.gz`, 2,007,929 bytes,
  SHA-256 `c4ea596ab8acbe0ae02d9f6783ed258fd27bf22dff90bb80ab796b038a2ff00e`.
- Both artifacts are retained in
  `/private/tmp/ashlar-current-artifact-20261029/dist/`.

## Package inspection

Wheel `METADATA` and sdist `PKG-INFO` agree on `Name: umf-ashlar`,
`Version: 0.1.0.dev1`, and `Requires-Python: >=3.9`. Both contain the exact
UMF Access `0.1.0.dev8` URL for UMF SDK release `0.5.8` and SHA-256
`1a21c1ddd59f6b23e9f0b33228280564af0e96bb4d13699b89616d5143e4fd23`. They
also retain UMF Core `0.10.3`, Weft `0.4.56`, and Truss `0.0.51` selections,
including platform-specific Core and Weft URLs and digests.

Each artifact contains `LICENSE.md`, `LICENSE-MIT`, and `LICENSE-APACHE`,
each byte-identical to the committed input. Each also contains all eight
selected resources byte-identical to source: `edge_current.umf.json`,
`index.json`, `object_current.umf.json`, `publication_attempt_phase.umf.json`,
`publication_manifest.umf.json`, `runtime-carriers.generated.json`,
`tombstone.umf.json`, and `whole_source_history.umf.json` from
`ashlar_host/resources/managed-current-core-010-xxhash64/`.

No optional dependency, tests, Spark, PostgreSQL, Databricks, publication,
tagging or upload was run. The successful artifact checks do not close the
release gates for installation, runtime execution, or stable publication.
