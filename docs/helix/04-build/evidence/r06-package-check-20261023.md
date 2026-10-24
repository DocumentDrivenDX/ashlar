---
ddx:
  id: ashlar.evidence.r06-package-check-20261023
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: R06
      kind: informed_by
---

# R06 packaging check: bounded offline result

Checked exact commit `34e97b5df2b0383567648b9598f50dab01381078` from a
`git archive` snapshot, outside the working checkout. The snapshot contains no
uncommitted files. Build host: the local macOS arm64 CPython 3.12.14 environment
with setuptools 84.1.0 and wheel 0.48.0; the build fetched no dependencies and
used no network.

The wheel build succeeded using:

```text
python -m pip wheel --no-deps --no-build-isolation --wheel-dir <dist> <snapshot>
```

Artifact: `ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl`, 2,362,066 bytes,
SHA-256 `bf38d9de476c533b95e814db80948b05629ec637f353f70e816035f6282b8ca5`.
Metadata reports `ashlar-graph-toolkit` 0.1.0.dev1, Python `>=3.9`,
`MIT OR Apache-2.0`, and the declared optional extras. All three license files
are present. The wheel contains all six local-current Delta 4 UMF bridge
resources, and the generated source/index plus six role models for each of the
managed-current 0.3 and 0.4 carrier profiles.

A new venv was created outside the checkout. Its bundled `ensurepip` pip import
was incomplete, so the build venv's pip was used with `--python` to target it;
that process copied package files but was stopped at the time bound before pip
reported success or created the `ashlar` console script. In the target venv,
`ashlar` and `ashlar_host` imported from site-packages, `python -m ashlar --help`
returned help, `ashlar_host.local_current_delta4_umf._pinned_resources()`
verified the packaged UMF bridge, and `load_current_managed_carriers()` verified both 0.3 and
0.4-xxhash64 resource sets. Treat this as installed-code/import/resource
evidence, not a completed installer or console-entrypoint check.

The sdist was not qualified. The generated default source list had 1,193
entries: 620 test files, 568 source files, four project metadata/license files,
and no `docs/` entries. Two bounded sdist attempts remained in file-copy stages
and were stopped; no sdist artifact or hash was produced. A temporary
`MANIFEST.in` experiment pruning tests and non-package directories was removed
without retaining a packaging change; the benefit of that pruning was not
established. There is no evidence here of an sdist content defect.

This is one local macOS wheel build and a partial isolated wheel check only. It
does not qualify sdist creation, console-script installation, optional extras,
other platforms, or native Spark, Delta, PostgreSQL, Unity Catalog, cloud,
publication or ACK behavior. No commit or push was made.
