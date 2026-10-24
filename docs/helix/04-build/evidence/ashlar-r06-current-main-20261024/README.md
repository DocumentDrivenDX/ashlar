---
ddx:
  id: ashlar.evidence.r06-current-main-package-check-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.toolkit-delivery-plan
      kind: informed_by
---

# R06 current-main package check

## Result

The wheel and source distribution built successfully from the clean committed
snapshot at `25e7b3d9edfe95a5483956016bac9b658fbefe6b`, equal to `origin/main`
at the start of this check. A fresh CPython 3.12.14 environment installed the
wheel offline outside the checkout. Package imports resolved from that
environment, and selected packaged resources plus both managed carrier
profiles loaded successfully. No concrete packaging defect was found, so no
source or packaging configuration change was made.

This is a bounded local macOS arm64 package check. It does not complete R06 or
the release handoff: current Truss, R05 and Unity Catalog gates remain open.
It does not qualify other platforms, optional dependency sets, Spark/Delta
execution, or native behavior.

## Build and artifact identity

Host: macOS 27.0.1 arm64; CPython 3.12.14; setuptools 84.1.0; wheel 0.48.0.
Both build calls used the installed backend directly, with no build isolation
or dependency installation. The source snapshot included `pyproject.toml`,
`README.md`, the three license files, `src/` and `tests/`; it excluded working
tree changes and documentation. The snapshot and build outputs were kept under
`/private/tmp/ashlar-r06-current-ialnNf/`.

The snapshot command was:

```sh
git archive --format=tar HEAD pyproject.toml README.md LICENSE.md LICENSE-MIT LICENSE-APACHE src tests | tar -xf - -C "$BASE/source"
```

From the extracted snapshot, one `setuptools.build_meta.build_wheel(dist)` call
completed in 3.216 seconds and one `setuptools.build_meta.build_sdist(dist)`
call completed in 2.947 seconds.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| `ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl` | 2,373,668 | `dbd9ee1a81bca0d94f9013ea5c8962a722a77a6018a785a301a756d456d7e0a6` |
| `ashlar_graph_toolkit-0.1.0.dev1.tar.gz` | 3,244,611 | `8f23dd8d146a2d28967040565f44744f3b2e64380cb4cad2f87d67a5eba8c621` |

## Installed smoke

A fresh venv was created under the private temporary directory. The wheel was
installed without index access or dependencies:

```sh
"$BASE/venv/bin/python" -m pip install --no-index --no-deps "$BASE/dist/ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl"
```

Imports of `ashlar` and `ashlar_host` resolved from the venv's
`site-packages`, outside the checkout. `importlib.resources` read
`source-correspondence.json`, `resources/graph.json` and
`resources/ontology.json`. The local Delta 4 `_pinned_resources()` loader
returned four resources, and `load_current_managed_carriers()` loaded both
`ashlar-delta/0.3` and `ashlar-delta/0.4-xxhash64` profiles. The install
completed successfully; its exact elapsed time was not captured.

No source code, `pyproject.toml`, dependency pins, native systems, cloud
services, or shared checkout main state were changed by the build and smoke.
