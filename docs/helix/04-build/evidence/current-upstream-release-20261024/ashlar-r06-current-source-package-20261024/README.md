---
ddx:
  id: ashlar.evidence.r06-current-source-package-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss050-package-cohort-qualification-20261024
      kind: informed_by
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
---

# R06 current-source package check

From a clean Git archive of Ashlar `96b0bf40b08305dd5a48ff08a6bce383d8327a20`, CPython 3.12.14, setuptools 84.1.0 and wheel 0.48.0 built a wheel and sdist in 17.675 seconds without network access. Both artifacts identify `ashlar-graph-toolkit` 0.1.0.dev1, declare Python `>=3.9` and `MIT OR Apache-2.0`, carry the three license texts, expose `ashlar = ashlar.cli:main`, and contain the exact Truss 0.0.50 hash pin. The wheel has 571 RECORD payloads verified with no errors. Both archives include all 67 tracked runtime resource files, including current `.4-xxhash64` and managed carrier profiles.

Each artifact was installed separately outside the checkout using pip with `--no-deps --no-build-isolation --no-index --target`. Imports resolved from each target; the packaged local Delta4 resources and managed `.3`/`.4-xxhash64` loaders worked; and the installed `ashlar --help` entrypoint succeeded. Exact hashes, archive inspection, source snapshot and command receipts are in [`package-audit.json`](package-audit.json) and [`install-smoke-summary.json`](install-smoke-summary.json).

This focused check does not install or resolve optional extras. The install was deliberately offline and excluded PySpark/Delta and other native runtime dependencies. It also does not qualify other operating systems, runtime behavior, or production support. Rebuild from the reviewed release candidate after any further source or dependency-pin changes.
