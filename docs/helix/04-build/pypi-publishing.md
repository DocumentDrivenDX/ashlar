# PyPI publication boundary

`release-candidate.yml` builds only stable `vMAJOR.MINOR.PATCH` tags and stages a draft GitHub release with a SHA256 manifest bound to the exact source commit and `umf-ashlar` metadata. Draft staging does not publish the package. An owner must satisfy the existing Ashlar release and native acceptance gates before publishing the GitHub release. `publish-pypi.yml` accepts only an existing published immutable release and verifies its tag, source commit, package metadata, exact asset set, and GitHub asset digests before OIDC upload. Its manual backfill has the same checks.

Configure the PyPI Trusted Publisher for this repository and `.github/workflows/publish-pypi.yml` with environment `pypi`; enable immutable releases in GitHub repository settings. The workflow refuses uploads when the publisher identity or immutable release evidence is unavailable. A draft or an unqualified native gate cannot upload to PyPI.
