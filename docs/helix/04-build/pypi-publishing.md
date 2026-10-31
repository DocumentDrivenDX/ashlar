# PyPI publication boundary

`release-candidate.yml` is manually dispatched for an existing stable `vMAJOR.MINOR.PATCH` tag. It builds a draft GitHub release with a SHA256 manifest bound to the exact tag commit and `umf-ashlar` metadata. Tag pushes do not start Actions. Draft staging does not publish the package. An owner must satisfy the existing Ashlar release and native acceptance gates before publishing the GitHub release. `publish-pypi.yml` is also manually dispatched: it accepts only an existing published immutable release and verifies its tag, source commit, package metadata, exact asset set, and GitHub asset digests before OIDC upload. Publishing a GitHub release does not automatically start PyPI upload.

When Actions capacity is unavailable, `scripts/publish_github_release_local.py`
provides the same GitHub release staging and publication steps from a local
checkout. Start with `preflight`; it requires a clean checkout at the exact
current `origin/main` commit, confirms that the remote tag resolves to it,
checks the package version and GitHub repository, and reads GitHub's immutable
release policy, requiring `enabled: true`. It then builds and manifests the
wheel and sdist in a temporary directory. Use
`stage` to create and verify a draft release, and `publish` to verify that
draft's asset digests before making it public. `stage` prints a dry run unless
`--execute` is given. `publish` requires both `--execute` and an exact
`--confirm-publish <tag>` match. These commands call local `uv`, `git`, and
`gh`; they do not invoke Actions or upload to PyPI.

After an owner approves the exact reviewed stable commit and its release gates,
run `python scripts/publish_github_release_local.py preflight --tag vX.Y.Z
--expected-commit <full-commit> --repository OWNER/REPO`, then run `stage` with
the same arguments and `--execute`. Review the draft assets before running
`publish` with those arguments, `--execute`, and `--confirm-publish vX.Y.Z`.
This procedure does not authorize tagging, pushing tags, publishing the GitHub
release, or PyPI publication by itself.

Configure the PyPI Trusted Publisher for this repository and `.github/workflows/publish-pypi.yml` with environment `pypi`; enable immutable releases in GitHub repository settings. The workflow refuses uploads when the publisher identity or immutable release evidence is unavailable. A draft or an unqualified native gate cannot upload to PyPI.
