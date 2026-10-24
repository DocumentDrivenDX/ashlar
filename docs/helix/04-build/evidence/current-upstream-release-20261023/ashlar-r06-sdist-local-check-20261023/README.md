# Ashlar R06 local sdist check

## Result

The compact snapshot from selected source commit `a37a9ddb17af2a5a607011feb9a1d9f0bbc288eb`
produced an sdist successfully with CPython 3.12.14, setuptools 84.1.0 and wheel
0.48.0. A no-index, no-dependency target install and import/resource smoke also
succeeded. The sdist SHA-256 is
`4c4ee38b6ed83444390dde5ec8a4360db6ff92234d24dd3d44fdf18511e18b41`; its full
build, archive and smoke details and receipts follow below. The earlier
interrupted and pre-backend setup attempts are retained as history and did not
produce this result.

## Existing R06 wheel inspection

The scratch wheel
`/private/tmp/ashlar-r06-packaging-34e97b5d/dist/ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl`
has SHA-256 `bf38d9de476c533b95e814db80948b05629ec637f353f70e816035f6282b8ca5`.
Its archive has 570 entries: 562 package code entries and 67 resource entries.
It includes `LICENSE.md`, `LICENSE-MIT`, and `LICENSE-APACHE` in distribution
metadata. Metadata reports `ashlar-graph-toolkit` version `0.1.0.dev1`.
This is a wheel-only archive inspection; its source revision was not established
from the retained scratch directory, so it is not attributed to the current
checkout.

## Scope and provenance

The checkout was at source revision
`92be14be6390bf67cb49f17941baf189b372cbab` (`git status` showed only unrelated
untracked R05 evidence). No files under `src/` or `pyproject.toml` were changed.
No import from a fresh sdist install was attempted, so import origin and runtime
resource access remain unverified for the sdist.

The retained interrupted log is
`/private/tmp/ashlar-r06-packaging-34e97b5d/sdist-build-postfix.log`; its final
status is `interrupted` during `Creating tar archive`. The configured build
requirements are in the checkout's `pyproject.toml`.

## Follow-up on current checkout (2026-10-24)

Current checkout HEAD is `3ce9c88b7b128550491387d6e4b4a3946168cc4f`; its
`pyproject.toml` is byte-identical to the R06 checked commit. An offline
`uv build --sdist` was attempted with a task-local cache and stopped before
the backend ran because that cache had no `setuptools>=77` or `wheel`. The
shared uv cache could not be used because uv could not create its lock file
there. No sdist was produced.

The retained earlier interrupted log shows setuptools had copied package
modules and resource files into the temporary source tree, including the
local-current Delta 4 and managed carrier resources. That is not a completed
archive or an sdist-install check. Inspection of the current `pyproject.toml`
and the prior wheel/resource inventory found no specific missing sdist
declaration or other concrete packaging defect. No source or packaging
configuration change is justified by this evidence. Sdist inclusion and
installed resource access remain unqualified pending a runnable build backend.

## Follow-up: committed snapshot preparation (2026-10-24)

The first preparation path failed before extraction with the exact host-Python
diagnostic `TypeError: extractall() got an unexpected keyword argument 'filter'`.
After clarification, the captured git archive was extracted with system `tar`
into `/private/tmp/ashlar-r06-a37a9ddb-5dwcmhqx/source`. Its root snapshot was
verified against HEAD `a37a9ddb17af2a5a607011feb9a1d9f0bbc288eb`; the retained
`source.tar` is 711,219,200 bytes and the extracted tree is 703 MiB.

The combined preparation/build wrapper produced no result JSON or stdout/stderr
log. Its `dist/` is empty, and inspection found no setuptools `build/` or
`.egg-info` directories. The parent process inventory later showed no matching
tar, `build_sdist`, or selected Python process. Backend start, exit status,
timeout status, and backend diagnostic could not be established from retained
evidence. This large archive preparation produced no sdist or install smoke; the
later compact snapshot check below supplies the completed result.

## Follow-up: compact committed snapshot and attempt history (2026-10-24)

A compact Git archive was made from exact HEAD `a37a9ddb17af2a5a607011feb9a1d9f0bbc288eb`, selecting only `pyproject.toml`, `README.md`, the three license files, `src/`, and `tests/`. The 16,936,960-byte archive SHA-256 is `a8c70bb2cd718a94ccec7a62d46eb0b0c24f0b5d18d451b2c4fae51d5ebf4caa`. Its 1,245 files matched the selected Git inventory and every extracted file's Git blob ID. The `src/` tree is `0b1a7f94bede1dc2993f31d74cfff41ec8fbd236`; `tests/` is `b4db04bfdcd3a03308917dd97b9a75849db221c0`. Snapshot receipt and files remain under `/private/tmp/ashlar-r06-compact-a37a9ddb-z9sh1kpf/`.

An initial launch setup failed before the backend process was created: the
`os.nice(10)` pre-exec hook raised
`subprocess.SubprocessError: Exception occurred in preexec_fn.` This was a
pre-backend setup failure. After removing that hook, exactly one actual backend
call ran; its result is recorded below. No source/config files changed.

## Completed compact sdist and isolated smoke (2026-10-24)

From the verified compact archive above, one offline
`setuptools.build_meta.build_sdist` call completed on CPython 3.12.14 with
setuptools 84.1.0 and wheel 0.48.0. It exited 0 in 89.779 seconds. The sdist
`ashlar_graph_toolkit-0.1.0.dev1.tar.gz` is 3,215,041 bytes with SHA-256
`4c4ee38b6ed83444390dde5ec8a4360db6ff92234d24dd3d44fdf18511e18b41`. Its 1,195
files include `PKG-INFO`, all three license files, README and project metadata;
metadata identifies `ashlar-graph-toolkit` 0.1.0.dev1, `MIT OR Apache-2.0`, and
Python `>=3.9`. All 67 resource files from the selected source snapshot are
present, including `source-correspondence.json`, all six local-current Delta 4
bridge resources, and all six role models in each managed-current carrier
profile. No resource files were missing.

A no-dependency, no-build-isolation, no-index install from that sdist into a
fresh target directory completed successfully in the same bounded 120-second
install/import/resource window (82.515 seconds). Imports resolved from the
target directory; `_pinned_resources()` succeeded, and the managed-current 0.3
and 0.4-xxhash64 loaders both succeeded. The machine-readable receipts are [snapshot verification](snapshot-verification.json),
[build result](build-result.json), [archive inspection](archive-inspection.json),
and [install/import/resource smoke](install-smoke-result.json). Raw logs and
artifacts remain under `/private/tmp/ashlar-r06-compact-a37a9ddb-z9sh1kpf/`.
This qualifies this local macOS packaging path only; it does not qualify other
platforms, optional dependencies, or native Spark/Delta behavior. No source or
configuration file changed during this check.
