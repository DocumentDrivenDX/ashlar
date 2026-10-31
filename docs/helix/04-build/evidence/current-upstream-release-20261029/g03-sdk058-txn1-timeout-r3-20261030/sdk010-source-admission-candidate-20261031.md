---
ddx:
  id: ashlar.evidence.g03-sdk010-source-admission-candidate-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-source-admission-gap-20261031
      kind: informed_by
    - id: ashlar.evidence.sdk010-delta4-runtime-integration-20261031
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# G03 SDK 0.10 typed-graph source-admission candidate — 2026-10-31

The isolated candidate checkout is
`/private/tmp/ashlar-g03-sdk010-source-admission-candidate-20261031`, based on
Ashlar commit `d0c2791fa0478784cb6cf6d6509f743c021ef362`. The reviewed candidate
is preserved as local branch commit `90df684f`; it has not been pushed or merged.
Its changed files are
`src/ashlar_host/configured_local_runtime.py`,
`src/ashlar_host/configured_source_local.py`,
`src/ashlar_host/typed_graph_sdk010_admission.py`,
`src/ashlar_host/resources/typed_graph_sdk010_source_admission.json`,
`src/ashlar_host/resources/typed_graph_values_sdk010.mjs`,
`tests/test_configured_local_runtime.py`, and
`tests/test_typed_graph_sdk010_admission.py`. The main checkout has no source
changes from it.

The separate opt-in binds the official SDK 0.10.0 archive SHA-256
`2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c` and public
entry SHA-256 `b32bce2fdcf928bc5b9fd8376b4c7cab9beb1b63d90cb1d20b9ad0ec94595951`.
It checks the installed package entry and `package.json` against those exact
archive members, plus the pinned Node executable and checker/manifest resources.
The public entry parsed as an ESM module with no dependency specifiers. The
real-SDK test ran the checker against the frozen G03 model/request and obtained
a three-group receipt that differs from the historical fixture with only its
producer revision relabelled. It also verifies Core record and complete compact
dataset correspondence through the public SDK APIs.

The candidate preserves SDK058 as a separate option. SDK010 admission is bound
to the configured-local `.4` initialization profile; transaction 1 now selects
the SDK010 source manager only for retained installation profile `.3`, and
refuses a missing or mismatched SDK/source profile before source or Spark
opening. SDK058 profile `.2` continues to require SDK058 admission. Source open
and close perform full correspondence checks; inner renewals verify exact
source/request and SDK custody without another Node run. Focused tests cover
profile routing, missing admission refusal, source-view unwrapping, exact
renewal custody, and propagation of a failing final SDK check. These address
Sol's three reviewed boundary checks for cohort selection, source-manager
dispatch, and coordinator source unwrapping; the close-failure check is covered
separately.

The effect-free command passed 27 focused tests in 10.577 seconds:

```sh
PYTHONPATH=src:tests /private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -m unittest \
  test_configured_local_runtime test_typed_graph_sdk010_admission test_typed_graph_sdk058_admission
```

Sol independently reran the real SDK correspondence, final-close refusal, and
transaction-1 source-selection tests (3 tests passed in 10.708 seconds).

No Spark session, native Delta operation, cloud resource, publication, manifest,
source ACK or Weft query ran. The candidate is not on main and reports no
transaction-1 or consumer result. Its admission manifest is pinned to the frozen
G03 typed-graph configuration, source, binding and model; it does not qualify
other source profiles or repair the historical producer's authority. The
separate SDK010-native attempt and Weft 0.4.56 consumer gates remain outstanding.
