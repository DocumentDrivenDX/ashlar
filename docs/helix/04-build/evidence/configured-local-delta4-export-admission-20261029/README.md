---
ddx:
  id: ashlar.configured-local-delta4-export-admission-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: TD-008
      kind: informed_by
---

# Configured local graph export profile admission

The configured-local immutable graph export now takes its carrier profile from
the already admitted publication descriptor and passes that exact profile to
both the publication resolver and the independent source-carrier comparison.
It accepts only `ashlar-delta/0.3` and `ashlar-delta/0.4-xxhash64`; it does not
silently reinterpret an unknown profile as `.3`. The shared `.4` export reader
recomputes Spark SQL xxhash64 over the typed source/type/ID tuple and retains
the complete canonical carrier rows, reversible node and edge IDs, independent
parallel-edge identities, and the resolver's complete publication vector.

Focused verification on 2026-10-29:

- `PYTHONPATH=src:src/ashlar_host:tests ./.venv/bin/python -m unittest test_configured_local_graph_release test_graph_release` — 13 tests passed. This includes configured `.3` source/vector comparison, shared `.4` xxhash64 export/admission controls, and an inert controlled-scope test that verifies `.4` forwarding, `.4` source-carrier validation, and unknown-profile refusal before the reader call.
- No `.4` configured-local publication was read or exported. The retained current `.4` evidence is initialization only, so no actual `.4` full-vector publication export is claimed.
- No GraphFrames or PuppyGraph engine was activated. Export preparation does not establish engine mapping/query behavior, external engine availability, managed Unity Catalog, production authority, or scale qualification. Full 1B/5B work remains held by the release closure.

The failed initial test invocation used the system `python` command, which is
not installed in this shell. The successful run used the repository virtual
environment and explicit source/test import paths.
