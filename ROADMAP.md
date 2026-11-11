# Roadmap

Open work, not commitments. Support claims in the README match what CI tests.

## Next release (0.1.1)

- Publish `umf-ashlar` to PyPI with trusted publishing (`release.yml`).
- Move dependencies from exact GitHub-asset pins to ranges on released PyPI packages
  (`umf-core`, `umf-access`, `umf-weft`, `umf-truss`), require Python 3.11 or later, and
  add a stack-smoke job that installs those releases. Blocked on the upstream releases.
- Delete the remaining adapters for superseded Truss, UMF and Weft versions
  (`*_sdk058*`, `*_sdk010*`, `*_sdk0101*`, `weft_053/056_distribution`, `truss052/053_*`).
  Live configured-local code still imports them, so they need the profile dispatch in
  `configured_local_runtime` and `configured_local_delta4_publication` reduced first.

## Later

- Databricks / Unity Catalog: a periodic live check needs a dedicated workspace and secret;
  until then the managed path stays experimental and is only unit-tested with mocks.
- Fabric Graph GQL: run the shared graph corpus against a real tenant; currently only
  generated mappings and fakes are tested.
- Broader Weft query support beyond the documented subsets in the query gallery.
- Scale path: one measured full materialization of the 1B-node / 5B-edge workload. Not attempted.
