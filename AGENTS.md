# Working in ashlar

Ashlar is a property-graph toolkit on Delta tables. See `README.md` for what it is and
`docs/helix/` for the requirements, contracts and design (HELIX layout).

- UMF owns schema semantics and DDL generation; Weft owns query compilation; Truss owns its
  source boundary. Do not re-interpret their meaning in Ashlar, and do not repair compiler SQL
  in an adapter. Preserve declared schema versions and unknown extensions.
- Before a PR run `pytest -n 4 -m "not slow and not spark" tests`,
  `python tools/check_module_boundaries.py` and `python scripts/check_tree_size.py`.
- Never commit run output, receipts, packets or built bundles. Attach them to a PR or a CI
  artifact. Do not write evidence, qualification or handoff documents.
- A support claim in the README or microsite must be exercised by a CI test. Anything that
  needs a Databricks, Fabric or PuppyGraph service is experimental.
- Use local systems for native checks, never a shared default cluster.
