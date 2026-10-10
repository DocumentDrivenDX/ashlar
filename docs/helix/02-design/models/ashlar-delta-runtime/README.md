# Ashlar runtime model in UMF

The runtime table package uses UMF as its authored model and DDL generator under
[CONTRACT-003](../../contracts/CONTRACT-003-delta-graph-tables.md). Preserve exact
native columns, types, order, nullability, clustering and property literals through
`umf.delta.definition` 0.1.0 and `umf.delta` 0.1.0. Native table UUIDs and observed
protocols are runtime observations rather than authored fresh-table definitions.
Keep predictive optimization configuration intact.

[index.json](index.json) defines the package's table inventory and pins the UMF
generator revision. Use that exact revision with its locked dependencies;
a branch name or a newer checkout is not a substitute for the pin. Optional
adjacency, degree, typed projections and publisher coordination layouts require
separate UMF coverage before this package can claim them.

From the Ashlar repository root:

```sh
bun tools/generate_delta_model.ts /path/to/pinned/umf --check
bun tools/generate_delta_model.ts /path/to/pinned/umf --write
```

Edit the authored documents, review the semantic change, regenerate and run
`--check` before committing. Installation consumes
`sql/ashlar-delta-v03/runtime-carriers.generated.json` and verifies its complete
input fingerprint inventory. Repository-trusted generated proposals do not admit
arbitrary caller-supplied SQL or models. Generation applies no native migration;
source/catalog acceptance, retention and publication remain separate boundaries.

## Model-derived diagram and schema browser

The microsite consumes the authored model for the runtime ER diagram and complete
physical field tables. [relationships.umf.json](relationships.umf.json) supplies
logical Records, ordered Fields, typed identity Keys and directed endpoint roles.
Logical families must preserve their correspondence to native Delta types rather
than replace those types. Preserve authored availability: SQL nullability alone
does not establish ideal absence. Keys and multiplicities state publisher
validation requirements, not Delta primary-key or foreign-key enforcement.
A reusable physical endpoint binding requires explicit column correspondence.
Identify the broader authored conceptual map separately from this derived model.

```sh
bun tools/generate_runtime_diagram.ts /path/to/pinned/umf --write
bun tools/generate_runtime_diagram.ts /path/to/pinned/umf --check
python3 tools/check_runtime_diagram.py /path/to/pinned/umf
python3 website/scripts/check_runtime_model.py
bun tools/check_runtime_diagram_browser.ts /path/to/built/site /path/to/pinned/umf
```

Check complete ordered membership, native/value-family correspondence and exact
input/output fingerprints. Semantic refusal must leave prior output intact.
Browser checks cover model roles, complete fields, image loading, navigation and
narrow-screen overflow. Keep the embedded schema browser pinned to its qualified
UMF component and use its public rendering boundary. Generated fingerprints detect
staleness; they do not establish cryptographic provenance, native constraint
enforcement or coverage by a page-source signature.

Historical capture, generation and browser observations are preserved in the
[documentation custody record](../../../04-build/evidence/runtime-model-documentation-20261009/custody.json)
and its exact archived source. Each future support claim requires its own scoped
receipt; historical comparisons do not approve later edits or native execution.
