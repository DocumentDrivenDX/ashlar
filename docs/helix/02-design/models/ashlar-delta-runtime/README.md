# Ashlar runtime model in UMF

These eight UMF documents are the source for the current end-to-end runtime
table installation proposals. Each retains an exact `umf.delta` 0.1.0 schema
and an authored `umf.delta.definition` 0.1.0 physical definition. Generation is
implemented in UMF, not a second Ashlar SQL emitter.

The inventory covers six baseline tables plus `publication_attempt_phase` and
`whole_source_history`. Existing optional adjacency, degree, typed projection
and publisher coordination candidate tables are not yet captured here. Native
table UUIDs and observed protocols belong to runtime observations, not authored
fresh-table definitions. Predictive optimization is not disabled by this model.

`index.json` pins UMF source revision `fac1497a5ac5cb39bbaaac34eb0da8dc8b208c69`,
available on `origin/codex/delta-ddl-generator` in the UMF repository. That branch
has been pushed; it has not been merged to UMF main. A clean checkout at that
revision with its Bun dependencies is required to regenerate or check output.

From the Ashlar repository root:

```sh
bun tools/generate_delta_model.ts /path/to/pinned/umf --check
bun tools/generate_delta_model.ts /path/to/pinned/umf --write
```

Edit the UMF documents, review the semantic change, regenerate and run `--check`
before committing. The installer reads
`sql/ashlar-delta-v03/runtime-carriers.generated.json` and verifies the complete
input fingerprint inventory. This is checked-in code under repository trust,
not an admission mechanism for arbitrary caller-supplied SQL/model artifacts.
No generation command connects to Databricks or applies a migration.

The initial [capture comparison](capture-comparison.json) matches complete old
and generated DDL after stripping comments, whitespace and safe identifier
quoting. It covers column order/type/nullability, cluster keys and all property
literals. It is not a standing acceptance of future edits or new native execution
evidence. The comparison caught an initial extraction bug caused by a semicolon
inside a comment; that failed capture was corrected before adoption.

The UMF library passed eight focused Bun tests with 151 assertions, library
typechecking and one real Chromium 153 JSON/YAML generation/recovery/refusal
check. Ashlar's 173 small local tests pass, including stale-model and unexpected
input-inventory refusal. No new cloud DDL or benchmark was run for this conversion.

## Generated runtime ER diagram

The microsite now renders `website/static/model/runtime-er.svg` and expandable
physical field tables from these definitions. `relationships.umf.json` is a
separate authored UMF core 0.7.0 model: eight Records and 97 singular Fields,
with logical typed-identity Keys on current objects and edges, and two directed
edge-endpoint Relationships. Other role keys are not asserted by this initial
logical model. Core value families do not replace the retained native Delta
types. Only identity components assert ideal value availability; other Fields
remain unspecified rather than treating SQL nullable as ideal absence.

The diagram generator checks complete ordered Field membership and value-family
correspondence against the physical definitions. Logical keys and multiplicities
are requirements for publisher validation, not evidence of Delta FK or primary-key
enforcement. Physical endpoint-column correspondence is not yet a reusable UMF
binding claim; the two relationships identify source/target roles at Record level.
The retained authored page map covers broader optional projections separately.

```sh
bun tools/generate_runtime_diagram.ts /path/to/pinned/umf --write
bun tools/generate_runtime_diagram.ts /path/to/pinned/umf --check
python3 tools/check_runtime_diagram.py /path/to/pinned/umf
python3 website/scripts/check_runtime_model.py
bun tools/check_runtime_diagram_browser.ts /path/to/built/site /path/to/pinned/umf
```

The [integration receipt](diagram-integration.json) covers exact regeneration of
all current output plus four semantic refusal controls; prior output survives
refusal. [Chromium evidence](diagram-browser.json) covers desktop/mobile image
loading, all eight expandable roles, 17 object fields and page overflow/errors.
Visual inspection corrected one overlong cluster label and made field tables
scroll within their own regions on narrow screens. Strict Hugo build and the
existing seven-page link/navigation/seal checks pass. The 173 local runtime tests
also pass. No cloud run or benchmark is needed for this diagram iteration.

Generated template and asset bytes are outside the existing page-source signature
coverage. The seven signed Markdown sources and their claims are unchanged.
CI verifies input/output fingerprints before its regular sealed-source build;
that check detects staleness, not cryptographic provenance or native enforcement.
