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

The microsite ER diagram remains a follow-up. It must derive fields from this
model and use explicit authored logical references, without claiming Delta FK
enforcement. Existing manually authored website diagrams are not UMF-generated.
