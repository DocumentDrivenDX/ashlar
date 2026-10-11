# Original-commerce local physical models

These six UMF physical variants define the exact local carriers of the admitted
original-commerce workflow. They preserve all ordered field names, Delta types,
metadata and nullable states from qualified E; all columns are nullable, with no
partitioning, clustering or explicitly configured table properties. Logical UMF0.8
commerce admission remains a separate model/version boundary.

The canonical managed runtime package is unchanged. Local `attempts` corresponds
to canonical `publication_attempt_phase`; local `manifest` corresponds to
`publication_manifest`. Differences in names, nullability and layout are explicit
in these variants. `e-correspondence.json` preserves independently retained native
version-zero metadata and original-log digests. UUIDs are observed runtime identity
and are never authored into a fresh definition.

Use the exact clean UMF revision in `index.json` with its admitted dependencies:

```sh
bun tools/generate_original_commerce_local_model.ts /path/to/pinned/umf --write
bun tools/generate_original_commerce_local_model.ts /path/to/pinned/umf --check
```

The public UMF generator owns schema interpretation and CREATE rendering.
`sql/ashlar-original-commerce-local/runtime-carriers.generated.json` retains its
complete `schemaJson`, layout and SQL outputs plus all input fingerprints. This
is a generated proposal; it creates no table and grants no source/native authority.
The installed local initializer must independently bind the selected generated
schema to a fresh Delta path under current admitted setup custody, refusing unknown
properties or metadata rather than discarding them. Managed-definition SQL does
not establish Unity Catalog or production compatibility for this local profile.
