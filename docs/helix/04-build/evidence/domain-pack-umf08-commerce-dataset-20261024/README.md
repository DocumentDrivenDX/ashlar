---
ddx:
  id: ashlar.domain-pack-umf08-commerce-dataset-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.toolkit-delivery-plan
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
---

# Original commerce finite dataset admission under UMF 0.8 (2026-10-24)

**Result:** the original UMF 0.8.0 commerce ontology and graph fixture passed
the pinned public UMF finite-dataset admission. This is one offline P08
representative, kept at its declared version. It does not qualify the current
UMF 0.10 profile, source authority, canonical storage binding, publication,
Weft execution, or a graph engine.

The run used the existing versioned adapter
[`tools/check_commerce_dataset.ts`](../../../../../tools/check_commerce_dataset.ts)
against a clean temporary checkout of public UMF revision
`c7c95e1c4ea5b72541f47fa0350ca467ff02f395` (2026-10-09). The checkout was
created under `/private/tmp` from the existing local UMF Git object database;
its node_modules directory symlinked to the local UMF dependency install. Bun
was 1.4.3. The exact command was:

```sh
bun tools/check_commerce_dataset.ts \
  /private/tmp/ashlar-umf-c7-domainpack-check \
  /private/tmp/ashlar-commerce-umf08-admission-20261024.json
```

The adapter verified the original ontology SHA-256
`51d87c554df36846e41378cfaafc81277fcab61f6c891a6c64f34dba8fd9ac2a` and graph
fixture SHA-256 `8052dbcf2cf48a5e344c492d661872bc0a3f80a5c7be033ab5008dea9c86119e`,
then called UMF's `readDocument`, `readJsonValue`,
`validateCoreDatasetValues`, and `verifyCoreDatasetValues` on the original
lexical carriers. Result: 11 records, 11 keys, 10 relationship occurrences,
with exact source and target occurrence identities. The duplicate-key,
unresolved-target, and missing-required-relationship controls were all refused.

The full bounded public receipt is [result.json](result.json), SHA-256
`76a9bb3a03bb533f8fae2da5b4e700f4d8f3496b4cbfebb1a9b7ceb2145a7d16`.
The checked-in adapter documents the exact UMF revision and the original model
and graph hashes; reproducing it requires that clean UMF source checkout and a
fresh output path. Original pack bytes were not modified.

This closes only the declared-version original commerce finite-dataset
admission slice. The separate current-core 0.10 profile remains governed by
its own source and runtime evidence. No UMF 0.8 document was relabeled or
interpreted by the UMF 0.10 validator.
