---
ddx:
  id: ashlar.domain-pack-umf08-supply-chain-dataset-20261024
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

# Original supply-chain finite dataset admission under UMF 0.8 (2026-10-24)

**Result:** the original supply-chain UMF 0.8.0 ontology and graph fixture passed
the pinned public compact dataset admission. This adds one distinct P08 semantic
representative for recursive typed relationships and explicit null/presence. The
fixture is synthetic; this does not establish supply-chain production data or
source authority.

The selection uses the versioned adapter
[`tools/check_domain_pack_datasets.ts`](../../../../../tools/check_domain_pack_datasets.ts)
with public producer source revision
`e44cd15f336dfb33db35acf20eee13dd120a1a28` and original pack source revision
`1f7b5f5d2a355c4b476e3a96b289b9048f03f567`. Both were checked out cleanly in
temporary directories under `/private/tmp`; Bun was 1.4.3. The exact focused
command was:

```sh
bun tools/check_domain_pack_datasets.ts \
  /private/tmp/ashlar-umf-e44-pack-selection \
  /private/tmp/ashlar-packs-1f7-pack-selection \
  /private/tmp/ashlar-supplychain-umf08-selection-20261024.json \
  --pack supply-chain
```

The selected result binds the original ontology SHA-256
`53ae68617d313548bf6c4b3c507c5fc33632c2e8527f88c310927652f635fe9a` and graph
SHA-256 `2c79e403545766dc7c221d6a6c8ff184ea852a8ecf8ef7f00e26c7f2ffc68e13`.
Public validation completed for all 20 record instances and 23 relationship
occurrences; the adapter independently checked every relationship's source,
target and occurrence identity. The selected original values retain two
`containers.parent_id` values as **present with null** (C1 and C3), rather than
converting them to absent fields. The relationship set includes the distinct
typed `containers.parent_id`, `containment.container_id`, and
`containment.item_id` relations. The duplicate-key and unresolved-target
negative controls were both refused.

The full bounded public receipt is [result.json](result.json), SHA-256
`2743f8afaa2acb08249fcfe4b3d1ec5dfea41fa101caa7a3e865d4bf1fbdcc04`.
Original pack/model/graph bytes were not modified. This is finite supplied-data
admission only: no canonical storage binding, source admission, publication,
Weft query, or native engine execution is claimed. The adapter's focused test
command passed 13 test cases (70 expectations) across the project test and one
retained evidence copy; no broad test or platform matrix was run.
