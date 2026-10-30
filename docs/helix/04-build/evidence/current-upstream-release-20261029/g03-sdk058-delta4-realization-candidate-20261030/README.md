---
ddx:
  id: ashlar.evidence.g03-sdk058-delta4-realization-candidate-20261030
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.r01-upstream-pin-refresh-20261029
      kind: informed_by
    - id: ashlar.evidence.g03-weft056-delta4-consumer-candidate-20261029
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# G03 SDK 0.5.8 Delta 4 realization candidate — 2026-10-30

Status: **effect-free candidate only**. The selected UMF SDK 0.5.8 package's
public APIs reproduced the frozen typed-graph Core result objects and all six
current Delta 4 carrier proposals. This does not qualify the release producer
for the complete typed-graph source contract, Spark, native Delta execution,
publication, managed UC, or ACK.

## Typed-graph comparison boundary

The candidate builds the frozen request from the copied G03 model, binding,
configured source and eight-event JSONL input. SDK 0.5.8's public
`validateCoreRecordValues`, `validateCoreDatasetValuesCompact` and
`verifyCoreDatasetValuesCompact` results were compared with the matching
results in the historical `typed_graph_values.ts` receipt from producer
revision `2ede4c879bea5db13794a6fa30e67c821306c17f`. All three request groups
match exactly at those result objects, including `typed-graph-1`'s four record
results and compact dataset result. The old receipt digest and comparison are
captured in `outputs/qualification.json`.

This is not reproduction of the complete old receipt by SDK 0.5.8. Its
event-to-source correspondence checks—raw event bytes, edge shape, transaction
cursor and digest checks—were not rerun through a release-SDK-backed checker;
they remain represented only by the frozen inputs and historical receipt. The
next narrow check is to port the Ashlar-owned correspondence logic to the
selected SDK 0.5.8 path and compare those receipt fields. Python Core 0.10.3
and Access 0.1.0.dev8 were not used in this candidate because the exercised
public APIs are in the SDK's TypeScript package.

## Delta 4 proposal comparison

The SDK 0.5.8 `realizeSparkDelta4DDL` output was compared with the existing
local `.4` bridge for `object_current`, `edge_current`, `tombstone`,
`whole_source_history`, `publication_manifest` and
`publication_attempt_phase`. The full proposals, schema field ordering and
SQL matched for all six roles. Per-role source, schema and SQL digests are in
`outputs/carrier-proposals.json`; no CREATE statement was executed.

The release pin is UMF SDK 0.5.8 tag target
`efb4042719866888ad80c01cb2da694972eb2738`, per the R01 check. The consumed
package was the locally retained distribution archive
`2aaf24741aee1c135fade5b7631db90d710e6211977fb9ca7fcc080ea7fdf58d` (30,834,843
bytes), produced from source commit
`855f6c48649496fc60b3303798790d0ddb55d38f`. Its installed public entry
`dist/umf.js` is 13,864,963 bytes with SHA-256
`071544fcc83c5abcf1e280890e1516613f679a58cf245b31a7bf16f0b6dd24fd`. The
Node executable was v26.12.1, 132,848 bytes, SHA-256
`9af7e24478e538455cf4af53a2607f1b79304bd5e25458b041fb855c3c167991`.
`outputs/closure-receipt.json` pins these bytes, the 0.5.8 producer custody,
frozen inputs, candidate tools and generated outputs.
Before importing the SDK or writing output, the candidate checks the retained
closure status, producer commit/tree, archive/closure/producer-receipt hashes,
installed tree and entry/package version, and existing bridge hash. Focused
negative controls confirmed that an altered producer commit and altered bridge
bytes are refused before outputs are written.

The archive's recorded producer commit is distinct from the selected release
tag target; this receipt does not assert they are the same source revision.
Semantic Core in the SDK package is 0.10.0. No native Spark, Delta table,
managed catalog, publication, query, source holder, or ACK activity occurred.
