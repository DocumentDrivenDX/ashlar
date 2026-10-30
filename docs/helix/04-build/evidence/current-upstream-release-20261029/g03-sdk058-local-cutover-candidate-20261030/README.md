---
ddx:
  id: ashlar.evidence.g03-sdk058-local-cutover-candidate-20261030
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk058-delta4-realization-candidate-20261030
      kind: informed_by
    - id: ashlar.evidence.g03-sdk058-source-correspondence-20261030
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# SDK 0.5.8 configured-local `.4` cutover candidate

This additive path lets `ConfiguredLocalRuntimeConfig` opt into the reviewed SDK 0.5.8 typed-graph receipt and source-backed `.4` IIFE bridge. The JSON source configuration still carries its historical source producer identity; an explicit wrapper binds the SDK 0.5.8 policy receipt without changing the historical loader or its default policy. The existing `.4` creator remains the default and Truss call sites stay on their historical path. The versioned installation profile `/0.2` records the SDK policy token, archive producer commit `855f6c48649496fc60b3303798790d0ddb55d38f`, selected tag commit `efb4042719866888ad80c01cb2da694972eb2738`, source tree, archive, bridge and manifest pins. Six role names, distinct Delta UUIDs, alias mapping, native integer hash fields and publication-vector checks remain in the existing transport path.

Effect-free focused checks passed: 37 tests across SDK source admission, bridge realization, six-role profile construction, Delta 4 publication admission and configured-local runtime; one optional check was skipped. The bridge test uses an inert Spark port and observes proposals only. No Spark session, Delta table, publication, ACK, cloud resource or production authority was exercised. This candidate is not an execution receipt.

An additive one-shot packet candidate now binds the exact source, runtime, SDK 0.5.8 admission and `/0.2` bridge inputs; it runs focused effect-free preflights and tests only. Its new parent path was observed absent, but no attempt directory, Spark session or Delta table was created. R1 and r2 dispatches remain consumed retained failures. The packet is awaiting Sol review and has no execution permit ([SDK 0.5.8 one-shot packet](one-shot-sdk058-native-attempt/README.md)).

The source admission baseline and exact SDK receipts remain in the [release realization packet](../g03-sdk058-delta4-realization-candidate-20261030/README.md) and [SDK 0.5.8 source correspondence addendum](../g03-sdk058-source-correspondence-20261030/README.md). This candidate does not establish that other runtime consumers, Truss, managed Unity Catalog or production adapters have cut over.
