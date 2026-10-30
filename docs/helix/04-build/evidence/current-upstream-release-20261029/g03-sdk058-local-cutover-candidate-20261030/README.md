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

A future one-shot local transaction packet requires all of the following before any Spark process starts: a fresh private installation/output parent with no existing target role paths or registry; the exact typed-graph source configuration and its bound source/schema/binding files; the SDK058 admission inputs pinned by the source-correspondence packet (Node, installed package, archive, closure/producer receipt, install custody and source-admission manifest); and the retained local runtime cohort: Java 21, PySpark/Spark 4.2.0, Delta 4.4.1, `delta-spark_4.2_2.13-4.4.1.jar` SHA-256 `2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be`, and `delta-storage-4.4.1.jar` SHA-256 `f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f`. The packet must re-attest these bytes at launch. It also needs a `.4-xxhash64` runtime config carrying that explicit SDK058 admission and a new review of the bounded initialization plus transaction-1 packet. Initialization must retain all six UUIDs and the versioned carrier profile before transaction 1 can use it. The transaction remains local-only: no manifest publication or protected source ACK is claimed. No native run is authorized by this candidate note.

The source admission baseline and exact SDK receipts remain in the [release realization packet](../g03-sdk058-delta4-realization-candidate-20261030/README.md) and [SDK 0.5.8 source correspondence addendum](../g03-sdk058-source-correspondence-20261030/README.md). This candidate does not establish that other runtime consumers, Truss, managed Unity Catalog or production adapters have cut over.
