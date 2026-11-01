---
ddx:
  id: ashlar.evidence.r20-truss051-registrar-catalog-preflight-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-002
      kind: informed_by
    - id: TD-004
      kind: informed_by
---

# R20 registrar-owned current catalog evidence preflight

R19 used the dedicated reader principal for complete catalog-view evidence and received Truss `.51` unauthorized (`P1403`). R20 retains that reader only for the feed page, then derives a native batch from the same page and calls `read_graph_catalog_evidence` with a separate factory authenticated as the existing `ashlar_local_dev_registrar_r6_20261031` role. Before that call and before Spark startup, R20 checks that role's safe LOGIN attributes, registrar membership, module-writer membership, and EXECUTE rights for both public person entrypoints and their original functions using a SELECT-only transaction.

The public catalog evidence call reads the report, accepted source document bytes, and complete catalog view in one bounded READ COMMITTED transaction using the registrar factory. Truss `.51`'s catalog-view basis requires a READ WRITE transaction because it takes `FOR SHARE NOWAIT`; R20 records this accurately and relies on the Ashlar reader to roll the transaction back after its SELECT-only evidence capture. The same separate factory is provided to the publisher, while the feed reader connection remains the page owner. No registrar grant is created or broadened by R20.

The R20 worker and supervisor effect-free preflights passed against R19's immutable cold backup and fresh output paths. The backup's source-copy receipt records 65,754,852 bytes, 1,255 files, and tree digest `b1ab6a55e3d13981e706b0cdb7ddd653ba7db9e241da6bef9ad4c16a47adc82e`; a fresh layout/hash/process-custody readback matched with zero process owners. This uses a stable copy because the later mutable R16b source path has since changed. The checks confirmed the selected runtime, public API call shapes, R16b page/checkpoint binding, and source custody; they did not start PostgreSQL or Spark or contact the database. At the time of effect-free preflight, the standing authorization receipt was pending Sol review and native dispatch was disabled. Sol later reviewed and enabled one bounded local run; see the [R20 publication outcome reconciliation](r20-publication-outcome-reconciliation.md).

This evidence follows [CONTRACT-001](../../../../../02-design/contracts/CONTRACT-001-publication-boundary.md) for page identity and publication-before-ACK, [CONTRACT-002](../../../../../02-design/contracts/CONTRACT-002-consumer-read-boundary.md) for source-scoped read authority, and [TD-004](../../../../../02-design/technical-designs/TD-004-truss-catalog-preparation.md) for Truss source authority and effect exclusions. The accepted Truss source remains local development authority only.
