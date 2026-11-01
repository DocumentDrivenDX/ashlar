---
ddx:
  id: ashlar.evidence.r20-truss051-publication-outcome-reconciliation-20261101
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

# R20 Truss-to-Delta publication outcome reconciliation

R20 committed a local development Delta publication for the verified R16b Truss page, with source ACK explicitly deferred. The worker completion receipt records publication ID `truss-graph-949ff6d2eaa6e605d22432366c3ae4b557dfae10ecc858eecb7920a43e450f2b`, request digest `949ff6d2eaa6e605d22432366c3ae4b557dfae10ecc858eecb7920a43e450f2b`, `publication_committed: true`, and `acknowledged: false`. Independent read-only checks revalidated the request digest, exact manifest bytes, four-role publication vector, four Delta log heads, and each current Delta data bag. All four graph data tables are at version 1 with no active Parquet files and zero Spark readback rows. This matches the verified Truss page, which contains no graph changes or source rows and carries a schema revision only; the result establishes metadata-only local publication for that page.

The publication receipt does not make the overall supervisor result successful. The supervisor exited nonzero with `child exited with process-group members remaining: [59112]` after the worker had written its completion receipt. Its receipt preserves `status: failed_or_interrupted`, `publication: false`, and `database_effects_may_remain: true`. It also records SIGTERM cleanup, `process_group_termination.status: terminated`, no remaining process-group PIDs, and `process_custody_status: no_owned_processes_remain`. A fresh process inventory after reconciliation found no R20 PostgreSQL or Spark process. The retained supervisor receipt contains only PID 59112, not its command or parent, so its executable cannot be identified from current evidence; a read-only unified-log query for that PID returned no entries. A lingering local Spark JVM is plausible because R20 ran local Spark and no detached PostgreSQL clone owners remained, but that attribution is an inference. A bounded post-child grace period that continues the existing resource/custody checks, records PID/parent/command details, and terminates any process still present after the grace interval is a narrow future supervisor improvement; no supervisor code or historical receipt was changed here.

The independent source check used a separate copy of the stopped R20 migration clone. Through Truss `.51` public `read_page`, it reread the exact 32,337-byte first page (SHA-256 `de5c6d50f03082e5650126db86b4b50871a70f3a74f183a06a1f645a31e51760`), prior checkpoint `802f677f84076a2deaa1da682c836550a38654e2e1b67494b9cd2acf01299725`, and next checkpoint `195f826d5257fbcd920c19569d9dc33930dfeef22aa7ee14dce3e6c3fe4f280c`. A separate PostgreSQL READ ONLY transaction confirmed the consumer checkpoint remained at the prior digest and `runtime_feed_ack_receipt` had zero rows. The audit clone was cleanly stopped after the query. The retained registrar receipt binds the same reader page and registration to report, accepted-document, and complete-view evidence from the existing local registrar. The accepted document remains `ashlar-local-dev-typed-graph-r6-20261031`, revision `1`, SHA-256 `2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880`; no registrar grant was created by R20.

The exact native Delta heads are `edge_current` v1 (`6af413a9-6324-4f9b-a8ac-c2d8fededb27`, head SHA-256 `10cd156622cd1ef71b065ff8f414af321bf1c04c90853e4e1c461c3f6aeec51f`), `object_current` v1 (`007c6ab7-f602-4047-88d0-80c67594f1ab`, `b7795e4c44a75473dc894c0f91850fd960e187fbe76560e4fde28716d2ecfb2d`), `tombstone` v1 (`254b7134-6e35-4a4e-97b8-03522b552308`, `8a4225fc500568af52454701074fca69b91be8ae356545587f94d99650a3dacc`), and `whole_source_history` v1 (`90ca86d0-7818-4672-b321-06ee3b3d0329`, `449ed9a9b1ffcd167daa14119c16dde9e21ec7d2879dee5b3da8a39cdd7c36d8`). The four current heads have no `add` actions and no active Parquet data files. The source progress vector identifies feed `ashlar-local-dev-typed-graph-feed-r19-20261101`, batch `754`, and the verified page digest; it does not represent a Truss ACK. The R19 retained source and R20 cold backup still match at 65,754,852 bytes, 1,255 files, digest `b1ab6a55e3d13981e706b0cdb7ddd653ba7db9e241da6bef9ad4c16a47adc82e`. The R20 migrated working clone and the separate audit clone are retained independently. The R20 publication request, manifest, vector, source page, Delta result, worker receipts, supervisor receipt, and Delta logs remain at `/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101` and `/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101-supervisor`; their key digests are recorded in [the machine-readable reconciliation](r20-readonly-reconciliation.json). The committed publication is recoverable by its retained request digest and descriptor. Do not replay it or ACK the Truss page without a separate reviewed continuation.

This remains local development evidence only. It does not establish production source authority, cloud publication, a nonempty graph data bag, or source acknowledgement. The original Truss page remains unacknowledged.
