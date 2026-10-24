---
ddx:
  id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: TD-006
      kind: informed_by
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
---

# Managed A1 correctness trial: offline constraints

Prepared 2026-10-23. This is a reviewable plan, not launch approval, current capability evidence or a cost admission. No cloud resource, schema, compute, source mutation, publication or ACK was created or started. The local 1B/5B scale trial remains separate.

Use one new per-run temporary schema under `client_dev`, only owned objects, concurrency one and zero retries. Preserve predictive optimization. Begin with A1 only; continue the frozen `A1,B1,A2,B2,A3,B3,A4,B4` schedule only after each predecessor's independent verification and protected source-specific ACK. A pilot is not full R04 acceptance.

## Launch prerequisites

- Complete actual R03 Truss 0.0.49 migration/resume, genesis publication/verification/ACK and seed readback. Independently accept B with separate installation/feed/epoch/namespace/consumer/destination ACK custody. Freeze baseline bags, accepted UMF 0.10 models, eight-slot semantic schedule and oracle. Freeze each exact request before its source mutation and bind its actual committed page before graph effects.
- Admit exact Ashlar, Weft and UMF artifacts on a separately versioned managed runtime profile. The published UMF 0.5.6 wheel provenance and local Spark/Delta qualification do not qualify the selected native Weft wheel or managed runtime. The UMF 0.10.1 Linux extension statically requires glibc symbols through `GLIBC_2.34`; the candidate Databricks runtime's glibc/loader versions are unknown ([static ELF inspection](umf-0.10.1-linux-elf-static-inspection-20261023.md)). The closest documented Spark match is Databricks Runtime 19, which bundles Apache Spark 4.2.0, Scala 2.13.18, Java 21 and Delta Lake 4.2.0; it is not the local Spark 4.2.0/Delta 4.4.1 profile. Databricks documents that runtime Delta feature support can differ from OSS Delta, including for backported features ([Runtime 19 release notes](https://learn.microsoft.com/azure/databricks/release-notes/runtime/19), [Delta feature compatibility](https://learn.microsoft.com/en-us/azure/databricks/tables/features/feature-compatibility)). These published versions do not establish availability under this workspace's policy or that the chosen runtime passes Ashlar's behavior requirements.
- Review/adopt [TD-009](../../../02-design/technical-designs/TD-009-managed-spark-receipt-verification.md) and implement its managed runtime/campaign integration before selecting the managed Spark verifier. Its offline reopen and bounded source-prefix comparison slices are implemented, but do not qualify native execution or campaign use. The existing SQL verifier still requires a dedicated serverless current-channel warehouse; no accessible dedicated endpoint was established by retained discovery.
- Establish the reviewed external live source/read/pin renewal interval through ACK commit, reconciliation and closing. A verifier report or terminated task is not a live custody holder. Jobs tasks receive no source ACK credential. Truss 0.0.49 exposes the receipt ACK/reconciliation API and Ashlar has receipt-mode composition, but live source and managed-publication custody remain open ([read-only API observation](truss049-public-receipt-api-observation-20261023.md)).
- Verify live authentication, run-as/grants, Jobs creation/cancel/termination authority, quota, UC access-mode and policy compatibility, exact runtime/protocol/features, effective inherited predictive optimization, billing meters and owned cleanup permissions before schema creation or compute. Read-only admission must establish the exact allowed runtime identifier and maintenance build, node/runtime/policy combination, access mode and effective principal grants. After any separately authorized compute start, read back the actual Spark, Scala, Delta, Java and Python versions, runtime/build identity, access mode, principal, and effective relevant settings. Before A1, qualify the managed DDL and required table realization on the selected runtime with bounded owned-table readback; retain exact managed status, three-part names, UUIDs, schema, properties/clustering, protocol and feature metadata. Do not infer runtime or UC policy/grants from local versions or workspace-advertised metadata.

## Conditional compute and cost envelope

Proposed conditional dedicated shape: one non-Photon `Standard_D4ds_v5` driver, zero workers, no autoscaling, using DBR 19 only if its exact runtime, node and UC policy combination is admitted. Official DBR 19 documentation reports Spark 4.2.0, Delta 4.2.0, Scala 2.13.18 and Java 21, which differs from the locally qualified Delta 4.4.1 profile and requires the managed DDL/protocol/features readback above. The earlier read-only workspace metadata advertised `17.3.x-scala2.13` (Spark 4.0/Scala 2.13) as a candidate and listed 19.x identifiers, but validated neither exact combination, UC compatibility, policy allowance nor a runnable target. The authenticated Personal Compute policy `001654FA4AA08D1A` is fixed to `cluster_type=all-purpose` and `num_workers=0`; although it allows `Standard_D4ds_v5`, requires Standard engine and leaves `spark_version` unrestricted, it does not establish an available dedicated Jobs-cluster path under TD-009 and cannot be used for that proposed shape as-is. Admission still needs a reviewed policy variant or other Jobs-compute path plus exact node/runtime/access-mode compatibility and principal entitlement/Jobs permissions. Using an all-purpose cluster would be a separately reviewed design and cost variant, distinct from TD-009's Jobs proposal. The read-only `users get` entitlement detail was admin-only. The 2026-10-23 authenticated discovery confirmed membership in `db-aidev-users` and effective catalog `CREATE_SCHEMA` and `USE_CATALOG` privileges through that group; schema ownership and drop authority remain unconfirmed. Effective inherited predictive optimization is confirmed as `ENABLE`, but exact runtime, policy and trial capability remain unresolved ([authenticated discovery](managed-a1-authenticated-discovery-20261023.md)).

Proposed per-slot caller wall budget: 15 minutes provisioning/admission, five publisher, five fresh verifier, five external source/ACK work, and ten cancellation/termination/cleanup: **40 minutes total**. At minute 30 ordinary work stops. Include all billable source preparation/ACK idle time; expand and recost if it does not fit. Confirm terminal compute state; these are requested bounds, not provider shutdown guarantees.

[Dated October23 retail observation](managed-fixed-jobs-retail-observation.json) records Central US Linux VM $0.255/hour and Premium Jobs $0.30/DBU-hour. At an **unverified maximum 2 DBU/hour**, compute is $0.855/hour: $0.57 per 40-minute slot or $4.56 for eight. An assumed campaign-wide $5 ancillary reserve gives $9.56; $20 gives $24.56. Eight one-hour minimum charges would instead cost $6.84 compute. These figures, including $0.57 and $4.56, are illustrative assumptions, not reliable admission bounds. Actual DBU consumption, billing increments, startup/idle, disk/network/IP, background optimization and cleanup charges remain unresolved.

The owner preference is **$10 total**, with a **$100 hard total ceiling**. Neither estimate nor cancellation enforces an invoice cap. Admit a reliable conservative bound including incurred spend, remaining work and cleanup before launch and each continuation; stop for review if unavailable or above the hard ceiling. Track projected and actual total against the $10 preference; exceeding that preference alone does not require approval. Ask the owner only if the proposed trial would exceed $100 or requires separate owner scale authorization; do not proceed above $100 under the current ceiling. Do not substitute an unpriced SQL endpoint or shared cluster.

## Focused sequence and closure

Publish A1's actual transaction; a fresh verifier independently reopens six-role installation custody, exact four-role manifest versions and bounded complete source-derived bags/progress. The external holder independently admits the receipt under live custody, commits protected A-only ACK and reads it back. Exact replay must add no native/manifest commit or ACK. Later A2/B2 update their R1-created identities; A3/B3 delete them using the frozen incident-edge policy; A4/B4 use only the accepted additive candidate. Retain old reads. Run the selected current Weft query checks once on admitted A2, not a repeated matrix.

Retain exact requests/pages, source/ACK readbacks, UUID/version vectors, plans, bounded results, timings and usage. After evidence retention and closing custody, remove only trial-owned objects/schema and verify absence; terminate owned compute and verify its terminal state. An uncertain operation stops later source mutations and ACKs: retain original references for reconciliation, never retry blindly.

## Read-only verification limitation

Authenticated read-only discovery was refreshed on 2026-10-24; see the [2026-10-23 discovery](managed-a1-authenticated-discovery-20261023.md) and [2026-10-24 addendum](managed-a1-authenticated-discovery-20261024.md). The refresh found no clusters, advertised `19.x-scala2.13` and `17.3.x-scala2.13` runtime identifiers plus D4ds_v5/D4s_v5 node metadata, and exactly one Personal Compute policy fixed to all-purpose, zero workers and Standard engine. It confirmed catalog-level `CREATE_SCHEMA`/`USE_CATALOG` via `db-aidev-users` and inherited predictive optimization `ENABLE`; policy permission inspection was denied, and schema/object cleanup authority remains unverified. The policy cannot supply TD-009's Jobs-cluster path as-is. No schema, job, cluster or warehouse was created or started. Exact runtime/build/access-mode compatibility, compute/job entitlements and cancellation authority, cleanup, native managed qualification, external source/ACK custody, and current billing/DBU usage and conservative total cost remain unresolved. The prior retail estimates remain conditional only; no total bound under the $100 ceiling is admitted.
