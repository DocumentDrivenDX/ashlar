---
ddx:
  id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-compute-policy-refresh-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
---

# Managed A1 dedicated Jobs admission packet

Packet profile: `ashlar-managed-a1-jobs-admission/0.1`  
Prepared: 2026-10-29  
State: effect-free candidate; not admitted for launch.

This packet converts the read-only Jobs policy refresh into a bounded candidate
configuration and an explicit pre-launch evidence record. It authorizes no cloud
operation. No cluster, job, schema, table, warehouse, source mutation,
publication, or ACK was created or started while preparing it. The current
evidence makes a dedicated Jobs policy-use route plausible, but does not prove
cluster creation, task execution, runtime compatibility, UC access mode, or a
whole-trial cost bound.

## Candidate run shape

| Setting | Candidate | Admission status |
| --- | --- | --- |
| Workspace/catalog | `aidev-cus` / owner-designated `client_dev` | Read-only catalog presence observed; no object is selected for creation |
| Compute | One dedicated Jobs cluster, `cluster_type=job` | Policy `0016F2019AE68DFD` observed; actual create entitlement and Jobs permissions unproven |
| Worker shape | Candidate: one `Standard_D4ds_v5` driver plus one `Standard_D4ds_v5` worker; fixed at one worker, no autoscaling | Two-node candidate; policy and UC-mode compatibility remain unproven |
| Runtime | Databricks Runtime 19 family, Spark 4.2.0 candidate | Workspace lists `19.x-scala2.13`; exact maintenance build and policy allowance unknown |
| Access mode | Must be explicitly selected and read back | Compatibility and effective grants unproven |
| Engine/features | Standard engine; no Photon | Candidate setting; runtime and UC behavior untested |
| Job structure | One publisher task, then a distinct verifier process/session | TD-009 proposal; stale-state independence and task/run permissions unproven |
| Concurrency/retries | One run at a time; zero automatic retries | Required trial control; Jobs configuration and enforcement untested |
| Lifetime | Finite caller deadline; terminate and verify terminal cluster state | No provider hard lifetime or dollar cap is established by auto-termination |
| Source ACK | No ACK credential in either task; separate live source holder required | Source/ACK custody is not admitted |
| Predictive optimization | Preserve inherited `ENABLE` | Read-only value observed; do not disable |

The previously proposed zero-worker shape is not valid for Spark under this
policy: Azure Databricks requires at least one worker unless compute is in
single-node mode, and single-node mode relies on a cluster-profile Spark
configuration that the selected Job Compute policy forbids. The official
[compute configuration reference](https://learn.microsoft.com/en-us/azure/databricks/compute/configure)
documents the worker requirement and single-node behavior; the
[compute policy reference](https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition)
documents the `spark_conf.spark.databricks.cluster.profile` policy field. This
packet therefore records a two-node shape as a candidate, not as an admitted
configuration. The policy defaults to four workers but leaves worker count
configurable; it also leaves runtime and node type configurable and forbids
instance pools. Its `admins` ACL grants inherited `CAN_USE`; the caller is an
indirect `admins` member. These observations support policy use only and do not
establish cluster-create entitlement, job permissions, UC-mode compatibility,
or successful execution.

## Cost admission

The preference is less than $10 total and the hard ceiling is $100 total,
including prior campaign spend, startup, idle time during external ACK, storage,
network, background usage, cancellation/termination delay and owned cleanup.
The current Azure [Databricks pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/)
lists Premium Jobs at `$0.30/DBU-hour` and D4ds v5 Jobs at 1.00 DBU/node; paired
with the retained [Central US VM price observation](current-upstream-release-20261023/managed-fixed-jobs-retail-observation.json),
the two-node 40-minute compute reference is about `$0.74` (about `$0.76` using
the pricing page's displayed PAYG total). The practical `$10` target and `$100`
escalation operating plan is recorded in the [cost-plan review](managed-a1-jobs-cost-bound-review-20261029.md);
it is not a provider-enforced invoice cap. Do not launch until prior attributable
A1 spend is checked and runtime/access and source-holder gates pass.

## Minimal source-holder route

The minimal candidate is a separately authenticated external owner that starts
only after the Jobs verifier has durably succeeded. That owner must retain the
exact request, manifest, destination receipt and complete four-role publication
vector; open live managed-publication-read, native-pin and Truss source-custody
handles; renew all three immediately before one protected exact-byte ACK; then
reconcile from a fresh source transaction and independently close all three
handles. Publisher and verifier tasks receive neither ACK credentials nor
authority to renew or synthesize these handles. This ordering follows the
source-holder port contract and selected Truss `.51` capability review; it does
not require holding Truss source custody through publisher or verifier
execution.

This remains a route shape, not an admitted implementation. The selected Truss
`.51` owner has read-only current/retained-page renewal plus ACK and reconciliation
primitives, but no source lease. Ashlar has no live managed-read handle bound to
an independent source-derived data witness, and `PostgresPins.hold()` has no
renewable native-pin handle with an independently admitted retention provider.
The existing ACK boundary therefore refuses without those live handles. See
the [Truss `.51` capability receipt](source-holder-truss051-capability-20261029.md)
and [A1 live port-gap review](source-holder-a1-live-port-gap-20261024.md).
Until both missing owners and their close-time evidence exist, no task graph or
protocol-fake choreography can qualify this route, and the A1 source-holder gate
remains closed.

## Required admission and post-start readback

Before any effectful run, attach a dated receipt for items 1–4. Items 5–6
are checks within a separately admitted bounded run, before publication. A
missing, ambiguous, or conflicting result stops later effects.

1. Reconfirm workspace, principal, catalog and schema grants; Jobs create, run,
   cancel and cluster terminate permissions; quota; and policy `CAN_USE` for
   this caller. Do not infer these from group membership alone.
2. Resolve the exact policy-conformant node/runtime/access-mode tuple. Record
   the exact `spark_version` maintenance identifier, policy-compliant worker
   count and node type, engine, Photon setting, UC access mode, and full
   effective cluster configuration. Confirm no pool is used.
3. Apply the reviewed cost plan to available current rates and usage evidence;
   check attributable prior A1 campaign spend and retain the cleanup reserve
   within the $10 target and $100 escalation ceiling. Include startup/idle, disk,
   network, background optimization, external ACK wait, and termination/cleanup
   in the forecast; the plan is an operating control, not a provider cap.
4. Independently pass the source/ACK holder, frozen request/source prefix and
   latest package-cohort gates in the A1 constraints and TD-009. Confirm the
   live source read/pin remains held through ACK commit, reconciliation and
   closing. Neither Jobs task receives ACK authority.
5. After a separately authorized cluster start, read back actual cluster/run/task
   identifiers, runtime/build, Spark/Scala/Delta/Java/Python versions, principal,
   access mode, namespace, protocol/features, and effective relevant settings.
   Qualify native wheels and required table realization before any publication.
6. Exercise finite deadline, zero-retry behavior, publisher/verifier process and
   session separation, verifier fresh-state reads, unconditional close after
   failure, owned cleanup and terminal cluster-state readback. Preserve evidence
   before removing only independently confirmed trial-owned objects.

## Disposition

The Jobs policy-use route is a candidate improvement over the prior
Personal-Compute-only observation. The prerequisites above remain open, so this
packet admits no Jobs launch and establishes no managed qualification. Do not
substitute the shared default cluster, an all-purpose cluster, or the stopped
SQL warehouse. Reassess only after the missing read-only evidence and independent
source/ACK gates are attached and reviewed. Keep this evidence separate from the
Truss R16 recovery packet and from implementation-status prose.
