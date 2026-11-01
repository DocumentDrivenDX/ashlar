---
ddx:
  id: ashlar.evidence.managed-a1-personal-compute-admission-candidate-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-operational-retention-profile-proposal-20261031
      kind: informed_by
    - id: ashlar.evidence.managed-a1-operational-interval-candidate-20261031
      kind: informed_by
    - id: ashlar.evidence.managed-a1-readonly-refresh-20261031
      kind: informed_by
    - id: ashlar.evidence.managed-a1-jobs-cost-bound-review-20261029
      kind: informed_by
---

# Managed A1 Personal Compute admission candidate — 2026-10-31

**Disposition: policy-shaped candidate, not admitted.** The owner-selected
15-minute operational-retention profile can be paired in principle with a
separately owned Personal Compute all-purpose cluster and sequential publisher
and verifier Jobs tasks attached by `existing_cluster_id`. This does not prove
the exact Unity Catalog-capable runtime/access mode, task execution, total cost,
or live source/ACK custody required by A1. No cloud operation or paid compute
was performed for this packet.

## Workspace scope

The authorized disposable `client_dev` trial targets profile `aidev-cus`,
workspace 7405607548213398. The user's usual `ashlar-dev` workspace
(7405619521761591) is a separate environment; its prior catalog inventory did
not include `client_dev`, so its policy and grants do not govern this trial.
Do not transfer results between the workspaces. The owner has refreshed SSO for
both profiles in an unsandboxed context. The current `aidev-cus` observations
below govern this trial.

## Read-only evidence and limit

The refreshed 2026-10-31 read-only inventory for workspace `aidev-cus`
identifies caller `erik.labianca@synaptiq.ai`, groups `db-aidev-users` and
`db-aidev-fdle`, and one policy, Personal Compute (`001654FA4AA08D1A`). Its
read-back rules set `cluster_type=all-purpose`, zero workers, `STANDARD` engine
and `spark.databricks.cluster.profile=singleNode`; `Standard_D4ds_v5` is the
default and allowed node type. The `spark_version` rule is unlimited with
default `auto:latest-ml`; this does not identify the maintenance runtime that
an eventual cluster would run. Access-mode values include `AUTO`, `STANDARD`
and `DEDICATED`. Pools are disallowed. Auto-termination is optional/unlimited
with a 4,320-minute default. Thus the policy shape matches a single-node
all-purpose candidate. The caller's effective policy permission is
`CAN_USE` (“Can use the policy”). This does not establish cluster-create
entitlement. A 10-minute setting,
exact runtime, effective UC mode, task execution and actual cluster readback
remain unproven.

The refreshed `client_dev` catalog read confirms a managed isolated catalog,
with the comment “Client data for the ADR-024 rehearsal. FAKE DATA ONLY.” Its
effective predictive optimization is `ENABLE`, inherited from the metastore.
The `db-aidev-users` group has `CREATE_SCHEMA`, `CREATE_TABLE`, `MODIFY`,
`SELECT`, `USE_CATALOG` and `USE_SCHEMA` (among other grants); the caller is a
member of that group. The catalog comment is not source authority. The trial
must use only its independently admitted synthetic source inputs. `clusters
list` remained empty. The refreshed runtime catalog lists `19.x-scala2.13`
(Spark 4.2.0, Scala 2.13), and node catalog lists D4ds v5 as four cores and
16,384 MiB. These are availability catalogs, not a selected maintenance build,
cluster readback or runtime/UC execution proof.

## Minimal candidate packet

| Item | Candidate or bound | Gate |
| --- | --- | --- |
| Workspace/data | `aidev-cus`; owner-designated disposable schema under managed isolated `client_dev`; four tiny managed Delta roles | Catalog and group grants observed. Reconfirm the exact schema is absent and select only independently admitted synthetic input. |
| Compute | One separately owned Personal Compute all-purpose cluster; `Standard_D4ds_v5` driver; zero workers/singleNode; Standard engine; no pools, Photon or autoscaling | Policy rules and `CAN_USE` observed; cluster-create entitlement, finite 10-minute auto-termination override and exact create/readback remain open. Never use a shared or pre-existing cluster. |
| Lifetime | Ten-minute inactivity auto-termination candidate; external caller deadline no more than 40 minutes, including startup, ACK wait, cancellation and cleanup | Auto-termination is a cleanup aid, not a hard duration or invoice cap. Supervisor must cancel/terminate and verify terminal state. |
| Runtime and UC | Policy allows `AUTO`, `STANDARD`, `DEDICATED`; `spark_version` is unlimited with `auto:latest-ml` default; DBR 19 family is listed as `19.x-scala2.13` / Spark 4.2.0 | Exact DBR maintenance build, effective UC mode, Delta/Python/Java versions and compatibility remain unknown; don't infer from the family listing or policy default. |
| Tasks | One publisher then one verifier, separate Jobs tasks, processes and Spark sessions, attached to the dedicated cluster with `existing_cluster_id`; concurrency one, retries zero | Jobs listing is readable, but no A1 task exists; no Jobs create dry-run was attempted. Task attachment, effective run-as identity, task permissions and fresh verifier reads remain open. |
| Source/ACK | 15-minute observation interval; exact complete four-role reads at open, immediately pre-ACK and close; source custody independently held | Still blocked on the native read/pin renewal and source/ACK holder ports. No task receives ACK credentials. |
| Predictive optimization | Preserve effective `ENABLE` | Observe only; do not change it. |
| Spend | Prefer total below `$10`; hard total ceiling `$100`, inclusive of prior A1 spend and cleanup | No authenticated billing view was available without starting an endpoint. Prior attributable spend, effective all-purpose DBU/VM rates and increments, ancillary/storage/network/PO usage and conservative reserve remain unresolved. |
| Cleanup | Retain evidence first; terminate the exact owned cluster and verify terminal state; delete only the trial-owned schema after rechecking exact owner/name/UUID; verify absence | No cleanup operation is authorized by this packet. Preserve ambiguous effects and reconcile rather than retry. |

## Cost reference and conclusion

TD-009's retained one-node all-purpose reference is approximately `$1.355` per
running node-hour, or about `$0.90` for 40 running minutes, before startup/idle,
storage, networking, predictive-optimization activity, delayed charges and
cleanup. This makes the `$10` preference plausible for a tiny, short run, but
it is a compute-only reference, not an effective account meter or bound. The
earlier Jobs pricing review uses a different compute class and must not be
copied as this candidate's rate. No authenticated billing view was available
without starting an endpoint; prior attributable spend and ancillary meters
are unresolved, so neither the `$10` target nor `$100` maximum is admitted.

Thus Personal Compute is the only currently evidenced policy-shaped route, and
its one-driver/zero-worker shape matches the candidate described in TD-009. It
may satisfy the topology portion of the selected profile after readback. The
available evidence cannot establish that the full A1 contract is met: exact
runtime and UC access, task binding and separation, fresh total-cost envelope,
source/ACK custody, and native holder ports remain open. Do not create compute,
create the schema, publish, or ACK on this evidence alone.

## Remaining admission gaps

Policy `CAN_USE` is proven, but no cluster-create or Jobs-create dry-run was
attempted; no task attachment, run-as identity or task permissions were
established. The Jobs list is readable, but no A1 job exists. No authenticated
billing view was available without starting an endpoint, so prior campaign
spend and ancillary meters remain unknown. The `$10` preference is a plausible
compute-only estimate; the `$100` all-in ceiling is not yet evidenced.
Source/ACK custody and native holder ports remain separate release gaps.

## Governing references

- [Selected operational retention profile](managed-a1-operational-retention-profile-proposal-20261031.md)
- [A1 operational interval candidate](managed-a1-operational-interval-candidate-20261031.md)
- [TD-009 managed Spark receipt verification](../../02-design/technical-designs/TD-009-managed-spark-receipt-verification.md)
- [2026-10-31 read-only refresh](managed-a1-readonly-refresh-20261031.md)
- [Jobs cost review](managed-a1-jobs-cost-bound-review-20261029.md)
- [Azure Databricks Personal Compute](https://learn.microsoft.com/en-us/azure/databricks/clusters/personal-compute)
- [Azure Databricks Jobs compute](https://learn.microsoft.com/en-us/azure/databricks/jobs/compute)
