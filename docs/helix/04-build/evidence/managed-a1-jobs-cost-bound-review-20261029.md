---
ddx:
  id: ashlar.evidence.managed-a1-jobs-cost-bound-review-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-authenticated-discovery-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
---

# Managed A1 Jobs cost plan review

Reviewed 2026-10-29 for one dedicated, non-Photon `Standard_D4ds_v5` driver
and one matching worker, Databricks Runtime 19 family, and a caller wall limit
of 40 minutes. This is a cost review only. It creates no launch authority and
does not change predictive optimization.

## Observed price basis

Microsoft's Azure Databricks pricing page, accessed 2026-10-29, lists Premium
Jobs Compute at `$0.30/DBU-hour`; its Ddsv5 Jobs table lists D4ds v5 at 1.00
DBU per node and a PAYG total of `$413.18/month` including VM and DBU price.
The displayed total is about `$0.566/node-hour`, or `$1.132/hour` for two
nodes and `$0.755` for 40 minutes. The retained Azure Retail Prices API
observation for the exact Central US D4ds v5 meter is `$0.255/VM-hour`
(2026-10-23); combined with the current published Jobs DBU rate and count, that
gives `$1.11/hour` and `$0.74` for 40 minutes. These are conditional
compute-only list-price calculations, not effective account rates or whole-run
bounds. The `$0.755` calculation uses the pricing page's displayed total; its
regional applicability to this workspace is not established.

Microsoft's pricing notes identify managed/disk/blob storage and Public IP as
additional VM charges, and data transfer, NAT gateway and Private Endpoint
usage as additional charges. With inherited predictive optimization left
enabled, Databricks can run managed-table maintenance on serverless Jobs
compute. The billing usage reference lists compute, storage, network and API
usage types, and says original usage records are typically available within
12 hours; predictive-optimization billing records can take up to 24 hours.
These are post-usage records, not a prospective dollar cap.

## Practical operating budget

For one A1 correctness run, plan on one fixed two-node Jobs cluster, no
autoscaling, zero automatic retries, one publisher and one verifier task in the
same job, and a 40-minute caller deadline including startup, external ACK wait,
termination and cleanup. The compute-only reference is approximately `$0.76`
for 40 minutes at the displayed PAYG total. Use a `$10` target allocation:
reserve up to `$9.24` for attributable ancillary/cleanup and any checked prior
A1 campaign spend. Retained trial receipts show no A1 resource or paid compute
was started; the 2026-10-29 discovery did not query billing to verify total
attributable campaign charges. Keep the user-authorized `$100` as the hard
escalation ceiling; a forecast above it stops for owner review before the next effect.
This is a conservative planning reserve, not a provider-enforced invoice cap.

Keep the correctness fixture tiny and owned by this run. Do not provision
optional NAT gateways, Private Endpoints, or egress paths for the trial; avoid
external data transfer. Preserve predictive optimization `ENABLE`. Before
launch, check attributable A1 campaign charges and remaining headroom in the
available billing view. During the run, monitor the caller deadline and
resource state; stop trial work at minute 30, request cancellation/termination
by minute 40, then continue observing until the cluster is terminal and owned
objects are removed. Do not begin follow-on work if termination or cleanup is
uncertain. Databricks billing records are delayed (typically 12 hours, and
predictive-optimization history up to 24 hours), so reconcile the delayed usage
afterward and carry any late charges into the remaining campaign budget.

This makes a run a reasonable candidate to stay near `$10` while preserving the
`$100` ceiling, but it does not guarantee an invoice maximum: provider-side
shutdown delay, actual storage/network meters, inherited predictive-optimization
work and effective account discounts are not provider-capped by this plan.
Reopen cost admission after access/runtime and source-holder gates pass and the
prior attributable A1 spend check is recorded. If the best available forecast,
including cleanup reserve, can exceed `$100`, do not launch without owner
review. A $10 preference exceedance alone is not an escalation.

## Sources

- [Azure Databricks pricing](https://azure.microsoft.com/en-us/pricing/details/databricks/) — accessed 2026-10-29; Premium Jobs rate, D4ds v5 Jobs DBU count and displayed PAYG total; pricing notes for separate Azure meters.
- [Billable usage system table reference](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing) — accessed 2026-10-29; usage types, product attribution and record availability.
- [Predictive optimization for Unity Catalog managed tables](https://learn.microsoft.com/azure/databricks/optimizations/predictive-optimization) — accessed 2026-10-29; maintenance compute uses serverless Jobs and requires managed tables.
- [Predictive optimization system table reference](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/predictive-optimization) — accessed 2026-10-29; billing history availability.
- [Central US retail-rate capture](current-upstream-release-20261023/managed-fixed-jobs-retail-observation.json) — Azure Retail Prices API query completed 2026-10-23; `$0.255` per D4ds v5 VM-hour, Premium Jobs `$0.30` per DBU-hour.
