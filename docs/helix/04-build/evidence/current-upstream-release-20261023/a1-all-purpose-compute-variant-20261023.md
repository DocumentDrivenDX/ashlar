---
ddx:
  id: ashlar.evidence.a1-all-purpose-compute-variant-20261023
  type: build-evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.managed-a1-authenticated-discovery-20261023
      kind: informed_by
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
    - id: TD-009
      kind: informed_by
---

# A1 all-purpose compute variant evidence

## Observation

The [authenticated discovery](managed-a1-authenticated-discovery-20261023.md)
verified Personal Compute policy `001654FA4AA08D1A`: fixed
all-purpose compute, Standard engine, zero workers, D4ds_v5 allowed, and runtime
unrestricted. This establishes policy admission only; it does not establish that
the A1 workload can be created or run under it.

Microsoft Learn documents Python script and wheel tasks as supporting classic
all-purpose compute, while warning that this is not recommended for production
([Jobs compute](https://learn.microsoft.com/en-us/azure/databricks/jobs/compute),
task support and production guidance). Classic all-purpose compute is billed
differently, with auto-termination tied to inactivity
([classic jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/run-classic-jobs)).
The [billing usage table](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/system-tables/billing)
does not populate `job_id` for jobs on all-purpose compute, so a bounded pilot
would need cost attribution to its dedicated owned cluster and runtime interval.

The [Central US retail observation](managed-fixed-jobs-retail-observation.json)
records $0.255 per D4ds_v5 VM-hour. Microsoft's current
[Azure Databricks pricing table](https://azure.microsoft.com/en-us/pricing/details/databricks/)
lists D4ds v5 (4 vCPU, 16 GiB) at 2.00 DBU under Premium All-Purpose Compute's
Ddsv5 series. The policy fixes the Standard engine; the public table row supplies
the per-node retail DBU count for this candidate shape, not a measurement from
this workspace. The Premium All-purpose Compute DBU rate is $0.55 per DBU-hour
(Azure Retail Prices API meter `aad24629-5578-4bfb-b890-62e2779afe8a`). On that
public PAYG basis, one node costs `$0.255 + (2.00 * $0.55) = $1.355/hour`:
about $0.90 for one 40-minute slot and $7.23 for eight 40-minute slots, before
ancillary or idle charges. Adding an illustrative, assumed campaign-wide $20
ancillary and cleanup reserve gives about $27.23 for eight slots. This is a
scoped retail estimate, not a total bound: actual billing
granularity, negotiated
account rates, startup/idle time, disks, networking, predictive-optimization
background usage and cleanup charges remain unmeasured. The $100 hard ceiling
is not admitted by this estimate.

## Limits and next gate

The 2.00 DBU count is the public rate-table basis for the named SKU and tier;
workspace billing and ancillary usage remain unobserved. No build, runtime,
grant, provisioning, task-isolation, or cleanup evidence exists. Separate task
processes on the same cluster still require a stale-state test and a retained
source holder. This note does not establish execution or authorize launch.

An all-purpose option needs separate reviewed design and cost admission. The
policy's auto-termination default is 4,320 minutes and permits the setting to be
unlimited; any future dedicated cluster must explicitly set a short
auto-termination interval (for example, 10 minutes) and have an external
termination control. These are launch guards, not provider shutdown guarantees.
The [A1 constraints](managed-a1-correctness-trial-constraints.md) and TD-009's
Jobs proposal remain unchanged.
