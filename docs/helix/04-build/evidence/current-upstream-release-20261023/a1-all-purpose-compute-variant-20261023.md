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

The existing [Central US retail observation](managed-fixed-jobs-retail-observation.json)
records $0.255 per D4ds_v5 VM-hour. The Azure Retail Prices API observation for
the Premium All-purpose Compute DBU meter
`aad24629-5578-4bfb-b890-62e2779afe8a` is $0.55 per DBU-hour. Conditional
one-node estimate: `$0.255 + $0.55 * x` per hour, where `x` is actual DBU/hour.
At an unverified assumption of `x = 2`, a 40-minute A1 slot is about $0.90;
eight slots are about $7.23 before ancillary or idle charges. Adding the existing
$5 preferred reserve gives about $12.23 for eight slots, above the preferred
$10 envelope. The $100 hard ceiling is not admitted by this estimate.

## Limits and next gate

`x = 2` is an unverified assumption; public pricing may list a 2-DBU shape, but
the workspace's actual consumption is unknown. No build, runtime, grant,
provisioning, task-isolation, or cleanup evidence exists. Separate task processes
on the same cluster still require a stale-state test and a retained source holder.
This note does not establish execution or authorize launch.

An all-purpose option needs separate reviewed design and cost admission, including
explicit termination controls. The [A1 constraints](managed-a1-correctness-trial-constraints.md)
and TD-009's Jobs proposal remain unchanged.
