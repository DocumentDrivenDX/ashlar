---
ddx:
  id: ashlar.evidence.managed-a1-cost-readonly-followup-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-authenticated-discovery-20261024
      kind: informed_by
    - id: ashlar.evidence.managed-a1-correctness-trial-constraints-20261023
      kind: informed_by
---

# Managed A1 read-only cost follow-up

Observed 2026-10-24. No Databricks request succeeded and no compute or other
cloud resource was created, started, changed or deleted.

## Exact read-only attempt

From the Ashlar repository, ran:

```text
command -v databricks
databricks --version
databricks --profile aidev-cus clusters list --output json
databricks --profile aidev-cus clusters spark-versions --output json
databricks --profile aidev-cus cluster-policies list --output json
```

The CLI resolved to `/opt/homebrew/bin/databricks`, version `1.21.0`. The first
API command stopped with `Error: error getting token: cache: no cached
credentials; run databricks auth login to sign in`; the remaining API commands
were not attempted. No login or credential operation was made. Thus this
environment could not refresh the authenticated metadata or query account
usage/pricing. This is not a contrary result to the prior authenticated
2026-10-24 discovery.

## Current public pricing evidence

Microsoft's current [Azure Databricks pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/)
lists Premium All-Purpose Compute at $0.55 per DBU-hour and states that DBU
usage is billed per second. Its D4ds v5 price table shows 2 DBUs for the
instance. The previously retained Central US VM retail observation is $0.255
per VM-hour. Conditional arithmetic at those public retail rates is therefore
`$0.255 + (2 × $0.55) = $1.355/node-hour`, or about `$0.903` for 40 minutes
of one node, before other charges. This refines the compute-only illustration;
it is not an account rate or a whole-trial estimate.

The public retail rate does not establish this workspace's effective rate,
discounts or invoice treatment. The billing system table documentation says
usage records are typically available within 12 hours and enumerates additional
usage types including storage, network and API operations; these records are
post-usage monitoring, not a pre-run cap. It also identifies predictive
optimization as a separately attributable product. See Microsoft's
[billable usage table reference](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing)
and [cost monitoring guidance](https://learn.microsoft.com/en-us/azure/databricks/admin/usage/system-tables).

## Finding

A defensible conservative whole-trial upper bound below the owner's $100
ceiling is still not established. The $0.903 40-minute compute figure assumes
the duration and list-price inputs; it excludes uncertain provisioning,
startup/idle and termination tail, effective workspace price, ancillary usage,
cleanup, and predictive-optimization charges. The existing $20 reserve is an
assumption, not a bounded maximum. A timeout or cancellation request does not
bound provider-side termination or invoice spend. No actual D4ds v5 A1 usage
exists to measure yet, and querying billing after a run would not provide
pre-launch cost admission. Keep the no-launch decision from the 2026-10-24
discovery unchanged pending an admitted billing basis and defensible bound.
