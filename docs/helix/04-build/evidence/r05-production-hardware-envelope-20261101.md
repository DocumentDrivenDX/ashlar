---
ddx:
  id: ashlar.evidence.r05-production-hardware-envelope-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: TD-008
      kind: governed_by
    - id: ashlar.evidence.r05-redesign-napkin-math-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-jobs-cost-bound-review-20261029
      kind: informed_by
---

# R05 production hardware envelope — 2026-11-01

The current 18-core local sample cannot support a five-minute claim for 1B
nodes and 5B edges. Its 45M/225M concurrent write took 30.620 seconds;
setup, writes and one combined persisted witness took 60.924 seconds. Scaling
the row count by 22.222 at 80% of observed throughput projects 14.176 minutes
for writes and 28.206 minutes for that measured bundle, **before** the second
distinct scan, two point lookups, GraphFrames action, startup and cleanup. This
is a planning estimate, not a full-run measurement.

Ideal scale-out of the already discounted 28.206-minute bundle from 18 cores
would take 3.97 minutes at 128 vCPU, 2.64 minutes at 192 vCPU, or 1.98 minutes
at 256 vCPU. The respective time left within five minutes is about 62, 142
or 181 seconds for all omitted work. Scaling the whole bundle is optimistic:
its setup and scan portions were not timed separately, and task geometry,
shuffle, metadata and graph-action behavior at target size are unknown. A
192–256-vCPU dedicated production candidate is therefore a **plausible design
envelope to admit and measure**, not a qualified performance target. The
128-vCPU case leaves too little unmeasured-work allowance to select as the
primary candidate.

The sample wrote 3.041 GB of active Parquet, projecting 67.585 GB at target.
Five minutes requires at least 225 MB/s of output throughput. If both requested
full scans read all active bytes, output plus two reads would move at least
about 203 GB, requiring 676 MB/s aggregate even before graph-engine I/O,
Delta logs, retained versions, shuffle or spill. These are byte-rate checks,
not evidence that network or disk is the limiting resource. The separate
225M-edge sample used 310.684 executor CPU-seconds; 5B edges project about
6,904 CPU-seconds for that edge work alone, excluding nodes, reads and graph
action. Its 80%-utilization time is about 45 seconds on 192 cores or 34 seconds
on 256 cores. These calculations show no immediate CPU impossibility for the
larger candidate, but they do not predict full-workload time.

The retained two-node Central US `Standard_D4ds_v5` candidate reference was
about $0.74 for 40 minutes of compute; [Microsoft's size table](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/ddsv5-series)
lists four vCPU per node. Linear same-SKU arithmetic gives roughly
$2.22 at 192 vCPU or $2.96 at 256 vCPU for **five billed minutes of compute
only**. The actual policy, available worker count, runtime, billing interval,
startup/termination, storage, background optimization and full-trial spend
have not been admitted. This arithmetic suggests that a short dedicated run
could fit the owner's preferred $10 trial target, but it is not a current
provider quote or a cost cap. The hard authorized cloud ceiling remains $100.

Next, resolve one actual production runtime/policy and 192–256-vCPU shape,
including cost and a bounded termination plan. Use the existing small
`client_dev` correctness trial to measure startup and selected native behavior.
If the resulting complete 1B/5B projection exceeds 15 minutes, review it with
the owner before a scale run. Otherwise seek the separately required owner
authorization and resource limits for exactly one full load, two point reads,
two distinct scans and one graph-engine export. Preserve the six-role Delta
installation, exact identities, values, multiplicity and full publication
semantics. The weak `source_delivery_id` statistics micro-pair is deferred:
it cannot establish the needed scale-out behavior.

This note changes no release gate or production support claim. The local
synthetic workload can use an environment-appropriate time target; its
18-core estimate is not a production-runtime benchmark. Source measurements
and limitations are in the [R05 redesign napkin](r05-redesign-napkin-math-20261029.md),
with the prior compute reference in the
[A1 cost review](managed-a1-jobs-cost-bound-review-20261029.md).
