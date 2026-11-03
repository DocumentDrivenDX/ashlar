---
ddx:
  id: ashlar.evidence.managed-a1-serverless-runner-20261102
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: ashlar.evidence.a1-managed-native-adapter-prerequisite-gap-20261101
      kind: follows
---

# Managed A1 serverless runner — 2026-11-02

`tools/run_managed_a1_serverless.py` is an inert-by-default candidate
controller. It pins the workspace, trial catalog/schema namespace, run-as
principal, notebook and dependency artifact hashes, serverless environment
versions, task timeouts, retry policy, total deadline, and cost estimate.
`--print-payload` is local-only. The `--launch` path is intentionally gated by
a digest-checked local qualification receipt and exact remote artifact
readback. This evidence does not claim that a live qualification or managed
trial has occurred.

The controller and task contract are still under construction. The intended
managed sequence is three separate, sequential serverless Jobs runs: fresh
six-role/genesis initialization from the exact detached A/B package, one A1
publisher, then one independent verifier. The same source-holder child must
retain original A custody across the latter two runs; only the durable
verifier receipt may be bound to its exact selection and dispatched to the
single protected ACK. A new owned managed Volume is the private transport for
bounded exact source evidence. The complete owned schema and Volume are
removed only after exact reconciliation and inventory checks.

The published controller source currently includes strict one-task Jobs
readback, artifact hash preflight, cost/deadline admission, private Volume
readback, publisher receipt/vector checks, independent terminal proof checks,
and guarded cleanup helpers. The detached A/B admission adapter
`src/ashlar_host/managed_a1_detached_admission.py` reconstructs only publisher
plan inputs from original public evidence bytes; it creates no Truss source or
ACK authority. These components have not yet been wired into the complete
three-run launch path.

No Databricks API request, schema or job mutation, or compute launch was made
for this implementation. Syntax compilation of the controller and detached
admission adapter passed. A local end-to-end admission check awaits the
source-holder A/B codec and frozen local qualification inputs. The remaining
review boundary includes concrete initializer/publisher/verifier notebook
entrypoints, same-session source-holder process integration, and the managed
Spark read/pin/policy observation owners. The controller must remain
non-launchable until those pieces and local qualification pass.

The cost value is an admission estimate, not a platform invoice cap. The
approximately 60 DBU/hour executor estimate is only an approximation and is
not an account spending limit. Predictive optimization remains enabled.
