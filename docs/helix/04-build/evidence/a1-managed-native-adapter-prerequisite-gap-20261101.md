---
ddx:
  id: ashlar.evidence.a1-managed-native-adapter-prerequisite-gap-20261101
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
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: ashlar.evidence.a1-managed-source-prefix-owner-gap-20261101
      kind: follows
---

# A1 managed native adapter prerequisite gap

Static review on 2026-11-01 found no admitted inputs for a managed native A1
execution adapter. No cloud/Databricks, native UC, Truss mutation, or ACK action
was run. No source evidence was created or substituted.

The existing `ManagedCurrentTrialConfig` requires the `client_dev` destination,
an explicit task deadline, estimated and authorized DBU bounds including
cleanup, cost estimate and cleanup reserve, an exact runtime/environment tuple,
one serverless task, one concurrency slot, and zero retries. It requires
predictive optimization to remain enabled (`predictive_optimization_disabled`
must be false). `ManagedCurrentServerlessJobSpec` builds an inert job settings
document and `admit_serverless_job_readback` checks a caller-supplied Jobs
readback; neither selects a live environment or submits a task.

The publication task has a distinct source boundary. It now accepts either a
digest-bound preinstallation source bundle or an exact
`ManagedCurrentA1TrialPlan`. The A1 path checks the selected A/B registrations,
genesis predecessor and first admitted A batch before constructing the
incremental driver. The caller-facing trial worker passes that plan through,
and the native receipt reader verifies the `/0.2` source prefix and exact row
bags. The operational interval retains immutable selected prefix anchors for
opening, pre-ACK and close reads. These are prepared code paths, not live A/B
source-owner output or managed execution evidence. The publication task still
labels ACK as external-holder work.

`NativePinPort` adapts the existing `PostgresPins` owner and a separately
supplied source-bound retention admission. It is not a Unity Catalog Delta
table pin implementation. The proposed CONTRACT-001 holder interval also
requires a managed publication reader, the native pin handle, Truss source
custody, and a protected Truss ACK transaction owner. The publisher task must
not receive ACK credentials or manufacture these handles. CONTRACT-004
requires exact descriptor, UUID, schema, protocol and version resolution;
CONTRACT-001's operational A1 profile additionally requires complete native
row reads and independent source-oracle comparison at open, pre-ACK and close.

The pure A1 trial-plan boundary can bind a selected Truss .52 owner result to a
verified genesis manifest row and prepare one request. The task and reader can
consume that prepared plan, but the code does not authenticate the caller's
selection, produce the original live source-owner output, admit a managed
runtime, prove target ownership on the selected workspace, hold native Delta
pins, or establish source/ACK authority. The governing
CONTRACT-003 `/0.2` inputs still require independently selected original A/B
registration objects and genesis cursors, complete initial two-feed Ashlar
progress, the original genesis publication bytes/ID, and retained page,
request, clock and manifest bytes. Those inputs are not available as a live
source-owner output in this environment.

## Required prerequisites for the adapter

1. A separately authorized live Truss .52 A/B owner invocation that returns
   exact current registrations, accepted report/view bytes, both genesis
   cursors, and the first actual A page, with source-owner reread evidence.
2. A separately admitted serverless task environment and explicit resource/cost
   envelope, including effective `client_dev` principal, protocol/runtime pins,
   serverless environment version and immutable dependencies.
3. A selected fresh Unity Catalog MANAGED six-role target owner that proves
   role inventory, UUIDs, ownership, protocols and the initial four-role empty
   vector before creating the exact genesis manifest.
4. A live composition that passes the independently selected A/B owner output
   through the existing A1 plan, task and reader, retaining the original
   request, manifest and `/0.2` package across the holder interval.
5. An independently admitted managed readback and native pin/retention owner
   for the exact four-role vector, plus a separate source-holder ACK owner. The
   publication task must remain unable to ACK.

Until those prerequisites exist, the narrowest safe outcome is a reported gap.
Adding an execution adapter with caller-constructed source selections, fake
runtime observations, synthetic publication receipts, or an assumed pin/read
owner would not establish the required authority or native behavior.
