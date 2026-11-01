---
ddx:
  id: ashlar.evidence.g03-sdk010-r7-txn1-attempt-0c195c3e-failure-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-r7-transaction1-candidate-20261101
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-init-r7-sparse-source-20261101
      kind: informed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# R7 transaction-one attempt closure

The sole transaction-one attempt, `ashlar-g03-sdk010-r7-txn1-0c195c3e7e204191a31cdbdb5d4294af`, remains classified `uncertain-after-dispatch` with `effectsPossible: true`. Its worker failed while opening the source, before Spark startup, with `HostError: evolution-producer-refused`. The recorded `source.open_sdk010` stage duration was 50.955 seconds. No result receipt was produced.

Read-only checks found all six R7 Delta logs still at versions 0 and 1, with current heads at v1. The `operations.sqlite` counts were zero for `local_operation`, `local_plan`, `local_publication_artifact`, and `local_source_plan`. The retained execution status records an empty worker process group. No Spark or Delta transaction effects were observed; the dispatched attempt remains uncertain and is not reported as a native success.

The effect-free gate's successful SDK010 admission uses the pinned Node 26.12.1 runtime, the SDK 0.10.0 package entry, and `typed_graph_values_sdk010.mjs`. The worker's runtime source-open path instead called the generic `ashlar_host.typed_graph_admission.admit_typed_graph`, which invokes `typed_graph_values.ts` with Bun 1.4.3. Its retained invocation config set `ProducerConfig.timeout_seconds` to 30, while the checker is bounded by that timeout and the process-capture layer suppresses child stderr when it raises `evolution-producer-refused`.

An effect-free standalone invocation of the exact Bun checker, with the retained frozen model/request and the same argv/environment, was still running after 38 seconds with no stdout or stderr. With a 65-second diagnostic bound it exited successfully after 50.371 seconds, returned 86,463 bytes on stdout, and produced no stderr. Calling Ashlar's existing generic admission API with only the producer timeout raised to 60 seconds also succeeded and verified the receipt in 67.6 seconds including its producer-custody checks. This isolates the failure to the 30-second checker timeout; it was neither a nonzero checker exit nor an output-size refusal.

The minimal packet-level correction is to construct the future runtime's `ProducerConfig` with a 60-second timeout, its accepted maximum. This preserves the same checker and validation semantics while leaving the attempt's independent 300-second wall bound in force. The isolated worker candidate includes that bounded config override and a focused assertion. It does not reopen or retry the retained attempt. The source-open result and timeout trace should be reconciled with the emerging SDK 0.10.1 cohort before any future native dispatch.

The original initialization's separate read-only reconciliation remains at `r7-init-reconciliation/r7-init-reconciliation.json` (SHA-256 `aa35757da908b74698ff81ea6791b0a4d797bf334f2a620fa5f26ebf66b53034`). It establishes initialization completed on the six tables, while preserving the original `uncertain-after-dispatch` status and missing worker result.
