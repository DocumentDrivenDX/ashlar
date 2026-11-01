---
ddx:
  id: ashlar.evidence.g03-sdk010-init-r6-reconciliation-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-init-r7-sparse-source-20261101
      kind: informed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-009
      kind: governed_by
---

# R6 SDK010 initializer attempt reconciliation

R6 remains **`uncertain-after-dispatch`** in its original supervisor status. This note does not replace that classification or alter any receipt. The retained failure trace and absent runtime outputs independently support the narrower conclusion that R6 refused before Spark startup and before table or Delta effects.

The retained attempt is `/private/tmp/ashlar-g03-sdk010-delta4-init-r6-attempts-20261101/ashlar-g03-sdk010-init-af3d0ceee7724957b3f697a205456804`. Its `execution-status.json` SHA256 is `37168dccbc09f825a9b6c2f162e0ceb7deaa949bceddf2c91a5121641ebde05b` and its classification is unchanged. `initialize.stderr.log` SHA256 is `a2c210b99133cd4db53050122a27a502dcc8f37606c63b2f97e734b52c7dedf6`; the traceback ends in `typed_graph_admission.admit_typed_graph` while `evolution_producer.capture` invokes `process_capture.capture_process` for Git custody. It fails before the configured runtime reaches `open_finite_spark`. The timing receipt records 35,186 ms for `source.open_sdk010` and 35,324 ms for `runtime.initialize`.

The supervisor's `process-group-cleanup.json` SHA256 is `795505c4b6bfa7efc12a226f246da684ed8bc40d39494e114aa02e353fee4e67`; it records a verified empty group. `initialize.stdout.log` is empty (SHA256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`). The attempt has no `initialize-output/` or `init-result.json`; its only installation artifact is the empty `.finite-installation.lock`. These facts do not justify changing the supervisor's uncertainty state.

The cause was a custody deadline against the 60,399-path producer checkout. R6's legacy typed-graph admission checks the producer commit and clean working tree through captured Git subprocesses; the checker repeats clean custody around its import and the Python admission renews custody on close. The configured per-command limit is 30 seconds. Direct pinned Git observations on the full checkout were 18.3 seconds for one clean status and a later status timeout at 35 seconds. This evidence points to full-tree clean-status cost, not a changed source revision.

## Sparse source custody and full preflight

R7 uses a separate sparse checkout at `/private/tmp/ashlar-g03-pinned-umf-producer-sparse-sdk010`. It has the same UMF commit `2ede4c879bea5db13794a6fa30e67c821306c17f`, full commit tree `61eebfbe6dcd63c0768b20e514c250a526113d17`, and `src` tree `f8f7b07a15b0ffbfed94ea5e74767bb54bc712ae`. Its cone paths are `native`, `package.json`, `spec`, `src`, and `tsconfig.json`; `index.sparse=true`. The existing ignored `node_modules` link resolves to `/private/tmp/ashlar-g03-pinned-umf-producer/node_modules`. A sanitized `git status --porcelain --untracked-files=normal` returned empty in 0.15 seconds after sparse-index reapplication. R7 verifies these exact values and the clean status before admission.

The exact isolated-child command for the full effect-free source admission was:

```sh
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python3.12 -I -B /private/tmp/g03_sdk010_source_preflight_once.py
```

It parsed the canonical R7-style initialize invocation through `parse_configured_local`, opened the configured typed-graph source with SDK 0.10 admission, read source metadata, and closed the source. It called no Spark, initializer, installation, or Delta APIs. The open/metadata/close interval was 20.793 seconds (21.70 seconds process wall time). It admitted three batches with the R6-frozen configuration, model, and source hashes. SDK receipt SHA256 was `20ccae34ef3ee1a4d95a5c487492941e0afaa3eeefa7fdb2467efaa2efb31e69`.

The retained temporary admission output is `/private/var/folders/z0/kv_wlyps77b_txh08w55z99w0000gn/T/ashlar-g03-sdk010-admission-preflight-8rtcm5r1/attempt/initialize-output/admission`. `report.json` SHA256 is `e63a909db99e080fc83d03629a891e6ed602c955ecb69abb0a9b7a43ee8de1c3`; `graph-1.json` SHA256 is `cee1a4df50dd6103cea7ac59884a498019387f3607eed300ca79140c9fc502e9`. This preflight establishes a bounded path to complete configured source admission with strict clean-source custody. It does not repair or reclassify R6, authorize native effects, or qualify transaction 1.
