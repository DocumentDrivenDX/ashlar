---
ddx:
  id: ashlar.evidence.managed-current-spark-receipt-owner-20261023
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: TD-009
      kind: informed_by
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
---

# Managed Spark receipt owner: offline source review

Reviewed 2026-10-23 from repository source only. This note records implemented
offline scope; it is not a managed Unity Catalog execution result or runtime
qualification. No tests or cloud resources were run for this review.

`src/ashlar_host/managed_current_spark_receipt_read.py` contains the internal
`ashlar-managed-current-spark-receipt-reopen/0.1` owner. It accepts an injected
Spark session and explicit bounded configuration, reopens the receipt, manifest
and committed-attempt custody, resolves the four-role vector, captures complete
versioned role bags when supplied an independent oracle, and checks runtime and
six-role inventory again at close. The `.4` path uses bounded native typed-key
hash probes for supplied frozen-prefix inputs. The implementation is read-only
and returns no source-ACK port.

`tests/test_managed_current_spark_receipt_read.py` contains inert fake-Spark
coverage. Its presence does not mean those tests passed in this review. The
offline profile and its checks do not establish a full A1 A/B prefix, accepted
source registrations or genesis custody, actual managed Jobs run/task identity,
current runtime/auth feasibility, retained-file authority through external ACK,
or the live source-holder interval proposed in CONTRACT-001/003/004. The
`/0.1` source-prefix shape does not preserve the independently admitted unchanged
B progress required at A1.

No publisher/verifier Jobs composition, source mutation, protected ACK, or
campaign orchestration is qualified by this source review.
