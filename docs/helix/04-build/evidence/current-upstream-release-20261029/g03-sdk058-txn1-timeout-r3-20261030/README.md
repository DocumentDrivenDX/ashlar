---
ddx:
  id: ashlar.evidence.g03-sdk058-txn1-timeout-r3-20261030
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk058-local-delta4-txn1-packet-20261030
      kind: informed_by
    - id: ashlar.evidence.g03-sdk058-local-cutover-candidate-20261030
      kind: informed_by
    - id: ashlar.evidence.g03-sdk058-source-correspondence-20261030
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# G03 SDK058 transaction-1 timeout: consumed attempt r3

The one-shot attempt `ashlar-g03-txn1-f66791eded7f449eb4b21ef60cd50bca` reached its 450-second supervisor deadline after successful six-role initialization and source admission. Its approval was consumed; the attempt was not retried. The retained attempt root is `/private/tmp/ashlar-g03-sdk058-delta4-txn1-attempts-20261030/ashlar-g03-txn1-f66791eded7f449eb4b21ef60cd50bca`. Dispatch binds packet SHA `61b6002030051592e8e86130a26957900b2c29823e83b6b8d26c7497b2322b69`, permit SHA `2b44b12c8250db25a7efb85721918fcadd7acf3dd3ab9e1490d250bab988d4e2`, and prepared input SHA `04267b3a34671e51347c06e2e6652740d2243c780c3959fa55583efd6c88a342`.

The durable SQLite journal contains `prepared` and `applying` phase rows for request `065ce73eb7dbb1b4438ca33f1e61541f9035a0e4a5fde1bf11ac05f7bda07355`, one local plan, both metadata reservation rows, and no `local_operation` row. The `local_delta4_metadata_operation` table is absent. Each of the six Delta logs ends at version 1; that commit has metadata actions only and no `add` actions. The last durable boundary is therefore after metadata reservation and before the first metadata operation. No transaction1 runtime report or stage result exists. Initialization and source admission succeeded; the graph effects did not begin.

The retained transaction log contains Spark startup and completed stages 0 and 2 but no application traceback. The SDK058 checker left temporary `model.json` and `request.json` at `12:48:26` local time, about ten seconds before the supervisor deadline. Those files are created immediately before the Node correspondence checker runs. This is evidence that a checker invocation was underway near timeout, but it does not establish that it caused the full delay or identify its exact caller. The source admission report records 3 batches and 13 records, source SHA `9edb7566f82324fadb1e7cf1ba0eaf22691096a26209e938c83730d096d4882b`, model SHA `19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b`, and SDK058 receipt SHA `8898ecbf39149fb92a92066535b25140818d3d0e2967ccaed0ff59f0b98745b2`.

## Effect-free diagnosis and correction candidate

Three fresh, bounded runs of the exact SDK 0.5.8 source checker over the retained packet inputs took 2.871s, 2.474s, and 2.517s. Each produced the same 86,527-byte receipt and reported `sparkStarted: false`; the timing receipt is [source-admission-timing.json](source-admission-timing.json). Separate substep measurements from the same pinned environment were 0.017–0.022s for SDK file custody and 0.0021–0.0025s for pure Python policy reconstruction and receipt replay. These are current effect-free timings, not measurements recovered from r3.

The current code path previously called full SDK058 correspondence from every source `renew`; publication helpers invoke source renewal repeatedly around reservation and metadata work. Runtime attestation and forced six-role native closing checks also repeat. No r3 counters or per-call timings were retained, so the number of checker/native calls and their respective wall-time contribution remain unknown. Repeated checker calls are a plausible bottleneck, not a proven sole cause.

An additive correction now retains the exact SDK custody bytes and file identity tuple at opening. Inner renewals recheck that exact custody before and after rebuilding the source policy from the retained receipt and rederiving the request; the pure Python verifier refuses receipt/source/request divergence. Final `source.close()` reruns the full Node correspondence and compares the returned receipt and custody tuple. The historical default path is unchanged, and native closing checks are unchanged. Focused tests use a mocked checker counter: opening calls it once, repeated `renew`/`spec`/`admit_request`/`metadata` do not call it again, final close calls it once more, and changed custody refuses without replay. This validates control flow and fail-closed behavior only; it does not reproduce r3 timing or validate Spark execution.

The focused SDK058 tests and two historical configured-source regressions pass in the pinned CPython 3.12.14 environment. One broader combined invocation also attempted `test_graph_query_invalid_registry_refuses_before_spark_and_releases_actual_lease`, which errors during test fixture construction because the test's empty JAR path no longer matches the current runtime-config inventory requirement; Spark was not opened. The next native attempt remains unreviewed and is not authorized by this note. No Spark, Delta, cloud resource, or retry was run for this correction.
