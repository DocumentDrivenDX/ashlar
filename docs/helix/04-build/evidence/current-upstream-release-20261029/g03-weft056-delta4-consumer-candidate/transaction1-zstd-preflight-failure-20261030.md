---
ddx:
  id: ashlar.evidence.g03-delta4-transaction1-zstd-preflight-failure-20261030
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-weft056-delta4-consumer-candidate-20261029
      kind: informed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# Gate 3 transaction-1 writer-setting preflight failure

On 2026-10-30, the reviewed one-shot G03 attempt completed six-role local
initialization, then failed in the separate transaction-1 Spark session before
graph effects. `ConfiguredLocalAdmission` correctly refused because the fresh
session did not report the required `spark.sql.parquet.compression.codec=zstd`
setting. The retained attempt is
`/private/tmp/ashlar-g03-delta4-txn1-attempts/ashlar-g03-txn1-19c69741b58c4e46a15d4f3645ba7e23`.

The six initialized tables remain at their initial creation/registration
versions (v0/v1); the local SQLite operation journal contains no transaction
operation rows. The worker stopped before coordinator or native graph SQL
dispatch. This attempt is retained as failure evidence and must not be retried.

The bridge now applies `apply_local_writer_settings(spark)` in each new
transaction-1 Spark session after runtime/class checks and before constructing
or invoking `ConfiguredLocalAdmission`. Admission and closing continue to
verify the effective setting without repairing drift. Focused inert tests cover
effective setting before admission and refusal without repair after admission;
no native retry has been run.
