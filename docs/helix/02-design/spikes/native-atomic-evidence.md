# Catalog-commit atomicity probe

On dbw-aidev-cus SQL channel 2026.38, two tiny isolated Delta tables in
`client_dev.ashlar_atomic_20261005_g1` were created with
`delta.feature.catalogManaged=supported`. Both DESCRIBE DETAIL responses include
`catalogManaged`. No workspace preview setting or existing table was changed.

A BEGIN ATOMIC block updated an object carrier and inserted its old/new carrier
into a journal, then deliberately raised SIGNAL SQLSTATE 45000. Native failure
reported the intended signal. Subsequent reads verified the original object and
an empty journal: both writes rolled back. A successful block then committed
both writes; the resulting object, journal and retained text exactly matched
expected strings, including int64, high-precision decimal, explicit null and an
unknown nested retained value. No numeric conversion was used for these bags.

This proves scoped two-table commit/rollback availability, not full publication
atomicity, reader isolation under load, endpoint constraints, conflict/replay
semantics, progress/version-vector recovery or throughput at graph scale.
CONTRACT-003 and ADR-001 remain proposed; no production dependency on catalog
commits has been selected. Immutable publication readers still require an
established table-version vector. A native transaction alone does not solve
that mapping or external-engine snapshot correspondence.

Enabled feature lists also include inCommitTimestamp and vacuumProtocolCheck
in addition to deletionVectors/rowTracking/v2Checkpoint. Exact lists and native
statement responses are retained under
`SPIKE-001-table-layout/out/native/ashlar_atomic_20261005_g1/`. Reader protocols,
PuppyGraph access, GraphFrames runtime and bounded Fabric export must be checked
for this alternative. Prior ordinary Delta fixtures lack catalogManaged and
therefore cannot be treated as multi-table transaction targets.

The current official documentation labels catalog commits Beta and specifies
managed-table/runtime requirements: [catalog commits](https://learn.microsoft.com/en-us/azure/databricks/tables/features/catalog-commits),
[transactions](https://learn.microsoft.com/en-us/azure/databricks/transactions/).
Availability here is native evidence for this workspace/channel, not a blanket
claim about other deployments or third-party readers. The intended next test is
complete publication validation and interruption/recovery using this feature.
