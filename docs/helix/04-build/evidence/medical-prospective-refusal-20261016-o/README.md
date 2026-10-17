# Original-medical prospective submission admission diagnosis

After the [successful inspection](../medical-commit-inspection-20261016-o/README.md),
one explicit prospective reconciliation ran on the same exact installed
`4e87f922c2c791aad7e2f8b8f317c1136dcdaba3` wheel. It refused with status 2 after
172.426 seconds, within the unchanged 300-second native budget. No completed
publication or ACK receipt was released.

The retained failure-location record identifies `KeyError` in
`driver._validate_manifest_body` line 2242, through the resumed publisher's
`validate` adapter. That adapter delegated validation before initializing the
active manifest from the original retained artifact. The correction binds only
after exact request/artifact/descriptor correspondence, refuses a conflicting
existing manifest and then delegates unchanged full native validation.

An installed read-only journal observation under the actual original lease after
termination verified all five complete table inventories against the retained
lineage: seven baseline committed operations, no new manifest intent and no
retained prospective authorization sidecar. Its three observations preserve the
same inode and conditional exclusive-storage/no-rollback assumptions. No native
Delta success is inferred from this journal check. Independent ordinary source
reader and protected ACK SELECT observations with closing found head 1, one
batch, protected ACK 0 and no receipt.

`manifest.json` binds the 90 exact original and compressed streams, including
the explicit authorization/configuration, 82 stage records, bounded failure
location and post-refusal journal/source observations. The prepared authorization
was never retained by the submission owner. No paid Databricks work was started;
publication, durable ACK, settled replay and read-only reopen remain pending.
