# Fresh medical write-ahead native attempt

Current wheel from 2b520418 initialized a fresh original-medical Delta
installation, then attempted publication once. Initialization exited0 in
31.612 seconds. Publication exited2 in125.879 seconds with native-deadline,
phase begun. There is no successful publication or recovery claim.

The existing dedicated PostgreSQL17.9 container was reused with a fresh medical16b
database, three distinct ordinary roles and a new installation/source/ACK scope.
No container, cloud compute, paid capacity or grants outside this local fixture
were created. The previous medical16a interrupted attempt was not modified.
Spark4.0.1/Delta4.0.0 used local[1], a512MiB driver, native120s/whole210s limits
and an outer240s supervisor. SDK closure hashes reuse reviewed earlier hashes
with current metadata; this is not a fresh complete payload rehash or OS closure.

The new write-ahead path retained one exact source plan and one complete effect
plan before submitting prepared. Read-only inspection under the actual existing
installation lease found two committed attempt operations (native versions2/3)
and one submitted object_current operation without a local receipt. The native
object table has version2; other graph roles and manifest remain at registered
version1. No publication artifact was stored. The stored effect plan digest and
operation intent digests were checked; compressed committed receipts were decoded
with the installed lossless codec. Commit-log observation is not a logical row
read or independent native recovery admission.

Ordinary reader and ACK sessions independently verified roles, privileges and
source-signature/scope correspondence at opening and closing. Source head is1
with one batch; protected ACK remains0 with no receipt. No observation advanced
the checkpoint, appended source events or invoked recovery.

Unlike the previous interruption before write-ahead preparation, the exact source
and effect originals now exist. The next operation is recovery of this retained
uncertain commit, not a new publication or reconstruction of missing custody.
The original request is publication-output/request.json in the retained private
run root /private/tmp/ashlar-medical-writeahead-native-20261016-b.

The archive includes supervisor status, selected configuration/runtime records,
source setup/observation, installation handoff, exact original request, read-only
journal observations, a WAL-aware logical SQLite backup, and all current native
Parquet/Delta JSON files. These are evidence snapshots, not runtime authority,
a supported restore procedure, or proof of source-resource retention. The live
original journal/tables and source fixture remain untouched for continuation.
No automatic retry was performed. manifest.json binds exact raw/compressed bytes;
operator scripts retain historical temporary paths. Independent review verified
fresh source/installation correspondence and installed write-ahead modules.
