# Recovery of fresh medical write-ahead interruption

One recovery operation used the exact original installation, source/effect plans
and request retained in the [preceding attempt](../medical-writeahead-native-20261016-b/README.md).
It did not initialize another installation or append a new source batch. The
installed wheel remains the recorded2b520418 candidate; the intermediate commit
changed evidence only. Local Spark4.0.1/Delta4.0.0 used local[1]/512MiB with
native240s/whole300s and outer330s limits. No cloud or additional container ran.

The supervisor returned terminal exit2 in253.957 seconds; the public failure
was native-deadline with phase begun. No final recovery receipt, publication
artifact or durable ACK was produced. This is not a successful recovery.

A subsequent read-only leased journal observation found all five operation
records committed: the two original attempt records at native versions2/3,
object_current at version2, edge_current at version2 and whole_source_history
at version2. Object current did not receive a duplicate version3 write: recovery
reconciled its original uncertain version2 into a retained local receipt. The
manifest and tombstone remain at registered version1. Source/effect plans remain
one each; no publication artifact exists. These are receipt/commit-log and
journal facts, not a final publication or whole-native logical-read claim.

Opening/closing ordinary reader/ACK checks still observe one source batch at
head1 and protected ACK0 without a receipt. Original scope/signature/privilege
checks completed; the observation submitted no ACK or writes. Full prior source
setup, installation and runtime selections are linked in the preceding evidence.

Exact current Delta JSON/Parquet files and a WAL-aware logical SQLite backup are
archived with supervisor/configuration/request/observer records and dual hashes
in manifest.json. They are evidence snapshots, not runtime grants or an automatic
restore workflow. Live originals remain at
/private/tmp/ashlar-medical-writeahead-native-20261016-b for continuation.
No further native retry was made.

The recovery command now enables its existing bounded, fixed-name stage evidence
through preflight, publication application/observation and all closing. This code
change was made after this run and was not installed or natively exercised here.
Focused controlled composition checks qualify its diagnostics and cleanup;
future native stage prefixes may identify the remaining cost without replacing
checks, raising budgets again or inferring a cause from an incomplete prefix.
