# Installed medical inspection and held-writer refusal

The exact installed `604f1713ab13018d2c9e76a9aca8eb4590f37d71` wheel completed
its read-only inspection in 231.634 seconds, exit 0, with closing success and
76 stage records. It observed original manifest UUID
`4529f3e4-1000-4c19-8bd2-89744a7a3a8d` at version 1 with no rows, protected
ACK 0 and no original manifest submission intent. The selected authorization
binds that report, original applied artifact and unchanged proposed manifest.

The subsequent reconciliation exited 2 after 294.770 seconds. Its retained
failure location identifies `PermissionError` at the attempted nested
`NativeDriver.writer` inside `MedicalProspectiveManifest.reconcile`: the shared
publisher already held that writer interval. The public cause is `child-exit`,
not a native deadline. No reconciliation success receipt was released. Its
82 stage records and failure location are retained unchanged.

After termination, an installed observation under the actual original lease
validated the complete conditional journal lineage with seven original committed
operations and no prospective authorization sidecar. Three independent ordinary
source/ACK observations, each with closing, found head 1, one batch, ACK 0 and no
receipt before inspection, after inspection and after reconciliation. These
postchecks qualify their observed source/journal state; they do not establish a
completed native publication, ACK or production authority.

Native and whole-worker budgets were 300 and 360 seconds, with local[1]/512 MiB
and an outer 390-second harness bound. This is one inspection/reconciliation
observation, not a performance comparison or architecture gate. The later
held-writer correction and extended functional-run ceilings were not executed
by this wheel.

`manifest.json` binds 177 exact original streams and their deterministic gzip
copies: installed/preparation selections, configurations, authorization, both
attempts and stdout/stderr, successful inspection receipt/report/proposal,
failure location, three source observations, the post-refusal journal
observation, and all 158 inspection/reconciliation stage records. Original
conditional exclusive-storage, complete-known-execution and no-out-of-band-
rollback assumptions remain explicit. Native commit, protected ACK, settled
replay and read-only reopen remain unqualified by this attempt.
