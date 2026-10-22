# Current Truss 0.10 lifecycle and publication

The receipt records one development-only native lifecycle using accepted UMF
0.10 source on Truss 0.0.44. Three mutation requests committed in one
PostgreSQL transaction as one seven-change feed page. The page was admitted,
planned and published to a retained local Delta installation. A read-only
reconciliation later confirmed that the source checkpoint had advanced and
that the exact destination receipt matched. The reconciliation itself wrote no
ACK and made no Spark writes.

The first continuation worker stopped on an overstrict source-wrapper archive
hash check after the public continuation returned. The wrapper archives differ
only in `safe_watermark_xid`; the page content, cursor pair and identity are
unchanged. The later read-only reconciliation independently checked the
committed manifest, exact rows and source history, all four data-role versions,
all six registered Delta target UUIDs/versions, and the public source receipt.
Private source bytes, page tokens, ACK bytes and connection strings remain in
the retained private task evidence and are not copied into this packet.

The exact Truss 0.0.44 to 0.0.45 source comparison and the isolated official
0.45 wheel probe are in [truss-0.0.45-source-comparison.json](truss-0.0.45-source-comparison.json)
and [truss-0.0.45-public-artifact-probe.json](truss-0.0.45-public-artifact-probe.json).
Ashlar now selects a distinct 0.45 profile pinned to that wheel and runtime
manifest. The public interface probe made no catalog or feed changes. Fresh Truss 0.0.45
source acceptance and complete public report/source/view readback are now recorded
in [truss-0.0.45-source-acceptance.json](truss-0.0.45-source-acceptance.json).
This was a local macOS arm64 source-boundary run; the upstream release lists
`linux-x86_64` as its qualified native platform. The 0.44 native lifecycle
receipt does not transfer, and .45 graph mutation, feed, Delta publication, ACK,
and production authority remain unqualified.

The seven-operation .45 seed-group attempt stopped before commit, and the Ashlar
wrapper discarded the public typed failure. A fresh rollback-only call found no
candidate seed objects and returned `ExecutionFailure(code="cancelled",
sql_state="57014", retry_scope="none")`; the caller transaction rolled back.
The cause is unproven, and this result does not authorize retry or establish a
committed seed. See the [attempt and rollback diagnostic](seed-group-refusal-attempt-02-rollback-diagnostic.json).

This evidence follows [CONTRACT-005's current UMF 0.10 source-custody
boundary](../../../02-design/contracts/CONTRACT-005-schema-intake.md#current-umf-010-accepted-catalog-source-custody)
and retains the four data-role and publication-vector meanings in
[CONTRACT-003](../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md).
The later receipt reconciliation observes the committed source destination
receipt and checkpoint; it is not an alternate publication or ACK authority
under [CONTRACT-001](../../../02-design/contracts/CONTRACT-001-publication-boundary.md).
