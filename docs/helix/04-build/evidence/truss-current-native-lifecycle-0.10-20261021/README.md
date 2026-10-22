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
receipt does not transfer. The acceptance-only receipt predates the seed run;
the bounded record/link creates are documented separately below, while broader
.45 graph lifecycle, feed, Delta publication, ACK, and production authority
remain unqualified.

Attempt 02 remains a cancelled seven-operation group with confirmed rollback
and no seed objects; its [rollback diagnostic](seed-group-refusal-attempt-02-rollback-diagnostic.json)
is unchanged. A separately approved fresh attempt 03-r2 committed its seven
public record/link creates, read back four objects and three edges, and
successfully replayed the exact committed receipt. The worker then exited 1
while freezing the later request offline: its private helper expected a nested
identity field, while Truss's public object snapshot exposed flat fields. A
reviewed offline recovery used that retained committed report to freeze the
five-operation update/delete request; it did not apply those operations. No
ACK or destination publication was created. The compact, digest-pinned
[attempt 03-r2 evidence](truss-0.0.45-seed-and-offline-freeze-attempt-03-r2.json)
records both outcomes. This remains a local macOS arm64 run outside the
release's listed `linux-x86_64` native qualification, and does not qualify
feed, publication, ACK, query, or production authority.

This evidence follows [CONTRACT-005's current UMF 0.10 source-custody
boundary](../../../02-design/contracts/CONTRACT-005-schema-intake.md#current-umf-010-accepted-catalog-source-custody)
and retains the four data-role and publication-vector meanings in
[CONTRACT-003](../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md).
The later receipt reconciliation observes the committed source destination
receipt and checkpoint; it is not an alternate publication or ACK authority
under [CONTRACT-001](../../../02-design/contracts/CONTRACT-001-publication-boundary.md).
