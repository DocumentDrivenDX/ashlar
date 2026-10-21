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

The exact Truss 0.0.44 to 0.0.45 source comparison and proposed adoption boundary
are in [truss-0.0.45-source-comparison.json](truss-0.0.45-source-comparison.json).
The 0.44 result does not transfer to 0.45. The current catalog adapter enforces
an exact 0.44 profile; 0.45 needs its own versioned source/runtime profile and
fresh-source qualification before any adoption claim.

This evidence follows [CONTRACT-005's current UMF 0.10 source-custody
boundary](../../../02-design/contracts/CONTRACT-005-schema-intake.md#current-umf-010-accepted-catalog-source-custody)
and retains the four data-role and publication-vector meanings in
[CONTRACT-003](../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md).
The later receipt reconciliation observes the committed source destination
receipt and checkpoint; it is not an alternate publication or ACK authority
under [CONTRACT-001](../../../02-design/contracts/CONTRACT-001-publication-boundary.md).
