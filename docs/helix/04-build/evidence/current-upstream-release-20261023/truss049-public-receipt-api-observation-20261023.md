# Truss 0.0.49 public receipt API observation

Prepared 2026-10-23 from read-only inspection. This records API availability and
Ashlar source composition; it is not a live Truss feed, publication, ACK, or
managed-runtime execution result.

The inspected installed runtime is the R4 runtime reused by the R5 packet:
`/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926/runtime/venv`
(CPython 3.12.14, macOS arm64, Truss 0.0.49). R5 binds the runtime preparation
profile `ashlar-truss-0.0.49-lifecycle-runtime-preparation/0.2` in its worker
guard ([R5 worker](truss049-r5-readiness-20261023/campaign_worker.py#L115);
[R5 readiness](truss049-r5-readiness-20261023/README.md#L7)). The installed
public `truss/capabilities.py` declares
`acknowledge_feed_with_receipt_in_transaction` and
`reconcile_feed_ack_in_transaction` (installed source:
`/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926/runtime/venv/lib/python3.12/site-packages/truss/capabilities.py`, lines 185–192);
`catalog_runtime.py` implements them as a receipt write and fresh read-only
reconciliation, respectively (installed source:
`/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926/runtime/venv/lib/python3.12/site-packages/truss/catalog_runtime.py`, lines 1615–1655).
The latter reports matched, missing, or conflicting custody and does not write.

The same public capability also exposes `read_feed_in_transaction` and
`reread_feed_in_transaction` (`/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926/runtime/venv/lib/python3.12/site-packages/truss/capabilities.py`, lines 177–183;
`/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926/runtime/venv/lib/python3.12/site-packages/truss/capability_contracts.py`, lines 103–107).
`/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926/runtime/venv/lib/python3.12/site-packages/truss/catalog_runtime.py`
implements bounded whole-transaction reads at lines 1563–1582 and exact
retained-page rereads at lines 1584–1613. The reread checks original content and digest,
prior/next cursors, consumer and registration IDs, writer XID, and outcome; it
requires current source authority and does not advance the checkpoint. Ashlar's
`TrussFeedSessions._read` can request that reread when the ordinary current page
differs, to recover the exact original retained page
([source](../../../../../src/ashlar_host/truss_feed_sessions.py#L150)).
These are read-only page observation mechanics; they do not give Ashlar a held
source custody interval through the later publication and ACK operations.

Ashlar's `TrussFeedSessions` explicitly gates receipt mode on those public SDK
types, builds a bounded opaque receipt from the original request, manifest, and
full vector, and checks exact identity, page, request, publication, receipt
bytes, and digest ([receipt setup and exact comparison](../../../../../src/ashlar_host/truss_feed_sessions.py#L93),
[receipt ACK/reconcile path](../../../../../src/ashlar_host/truss_feed_sessions.py#L275)).
It resolves the destination publication and holds the supplied pin context before
the source transaction; the caller commits the receipt ACK. This confirms an
existing software composition path, not live source or destination authority.

The implementation does not establish a renewable, source-owned interval that
keeps managed publication reads, the native pin, and Truss source custody live
through ACK commit, exact reconciliation, and independent close. `CONTRACT-001`
requires that interval and leaves the proposed source-holder ports in draft
([receipt ACK boundary](../../../02-design/contracts/CONTRACT-001-publication-boundary.md#L1462),
[proposed live custody ports](../../../02-design/contracts/CONTRACT-001-publication-boundary.md#L1571));
`TD-009` likewise says the renewal handoff is a prerequisite, not an inferred
permission ([TD-009](../../../02-design/technical-designs/TD-009-managed-spark-receipt-verification.md#L70)).
The installed API's presence therefore establishes neither current source/feed
custody nor managed publication authority. No live Truss or managed execution is
claimed here.
