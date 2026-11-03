# R61 fresh Truss / UMF local qualification

Status: end-to-end qualification remains incomplete. The candidate cohort is
Truss 0.0.53, UMF SDK 0.10.2, Core 0.10.6, declared model 0.10.0, Weft 0.4.56,
Spark 4.2, and Delta 4.4.1. The reviewed Ashlar wheel is from candidate
`6a008d1cdaf0e6b82e63bc11ebc52c3a59002aab`, SHA-256
`b2f31bc58c7dff226f87075c6a5398e0a2056d9bbbf97e8ef7e3a12de6f81433`; both
isolated Python environments have that exact wheel installed and verified.
No release, tag, publication, cloud, scale run, or production authority is
claimed.

## Focused execution history

Attempts 1–6 retained bounded refusal and cleanup receipts. They exposed
supervisor setup, PostgreSQL bootstrap, and Truss install transaction issues;
none performed a source ACK, Delta publication, Spark action, or graph export.

Attempt 7 completed public Truss installation, UMF 0.10.0 acceptance and
genesis consumer registration. Its first page read hit SQLSTATE 57014 after two
seconds because the adopted host retained Truss's 2-second default while the
helper set SQL's statement timeout to 15 seconds. The fix aligns both host
timeouts to the helper's existing 15-second bound.

Attempt 8 copied the exact stopped install and accepted source without
reinstall or reaccept. It reached the public feed read, then refused because the
resume guard incorrectly required an empty interval. One focused public read
captured the actual page in
[attempt8-page-readback.json](attempt8-page-readback.json), SHA-256
`236e00f5cca21b87354e58104c7ff7ba99bcec28dfc39084fb53550bfbb1cdbb`. It is a
31,938-byte transaction page, writer XID 758, with prior cursor 0 and next 759.
It carries one accepted schema revision plus two catalog prerequisites, with
zero entity changes, sources, or reservations. PostgreSQL closed successfully;
there was no Delta write, Spark action, or ACK. Truss `.53` intentionally returns
an advanced safe-watermark cursor for empty intervals, so prior and next cursor
bytes need not match when no writer transaction exists.

The next attempt resumes a verified copy of attempt 8's stopped,
accepted, genesis-only source state. It binds the failure/cleanup receipts,
model/setup/origin, full PG18 tree/control identity, lease/socket and exact page
readback. The intended order is to publish and protectively ACK the
metadata-only catalog page as graph genesis, apply and publish the seven-op
seed as its continuation, run native Weft reads, then publish and ACK the
two-record continuation while capturing the original-owner graph release and
custody pair. GraphFrames will consume those exact owner-captured bytes with
the same reviewed wheel.

## Current bounded scope

[scope.json](scope.json) binds the new output root, exact retained attempt 8
state, runtime artifacts, worker and supervisor. Limits remain one attempt,
900 seconds including cleanup, 4 GiB aggregate RSS and 512 MiB disposable
output. The new worker order has passed syntax and diff checks; native dispatch
waits for independent review of the metadata-genesis sequence and exact updated
scope.

No broad test matrix is part of this qualification.
