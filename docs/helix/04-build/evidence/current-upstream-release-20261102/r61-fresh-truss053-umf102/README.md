# R61 fresh Truss / UMF local qualification

Status: the R61 end-to-end qualification is still incomplete. The reviewed
candidate pins Truss 0.0.53, UMF SDK 0.10.2, Core 0.10.6, declared model 0.10.0,
Weft 0.4.56, Spark 4.2, and Delta 4.4.1. No release, tag, publication, cloud,
scale run, or production authority is claimed.

## Focused execution history

Attempts 1–6 retained their bounded refusal and cleanup receipts. They exposed
supervisor setup, PostgreSQL bootstrap, and Truss install transaction issues;
none performed a source ACK, Delta publication, Spark action, or graph export.

Attempt 7 completed the fresh PG18 copy, public Truss 0.0.53 install, UMF 0.10.0
acceptance, and genesis consumer registration. The first public page read was
cancelled in `truss.runtime_feed_revision` after two seconds because the Ashlar
helper's adopted host kept Truss's 2-second default despite setting SQL's
statement timeout to 15 seconds. The PostgreSQL log records SQLSTATE 57014.
The supervisor stopped PostgreSQL and confirmed no Delta root or source ACK.
Peak RSS was 294,649,856 bytes; bounded output was 64,167,004 bytes; elapsed
time was 207.683 seconds.

The focused fix aligns the read helper's adopted host statement and operation
timeouts with its existing 15-second SQL bound and retains the structured Truss
error. The next attempt resumes a verified copy of attempt 7's stopped,
accepted, genesis-only source state. It checks the failure and cleanup receipts,
model/setup/origin hashes, PostgreSQL tree and control identity, lease and
socket, then reads the existing consumer page and accepted catalog. It does not
reinstall or reaccept the model, repeat consumer registration, or replay any
uncertain publication or ACK.

## Current bounded scope

[scope.json](scope.json) binds the new attempt 8 output root, exact retained
attempt 7 state, runtime artifacts, worker, and supervisor. Its fixed limits
are 900 seconds including cleanup, 4 GiB aggregate RSS, and 512 MiB disposable
output, with one dispatch. Exact candidate wheel rebuilding/reinstallation and
the resulting updated artifact hash are still required before dispatch.

The intended continuation remains one seven-operation public seed, native Weft
reads, one continuation and protected ACK, original-owner graph release and
custody, and one GraphFrames action over those exact bytes. The separate
GraphFrames runtime must load the same reviewed candidate wheel. No broad test
matrix is part of this qualification.
