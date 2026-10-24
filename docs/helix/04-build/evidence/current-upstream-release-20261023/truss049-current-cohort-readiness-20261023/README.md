# Truss 0.0.49 current-cohort readiness packet

Status: **static packet assembled; no preflight or native action run**. This
packet rebinds the selected Ashlar source objects at `6986e832` to the fresh,
isolated CPython 3.12.14/macOS arm64 runtime with Truss 0.0.49, UMF core 0.10.0,
UMF access dev7, Weft 0.4.53, Spark 4.2.0 and Delta 4.4.1. The current Weft
source/wheel selection and preserved lineage are in the [R5 refresh proposal](../truss049-r5-current-cohort-refresh-20261023/README.md);
its selected source objects and refresh-proposal digest are checked here.
The runtime package provenance and install receipt are in the
[current-cohort runtime evidence](../truss049-current-cohort-runtime-20261023/README.md).

The detached source snapshot, private worker and supervisor, current runtime
receipt, bridge-readiness pin, original seed request and preserved 0.48
read-only helper are assembled under the private root recorded in
[`preparation.json`](preparation.json). A new R6 migration attempt UUID and
destination identity are reserved there. The existing accepted source,
feed registration, consumer, roles, genesis checkpoint and seed bytes retain
their previous identities. The retained PostgreSQL data remains a reference to
the R5 preparation's JSON pointer; its bytes have not been copied, reset, or
re-observed. R5's Weft 0.4.52 preflight and approval payload are historical and
must not be reused.

The exact future command is recorded in `preparation.json`: invoke the private
R6 virtualenv's Python with `-I -B`, the R6 `run_supervisor.py`, and only
`--preflight`, from the private R6 root. This packet has **not** run that
command. The current preflight code checks pinned artifacts/runtime/bridge,
PGDATA directory/version and absence of `postmaster.pid`, compatibility imports,
process/SysV admission and exact worker/supervisor approval-payload parity. It
does not query the current PostgreSQL catalog, feed or consumer checkpoint;
those source/consumer custody reads remain required after a separately
authorized lifecycle start and before migration or mutation. A preflight result
would not authorize `--execute`; execution needs a separate explicit owner
decision after review of the exact packet and live observations.

No PostgreSQL or Spark process, Truss campaign, migration, backup, source/feed
mutation, publication, ACK, execution-approval file, or started marker was
created. Resource limits carried forward for any separately authorized
campaign are 600 seconds, 1 GiB combined retained/attempt/worker-log bytes,
16 MiB per worker log, local Spark only and zero retries. See
[`check_current_cohort_packet.py`](check_current_cohort_packet.py) for static
checks. `artifacts.sha256` binds the review packet files; the private source
snapshot is bound by immutable Git commit/tree/object IDs.
