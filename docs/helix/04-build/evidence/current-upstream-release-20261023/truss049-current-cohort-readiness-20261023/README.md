# Truss 0.0.49 current-cohort readiness packet

Status: **frozen static packet; no preflight or native action run; not current for lifecycle execution**. This
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

This packet remains immutably bound to source snapshot `6986e832`. Ashlar main subsequently advanced to `78625f35` with only the public UMF core URL changing from release `umf-v0.5.4` to `umf-v0.5.5`; the core wheel bytes and digest are unchanged. That pin refresh does not rebind this packet. Any future lifecycle requires a complete worker/supervisor/preparation/bridge rebind to final release pins and renewed review/owner approval.

The later public UMF 0.5.6 adoption also changes Ashlar's core distribution
pin to 0.10.1 and rebuilds the embedded bridge from the released source. Those
changes are outside this frozen R6 source snapshot. Its live-HEAD guard is
expected to reject a current checkout after that adoption; a passing historical
snapshot check cannot authorize execution from current main.

The historical R6 preflight command is recorded in `preparation.json`: invoke the private
R6 virtualenv's Python with `-I -B`, the R6 `run_supervisor.py`, and only
`--preflight`, from the private R6 root. Do not invoke it before the required
rebind and approval. This packet has **not** run that
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
