---
ddx:
  id: ashlar.evidence.truss050-r15-catalog-view-reason-r1
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss050-r15-selected-source-lifecycle-20261024
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
---

# R15 catalog-view reason diagnostic proposal

This is a proposal for one bounded, read-only diagnostic of the failure recorded
in the [R15 execution result](../execution-result.json). It is not executed and
does not use the consumed R15 approval. No PostgreSQL startup, helper run, IPC
probe, approval marker, backup copy, source/feed write, or release action has
occurred for this proposal.

The existing R15 supervisor cannot be reused directly: its only execution path
launches `campaign_worker_r15.py --execute`, which performs the full migration
and lifecycle. The candidate below is a narrow supervisor and helper pair. The
supervisor starts the retained PostgreSQL 18 directory through the pinned R15
Truss `LocalPostgres` runtime; the helper runs under the existing Truss 0.0.48
read-only runtime and asks for exactly one complete catalog view as the existing
`ashlar048_accept_consumer_r1` registrar. It sets a read-only transaction and a
2-second statement timeout. It outputs only `available` metadata or the actual
`CatalogViewUnavailable.reason`; it does not read accepted source bytes, feed
pages, or ACK state. The candidate helper imports the separately tested
`require_catalog_view_bytes` classifier.

Before startup, the supervisor requires the exact stopped post-R15 PGDATA tree
from commit `4c004980` (63,998,352 bytes, 1,248 files, tree SHA-256
`abb464cc096dfce6bc6bd2720f6f6a17aa92c08af5a601146736ca71a308e6d2`) and takes
a new cold, byte-verified copy at
`/private/tmp/ashlar-truss050-current-source-2eeecf40-r15/catalog-view-reason-r1/cold-backup/pgdata`.
This copies the current stopped state; it does not restore the earlier backup.
The retained database remains at its existing path. PostgreSQL startup and
shutdown may change internal PostgreSQL files; the new copy preserves the exact
pre-start state. No application source, feed, publication, or ACK write is in
the helper path.

The proposed approval is one attempt, zero automatic retries, a 120-second
read-only child deadline, 1 GiB combined retained/attempt bytes, 16 MiB logs,
sequential local execution, and no cloud resources. The pinned `LocalPostgres`
runtime has its existing bounded 60-second startup and shutdown calls; shutdown
is mandatory and may continue after the child deadline. A stale PGDATA hash,
existing attempt output, startup/cleanup problem, output-limit breach, or
deadline expiry stops the attempt. The wrapper reuses the pinned Truss `.50`
startup/cleanup runtime and R15 retained-PGDATA/backup limits; its only child is
the fixed read-only helper. It binds exact R15 source/runtime receipts, Truss
`.48` catalog API files, Truss `.50` startup runtime, helper/classifier/supervisor
hashes, and the pinned `pg_ctl` hash. The runtime lock and empty private socket
directory must already exist with their recorded ownership and mode; they are
reused, and the socket directory must be empty again after shutdown. PostgreSQL
may append to `PGDATA/log`, which shares the 16 MiB log cap. The approval payload
is [approval-payload.json](approval-payload.json); canonical payload SHA-256 is
recorded in [packet-result.json](packet-result.json).

The focused nonnative controls are in
[`test_readonly_reason_controls.py`](test_readonly_reason_controls.py). They
check payload bounds, the helper's single catalog-view path and read-only SQL,
that the runner launches only the fixed helper, and that the worker plus its
hash-pinned sibling classifier import successfully under the exact isolated
`.48` invocation (`python -I -B`) without calling `capture()` or connecting to
PostgreSQL. The helper/classifier tests from the preceding candidate are also
run under the `.48` helper interpreter. Five supervisor-control tests and three
classifier tests passed. These tests establish only the proposed control shape.
The underlying view availability remains unknown until a separately approved
diagnostic is run.
