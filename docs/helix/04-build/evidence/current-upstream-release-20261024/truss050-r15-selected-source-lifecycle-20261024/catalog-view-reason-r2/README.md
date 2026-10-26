---
ddx:
  id: ashlar.evidence.truss050-r15-catalog-view-reason-r2
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss050-r15-catalog-view-reason-r1
      kind: informed_by
    - id: ashlar.evidence.truss050-r15-selected-source-lifecycle-20261024
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
---

# R2 catalog-view reason diagnostic proposal

The approved R1 diagnostic stopped before opening a database connection because
its helper asked package metadata for `truss`; the pinned `.48` environment
installs the distribution as `truss-toolkit==0.0.48`. The R1 result is recorded
in [execution-result.json](../catalog-view-reason-r1/execution-result.json).
The corrected helper below checks the installed distribution identity
`truss-toolkit==0.0.48`. It is a new source hash and a new approval payload; R1's consumed approval does not
authorize this attempt. The view's availability and reason remain unknown.

This packet proposes one local startup against only the stopped post-R1 retained
state. It binds the exact tree `3af09760735b08e5ff094596ace02b7ab5762f5d87c2c482f5531c97b79b7c39`
(63,999,633 bytes, 1,248 files) and the R1 execution receipt. Its cold copy is
at `/private/tmp/ashlar-truss050-current-source-2eeecf40-r15/catalog-view-reason-r2/cold-backup/pgdata`;
the one-shot output is in the separate `catalog-view-reason-r2` attempt path.
The R1 backup and execution files are not reused or overwritten.

The candidate keeps the same narrow boundary: existing registrar
`ashlar048_accept_consumer_r1`, one complete catalog-view request in a
`READ COMMITTED READ ONLY` transaction, 2-second statement timeout, and output
limited to availability/reason or available-view length and digest. The
supervisor binds the `.48` wheel/runtime receipt and public API files, the `.50`
startup runtime and `pg_ctl`, selected source, failed R15 result, R1 execution
receipt, helper, classifier, and supervisor. Only the fixed helper is launched.
Migration, source/feed/publication/ACK/seed/continuation operations remain
excluded.

The proposed bounds remain one attempt, zero retries, a 120-second helper
deadline, 1 GiB combined retained/attempt bytes, 16 MiB per log, sequential local
execution, and no cloud resources. The supervisor verifies the current stopped
PGDATA tree, takes and checks the cold copy, and stops PostgreSQL in its cleanup
path. A new approval is required for this exact packet before any startup or
helper execution. The [approval payload](approval-payload.json) is not an
approval marker and this packet has not been executed.

The source-only controls passed under the pinned `.48` Python 3.12.14 runtime.
Before creating an attempt marker or starting PostgreSQL, the supervisor checks
the exact distribution version and required public API in the isolated `.48`
interpreter. Five helper tests also execute the actual version guard in `-I -B`
mode, then stop at an injected mock connection boundary without network access.
They cover available/unavailable metadata and redacted errors. Five supervisor
controls verify the one-shot bounds, exact R1 state binding, the actual
distribution/API preflight, approval-before-start ordering, and read-only
auxiliary preconditions.
These checks establish only the candidate's control shape; they do not show
that the retained catalog view is available.

See [packet-result.json](packet-result.json) for exact file hashes, canonical
approval payload SHA-256, and test commands.
