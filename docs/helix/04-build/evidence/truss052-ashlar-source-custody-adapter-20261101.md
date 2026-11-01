---
ddx:
  id: ashlar.evidence.truss052-ashlar-source-custody-adapter-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.source-holder-a1-live-port-gap-20261024
      kind: informed_by
---

# Truss 0.0.52 Ashlar source-custody adapter — 2026-11-01

This isolated candidate adds an Ashlar adapter for Truss 0.0.52's public
same-session source-custody API and a separate local-development source
selection. The candidate is based on Ashlar `e60e2a8f` and is on branch
`codex/truss052-feed-source-ashlar-adapter` in the isolated checkout. The
Truss runtime gate is the exact installed public status
`truss-catalog-runtime/0.75-candidate`, manifest SHA-256
`5ecb599f65d64e6656bf6c83c30cea6e2aefd488cb3fe821b749458d5e9b3522`, and
distribution version `0.0.52`. The staged local wheel is
`umf_truss-0.0.52-py3-none-any.whl`, SHA-256
`00d8b11abc83d9c84fa131ae6f3eef23ecad9d321d4b04a13641ed31c77e9747`; the
GitHub release asset upload remains pending.

`Truss052SourceCustodySession` calls the public runtime status check on the
retained connection before materializing the exact `.52` selection. It keeps
one borrowed psycopg connection, host and reference assembly across writable
open/renew/source-ACK/close transactions and read-only reconciliation. An
unknown source-ACK commit is one-shot and permits reconciliation only; a lost
connection is never replaced. The local-development binder requires public
Truss acceptance, consumer, installed-status and feed-page result types, then
binds their exact registration/report/page/cursor identity to canonical
destination-registration and ACK-scope bytes. The scope includes the selected
publication ID and a one-page bound. It does not inherit the R04 A/B schedule
identity. Its separate publication binder requires the exact canonical local
`.4` manifest, request, full four-role pin vector and receipt; an issuer-guarded
expected post-state from the publisher; and an independently read local Delta
observation with contiguous log history, exact UUID/version/head and actual
row-bag digest/count for each role. R04 continues to require its managed-reader
observation. The local feed label is an Ashlar selection binding; Truss's
public page exposes consumer registration/cursors, not a separate named feed
object. The candidate therefore makes no claim that Truss independently
registered that local feed label or that local Delta readback grants managed
production authority.

The adapter preserves Ashlar ownership of destination registration and ACK
scope meaning. It does not execute the external protected destination ACK,
grant production source authority, qualify a managed Unity Catalog read, or
claim native PostgreSQL behavior. Production release still requires a
separately reviewed composition with the actual destination ACK, managed-read
and native-pin owners, followed by the scoped native lifecycle evidence.

Focused tests were run with the selected Python 3.12 runtime:

```text
PYTHONPATH=src:tests /private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/bin/python -m unittest test_truss_local_dev_source_custody_selection test_truss_source_custody_selection test_truss052_source_custody test_truss_current_catalog -v
Ran 35 tests — OK
```

These are fake-port and source-selection checks only; no local PostgreSQL,
cloud, Delta write or protected destination ACK was run for this adapter.
