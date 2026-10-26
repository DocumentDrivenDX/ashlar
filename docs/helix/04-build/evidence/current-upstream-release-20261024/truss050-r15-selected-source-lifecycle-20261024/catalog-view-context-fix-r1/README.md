# Truss 0.0.48 catalog-view context correction

This record traces the completed R2 result to the pinned `.48` implementation
and records an Ashlar-side correction for future source-custody calls. It does
not modify or replace the hash-bound R15, R1, or R2 evidence.

## Cause established from source

R2 captured one current catalog-view request and returned
`CatalogViewUnavailable(reason="context")`; its result records zero retries and
the exact helper digest. The helper explicitly set
`READ COMMITTED READ ONLY` and adopted the same transaction as `read_only`.
The pinned `.48` Python implementation calls
`person_catalog_view_basis(selected, count, 16777216)` and maps a SQL `NULL` to
`reason="context"`. For this request, `selected` is `None`, so the historical
revision length branch does not apply.

The installed `.48` `view.sql` returns `NULL` at its first context guard when
`transaction_read_only` is `on` (line 10), before checking the catalog head or
provenance. It later reads the head with `FOR SHARE NOWAIT` (line 13). The
public person wrapper requires the registrar admission family before calling
that function. This source chain explains the exact R2 result. It does not show
whether the view would be available after changing transaction mode or whether
another gate would then refuse it.

Source inputs:

- Pinned Truss 0.0.48 `view.sql`, SHA256
  `857a2c62c284a2462a71acfae88a1fcab840982fe274bcf39a49555692a88614`.
- Pinned `catalog_runtime.py`, SHA256
  `c3090a85ae7a3d4b210509defc31514729670f62fb2755bd78dbc7852a3e31b0`.
- Pinned `person-entrypoints.sql`, SHA256
  `2c99d9894bf5ae278feb227419851107567e5ee0f2a1583e95113f68e29b5164`.
- R2 result, SHA256
  `7c9ad29c219434b57f446ab3b331961e68d6e2c7c1b084653b60600d19b25bf2`.
- Original R15 `.48` helper, SHA256
  `cd9a39b76490db46e93d68edb44f5bc2b9cb978f4a152ded359a04ec66d3efcf`;
  original R2 helper, SHA256
  `42863dfe421b8738fc1721ba40612f7e3eec773a34eba88f790592813b4a8cc6`.

## Ashlar correction

Ashlar's supported historical and current catalog readers already run in an
ordinary `READ COMMITTED` transaction and roll it back after the public catalog
observation. This change makes the required Truss transaction access mode
explicit as `read_write`, including for the current accepted-document and view
calls that must share one transaction. The pinned view function uses only
catalog reads and a `FOR SHARE` row lock; it contains no catalog DML. The
transaction mode permits that lock and does not grant new PostgreSQL role
privileges. The registrar endpoint remains subject to Truss's public registrar
admission check; the separate source-reader path remains read-only.

The hash-bound original helper remains unchanged. The separate
`source_048_context_compatible_helper.py` is a future candidate that applies the
same narrow mode choice to the old source-custody helper: registrar context
checks use `read_write`; accepted-document reads under the source-reader role
and feed-page reads remain `read_only`. It is not wired into the executed R15
worker, and it has not been run against PostgreSQL.

## Verification and limits

`test-receipt.json` records two focused Ashlar library tests and three
non-native candidate controls. They verify transaction-mode selection,
same-transaction accepted-document/view use, rollback, and fail-closed view
handling with mocks or source inspection. No PostgreSQL process, native helper,
IPC resource, feed, migration, publication, or ACK was invoked by this change.

The R2 approval is consumed. This correction does not establish a successful
catalog view, source-custody acceptance, `.50` migration readiness, or any
release qualification. The underlying result remains `context` in immutable R2
evidence; no retry or new diagnostic is authorized here.
