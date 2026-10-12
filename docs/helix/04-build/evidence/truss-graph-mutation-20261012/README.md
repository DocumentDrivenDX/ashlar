# Public Truss graph mutation

On 2026-10-12, `examples/truss-native/mutate-graph.py` completed one public
mutation/readback/replay workflow in 2.616 seconds with exit 0. It reused only
Ashlar's stopped synthetic PostgreSQL 18.6 cluster from the accepted-schema run,
with the same installed Truss package and psycopg 3.3.6/libpq 18.6.

An administrator configured the accepted `cohort/m` module's distinct reader and
writer roles through Truss's public API, then granted those roles to the existing
ordinary consumer. The original UMF 0.7.0 document and revision `r1` were used
unchanged. No schema reinstall, private Truss API, business-table SQL or fabricated
catalog identity was involved.

One group created objects `1` and `2` at version `1`, and independent edge `3` at
version `1`. New-transaction identity reads matched the original values, versions
and edge endpoints. Reapplying the exact request returned `CommittedReceiptReplay`
with identical semantics and an original receipt token. Complete public results,
including original Base64 evidence, are retained in `mutation-readback.json`.

Connections and the owned server closed before the result was released; the
postmaster marker was absent afterward. This qualifies only this small create,
readback and committed-replay workflow. Updates/deletes, evolution, imports,
Truss feed/ACK, Weft execution and Delta integration remain required separately.
