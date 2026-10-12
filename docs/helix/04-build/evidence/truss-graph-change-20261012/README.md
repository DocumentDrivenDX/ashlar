# Public Truss update and deletion

On 2026-10-12, `examples/truss-native/change-graph.py` completed one public
workflow in 1.082 seconds with exit 0, using the existing Ashlar-owned synthetic
cluster and ordinary module-authorized consumer. PostgreSQL 18.6 and the installed
Truss/psycopg 3.3.6/libpq 18.6 tuple were unchanged. No install, acceptance or
permission setup was rerun.

Current public reads admitted the selected identities and versions. Edge `3`
was updated to version `2`, retaining `note = "changed by Ashlar example"` and
order key `ordered-first`; full readback preserved its identity and endpoints.
A second group deleted that edge and objects `1` and `2`. New-transaction reads
returned absence for all three. Both original requests returned committed
receipt replays with identical semantics and original receipt tokens.

Complete public results and original Base64 evidence are retained in
`change-readback.json`. Connections and the server closed before successful
output; the owned postmaster marker was absent. No retry or wider suite was run.
This proves the selected independent-edge update and graph-deletion workflow;
object updates, evolution, imports, Truss feed/ACK, Weft and Delta integration
remain separate required outcomes.
