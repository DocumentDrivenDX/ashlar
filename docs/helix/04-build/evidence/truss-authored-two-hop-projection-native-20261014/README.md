# Authored two-hop root rows on retained Truss data

The installed public `query-truss-typed-batch` command completed one original
indexed 0.4.2 query on retained Truss15 publication `truss-graph-5`:

```sql
SELECT r."record-id" AS root FROM TypedRecord r
CROSS JOIN EXPAND_PATHS(r.related,related) AS p
```

Two independent R1 self-loop edges produce four ordered edge-occurrence pairs,
including reuse of either edge in both positions. The full result is four
`{"root":"R1"}` rows; isolated R2 emits none. The complete independent source
bag matches, all 13 original scalar/relationship/occurrence checks return zero,
and original compiler response and independent recompilation bytes are identical.
The batch retains its complete publication manifest and original protocols.

The public worker reported 28.889 seconds. The supervisor completed exit zero in
30.726788541069254 seconds, including owned PostgreSQL startup and closing; it
records the source stopped after child completion. Actual checkpoint-before and
checkpoint-after files are byte-identical and retained privately by hash. No
source mutation, publication or ACK was submitted.

Qualification is the wheel built from Ashlar
`6a081610582aee767ecf68f845af4aed4f3718d0`, released Truss toolkit 0.0.15,
PostgreSQL 18.6, Spark 4.0.1, Delta 4.0.0 and the pinned public indexed Weft 0.4.2 realization.
The original model remains UMF 0.7 with one physical revision. This admits only
one required STRING root-key field, without filters/order/limit/aggregation;
it does not qualify destination projections, general joins, another runtime,
managed Unity Catalog, production authority or a descendant-process inventory.

The small exact outputs, executed scripts and invocation are copied here;
`SHA256.json` records correspondence and private checkpoint/wheel/module pins.
Connection configurations, raw logs, compiler binaries and runtime inventories
are not copied. Native execution reused the retained graph rather than ingesting
or recreating it.
