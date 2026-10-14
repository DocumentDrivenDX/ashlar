# Three original commerce queries on a retained Truss publication

Partial return, fulfillment and refund passed with unchanged original pack SQL
on publication `truss-graph-ce9a7e185f33f545e6cd30bdda650ed240bfb4db4fa86d4165c654e370d4567f`.
This is the complete original eleven-object/ten-edge publication, logical and
physical revision 1. The live source remains evolved with a later publication;
these reads select the retained original snapshot rather than its current values.
Each result matches the complete independent source-occurrence oracle:

| Case | Original positional result | Whole supervised duration |
| --- | --- | --- |
| Partial return | `["6","2","6"]` | 35.133 seconds |
| Fulfillment | `["[42,0,\"F2\"]"]` | 27.224 seconds |
| Refund | `["[42,0,\"RF1\"]"]` | 26.517 seconds |

Partial return preserves both `quantity` labels and the original expression label
as three positioned columns. All required facetless Integer source projections
contain their complete six-STRING row footprints: one order line, both
fulfillments and one return. The nonmatching fulfillment remains guarded.
The unchanged net expression is `10 - 6 + 2 = 6`. Fulfillment retains authored
ordering and selects the overfulfilled quantity 11. Refund retains the original
equality `amount = quantity * unit_price`, with original quantity 2, price 12.5
and refund amount 25. This is not an inequality or repaired scenario.

For every case, all remaining scalar, representability and arithmetic checks
return native STRING zero, and the two original compiler responses are
byte-identical. All source checks precede finite capacity checks and result SQL.
The current live source checkpoint is byte-identical before and after each read.
Every supervisor completed exit 0, after the query child closed Spark/source
sessions and the surviving supervisor stopped its owned PostgreSQL server before
releasing the directory lease. No owned query process remains running.

Qualification is local Truss toolkit 0.0.22, PostgreSQL 18.6, Spark 4.0.1,
Delta 4.0.0 and the public indexed Weft 0.4.2 implicit-arithmetic realization.
The existing installed Ashlar wheel from source commit
`68c72121721665b4417c59d23234255815bdb7ca` is reused, with SHA-256
`2a7220b19db816ab2f006e8f78534f2a94571ec67bcbf55b70d2632bcb5cc4a6`.
No new dependencies or schema/source/grant/retention settings were introduced.
The wheel report's provisional outer-cleanup qualification is superseded only
by these completed full-attempt closures. Original UMF remains declared 0.8.0.
Original source ID literals remain separate from actual allocated native IDs.

Together with [the evolved settlement pass](../truss22-commerce-indexed-settlement-20261014-c/README.md),
these qualify all four authored commerce queries on real Truss-derived local
Delta publications. They do not qualify another compiler/runtime, native edge
joins, all unbounded numerical populations, a release refresh/import, managed
Unity Catalog or production source/security authority. The development
caller-asserted receipt profile remains explicit.

The bounded archive retains each exact query report with original protocols,
source captures/guards/descriptors, before/after checkpoints, selections, scripts,
logs and terminal result. `inventory.json` hashes every capture. Large reused
binaries and prior publication/source archives remain externally pinned inputs.
