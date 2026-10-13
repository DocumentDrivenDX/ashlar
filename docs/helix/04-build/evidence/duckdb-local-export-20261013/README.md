# Saved-release DuckDB query

The public `query-duckdb-export` CLI queried the existing Truss R2 export on
DuckDB v1.5.0 without starting Spark or connecting to Truss. Full carriers match;
three nodes and one edge produce one directed one-hop result, no two-hop paths,
and out-degrees 0, 0 and 1. The fresh report has mode 0600.

Two basic native DuckDB controls cover parallel edges, a cycle, self-loop,
isolate, path versus distinct counts, digest refusal and final-owner-close
refusal. They pass in under one second. Module boundaries and independent review
pass. This qualifies finite saved-carrier relational topology only; no Weft,
current source, native Delta or remote graph query claim is made.
