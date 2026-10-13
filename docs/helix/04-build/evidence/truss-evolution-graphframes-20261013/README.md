# Truss schema evolution: immutable export functionality

Two existing publications were exported and queried in one local GraphFrames session in 25.435 seconds. Both contain three nodes and one edge. Graph IDs, two untouched nodes, the edge, one-hop results and degree results match. Beta advances to version 2 and schema revision 2 with its new optional tag preserved in the exact property carrier. The source checkpoint was unchanged.

The engine-free `compare-graph-exports` CLI independently reads the same two exports and reports one changed node, zero additions/removals and unchanged edges. Two focused controls cover property changes, digest refusal and final-owner-close refusal; they pass in 0.085 seconds. Module boundaries pass. Independent review found no blocker.

These results cover bounded immutable local exports with Spark 3.5.3 and GraphFrames 0.12.3. They do not establish native optional-property query interpretation, export ancestry, Unity Catalog operation or complete domain-pack coverage. No new ingestion or scale run was performed.
