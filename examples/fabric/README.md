# Query two frozen Fabric graph releases

Run this checker from the Ashlar repository root with Python 3.9 or later and
Azure CLI on `PATH`. Use an existing signed-in account that can query both graph
items; the command does not provision capacity or change permissions.

Supply two distinct, already loaded graph items in the same workspace. Each
maps its own frozen release tables to `AshlarNode` and `AshlarEdge`, using
`graph_id` as the node key and `src`/`dst` as incident node keys. Map every
exported carrier column as `STRING`, preserving nulls and exact JSON text.
Open REST-created models once in the Fabric editor and wait for their initial
load to complete before querying.

Set `WORKSPACE_ID`, `R1_GRAPH_ID`, `R2_GRAPH_ID`, `R1_FILE`, `R2_FILE`,
`R1_SHA256` and `R2_SHA256` to your workspace, graph handles and original export
files/digests. Choose a fresh output filename:

```sh
PYTHONPATH=src python3 tools/check_fabric_releases.py \
  --workspace "$WORKSPACE_ID" \
  --r1-graph "$R1_GRAPH_ID" --r2-graph "$R2_GRAPH_ID" \
  --r1 "$R1_FILE" --r1-sha256 "$R1_SHA256" \
  --r2 "$R2_FILE" --r2-sha256 "$R2_SHA256" \
  --output fabric-report.json
```

The checker compares complete node/edge bags, actual incident node keys, counts,
distinct-edge two-hop paths and explicit `WALK` paths that allow repeated edges.
It then reads R1's complete bags again after R2. Native errors stop the run and
remain in the report; there are no automatic retries. Each request has a
60-second limit and a 1 MiB response limit. Tokens remain in process memory.

This checks frozen graph fixture correspondence through native beta GQL. A
passing report does not grant publication authority, prove original source
retention, or make a mutable graph safe for continuous refresh. The
publication-bound public reader requires its separate current authority owner.

Fabric's [model initialization](https://learn.microsoft.com/en-us/fabric/graph/manage-data)
and [path semantics](https://learn.microsoft.com/en-us/fabric/graph/gql-graph-patterns)
describe the corresponding native operations.
