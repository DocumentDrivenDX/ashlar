# Local Delta writer smoke check

The new `materialize_graph_release_delta` helper ran once on Spark 4.0.1,
local[1], with 512 MiB driver memory and the existing frozen graph fixture.
It wrote three nodes and four edges to fresh Delta tables, checked every STRING
carrier and null through complete bounded row-bag comparison, and observed
separate table UUIDs at version 0. The result is retained in `result.json`.

Three basic adapter tests also passed. The smoke used a fixture admission policy;
it does not qualify current production publication authority, OneLake access,
Fabric loading, GQL, or immutable deployment. No source pipeline was rerun and
no SDK inventory or scale benchmark was performed.
