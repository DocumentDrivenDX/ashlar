# Fabric UI query check

On 2026-10-12, the existing trial workspace `Ashlar Graph Integration` and
`Ashlar_R1` graph model were accessible through the signed-in Fabric UI.
The model displayed a last-loaded timestamp of 2026-10-11 17:37:43.

The following single query was submitted through Query now → Code editor:

```gql
MATCH (s:AshlarNode)-[e:AshlarEdge]->(t:AshlarNode)
RETURN e.graph_id AS edge_id, s.graph_id AS src, t.graph_id AS dst
ORDER BY edge_id
```

The UI remained at “Running Query” across subsequent observations. Cancel was
clicked; the running indicator and Cancel control disappeared, and the UI
displayed “Query result contains no data.” No result was obtained and this is
not an additional GQL pass. No retry, graph refresh, Spark job, capacity change
or permission change was performed. The draft queryset was not saved.

The next live integration should use the bounded query adapter or diagnose the
UI request before retrying. The retained successful 2026-10-11 corpus remains
the source for the copyable query gallery; it does not qualify a new release.
