# Read a publication through Fabric GQL

Use `ashlar_host.fabric_gql.execute_gql` with an explicit existing Fabric
workspace, graph model and original Ashlar release SHA-256. The binding identifies
the intended graph; an independent policy must admit its actual correspondence
to the immutable publication and hold that authority through result release.

Supply an ordinary authenticated transport with this interface:

```python
post(url, original_body_bytes, context, timeout_seconds) -> (http_status, raw_bytes)
```

It must honor the timeout, enforce a response limit before buffering, retain raw
failed or partial observations, and own credential handling. The reader neither
discovers credentials nor creates capacity or graph models. Supply
`policy.hold(binding, context)` and `policy.admit(binding, original_query, context)`
from the actual publication/native graph owner. Successful admission returns
`None`; the hold closes all acquired resources and revalidates current authority.

```python
from ashlar_host.fabric_gql import FabricGraphBinding, execute_gql

binding = FabricGraphBinding(workspace_id, graph_model_id, original_release_sha256)
result = execute_gql(
    binding, original_gql,
    transport=authenticated_transport, policy=publication_policy, context=held_context,
    maximum_response_bytes=1048576, maximum_total_bytes=8388608,
    maximum_rows=1000, maximum_requests=32, deadline_seconds=60,
)
```

The reader submits the unchanged query to the documented beta endpoint. It polls
opaque continuation tokens with the same body and encodes each token once. No
rows are released while execution is incomplete. Warnings, truncation, HTTP
errors, missing authority and exceeded capacity or deadline refuse the result.

The scalar projection supports STRING, BOOL, signed INT64, unsigned UINT64 and
null. Integer numbers and canonical decimal strings retain exact integer values;
invalid or unsupported types refuse. Returned columns and rows are immutable
tuples. Exact raw responses retain additional metadata for inspection. This
reader does not translate Weft SQL or prove a OneLake projection's graph identity.

Fabric's beta contract, supported encoding and continuation behavior are defined
by the [official GQL API reference](https://learn.microsoft.com/en-us/fabric/graph/gql-query-api).
Use a separately admitted OneLake projection and original-value oracle when
qualifying a real Ashlar graph in that engine.
