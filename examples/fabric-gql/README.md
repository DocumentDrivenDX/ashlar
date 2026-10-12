# Query an admitted Fabric graph

`ashlar query-fabric-gql` runs a bounded GQL read through an explicitly selected
operator provider. It returns columns, decoded scalar rows, and the original
responses as Base64 JSON after the provider closes. Supported scalar columns are
STRING, BOOL, INT64, UINT64, and null. Raw responses preserve extension content.

Create `fabric-query.json` with your actual workspace, graph model, and admitted
release SHA256. Replace the three placeholders; a release hash alone does not
authorize a query.

```json
{
  "profile": "ashlar-fabric-gql-invocation/0.1",
  "binding": {
    "workspace_id": "YOUR_CANONICAL_WORKSPACE_UUID",
    "graph_model_id": "YOUR_CANONICAL_GRAPH_MODEL_UUID",
    "release_sha256": "YOUR_ADMITTED_RELEASE_SHA256"
  },
  "query": "MATCH (n) RETURN count(n) AS count",
  "limits": {
    "maximum_response_bytes": 1048576,
    "maximum_total_bytes": 8388608,
    "maximum_rows": 1000,
    "maximum_requests": 32,
    "deadline_seconds": 60
  }
}
```

The provider is trusted Python operator code. It supplies ordinary authenticated
HTTP and the actual current publication/release policy; the CLI does not create
workspaces, load graphs, log in, or infer authority from retained hashes. Its
public entrypoint is `open_fabric_gql(invocation)`, a context manager yielding
`FabricGqlSession`. This minimal skeleton connects the existing HTTP owner to
your operator's actual release policy:

```python
from contextlib import contextmanager
from ashlar_host.fabric_http import open_fabric_http
from ashlar_host.fabric_gql_cli import FabricGqlSession
from my_operator import HTTP_CONFIG, open_current_release_policy

@contextmanager
def open_fabric_gql(invocation):
    with open_fabric_http(HTTP_CONFIG) as http:
        with open_current_release_policy(invocation.binding, http) as policy:
            yield FabricGqlSession(
                invocation.binding, http, policy, http.context)
```

`my_operator` is your deployment-specific implementation, not an Ashlar module.
`HTTP_CONFIG` is the existing typed `FabricHttpConfig`: it selects the already
authenticated delegated Azure CLI account, explicit configuration directory,
CLI/HTTP runtime selections, and finite capacities. Keep credentials out of the
query JSON and provider source. The HTTP owner obtains the token privately from
the selected CLI and closes its authenticated interval.

`open_current_release_policy` must acquire your genuine publication/source and
loaded Fabric release owners. Its yielded policy implements these ports:

- `hold(binding, context)` holds the matching publication/native release interval
  and yields `None` only after actual admission; closing independently renews it.
- `admit(binding, query, context)` returns `None` only while that exact interval
  still admits the original publication, current Fabric definition/loaded
  release, query, and HTTP context. It refuses changed or unavailable authority.

A no-op policy or a frozen release snapshot is insufficient. If you do not have
that actual operator integration, complete it before invoking the command.

Select the provider's bytes and run:

```sh
PROVIDER_SHA256=$(shasum -a 256 operator-fabric.py | cut -d ' ' -f 1)
python -m ashlar query-fabric-gql \
  --configuration /absolute/path/to/fabric-query.json \
  --provider /absolute/path/to/operator-fabric.py \
  --provider-sha256 "$PROVIDER_SHA256" > result.json
```

The provider digest selects trusted code; it grants no Fabric permission. Use
absolute canonical configuration/provider paths. Errors return exit status 2
with a fixed refusal message and no successful result. This command's basic
adapter tests use inert HTTP/policy ports; they do not qualify an installed
credential runtime, a live Fabric deployment, or public release-snapshot support.
