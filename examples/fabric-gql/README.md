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

## Prepare a graph definition locally

For already materialized, release-specific OneLake node and edge Delta tables,
generate the mapping files without contacting Fabric:

```sh
python -m ashlar prepare-fabric-graph-definition \
  --nodes 'abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/release_1_nodes' \
  --edges 'abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/release_1_edges' \
  --output /absolute/path/to/fresh-definition-directory
```

Replace the example workspace/lakehouse UUIDs and table names with your actual
immutable release paths. This subset accepts GUID-form `abfss` OneLake paths;
node/edge paths must differ, and literal `latest`/`current` path components are
unsupported. These checks do not prove that a table is immutable.

The output contains `dataSources.json`, `graphDefinition.json`, `graphType.json`,
`stylingConfiguration.json`, and `definition.json` with the REST `format: json`
and InlineBase64 parts. They follow the primary
[Graph Model definition](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/graph-model-definition)
and [Create Graph Model](https://learn.microsoft.com/en-us/rest/api/fabric/graphmodel/items/create-graph-model)
examples. `definition.json` is the **definition field**, not a complete create
request; an operator must supply the display name and perform deployment.

All 18 node and 23 edge carrier fields map as STRING, including canonical text
encodings. Nodes use `graph_id` as their primary key; edges retain independent
`graph_id` properties and map `src`/`dst` to node keys. The generator reads no
rows and makes no claim about materialization, parallel-edge ingestion,
loaded-engine authority, or live GQL correctness. It creates a new minimal
definition; it does not edit an existing definition or retain its custom parts.

## Materialize release tables in a Spark notebook

Use [materialize-release.py](materialize-release.py) as a Fabric notebook or
local Spark template. Supply your existing Spark session, actual admitted public
canonical `GraphRelease`, and a policy implementing
`admit_original_release(release, context)`. The template's `my_operator` is your
integration with the current publication/read owner, not an Ashlar module or a
hash-only policy. The strict public release loader retains its 16 MiB input and
10,000-row-per-bag limits. The public entrypoint does not accept private-custody
export profiles; use the separate explicit branch below.

`materialize_graph_release_delta` writes explicit STRING schemas to distinct,
nonoverlapping destinations with `errorifexists`. It compares the complete
version-0 row bags, including nulls, canonical IDs and endpoint values, using
bounded readback, and reports observed UUIDs/versions. It renews original release
admission before and after work. Local absolute paths or OneLake `abfss` paths
are supported; the notebook owns Spark and its ordinary storage access.

Failures preserve partial tables for inspection; the helper never overwrites or
repairs them. Choose fresh paths for a new explicit action. The template generates
a definition only after both tables verify. This is materialization preparation,
not a Fabric load or a graph-query qualification; no notebook/cloud run is
claimed by the basic inert Spark tests.


### Explicit private-local commerce export

For a retained commerce export with named
`ashlar-private-local-graph-custody/0.1` receipt, select **both** original release
and custody digests through your actual operator. Do not rewrite its manifest or
try the private loader as an automatic fallback. The separate entrypoint shares
the same STRING writes and complete bag checks:

```python
from ashlar_host.graph_release_delta import materialize_private_graph_release_delta
from ashlar_host.fabric_graph_definition import prepare_fabric_graph_definition
from my_operator import open_admitted_private_export

with open_admitted_private_export() as session:
    receipt = materialize_private_graph_release_delta(
        spark, session.release.payload, session.release.sha256,
        custody_payload=session.custody_bytes,
        trusted_custody_sha256=session.custody_sha256,
        source=session.source, node_path=NODES, edge_path=EDGES,
        policy=session.policy, context=session.context)

# After the original writer interval closes successfully:
definition = prepare_fabric_graph_definition(NODES, EDGES, DEFINITION_OUTPUT)
```

Your operator supplies the original source and a real policy with
`writer(context)` and
`admit_original_export(release, custody_bytes, source, context)`. The materializer
owns that writer interval; do not open a nested writer around it. Receipt/source
admission is renewed inside the interval before work and independently at
closing. The strict private loader validates the exact release/receipt pair;
there is no GraphFrames SDK dependency. This admits an explicitly selected
original export, not future canonical availability or a Fabric loaded graph.
