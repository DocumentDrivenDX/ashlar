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
examples. By default, `definition.json` is the **definition field**. Add
`--display-name 'Ashlar release 1'` to the command above to also generate
`create-request.json`, containing `displayName` and the same unmodified
`definition`. Names must be nonblank, at most 256 characters, and contain no
control characters; Fabric applies its own item naming rules on submission.

An authorized operator can submit that file as the JSON body of
`POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/graphModels`
with their Fabric bearer token. Creating the item can return 201 or 202;
follow the service's operation response when asynchronous. Generation performs
no API call, and item creation does not establish that the graph is loaded or
qualified for queries.

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
`ashlar-private-local-graph-custody/0.1` receipt, use the concrete
[local-development notebook template](materialize-private-export.py). It takes
both file paths and independently selected digests from the
`export-original-commerce` summary; it requires no continued PostgreSQL or
canonical snapshot availability and no fictional provider module.

```python
from pathlib import Path
from ashlar_host.local_graph_export import LocalGraphExportSelection, open_local_graph_export
from ashlar_host.fabric_graph_definition import prepare_fabric_graph_definition
from ashlar_host.graph_release_delta import materialize_private_graph_release_delta

selection = LocalGraphExportSelection(
    Path('/absolute/path/to/export/release.graph.json'),
    Path('/absolute/path/to/export/custody.json'),
    SELECTED_RELEASE_SHA256, SELECTED_CUSTODY_SHA256)
with open_local_graph_export(selection) as session:
    receipt = materialize_private_graph_release_delta(
        spark, session.release.payload, session.release.sha256,
        custody_payload=session.custody_bytes,
        trusted_custody_sha256=session.custody_sha256,
        source=session.source, node_path=NODES, edge_path=EDGES,
        policy=session.policy, context=session.context)

# After both tables and the local file-use owner close:
definition = prepare_fabric_graph_definition(NODES, EDGES, DEFINITION_OUTPUT)
```

This explicitly selects `ashlar-local-development-private-export-use/0.1`.
The owner renews ordinary effective-user read access and exact bytes/identities
of two owned regular files before use and at closing. Its nonnested `writer` is a
local-use interval, not a canonical publication lease. Source metadata remains
historical capture. Do not select digests by hashing untrusted inputs and treating
that as authorization. There is no manifest rewrite or public-loader fallback.

The notebook owns its existing Spark session and ordinary storage access; use
fresh, nonoverlapping Delta destinations. This owner supplies no storage
credentials, production/native publication authority, Fabric deployment or loaded
graph authority. It is not a GQL query policy. Both expected digests must be
independently trusted, and a changed file or revoked local read permission
withholds success. Basic local-file tests do not qualify a notebook/cloud run.

### Capture a commerce export from its actual reader

The package now supplies `read_commerce_export(reader_factory)` and the pure
`build_private_custody` receipt builder. The exporter uses the public commerce
held-release owner, captures the reader's actual closed protected-ACK
observations and original source metadata, and builds the pair only after the
outer reader closes. The builder preserves the original receipt encoding; it
validates correspondence and creates no source/ACK authority.

In an existing configured commerce Spark/read lifecycle, use the ordinary
connection and registered-source factories from the shipped
[query provider template](../original-commerce-local/query-provider.template.py):

```python
from contextlib import contextmanager
from ashlar_host.publication_reader import open_commerce_reader
from ashlar_host.commerce_graph_release import read_commerce_export
from completed_query_provider import (
    ordinary_query_connection_factory, commerce_query_source_factory)

# spark, publication and query_config are your existing configured query inputs.
@contextmanager
def reader_factory():
    with open_commerce_reader(
        spark, publication,
        ordinary_session=ordinary_query_connection_factory(query_config),
        source_factory=commerce_query_source_factory(query_config)) as opened:
        yield opened

release, custody_bytes, original_native_files = read_commerce_export(reader_factory)
# Only now persist release.payload and custody_bytes, or explicitly admit this
# pair through the private materializer. Never persist inside the reader factory.
```

`completed_query_provider` is your completed copy of the shipped template, with
its original configuration/endpoint selections. The reader factory must own its
surrounding configured resources; the caller retains Spark ownership.

### Export an existing local commerce publication from the CLI

The concrete command uses your existing original-commerce publication JSON to
select its ordinary source/ACK registration. It opens the actual read owner;
no operator-provider module or compiler installation is needed:

```sh
python -m ashlar export-original-commerce \
  --configuration /absolute/path/to/original-publish.json \
  --publication /absolute/path/to/existing-publication \
  --jars /absolute/path/to/selected-delta4-jars \
  --output /absolute/path/to/fresh-export
```

Use the supported **query** environment: Python 3.11 with SQLite 3.53.1, PySpark
4.0.1, Delta Spark 4.0.0, JDK 21, and the two pinned Delta 4 JARs listed in the
[supply-chain guide](../supply-chain-native/README.md). Install the ordinary
PostgreSQL driver required by the existing local source profile and retain the
running original fixture. Set `JAVA_HOME`, `PYSPARK_PYTHON`, and
`PYSPARK_DRIVER_PYTHON` for that selected query environment; leave ambient `PG*`
settings unset. The original publication's recorded publishing runtime does not
replace this query-runtime selection. Invoke through your finite process
supervisor, as with the existing commerce query commands.

The publication path must equal the existing configuration's output. The export
output must be fresh and separate from the publication, configuration, source
inputs and JARs. The command registers only ephemeral Spark catalog aliases of
existing admitted Delta targets; it does not initialize, append, submit ACK, or
repair persistent data. It uses local Spark with one thread and 512 MiB memory.

Only after the original reader/ACK/native intervals, Spark, and configuration
close successfully does the command expose read-only `release.graph.json` and
`custody.json` in the fresh private output directory. Its JSON summary includes
both digests for explicit selection by the private materializer. Existing paths
are never overwritten. A failed read or cleanup returns exit status 2 without a
successful pair; a later file-write failure may leave partial output for
inspection. The retained pair does not establish future canonical availability
or Fabric loaded-graph authority. Basic command tests use inert Spark/reader
ports; they do not claim a new native export execution.

### Materialize an exported pair locally from the CLI

After `export-original-commerce` succeeds, copy the two digests from its trusted
summary and run the concrete local-development materializer. This command needs
neither PostgreSQL, a source provider, nor a compiler installation:

```sh
python -m ashlar materialize-graph-export \
  --release /absolute/path/to/export/release.graph.json \
  --custody /absolute/path/to/export/custody.json \
  --release-sha256 "$SELECTED_RELEASE_SHA256" \
  --custody-sha256 "$SELECTED_CUSTODY_SHA256" \
  --node-path /absolute/path/to/fresh-local-nodes \
  --edge-path /absolute/path/to/fresh-local-edges \
  --jars /absolute/path/to/selected-delta4-jars \
  --output /absolute/path/to/fresh-materialization-report.json
```

The report's parent directory must already exist. Input files, node/edge
locations, JARs and report must not overlap; node/edge locations and report must
be fresh. The command supports **local absolute paths only**. For OneLake paths,
use the notebook template with its existing Spark session and storage access.

Use Python 3.11, PySpark 4.0.1, Delta Spark 4.0.0, JDK 21 and the same two pinned
Delta 4 JARs. This reuses the existing finite Spark runtime/JAR check without
requiring unrelated original model files or SQLite query machinery. Set the
selected Java/PySpark environment as above and run under your finite process
supervisor. The owned session uses one local thread and 512 MiB driver memory.

The actual local file-use owner validates the selected pair before Spark starts;
the private materializer compares complete fresh STRING bags. Only after Spark
and the file-use owner close successfully is the report written. Failed writes
preserve partial Delta tables and do not overwrite prior destinations. This is
local retained-export rematerialization, not continued canonical availability,
production source authority, storage credentials or Fabric graph activation.

## Copyable query gallery

[queries.gql](queries.gql) contains five statements copied exactly from the
successful [retained Fabric corpus](../../docs/helix/04-build/evidence/fabric-gql-corpus-20261011/fabric-corpus-d.json):
full 18-property node and 23-property edge projections, one-hop relationship
identities, and exact node/edge counts. Copy **one statement at a time** into
the selected loaded Graph Model's query editor, or use its text as the `query`
in a `query-fabric-gql` configuration with your actual release owner.

The mapping uses `AshlarNode` / `AshlarEdge` labels. Carrier properties remain
STRING (including canonical JSON text and textual versions); nulls remain
nulls. The edge projection retains independent `graph_id`, `src` and `dst`.
The one-hop query returns each relationship identity without `DISTINCT`, so
parallel relationships remain separate; node projection/count also includes
isolated nodes. Result order is unspecified. These counts describe the
selected loaded graph, not a fixed expected count for every release.

Keep the aliases as written: the earlier Fabric run rejected unquoted `edge`
and `value` as reserved identifiers. This is a convenience gallery from the
finite R1/R2 evidence, not a replacement for the Weft compiler or a claim that
an arbitrary current graph matches its publication. No new cloud run was
performed for this example.

### Ordinary delegated HTTP transport

A provider can use the separate ordinary-auth owner below instead of selecting
SDK/runtime inventories. Select your installed Azure CLI and its signed-in
operator-controlled configuration directory explicitly (current-user owned,
not writable by group or others; standard 0755 is accepted). Login is an operator
action. The owner
checks the selected subscription, tenant and delegated user before use and at
closing, obtains a private Fabric token, disables proxies/redirects, and bounds
authentication output and HTTP responses. A small
[live functional check](../../docs/helix/04-build/evidence/fabric-ordinary-http-20261012.md)
acquired and closed authentication and read the existing R1 node count. It does
not establish publication correspondence or broad runtime compatibility.

```python
from pathlib import Path
from contextlib import contextmanager
from ashlar_host.fabric_ordinary_http import (
    FabricOrdinaryHttpConfig, open_fabric_ordinary_http,
)
from ashlar_host.fabric_gql_cli import FabricGqlSession

@contextmanager
def open_fabric_gql(invocation):
    config = FabricOrdinaryHttpConfig(
        azure_cli=Path("/absolute/path/to/az"),
        azure_config_dir=Path("/absolute/path/to/azure-config"),
        account_id="YOUR-SUBSCRIPTION-UUID", tenant_id="YOUR-TENANT-UUID",
        account_user="you@example.com",
        workspace_id=invocation.binding.workspace_id,
        graph_model_id=invocation.binding.graph_model_id,
        maximum_response_bytes=invocation.limits["maximum_response_bytes"],
        maximum_total_bytes=invocation.limits["maximum_total_bytes"],
        maximum_requests=invocation.limits["maximum_requests"],
        deadline_seconds=invocation.limits["deadline_seconds"],
    )
    with open_fabric_ordinary_http(config) as http:
        # actual_release_policy must own current publication/native graph
        # correspondence; authentication alone cannot supply this policy.
        yield FabricGqlSession(invocation.binding, http,
                               actual_release_policy, http.context)
```

The defaults bound requests to 64 KiB, responses to 1 MiB, aggregate responses
to 8 MiB, authentication output to 64 KiB, and the interval to 60 seconds.
HTTP socket operations use bounded timeouts and deadline checks between reads;
these synchronous operations are not an independent hard wall-clock supervisor.
The ordinary owner supplies access only, never release authority. The existing
`execute_gql` still requires its genuine publication policy; the strict
`open_fabric_http` owner remains available and unchanged.

### Export an older retained original commerce publication

`export-original-commerce` also accepts this **export-only** configuration. It
selects an existing publication and actual ordinary source/ACK ports, without
requiring a new initialization handoff or re-running a producer:

```json
{
  "profile": "ashlar-original-commerce-export-configuration/0.1",
  "publication": "/absolute/path/to/existing-publication",
  "source": {
    "host": "127.0.0.1", "port": 25434, "database": "truss_e2e",
    "authentication": "isolated-loopback-trust",
    "scope": {
      "service_schema": "YOUR_ORIGINAL_ACK_SERVICE_SCHEMA",
      "scope_id": "YOUR_ORIGINAL_SCOPE_UUID",
      "installation_id": "YOUR_ORIGINAL_SCOPE_INSTALLATION_UUID",
      "consumer": "YOUR_ORIGINAL_CONSUMER",
      "feed": "private-original-commerce-fixture", "epoch": "original-commerce-v1"
    },
    "source_schema": "your_original_source_schema",
    "source_signature_sha256": "YOUR_ORIGINAL_SOURCE_SIGNATURE_SHA256",
    "writer_role": "your_original_writer",
    "reader_role": "your_original_reader",
    "ack_role": "your_original_ack"
  }
}
```

Copy the actual selections from the retained publication's protected ACK scope
and the original source fixture inventory; placeholders above are not grants.
Current ordinary reader/ACK access and source signature must still pass, with
full publication/native retention and fencing. The command reads original model
and graph bytes from the selected publication. Its existing Python 3.11,
SQLite 3.53.1, Spark 4.0.1 / Delta 4.0.0 and exact two-JAR requirements are unchanged;
no whole SDK/JDK inventory is added. The full publication configuration remains
supported.

```sh
ashlar export-original-commerce \
  --configuration /absolute/private/export-only.json \
  --publication /absolute/path/to/existing-publication \
  --jars /absolute/path/to/selected-delta-jars \
  --output /absolute/private/fresh-export
```

Use that closed pair in the materialization/OneLake notebook flow above. This
export-only profile does not initialize, append, submit ACKs, repair data or
construct a Fabric release grant.

## Generate a private Fabric notebook

After `export-original-commerce` succeeds, use its independently selected release
and custody hashes to create a notebook with the original pair embedded:

```sh
PYTHONPATH=src python examples/fabric-gql/prepare-commerce-notebook.py \
  --release /absolute/export/release.graph.json \
  --custody /absolute/export/custody.json \
  --release-sha256 YOUR_SELECTED_RELEASE_SHA256 \
  --custody-sha256 YOUR_SELECTED_CUSTODY_SHA256 \
  --nodes YOUR_FRESH_ONELAKE_NODE_PATH \
  --edges YOUR_FRESH_ONELAKE_EDGE_PATH \
  --display-name YOUR_GRAPH_MODEL_NAME \
  --output /absolute/fresh-commerce.ipynb
```

Import the generated notebook into your intended Fabric workspace and attach the
lakehouse. It installs the pinned Ashlar source, uses the notebook's existing
Spark session, writes fresh release-specific tables, and prepares model-definition
files. It does not create or load a model. The generated notebook contains your
selected data; keep it local until uploading to the intended workspace.
